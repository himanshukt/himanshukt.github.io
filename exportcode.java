package com.example.service.impl;

import com.example.constant.NetcAcqConstant;
import com.example.entity.InitFileDetails;
import com.example.enums.NetcAcqErrorCode;
import com.example.exceptions.ServiceException;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Service for exporting database data to CSV files using multithreaded processing.
 * 
 * <p>This service implements parallel data export by partitioning the data across multiple threads,
 * each thread processes a subset of data and writes to separate CSV files. After all partitions
 * complete, the files are merged into a single output file.</p>
 * 
 * <p>Key features:</p>
 * <ul>
 *   <li>Configurable number of partitions for parallel processing</li>
 *   <li>Configurable fetch size for database result sets</li>
 *   <li>Automatic file merging and cleanup of temporary partition files</li>
 *   <li>Progress tracking and performance metrics logging</li>
 * </ul>
 * 
 * @author Himanshu Kumar 
 * @since 18/09/25
 */
@Service
@Slf4j
public class InitExportUsingThreadPool {

    private final DataSource dataSource;

    @Value("${export.numPartitions}")
    private int numPartitions;

    @Value("${export.fetchSize}")
    private int fetchSize;

    @Value("${export.outputDir}")
    private String outputDir;

    @Value("${export.query}")
    private String query;

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");

    public static final String INIT_WORKER = "init-worker-";
    public InitExportUsingThreadPool(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * Exports data from database to CSV file using multithreaded processing.
     * 
     * <p>This method creates multiple worker threads to process data in parallel,
     * waits for all threads to complete, merges the partition files, and updates
     * the file details with total record count.</p>
     * 
     * @param fileDetails the file details containing output filename and other metadata
     * @throws RuntimeException if thread interruption occurs during processing
     */
    public void exportData(InitFileDetails fileDetails) {
        Instant start = Instant.now();
        log.info("Starting export with {} partitions ", numPartitions);
        ExecutorService executor = getThreadPool(numPartitions);
        CountDownLatch latch = new CountDownLatch(numPartitions);
        List<Future<Long>> futures = new ArrayList<>();

        String baseFileName = fileDetails.getFileName().replaceAll(".csv$", "");
        for (int partId = 0; partId < numPartitions; partId++) {
            futures.add(executor.submit(
                    new ExportWorker(dataSource, partId, numPartitions, fetchSize, outputDir, query, latch, baseFileName)));
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            log.error("Exception Occurred while thread execution ", e);
            throw new ServiceException(NetcAcqErrorCode.THREAD_INTERRUPTED);
        }
        executor.shutdown();
        long totalRows = futures.stream().mapToLong(f -> {
            try {
                return f.get();
            } catch (Exception e) {
                return 0;
            }
        }).sum();
        Instant end = Instant.now();
        log.info(" Export completed. Total rows exported = {} ", totalRows);
        log.info("Total time taken = {} seconds" , Duration.between(start, end).toSeconds());
        mergeFilesAndCleanup(outputDir, fileDetails.getFileName(), numPartitions);
        fileDetails.setRecords(totalRows);
    }

    private ExecutorService getThreadPool(int numPartitions) {
        ThreadFactory threadFactory = new ThreadFactory() {
            private final AtomicInteger counter = new AtomicInteger(1);
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setName(INIT_WORKER + counter.getAndIncrement());
                return t;
            }
        };
        return Executors.newFixedThreadPool(numPartitions, threadFactory);
    }

    /**
     * Worker thread that processes a partition of data and writes to a CSV file.
     * 
     * <p>Each worker handles a specific partition of the data based on modulo operation
     * on the partition ID. The worker executes the configured query with partition
     * parameters and writes results to a temporary CSV file.</p>
     */
    static class ExportWorker implements Callable<Long> {
        private final DataSource dataSource;
        private final int partitionId;
        private final int numPartitions;
        private final int fetchSize;
        private final String outputDir;
        private final String query;
        private final CountDownLatch latch;
        private final String baseFileName;

        /**
         * Constructs an ExportWorker for processing a data partition.
         * 
         * @param dataSource the database connection source
         * @param partitionId the ID of this partition (0-based)
         * @param numPartitions total number of partitions
         * @param fetchSize number of rows to fetch at once from database
         * @param outputDir directory where partition files will be written
         * @param query SQL query to execute (should accept numPartitions-1 and partitionId as parameters)
         * @param latch countdown latch for synchronization
         * @param baseFileName base name for the partition file (without extension)
         */
        ExportWorker(DataSource dataSource, int partitionId, int numPartitions,
                     int fetchSize, String outputDir, String query, CountDownLatch latch, String baseFileName) {
            this.dataSource = dataSource;
            this.partitionId = partitionId;
            this.numPartitions = numPartitions;
            this.fetchSize = fetchSize;
            this.outputDir = outputDir;
            this.query = query;
            this.latch = latch;
            this.baseFileName = baseFileName;
        }

        /**
         * Executes the data export for this partition.
         * 
         * @return the number of rows processed by this partition
         */
        @Override
        public Long call() {
            long rowCount = 0;
            String fileName = outputDir + "/" + baseFileName + "_part_" + partitionId + ".csv";
            MDC.put(NetcAcqConstant.LOG_CONTENT_ID, fileName);
            log.info("Worker for partition {} writing to file {}", partitionId, fileName);
            Instant partStart = Instant.now();
            log.info(" Partition {}  started at  {}", partitionId , partStart);

            try (Connection conn = dataSource.getConnection();
                 PreparedStatement ps = conn.prepareStatement(query,
                         ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                 BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {

                ps.setFetchSize(fetchSize);
                ps.setInt(1, numPartitions - 1);
                ps.setInt(2, partitionId);

                log.info("Partition {} executing query...", partitionId);
                Instant queryStart = Instant.now();
                try (ResultSet rs = ps.executeQuery()) {
                    log.info("Partition {} started fetching rows...", partitionId);
                    StringBuilder sb = new StringBuilder(100);
                    while (rs.next()) {
                        sb.setLength(0);
                        sb.append(rs.getString(1)).append(",")
                                .append(rs.getString(2))
                                .append(",").append(getFormatedDate(rs.getTimestamp(3)))
                                .append(",").append(rs.getString(4))
                                .append(",").append(rs.getString(5));
                        writer.write(sb.toString());
                        writer.newLine();
                        rowCount++;
                        if (rowCount % fetchSize == 0) {
                            log.info("Partition {} processed  {} rows so far...",partitionId, rowCount);
                        }
                    }
                }
                writer.flush();
                Instant queryEnd = Instant.now();
                log.info("Partition {}  completed. Rows: {} , Time taken = {}  sec", partitionId,rowCount,Duration.between(queryStart, queryEnd).toSeconds() );

            } catch (Exception e) {
                log.error(" Partition  {},failed {} ,{}", partitionId , e.getMessage(),e);
                throw new ServiceException(NetcAcqErrorCode.DATA_EXPORT_FAILED);
            } finally {
                latch.countDown();
            }
            Instant partEnd = Instant.now();
            log.info("Partition {} finished in {}seconds", partitionId , Duration.between(partStart, partEnd).toSeconds() );
            return rowCount;
        }
    }

    static String getFormatedDate(Timestamp  timestamp) {
        try{
            return (timestamp != null) ? timestamp.toLocalDateTime().format(FORMATTER) : "";
        }catch (Exception e){
            log.info("Exception in date formatting {}", e.getMessage());
            return "";
        }
    }

    /**
     * Merges all partition CSV files into a single output file and cleans up temporary files.
     * 
     * <p>This method reads all partition files sequentially, writes their content to the
     * final output file, and deletes the temporary partition files after successful merge.</p>
     * 
     * @param outputDir directory containing the partition files
     * @param fileName name of the final merged output file
     * @param numPartitions number of partition files to merge
     * @throws RuntimeException if file I/O operations fail during merge
     */
    public void mergeFilesAndCleanup(String outputDir, String fileName, int numPartitions) {
        File mergedFile = new File(outputDir, fileName);

        log.info(" Starting merge of {} files into {}", numPartitions, mergedFile.getAbsolutePath());
        long start = System.currentTimeMillis();

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(mergedFile))) {
            for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
                String baseFileName = fileName.replaceAll(".csv$", "");
                File partFile = new File(outputDir, baseFileName + "_part_" + partitionId + ".csv");
                if (!partFile.exists()) {
                    log.error(" Part file missing: {}", partFile.getAbsolutePath());
                    throw new ServiceException(NetcAcqErrorCode.PART_FILE_DOESNT_EXIST);
                }
                log.info(" Merging {}", partFile.getName());
                try (BufferedReader reader = new BufferedReader(new FileReader(partFile))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        writer.write(line);
                        writer.newLine();
                    }
                }
                // Delete part file after merging
                if (partFile.delete()) {
                    log.info("Deleted {}", partFile.getName());
                } else {
                    log.error(" Could not delete {}", partFile.getName());
                }
            }
        } catch (IOException e) {
            log.error(" Failed during merge", e);
            throw new ServiceException(NetcAcqErrorCode.FILE_MERGE_FAILED);
        }
        long end = System.currentTimeMillis();
        log.info("Merge completed. Final file: {} | Time taken = {} sec", mergedFile.getAbsolutePath(), (end - start) / 1000);
    }

}

