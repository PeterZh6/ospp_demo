package org.apache.flink.connector.opengemini;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.Charset;

/**
 * A basic Apache Flink sink connector for OpenGemini time series database.
 * This implementation provides a minimal but functional connector for testing purposes.
 *
 * @param <T> The type of elements handled by this sink
 */
public class OpenGeminiSink<T> extends RichSinkFunction<T> implements CheckpointedFunction {
    private static final Logger LOG = LoggerFactory.getLogger(OpenGeminiSink.class);
    private static final long serialVersionUID = 1L;


    // Configuration options
    private final String url;
    private final String database;
    private final String measurement;
    private final String username;
    private final String password;
    private final int batchSize;
    private final long flushIntervalMillis;
    private final int maxRetries;
    private final OpenGeminiConverter<T> converter;

    // Runtime state
    private transient LinkedBlockingQueue<String> batchQueue;
    private transient List<String> currentBatch;
    private transient ExecutorService executorService;
    private transient ScheduledExecutorService scheduledExecutorService;
    private transient ListState<List<String>> checkpointedState;
    private transient AtomicLong totalBytesWritten;
    private transient AtomicLong totalPointsWritten;
    private transient AtomicLong errorCount;

    private OpenGeminiSink(Builder<T> builder) {
        this.url = builder.url;
        this.database = builder.database;
        this.measurement = builder.measurement;
        this.username = builder.username;
        this.password = builder.password;
        this.batchSize = builder.batchSize;
        this.flushIntervalMillis = builder.flushIntervalMillis;
        this.maxRetries = builder.maxRetries;
        this.converter = builder.converter;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        batchQueue = new LinkedBlockingQueue<>();
        currentBatch = new ArrayList<>(batchSize);
        executorService = Executors.newFixedThreadPool(1);
        scheduledExecutorService = Executors.newScheduledThreadPool(1);
        totalBytesWritten = new AtomicLong(0);
        totalPointsWritten = new AtomicLong(0);
        errorCount = new AtomicLong(0);

        // Start periodic flusher
        scheduledExecutorService.scheduleAtFixedRate(
                this::tryFlush,
                flushIntervalMillis,
                flushIntervalMillis,
                TimeUnit.MILLISECONDS
        );

        LOG.info("OpenGeminiSink initialized with url={}, database={}, measurement={}, batchSize={}",
                url, database, measurement, batchSize);
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        String point = converter.convert(value, measurement);
        if (point == null || point.isEmpty()) {
            return;
        }

        synchronized (currentBatch) {
            currentBatch.add(point);

            if (currentBatch.size() >= batchSize) {
                List<String> batchToWrite = new ArrayList<>(currentBatch);
                currentBatch.clear();
                batchQueue.put(String.join("\n", batchToWrite));
            }
        }
    }

    private void tryFlush() {
        try {
            flush(false);
        } catch (Exception e) {
            LOG.error("Error during scheduled flush", e);
        }
    }

    private void flush(boolean checkpointMode) throws InterruptedException {
        List<String> batchToWrite = null;

        synchronized (currentBatch) {
            if (!currentBatch.isEmpty()) {
                batchToWrite = new ArrayList<>(currentBatch);
                currentBatch.clear();
            }
        }

        if (batchToWrite != null) {
            batchQueue.put(String.join("\n", batchToWrite));
        }

        if (checkpointMode) {
            // In checkpoint mode, we process all batches in the queue
            List<CompletableFuture<Void>> futures = new ArrayList<>();

            String batch;
            while ((batch = batchQueue.poll()) != null) {
                final String batchToSend = batch;
                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                    try {
                        writeBatch(batchToSend);
                    } catch (Exception e) {
                        LOG.error("Error writing batch during checkpoint", e);
                        throw new RuntimeException(e);
                    }
                }, executorService);
                futures.add(future);
            }

            // Wait for all batch writes to complete
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        } else {
            // In normal mode, we start an async task to process batches
            CompletableFuture.runAsync(() -> {
                try {
                    String batch = batchQueue.poll(100, TimeUnit.MILLISECONDS);
                    if (batch != null) {
                        writeBatch(batch);
                    }
                } catch (Exception e) {
                    LOG.error("Error in async batch writing", e);
                }
            }, executorService);
        }
    }

    private void writeBatch(String batchData) throws Exception {
        byte[] data = batchData.getBytes(StandardCharsets.UTF_8);
        int retries = 0;
        boolean success = false;

        while (!success && retries <= maxRetries) {
            try {
                HttpURLConnection connection = createConnection();
                connection.setRequestMethod("POST");
                connection.setDoOutput(true);
                connection.getOutputStream().write(data);

                int responseCode = connection.getResponseCode();
                if (responseCode == HttpURLConnection.HTTP_OK || responseCode == HttpURLConnection.HTTP_NO_CONTENT) {
                    success = true;
                    totalBytesWritten.addAndGet(data.length);
                    totalPointsWritten.addAndGet(batchData.split("\n").length);
                    LOG.debug("Successfully wrote batch with {} bytes", data.length);
                } else {
                    String errorMessage = readAllBytesAsString(connection.getErrorStream(), StandardCharsets.UTF_8);
                    LOG.warn("Error writing to OpenGemini (code: {}): {}", responseCode, errorMessage);
                    errorCount.incrementAndGet();
                    retries++;
                    if (retries <= maxRetries) {
                        Thread.sleep(Math.min(1000 * retries, 10000)); // Exponential backoff capped at 10 seconds
                    }
                }

                connection.disconnect();
            } catch (IOException e) {
                LOG.warn("IO exception while writing to OpenGemini", e);
                errorCount.incrementAndGet();
                retries++;
                if (retries <= maxRetries) {
                    Thread.sleep(Math.min(1000 * retries, 10000));
                } else {
                    throw e; // Re-throw the exception if we've exhausted retries
                }
            }
        }

        if (!success) {
            throw new IOException("Failed to write batch after " + maxRetries + " retries");
        }
    }

    private HttpURLConnection createConnection() throws IOException {
        // Construct the write URL: http://host:port/write?db=database
        String writeUrl = url + "/write?db=" + database + "&precision=ns";
        URL url = new URL(writeUrl);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();

        // Set basic auth if provided
        if (username != null && !username.isEmpty() && password != null) {
            String auth = username + ":" + password;
            String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes(StandardCharsets.UTF_8));
            connection.setRequestProperty("Authorization", "Basic " + encodedAuth);
        }

        connection.setRequestProperty("Content-Type", "text/plain");
        connection.setConnectTimeout(5000);
        connection.setReadTimeout(30000);

        return connection;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        LOG.info("Starting checkpoint {} for OpenGeminiSink", context.getCheckpointId());

        // Flush any pending batches
        flush(true);

        // Update checkpointed state if needed
        synchronized (currentBatch) {
            if (!currentBatch.isEmpty()) {
                checkpointedState.clear();
                checkpointedState.add(new ArrayList<>(currentBatch));
            } else {
                checkpointedState.clear();
            }
        }

        LOG.info("Completed checkpoint {} for OpenGeminiSink. Stats: written={} bytes, points={}, errors={}",
                context.getCheckpointId(), totalBytesWritten.get(), totalPointsWritten.get(), errorCount.get());
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<List<String>> descriptor = new ListStateDescriptor<>(
                "opengemini-sink-state",
                TypeInformation.of(new TypeHint<List<String>>() {
                })
        );

        checkpointedState = context.getOperatorStateStore().getListState(descriptor);

        if (context.isRestored()) {
            // Restore state after a failure
            LOG.info("Restoring state for OpenGeminiSink");
            for (List<String> batch : checkpointedState.get()) {
                synchronized (currentBatch) {
                    currentBatch.addAll(batch);
                }
            }
            LOG.info("Restored {} points from checkpoint", currentBatch.size());
        }
    }

    @Override
    public void close() throws Exception {
        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdown();
            try {
                if (!scheduledExecutorService.awaitTermination(10, TimeUnit.SECONDS)) {
                    scheduledExecutorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduledExecutorService.shutdownNow();
            }
        }

        if (executorService != null) {
            try {
                // Try to flush any remaining batches before shutting down
                flush(true);
            } catch (Exception e) {
                LOG.warn("Error during final flush", e);
            }

            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(20, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
            }
        }

        LOG.info("OpenGeminiSink closed. Final stats: written={} bytes, points={}, errors={}",
                totalBytesWritten, totalPointsWritten, errorCount);
    }

    /**
     * Builder for the OpenGeminiSink.
     *
     * @param <T> The type of elements handled by the sink
     */
    public static class Builder<T> {
        private String url;
        private String database;
        private String measurement;
        private String username;
        private String password;
        private int batchSize = 1000;
        private long flushIntervalMillis = 1000;
        private int maxRetries = 3;
        private OpenGeminiConverter<T> converter;

        public Builder<T> setUrl(String url) {
            this.url = url;
            return this;
        }

        public Builder<T> setDatabase(String database) {
            this.database = database;
            return this;
        }

        public Builder<T> setMeasurement(String measurement) {
            this.measurement = measurement;
            return this;
        }

        public Builder<T> setUsername(String username) {
            this.username = username;
            return this;
        }

        public Builder<T> setPassword(String password) {
            this.password = password;
            return this;
        }

        public Builder<T> setBatchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder<T> setFlushInterval(long interval, TimeUnit unit) {
            this.flushIntervalMillis = unit.toMillis(interval);
            return this;
        }

        public Builder<T> setMaxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public Builder<T> setConverter(OpenGeminiConverter<T> converter) {
            this.converter = converter;
            return this;
        }

        public OpenGeminiSink<T> build() {
            if (url == null || url.isEmpty()) {
                throw new IllegalArgumentException("URL must be provided");
            }
            if (database == null || database.isEmpty()) {
                throw new IllegalArgumentException("Database must be provided");
            }
            if (measurement == null || measurement.isEmpty()) {
                throw new IllegalArgumentException("Measurement must be provided");
            }
            if (converter == null) {
                throw new IllegalArgumentException("Converter must be provided");
            }

            return new OpenGeminiSink<>(this);
        }
    }

    /**
     * Creates a new builder for the OpenGeminiSink.
     *
     * @param <T> The type of elements handled by the sink
     * @return A new builder instance
     */
    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    private String readAllBytesAsString(InputStream inputStream, Charset charset) throws IOException {
        if (inputStream == null) {
            return "";
        }
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int length;
        while ((length = inputStream.read(buffer)) != -1) {
            result.write(buffer, 0, length);
        }
        return result.toString(charset.name());
    }
}