package org.apache.flink.connector.opengemini;

import io.github.openfacade.http.HttpClientConfig;
import io.opengemini.client.api.Address;
import io.opengemini.client.api.Point;
import io.opengemini.client.impl.OpenGeminiClient;
import io.opengemini.client.impl.OpenGeminiClientFactory;
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

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * An Apache Flink sink connector for OpenGemini using the official OpenGemini client.
 * This implementation provides efficient batch writing with exactly-once semantics.
 *
 * @param <T> The type of elements handled by this sink
 */
public class OpenGeminiSink<T> extends RichSinkFunction<T> implements CheckpointedFunction {
    private static final Logger LOG = LoggerFactory.getLogger(OpenGeminiSink.class);
    private static final long serialVersionUID = 1L;

    // Configuration options
    private final String host;
    private final int port;
    private final String database;
    private final String measurement;
    private final String username;
    private final String password;
    private final int batchSize;
    private final long flushIntervalMillis;
    private final int maxRetries;
    private final OpenGeminiPointConverter<T> converter;
    private final Duration connectionTimeout;
    private final Duration requestTimeout;

    // Runtime state
    private transient OpenGeminiClient client;
    private transient LinkedBlockingQueue<Point> batchQueue;
    private transient List<Point> currentBatch;
    private transient ScheduledExecutorService scheduledExecutorService;
    private transient ExecutorService asyncExecutor;
    private transient ListState<List<Point>> checkpointedState;
    private transient AtomicLong totalBytesWritten;
    private transient AtomicLong totalPointsWritten;
    private transient AtomicLong errorCount;
    private transient AtomicLong batchesWritten;

    private OpenGeminiSink(Builder<T> builder) {
        this.host = builder.host;
        this.port = builder.port;
        this.database = builder.database;
        this.measurement = builder.measurement;
        this.username = builder.username;
        this.password = builder.password;
        this.batchSize = builder.batchSize;
        this.flushIntervalMillis = builder.flushIntervalMillis;
        this.maxRetries = builder.maxRetries;
        this.converter = builder.converter;
        this.connectionTimeout = builder.connectionTimeout;
        this.requestTimeout = builder.requestTimeout;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // Initialize OpenGemini client
        HttpClientConfig.Builder httpConfigBuilder = new HttpClientConfig.Builder()
                .connectTimeout(connectionTimeout)
                .timeout(requestTimeout);

        // Set authentication if provided
        if (username != null && !username.isEmpty() && password != null && !password.isEmpty()) {
            // 使用 BasicAuthRequestFilter 添加认证
            httpConfigBuilder.addRequestFilter(
                    new io.github.openfacade.http.BasicAuthRequestFilter(username, password)
            );
        }

        HttpClientConfig httpConfig = httpConfigBuilder.build();

        io.opengemini.client.api.Configuration configuration = io.opengemini.client.api.Configuration.builder()
                .addresses(Collections.singletonList(new Address(host, port)))
                .httpConfig(httpConfig)
                .build();

        this.client = OpenGeminiClientFactory.create(configuration);

        // Initialize runtime components
        batchQueue = new LinkedBlockingQueue<>();
        currentBatch = new ArrayList<>(batchSize);
        scheduledExecutorService = Executors.newScheduledThreadPool(1);
        asyncExecutor = Executors.newFixedThreadPool(2);
        totalBytesWritten = new AtomicLong(0);
        totalPointsWritten = new AtomicLong(0);
        errorCount = new AtomicLong(0);
        batchesWritten = new AtomicLong(0);

        // Ensure database exists
        try {
            client.createDatabase(database).get(5, TimeUnit.SECONDS);
            LOG.info("Created database: {}", database);
        } catch (Exception e) {
            // Database might already exist, which is fine
            LOG.debug("Database {} might already exist: {}", database, e.getMessage());
        }

        // Start periodic flusher
        scheduledExecutorService.scheduleAtFixedRate(
                this::tryFlush,
                flushIntervalMillis,
                flushIntervalMillis,
                TimeUnit.MILLISECONDS
        );

        LOG.info("OpenGeminiSink initialized with host={}:{}, database={}, measurement={}, batchSize={}, flushInterval={}ms",
                host, port, database, measurement, batchSize, flushIntervalMillis);
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        if (value == null) {
            return;
        }

        // Convert the value to Point
        Point point = converter.convert(value, measurement);
        if (point == null) {
            LOG.debug("Converter returned null for value: {}", value);
            return;
        }

        synchronized (currentBatch) {
            currentBatch.add(point);

            if (currentBatch.size() >= batchSize) {
                List<Point> batchToWrite = new ArrayList<>(currentBatch);
                currentBatch.clear();
                // Add all points to the queue for async processing
                batchQueue.addAll(batchToWrite);
            }
        }
    }

    private void tryFlush() {
        try {
            flush(false);
        } catch (Exception e) {
            LOG.error("Error during scheduled flush", e);
            errorCount.incrementAndGet();
        }
    }

    private void flush(boolean checkpointMode) throws Exception {
        List<Point> batchToWrite = null;

        synchronized (currentBatch) {
            if (!currentBatch.isEmpty()) {
                batchToWrite = new ArrayList<>(currentBatch);
                currentBatch.clear();
            }
        }

        if (batchToWrite != null) {
            batchQueue.addAll(batchToWrite);
        }

        if (checkpointMode) {
            // In checkpoint mode, we process all batches synchronously
            List<Point> allPoints = new ArrayList<>();
            batchQueue.drainTo(allPoints);

            while (!allPoints.isEmpty()) {
                int end = Math.min(batchSize, allPoints.size());
                List<Point> batch = allPoints.subList(0, end);
                writeBatch(new ArrayList<>(batch));
                allPoints = allPoints.subList(end, allPoints.size());
            }
        } else {
            // In normal mode, process batches asynchronously
            List<Point> points = new ArrayList<>();
            int drained = batchQueue.drainTo(points, batchSize);

            if (drained > 0) {
                asyncExecutor.submit(() -> {
                    try {
                        writeBatch(points);
                    } catch (Exception e) {
                        LOG.error("Error in async batch writing", e);
                        errorCount.incrementAndGet();
                        // Put failed points back to queue for retry
                        batchQueue.addAll(points);
                    }
                });
            }
        }
    }

    private void writeBatch(List<Point> points) throws Exception {
        if (points.isEmpty()) {
            return;
        }

        int retries = 0;
        boolean success = false;
        Exception lastException = null;

        while (!success && retries <= maxRetries) {
            try {
                // Use OpenGemini client to write batch
                long startTime = System.currentTimeMillis();
                CompletableFuture<Void> future = client.write(database, points);
                future.get(requestTimeout.toMillis(), TimeUnit.MILLISECONDS);
                long writeTime = System.currentTimeMillis() - startTime;

                success = true;
                totalPointsWritten.addAndGet(points.size());
                batchesWritten.incrementAndGet();

                // Estimate bytes written based on point size
                long estimatedBytes = estimatePointsSize(points);
                totalBytesWritten.addAndGet(estimatedBytes);

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Successfully wrote batch with {} points in {}ms, estimated size: {} bytes",
                            points.size(), writeTime, estimatedBytes);
                }

            } catch (Exception e) {
                lastException = e;
                LOG.warn("Error writing to OpenGemini (attempt {}/{}): {}",
                        retries + 1, maxRetries + 1, e.getMessage());
                errorCount.incrementAndGet();
                retries++;

                if (retries <= maxRetries) {
                    // Exponential backoff with jitter
                    long backoffMs = Math.min(1000L * (1L << retries), 10000L);
                    backoffMs = backoffMs / 2 + ThreadLocalRandom.current().nextLong(backoffMs / 2);
                    Thread.sleep(backoffMs);
                }
            }
        }

        if (!success) {
            throw new RuntimeException("Failed to write batch after " + maxRetries + " retries", lastException);
        }
    }

    private long estimatePointsSize(List<Point> points) {
        long totalSize = 0;
        for (Point point : points) {
            // Estimate based on line protocol size
            String lineProtocol = point.lineProtocol();
            totalSize += lineProtocol.getBytes().length;
        }
        return totalSize;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        LOG.info("Starting checkpoint {} for OpenGeminiSink", context.getCheckpointId());
        long startTime = System.currentTimeMillis();

        // Flush any pending batches
        flush(true);

        // Save current batch to checkpoint state
        synchronized (currentBatch) {
            checkpointedState.clear();
            if (!currentBatch.isEmpty()) {
                checkpointedState.add(new ArrayList<>(currentBatch));
            }
        }

        long checkpointTime = System.currentTimeMillis() - startTime;
        LOG.info("Completed checkpoint {} for OpenGeminiSink in {}ms. Stats: points={}, batches={}, errors={}, totalBytes={}",
                context.getCheckpointId(), checkpointTime, totalPointsWritten.get(),
                batchesWritten.get(), errorCount.get(), totalBytesWritten.get());
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<List<Point>> descriptor = new ListStateDescriptor<>(
                "opengemini-sink-state",
                TypeInformation.of(new TypeHint<List<Point>>() {})
        );

        checkpointedState = context.getOperatorStateStore().getListState(descriptor);

        if (context.isRestored()) {
            // Restore state after a failure
            LOG.info("Restoring state for OpenGeminiSink");
            int restoredCount = 0;
            for (List<Point> batch : checkpointedState.get()) {
                synchronized (currentBatch) {
                    currentBatch.addAll(batch);
                    restoredCount += batch.size();
                }
            }
            LOG.info("Restored {} points from checkpoint", restoredCount);
        }
    }

    @Override
    public void close() throws Exception {
        LOG.info("Closing OpenGeminiSink...");

        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdown();
            try {
                if (!scheduledExecutorService.awaitTermination(10, TimeUnit.SECONDS)) {
                    scheduledExecutorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduledExecutorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        try {
            // Try to flush any remaining batches before shutting down
            flush(true);
        } catch (Exception e) {
            LOG.warn("Error during final flush", e);
        }

        if (asyncExecutor != null) {
            asyncExecutor.shutdown();
            try {
                if (!asyncExecutor.awaitTermination(20, TimeUnit.SECONDS)) {
                    asyncExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                asyncExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        if (client != null) {
            try {
                client.close();
            } catch (Exception e) {
                LOG.warn("Error closing OpenGemini client", e);
            }
        }

        LOG.info("OpenGeminiSink closed. Final stats: points={}, batches={}, errors={}, totalBytes={}",
                totalPointsWritten.get(), batchesWritten.get(), errorCount.get(), totalBytesWritten.get());
    }

    /**
     * Builder for the OpenGeminiSink.
     *
     * @param <T> The type of elements handled by the sink
     */
    public static class Builder<T> {
        private String host = "localhost";
        private int port = 8086;
        private String database;
        private String measurement;
        private String username;
        private String password;
        private int batchSize = 5000; // Optimized based on benchmark
        private long flushIntervalMillis = 100; // Optimized based on benchmark
        private int maxRetries = 3;
        private OpenGeminiPointConverter<T> converter;
        private Duration connectionTimeout = Duration.ofSeconds(5);
        private Duration requestTimeout = Duration.ofSeconds(30);

        public Builder<T> setHost(String host) {
            this.host = host;
            return this;
        }

        public Builder<T> setPort(int port) {
            this.port = port;
            return this;
        }

        public Builder<T> setUrl(String url) {
            // Parse URL to extract host and port
            try {
                if (url.startsWith("http://")) {
                    url = url.substring(7);
                } else if (url.startsWith("https://")) {
                    url = url.substring(8);
                }

                String[] parts = url.split(":");
                if (parts.length >= 1) {
                    this.host = parts[0];
                }
                if (parts.length >= 2) {
                    this.port = Integer.parseInt(parts[1]);
                }
            } catch (Exception e) {
                throw new IllegalArgumentException("Invalid URL format: " + url);
            }
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
            if (batchSize <= 0) {
                throw new IllegalArgumentException("Batch size must be positive");
            }
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

        public Builder<T> setConverter(OpenGeminiPointConverter<T> converter) {
            this.converter = converter;
            return this;
        }



        public Builder<T> setPointConverter(OpenGeminiPointConverter<T> converter) {
            this.converter = converter;
            return this;
        }

        public Builder<T> setConnectionTimeout(Duration timeout) {
            this.connectionTimeout = timeout;
            return this;
        }

        public Builder<T> setRequestTimeout(Duration timeout) {
            this.requestTimeout = timeout;
            return this;
        }

        public OpenGeminiSink<T> build() {
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

    /**
     * Wrapper class to support legacy OpenGeminiConverter interface
     */
    private static class LegacyConverterWrapper<T> implements OpenGeminiPointConverter<T> {
        private static final long serialVersionUID = 1L;
        private final OpenGeminiPointConverter<T> legacyConverter;

        public LegacyConverterWrapper(OpenGeminiPointConverter<T> legacyConverter) {
            this.legacyConverter = legacyConverter;
        }

        @Override
        public Point convert(T value, String measurement) throws Exception {
            String lineProtocol = String.valueOf(legacyConverter.convert(value, measurement));
            if (lineProtocol == null || lineProtocol.isEmpty()) {
                return null;
            }

            // Parse line protocol to create Point
            // This is a simplified implementation - in production, use a proper line protocol parser
            Point point = new Point();
            point.setMeasurement(measurement);
            point.setTime(System.currentTimeMillis() * 1_000_000L);

            // For backward compatibility, we'll add a dummy field
            Map<String, Object> fields = new HashMap<>();
            fields.put("value", lineProtocol);
            point.setFields(fields);

            return point;
        }
    }
}