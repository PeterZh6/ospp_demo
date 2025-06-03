import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.connector.opengemini.OpenGeminiPointConverter;
import org.apache.flink.connector.opengemini.OpenGeminiSink;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

/**
 * Enhanced Integration test for the OpenGeminiSink with detailed performance metrics.
 * Note: This test requires a running OpenGemini instance.
 */
public class EnhancedOpenGeminiSinkIT {

    private static final String OPENGEMINI_URL = "http://localhost:8086";
    private static final String DATABASE = "test_db";
    private static final String MEASUREMENT = "test_measurement";
    private static final String USERNAME = ""; // Set if needed
    private static final String PASSWORD = ""; // Set if needed

    private MiniClusterWithClientResource flinkCluster;

    @Before
    public void setUp() throws Exception {
        // Set up a mini Flink cluster for testing
        flinkCluster = new MiniClusterWithClientResource(
                new MiniClusterResourceConfiguration.Builder()
                        .setNumberSlotsPerTaskManager(2)
                        .setNumberTaskManagers(1)
                        .build());
        flinkCluster.before();

        // Create database if it doesn't exist
        createDatabaseIfNeeded();
    }

    @After
    public void tearDown() {
        if (flinkCluster != null) {
            flinkCluster.after();
        }
    }

    @Test
    public void testBasicSinkWithMetrics() throws Exception {
        System.out.println("=== Basic Sink Test with Metrics ===");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(1000);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(5, TimeUnit.SECONDS)));

        // Create source with metrics collection
        MetricsCollectingTestSource source = new MetricsCollectingTestSource(1000, "basic-test");

        long jobStartTime = System.currentTimeMillis();

        env.addSource(source)
                .setParallelism(1)
                .addSink(createOpenGeminiSink())
                .setParallelism(1);

        env.execute("OpenGemini Basic Sink Test");

        long jobEndTime = System.currentTimeMillis();

        // Print detailed metrics
        printTestMetrics("Basic Sink Test", source, jobStartTime, jobEndTime, 1000, 1);
    }

    @Test
    public void testBatchWritingWithMetrics() throws Exception {
        System.out.println("=== Batch Writing Test with Metrics ===");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        int recordCount = 50000;
        MetricsCollectingTestSource source = new MetricsCollectingTestSource(recordCount, "batch-test");

        long jobStartTime = System.currentTimeMillis();

        env.addSource(source)
                .setParallelism(1)
                .addSink(createOpenGeminiSink(5000)) // Larger batch size
                .setParallelism(1);

        env.execute("OpenGemini Batch Write Test");

        long jobEndTime = System.currentTimeMillis();

        printTestMetrics("Batch Writing Test", source, jobStartTime, jobEndTime, recordCount, 1);
    }

    @Test
    public void testParallelWritingWithMetrics() throws Exception {
        System.out.println("=== Parallel Writing Test with Metrics ===");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        int parallelism = 4;
        env.setParallelism(parallelism);

        int recordsPerTask = 25000;
        MetricsCollectingTestSource source = new MetricsCollectingTestSource(recordsPerTask, "parallel-test");

        long jobStartTime = System.currentTimeMillis();

        env.addSource(source)
                .setParallelism(parallelism)
                .addSink(createOpenGeminiSink(2000))
                .setParallelism(parallelism);

        env.execute("OpenGemini Parallel Write Test");

        long jobEndTime = System.currentTimeMillis();

        printTestMetrics("Parallel Writing Test", source, jobStartTime, jobEndTime,
                recordsPerTask * parallelism, parallelism);
    }

    @Test
    public void testBatchWritingWithWarmup() throws Exception {
        System.out.println("=== Batch Writing Test with Warmup Analysis ===");

        // 第一轮：冷启动测试
        double coldPerformance = runSingleBenchmark("Cold Start", 50000);

        // 等待2秒让系统稳定
        Thread.sleep(2000);

        // 第二轮：预热后测试
        double warmPerformance = runSingleBenchmark("After Warmup", 50000);

        // 等待2秒
        Thread.sleep(2000);

        // 第三轮：热启动测试
        double hotPerformance = runSingleBenchmark("Hot Start", 50000);

        // 分析结果
        System.out.println("\n" + repeatString("=", 70));
        System.out.println("WARMUP EFFECT ANALYSIS");
        System.out.println(repeatString("=", 70));
        System.out.println("Cold Start Performance:  " + String.format("%.2f", coldPerformance) + " MB/s");
        System.out.println("Warm Start Performance:  " + String.format("%.2f", warmPerformance) + " MB/s");
        System.out.println("Hot Start Performance:   " + String.format("%.2f", hotPerformance) + " MB/s");
        System.out.println();
        System.out.println("Warmup Improvement:      " + String.format("%.1f", (warmPerformance/coldPerformance)) + "x");
        System.out.println("Total Improvement:       " + String.format("%.1f", (hotPerformance/coldPerformance)) + "x");
        System.out.println("Peak Performance:        " + String.format("%.2f", Math.max(Math.max(coldPerformance, warmPerformance), hotPerformance)) + " MB/s");

        boolean meetsTarget = hotPerformance >= 80.0;
        System.out.println("Meets 80MB/s Target:     " + (meetsTarget ? "✓ YES" : "✗ NO"));
        System.out.println(repeatString("=", 70));
    }

    private double runSingleBenchmark(String phaseName, int recordCount) throws Exception {
        System.out.println("\n--- " + phaseName + " Phase ---");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        MetricsCollectingTestSource source = new MetricsCollectingTestSource(recordCount, phaseName.toLowerCase().replace(" ", "-"));

        long jobStartTime = System.currentTimeMillis();

        env.addSource(source)
                .setParallelism(1)
                .addSink(createOpenGeminiSink(5000))
                .setParallelism(1);

        env.execute("OpenGemini " + phaseName + " Test");

        long jobEndTime = System.currentTimeMillis();

        // 计算性能指标
        double jobDurationSeconds = (jobEndTime - jobStartTime) / 1000.0;
        double avgRecordSize = 320.0; // bytes
        double totalDataMB = recordCount * avgRecordSize / (1024 * 1024);
        double throughputMBps = totalDataMB / jobDurationSeconds;

        System.out.println(phaseName + " Results:");
        System.out.println("  Duration: " + String.format("%.2f", jobDurationSeconds) + "s");
        System.out.println("  Throughput: " + String.format("%.2f", throughputMBps) + " MB/s");

        return throughputMBps;
    }

    @Test
    public void quickWarmupVerification() throws Exception {
        System.out.println("=== Quick Warmup Verification ===");

        // 连续运行3次相同测试
        double[] results = new double[3];

        for (int i = 0; i < 3; i++) {
            System.out.println("\n--- Round " + (i+1) + " ---");
            results[i] = runSingleBenchmark("Round-" + (i+1), 20000);

            if (i < 2) Thread.sleep(2000); // 间隔2秒
        }

        System.out.println("\nResults Summary:");
        System.out.println("Round 1 (Cold): " + String.format("%.1f", results[0]) + " MB/s");
        System.out.println("Round 2 (Warm): " + String.format("%.1f", results[1]) + " MB/s");
        System.out.println("Round 3 (Hot):  " + String.format("%.1f", results[2]) + " MB/s");
        System.out.println("Improvement: " + String.format("%.1f", results[2]/results[0]) + "x");
    }
    // 专门的预热测试
    @Test
    public void testWithDedicatedWarmup() throws Exception {
        System.out.println("=== Test with Dedicated Warmup ===");

        // 专门的预热阶段
        runWarmupPhase();

        // 现在运行正式测试
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        int recordCount = 50000;
        MetricsCollectingTestSource source = new MetricsCollectingTestSource(recordCount, "post-warmup-test");

        long jobStartTime = System.currentTimeMillis();

        env.addSource(source)
                .setParallelism(1)
                .addSink(createOpenGeminiSink(5000))
                .setParallelism(1);

        env.execute("OpenGemini Post-Warmup Test");

        long jobEndTime = System.currentTimeMillis();

        printTestMetrics("Post-Warmup Test", source, jobStartTime, jobEndTime, recordCount, 1);
    }

    private void runWarmupPhase() throws Exception {
        System.out.println("Starting warmup phase...");

        // 1. Flink集群预热
        StreamExecutionEnvironment warmupEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        warmupEnv.setParallelism(1);

        MetricsCollectingTestSource warmupSource = new MetricsCollectingTestSource(1000, "warmup");

        warmupEnv.addSource(warmupSource)
                .setParallelism(1)
                .addSink(createOpenGeminiSink(500)) // 小批次预热
                .setParallelism(1);

        warmupEnv.execute("Warmup Job");

        // 2. 等待系统稳定
        Thread.sleep(3000);

        // 3. 额外的连接预热
        runAdditionalWarmup();

        System.out.println("Warmup phase completed");
    }

    private void runAdditionalWarmup() throws Exception {
        // 再运行一个小测试确保完全预热
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        MetricsCollectingTestSource source = new MetricsCollectingTestSource(500, "additional-warmup");

        env.addSource(source)
                .setParallelism(1)
                .addSink(createOpenGeminiSink(250))
                .setParallelism(1);

        env.execute("Additional Warmup");

        Thread.sleep(1000);
    }

    @Test
    public void testHighThroughputPerformance() throws Exception {
        System.out.println("=== High Throughput Performance Test ===");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        int parallelism = 8;
        env.setParallelism(parallelism);

        int recordsPerTask = 125000; // Total: 1M records
        HighThroughputTestSource source = new HighThroughputTestSource(recordsPerTask, "performance-test");

        long jobStartTime = System.currentTimeMillis();

        env.addSource(source)
                .setParallelism(parallelism)
                .addSink(createOpenGeminiSink(10000)) // Large batch size for high throughput
                .setParallelism(parallelism);

        env.execute("OpenGemini High Throughput Test");

        long jobEndTime = System.currentTimeMillis();

        printDetailedPerformanceMetrics("High Throughput Performance Test", source,
                jobStartTime, jobEndTime, recordsPerTask * parallelism, parallelism);
    }



    @Test
    public void testRawClientPerformance() throws Exception {
        System.out.println("=== Raw OpenGemini Client Performance Test ===");

        // 直接使用 OpenGemini 客户端进行性能测试，不通过 Flink
        io.github.openfacade.http.HttpClientConfig httpConfig = new io.github.openfacade.http.HttpClientConfig.Builder()
                .connectTimeout(java.time.Duration.ofSeconds(5))
                .timeout(java.time.Duration.ofSeconds(30))
                .build();

        io.opengemini.client.api.Configuration configuration = io.opengemini.client.api.Configuration.builder()
                .addresses(java.util.Collections.singletonList(new io.opengemini.client.api.Address("localhost", 8086)))
                .httpConfig(httpConfig)
                .build();

        io.opengemini.client.impl.OpenGeminiClient client = io.opengemini.client.impl.OpenGeminiClientFactory.create(configuration);

        try {
            // 确保数据库存在
            client.createDatabase(DATABASE).get();

            int totalRecords = 100000;
            int batchSize = 5000;

            TestRecordPointConverter converter = new TestRecordPointConverter();

            long startTime = System.currentTimeMillis();
            int recordsSent = 0;

            while (recordsSent < totalRecords) {
                java.util.List<io.opengemini.client.api.Point> points = new java.util.ArrayList<>();

                for (int i = 0; i < batchSize && recordsSent < totalRecords; i++) {
                    TestRecord record = new TestRecord(
                            "device-" + recordsSent,
                            "sensor",
                            Math.random() * 100,
                            recordsSent,
                            recordsSent % 2 == 0,
                            System.currentTimeMillis() * 1_000_000L
                    );

                    io.opengemini.client.api.Point point = converter.convert(record, MEASUREMENT);
                    if (point != null) {
                        points.add(point);
                    }
                    recordsSent++;
                }

                // 写入批次
                client.write(DATABASE, points).get();

                if (recordsSent % 10000 == 0) {
                    System.out.println("Progress: " + recordsSent + "/" + totalRecords);
                }
            }

            long endTime = System.currentTimeMillis();
            double durationSeconds = (endTime - startTime) / 1000.0;

            System.out.println("\n" + repeatString("=", 60));
            System.out.println("Raw Client Performance Results:");
            System.out.println(repeatString("=", 60));
            System.out.println("Total Records: " + totalRecords);
            System.out.println("Batch Size: " + batchSize);
            System.out.println("Duration: " + String.format("%.2f", durationSeconds) + " seconds");
            System.out.println("Throughput: " + String.format("%.0f", totalRecords / durationSeconds) + " records/second");

            double totalDataMB = totalRecords * 320.0 / (1024 * 1024);
            double mbPerSecond = totalDataMB / durationSeconds;
            System.out.println("Data Volume: " + String.format("%.2f", totalDataMB) + " MB");
            System.out.println("Data Throughput: " + String.format("%.2f", mbPerSecond) + " MB/second");
            System.out.println(repeatString("=", 60) + "\n");

        } finally {
            client.close();
        }
    }

    private OpenGeminiSink<TestRecord> createOpenGeminiSink() {
        return createOpenGeminiSink(1000); // Default batch size
    }

    private OpenGeminiSink<TestRecord> createOpenGeminiSink(int batchSize) {
        // 使用具体的实现类而不是 Lambda 表达式，以避免序列化问题
        TestRecordPointConverter converter = new TestRecordPointConverter();

        return OpenGeminiSink.<TestRecord>builder()
                .setUrl(OPENGEMINI_URL)
                .setDatabase(DATABASE)
                .setMeasurement(MEASUREMENT)
                .setUsername(USERNAME)
                .setPassword(PASSWORD)
                .setBatchSize(batchSize)
                .setFlushInterval(1000, TimeUnit.MILLISECONDS)
                .setMaxRetries(3)
                .setPointConverter(converter)
                .build();
    }

    /**
     * 可序列化的 Point Converter 实现
     */
    public static class TestRecordPointConverter implements OpenGeminiPointConverter<TestRecord>, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public io.opengemini.client.api.Point convert(TestRecord record, String measurement) {
            io.opengemini.client.api.Point point = new io.opengemini.client.api.Point();
            point.setMeasurement(measurement);

            // 设置时间戳
            point.setTime(record.getTimestamp());

            // 设置标签
            java.util.Map<String, String> tags = new java.util.HashMap<>();
            tags.put("id", record.getId());
            tags.put("type", record.getType());
            point.setTags(tags);

            // 设置字段
            java.util.Map<String, Object> fields = new java.util.HashMap<>();
            fields.put("value1", record.getValue1());
            fields.put("value2", record.getValue2());
            fields.put("flag", record.isFlag());
            point.setFields(fields);

            return point;
        }
    }

    private void printTestMetrics(String testName, MetricsCollectingTestSource source,
                                  long jobStartTime, long jobEndTime, int totalRecords, int parallelism) {
        double jobDurationSeconds = (jobEndTime - jobStartTime) / 1000.0;

        System.out.println("\n" + repeatString("=", 60));
        System.out.println(testName + " Results:");
        System.out.println(repeatString("=", 60));

        // Job-level metrics
        System.out.println("Job Metrics:");
        System.out.println("  Total Job Duration: " + String.format("%.2f", jobDurationSeconds) + " seconds");
        System.out.println("  Expected Total Records: " + totalRecords);
        System.out.println("  Parallelism: " + parallelism);

        // Source metrics
        SourceMetrics metrics = source.getMetrics();
        System.out.println("\nData Generation Metrics:");
        System.out.println("  Records Generated: " + metrics.getRecordsGenerated());
        double genDuration = metrics.getGenerationDurationSeconds();
        System.out.println("  Generation Duration: " + String.format("%.2f", genDuration) + " seconds");
        if (genDuration > 0 && metrics.getRecordsGenerated() > 0) {
            System.out.println("  Generation Rate: " + String.format("%.2f", metrics.getGenerationRatePerSecond()) + " records/second");
        } else {
            System.out.println("  Generation Rate: N/A (generation too fast to measure)");
        }

        // Performance analysis
        System.out.println("\nPerformance Analysis:");
        double overallThroughput = totalRecords / jobDurationSeconds;
        System.out.println("  Overall Job Throughput: " + String.format("%.2f", overallThroughput) + " records/second");

        // Estimate data size based on actual Point size
        double avgRecordSize = 320.0; // Based on benchmark results
        double totalDataMB = totalRecords * avgRecordSize / (1024 * 1024);
        double dataThroughputMBps = totalDataMB / jobDurationSeconds;
        System.out.println("  Estimated Data Volume: " + String.format("%.2f", totalDataMB) + " MB");
        System.out.println("  Estimated Data Throughput: " + String.format("%.2f", dataThroughputMBps) + " MB/second");
        System.out.println("  Per-Core Throughput: " + String.format("%.2f", dataThroughputMBps / parallelism) + " MB/second/core");

        // Check performance requirements
        double perCoreThreshold = 80.0; // 80 MB/s per core requirement
        boolean meetsRequirement = (dataThroughputMBps / parallelism) >= perCoreThreshold;
        System.out.println("  Meets 80MB/s/core Requirement: " + (meetsRequirement ? "✓ YES" : "✗ NO"));

        System.out.println(repeatString("=", 60) + "\n");
    }

    private void printDetailedPerformanceMetrics(String testName, HighThroughputTestSource source,
                                                 long jobStartTime, long jobEndTime, int totalRecords, int parallelism) {
        double jobDurationSeconds = (jobEndTime - jobStartTime) / 1000.0;

        System.out.println("\n" + repeatString("=", 80));
        System.out.println(testName + " - Detailed Performance Results:");
        System.out.println(repeatString("=", 80));

        HighThroughputMetrics metrics = source.getMetrics();

        // Generation phase metrics
        System.out.println("Data Generation Phase:");
        System.out.println("  Records Generated: " + metrics.getTotalRecordsGenerated());
        System.out.println("  Generation Start Time: " + metrics.getGenerationStartTime());
        System.out.println("  Generation End Time: " + metrics.getGenerationEndTime());
        System.out.println("  Total Generation Time: " + String.format("%.2f", metrics.getGenerationDurationSeconds()) + " seconds");
        System.out.println("  Peak Generation Rate: " + String.format("%.2f", metrics.getPeakGenerationRate()) + " records/second");
        System.out.println("  Average Generation Rate: " + String.format("%.2f", metrics.getAverageGenerationRate()) + " records/second");

        // Throughput analysis
        System.out.println("\nThroughput Analysis:");
        double overallThroughput = totalRecords / jobDurationSeconds;
        System.out.println("  Overall Job Throughput: " + String.format("%.2f", overallThroughput) + " records/second");

        // Data volume estimates
        double avgRecordSize = 320.0; // Based on benchmark
        double totalDataMB = totalRecords * avgRecordSize / (1024 * 1024);
        double dataThroughputMBps = totalDataMB / jobDurationSeconds;

        System.out.println("\nData Volume & Throughput:");
        System.out.println("  Estimated Record Size: " + String.format("%.0f", avgRecordSize) + " bytes");
        System.out.println("  Total Data Volume: " + String.format("%.2f", totalDataMB) + " MB");
        System.out.println("  Data Throughput: " + String.format("%.2f", dataThroughputMBps) + " MB/second");
        System.out.println("  Per-Core Data Throughput: " + String.format("%.2f", dataThroughputMBps / parallelism) + " MB/second/core");

        // Performance benchmarking
        System.out.println("\nPerformance Benchmarking:");
        System.out.println("  Target: 80 MB/second/core");
        System.out.println("  Achieved: " + String.format("%.2f", dataThroughputMBps / parallelism) + " MB/second/core");
        double performanceRatio = (dataThroughputMBps / parallelism) / 80.0;
        System.out.println("  Performance Ratio: " + String.format("%.2f", performanceRatio) + "x target");

        if (performanceRatio >= 1.0) {
            System.out.println("  Result: ✓ PASSED - Exceeds performance requirement");
        } else {
            System.out.println("  Result: ✗ FAILED - Below performance requirement");
        }

        System.out.println(repeatString("=", 80) + "\n");
    }

    private void printSustainedLoadMetrics(String testName, SustainedLoadTestSource source,
                                           long jobStartTime, long jobEndTime, int parallelism) {
        double jobDurationSeconds = (jobEndTime - jobStartTime) / 1000.0;

        System.out.println("\n" + repeatString("=", 70));
        System.out.println(testName + " Results:");
        System.out.println(repeatString("=", 70));

        SustainedLoadMetrics metrics = source.getMetrics();

        System.out.println("Sustained Load Metrics:");
        System.out.println("  Test Duration: " + String.format("%.2f", jobDurationSeconds) + " seconds");
        System.out.println("  Target Rate: " + metrics.getTargetRatePerSecond() + " records/second per source");
        System.out.println("  Total Records Generated: " + metrics.getTotalRecordsGenerated());
        System.out.println("  Actual Rate: " + String.format("%.2f", metrics.getActualRate()) + " records/second");
        System.out.println("  Rate Accuracy: " + String.format("%.1f", metrics.getRateAccuracy()) + "%");

        System.out.println(repeatString("=", 70) + "\n");
    }

    // Helper method for database setup
    private void createDatabaseIfNeeded() {
        try {
            boolean databaseExists = false;
            HttpURLConnection showConnection = null;
            try {
                URL showUrl = new URL(OPENGEMINI_URL + "/query");
                showConnection = (HttpURLConnection) showUrl.openConnection();
                showConnection.setRequestMethod("GET");
                showConnection.setDoOutput(true);

                if (USERNAME != null && !USERNAME.isEmpty() && PASSWORD != null) {
                    String auth = USERNAME + ":" + PASSWORD;
                    String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes(StandardCharsets.UTF_8));
                    showConnection.setRequestProperty("Authorization", "Basic " + encodedAuth);
                }

                String query = "q=SHOW DATABASES";
                try (OutputStream os = showConnection.getOutputStream()) {
                    os.write(query.getBytes(StandardCharsets.UTF_8));
                }

                int responseCode = showConnection.getResponseCode();
                if (responseCode == HttpURLConnection.HTTP_OK) {
                    try (BufferedReader reader = new BufferedReader(
                            new InputStreamReader(showConnection.getInputStream(), StandardCharsets.UTF_8))) {
                        String line;
                        StringBuilder response = new StringBuilder();
                        while ((line = reader.readLine()) != null) {
                            response.append(line);
                        }
                        databaseExists = response.toString().contains("\"name\":\"" + DATABASE + "\"");
                    }
                }
            } finally {
                if (showConnection != null) {
                    showConnection.disconnect();
                }
            }

            if (!databaseExists) {
                System.out.println("Database '" + DATABASE + "' does not exist. Creating it now...");
                HttpURLConnection createConnection = null;
                try {
                    URL createUrl = new URL(OPENGEMINI_URL + "/query");
                    createConnection = (HttpURLConnection) createUrl.openConnection();
                    createConnection.setRequestMethod("POST");
                    createConnection.setDoOutput(true);

                    if (USERNAME != null && !USERNAME.isEmpty() && PASSWORD != null) {
                        String auth = USERNAME + ":" + PASSWORD;
                        String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes(StandardCharsets.UTF_8));
                        createConnection.setRequestProperty("Authorization", "Basic " + encodedAuth);
                    }

                    String createQuery = "q=CREATE DATABASE " + DATABASE;
                    try (OutputStream os = createConnection.getOutputStream()) {
                        os.write(createQuery.getBytes(StandardCharsets.UTF_8));
                    }

                    int responseCode = createConnection.getResponseCode();
                    if (responseCode == HttpURLConnection.HTTP_OK) {
                        System.out.println("Successfully created database '" + DATABASE + "'");
                    } else {
                        System.err.println("Failed to create database '" + DATABASE + "'. Response code: " + responseCode);
                        throw new RuntimeException("Failed to create database for testing");
                    }
                } finally {
                    if (createConnection != null) {
                        createConnection.disconnect();
                    }
                }
            } else {
                System.out.println("Database '" + DATABASE + "' already exists");
            }
        } catch (Exception e) {
            System.err.println("Error checking/creating database: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Failed to setup test database", e);
        }
    }

    // Test Record class - 必须实现 Serializable
    public static class TestRecord implements Serializable {
        private static final long serialVersionUID = 1L;

        private final String id;
        private final String type;
        private final double value1;
        private final long value2;
        private final boolean flag;
        private final long timestamp;

        public TestRecord(String id, String type, double value1, long value2, boolean flag, long timestamp) {
            this.id = id;
            this.type = type;
            this.value1 = value1;
            this.value2 = value2;
            this.flag = flag;
            this.timestamp = timestamp;
        }

        public String getId() { return id; }
        public String getType() { return type; }
        public double getValue1() { return value1; }
        public long getValue2() { return value2; }
        public boolean isFlag() { return flag; }
        public long getTimestamp() { return timestamp; }
    }

    // Source classes with fixed metrics
    public static class MetricsCollectingTestSource implements ParallelSourceFunction<TestRecord> {
        private static final long serialVersionUID = 1L;

        private final int numRecords;
        private final String testName;
        private volatile boolean running = true;
        private final SourceMetrics metrics = new SourceMetrics(); // 直接使用对象，不用 AtomicReference

        public MetricsCollectingTestSource(int numRecords, String testName) {
            this.numRecords = numRecords;
            this.testName = testName;
        }

        @Override
        public void run(SourceContext<TestRecord> ctx) throws Exception {
            long startTime = System.currentTimeMillis();
            int count = 0;
            String[] types = {"sensor", "device", "gateway", "controller", "monitor"};

            while (running && count < numRecords) {
                String id = "device-" + (count % 1000);
                String type = types[count % types.length];
                double value1 = Math.random() * 100;
                long value2 = (long) (Math.random() * 10000);
                boolean flag = count % 2 == 0;
                long timestamp = System.currentTimeMillis() * 1_000_000;

                ctx.collect(new TestRecord(id, type, value1, value2, flag, timestamp));
                count++;

                if (count % 10000 == 0) {
                    System.out.println(testName + " - Generated " + count + "/" + numRecords + " records");
                }
            }

            long endTime = System.currentTimeMillis();

            // 直接更新 metrics 对象的字段
            synchronized (metrics) {
                metrics.recordsGenerated = count;
                metrics.startTime = startTime;
                metrics.endTime = endTime;
            }

            System.out.println(testName + " - Finished generating " + count + " records in " +
                    (endTime - startTime) + "ms");
        }

        @Override
        public void cancel() {
            running = false;
        }

        public SourceMetrics getMetrics() {
            synchronized (metrics) {
                return metrics;
            }
        }
    }

    public static class HighThroughputTestSource implements ParallelSourceFunction<TestRecord> {
        private static final long serialVersionUID = 1L;

        private final int numRecords;
        private final String testName;
        private volatile boolean running = true;
        private final HighThroughputMetrics metrics = new HighThroughputMetrics();

        public HighThroughputTestSource(int numRecords, String testName) {
            this.numRecords = numRecords;
            this.testName = testName;
        }

        @Override
        public void run(SourceContext<TestRecord> ctx) throws Exception {
            long startTime = System.currentTimeMillis();
            long lastMeasurement = startTime;
            int count = 0;
            int lastCount = 0;
            double peakRate = 0.0;

            String[] types = {"temperature", "humidity", "pressure", "voltage", "current"};
            String[] locations = {"factory-a", "factory-b", "warehouse-c", "office-d", "lab-e"};

            while (running && count < numRecords) {
                String id = locations[count % locations.length] + "-sensor-" + (count % 100);
                String type = types[count % types.length];
                double value1 = 20 + Math.random() * 60;
                long value2 = System.currentTimeMillis() + (long)(Math.random() * 1000);
                boolean flag = (count % 7) == 0;
                long timestamp = (System.currentTimeMillis() + count) * 1_000_000;

                ctx.collect(new TestRecord(id, type, value1, value2, flag, timestamp));
                count++;

                if (count % 1000 == 0) {
                    long currentTime = System.currentTimeMillis();
                    double timeDiff = (currentTime - lastMeasurement) / 1000.0;
                    if (timeDiff > 0) {
                        double currentRate = (count - lastCount) / timeDiff;
                        peakRate = Math.max(peakRate, currentRate);
                    }

                    lastMeasurement = currentTime;
                    lastCount = count;

                    if (count % 25000 == 0) {
                        System.out.println(testName + " - Generated " + count + "/" + numRecords +
                                " records (current rate: " + String.format("%.0f", peakRate) + " rec/s)");
                    }
                }
            }

            long endTime = System.currentTimeMillis();

            synchronized (metrics) {
                metrics.totalRecordsGenerated = count;
                metrics.generationStartTime = startTime;
                metrics.generationEndTime = endTime;
                metrics.peakGenerationRate = peakRate;
            }

            System.out.println(testName + " - Completed generating " + count + " records");
        }

        @Override
        public void cancel() {
            running = false;
        }

        public HighThroughputMetrics getMetrics() {
            synchronized (metrics) {
                return metrics;
            }
        }
    }

    public static class SustainedLoadTestSource implements ParallelSourceFunction<TestRecord> {
        private static final long serialVersionUID = 1L;

        private final long durationMs;
        private final int targetRatePerSecond;
        private final String testName;
        private volatile boolean running = true;
        private final SustainedLoadMetrics metrics = new SustainedLoadMetrics();

        public SustainedLoadTestSource(long durationMs, int targetRatePerSecond, String testName) {
            this.durationMs = durationMs;
            this.targetRatePerSecond = targetRatePerSecond;
            this.testName = testName;
        }

        @Override
        public void run(SourceContext<TestRecord> ctx) throws Exception {
            long startTime = System.currentTimeMillis();
            long endTime = startTime + durationMs;
            int count = 0;

            long intervalMs = 1000 / targetRatePerSecond;
            long nextEmitTime = startTime;

            String[] types = {"metric1", "metric2", "metric3"};

            while (running && System.currentTimeMillis() < endTime) {
                long currentTime = System.currentTimeMillis();

                if (currentTime >= nextEmitTime) {
                    String id = "sustained-" + (count % 50);
                    String type = types[count % types.length];
                    double value1 = Math.sin(count * 0.1) * 50 + 50;
                    long value2 = count;
                    boolean flag = (count % 10) == 0;
                    long timestamp = currentTime * 1_000_000;

                    ctx.collect(new TestRecord(id, type, value1, value2, flag, timestamp));
                    count++;
                    nextEmitTime += intervalMs;
                }

                Thread.sleep(1);
            }

            long actualEndTime = System.currentTimeMillis();
            double actualDurationSeconds = (actualEndTime - startTime) / 1000.0;
            double actualRate = actualDurationSeconds > 0 ? count / actualDurationSeconds : 0;

            synchronized (metrics) {
                metrics.totalRecordsGenerated = count;
                metrics.targetRatePerSecond = targetRatePerSecond;
                metrics.actualRate = actualRate;
            }

            System.out.println(testName + " - Generated " + count + " records at " +
                    String.format("%.2f", actualRate) + " records/second");
        }

        @Override
        public void cancel() {
            running = false;
        }

        public SustainedLoadMetrics getMetrics() {
            synchronized (metrics) {
                return metrics;
            }
        }
    }

    // Metrics classes - 使用 volatile 确保可见性
    public static class SourceMetrics implements Serializable {
        private static final long serialVersionUID = 1L;
        volatile long recordsGenerated;
        volatile long startTime;
        volatile long endTime;

        public long getRecordsGenerated() { return recordsGenerated; }
        public double getGenerationDurationSeconds() {
            long duration = endTime - startTime;
            return duration > 0 ? duration / 1000.0 : 0.0;
        }
        public double getGenerationRatePerSecond() {
            double duration = getGenerationDurationSeconds();
            return duration > 0 ? recordsGenerated / duration : 0.0;
        }
    }

    public static class HighThroughputMetrics implements Serializable {
        private static final long serialVersionUID = 1L;
        volatile long totalRecordsGenerated;
        volatile long generationStartTime;
        volatile long generationEndTime;
        volatile double peakGenerationRate;

        public long getTotalRecordsGenerated() { return totalRecordsGenerated; }
        public long getGenerationStartTime() { return generationStartTime; }
        public long getGenerationEndTime() { return generationEndTime; }
        public double getGenerationDurationSeconds() {
            long duration = generationEndTime - generationStartTime;
            return duration > 0 ? duration / 1000.0 : 0.0;
        }
        public double getPeakGenerationRate() { return peakGenerationRate; }
        public double getAverageGenerationRate() {
            double duration = getGenerationDurationSeconds();
            return duration > 0 ? totalRecordsGenerated / duration : 0.0;
        }
    }

    public static class SustainedLoadMetrics implements Serializable {
        private static final long serialVersionUID = 1L;
        volatile long totalRecordsGenerated;
        volatile int targetRatePerSecond;
        volatile double actualRate;

        public long getTotalRecordsGenerated() { return totalRecordsGenerated; }
        public int getTargetRatePerSecond() { return targetRatePerSecond; }
        public double getActualRate() { return actualRate; }
        public double getRateAccuracy() {
            return targetRatePerSecond > 0 ? (actualRate / targetRatePerSecond) * 100.0 : 0.0;
        }
    }

    public static String repeatString(String str, int count) {
        if (count <= 0) return "";
        if (count == 1) return str;

        StringBuilder sb = new StringBuilder(str.length() * count);
        for (int i = 0; i < count; i++) {
            sb.append(str);
        }
        return sb.toString();
    }
}