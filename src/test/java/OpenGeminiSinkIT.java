import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.opengemini.OpenGeminiSink;
import org.apache.flink.connector.opengemini.SimpleOpenGeminiConverter;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Integration test for the OpenGeminiSink.
 * Note: This test requires a running OpenGemini instance.
 */
public class OpenGeminiSinkIT {

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
    public void testOpenGeminiSink() throws Exception {
        // Set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(1000);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // number of restart attempts
                Time.of(5, TimeUnit.SECONDS) // delay
        ));

        // Generate test data (10 records)
        env.addSource(new TestSource(10))
                .setParallelism(1)
                .addSink(createOpenGeminiSink())
                .setParallelism(1);

        env.execute("OpenGemini Sink Test");

        // At this point, you should verify the data was written to OpenGemini
        // You can either:
        // 1. Use OpenGemini client to query the data and verify
        // 2. Check logs for successful write operations
        System.out.println("Test job completed. Verify data in OpenGemini.");
    }

    @Test
    public void testBatchWriting() throws Exception {
        // Set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Generate a larger batch of test data (10,000 records)
        env.addSource(new TestSource(10000))
                .setParallelism(1)
                .addSink(createOpenGeminiSink())
                .setParallelism(1);

        env.execute("OpenGemini Batch Write Test");

        System.out.println("Batch write test completed. Verify data in OpenGemini.");
    }

    @Test
    public void testParallelWriting() throws Exception {
        // Set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2); // Use 2 parallel tasks

        // Generate test data with multiple parallel instances
        env.addSource(new TestSource(5000))
                .setParallelism(2)
                .addSink(createOpenGeminiSink())
                .setParallelism(2);

        env.execute("OpenGemini Parallel Write Test");

        System.out.println("Parallel write test completed. Verify data in OpenGemini.");
    }

    @Test
    public void testFailureRecovery() throws Exception {
        // Set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(1000);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // number of restart attempts
                Time.of(2, TimeUnit.SECONDS) // delay
        ));

        // Generate test data (1000 records) with an injected failure
        env.addSource(new FailingTestSource(1000, 500)) // Fail after 500 records
                .setParallelism(1)
                .addSink(createOpenGeminiSink())
                .setParallelism(1);

        try {
            env.execute("OpenGemini Failure Recovery Test");
        } catch (JobExecutionException e) {
            // Expected exception due to failure
            System.out.println("Job failed as expected. Restart strategy should recover.");
        }

        System.out.println("Failure recovery test completed. Verify data in OpenGemini.");
    }

    @Test
    public void testPerformance() throws Exception {
        // Set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Use multiple parallel tasks for higher throughput
        int parallelism = 4;
        env.setParallelism(parallelism);

        // Number of records per task (adjust based on your system capabilities)
        int recordsPerTask = 250000;

        // Generate a large volume of test data
        env.addSource(new TestSource(recordsPerTask))
                .setParallelism(parallelism)
                .addSink(createOpenGeminiSink(5000)) // Larger batch size for performance
                .setParallelism(parallelism);

        long startTime = System.currentTimeMillis();
        env.execute("OpenGemini Performance Test");
        long endTime = System.currentTimeMillis();

        // Calculate throughput
        long totalRecords = (long) recordsPerTask * parallelism;
        double durationSeconds = (endTime - startTime) / 1000.0;
        double recordsPerSecond = totalRecords / durationSeconds;

        // Assuming average record size of 100 bytes
        double bytesPerSecond = recordsPerSecond * 100;
        double mbPerSecond = bytesPerSecond / (1024 * 1024);

        System.out.println("Performance test completed:");
        System.out.println("Total records: " + totalRecords);
        System.out.println("Duration: " + durationSeconds + " seconds");
        System.out.println("Throughput: " + recordsPerSecond + " records/second");
        System.out.println("Throughput: " + String.format("%.2f", mbPerSecond) + " MB/second");

        // Check if performance meets the 80 MB/s requirement (per core)
        System.out.println("Estimated throughput per core: " +
                String.format("%.2f", mbPerSecond / parallelism) + " MB/second/core");
    }

    private OpenGeminiSink<TestRecord> createOpenGeminiSink() {
        return createOpenGeminiSink(1000); // Default batch size
    }

    private OpenGeminiSink<TestRecord> createOpenGeminiSink(int batchSize) {
        // Create converter for the test records
        SimpleOpenGeminiConverter<TestRecord> converter = SimpleOpenGeminiConverter.<TestRecord>builder()
                .addTag("id", TestRecord::getId)
                .addTag("type", TestRecord::getType)
                .addField("value1", TestRecord::getValue1)
                .addField("value2", TestRecord::getValue2)
                .addField("flag", TestRecord::isFlag)
                .withTimestamp(TestRecord::getTimestamp)
                .build();

        // Create the sink
        return OpenGeminiSink.<TestRecord>builder()
                .setUrl(OPENGEMINI_URL)
                .setDatabase(DATABASE)
                .setMeasurement(MEASUREMENT)
                .setUsername(USERNAME)
                .setPassword(PASSWORD)
                .setBatchSize(batchSize)
                .setFlushInterval(1000, TimeUnit.MILLISECONDS)
                .setMaxRetries(3)
                .setConverter(converter)
                .build();
    }

    private void createDatabaseIfNeeded() {
        // In a real implementation, you would use the OpenGemini client to create the database
        // For simplicity, we assume the database exists or you've created it manually
        System.out.println("Make sure database '" + DATABASE + "' exists in your OpenGemini instance");
    }

    /**
     * A simple record class for testing.
     */
    public static class TestRecord {
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

        public String getId() {
            return id;
        }

        public String getType() {
            return type;
        }

        public double getValue1() {
            return value1;
        }

        public long getValue2() {
            return value2;
        }

        public boolean isFlag() {
            return flag;
        }

        public long getTimestamp() {
            return timestamp;
        }
    }

    /**
     * Source function that generates test records.
     */
    public static class TestSource implements SourceFunction<TestRecord> {
        private static final long serialVersionUID = 1L;

        private final int numRecords;
        private volatile boolean running = true;

        public TestSource(int numRecords) {
            this.numRecords = numRecords;
        }

        @Override
        public void run(SourceContext<TestRecord> ctx) throws Exception {
            int count = 0;
            String[] types = {"type1", "type2", "type3", "type4", "type5"};

            while (running && count < numRecords) {
                String id = UUID.randomUUID().toString();
                String type = types[count % types.length];
                double value1 = Math.random() * 100;
                long value2 = (long) (Math.random() * 10000);
                boolean flag = count % 2 == 0;
                long timestamp = System.currentTimeMillis() * 1_000_000; // Convert to nanoseconds

                ctx.collect(new TestRecord(id, type, value1, value2, flag, timestamp));

                count++;

                // Add a short delay to avoid generating records too quickly for small tests
                if (numRecords < 100) {
                    Thread.sleep(10);
                } else if (count % 10000 == 0) {
                    // For larger tests, periodically output progress
                    System.out.println("Generated " + count + "/" + numRecords + " records");
                }
            }

            System.out.println("TestSource finished generating " + count + " records");
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    /**
     * Source function that generates test records and fails after a certain number.
     */
    public static class FailingTestSource implements SourceFunction<TestRecord> {
        private static final long serialVersionUID = 1L;

        private final int numRecords;
        private final int failAfter;
        private volatile boolean running = true;

        public FailingTestSource(int numRecords, int failAfter) {
            this.numRecords = numRecords;
            this.failAfter = failAfter;
        }

        @Override
        public void run(SourceContext<TestRecord> ctx) throws Exception {
            int count = 0;
            String[] types = {"type1", "type2", "type3", "type4", "type5"};

            while (running && count < numRecords) {
                // Inject failure at specified point
                if (count == failAfter) {
                    System.out.println("FailingTestSource is injecting a failure after " + count + " records");
                    throw new RuntimeException("Injected failure for testing recovery");
                }

                String id = UUID.randomUUID().toString();
                String type = types[count % types.length];
                double value1 = Math.random() * 100;
                long value2 = (long) (Math.random() * 10000);
                boolean flag = count % 2 == 0;
                long timestamp = System.currentTimeMillis() * 1_000_000; // Convert to nanoseconds

                ctx.collect(new TestRecord(id, type, value1, value2, flag, timestamp));

                count++;

                // Add a short delay
                if (count % 100 == 0) {
                    Thread.sleep(1);
                }
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}