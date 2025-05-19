package org.apache.flink.connector.opengemini;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.opengemini.OpenGeminiSink;
import org.apache.flink.connector.opengemini.SimpleOpenGeminiConverter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.Serializable;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Example application that demonstrates how to use the OpenGeminiSink.
 * This generates sensor data and writes it to OpenGemini.
 */
public class OpenGeminiSinkExample {

    public static void main(String[] args) throws Exception {
        // Parse command line parameters
        ParameterTool params = ParameterTool.fromArgs(args);
        String openGeminiUrl = params.get("url", "http://localhost:8086");
        String database = params.get("db", "sensordb");
        String measurement = params.get("measurement", "sensor_data");
        int batchSize = params.getInt("batchSize", 1000);
        long flushInterval = params.getLong("flushInterval", 1000);
        int numSensors = params.getInt("sensors", 10);
        int recordsPerSecond = params.getInt("rate", 10000);
        String username = params.get("username", "");
        String password = params.get("password", "");

        // Set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Enable checkpointing for exactly-once processing
        env.enableCheckpointing(60000); // Checkpoint every 60 seconds

        // Generate sample sensor data
        DataStream<SensorReading> sensorData = env.addSource(new SensorSource(numSensors, recordsPerSecond))
                .name("sensor-source");

        // Create a converter for SensorReading objects
        SimpleOpenGeminiConverter<SensorReading> converter = SimpleOpenGeminiConverter.<SensorReading>builder()
                .addTag("sensorId", SensorReading::getSensorId)
                .addTag("location", SensorReading::getLocation)
                .addField("temperature", SensorReading::getTemperature)
                .addField("humidity", SensorReading::getHumidity)
                .addField("pressure", SensorReading::getPressure)
                .withTimestamp(SensorReading::getTimestamp)
                .build();

        // Create the OpenGemini sink
        OpenGeminiSink<SensorReading> openGeminiSink = OpenGeminiSink.<SensorReading>builder()
                .setUrl(openGeminiUrl)
                .setDatabase(database)
                .setMeasurement(measurement)
                .setUsername(username)
                .setPassword(password)
                .setBatchSize(batchSize)
                .setFlushInterval(flushInterval, TimeUnit.MILLISECONDS)
                .setMaxRetries(3)
                .setConverter(converter)
                .build();

        // Add the sink to the pipeline
        sensorData.addSink(openGeminiSink)
                .name("opengemini-sink");

        // Execute the job
        System.out.println("Starting Flink job to write sensor data to OpenGemini...");
        System.out.println("URL: " + openGeminiUrl);
        System.out.println("Database: " + database);
        System.out.println("Measurement: " + measurement);
        System.out.println("Batch size: " + batchSize);
        System.out.println("Flush interval: " + flushInterval + "ms");
        System.out.println("Number of sensors: " + numSensors);
        System.out.println("Records per second: " + recordsPerSecond);

        env.execute("OpenGemini Sink Example");
    }

    /**
     * A simple sensor reading class.
     */
    public static class SensorReading implements Serializable {
        private final String sensorId;
        private final String location;
        private final double temperature;
        private final double humidity;
        private final double pressure;
        private final long timestamp;

        public SensorReading(String sensorId, String location, double temperature,
                             double humidity, double pressure, long timestamp) {
            this.sensorId = sensorId;
            this.location = location;
            this.temperature = temperature;
            this.humidity = humidity;
            this.pressure = pressure;
            this.timestamp = timestamp;
        }

        public String getSensorId() {
            return sensorId;
        }

        public String getLocation() {
            return location;
        }

        public double getTemperature() {
            return temperature;
        }

        public double getHumidity() {
            return humidity;
        }

        public double getPressure() {
            return pressure;
        }

        public long getTimestamp() {
            return timestamp;
        }

        @Override
        public String toString() {
            return "SensorReading{" +
                    "sensorId='" + sensorId + '\'' +
                    ", location='" + location + '\'' +
                    ", temperature=" + temperature +
                    ", humidity=" + humidity +
                    ", pressure=" + pressure +
                    ", timestamp=" + timestamp +
                    '}';
        }
    }

    /**
     * Source function that generates random sensor readings.
     */
    public static class SensorSource implements SourceFunction<SensorReading> {
        private static final long serialVersionUID = 1L;

        private final int numSensors;
        private final int recordsPerSecond;
        private volatile boolean running = true;
        private final Random random = new Random();
        private final String[] locations = {"datacenter1", "datacenter2", "office", "factory", "warehouse"};

        public SensorSource(int numSensors, int recordsPerSecond) {
            this.numSensors = numSensors;
            this.recordsPerSecond = recordsPerSecond;
        }

        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {
            int sleepBatchSize = Math.max(1, recordsPerSecond / 100);
            long sleepTime = (1000L * sleepBatchSize) / recordsPerSecond;

            while (running) {
                long emitStartTime = System.currentTimeMillis();

                // Generate a batch of records
                for (int i = 0; i < sleepBatchSize; i++) {
                    if (!running) {
                        break;
                    }

                    // Generate a record for each sensor
                    for (int s = 0; s < numSensors; s++) {
                        String sensorId = "sensor-" + s;
                        String location = locations[s % locations.length];
                        double temperature = 20 + 15 * random.nextGaussian();
                        double humidity = 50 + 15 * random.nextGaussian();
                        double pressure = 1000 + 20 * random.nextGaussian();
                        long timestamp = System.currentTimeMillis() * 1_000_000; // Convert to nanoseconds

                        ctx.collect(new SensorReading(
                                sensorId, location, temperature, humidity, pressure, timestamp));
                    }
                }

                // Sleep to control throughput
                long emitTime = System.currentTimeMillis() - emitStartTime;
                long sleepRequired = Math.max(0, sleepTime - emitTime);
                if (sleepRequired > 0) {
                    Thread.sleep(sleepRequired);
                }
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}