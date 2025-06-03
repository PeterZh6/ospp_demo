package org.apache.flink.connector.opengemini;

import io.opengemini.client.api.Point;
import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;



/**
 * A builder-style converter for creating OpenGemini Points.
 *
 * @param <T> The type of object to convert
 */
public class SimpleOpenGeminiPointConverter<T> implements OpenGeminiPointConverter<T> {
    private static final long serialVersionUID = 1L;

    private final Map<String, Function<T, String>> tagExtractors;
    private final Map<String, Function<T, Object>> fieldExtractors;
    private final Function<T, Long> timestampExtractor;

    private SimpleOpenGeminiPointConverter(
            Map<String, Function<T, String>> tagExtractors,
            Map<String, Function<T, Object>> fieldExtractors,
            Function<T, Long> timestampExtractor) {
        this.tagExtractors = tagExtractors;
        this.fieldExtractors = fieldExtractors;
        this.timestampExtractor = timestampExtractor;
    }

    @Override
    public Point convert(T value, String measurement) {
        if (value == null) {
            return null;
        }

        Point point = new Point();
        point.setMeasurement(measurement);

        // Set tags
        Map<String, String> tags = new HashMap<>();
        for (Map.Entry<String, Function<T, String>> entry : tagExtractors.entrySet()) {
            String tagValue = entry.getValue().apply(value);
            if (tagValue != null) {
                tags.put(entry.getKey(), tagValue);
            }
        }
        if (!tags.isEmpty()) {
            point.setTags(tags);
        }

        // Set fields
        Map<String, Object> fields = new HashMap<>();
        for (Map.Entry<String, Function<T, Object>> entry : fieldExtractors.entrySet()) {
            Object fieldValue = entry.getValue().apply(value);
            if (fieldValue != null) {
                fields.put(entry.getKey(), fieldValue);
            }
        }
        if (fields.isEmpty()) {
            // At least one field is required
            return null;
        }
        point.setFields(fields);

        // Set timestamp
        if (timestampExtractor != null) {
            Long timestamp = timestampExtractor.apply(value);
            if (timestamp != null) {
                point.setTime(timestamp);
            } else {
                point.setTime(System.currentTimeMillis() * 1_000_000L); // Current time in nanoseconds
            }
        } else {
            point.setTime(System.currentTimeMillis() * 1_000_000L);
        }

        return point;
    }

    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    public static class Builder<T> {
        private final Map<String, Function<T, String>> tagExtractors = new HashMap<>();
        private final Map<String, Function<T, Object>> fieldExtractors = new HashMap<>();
        private Function<T, Long> timestampExtractor;

        public Builder<T> addTag(String tagName, Function<T, String> extractor) {
            tagExtractors.put(tagName, extractor);
            return this;
        }

        public Builder<T> addField(String fieldName, Function<T, Object> extractor) {
            fieldExtractors.put(fieldName, extractor);
            return this;
        }

        public Builder<T> withTimestamp(Function<T, Long> extractor) {
            this.timestampExtractor = extractor;
            return this;
        }

        public Builder<T> withTimestampMillis(Function<T, Long> extractor) {
            // Convert milliseconds to nanoseconds
            this.timestampExtractor = value -> {
                Long millis = extractor.apply(value);
                return millis != null ? millis * 1_000_000L : null;
            };
            return this;
        }

        public Builder<T> withTimestampInstant(Function<T, Instant> extractor) {
            // Convert Instant to nanoseconds
            this.timestampExtractor = value -> {
                Instant instant = extractor.apply(value);
                return instant != null ? instant.toEpochMilli() * 1_000_000L : null;
            };
            return this;
        }

        public SimpleOpenGeminiPointConverter<T> build() {
            if (fieldExtractors.isEmpty()) {
                throw new IllegalStateException("At least one field must be defined");
            }
            return new SimpleOpenGeminiPointConverter<>(tagExtractors, fieldExtractors, timestampExtractor);
        }
    }
}

