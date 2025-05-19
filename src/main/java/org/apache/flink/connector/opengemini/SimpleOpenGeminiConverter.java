package org.apache.flink.connector.opengemini;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;

/**
 * A simple implementation of the OpenGeminiConverter for converting POJOs to line protocol format.
 *
 * @param <T> The type of elements to convert
 */
public class SimpleOpenGeminiConverter<T> implements OpenGeminiConverter<T>, Serializable {

    private final FieldExtractor<T> tagExtractor;
    private final FieldExtractor<T> fieldExtractor;
    private final TimestampExtractor<T> timestampExtractor;

    public SimpleOpenGeminiConverter(
            FieldExtractor<T> tagExtractor,
            FieldExtractor<T> fieldExtractor,
            TimestampExtractor<T> timestampExtractor) {
        this.tagExtractor = tagExtractor;
        this.fieldExtractor = fieldExtractor;
        this.timestampExtractor = timestampExtractor;
    }

    @Override
    public String convert(T value, String measurement) throws Exception {
        if (value == null) {
            return null;
        }

        StringBuilder lineBuilder = new StringBuilder();
        lineBuilder.append(escapeSpecialChars(measurement));

        // Add tags if present
        Map<String, Object> tags = tagExtractor.extract(value);
        if (tags != null && !tags.isEmpty()) {
            StringJoiner tagJoiner = new StringJoiner(",");
            for (Map.Entry<String, Object> tag : tags.entrySet()) {
                if (tag.getValue() != null) {
                    tagJoiner.add(escapeSpecialChars(tag.getKey()) + "=" + escapeSpecialChars(tag.getValue().toString()));
                }
            }
            String tagStr = tagJoiner.toString();
            if (!tagStr.isEmpty()) {
                lineBuilder.append(",").append(tagStr);
            }
        }

        // Add fields (required)
        Map<String, Object> fields = fieldExtractor.extract(value);
        if (fields == null || fields.isEmpty()) {
            return null; // No fields to write
        }

        lineBuilder.append(" ");

        StringJoiner fieldJoiner = new StringJoiner(",");
        for (Map.Entry<String, Object> field : fields.entrySet()) {
            if (field.getValue() != null) {
                String fieldStr = formatField(field.getKey(), field.getValue());
                if (fieldStr != null) {
                    fieldJoiner.add(fieldStr);
                }
            }
        }

        String fieldStr = fieldJoiner.toString();
        if (fieldStr.isEmpty()) {
            return null; // No valid fields to write
        }

        lineBuilder.append(fieldStr);

        // Add timestamp if available
        Long timestamp = timestampExtractor.extractTimestamp(value);
        if (timestamp != null) {
            lineBuilder.append(" ").append(timestamp);
        }

        return lineBuilder.toString();
    }

    private String formatField(String key, Object value) {
        if (value == null) {
            return null;
        }

        String escapedKey = escapeSpecialChars(key);

        if (value instanceof Number) {
            // Numbers don't need quotes
            if (value instanceof Float || value instanceof Double) {
                // Float and Double need special handling to ensure they have decimal point
                double doubleValue = ((Number) value).doubleValue();
                // Check if it's a whole number
                if (doubleValue == Math.floor(doubleValue)) {
                    return escapedKey + "=" + doubleValue + "i";
                } else {
                    return escapedKey + "=" + doubleValue;
                }
            } else {
                return escapedKey + "=" + value + "i";
            }
        } else if (value instanceof Boolean) {
            return escapedKey + "=" + value;
        } else {
            // Strings need quotes and escaping
            return escapedKey + "=\"" + escapeSpecialChars(value.toString()) + "\"";
        }
    }

    private String escapeSpecialChars(String input) {
        if (input == null) {
            return "";
        }

        return input
                .replace(" ", "\\ ")
                .replace(",", "\\,")
                .replace("=", "\\=")
                .replace("\"", "\\\"");
    }

    /**
     * Functional interface for extracting fields from a record.
     *
     * @param <T> The type of record
     */
    @FunctionalInterface
    public interface FieldExtractor<T> extends Serializable {
        Map<String, Object> extract(T value) throws Exception;
    }

    /**
     * Functional interface for extracting timestamp from a record.
     *
     * @param <T> The type of record
     */
    @FunctionalInterface
    public interface TimestampExtractor<T> extends Serializable {
        Long extractTimestamp(T value) throws Exception;
    }

    /**
     * Functional interface for accessing fields of a record.
     *
     * @param <T> The type of record
     */
    @FunctionalInterface
    public interface FieldAccessor<T> extends Serializable {
        Object getField(T value) throws Exception;
    }

    /**
     * Builder for SimpleOpenGeminiConverter.
     *
     * @param <T> The type of elements to convert
     */
    public static class Builder<T> implements Serializable {
        private final Map<String, FieldAccessor<T>> tagAccessors = new HashMap<>();
        private final Map<String, FieldAccessor<T>> fieldAccessors = new HashMap<>();
        private TimestampExtractor<T> timestampExtractor = value -> null; // Default: no timestamp

        public Builder<T> addTag(String name, FieldAccessor<T> accessor) {
            tagAccessors.put(name, accessor);
            return this;
        }

        public Builder<T> addField(String name, FieldAccessor<T> accessor) {
            fieldAccessors.put(name, accessor);
            return this;
        }

        public Builder<T> withTimestamp(TimestampExtractor<T> extractor) {
            this.timestampExtractor = extractor;
            return this;
        }

        public SimpleOpenGeminiConverter<T> build() {
            // Create field extractors from accessors
            FieldExtractor<T> tagExtractor = value -> {
                Map<String, Object> tags = new HashMap<>();
                for (Map.Entry<String, FieldAccessor<T>> entry : tagAccessors.entrySet()) {
                    Object tagValue = entry.getValue().getField(value);
                    if (tagValue != null) {
                        tags.put(entry.getKey(), tagValue);
                    }
                }
                return tags;
            };

            FieldExtractor<T> fieldExtractor = value -> {
                Map<String, Object> fields = new HashMap<>();
                for (Map.Entry<String, FieldAccessor<T>> entry : fieldAccessors.entrySet()) {
                    Object fieldValue = entry.getValue().getField(value);
                    if (fieldValue != null) {
                        fields.put(entry.getKey(), fieldValue);
                    }
                }
                return fields;
            };

            return new SimpleOpenGeminiConverter<>(tagExtractor, fieldExtractor, timestampExtractor);
        }
    }

    /**
     * Creates a new builder for SimpleOpenGeminiConverter.
     *
     * @param <T> The type of elements to convert
     * @return A new builder instance
     */
    public static <T> Builder<T> builder() {
        return new Builder<>();
    }
}