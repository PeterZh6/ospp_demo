package org.apache.flink.connector.opengemini;

import java.io.Serializable;

/**
 * Converter interface for transforming input records into OpenGemini line protocol format.
 *
 * @param <T> The type of elements to convert
 */
public interface OpenGeminiConverter<T> extends Serializable {

    /**
     * Converts an input record to OpenGemini line protocol format.
     *
     * @param value The input record to convert
     * @param measurement The measurement name to use
     * @return A string in the OpenGemini line protocol format
     * @throws Exception if conversion fails
     */
    String convert(T value, String measurement) throws Exception;
}