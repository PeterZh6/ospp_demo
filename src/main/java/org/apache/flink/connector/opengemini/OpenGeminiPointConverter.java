package org.apache.flink.connector.opengemini;

import io.opengemini.client.api.Point;

import java.io.Serializable;

/**
 * Enhanced converter interface for converting objects to OpenGemini Points.
 * This provides better integration with the OpenGemini client.
 *
 * @param <T> The type of object to convert
 */
public interface OpenGeminiPointConverter<T> extends Serializable {
    /**
     * Convert an object to an OpenGemini Point.
     *
     * @param value The value to convert
     * @param measurement The measurement name
     * @return The converted Point, or null if the value should be skipped
     */
    Point convert(T value, String measurement) throws Exception;
}