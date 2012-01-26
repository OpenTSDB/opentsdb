package net.opentsdb.core;

/**
 * Represents a single annotation value to mark meta data in a graph at a
 * certain time.
 */
public class Annotation {
    private long timestamp;
    private byte[] value;
    
    public Annotation(long timestamp, byte[] value) {
        this.timestamp = timestamp;
        this.value = value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public byte[] getValue() {
        return value;
    }
}
