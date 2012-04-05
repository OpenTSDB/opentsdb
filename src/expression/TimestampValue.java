package net.opentsdb.expression;

import java.io.Serializable;

public class TimestampValue implements Serializable {
  private static final long serialVersionUID = -6220718919289644586L;

  private long timestamp;
  private double value;

  public TimestampValue(long timestamp, double value) {
    this.timestamp = timestamp;
    this.value = value;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public double getValue() {
    return value;
  }

  public String toString() {
    return new StringBuilder().append(timestamp).append("|").append(value)
        .toString();
  }
}