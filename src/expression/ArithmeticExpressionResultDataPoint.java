package net.opentsdb.expression;

import net.opentsdb.core.DataPoint;

public class ArithmeticExpressionResultDataPoint implements DataPoint {
  private long timestamp;
  private double value;

  public ArithmeticExpressionResultDataPoint(final long timestamp,
      final double value) {
    this.timestamp = timestamp;
    this.value = value;
  }

  @Override
  public long timestamp() {
    return timestamp;
  }

  @Override
  public boolean isInteger() {
    return false;
  }

  @Override
  public long longValue() {
    throw new ClassCastException(
        "ArithmeticExpressionResultDataPoints are always storing their values as doubles");
  }

  @Override
  public double doubleValue() {
    return value;
  }

  @Override
  public double toDouble() {
    return value;
  }

}