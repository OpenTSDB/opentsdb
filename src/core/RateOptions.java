package net.opentsdb.core;

public class RateOptions {
  public static final long DEFAULT_RESET_VALUE = 0;

  /**
   * If true, then when calculating a rate of change assume that the metric
   * values are counters and thus non-zero, always increasing and wrap around at
   * some maximum
   */
  public final boolean counter;

  /**
   * If calculating a rate of change over a metric that is a counter, then this
   * value specifies the maximum value the counter will obtain before it rolls
   * over. This value will default to Long.MAX_VALUE.
   */
  public final long counterMax;

  /**
   * Specifies the the rate change value which, if exceeded, will be considered
   * a data anomaly, such as a system reset of the counter, and the rate will be
   * returned as a zero value for a given data point.
   */
  public final long resetValue;

  public RateOptions(final boolean counter, final long counterMax,
      final long resetValue) {
    this.counter = counter;
    this.counterMax = counterMax;
    this.resetValue = resetValue;
  }

  public String toString() {
    StringBuilder buf = new StringBuilder();
    buf.append('{');
    buf.append(counter);
    buf.append(',').append(counterMax);
    buf.append(',').append(resetValue);
    buf.append('}');
    return buf.toString();
  }
}
