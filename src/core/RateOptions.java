// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.core;

/**
 * Provides additional options that will be used when calculating rates. These
 * options are useful when working with metrics that are raw counter values, 
 * where a counter is defined by a value that always increases until it hits
 * a maximum value and then it "rolls over" to start back at 0.
 * <p/>
 * These options will only be utilized if the query is for a rate calculation
 * and if the "counter" options is set to true.
 */
public class RateOptions {
  public static final long DEFAULT_RESET_VALUE = 0;

  /**
   * If true, then when calculating a rate of change assume that the metric
   * values are counters and thus non-zero, always increasing and wrap around at
   * some maximum
   */
  private final boolean counter;

  /**
   * If calculating a rate of change over a metric that is a counter, then this
   * value specifies the maximum value the counter will obtain before it rolls
   * over. This value will default to Long.MAX_VALUE.
   */
  private final long counter_max;

  /**
   * Specifies the the rate change value which, if exceeded, will be considered
   * a data anomaly, such as a system reset of the counter, and the rate will be
   * returned as a zero value for a given data point.
   */
  private final long reset_value;

  /**
   * Ctor
   */
  public RateOptions() {
    this.counter = false;
    this.counter_max = Long.MAX_VALUE;
    this.reset_value = DEFAULT_RESET_VALUE;
  }
  
  /**
   * Ctor
   * @param counter If true, indicates that the rate calculation should assume
   * that the underlying data is from a counter
   * @param counter_max Specifies the maximum value for the counter before it
   * will roll over and restart at 0
   * @param reset_value Specifies the largest rate change that is considered
   * acceptable, if a rate change is seen larger than this value then the
   * counter is assumed to have been reset
   */
  public RateOptions(final boolean counter, final long counter_max,
      final long reset_value) {
    this.counter = counter;
    this.counter_max = counter_max;
    this.reset_value = reset_value;
  }
  
  public boolean isCounter() {
    return counter;
  }

  public long getCounterMax() {
    return counter_max;
  }

  public long getResetValue() {
    return reset_value;
  }

  /**
   * Generates a String version of the rate option instance in a format that 
   * can be utilized in a query.
   * @return string version of the rate option instance.
   */
  public String toString() {
    StringBuilder buf = new StringBuilder();
    buf.append('{');
    buf.append(counter);
    buf.append(',').append(counter_max);
    buf.append(',').append(reset_value);
    buf.append('}');
    return buf.toString();
  }
}
