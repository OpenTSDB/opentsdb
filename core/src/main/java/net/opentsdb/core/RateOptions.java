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
 * Provides additional options that will be used when calculating rates. These options are useful
 * when working with metrics that are raw counter values, where a counter is defined by a value that
 * always increases until it hits a maximum value and then it "rolls over" to start back at 0.
 * <p/>
 * These options will only be utilized if the query is for a rate calculation and if the "counter"
 * options is set to true.
 *
 * @since 2.0
 */
public class RateOptions {
  public static final long DEFAULT_RESET_VALUE = 0;

  /**
   * If true, then when calculating a rate of change assume that the metric values are counters and
   * thus non-zero, always increasing and wrap around at some maximum
   */
  private boolean counter;

  /**
   * If calculating a rate of change over a metric that is a counter, then this value specifies the
   * maximum value the counter will obtain before it rolls over. This value will default to
   * Long.MAX_VALUE.
   */
  private long counterMax;

  /**
   * Specifies the the rate change value which, if exceeded, will be considered a data anomaly, such
   * as a system reset of the counter, and the rate will be returned as a zero value for a given
   * data point.
   */
  private long resetValue;

  /**
   * Ctor
   */
  public RateOptions() {
    this.counter = false;
    this.counterMax = Long.MAX_VALUE;
    this.resetValue = DEFAULT_RESET_VALUE;
  }

  /**
   * Ctor
   *
   * @param counter If true, indicates that the rate calculation should assume that the underlying
   * data is from a counter
   * @param counterMax Specifies the maximum value for the counter before it will roll over and
   * restart at 0
   * @param resetValue Specifies the largest rate change that is considered acceptable, if a rate
   * change is seen larger than this value then the counter is assumed to have been reset
   */
  public RateOptions(final boolean counter, final long counterMax,
                     final long resetValue) {
    this.counter = counter;
    this.counterMax = counterMax;
    this.resetValue = resetValue;
  }

  /** @return Whether or not the counter flag is set */
  public boolean isCounter() {
    return counter;
  }

  /** @return The counter max value */
  public long getCounterMax() {
    return counterMax;
  }

  /** @param counterMax The value at which counters roll over */
  public void setCounterMax(long counterMax) {
    this.counterMax = counterMax;
  }

  /** @return The optional reset value for anomaly suppression */
  public long getResetValue() {
    return resetValue;
  }

  /** @param resetValue A difference that may be an anomaly so suppress it */
  public void setResetValue(long resetValue) {
    this.resetValue = resetValue;
  }

  /** @param counter Whether or not the time series should be considered counters */
  public void setIsCounter(boolean counter) {
    this.counter = counter;
  }

  /**
   * Generates a String version of the rate option instance in a format that can be utilized in a
   * query.
   *
   * @return string version of the rate option instance.
   */
  public String toString() {
    return "{" + counter + ',' + counterMax + ',' + resetValue + '}';
  }
}
