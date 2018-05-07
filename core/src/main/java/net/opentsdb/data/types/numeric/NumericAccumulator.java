// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.data.types.numeric;

/**
 * A class used to accumulate a series of numbers to pass to a
 * {@link NumericAggregator}. Arrays are maintained for longs and doubles.
 * Longs are expected first and if one or more doubles are added, the
 * values are copied to the double array and all subsequent values are
 * stored there.
 * On resizing, the arrays stick around for the next usage to avoid
 * re-allocating more space. For growth, the array doubles until 1024
 * values, then increases by 1024 on each subsequent expansion.
 * 
 * @since 3.0
 */
public class NumericAccumulator {
  /** The initial array of long values. */
  protected long[] long_values;
  
  /** The array of double values. */
  protected double[] double_values;
  
  /** Whether or not the accumulator is currently dealing with longs. */
  protected boolean longs = true;
  
  /** The current write index into either array. */
  protected int value_idx = 0;
  
  /** A reference to data point populated with data after running the
   * aggregation function. */
  protected MutableNumericValue dp;
  
  /**
   * Default ctor.
   */
  public NumericAccumulator() {
    long_values = new long[2];
    dp = new MutableNumericValue();
  }
  
  /** @return The data point post {@link #run(NumericAggregator, boolean)}. */
  public MutableNumericValue dp() {
    return dp;
  }
  
  /**
   * Resets the index and longs to true.
   */
  public void reset() {
    longs = true;
    value_idx = 0;
  }
  
  /** @return The current write index. */
  public int valueIndex() {
    return value_idx;
  }
  
  /**
   * Adds a long value to the proper array.
   * @param value A long value to store.
   */
  public void add(final long value) {
    if (!longs) {
      add((double) value);
      return;
    }
    if (value_idx >= long_values.length) {
      final long[] temp = new long[long_values.length < 1024 ? 
          long_values.length * 2 : long_values.length + 1024];
      System.arraycopy(long_values, 0, temp, 0, long_values.length);
      long_values = temp;
    }
    long_values[value_idx] = value;
    value_idx++;
  }
  
  /**
   * Adds a double value to the proper array.
   * @param value A double value to store.
   */
  public void add(final double value) {
    if (longs) {
      shiftToDouble();
    }
    if (value_idx >= double_values.length) {
      final double[] temp = new double[double_values.length < 1024 ? 
          double_values.length * 2 : double_values.length + 1024];
      System.arraycopy(double_values, 0, temp, 0, double_values.length);
      double_values = temp;
    }
    double_values[value_idx] = value;
    value_idx++;
  }
  
  /**
   * Runs the given aggregation function over the proper array of data.
   * @param aggregator A non-null aggregator.
   * @param infectious_nan Whether or not NaNs in the double array are
   * infectious.
   */
  public void run(final NumericAggregator aggregator, 
                  final boolean infectious_nan) {
    if (longs) {
      aggregator.run(long_values, value_idx, dp);
    } else {
      aggregator.run(double_values, value_idx, infectious_nan, dp);
    }
  }
  
  /** Moves the long values to the double array. */
  private void shiftToDouble() {
    if (double_values == null) {
      double_values = new double[long_values.length];
    }
    if (value_idx >= double_values.length) {
      double_values = new double[long_values.length < 1024 ? 
          long_values.length * 2 : long_values.length + 1024];
    }
    for (int i = 0; i < value_idx; i++) {
      double_values[i] = (double) long_values[i];
    }
    longs = false;
  }
}