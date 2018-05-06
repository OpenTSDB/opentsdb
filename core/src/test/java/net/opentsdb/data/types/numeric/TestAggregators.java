// This file is part of OpenTSDB.
// Copyright (C) 2012-2018  The OpenTSDB Authors.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collections;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import net.opentsdb.core.Aggregator;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.exceptions.IllegalDataException;

public final class TestAggregators {

  private static final Random random;
  static {
    final long seed = System.nanoTime();
    random = new Random(seed);
  }

  /**
   * Epsilon used to compare floating point values.
   * Instead of using a fixed epsilon to compare our numbers, we calculate
   * it based on the percentage of our actual expected values.  We do things
   * this way because our numbers can be extremely large and if you change
   * the scale of the numbers a static precision may no longer work
   */
  private static final double EPSILON_PERCENTAGE = 0.0001;

  /** Helper class to hold a bunch of numbers we can iterate on.  */
  private static final class Numbers implements Aggregator.Longs, Aggregator.Doubles {
    private final long[] longs;
    private final double[] doubles;
    private int i = 0;

    public Numbers(final long[] numbers) {
      longs = numbers;
      doubles = null;
    }
    
    public Numbers(final double[] numbers) {
      longs = null;
      doubles = numbers;
    }

    public boolean isInteger() {
      return longs != null ? true : false;
    }
    
    @Override
    public boolean hasNextValue() {
      return longs != null ? i < longs.length : i < doubles.length; 
    }

    @Override
    public long nextLongValue() {
      return longs[i++];
    }

    @Override
    public double nextDoubleValue() {
      return doubles[i++];
    }

    void reset() {
      i = 0;
    }
  }

  @Test
  public void testStdDevKnownValues() {
    final long[] values = new long[10000];
    for (int i = 0; i < values.length; i++) {
      values[i] = i;
    }
    // Expected value calculated by NumPy
    // $ python2.7
    // >>> import numpy
    // >>> numpy.std(range(10000))
    // 2886.7513315143719
    final double expected = 2886.7513315143719D;
    final double epsilon = 0.01;
    checkSimilarStdDev(values, expected, epsilon);
  }

  @Test
  public void testStdDevRandomValues() {
    final long[] values = new long[1000];
    for (int i = 0; i < values.length; i++) {
      values[i] = random.nextLong();
    }
    final double expected = naiveStdDev(values);
    // Calculate the epsilon based on the percentage of the number.
    final double epsilon = EPSILON_PERCENTAGE * expected;
    checkSimilarStdDev(values, expected, epsilon);
  }

  @Test
  public void testStdDevNoDeviation() {
    final long[] values = {3,3,3};

    final double expected = 0;
    checkSimilarStdDev(values, expected, 0);
  }

  @Test
  public void testStdDevFewDataInputs() {
    final long[] values = {1,2};

    final double expected = 0.5;
    checkSimilarStdDev(values, expected, 0);
  }

  private static void checkSimilarStdDev(final long[] values,
                                         final double expected,
                                         final double epsilon) {
    final NumericAggregator agg = Aggregators.get("dev");
    MutableNumericValue dp = new MutableNumericValue(new MillisecondTimeStamp(1000), 0);
    agg.run(values, values.length, dp);
    Assert.assertEquals(expected, dp.value().toDouble(), Math.max(epsilon, 1.0));
  }

  private static double naiveStdDev(long[] values) {
    double sum = 0;
    for (final double value : values) {
      sum += value;
    }
    double mean = sum / values.length;

    double squaresum = 0;
    for (final double value : values) {
      squaresum += Math.pow(value - mean, 2);
    }
    final double variance = squaresum / values.length;
    return Math.sqrt(variance);
  }

  @Test
  public void sum() throws Exception {
    NumericAggregator agg = Aggregators.get("sum");
    
    MutableNumericValue dp = new MutableNumericValue(new MillisecondTimeStamp(1000), 0);
    agg.run(new long[] { 1, 2, 3 }, 3, dp);
    assertEquals(6, dp.longValue());
    
    agg.run(new long[] { 1, 2, 3 }, 2, dp);
    assertEquals(3, dp.longValue());
    
    agg.run(new long[] { 1, 2, 3 }, 1, dp);
    assertEquals(1, dp.longValue());
    
    try {
      agg.run(new long[] { 1, 2, 3 }, 0, dp);
      fail("Expected IllegalDataException");
    } catch (IllegalDataException e) { }
    
    agg.run(new double[] { 1.25, 2.25, 3.25 }, 3, false, dp);
    assertEquals(6.75, dp.doubleValue(), 0.001);
    
    agg.run(new double[] { 1.25, 2.25, 3.25 }, 2, false, dp);
    assertEquals(3.50, dp.doubleValue(), 0.001);
    
    agg.run(new double[] { 1.25, 2.25, 3.25 }, 1, false, dp);
    assertEquals(1.25, dp.doubleValue(), 0.001);
    
    try {
      agg.run(new double[] { 1.25, 2.25, 3.25 }, 0, false, dp);
      fail("Expected IllegalDataException");
    } catch (IllegalDataException e) { }
    
    agg.run(new double[] { 1.25, Double.NaN, 3.25 }, 3, false, dp);
    assertEquals(4.50, dp.doubleValue(), 0.001);
    
    agg.run(new double[] { 1.25, Double.NaN, 3.25 }, 3, true, dp);
    assertTrue(Double.isNaN(dp.doubleValue()));
    
    agg.run(new double[] { Double.NaN, Double.NaN, Double.NaN }, 3, false, dp);
    assertTrue(Double.isNaN(dp.doubleValue()));
    
    agg.run(new double[] { Double.NaN, Double.NaN, Double.NaN }, 3, true, dp);
    assertTrue(Double.isNaN(dp.doubleValue()));
  }
  
  @Test
  public void min() throws Exception {
    NumericAggregator agg = Aggregators.get("min");
    
    MutableNumericValue dp = new MutableNumericValue(new MillisecondTimeStamp(1000), 0);
    agg.run(new long[] { 1, 2, 3 }, 3, dp);
    assertEquals(1, dp.longValue());
    
    agg.run(new long[] { 1, 2, 3 }, 2, dp);
    assertEquals(1, dp.longValue());
    
    agg.run(new long[] { 1, 2, 3 }, 1, dp);
    assertEquals(1, dp.longValue());
    
    try {
      agg.run(new long[] { 1, 2, 3 }, 0, dp);
      fail("Expected IllegalDataException");
    } catch (IllegalDataException e) { }
    
    agg.run(new double[] { 1.25, 2.25, 3.25 }, 3, false, dp);
    assertEquals(1.25, dp.doubleValue(), 0.001);
    
    agg.run(new double[] { 1.25, 2.25, 3.25 }, 2, false, dp);
    assertEquals(1.25, dp.doubleValue(), 0.001);
    
    agg.run(new double[] { 1.25, 2.25, 3.25 }, 1, false, dp);
    assertEquals(1.25, dp.doubleValue(), 0.001);
    
    try {
      agg.run(new double[] { 1.25, 2.25, 3.25 }, 0, false, dp);
      fail("Expected IllegalDataException");
    } catch (IllegalDataException e) { }
    
    agg.run(new double[] { 1.25, Double.NaN, 3.25 }, 3, false, dp);
    assertEquals(1.25, dp.doubleValue(), 0.001);
    
    agg.run(new double[] { 1.25, Double.NaN, 3.25 }, 3, true, dp);
    assertTrue(Double.isNaN(dp.doubleValue()));
    
    agg.run(new double[] { Double.NaN, Double.NaN, Double.NaN }, 3, false, dp);
    assertTrue(Double.isNaN(dp.doubleValue()));
    
    agg.run(new double[] { Double.NaN, Double.NaN, Double.NaN }, 3, true, dp);
    assertTrue(Double.isNaN(dp.doubleValue()));
  }
  
  @Test
  public void max() throws Exception {
    NumericAggregator agg = Aggregators.get("max");
    
    MutableNumericValue dp = new MutableNumericValue(new MillisecondTimeStamp(1000), 0);
    agg.run(new long[] { 1, 2, 3 }, 3, dp);
    assertEquals(3, dp.longValue());
    
    agg.run(new long[] { 1, 2, 3 }, 2, dp);
    assertEquals(2, dp.longValue());
    
    agg.run(new long[] { 1, 2, 3 }, 1, dp);
    assertEquals(1, dp.longValue());
    
    try {
      agg.run(new long[] { 1, 2, 3 }, 0, dp);
      fail("Expected IllegalDataException");
    } catch (IllegalDataException e) { }
    
    agg.run(new double[] { 1.25, 2.25, 3.25 }, 3, false, dp);
    assertEquals(3.25, dp.doubleValue(), 0.001);
    
    agg.run(new double[] { 1.25, 2.25, 3.25 }, 2, false, dp);
    assertEquals(2.25, dp.doubleValue(), 0.001);
    
    agg.run(new double[] { 1.25, 2.25, 3.25 }, 1, false, dp);
    assertEquals(1.25, dp.doubleValue(), 0.001);
    
    try {
      agg.run(new double[] { 1.25, 2.25, 3.25 }, 0, false, dp);
      fail("Expected IllegalDataException");
    } catch (IllegalDataException e) { }
    
    agg.run(new double[] { 1.25, Double.NaN, 3.25 }, 3, false, dp);
    assertEquals(3.25, dp.doubleValue(), 0.001);
    
    agg.run(new double[] { 1.25, Double.NaN, 3.25 }, 3, true, dp);
    assertTrue(Double.isNaN(dp.doubleValue()));
    
    agg.run(new double[] { Double.NaN, Double.NaN, Double.NaN }, 3, false, dp);
    assertTrue(Double.isNaN(dp.doubleValue()));
    
    agg.run(new double[] { Double.NaN, Double.NaN, Double.NaN }, 3, true, dp);
    assertTrue(Double.isNaN(dp.doubleValue()));
  }
  
  @Test
  public void avg() throws Exception {
    NumericAggregator agg = Aggregators.get("avg");
    
    MutableNumericValue dp = new MutableNumericValue(new MillisecondTimeStamp(1000), 0);
    agg.run(new long[] { 1, 2, 3 }, 3, dp);
    assertEquals(2, dp.longValue());
    
    agg.run(new long[] { 1, 2, 3 }, 2, dp);
    assertEquals(1.5, dp.doubleValue(), 0.001);
    
    agg.run(new long[] { 1, 2, 3 }, 1, dp);
    assertEquals(1, dp.longValue());
    
    try {
      agg.run(new long[] { 1, 2, 3 }, 0, dp);
      fail("Expected IllegalDataException");
    } catch (IllegalDataException e) { }
    
    agg.run(new double[] { 1.25, 2.25, 3.25 }, 3, false, dp);
    assertEquals(2.25, dp.doubleValue(), 0.001);
    
    agg.run(new double[] { 1.25, 2.25, 3.25 }, 2, false, dp);
    assertEquals(1.75, dp.doubleValue(), 0.001);
    
    agg.run(new double[] { 1.25, 2.25, 3.25 }, 1, false, dp);
    assertEquals(1.25, dp.doubleValue(), 0.001);
    
    try {
      agg.run(new double[] { 1.25, 2.25, 3.25 }, 0, false, dp);
      fail("Expected IllegalDataException");
    } catch (IllegalDataException e) { }
    
    agg.run(new double[] { 1.25, Double.NaN, 3.25 }, 3, false, dp);
    assertEquals(2.25, dp.doubleValue(), 0.001);
    
    agg.run(new double[] { 1.25, Double.NaN, 3.25 }, 3, true, dp);
    assertTrue(Double.isNaN(dp.doubleValue()));
    
    agg.run(new double[] { Double.NaN, Double.NaN, Double.NaN }, 3, false, dp);
    assertTrue(Double.isNaN(dp.doubleValue()));
    
    agg.run(new double[] { Double.NaN, Double.NaN, Double.NaN }, 3, true, dp);
    assertTrue(Double.isNaN(dp.doubleValue()));
  }
  
  @Test
  public void median() throws Exception {
    NumericAggregator agg = Aggregators.get("median");
    
    MutableNumericValue dp = new MutableNumericValue(new MillisecondTimeStamp(1000), 0);
    agg.run(new long[] { 1, 2, 3 }, 3, dp);
    assertEquals(2, dp.longValue());
    
    agg.run(new long[] { 1, 2, 3 }, 2, dp);
    assertEquals(2, dp.longValue());
    
    agg.run(new long[] { 1, 2, 3 }, 1, dp);
    assertEquals(1, dp.longValue());
    
    try {
      agg.run(new long[] { 1, 2, 3 }, 0, dp);
      fail("Expected IllegalDataException");
    } catch (IllegalDataException e) { }
    
    agg.run(new double[] { 1.25, 2.25, 3.25 }, 3, false, dp);
    assertEquals(2.25, dp.doubleValue(), 0.001);
    
    agg.run(new double[] { 1.25, 2.25, 3.25 }, 2, false, dp);
    assertEquals(2.25, dp.doubleValue(), 0.001);
    
    agg.run(new double[] { 1.25, 2.25, 3.25 }, 1, false, dp);
    assertEquals(1.25, dp.doubleValue(), 0.001);
    
    try {
      agg.run(new double[] { 1.25, 2.25, 3.25 }, 0, false, dp);
      fail("Expected IllegalDataException");
    } catch (IllegalDataException e) { }
    
    agg.run(new double[] { 1.25, Double.NaN, 3.25 }, 3, false, dp);
    assertEquals(3.25, dp.doubleValue(), 0.001);
    
    agg.run(new double[] { 1.25, Double.NaN, 3.25 }, 3, true, dp);
    assertTrue(Double.isNaN(dp.doubleValue()));
    
    agg.run(new double[] { Double.NaN, Double.NaN, Double.NaN }, 3, false, dp);
    assertTrue(Double.isNaN(dp.doubleValue()));
    
    agg.run(new double[] { Double.NaN, Double.NaN, Double.NaN }, 3, true, dp);
    assertTrue(Double.isNaN(dp.doubleValue()));
  }
  
  @Test
  public void none() throws Exception {
    NumericAggregator agg = Aggregators.get("none");
    
    MutableNumericValue dp = new MutableNumericValue(new MillisecondTimeStamp(1000), 0);
    try {
      agg.run(new long[] { 1, 2, 3 }, 3, dp);
      fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) { }
    
    try {
      agg.run(new double[] { 1.25, 2.25, 3.25 }, 3, false, dp);
      fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) { }
  }
  
  @Test
  public void multiply() throws Exception {
    NumericAggregator agg = Aggregators.get("multiply");
    
    MutableNumericValue dp = new MutableNumericValue(new MillisecondTimeStamp(1000), 0);
    agg.run(new long[] { 1, 2, 3 }, 3, dp);
    assertEquals(6, dp.longValue());
    
    agg.run(new long[] { 1, 2, 3 }, 2, dp);
    assertEquals(2, dp.longValue());
    
    agg.run(new long[] { 1, 2, 3 }, 1, dp);
    assertEquals(1, dp.longValue());
    
    try {
      agg.run(new long[] { 1, 2, 3 }, 0, dp);
      fail("Expected IllegalDataException");
    } catch (IllegalDataException e) { }
    
    agg.run(new double[] { 1.25, 2.25, 3.25 }, 3, false, dp);
    assertEquals(9.140625, dp.doubleValue(), 0.001);
    
    agg.run(new double[] { 1.25, 2.25, 3.25 }, 2, false, dp);
    assertEquals(2.8125, dp.doubleValue(), 0.001);
    
    agg.run(new double[] { 1.25, 2.25, 3.25 }, 1, false, dp);
    assertEquals(1.25, dp.doubleValue(), 0.001);
    
    try {
      agg.run(new double[] { 1.25, 2.25, 3.25 }, 0, false, dp);
      fail("Expected IllegalDataException");
    } catch (IllegalDataException e) { }
    
    agg.run(new double[] { 1.25, Double.NaN, 3.25 }, 3, false, dp);
    assertEquals(4.0625, dp.doubleValue(), 0.001);
    
    agg.run(new double[] { 1.25, Double.NaN, 3.25 }, 3, true, dp);
    assertTrue(Double.isNaN(dp.doubleValue()));
    
    agg.run(new double[] { Double.NaN, Double.NaN, Double.NaN }, 3, false, dp);
    assertTrue(Double.isNaN(dp.doubleValue()));
    
    agg.run(new double[] { Double.NaN, Double.NaN, Double.NaN }, 3, true, dp);
    assertTrue(Double.isNaN(dp.doubleValue()));
  }
  
  @Test
  public void stdev() throws Exception {
    NumericAggregator agg = Aggregators.get("stdev");
    
    MutableNumericValue dp = new MutableNumericValue(new MillisecondTimeStamp(1000), 0);
    agg.run(new long[] { 1, 2, 3 }, 3, dp);
    assertEquals(0.816, dp.doubleValue(), 0.001);
    
    agg.run(new long[] { 1, 2, 3 }, 2, dp);
    assertEquals(0.5, dp.doubleValue(), 0.001);
    
    agg.run(new long[] { 1, 2, 3 }, 1, dp);
    assertEquals(0, dp.longValue());
    
    try {
      agg.run(new long[] { 1, 2, 3 }, 0, dp);
      fail("Expected IllegalDataException");
    } catch (IllegalDataException e) { }
    
    agg.run(new double[] { 1.25, 2.25, 3.25 }, 3, false, dp);
    assertEquals(0.816, dp.doubleValue(), 0.001);
    
    agg.run(new double[] { 1.25, 2.25, 3.25 }, 2, false, dp);
    assertEquals(0.5, dp.doubleValue(), 0.001);
    
    agg.run(new double[] { 1.25, 2.25, 3.25 }, 1, false, dp);
    assertEquals(0, dp.doubleValue(), 0.001);
    
    try {
      agg.run(new double[] { 1.25, 2.25, 3.25 }, 0, false, dp);
      fail("Expected IllegalDataException");
    } catch (IllegalDataException e) { }
    
    agg.run(new double[] { 1.25, Double.NaN, 3.25 }, 3, false, dp);
    assertEquals(1, dp.doubleValue(), 0.001);
    
    agg.run(new double[] { 1.25, Double.NaN, 3.25 }, 3, true, dp);
    assertTrue(Double.isNaN(dp.doubleValue()));
    
    agg.run(new double[] { Double.NaN, Double.NaN, Double.NaN }, 3, false, dp);
    assertTrue(Double.isNaN(dp.doubleValue()));
    
    agg.run(new double[] { Double.NaN, Double.NaN, Double.NaN }, 3, true, dp);
    assertTrue(Double.isNaN(dp.doubleValue()));
  }
  
  @Test
  public void count() throws Exception {
    NumericAggregator agg = Aggregators.get("count");
    
    MutableNumericValue dp = new MutableNumericValue(new MillisecondTimeStamp(1000), 0);
    agg.run(new long[] { 1, 2, 3 }, 3, dp);
    assertEquals(3, dp.longValue());
    
    agg.run(new long[] { 1, 2, 3 }, 2, dp);
    assertEquals(2, dp.longValue());
    
    agg.run(new long[] { 1, 2, 3 }, 1, dp);
    assertEquals(1, dp.longValue());
    
    agg.run(new long[] { 1, 2, 3 }, 0, dp);
    assertEquals(0, dp.longValue());
    
    agg.run(new double[] { 1.25, 2.25, 3.25 }, 3, false, dp);
    assertEquals(3, dp.longValue());
    
    agg.run(new double[] { 1.25, 2.25, 3.25 }, 2, false, dp);
    assertEquals(2, dp.longValue());
    
    agg.run(new double[] { 1.25, 2.25, 3.25 }, 1, false, dp);
    assertEquals(1, dp.longValue());
    
    agg.run(new double[] { 1.25, 2.25, 3.25 }, 0, false, dp);
    assertEquals(0, dp.longValue());
    
    agg.run(new double[] { 1.25, Double.NaN, 3.25 }, 3, false, dp);
    assertEquals(3, dp.longValue());
    
    agg.run(new double[] { 1.25, Double.NaN, 3.25 }, 3, true, dp);
    assertEquals(3, dp.longValue());
    
    agg.run(new double[] { Double.NaN, Double.NaN, Double.NaN }, 3, false, dp);
    assertEquals(3, dp.longValue());
    
    agg.run(new double[] { Double.NaN, Double.NaN, Double.NaN }, 3, true, dp);
    assertEquals(3, dp.longValue());
  }
  
  @Test
  public void percentiles() throws Exception {
    NumericAggregator agg = Aggregators.get("p99");
    
    MutableNumericValue dp = new MutableNumericValue(new MillisecondTimeStamp(1000), 0);
    agg.run(new long[] { 1, 2, 3 }, 3, dp);
    assertEquals(3, dp.longValue());
    
    agg.run(new long[] { 1, 2, 3 }, 2, dp);
    assertEquals(2, dp.longValue());
    
    agg.run(new long[] { 1, 2, 3 }, 1, dp);
    assertEquals(1, dp.longValue());
    
    try {
      agg.run(new long[] { 1, 2, 3 }, 0, dp);
      fail("Expected IllegalDataException");
    } catch (IllegalDataException e) { }
    
    agg.run(new double[] { 1.25, 2.25, 3.25 }, 3, false, dp);
    assertEquals(3.25, dp.doubleValue(), 0.001);
    
    agg.run(new double[] { 1.25, 2.25, 3.25 }, 2, false, dp);
    assertEquals(2.25, dp.doubleValue(), 0.001);
    
    agg.run(new double[] { 1.25, 2.25, 3.25 }, 1, false, dp);
    assertEquals(1.25, dp.doubleValue(), 0.001);
    
    try {
      agg.run(new double[] { 1.25, 2.25, 3.25 }, 0, false, dp);
      fail("Expected IllegalDataException");
    } catch (IllegalDataException e) { }
    
    agg.run(new double[] { 1.25, Double.NaN, 3.25 }, 3, false, dp);
    assertEquals(3.25, dp.doubleValue(), 0.001);
    
    agg.run(new double[] { 1.25, Double.NaN, 3.25 }, 3, true, dp);
    assertTrue(Double.isNaN(dp.doubleValue()));
    
    agg.run(new double[] { Double.NaN, Double.NaN, Double.NaN }, 3, false, dp);
    assertTrue(Double.isNaN(dp.doubleValue()));
    
    agg.run(new double[] { Double.NaN, Double.NaN, Double.NaN }, 3, true, dp);
    assertTrue(Double.isNaN(dp.doubleValue()));
  }
  
  @Test
  public void first() throws Exception {
    NumericAggregator agg = Aggregators.get("first");
    
    MutableNumericValue dp = new MutableNumericValue(new MillisecondTimeStamp(1000), 0);
    agg.run(new long[] { 1, 2, 3 }, 3, dp);
    assertEquals(1, dp.longValue());
    
    agg.run(new long[] { 1, 2, 3 }, 2, dp);
    assertEquals(1, dp.longValue());
    
    agg.run(new long[] { 1, 2, 3 }, 1, dp);
    assertEquals(1, dp.longValue());
    
    try {
      agg.run(new long[] { 1, 2, 3 }, 0, dp);
      fail("Expected IllegalDataException");
    } catch (IllegalDataException e) { }
    
    agg.run(new double[] { 1.25, 2.25, 3.25 }, 3, false, dp);
    assertEquals(1.25, dp.doubleValue(), 0.001);
    
    agg.run(new double[] { 1.25, 2.25, 3.25 }, 2, false, dp);
    assertEquals(1.25, dp.doubleValue(), 0.001);
    
    agg.run(new double[] { 1.25, 2.25, 3.25 }, 1, false, dp);
    assertEquals(1.25, dp.doubleValue(), 0.001);
    
    try {
      agg.run(new double[] { 1.25, 2.25, 3.25 }, 0, false, dp);
      fail("Expected IllegalDataException");
    } catch (IllegalDataException e) { }
    
    agg.run(new double[] { 1.25, Double.NaN, 3.25 }, 3, false, dp);
    assertEquals(1.25, dp.doubleValue(), 0.001);
    
    agg.run(new double[] { 1.25, Double.NaN, 3.25 }, 3, true, dp);
    assertTrue(Double.isNaN(dp.doubleValue()));
    
    agg.run(new double[] { Double.NaN, Double.NaN, Double.NaN }, 3, false, dp);
    assertTrue(Double.isNaN(dp.doubleValue()));
    
    agg.run(new double[] { Double.NaN, Double.NaN, Double.NaN }, 3, true, dp);
    assertTrue(Double.isNaN(dp.doubleValue()));
  }
  
  @Test
  public void last() throws Exception {
    NumericAggregator agg = Aggregators.get("last");
    
    MutableNumericValue dp = new MutableNumericValue(new MillisecondTimeStamp(1000), 0);
    agg.run(new long[] { 1, 2, 3 }, 3, dp);
    assertEquals(3, dp.longValue());
    
    agg.run(new long[] { 1, 2, 3 }, 2, dp);
    assertEquals(2, dp.longValue());
    
    agg.run(new long[] { 1, 2, 3 }, 1, dp);
    assertEquals(1, dp.longValue());
    
    try {
      agg.run(new long[] { 1, 2, 3 }, 0, dp);
      fail("Expected IllegalDataException");
    } catch (IllegalDataException e) { }
    
    agg.run(new double[] { 1.25, 2.25, 3.25 }, 3, false, dp);
    assertEquals(3.25, dp.doubleValue(), 0.001);
    
    agg.run(new double[] { 1.25, 2.25, 3.25 }, 2, false, dp);
    assertEquals(2.25, dp.doubleValue(), 0.001);
    
    agg.run(new double[] { 1.25, 2.25, 3.25 }, 1, false, dp);
    assertEquals(1.25, dp.doubleValue(), 0.001);
    
    try {
      agg.run(new double[] { 1.25, 2.25, 3.25 }, 0, false, dp);
      fail("Expected IllegalDataException");
    } catch (IllegalDataException e) { }
    
    agg.run(new double[] { 1.25, Double.NaN, 3.25 }, 3, false, dp);
    assertEquals(3.25, dp.doubleValue(), 0.001);
    
    agg.run(new double[] { 1.25, Double.NaN, 3.25 }, 3, true, dp);
    assertTrue(Double.isNaN(dp.doubleValue()));
    
    agg.run(new double[] { Double.NaN, Double.NaN, Double.NaN }, 3, false, dp);
    assertTrue(Double.isNaN(dp.doubleValue()));
    
    agg.run(new double[] { Double.NaN, Double.NaN, Double.NaN }, 3, true, dp);
    assertTrue(Double.isNaN(dp.doubleValue()));
  }
  @Test
  public void testPercentiles() {
    final long[] longValues = new long[1000];
    for (int i = 0; i < longValues.length; i++) {
      longValues[i] = i+1;
    }
  
    Numbers values = new Numbers(longValues);
    assertAggregatorEquals(500, Aggregators.get("p50"), values);
    assertAggregatorEquals(750, Aggregators.get("p75"), values);
    assertAggregatorEquals(900, Aggregators.get("p90"), values);
    assertAggregatorEquals(950, Aggregators.get("p95"), values);
    assertAggregatorEquals(990, Aggregators.get("p99"), values);
    assertAggregatorEquals(999, Aggregators.get("p999"), values);
  
    assertAggregatorEquals(500, Aggregators.get("ep50r3"), values);
    assertAggregatorEquals(750, Aggregators.get("ep75r3"), values);
    assertAggregatorEquals(900, Aggregators.get("ep90r3"), values);
    assertAggregatorEquals(950, Aggregators.get("ep95r3"), values);
    assertAggregatorEquals(990, Aggregators.get("ep99r3"), values);
    assertAggregatorEquals(999, Aggregators.get("ep999r3"), values);
    
    assertAggregatorEquals(500, Aggregators.get("ep50r7"), values);
    assertAggregatorEquals(750, Aggregators.get("ep75r7"), values);
    assertAggregatorEquals(900, Aggregators.get("ep90r7"), values);
    assertAggregatorEquals(950, Aggregators.get("ep95r7"), values);
    assertAggregatorEquals(990, Aggregators.get("ep99r7"), values);
    assertAggregatorEquals(999, Aggregators.get("ep999r7"), values);
  }
  
  private void assertAggregatorEquals(long value, NumericAggregator agg, Numbers numbers) {
    MutableNumericValue dp = new MutableNumericValue(new MillisecondTimeStamp(1000), 0);
    if (numbers.isInteger()) {
      agg.run(numbers.longs, numbers.longs.length, dp);
    } else {
      agg.run(numbers.doubles, numbers.doubles.length, false, dp);
    }
    Assert.assertEquals((double) value, dp.toDouble(), 1.0);
    numbers.reset();
  }
}
