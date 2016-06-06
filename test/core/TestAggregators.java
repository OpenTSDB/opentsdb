// This file is part of OpenTSDB.
// Copyright (C) 2012  The OpenTSDB Authors.
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

import static org.junit.Assert.assertEquals;

import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

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
    final Numbers numbers = new Numbers(values);
    final Aggregator agg = Aggregators.get("dev");
    Assert.assertEquals(expected, agg.runLong(numbers), Math.max(epsilon, 1.0));
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

  @Test
  public void testFirst() {
    final long[] values = new long[10];
    for (int i = 0; i < values.length; i++) {
      values[i] = i;
    }
    
    Aggregator agg = Aggregators.FIRST;
    Numbers numbers = new Numbers(values);
    assertEquals(0, agg.runLong(numbers));
    
    final double[] doubles = new double[10];
    double val = 0.5;
    for (int i = 0; i < doubles.length; i++) {
      doubles[i] = val++;
    }
    
    numbers = new Numbers(doubles);
    assertEquals(0.5, agg.runDouble(numbers), EPSILON_PERCENTAGE);
  }
  
  @Test
  public void testLast() {
    final long[] values = new long[10];
    for (int i = 0; i < values.length; i++) {
      values[i] = i;
    }
    
    Aggregator agg = Aggregators.LAST;
    Numbers numbers = new Numbers(values);
    assertEquals(9, agg.runLong(numbers));
    
    final double[] doubles = new double[10];
    double val = 0.5;
    for (int i = 0; i < doubles.length; i++) {
      doubles[i] = val++;
    }
    
    numbers = new Numbers(doubles);
    assertEquals(9.5, agg.runDouble(numbers), EPSILON_PERCENTAGE);
  }
  
  private void assertAggregatorEquals(long value, Aggregator agg, Numbers numbers) {
    if (numbers.isInteger()) {
      Assert.assertEquals(value, agg.runLong(numbers));
    } else {
      Assert.assertEquals((double)value, agg.runDouble(numbers), 1.0);
    }
    numbers.reset();
  }
}
