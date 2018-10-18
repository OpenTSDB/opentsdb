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
package net.opentsdb.data.types.numeric.aggregators;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import net.opentsdb.core.Registry;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.aggregators.NumericAggregator;
import net.opentsdb.data.types.numeric.aggregators.NumericAggregatorFactory;
import net.opentsdb.data.types.numeric.aggregators.StandardDeviationFactory;
import net.opentsdb.exceptions.IllegalDataException;

public class TestStandardDeviation {

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
  
  @Test
  public void factory() throws Exception {
    TSDB tsdb = mock(TSDB.class);
    Registry registry = mock(Registry.class);
    when(tsdb.getRegistry()).thenReturn(registry);
    NumericAggregatorFactory factory = new StandardDeviationFactory();
    assertNull(factory.initialize(tsdb, null).join());
    assertEquals(StandardDeviationFactory.TYPE, factory.id());
    assertNull(factory.shutdown().join());
  }
  
  @Test
  public void run() throws Exception {
    NumericAggregatorFactory factory = new StandardDeviationFactory();
    NumericAggregator agg = factory.newAggregator(false);
    
    MutableNumericValue dp = new MutableNumericValue(new MillisecondTimeStamp(1000), 0);
    agg.run(new long[] { 1, 2, 3 }, 0, 3, dp);
    assertEquals(0.816, dp.doubleValue(), 0.001);
    
    agg.run(new long[] { 1, 2, 3 }, 0, 2, dp);
    assertEquals(0.5, dp.doubleValue(), 0.001);
    
    agg.run(new long[] { 1, 2, 3 }, 0, 1, dp);
    assertEquals(0, dp.longValue());
    
    try {
      agg.run(new long[] { 1, 2, 3 }, 0, 0, dp);
      fail("Expected IllegalDataException");
    } catch (IllegalDataException e) { }
    
    agg.run(new double[] { 1.25, 2.25, 3.25 }, 0, 3, false, dp);
    assertEquals(0.816, dp.doubleValue(), 0.001);
    
    agg.run(new double[] { 1.25, 2.25, 3.25 }, 0, 2, false, dp);
    assertEquals(0.5, dp.doubleValue(), 0.001);
    
    agg.run(new double[] { 1.25, 2.25, 3.25 }, 0, 1, false, dp);
    assertEquals(0, dp.doubleValue(), 0.001);
    
    try {
      agg.run(new double[] { 1.25, 2.25, 3.25 }, 0, 0, false, dp);
      fail("Expected IllegalDataException");
    } catch (IllegalDataException e) { }
    
    agg.run(new double[] { 1.25, Double.NaN, 3.25 }, 0, 3, false, dp);
    assertEquals(1, dp.doubleValue(), 0.001);
    
    agg.run(new double[] { 1.25, Double.NaN, 3.25 }, 0, 3, true, dp);
    assertTrue(Double.isNaN(dp.doubleValue()));
    
    agg.run(new double[] { Double.NaN, Double.NaN, Double.NaN }, 0, 3, false, dp);
    assertTrue(Double.isNaN(dp.doubleValue()));
    
    agg.run(new double[] { Double.NaN, Double.NaN, Double.NaN }, 0, 3, true, dp);
    assertTrue(Double.isNaN(dp.doubleValue()));
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
    final NumericAggregator agg = new StandardDeviationFactory().newAggregator(false);
    MutableNumericValue dp = new MutableNumericValue(new MillisecondTimeStamp(1000), 0);
    agg.run(values, 0, values.length, dp);
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

}
