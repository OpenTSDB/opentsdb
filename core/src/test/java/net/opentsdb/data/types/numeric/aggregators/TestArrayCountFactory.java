// This file is part of OpenTSDB.
// Copyright (C) 2019-2020  The OpenTSDB Authors.
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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

public class TestArrayCountFactory extends BaseTestNumericArray {
  
  @Test
  public void longs() {
    ArrayCountFactory.ArrayCount agg = new ArrayCountFactory.ArrayCount(false, NON_POOLED);
    agg.accumulate(new long[] { 42, -24, 0, 1 });
    agg.accumulate(new long[] { 3, -13, 5, -1 });
    
    assertTrue(agg.isInteger());
    assertNull(agg.doubleArray());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertArrayEquals(new long[] { 2, 2, 2, 2 }, agg.longArray());
    
    agg = new ArrayCountFactory.ArrayCount(false, NON_POOLED);
    agg.accumulate(new long[] { });
    agg.accumulate(new long[] { });
    
    assertTrue(agg.isInteger());
    assertNull(agg.doubleArray());
    assertEquals(0, agg.offset());
    assertEquals(0, agg.end());
    assertArrayEquals(new long[] { }, agg.longArray());
    
    // bad length
    try {
      agg.accumulate(new long[] { 1 });
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void longsPooled() {
    ArrayCountFactory.ArrayCount agg = new ArrayCountFactory.ArrayCount(false, POOLED);
    agg.accumulate(new long[] { 42, -24, 0, 1 });
    agg.accumulate(new long[] { 3, -13, 5, -1 });
    
    assertTrue(agg.isInteger());
    assertNull(agg.doubleArray());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertPooledArrayEquals(new long[] { 2, 2, 2, 2 }, agg.longArray());
    assertPoolCounters(LONG_POOL, 1, 0, 0, 0);
    assertPoolCounters(DOUBLE_POOL, 0, 0, 0, 0);
    agg.close();
    assertPoolCounters(LONG_POOL, 1, 0, 0, 1);
    assertPoolCounters(DOUBLE_POOL, 0, 0, 0, 0);
    
    before();
    agg = new ArrayCountFactory.ArrayCount(false, POOLED);
    agg.accumulate(new long[] { });
    agg.accumulate(new long[] { });
    
    assertTrue(agg.isInteger());
    assertNull(agg.doubleArray());
    assertEquals(0, agg.offset());
    assertEquals(0, agg.end());
    assertPooledArrayEquals(new long[] { }, agg.longArray());
    
    // bad length
    try {
      agg.accumulate(new long[] { 1 });
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    assertPoolCounters(LONG_POOL, 1, 0, 0, 0);
    assertPoolCounters(DOUBLE_POOL, 0, 0, 0, 0);
    agg.close();
  }
  
  @Test
  public void doubles() throws Exception {
    ArrayCountFactory.ArrayCount agg = new ArrayCountFactory.ArrayCount(false, NON_POOLED);
    agg.accumulate(new double[] { 42, -24, 0, 1 });
    agg.accumulate(new double[] { 3, -13, 5, -1 });
    
    assertTrue(agg.isInteger());
    assertNull(agg.doubleArray());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertArrayEquals(new long[] { 2, 2, 2, 2 }, agg.longArray());
    
    // non-infectious nans
    agg = new ArrayCountFactory.ArrayCount(false, NON_POOLED);
    agg.accumulate(new double[] { 42, -24, 0, Double.NaN });
    agg.accumulate(new double[] { 3, Double.NaN, 5, -1 });
    
    assertTrue(agg.isInteger());
    assertNull(agg.doubleArray());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertArrayEquals(new long[] { 2, 1, 2, 1 }, agg.longArray());
    
    // infectious nans
    agg = new ArrayCountFactory.ArrayCount(true, NON_POOLED);
    agg.accumulate(new double[] { 42, -24, 0, Double.NaN });
    agg.accumulate(new double[] { 3, Double.NaN, 5, -1 });
    
    assertTrue(agg.isInteger());
    assertNull(agg.doubleArray());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertArrayEquals(new long[] { 2, 1, 2, 1 }, agg.longArray());
    
    // bad length
    try {
      agg.accumulate(new double[] { 1 });
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void doublesPooled() throws Exception {
    ArrayCountFactory.ArrayCount agg = new ArrayCountFactory.ArrayCount(false, POOLED);
    agg.accumulate(new double[] { 42, -24, 0, 1 });
    agg.accumulate(new double[] { 3, -13, 5, -1 });
    
    assertTrue(agg.isInteger());
    assertNull(agg.doubleArray());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertPooledArrayEquals(new long[] { 2, 2, 2, 2 }, agg.longArray());
    assertPoolCounters(LONG_POOL, 1, 0, 0, 0);
    assertPoolCounters(DOUBLE_POOL, 0, 0, 0, 0);
    agg.close();
    assertPoolCounters(LONG_POOL, 1, 0, 0, 1);
    assertPoolCounters(DOUBLE_POOL, 0, 0, 0, 0);
    
    // non-infectious nans
    before();
    agg = new ArrayCountFactory.ArrayCount(false, POOLED);
    agg.accumulate(new double[] { 42, -24, 0, Double.NaN });
    agg.accumulate(new double[] { 3, Double.NaN, 5, -1 });
    
    assertTrue(agg.isInteger());
    assertNull(agg.doubleArray());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertPooledArrayEquals(new long[] { 2, 1, 2, 1 }, agg.longArray());
    assertPoolCounters(LONG_POOL, 1, 0, 0, 0);
    assertPoolCounters(DOUBLE_POOL, 0, 0, 0, 0);
    agg.close();
    
    // infectious nans
    before();
    agg = new ArrayCountFactory.ArrayCount(true, POOLED);
    agg.accumulate(new double[] { 42, -24, 0, Double.NaN });
    agg.accumulate(new double[] { 3, Double.NaN, 5, -1 });
    
    assertTrue(agg.isInteger());
    assertNull(agg.doubleArray());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertPooledArrayEquals(new long[] { 2, 1, 2, 1 }, agg.longArray());
    
    // bad length
    try {
      agg.accumulate(new double[] { 1 });
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    assertPoolCounters(LONG_POOL, 1, 0, 0, 0);
    assertPoolCounters(DOUBLE_POOL, 0, 0, 0, 0);
    agg.close();
  }
  
  @Test
  public void mixed() throws Exception {
    ArrayCountFactory.ArrayCount agg = new ArrayCountFactory.ArrayCount(false, NON_POOLED);
    agg.accumulate(new long[] { 42, -24, 0, 1 });
    agg.accumulate(new double[] { 3, -13, 5, -1 });
    
    assertTrue(agg.isInteger());
    assertNull(agg.doubleArray());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertArrayEquals(new long[] { 2, 2, 2, 2 }, agg.longArray());
    
    agg = new ArrayCountFactory.ArrayCount(false, NON_POOLED);
    agg.accumulate(new double[] { 3, -13, 5, -1 });
    agg.accumulate(new long[] { 42, -24, 0, 1 });
    
    assertTrue(agg.isInteger());
    assertNull(agg.doubleArray());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertArrayEquals(new long[] { 2, 2, 2, 2 }, agg.longArray());
  }
  
  @Test
  public void mixedPooled() throws Exception {
    ArrayCountFactory.ArrayCount agg = new ArrayCountFactory.ArrayCount(false, POOLED);
    agg.accumulate(new long[] { 42, -24, 0, 1 });
    agg.accumulate(new double[] { 3, -13, 5, -1 });
    
    assertTrue(agg.isInteger());
    assertNull(agg.doubleArray());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertPooledArrayEquals(new long[] { 2, 2, 2, 2 }, agg.longArray());
    assertPoolCounters(LONG_POOL, 1, 0, 0, 0);
    assertPoolCounters(DOUBLE_POOL, 0, 0, 0, 0);
    agg.close();
    
    before();
    agg = new ArrayCountFactory.ArrayCount(false, POOLED);
    agg.accumulate(new double[] { 3, -13, 5, -1 });
    agg.accumulate(new long[] { 42, -24, 0, 1 });
    
    assertTrue(agg.isInteger());
    assertNull(agg.doubleArray());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertPooledArrayEquals(new long[] { 2, 2, 2, 2 }, agg.longArray());
    assertPoolCounters(LONG_POOL, 1, 0, 0, 0);
    assertPoolCounters(DOUBLE_POOL, 0, 0, 0, 0);
    agg.close();
  }
  
  @Test
  public void accumulateDoublesIndexed() throws Exception {
    ArrayCountFactory.ArrayCount agg = 
        new ArrayCountFactory.ArrayCount(DefaultArrayAggregatorConfig
          .newBuilder()
          .setArraySize(4)
          .build(), 
        NON_POOLED);
    agg.accumulate(-24.0, 1);
    assertArrayEquals(new long[] { 0, 1, 0, 0 }, agg.long_accumulator);
    
    agg.accumulate(42.0, 0);
    assertArrayEquals(new long[] { 1, 1, 0, 0 }, agg.long_accumulator);
    
    agg.accumulate(3.0, 0);
    assertArrayEquals(new long[] { 2, 1, 0, 0 }, agg.long_accumulator);
    
    agg.accumulate(Double.NaN, 1);
    assertArrayEquals(new long[] { 2, 1, 0, 0 }, agg.long_accumulator);
    
    // oob
    try {
      agg.accumulate(6.5, 5);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // others to match the doubles() UT.
    agg.accumulate(0.0, 2);
    agg.accumulate(Double.NaN, 3);
    agg.accumulate(5.0, 2);
    agg.accumulate(-1.0, 3);
    assertArrayEquals(new long[] { 2, 1, 2, 1 }, agg.long_accumulator);
  }
  
  @Test
  public void accumulateDoublesIndexedPooled() throws Exception {
    ArrayCountFactory.ArrayCount agg = 
        new ArrayCountFactory.ArrayCount(DefaultArrayAggregatorConfig
          .newBuilder()
          .setArraySize(4)
          .build(), 
        POOLED);
    agg.accumulate(-24.0, 1);
    assertPooledArrayEquals(new long[] { 0, 1, 0, 0 }, agg.long_accumulator);
    
    agg.accumulate(42.0, 0);
    assertPooledArrayEquals(new long[] { 1, 1, 0, 0 }, agg.long_accumulator);
    
    agg.accumulate(3.0, 0);
    assertPooledArrayEquals(new long[] { 2, 1, 0, 0 }, agg.long_accumulator);
    
    agg.accumulate(Double.NaN, 1);
    assertPooledArrayEquals(new long[] { 2, 1, 0, 0 }, agg.long_accumulator);
    
    // oob
    try {
      agg.accumulate(6.5, 5);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // others to match the doubles() UT.
    agg.accumulate(0.0, 2);
    agg.accumulate(Double.NaN, 3);
    agg.accumulate(5.0, 2);
    agg.accumulate(-1.0, 3);
    assertPooledArrayEquals(new long[] { 2, 1, 2, 1 }, agg.long_accumulator);    
    agg.close();
    
    // no size so we can't accumulate.
    agg = new ArrayCountFactory.ArrayCount(false, POOLED);
    try {
      agg.accumulate(6.5, 0);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    agg.close();
  }
  
  @Test
  public void offsets() throws Exception {
    ArrayCountFactory.ArrayCount agg = new ArrayCountFactory.ArrayCount(false, NON_POOLED);
    agg.accumulate(new long[] { 42, -24, 0, 1 }, 1, 3);
    agg.accumulate(new long[] { 3, -13, 5, -1 }, 1, 3);
    
    assertTrue(agg.isInteger());
    assertNull(agg.doubleArray());
    assertEquals(0, agg.offset());
    assertEquals(2, agg.end());
    assertArrayEquals(new long[] { 2, 2 }, agg.longArray());
    
    agg = new ArrayCountFactory.ArrayCount(false, NON_POOLED);
    agg.accumulate(new double[] { 42, -24, 0, 1 }, 1, 3);
    agg.accumulate(new double[] { 3, -13, 5, -1 }, 1, 3);
    
    assertTrue(agg.isInteger());
    assertNull(agg.doubleArray());
    assertEquals(0, agg.offset());
    assertEquals(2, agg.end());
    assertArrayEquals(new long[] { 2, 2 }, agg.longArray());
    
    agg = new ArrayCountFactory.ArrayCount(false, NON_POOLED);
    agg.accumulate(new long[] { 42, -24, 0, 1 }, 1, 3);
    agg.accumulate(new double[] { 3, -13, 5, -1 }, 1, 3);
    
    assertTrue(agg.isInteger());
    assertNull(agg.doubleArray());
    assertEquals(0, agg.offset());
    assertEquals(2, agg.end());
    assertArrayEquals(new long[] { 2, 2 }, agg.longArray());
  }
  
  @Test
  public void offsetsPooled() throws Exception {
    ArrayCountFactory.ArrayCount agg = new ArrayCountFactory.ArrayCount(false, POOLED);
    agg.accumulate(new long[] { 42, -24, 0, 1 }, 1, 3);
    agg.accumulate(new long[] { 3, -13, 5, -1 }, 1, 3);
    
    assertTrue(agg.isInteger());
    assertNull(agg.doubleArray());
    assertEquals(0, agg.offset());
    assertEquals(2, agg.end());
    assertPooledArrayEquals(new long[] { 2, 2 }, agg.longArray());
    assertPoolCounters(LONG_POOL, 1, 0, 0, 0);
    assertPoolCounters(DOUBLE_POOL, 0, 0, 0, 0);
    agg.close();
    assertPoolCounters(LONG_POOL, 1, 0, 0, 1);
    assertPoolCounters(DOUBLE_POOL, 0, 0, 0, 0);
    
    before();
    agg = new ArrayCountFactory.ArrayCount(false, POOLED);
    agg.accumulate(new double[] { 42, -24, 0, 1 }, 1, 3);
    agg.accumulate(new double[] { 3, -13, 5, -1 }, 1, 3);
    
    assertTrue(agg.isInteger());
    assertNull(agg.doubleArray());
    assertEquals(0, agg.offset());
    assertEquals(2, agg.end());
    assertPooledArrayEquals(new long[] { 2, 2 }, agg.longArray());
    assertPoolCounters(LONG_POOL, 1, 0, 0, 0);
    assertPoolCounters(DOUBLE_POOL, 0, 0, 0, 0);
    agg.close();
    
    before();
    agg = new ArrayCountFactory.ArrayCount(false, POOLED);
    agg.accumulate(new long[] { 42, -24, 0, 1 }, 1, 3);
    agg.accumulate(new double[] { 3, -13, 5, -1 }, 1, 3);
    
    assertTrue(agg.isInteger());
    assertNull(agg.doubleArray());
    assertEquals(0, agg.offset());
    assertEquals(2, agg.end());
    assertPooledArrayEquals(new long[] { 2, 2 }, agg.longArray());
    assertPoolCounters(LONG_POOL, 1, 0, 0, 0);
    assertPoolCounters(DOUBLE_POOL, 0, 0, 0, 0);
    agg.close();
  }

  @Test
  public void testCombine() {
    ArrayCountFactory arrayCountFactory = new ArrayCountFactory();
    ArraySumFactory arraySumFactory = new ArraySumFactory();

    NumericArrayAggregator sourceAggregator1 = arraySumFactory.newAggregator(false);
    NumericArrayAggregator sourceAggregator2 = arraySumFactory.newAggregator(false);

    sourceAggregator1.accumulate(new long[] {11, 11, 11, 11});
    sourceAggregator2.accumulate(new long[] {10, 10, 10, 10});

    NumericArrayAggregator destination = arrayCountFactory.newAggregator(false);

    destination.combine(sourceAggregator1);
    destination.combine(sourceAggregator2);

    assertArrayEquals(new long[] { 21, 21, 21, 21 }, destination.longArray());
  }
  
  @Test
  public void testCombinePooled() throws Exception {
    ArrayCountFactory arrayCountFactory = new ArrayCountFactory();
    ArraySumFactory arraySumFactory = new ArraySumFactory();

    NumericArrayAggregator sourceAggregator1 = arraySumFactory.newAggregator(false);
    NumericArrayAggregator sourceAggregator2 = arraySumFactory.newAggregator(false);

    sourceAggregator1.accumulate(new long[] {11, 11, 11, 11});
    sourceAggregator2.accumulate(new long[] {10, 10, 10, 10});

    NumericArrayAggregator destination = new ArrayCountFactory.ArrayCount(false, POOLED);

    destination.combine(sourceAggregator1);
    destination.combine(sourceAggregator2);

    assertPooledArrayEquals(new long[] { 21, 21, 21, 21 }, destination.longArray());
    assertPoolCounters(LONG_POOL, 1, 0, 0, 0);
    assertPoolCounters(DOUBLE_POOL, 0, 0, 0, 0);
    destination.close();
    assertPoolCounters(LONG_POOL, 1, 0, 0, 1);
    assertPoolCounters(DOUBLE_POOL, 0, 0, 0, 0);
  }
  
}