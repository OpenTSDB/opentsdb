// This file is part of OpenTSDB.
// Copyright (C) 2018-2020  The OpenTSDB Authors.
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;

import net.opentsdb.pools.DefaultObjectPoolConfig;
import net.opentsdb.pools.IntArrayPool;
import net.opentsdb.pools.MockArrayObjectPool;

public class TestArrayAverage extends BaseTestNumericArray {
  
  private static MockArrayObjectPool INT_POOL;
  
  @BeforeClass
  public static void beforeClassLocal() {
    INT_POOL = new MockArrayObjectPool(DefaultObjectPoolConfig.newBuilder()
        .setAllocator(new IntArrayPool())
        .setInitialCount(4)
        .setArrayLength(5)
        .setId(IntArrayPool.TYPE)
        .build());
    when(POOLED.intPool()).thenReturn(INT_POOL);
  }
  
  @Before
  public void before() {
    LONG_POOL.resetCounters();
    DOUBLE_POOL.resetCounters();
    INT_POOL.resetCounters();
  }
  
  @Test
  public void longs() {
    ArrayAverageFactory.ArrayAverage agg = 
        new ArrayAverageFactory.ArrayAverage(false, NON_POOLED);
    agg.accumulate(new long[] { 42, -24, 0, 1 });
    agg.accumulate(new long[] { 3, -13, 5, -1 });
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertArrayEquals(new double[] { 22.5, -18.5, 2.5, 0 }, agg.doubleArray(), 0.001);
    
    agg = new ArrayAverageFactory.ArrayAverage(false, NON_POOLED);
    agg.accumulate(new long[] { });
    agg.accumulate(new long[] { });
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(0, agg.end());
    assertArrayEquals(new double[] { }, agg.doubleArray(), 0.001);
    
    // bad length
    agg = new ArrayAverageFactory.ArrayAverage(false, NON_POOLED);
    agg.accumulate(new long[] { 42, -24, 0, 1 });
    try {
      agg.accumulate(new long[] { 1 });
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void longsPooled() {
    ArrayAverageFactory.ArrayAverage agg = 
        new ArrayAverageFactory.ArrayAverage(false, POOLED);
    agg.accumulate(new long[] { 42, -24, 0, 1 });
    agg.accumulate(new long[] { 3, -13, 5, -1 });
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertPooledArrayEquals(new double[] { 22.5, -18.5, 2.5, 0 }, agg.doubleArray(), 0.001);
    assertPoolCounters(INT_POOL, 1, 0, 0, 1);
    assertPoolCounters(LONG_POOL, 1, 0, 0, 1);
    assertPoolCounters(DOUBLE_POOL, 1, 0, 0, 0);
    
    // same, no-op
    assertPooledArrayEquals(new double[] { 22.5, -18.5, 2.5, 0 }, agg.doubleArray(), 0.001);
    agg.close();
    assertPoolCounters(INT_POOL, 1, 0, 0, 1);
    assertPoolCounters(LONG_POOL, 1, 0, 0, 1);
    assertPoolCounters(DOUBLE_POOL, 1, 0, 0, 1);
    
    agg = new ArrayAverageFactory.ArrayAverage(false, POOLED);
    agg.accumulate(new long[] { });
    agg.accumulate(new long[] { });
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(0, agg.end());
    assertPooledArrayEquals(new double[] { }, agg.doubleArray(), 0.001);
    agg.close();
    
    // bad length
    agg = new ArrayAverageFactory.ArrayAverage(false, POOLED);
    agg.accumulate(new long[] { 42, -24, 0, 1 });
    try {
      agg.accumulate(new long[] { 1 });
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    agg.close();
  }
  
  @Test
  public void doubles() throws Exception {
    ArrayAverageFactory.ArrayAverage agg = 
        new ArrayAverageFactory.ArrayAverage(false, NON_POOLED);
    agg.accumulate(new double[] { 42, -24, 0, 1 });
    agg.accumulate(new double[] { 3, -13, 5, -1 });
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertArrayEquals(new double[] { 22.5, -18.5, 2.5, 0 }, agg.doubleArray(), 0.001);
    
    // non-infectious nans
    agg = new ArrayAverageFactory.ArrayAverage(false, NON_POOLED);
    agg.accumulate(new double[] { 42, -24, 0, Double.NaN });
    agg.accumulate(new double[] { 3, Double.NaN, 5, -1 });
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertArrayEquals(new double[] { 22.5, -24, 2.5, -1 }, agg.doubleArray(), 0.001);
    
    // infectious nans
    agg = new ArrayAverageFactory.ArrayAverage(true, NON_POOLED);
    agg.accumulate(new double[] { 42, -24, 0, Double.NaN });
    agg.accumulate(new double[] { 3, Double.NaN, 5, -1 });
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertArrayEquals(new double[] { 22.5, Double.NaN, 2.5, Double.NaN }, 
        agg.doubleArray(), 0.001);
    
    // bad length
    agg = new ArrayAverageFactory.ArrayAverage(false, NON_POOLED);
    agg.accumulate(new double[] { 42, -24, 0, 1 });
    try {
      agg.accumulate(new double[] { 1 });
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void doublesPooled() throws Exception {
    ArrayAverageFactory.ArrayAverage agg = 
        new ArrayAverageFactory.ArrayAverage(false, POOLED);
    agg.accumulate(new double[] { 42, -24, 0, 1 });
    agg.accumulate(new double[] { 3, -13, 5, -1 });
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertPooledArrayEquals(new double[] { 22.5, -18.5, 2.5, 0 }, agg.doubleArray(), 0.001);
    assertPoolCounters(INT_POOL, 1, 0, 0, 1);
    assertPoolCounters(LONG_POOL, 0, 0, 0, 0);
    assertPoolCounters(DOUBLE_POOL, 1, 0, 0, 0);
    
    agg.close();
    assertPoolCounters(INT_POOL, 1, 0, 0, 1);
    assertPoolCounters(LONG_POOL, 0, 0, 0, 0);
    assertPoolCounters(DOUBLE_POOL, 1, 0, 0, 1);
    
    // non-infectious nans
    agg = new ArrayAverageFactory.ArrayAverage(false, POOLED);
    agg.accumulate(new double[] { 42, -24, 0, Double.NaN });
    agg.accumulate(new double[] { 3, Double.NaN, 5, -1 });
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertPooledArrayEquals(new double[] { 22.5, -24, 2.5, -1 }, agg.doubleArray(), 0.001);
    agg.close();
    
    // infectious nans
    agg = new ArrayAverageFactory.ArrayAverage(true, POOLED);
    agg.accumulate(new double[] { 42, -24, 0, Double.NaN });
    agg.accumulate(new double[] { 3, Double.NaN, 5, -1 });
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertPooledArrayEquals(new double[] { 22.5, Double.NaN, 2.5, Double.NaN }, 
        agg.doubleArray(), 0.001);
    agg.close();
    
    // bad length
    agg = new ArrayAverageFactory.ArrayAverage(false, POOLED);
    agg.accumulate(new double[] { 42, -24, 0, 1 });
    try {
      agg.accumulate(new double[] { 1 });
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    agg.close();
  }
  
  @Test
  public void mixed() throws Exception {
    ArrayAverageFactory.ArrayAverage agg = 
        new ArrayAverageFactory.ArrayAverage(false, NON_POOLED);
    agg.accumulate(new long[] { 42, -24, 0, 1 });
    agg.accumulate(new double[] { 3, -13, 5, -1 });
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertArrayEquals(new double[] { 22.5, -18.5, 2.5, 0 }, agg.doubleArray(), 0.001);
    
    agg = new ArrayAverageFactory.ArrayAverage(false, NON_POOLED);
    agg.accumulate(new double[] { 3, -13, 5, -1 });
    agg.accumulate(new long[] { 42, -24, 0, 1 });
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertArrayEquals(new double[] { 22.5, -18.5, 2.5, 0 }, agg.doubleArray(), 0.001);
  }
  
  @Test
  public void mixedPooled() throws Exception {
    ArrayAverageFactory.ArrayAverage agg = 
        new ArrayAverageFactory.ArrayAverage(false, POOLED);
    agg.accumulate(new long[] { 42, -24, 0, 1 });
    agg.accumulate(new double[] { 3, -13, 5, -1 });
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertPooledArrayEquals(new double[] { 22.5, -18.5, 2.5, 0 }, agg.doubleArray(), 0.001);
    assertPoolCounters(INT_POOL, 1, 0, 0, 1);
    assertPoolCounters(LONG_POOL, 1, 0, 0, 1);
    assertPoolCounters(DOUBLE_POOL, 1, 0, 0, 0);
    
    agg.close();
    assertPoolCounters(INT_POOL, 1, 0, 0, 1);
    assertPoolCounters(LONG_POOL, 1, 0, 0, 1);
    assertPoolCounters(DOUBLE_POOL, 1, 0, 0, 1);
    
    agg = new ArrayAverageFactory.ArrayAverage(false, POOLED);
    agg.accumulate(new double[] { 3, -13, 5, -1 });
    agg.accumulate(new long[] { 42, -24, 0, 1 });
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertPooledArrayEquals(new double[] { 22.5, -18.5, 2.5, 0 }, agg.doubleArray(), 0.001);
    agg.close();
  }
  
  @Test
  public void accumulateDoublesIndexed() throws Exception {
    ArrayAverageFactory.ArrayAverage agg = 
        new ArrayAverageFactory.ArrayAverage(DefaultArrayAggregatorConfig
              .newBuilder()
              .setArraySize(4)
              .build(), 
            NON_POOLED);
    agg.accumulate(-24.0, 1);
    assertArrayEquals(new double[] { Double.NaN, -24, Double.NaN, Double.NaN }, 
        agg.double_accumulator, 0.001);
    assertArrayEquals(new int[] { 0, 1, 0, 0 }, agg.counts);
    
    agg.accumulate(42.0, 0);
    assertArrayEquals(new double[] { 42, -24, Double.NaN, Double.NaN }, 
        agg.double_accumulator, 0.001);
    assertArrayEquals(new int[] { 1, 1, 0, 0 }, agg.counts);
    
    agg.accumulate(3.0, 0);
    assertArrayEquals(new double[] { 45, -24, Double.NaN, Double.NaN }, 
        agg.double_accumulator, 0.001);
    assertArrayEquals(new int[] { 2, 1, 0, 0 }, agg.counts);
    
    agg.accumulate(Double.NaN, 1);
    assertArrayEquals(new double[] { 45, -24, Double.NaN, Double.NaN }, 
        agg.double_accumulator, 0.001);
    assertArrayEquals(new int[] { 2, 1, 0, 0 }, agg.counts);
    
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
    assertArrayEquals(new double[] { 45, -24, 5, -1 }, 
        agg.double_accumulator, 0.001);
    assertArrayEquals(new int[] { 2, 1, 2, 1 }, agg.counts);
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertArrayEquals(new double[] { 22.5, -24, 2.5, -1 }, agg.doubleArray(), 0.001);
    
    // no agg after reading
    try {
      agg.accumulate(6.5, 0);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    
    // no size so we can't accumulate.
    agg = new ArrayAverageFactory.ArrayAverage(false, NON_POOLED);
    try {
      agg.accumulate(6.5, 0);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
  }
  
  @Test
  public void accumulateDoublesIndexedPooled() throws Exception {
    ArrayAverageFactory.ArrayAverage agg = 
        new ArrayAverageFactory.ArrayAverage(DefaultArrayAggregatorConfig
              .newBuilder()
              .setArraySize(4)
              .build(), 
            POOLED);
    agg.accumulate(-24.0, 1);
    assertPooledArrayEquals(new double[] { Double.NaN, -24, Double.NaN, Double.NaN }, 
        agg.double_accumulator, 0.001);
    assertPooledArrayEquals(new int[] { 0, 1, 0, 0 }, agg.counts);
    
    agg.accumulate(42.0, 0);
    assertPooledArrayEquals(new double[] { 42, -24, Double.NaN, Double.NaN }, 
        agg.double_accumulator, 0.001);
    assertPooledArrayEquals(new int[] { 1, 1, 0, 0 }, agg.counts);
    
    agg.accumulate(3.0, 0);
    assertPooledArrayEquals(new double[] { 45, -24, Double.NaN, Double.NaN }, 
        agg.double_accumulator, 0.001);
    assertPooledArrayEquals(new int[] { 2, 1, 0, 0 }, agg.counts);
    
    agg.accumulate(Double.NaN, 1);
    assertPooledArrayEquals(new double[] { 45, -24, Double.NaN, Double.NaN }, 
        agg.double_accumulator, 0.001);
    assertPooledArrayEquals(new int[] { 2, 1, 0, 0 }, agg.counts);
    
    // others to match the doubles() UT.
    agg.accumulate(0.0, 2);
    agg.accumulate(Double.NaN, 3);
    agg.accumulate(5.0, 2);
    agg.accumulate(-1.0, 3);
    assertPooledArrayEquals(new double[] { 45, -24, 5, -1 }, 
        agg.double_accumulator, 0.001);
    assertPooledArrayEquals(new int[] { 2, 1, 2, 1 }, agg.counts);
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertPooledArrayEquals(new double[] { 22.5, -24, 2.5, -1 }, agg.doubleArray(), 0.001);
    
    // no agg after reading
    try {
      agg.accumulate(6.5, 0);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    
    agg.close();
  }
  
  @Test
  public void offsets() throws Exception {
    ArrayAverageFactory.ArrayAverage agg = 
        new ArrayAverageFactory.ArrayAverage(false, NON_POOLED);
    agg.accumulate(new long[] { 42, -24, 0, 1 }, 1, 3);
    agg.accumulate(new long[] { 3, -13, 5, -1 }, 1, 3);
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(2, agg.end());
    assertArrayEquals(new double[] { -18.5, 2.5 }, agg.doubleArray(), 0.001);
    
    agg = new ArrayAverageFactory.ArrayAverage(false, NON_POOLED);
    agg.accumulate(new double[] { 42, -24, 0, 1 }, 1, 3);
    agg.accumulate(new double[] { 3, -13, 5, -1 }, 1, 3);
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(2, agg.end());
    assertArrayEquals(new double[] { -18.5, 2.5 }, agg.doubleArray(), 0.001);
    
    agg = new ArrayAverageFactory.ArrayAverage(false, NON_POOLED);
    agg.accumulate(new long[] { 42, -24, 0, 1 }, 1, 3);
    agg.accumulate(new double[] { 3, -13, 5, -1 }, 1, 3);
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(2, agg.end());
    assertArrayEquals(new double[] { -18.5, 2.5 }, agg.doubleArray(), 0.001);
  }
  
  @Test
  public void offsetsPooled() throws Exception {
    ArrayAverageFactory.ArrayAverage agg = 
        new ArrayAverageFactory.ArrayAverage(false, POOLED);
    agg.accumulate(new long[] { 42, -24, 0, 1 }, 1, 3);
    agg.accumulate(new long[] { 3, -13, 5, -1 }, 1, 3);
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(2, agg.end());
    assertPooledArrayEquals(new double[] { -18.5, 2.5 }, agg.doubleArray(), 0.001);
    assertPoolCounters(INT_POOL, 1, 0, 0, 1);
    assertPoolCounters(LONG_POOL, 1, 0, 0, 1);
    assertPoolCounters(DOUBLE_POOL, 1, 0, 0, 0);
    
    agg.close();
    assertPoolCounters(INT_POOL, 1, 0, 0, 1);
    assertPoolCounters(LONG_POOL, 1, 0, 0, 1);
    assertPoolCounters(DOUBLE_POOL, 1, 0, 0, 1);
    
    agg = new ArrayAverageFactory.ArrayAverage(false, POOLED);
    agg.accumulate(new double[] { 42, -24, 0, 1 }, 1, 3);
    agg.accumulate(new double[] { 3, -13, 5, -1 }, 1, 3);
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(2, agg.end());
    assertPooledArrayEquals(new double[] { -18.5, 2.5 }, agg.doubleArray(), 0.001);
    agg.close();
    
    agg = new ArrayAverageFactory.ArrayAverage(false, POOLED);
    agg.accumulate(new long[] { 42, -24, 0, 1 }, 1, 3);
    agg.accumulate(new double[] { 3, -13, 5, -1 }, 1, 3);
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(2, agg.end());
    assertPooledArrayEquals(new double[] { -18.5, 2.5 }, agg.doubleArray(), 0.001);
    agg.close();
  }

  @Test
  public void singleNanDouble() throws Exception {
    ArrayAverageFactory.ArrayAverage agg = 
        new ArrayAverageFactory.ArrayAverage(false, NON_POOLED);
    agg.accumulate(new double[] { 3, Double.NaN, 5, -1 });
    assertArrayEquals(new double[] { 3, Double.NaN, 5, -1 }, agg.doubleArray(), 0.001);
  }
  
  @Test
  public void singleNanDoublePooled() throws Exception {
    ArrayAverageFactory.ArrayAverage agg = 
        new ArrayAverageFactory.ArrayAverage(false, POOLED);
    agg.accumulate(new double[] { 3, Double.NaN, 5, -1 });
    assertPooledArrayEquals(new double[] { 3, Double.NaN, 5, -1 }, agg.doubleArray(), 0.001);
    agg.close();
  }
  
  @Test
  public void nanThenReals() throws Exception {
    ArrayAverageFactory.ArrayAverage agg = 
        new ArrayAverageFactory.ArrayAverage(false, NON_POOLED);
    agg.accumulate(new double[] { 3, Double.NaN, 5, -1 });
    agg.accumulate(new double[] { 5, 2, Double.NaN, 2 });
    assertArrayEquals(new double[] { 4, 2, 5, 0.5 }, agg.doubleArray(), 0.001);
  }

  @Test
  public void combineDoubles() {
    ArrayAverageFactory.ArrayAverage agg1 = 
        new ArrayAverageFactory.ArrayAverage(false, NON_POOLED);
    agg1.accumulate(new double[] { 3, Double.NaN, 9, -1 });
    agg1.accumulate(new double[] { 4, 5, Double.NaN, -19 });

    ArrayAverageFactory.ArrayAverage agg2 = 
        new ArrayAverageFactory.ArrayAverage(false, NON_POOLED);
    agg2.accumulate(new double[] { 3, 8, Double.NaN, 2 });

    ArrayAverageFactory.ArrayAverage combiner = 
        new ArrayAverageFactory.ArrayAverage(false, NON_POOLED);
    combiner.combine(agg1);
    assertArrayEquals(new double[] {7, 5, 9, -20}, combiner.double_accumulator, 0.001);
    assertArrayEquals(new int[] {2, 1, 1, 2}, combiner.counts);

    combiner.combine(agg2);
    assertArrayEquals(new double[] {10, 13, 9, -18}, combiner.double_accumulator, 0.001);
    assertArrayEquals(new int[] {3, 2, 1, 3}, combiner.counts);

    assertArrayEquals(new double[] {3.3333333333333335, 6.5, 9, -6}, combiner.doubleArray(), 0.0001);
    agg1.close();
    agg2.close();
    combiner.close();
  }
  
  @Test
  public void combinePooledLongs() {
    ArrayAverageFactory.ArrayAverage agg1 = 
        new ArrayAverageFactory.ArrayAverage(false, POOLED);
    agg1.accumulate(new long[] { 42, -24, 0, 1 });
    agg1.accumulate(new long[] { 3, -13, 5, -1 });
    assertPoolCounters(INT_POOL, 1, 0, 0, 0);
    assertPoolCounters(LONG_POOL, 1, 0, 0, 0);
    assertPoolCounters(DOUBLE_POOL, 0, 0, 0, 0);

    before();
    ArrayAverageFactory.ArrayAverage agg2 = 
        new ArrayAverageFactory.ArrayAverage(false, POOLED);
    agg2.accumulate(new long[] { 9, -12, 6, 2 });
    assertPoolCounters(INT_POOL, 1, 0, 0, 0);
    assertPoolCounters(LONG_POOL, 1, 0, 0, 0);
    assertPoolCounters(DOUBLE_POOL, 0, 0, 0, 0);

    before();
    ArrayAverageFactory.ArrayAverage combiner = 
        new ArrayAverageFactory.ArrayAverage(false, POOLED);
    combiner.combine(agg1);
    assertPooledArrayEquals(new long[] {45, -37, 5, 0}, combiner.long_accumulator);
    assertPooledArrayEquals(new int[] {2, 2, 2, 2}, combiner.counts);
    assertPoolCounters(INT_POOL, 1, 0, 0, 0);
    assertPoolCounters(LONG_POOL, 1, 0, 0, 0);
    assertPoolCounters(DOUBLE_POOL, 0, 0, 0, 0);

    combiner.combine(agg2);
    assertPooledArrayEquals(new long[] {54, -49, 11, 2}, combiner.long_accumulator);
    assertPooledArrayEquals(new int[] {3, 3, 3, 3}, combiner.counts);
    assertPoolCounters(INT_POOL, 1, 0, 0, 0);
    assertPoolCounters(LONG_POOL, 1, 0, 0, 0);
    assertPoolCounters(DOUBLE_POOL, 0, 0, 0, 0);
    
    assertPooledArrayEquals(new double[] {18, -16.333, 3.666, 0.666}, combiner.doubleArray(), 0.001);
    assertPoolCounters(INT_POOL, 1, 0, 0, 1);
    assertPoolCounters(LONG_POOL, 1, 0, 0, 1);
    assertPoolCounters(DOUBLE_POOL, 1, 0, 0, 0);
    agg1.close();
    agg2.close();
    combiner.close();
  }
  
  @Test
  public void combinePooledDoubles() {
    ArrayAverageFactory.ArrayAverage agg1 = 
        new ArrayAverageFactory.ArrayAverage(false, POOLED);
    agg1.accumulate(new double[] { 3, Double.NaN, 9, -1 });
    agg1.accumulate(new double[] { 4, 5, Double.NaN, -19 });
    assertPoolCounters(INT_POOL, 1, 0, 0, 0);
    assertPoolCounters(LONG_POOL, 0, 0, 0, 0);
    assertPoolCounters(DOUBLE_POOL, 1, 0, 0, 0);

    before();
    ArrayAverageFactory.ArrayAverage agg2 = 
        new ArrayAverageFactory.ArrayAverage(false, POOLED);
    agg2.accumulate(new double[] { 3, 8, Double.NaN, 2 });
    assertPoolCounters(INT_POOL, 1, 0, 0, 0);
    assertPoolCounters(LONG_POOL, 0, 0, 0, 0);
    assertPoolCounters(DOUBLE_POOL, 1, 0, 0, 0);

    before();
    ArrayAverageFactory.ArrayAverage combiner = 
        new ArrayAverageFactory.ArrayAverage(false, POOLED);
    combiner.combine(agg1);
    assertPooledArrayEquals(new double[] {7, 5, 9, -20}, combiner.double_accumulator, 0.001);
    assertPooledArrayEquals(new int[] {2, 1, 1, 2}, combiner.counts);
    assertPoolCounters(INT_POOL, 1, 0, 0, 0);
    assertPoolCounters(LONG_POOL, 0, 0, 0, 0);
    assertPoolCounters(DOUBLE_POOL, 1, 0, 0, 0);
    
    combiner.combine(agg2);
    assertPooledArrayEquals(new double[] {10, 13, 9, -18}, combiner.double_accumulator, 0.001);
    assertPooledArrayEquals(new int[] {3, 2, 1, 3}, combiner.counts);
    assertPoolCounters(INT_POOL, 1, 0, 0, 0);
    assertPoolCounters(LONG_POOL, 0, 0, 0, 0);
    assertPoolCounters(DOUBLE_POOL, 1, 0, 0, 0);
    
    assertPooledArrayEquals(new double[] {3.3333333333333335, 6.5, 9, -6}, combiner.doubleArray(), 0.0001);
    assertPoolCounters(INT_POOL, 1, 0, 0, 1);
    assertPoolCounters(LONG_POOL, 0, 0, 0, 0);
    assertPoolCounters(DOUBLE_POOL, 1, 0, 0, 0);
    agg1.close();
    agg2.close();
    combiner.close();
  }
  
  @Test
  public void combinePooledLongsThenDoubles() {
    ArrayAverageFactory.ArrayAverage agg1 = 
        new ArrayAverageFactory.ArrayAverage(false, POOLED);
    agg1.accumulate(new long[] { 42, -24, 0, 1 });
    agg1.accumulate(new long[] { 3, -13, 5, -1 });
    assertPoolCounters(INT_POOL, 1, 0, 0, 0);
    assertPoolCounters(LONG_POOL, 1, 0, 0, 0);
    assertPoolCounters(DOUBLE_POOL, 0, 0, 0, 0);
    
    before();
    ArrayAverageFactory.ArrayAverage agg2 = 
        new ArrayAverageFactory.ArrayAverage(false, POOLED);
    agg2.accumulate(new double[] { 3, 8, Double.NaN, 2 });
    assertPoolCounters(INT_POOL, 1, 0, 0, 0);
    assertPoolCounters(LONG_POOL, 0, 0, 0, 0);
    assertPoolCounters(DOUBLE_POOL, 1, 0, 0, 0);
    
    before();
    ArrayAverageFactory.ArrayAverage combiner = 
        new ArrayAverageFactory.ArrayAverage(false, POOLED);
    combiner.combine(agg1);
    assertPooledArrayEquals(new long[] {45, -37, 5, 0}, combiner.long_accumulator);
    assertPooledArrayEquals(new int[] {2, 2, 2, 2}, combiner.counts);
    assertPoolCounters(INT_POOL, 1, 0, 0, 0);
    assertPoolCounters(LONG_POOL, 1, 0, 0, 0);
    assertPoolCounters(DOUBLE_POOL, 0, 0, 0, 0);
    
    combiner.combine(agg2);
    assertPooledArrayEquals(new double[] {48, -29, 5, 2}, combiner.double_accumulator, 0.001);
    assertPooledArrayEquals(new int[] {3, 3, 2, 3}, combiner.counts);
    assertPoolCounters(INT_POOL, 1, 0, 0, 0);
    assertPoolCounters(LONG_POOL, 1, 0, 0, 1);
    assertPoolCounters(DOUBLE_POOL, 1, 0, 0, 0);
    
    assertPooledArrayEquals(new double[] {16, -9.6666, 2.5, 0.6666}, combiner.doubleArray(), 0.0001);
    assertPoolCounters(INT_POOL, 1, 0, 0, 1);
    assertPoolCounters(LONG_POOL, 1, 0, 0, 1);
    assertPoolCounters(DOUBLE_POOL, 1, 0, 0, 0);
    agg1.close();
    agg2.close();
    combiner.close();
  }

  @Test
  public void combineWithInfectiousNan() {
    ArrayAverageFactory.ArrayAverage agg1 = 
        new ArrayAverageFactory.ArrayAverage(true, NON_POOLED);
    agg1.accumulate(new double[] { 3, Double.NaN, 9, -1 });
    agg1.accumulate(new double[] { 4, 5, Double.NaN, -19 });

    ArrayAverageFactory.ArrayAverage agg2 = 
        new ArrayAverageFactory.ArrayAverage(true, NON_POOLED);
    agg2.accumulate(new double[] { 3, 8, Double.NaN, 2 });

    ArrayAverageFactory.ArrayAverage combiner = 
        new ArrayAverageFactory.ArrayAverage(true, NON_POOLED);
    combiner.combine(agg1);
    assertArrayEquals(new double[] {7, Double.NaN, Double.NaN, -20}, combiner.double_accumulator, 0.001);
    assertArrayEquals(new int[] {2, 0, 0, 2}, combiner.counts);

    combiner.combine(agg2);
    assertArrayEquals(new double[] {10, Double.NaN, Double.NaN, -18}, combiner.double_accumulator, 0.001);
    assertArrayEquals(new int[] {3, 0, 0, 3}, combiner.counts);

    assertArrayEquals(new double[] {3.3333333333333335, Double.NaN, Double.NaN, -6}, combiner.doubleArray(), 0.0001);
    agg1.close();
    agg2.close();
    combiner.close();
  }

  @Test
  public void combineWithInfectiousNanPooled() {
    ArrayAverageFactory.ArrayAverage agg1 = 
        new ArrayAverageFactory.ArrayAverage(true, POOLED);
    agg1.accumulate(new double[] { 3, Double.NaN, 9, -1 });
    agg1.accumulate(new double[] { 4, 5, Double.NaN, -19 });
    
    ArrayAverageFactory.ArrayAverage agg2 = 
        new ArrayAverageFactory.ArrayAverage(true, POOLED);
    agg2.accumulate(new double[] { 3, 8, Double.NaN, 2 });
    
    ArrayAverageFactory.ArrayAverage combiner = 
        new ArrayAverageFactory.ArrayAverage(true, POOLED);
    combiner.combine(agg1);
    assertPooledArrayEquals(new double[] {7, Double.NaN, Double.NaN, -20}, combiner.double_accumulator, 0.001);
    assertPooledArrayEquals(new int[] {2, 0, 0, 2}, combiner.counts);

    combiner.combine(agg2);
    assertPooledArrayEquals(new double[] {10, Double.NaN, Double.NaN, -18}, combiner.double_accumulator, 0.001);
    assertPooledArrayEquals(new int[] {3, 0, 0, 3}, combiner.counts);

    assertPooledArrayEquals(new double[] {3.3333333333333335, Double.NaN, Double.NaN, -6}, combiner.doubleArray(), 0.0001);
    assertPoolCounters(INT_POOL, 3, 0, 0, 1);
    assertPoolCounters(LONG_POOL, 0, 0, 0, 0);
    assertPoolCounters(DOUBLE_POOL, 3, 0, 0, 0);
    agg1.close();
    agg2.close();
    combiner.close();
  }
  
  @Test
  public void poolExhausted() throws Exception {
    List<ArrayAverageFactory.ArrayAverage> aggs = Lists.newArrayListWithCapacity(4);
    for (int i = 0; i < 4; i++) {
      ArrayAverageFactory.ArrayAverage agg = 
          new ArrayAverageFactory.ArrayAverage(false, POOLED);
      agg.accumulate(new double[] { 42, -24, 0, 1 });
      agg.accumulate(new double[] { 3, -13, 5, -1 });
      aggs.add(agg);
    }
    
    assertPoolCounters(INT_POOL, 4, 0, 0, 0);
    assertPoolCounters(LONG_POOL, 0, 0, 0, 0);
    assertPoolCounters(DOUBLE_POOL, 4, 0, 0, 0);
    
    ArrayAverageFactory.ArrayAverage agg = 
        new ArrayAverageFactory.ArrayAverage(false, POOLED);
    agg.accumulate(new double[] { 42, -24, 0, 1 });
    agg.accumulate(new double[] { 3, -13, 5, -1 });
    
    assertPoolCounters(INT_POOL, 4, 1, 0, 0);
    assertPoolCounters(LONG_POOL, 0, 0, 0, 0);
    assertPoolCounters(DOUBLE_POOL, 4, 1, 0, 0);
    
    for (ArrayAverageFactory.ArrayAverage a : aggs) {
      a.close();
    }
    assertPoolCounters(INT_POOL, 4, 1, 0, 4);
    assertPoolCounters(LONG_POOL, 0, 0, 0, 0);
    assertPoolCounters(DOUBLE_POOL, 4, 1, 0, 4);
    
    agg.close();
    assertPoolCounters(INT_POOL, 4, 1, 0, 4);
    assertPoolCounters(LONG_POOL, 0, 0, 0, 0);
    assertPoolCounters(DOUBLE_POOL, 4, 1, 0, 4);
  }
  
}