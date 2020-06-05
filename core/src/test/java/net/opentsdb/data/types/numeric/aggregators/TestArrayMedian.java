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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import net.opentsdb.pools.DefaultObjectPoolConfig;
import net.opentsdb.pools.IntArrayPool;
import net.opentsdb.pools.MockArrayObjectPool;

public class TestArrayMedian extends BaseTestNumericArray {
  
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
    ArrayMedianFactory.ArrayMedian agg = 
        new ArrayMedianFactory.ArrayMedian(false, NON_POOLED);
    for (int i = 0; i < LONGS.length; i++) {
      agg.accumulate(LONGS[i]);
    }
    
    assertTrue(agg.isInteger());
    assertNull(agg.double_accumulator);
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertArrayEquals(new long[] { 512, 513, 514, 515 }, agg.longArray());
    
    agg = new ArrayMedianFactory.ArrayMedian(false, NON_POOLED);
    agg.accumulate(new long[] { });
    agg.accumulate(new long[] { });
    
    assertTrue(agg.isInteger());
    assertNull(agg.double_accumulator);
    assertEquals(0, agg.offset());
    assertEquals(0, agg.end());
    assertArrayEquals(new long[] { }, agg.longArray());
    
    // bad length
    agg = new ArrayMedianFactory.ArrayMedian(false, NON_POOLED);
    agg.accumulate(new long[] { 42, -24, 0, 1 });
    try {
      agg.accumulate(new long[] { 1 });
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void longsPooled() {
    ArrayMedianFactory.ArrayMedian agg = 
        new ArrayMedianFactory.ArrayMedian(false, POOLED);
    for (int i = 0; i < LONGS.length; i++) {
      agg.accumulate(LONGS[i]);
    }
    
    assertTrue(agg.isInteger());
    assertNull(agg.double_accumulator);
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertPooledArrayEquals(new long[] { 512, 513, 514, 515 }, agg.longArray());
    assertPoolCounters(INT_POOL, 1, 0, 0, 0);
    assertPoolCounters(LONG_POOL, 1, 0, 0, 0);
    assertPoolCounters(DOUBLE_POOL, 0, 0, 0, 0);
    
    // same, no-op
    assertPooledArrayEquals(new long[] { 512, 513, 514, 515 }, agg.longArray());
    agg.close();
    assertPoolCounters(INT_POOL, 1, 0, 0, 1);
    assertPoolCounters(LONG_POOL, 1, 0, 0, 1);
    assertPoolCounters(DOUBLE_POOL, 0, 0, 0, 0);
    
    agg = new ArrayMedianFactory.ArrayMedian(false, POOLED);
    agg.accumulate(new long[] { });
    agg.accumulate(new long[] { });
    
    assertTrue(agg.isInteger());
    assertEquals(0, agg.offset());
    assertEquals(0, agg.end());
    assertPooledArrayEquals(new long[] { }, agg.longArray());
    agg.close();
    
    // bad length
    agg = new ArrayMedianFactory.ArrayMedian(false, POOLED);
    agg.accumulate(new long[] { 42, -24, 0, 1 });
    try {
      agg.accumulate(new long[] { 1 });
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    agg.close();
  }
  
  @Test
  public void doubles() throws Exception {
    // non-infectious nans
    ArrayMedianFactory.ArrayMedian agg = 
        new ArrayMedianFactory.ArrayMedian(false, NON_POOLED);
    for (int i = 0; i < DOUBLES.length; i++) {
      agg.accumulate(DOUBLES[i]);
    }
    
    assertFalse(agg.isInteger());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertArrayEquals(new double[] { 512, 513, 514, 515 }, agg.doubleArray(), 0.001);
    
    // infectious nans
    agg = new ArrayMedianFactory.ArrayMedian(true, NON_POOLED);
    for (int i = 0; i < DOUBLES.length; i++) {
      agg.accumulate(DOUBLES[i]);
    }
    
    assertFalse(agg.isInteger());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertArrayEquals(new double[] { 512, 513, Double.NaN, 515 }, agg.doubleArray(), 0.001);
    
    // bad length
    agg = new ArrayMedianFactory.ArrayMedian(false, NON_POOLED);
    agg.accumulate(new double[] { 42, -24, 0, 1 });
    try {
      agg.accumulate(new double[] { 1 });
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void doublesPooled() throws Exception {
    ArrayMedianFactory.ArrayMedian agg = 
        new ArrayMedianFactory.ArrayMedian(false, POOLED);
    for (int i = 0; i < DOUBLES.length; i++) {
      agg.accumulate(DOUBLES[i]);
    }
    
    assertFalse(agg.isInteger());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertPooledArrayEquals(new double[] { 512, 513, 514, 515 }, agg.doubleArray(), 0.001);
    assertPoolCounters(INT_POOL, 1, 0, 0, 0);
    assertPoolCounters(LONG_POOL, 0, 0, 0, 0);
    assertPoolCounters(DOUBLE_POOL, 1, 0, 0, 0);
    
    agg.close();
    assertPoolCounters(INT_POOL, 1, 0, 0, 1);
    assertPoolCounters(LONG_POOL, 0, 0, 0, 0);
    assertPoolCounters(DOUBLE_POOL, 1, 0, 0, 1);
    
    // infectious nans
    agg = new ArrayMedianFactory.ArrayMedian(true, POOLED);
    for (int i = 0; i < DOUBLES.length; i++) {
      agg.accumulate(DOUBLES[i]);
    }
    
    assertFalse(agg.isInteger());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertPooledArrayEquals(new double[] { 512, 513, Double.NaN, 515 }, agg.doubleArray(), 0.001);
    agg.close();
    
    // bad length
    agg = new ArrayMedianFactory.ArrayMedian(false, POOLED);
    agg.accumulate(new double[] { 42, -24, 0, 1 });
    try {
      agg.accumulate(new double[] { 1 });
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    agg.close();
  }
  
  @Test
  public void mixed() throws Exception {
    ArrayMedianFactory.ArrayMedian agg = 
        new ArrayMedianFactory.ArrayMedian(false, NON_POOLED);
    for (int i = 0; i < MIXED.length; i++) {
      if (i % 2 == 0) {
        agg.accumulate((long[]) MIXED[i]);
      } else {
        agg.accumulate((double[]) MIXED[i]);
      }
    }
    
    assertFalse(agg.isInteger());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertArrayEquals(new double[] { 512, 513, 514, 515 }, agg.doubleArray(), 0.001);
  }
  
  @Test
  public void mixedPooled() throws Exception {
    ArrayMedianFactory.ArrayMedian agg = 
        new ArrayMedianFactory.ArrayMedian(false, POOLED);
    for (int i = 0; i < MIXED.length; i++) {
      if (i % 2 == 0) {
        agg.accumulate((long[]) MIXED[i]);
      } else {
        agg.accumulate((double[]) MIXED[i]);
      }
    }
    
    assertFalse(agg.isInteger());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertPooledArrayEquals(new double[] { 512, 513, 514, 515 }, agg.doubleArray(), 0.001);
    assertPoolCounters(INT_POOL, 1, 0, 0, 0);
    assertPoolCounters(LONG_POOL, 0, 0, 0, 0);
    assertPoolCounters(DOUBLE_POOL, 1, 0, 0, 0);
    
    agg.close();
    assertPoolCounters(INT_POOL, 1, 0, 0, 1);
    assertPoolCounters(LONG_POOL, 0, 0, 0, 0);
    assertPoolCounters(DOUBLE_POOL, 1, 0, 0, 1);
  }
  
  @Test
  public void accumulateDoublesIndexed() throws Exception {
    ArrayMedianFactory.ArrayMedian agg = 
        new ArrayMedianFactory.ArrayMedian(DefaultArrayAggregatorConfig
              .newBuilder()
              .setArraySize(4)
              .build(), 
            NON_POOLED);
    agg.accumulate(-24.0, 1);
    agg.accumulate(42.0, 0);
    agg.accumulate(3.0, 0);
    agg.accumulate(Double.NaN, 1);
    
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
    
    assertFalse(agg.isInteger());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertArrayEquals(new double[] { 42, -24, 5, -1 }, agg.doubleArray(), 0.001);
    
    // no size so we can't accumulate.
    agg = new ArrayMedianFactory.ArrayMedian(false, NON_POOLED);
    try {
      agg.accumulate(6.5, 0);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
  }
  
  @Test
  public void combineLong() {
    ArrayMedianFactory.ArrayMedian agg1 = 
        new ArrayMedianFactory.ArrayMedian(false, NON_POOLED);
    agg1.accumulate(new long[] { 3, 2, 9, -1 });
    agg1.accumulate(new long[] { 4, 5, 8, -19 });

    ArrayMedianFactory.ArrayMedian agg2 = 
        new ArrayMedianFactory.ArrayMedian(false, NON_POOLED);
    agg2.accumulate(new long[] { 3, 8, 0, 2 });

    ArrayMedianFactory.ArrayMedian combiner = 
        new ArrayMedianFactory.ArrayMedian(false, NON_POOLED);
    combiner.combine(agg1);
    combiner.combine(agg2);

    assertArrayEquals(new long[] {3, 5, 8, -1}, combiner.longArray());
    agg1.close();
    agg2.close();
    combiner.close();
  }
  
  @Test
  public void combineDoubles() {
    ArrayMedianFactory.ArrayMedian agg1 = 
        new ArrayMedianFactory.ArrayMedian(false, NON_POOLED);
    agg1.accumulate(new double[] { 3, Double.NaN, 9, -1 });
    agg1.accumulate(new double[] { 4, 5, Double.NaN, -19 });

    ArrayMedianFactory.ArrayMedian agg2 = 
        new ArrayMedianFactory.ArrayMedian(false, NON_POOLED);
    agg2.accumulate(new double[] { 3, 8, Double.NaN, 2 });

    ArrayMedianFactory.ArrayMedian combiner = 
        new ArrayMedianFactory.ArrayMedian(false, NON_POOLED);
    combiner.combine(agg1);
    combiner.combine(agg2);

    assertArrayEquals(new double[] {3, 8, 9, -1}, combiner.doubleArray(), 0.0001);
    agg1.close();
    agg2.close();
    combiner.close();
  }

  @Test
  public void combineLongThenDouble() {
    ArrayMedianFactory.ArrayMedian agg1 = 
        new ArrayMedianFactory.ArrayMedian(false, NON_POOLED);
    agg1.accumulate(new long[] { 3, 2, 9, -1 });
    agg1.accumulate(new long[] { 4, 5, 8, -19 });

    ArrayMedianFactory.ArrayMedian agg2 = 
        new ArrayMedianFactory.ArrayMedian(false, NON_POOLED);
    agg2.accumulate(new double[] { 3, 8, Double.NaN, 2 });

    ArrayMedianFactory.ArrayMedian combiner = 
        new ArrayMedianFactory.ArrayMedian(false, NON_POOLED);
    combiner.combine(agg1);
    combiner.combine(agg2);

    assertArrayEquals(new double[] {3, 5, 9, -1}, combiner.doubleArray(), 0.001);
    agg1.close();
    agg2.close();
    combiner.close();
  }
  
  @Test
  public void combineDoublesThenLong() {
    ArrayMedianFactory.ArrayMedian agg1 = 
        new ArrayMedianFactory.ArrayMedian(false, NON_POOLED);
    agg1.accumulate(new double[] { 3, Double.NaN, 9, -1 });
    agg1.accumulate(new double[] { 4, 5, Double.NaN, -19 });

    ArrayMedianFactory.ArrayMedian agg2 = 
        new ArrayMedianFactory.ArrayMedian(false, NON_POOLED);
    agg2.accumulate(new long[] { 3, 8, 0, 2 });

    ArrayMedianFactory.ArrayMedian combiner = 
        new ArrayMedianFactory.ArrayMedian(false, NON_POOLED);
    combiner.combine(agg1);
    combiner.combine(agg2);

    assertArrayEquals(new double[] {3, 8, 9, -1}, combiner.doubleArray(), 0.0001);
    agg1.close();
    agg2.close();
    combiner.close();
  }
  
  @Test
  public void combineDoublesThenLongWithNan() {
    ArrayMedianFactory.ArrayMedian agg1 = 
        new ArrayMedianFactory.ArrayMedian(false, NON_POOLED);
    agg1.accumulate(new double[] { 3, Double.NaN, 9, -1 });
    agg1.accumulate(new double[] { 4, Double.NaN, 5, -19 });

    ArrayMedianFactory.ArrayMedian agg2 = 
        new ArrayMedianFactory.ArrayMedian(false, NON_POOLED);
    agg2.accumulate(new long[] { 3, 8, 0, 2 });

    ArrayMedianFactory.ArrayMedian combiner = 
        new ArrayMedianFactory.ArrayMedian(false, NON_POOLED);
    combiner.combine(agg1);
    combiner.combine(agg2);

    assertArrayEquals(new double[] {3, 8, 5, -1}, combiner.doubleArray(), 0.0001);
    agg1.close();
    agg2.close();
    combiner.close();
  }
  
  @Test
  public void combineWithInfectiousNan() {
    ArrayMedianFactory.ArrayMedian agg1 = 
        new ArrayMedianFactory.ArrayMedian(true, NON_POOLED);
    agg1.accumulate(new double[] { 3, Double.NaN, 9, -1 });
    agg1.accumulate(new double[] { 4, 5, Double.NaN, -19 });

    ArrayMedianFactory.ArrayMedian agg2 = 
        new ArrayMedianFactory.ArrayMedian(true, NON_POOLED);
    agg2.accumulate(new double[] { 3, 8, Double.NaN, 2 });

    ArrayMedianFactory.ArrayMedian combiner = 
        new ArrayMedianFactory.ArrayMedian(true, NON_POOLED);
    combiner.combine(agg1);
    combiner.combine(agg2);
    assertArrayEquals(new double[] {3, Double.NaN, Double.NaN, -1}, combiner.doubleArray(), 0.0001);
    agg1.close();
    agg2.close();
    combiner.close();
  }
  
}