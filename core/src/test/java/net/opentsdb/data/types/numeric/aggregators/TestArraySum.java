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

import org.junit.Test;

public class TestArraySum extends BaseTestNumericArray {
  
  @Test
  public void longs() {
    ArraySumFactory.ArraySum agg = new ArraySumFactory.ArraySum(false, NON_POOLED);
    agg.accumulate(new long[] { 42, -24, 0, 1 });
    agg.accumulate(new long[] { 3, -13, 5, -1 });
    
    assertTrue(agg.isInteger());
    assertNull(agg.doubleArray());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertArrayEquals(new long[] { 45, -37, 5, 0 }, agg.longArray());
    
    agg = new ArraySumFactory.ArraySum(false, NON_POOLED);
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
    ArraySumFactory.ArraySum agg = new ArraySumFactory.ArraySum(false, POOLED);
    agg.accumulate(new long[] { 42, -24, 0, 1 });
    agg.accumulate(new long[] { 3, -13, 5, -1 });
    
    assertTrue(agg.isInteger());
    assertNull(agg.doubleArray());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertPooledArrayEquals(new long[] { 45, -37, 5, 0 }, agg.longArray());
    assertPoolCounters(LONG_POOL, 1, 0, 0, 0);
    assertPoolCounters(DOUBLE_POOL, 0, 0, 0, 0);
    agg.close();
    assertPoolCounters(LONG_POOL, 1, 0, 0, 1);
    assertPoolCounters(DOUBLE_POOL, 0, 0, 0, 0);
    
    before();
    agg = new ArraySumFactory.ArraySum(false, POOLED);
    agg.accumulate(new long[] { });
    agg.accumulate(new long[] { });
    
    assertTrue(agg.isInteger());
    assertNull(agg.doubleArray());
    assertEquals(0, agg.offset());
    assertEquals(0, agg.end());
    assertPooledArrayEquals(new long[] { }, agg.longArray());
    assertPoolCounters(LONG_POOL, 1, 0, 0, 0);
    assertPoolCounters(DOUBLE_POOL, 0, 0, 0, 0);
    
    // bad length
    try {
      agg.accumulate(new long[] { 1 });
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    agg.close();
    assertPoolCounters(LONG_POOL, 1, 0, 0, 1);
    assertPoolCounters(DOUBLE_POOL, 0, 0, 0, 0);
  }
  
  @Test
  public void doubles() throws Exception {
    ArraySumFactory.ArraySum agg = new ArraySumFactory.ArraySum(false, NON_POOLED);
    agg.accumulate(new double[] { 42, -24, 0, 1 });
    agg.accumulate(new double[] { 3, -13, 5, -1 });
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertArrayEquals(new double[] { 45, -37, 5, 0 }, agg.doubleArray(), 0.001);
    
    // non-infectious nans
    agg = new ArraySumFactory.ArraySum(false, NON_POOLED);
    agg.accumulate(new double[] { 42, -24, 0, Double.NaN });
    agg.accumulate(new double[] { 3, Double.NaN, 5, -1 });
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertArrayEquals(new double[] { 45, -24, 5, -1 }, agg.doubleArray(), 0.001);
    
    // infectious nans
    agg = new ArraySumFactory.ArraySum(true, NON_POOLED);
    agg.accumulate(new double[] { 42, -24, 0, Double.NaN });
    agg.accumulate(new double[] { 3, Double.NaN, 5, -1 });
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertArrayEquals(new double[] { 45, Double.NaN, 5, Double.NaN }, 
        agg.doubleArray(), 0.001);
    
    // bad length
    try {
      agg.accumulate(new double[] { 1 });
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void doublesPooled() throws Exception {
    ArraySumFactory.ArraySum agg = new ArraySumFactory.ArraySum(false, POOLED);
    agg.accumulate(new double[] { 42, -24, 0, 1 });
    agg.accumulate(new double[] { 3, -13, 5, -1 });
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertPooledArrayEquals(new double[] { 45, -37, 5, 0 }, agg.doubleArray(), 0.001);
    assertPoolCounters(LONG_POOL, 0, 0, 0, 0);
    assertPoolCounters(DOUBLE_POOL, 1, 0, 0, 0);
    agg.close();
    assertPoolCounters(LONG_POOL, 0, 0, 0, 0);
    assertPoolCounters(DOUBLE_POOL, 1, 0, 0, 1);
    
    // non-infectious nans
    before();
    agg = new ArraySumFactory.ArraySum(false, POOLED);
    agg.accumulate(new double[] { 42, -24, 0, Double.NaN });
    agg.accumulate(new double[] { 3, Double.NaN, 5, -1 });
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertPooledArrayEquals(new double[] { 45, -24, 5, -1 }, agg.doubleArray(), 0.001);
    assertPoolCounters(LONG_POOL, 0, 0, 0, 0);
    assertPoolCounters(DOUBLE_POOL, 1, 0, 0, 0);
    agg.close();
    
    // infectious nans
    before();
    agg = new ArraySumFactory.ArraySum(true, POOLED);
    agg.accumulate(new double[] { 42, -24, 0, Double.NaN });
    agg.accumulate(new double[] { 3, Double.NaN, 5, -1 });
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertPooledArrayEquals(new double[] { 45, Double.NaN, 5, Double.NaN }, 
        agg.doubleArray(), 0.001);
    
    // bad length
    try {
      agg.accumulate(new double[] { 1 });
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    assertPoolCounters(LONG_POOL, 0, 0, 0, 0);
    assertPoolCounters(DOUBLE_POOL, 1, 0, 0, 0);
    agg.close();
  }
  
  @Test
  public void mixed() throws Exception {
    ArraySumFactory.ArraySum agg = new ArraySumFactory.ArraySum(false, NON_POOLED);
    agg.accumulate(new long[] { 42, -24, 0, 1 });
    agg.accumulate(new double[] { 3, -13, 5, -1 });
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertArrayEquals(new double[] { 45, -37, 5, 0 }, agg.doubleArray(), 0.001);
    
    agg = new ArraySumFactory.ArraySum(false, NON_POOLED);
    agg.accumulate(new double[] { 3, -13, 5, -1 });
    agg.accumulate(new long[] { 42, -24, 0, 1 });
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertArrayEquals(new double[] { 45, -37, 5, 0 }, agg.doubleArray(), 0.001);
  }
  
  @Test
  public void mixedPooled() throws Exception {
    ArraySumFactory.ArraySum agg = new ArraySumFactory.ArraySum(false, POOLED);
    agg.accumulate(new long[] { 42, -24, 0, 1 });
    agg.accumulate(new double[] { 3, -13, 5, -1 });
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertPooledArrayEquals(new double[] { 45, -37, 5, 0 }, agg.doubleArray(), 0.001);
    assertPoolCounters(LONG_POOL, 1, 0, 0, 1);
    assertPoolCounters(DOUBLE_POOL, 1, 0, 0, 0);
    agg.close();
    assertPoolCounters(LONG_POOL, 1, 0, 0, 1);
    assertPoolCounters(DOUBLE_POOL, 1, 0, 0, 1);
    
    before();
    agg = new ArraySumFactory.ArraySum(false, POOLED);
    agg.accumulate(new double[] { 3, -13, 5, -1 });
    agg.accumulate(new long[] { 42, -24, 0, 1 });
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertPooledArrayEquals(new double[] { 45, -37, 5, 0 }, agg.doubleArray(), 0.001);
    assertPoolCounters(LONG_POOL, 0, 0, 0, 0);
    assertPoolCounters(DOUBLE_POOL, 1, 0, 0, 0);
    agg.close();
  }
  
  @Test
  public void accumulateDoublesIndexed() throws Exception {
    ArraySumFactory.ArraySum agg = 
        new ArraySumFactory.ArraySum(DefaultArrayAggregatorConfig
          .newBuilder()
          .setArraySize(4)
          .build(), 
        NON_POOLED);
    agg.accumulate(-24.0, 1);
    assertArrayEquals(new double[] { Double.NaN, -24, Double.NaN, Double.NaN }, 
        agg.double_accumulator, 0.001);
    
    agg.accumulate(42.0, 0);
    assertArrayEquals(new double[] { 42, -24, Double.NaN, Double.NaN }, 
        agg.double_accumulator, 0.001);
    
    agg.accumulate(3.0, 0);
    assertArrayEquals(new double[] { 45, -24, Double.NaN, Double.NaN }, 
        agg.double_accumulator, 0.001);
    
    agg.accumulate(Double.NaN, 1);
    assertArrayEquals(new double[] { 45, -24, Double.NaN, Double.NaN }, 
        agg.double_accumulator, 0.001);
    
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
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertArrayEquals(new double[] { 45, -24, 5, -1 }, agg.doubleArray(), 0.001);
    
    // no size so we can't accumulate.
    agg = new ArraySumFactory.ArraySum(false, NON_POOLED);
    try {
      agg.accumulate(6.5, 0);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
  }
  
  @Test
  public void accumulateDoublesIndexedPooled() throws Exception {
    ArraySumFactory.ArraySum agg = 
        new ArraySumFactory.ArraySum(DefaultArrayAggregatorConfig
          .newBuilder()
          .setArraySize(4)
          .build(), 
        POOLED);
    agg.accumulate(-24.0, 1);
    assertPooledArrayEquals(new double[] { Double.NaN, -24, Double.NaN, Double.NaN }, 
        agg.double_accumulator, 0.001);
    
    agg.accumulate(42.0, 0);
    assertPooledArrayEquals(new double[] { 42, -24, Double.NaN, Double.NaN }, 
        agg.double_accumulator, 0.001);
    
    agg.accumulate(3.0, 0);
    assertPooledArrayEquals(new double[] { 45, -24, Double.NaN, Double.NaN }, 
        agg.double_accumulator, 0.001);
    
    agg.accumulate(Double.NaN, 1);
    assertPooledArrayEquals(new double[] { 45, -24, Double.NaN, Double.NaN }, 
        agg.double_accumulator, 0.001);
    
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
    assertPooledArrayEquals(new double[] { 45, -24, 5, -1 }, 
        agg.double_accumulator, 0.001);
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertPooledArrayEquals(new double[] { 45, -24, 5, -1 }, agg.doubleArray(), 0.001);
    agg.close();
    
    // no size so we can't accumulate.
    agg = new ArraySumFactory.ArraySum(false, POOLED);
    try {
      agg.accumulate(6.5, 0);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    agg.close();
  }
  
  @Test
  public void offsets() throws Exception {
    ArraySumFactory.ArraySum agg = new ArraySumFactory.ArraySum(false, NON_POOLED);
    agg.accumulate(new long[] { 42, -24, 0, 1 }, 1, 3);
    agg.accumulate(new long[] { 3, -13, 5, -1 }, 1, 3);
    
    assertTrue(agg.isInteger());
    assertNull(agg.doubleArray());
    assertEquals(0, agg.offset());
    assertEquals(2, agg.end());
    assertArrayEquals(new long[] { -37, 5 }, agg.longArray());
    
    agg = new ArraySumFactory.ArraySum(false, NON_POOLED);
    agg.accumulate(new double[] { 42, -24, 0, 1 }, 1, 3);
    agg.accumulate(new double[] { 3, -13, 5, -1 }, 1, 3);
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(2, agg.end());
    assertArrayEquals(new double[] { -37, 5 }, agg.doubleArray(), 0.001);
    
    agg = new ArraySumFactory.ArraySum(false, NON_POOLED);
    agg.accumulate(new long[] { 42, -24, 0, 1 }, 1, 3);
    agg.accumulate(new double[] { 3, -13, 5, -1 }, 1, 3);
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(2, agg.end());
    assertArrayEquals(new double[] { -37, 5 }, agg.doubleArray(), 0.001);
  }
  
  @Test
  public void offsetsPooled() throws Exception {
    ArraySumFactory.ArraySum agg = new ArraySumFactory.ArraySum(false, POOLED);
    agg.accumulate(new long[] { 42, -24, 0, 1 }, 1, 3);
    agg.accumulate(new long[] { 3, -13, 5, -1 }, 1, 3);
    
    assertTrue(agg.isInteger());
    assertNull(agg.doubleArray());
    assertEquals(0, agg.offset());
    assertEquals(2, agg.end());
    assertPooledArrayEquals(new long[] { -37, 5 }, agg.longArray());
    assertPoolCounters(LONG_POOL, 1, 0, 0, 0);
    assertPoolCounters(DOUBLE_POOL, 0, 0, 0, 0);
    agg.close();
    
    before();
    agg = new ArraySumFactory.ArraySum(false, POOLED);
    agg.accumulate(new double[] { 42, -24, 0, 1 }, 1, 3);
    agg.accumulate(new double[] { 3, -13, 5, -1 }, 1, 3);
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(2, agg.end());
    assertPooledArrayEquals(new double[] { -37, 5 }, agg.doubleArray(), 0.001);
    assertPoolCounters(LONG_POOL, 0, 0, 0, 0);
    assertPoolCounters(DOUBLE_POOL, 1, 0, 0, 0);
    agg.close();
    
    before();
    agg = new ArraySumFactory.ArraySum(false, POOLED);
    agg.accumulate(new long[] { 42, -24, 0, 1 }, 1, 3);
    agg.accumulate(new double[] { 3, -13, 5, -1 }, 1, 3);
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(2, agg.end());
    assertPooledArrayEquals(new double[] { -37, 5 }, agg.doubleArray(), 0.001);
    assertPoolCounters(LONG_POOL, 1, 0, 0, 1);
    assertPoolCounters(DOUBLE_POOL, 1, 0, 0, 0);
    agg.close();
  }

  @Test
  public void singleNanDouble() throws Exception {
    ArraySumFactory.ArraySum agg = new ArraySumFactory.ArraySum(false, NON_POOLED);
    agg.accumulate(new double[] { 3, Double.NaN, 5, -1 });
    assertArrayEquals(new double[] { 3, Double.NaN, 5, -1 }, agg.doubleArray(), 0.001);
  }
  
  @Test
  public void singleNanDoublePooled() throws Exception {
    ArraySumFactory.ArraySum agg = new ArraySumFactory.ArraySum(false, POOLED);
    agg.accumulate(new double[] { 3, Double.NaN, 5, -1 });
    assertPooledArrayEquals(new double[] { 3, Double.NaN, 5, -1 }, agg.doubleArray(), 0.001);
    agg.close();
  }

  @Test
  public void nanThenReals() throws Exception {
    ArraySumFactory.ArraySum agg = new ArraySumFactory.ArraySum(false, NON_POOLED);
    agg.accumulate(new double[] { 3, Double.NaN, 5, -1 });
    agg.accumulate(new double[] { 5, 2, Double.NaN, 2 });
    assertArrayEquals(new double[] { 8, 2, 5, 1 }, agg.doubleArray(), 0.001);
  }
  
}