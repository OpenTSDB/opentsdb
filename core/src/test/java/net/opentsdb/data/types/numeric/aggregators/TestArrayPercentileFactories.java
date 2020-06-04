// This file is part of OpenTSDB.
// Copyright (C) 2020  The OpenTSDB Authors.
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

import org.junit.BeforeClass;
import org.junit.Test;

import net.opentsdb.data.types.numeric.aggregators.ArrayPercentileFactories.PercentileType;

public class TestArrayPercentileFactories extends BaseTestNumericArray {
  
  private static long[][] LONGS;
  private static double[][] DOUBLES;
  private static Object[] MIXED;
  
  @BeforeClass
  public static void beforeClass() {
    BaseTestNumericArray.beforeClass();
    LONGS = new long[1024][];
    DOUBLES = new double[1024][];
    MIXED = new Object[1024];
    for (int i = 0; i < 1024; i++) {
      long[] dps = new long[4];
      double[] dbls = new double[4];
      for (int x = 0; x < 4; x++) {
        dps[x] = x + i;
        if (i == 42 && x == 2) {
          dbls[x] = Double.NaN;
        } else {
          dbls[x] = x + i;
        }
      }
      LONGS[i] = dps;
      DOUBLES[i] = dbls;
      if (i % 2 == 0) {
        MIXED[i] = dps;
      } else {
        MIXED[i] = dbls;
      }
    }
  }
  
  @Test
  public void longs() {
    ArrayPercentileFactories.ArrayPercentile agg = 
        new ArrayPercentileFactories.ArrayPercentile(false, NON_POOLED, PercentileType.P99);
    for (int i = 0; i < LONGS.length; i++) {
      agg.accumulate(LONGS[i]);
    }

    assertFalse(agg.isInteger());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertNull(agg.longArray());
    assertArrayEquals(new double[] {1013.75, 1014.75, 1015.75, 1016.75 }, 
        agg.doubleArray(), 0.001);

    // bad length
    try {
      agg.accumulate(new long[] { 1 });
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void longsPooled() {
    ArrayPercentileFactories.ArrayPercentile agg = 
        new ArrayPercentileFactories.ArrayPercentile(false, POOLED, PercentileType.P99);
    for (int i = 0; i < LONGS.length; i++) {
      agg.accumulate(LONGS[i]);
    }
    
    assertFalse(agg.isInteger());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertNull(agg.longArray());
    assertPooledArrayEquals(new double[] {1013.75, 1014.75, 1015.75, 1016.75 }, agg.doubleArray(), 0.001);
    assertPoolCounters(LONG_POOL, 0, 0, 0, 0);
    assertPoolCounters(DOUBLE_POOL, 1, 0, 0, 0);
    agg.close();
    assertPoolCounters(LONG_POOL, 0, 0, 0, 0);
    assertPoolCounters(DOUBLE_POOL, 1, 0, 0, 1);
  }
  
  @Test
  public void doubles() throws Exception {
    // non-infectious nans
    ArrayPercentileFactories.ArrayPercentile agg = 
        new ArrayPercentileFactories.ArrayPercentile(false, NON_POOLED, PercentileType.P99);
    for (int i = 0; i < DOUBLES.length; i++) {
      agg.accumulate(DOUBLES[i]);
    }
    
    assertFalse(agg.isInteger());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertNull(agg.longArray());
    assertArrayEquals(new double[] {1013.75, 1014.75, 1015.76, 1016.75 }, 
        agg.doubleArray(), 0.001);

    // infectious nans
    agg = new ArrayPercentileFactories.ArrayPercentile(true, NON_POOLED, PercentileType.P99);
    for (int i = 0; i < DOUBLES.length; i++) {
      agg.accumulate(DOUBLES[i]);
    }
    
    assertFalse(agg.isInteger());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertNull(agg.longArray());
    assertArrayEquals(new double[] {1013.75, 1014.75, Double.NaN, 1016.75 }, 
        agg.doubleArray(), 0.001);//    

    // bad length
    try {
      agg.accumulate(new double[] { 1 });
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void doublesPooled() throws Exception {
    // non-infectious nans
    ArrayPercentileFactories.ArrayPercentile agg = 
        new ArrayPercentileFactories.ArrayPercentile(false, POOLED, PercentileType.P99);
    for (int i = 0; i < DOUBLES.length; i++) {
      agg.accumulate(DOUBLES[i]);
    }
    
    assertFalse(agg.isInteger());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertNull(agg.longArray());
    assertPooledArrayEquals(new double[] {1013.75, 1014.75, 1015.76, 1016.75 }, 
        agg.doubleArray(), 0.001);
    assertPoolCounters(LONG_POOL, 0, 0, 0, 0);
    assertPoolCounters(DOUBLE_POOL, 1, 0, 0, 0);
    agg.close();
    assertPoolCounters(LONG_POOL, 0, 0, 0, 0);
    assertPoolCounters(DOUBLE_POOL, 1, 0, 0, 1);
    
    // infectious nans
    before();
    agg = new ArrayPercentileFactories.ArrayPercentile(true, POOLED, PercentileType.P99);
    for (int i = 0; i < DOUBLES.length; i++) {
      agg.accumulate(DOUBLES[i]);
    }
    
    assertFalse(agg.isInteger());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertNull(agg.longArray());
    assertPooledArrayEquals(new double[] {1013.75, 1014.75, Double.NaN, 1016.75 }, 
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
    ArrayPercentileFactories.ArrayPercentile agg = 
        new ArrayPercentileFactories.ArrayPercentile(false, NON_POOLED, PercentileType.P99);
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
    assertNull(agg.longArray());
    assertArrayEquals(new double[] {1013.75, 1014.75, 1015.75, 1016.75 }, 
        agg.doubleArray(), 0.001);
  }
  
  @Test
  public void mixedPooled() throws Exception {
    ArrayPercentileFactories.ArrayPercentile agg = 
        new ArrayPercentileFactories.ArrayPercentile(false, POOLED, PercentileType.P99);
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
    assertNull(agg.longArray());
    assertPooledArrayEquals(new double[] {1013.75, 1014.75, 1015.75, 1016.75 }, 
        agg.doubleArray(), 0.001);
    assertPoolCounters(LONG_POOL, 0, 0, 0, 0);
    assertPoolCounters(DOUBLE_POOL, 1, 0, 0, 0);
    agg.close();
    assertPoolCounters(LONG_POOL, 0, 0, 0, 0);
    assertPoolCounters(DOUBLE_POOL, 1, 0, 0, 1);
  }
  
  @Test
  public void accumulateDoublesIndexed() throws Exception {
    ArrayPercentileFactories.ArrayPercentile agg = 
        new ArrayPercentileFactories.ArrayPercentile(DefaultArrayAggregatorConfig
          .newBuilder()
          .setArraySize(4)
          .build(), 
        NON_POOLED, 
        PercentileType.P99);
    agg.accumulate(-24.0, 1);
    agg.accumulate(42.0, 0);
    agg.accumulate(3.0, 0);
    agg.accumulate(Double.NaN, 1);
    assertArrayEquals(new double[] { 42, -24, Double.NaN, Double.NaN }, 
        agg.doubleArray(), 0.001);
    
    // oob
    try {
      agg.accumulate(6.5, 5);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // no size so we can't accumulate.
    agg = new ArrayPercentileFactories.ArrayPercentile(false, NON_POOLED, PercentileType.P99);
    try {
      agg.accumulate(6.5, 0);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
  }
  
  @Test
  public void p999() throws Exception {
    ArrayPercentileFactories.ArrayPercentile agg = 
        new ArrayPercentileFactories.ArrayPercentile(false, NON_POOLED, PercentileType.P999);
    for (int i = 0; i < DOUBLES.length; i++) {
      agg.accumulate(DOUBLES[i]);
    }
    
    assertFalse(agg.isInteger());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertNull(agg.longArray());
    assertArrayEquals(new double[] { 1022.975, 1023.975, 1024.976, 1025.975 }, 
        agg.doubleArray(), 0.001);
  }
  
  @Test
  public void p95() throws Exception {
    ArrayPercentileFactories.ArrayPercentile agg = 
        new ArrayPercentileFactories.ArrayPercentile(false, NON_POOLED, PercentileType.P95);
    for (int i = 0; i < DOUBLES.length; i++) {
      agg.accumulate(DOUBLES[i]);
    }
    
    assertFalse(agg.isInteger());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertNull(agg.longArray());
    assertArrayEquals(new double[] { 972.75, 973.75, 974.8, 975.75 }, 
        agg.doubleArray(), 0.001);
  }
  
  @Test
  public void p90() throws Exception {
    ArrayPercentileFactories.ArrayPercentile agg = 
        new ArrayPercentileFactories.ArrayPercentile(false, NON_POOLED, PercentileType.P90);
    for (int i = 0; i < DOUBLES.length; i++) {
      agg.accumulate(DOUBLES[i]);
    }
    
    assertFalse(agg.isInteger());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertNull(agg.longArray());
    assertArrayEquals(new double[] { 921.5, 922.5, 923.6, 924.5 }, 
        agg.doubleArray(), 0.001);
  }
  
  @Test
  public void p75() throws Exception {
    ArrayPercentileFactories.ArrayPercentile agg = 
        new ArrayPercentileFactories.ArrayPercentile(false, NON_POOLED, PercentileType.P75);
    for (int i = 0; i < DOUBLES.length; i++) {
      agg.accumulate(DOUBLES[i]);
    }
    
    assertFalse(agg.isInteger());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertNull(agg.longArray());
    assertArrayEquals(new double[] { 767.75, 768.75, 770.0, 770.75 }, 
        agg.doubleArray(), 0.001);
  }
  
  @Test
  public void p50() throws Exception {
    ArrayPercentileFactories.ArrayPercentile agg = 
        new ArrayPercentileFactories.ArrayPercentile(false, NON_POOLED, PercentileType.P50);
    for (int i = 0; i < DOUBLES.length; i++) {
      agg.accumulate(DOUBLES[i]);
    }
    
    assertFalse(agg.isInteger());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertNull(agg.longArray());
    assertArrayEquals(new double[] { 511.5, 512.5, 514.0, 514.5 }, 
        agg.doubleArray(), 0.001);
  }
  
  @Test
  public void EP999R3() throws Exception {
    ArrayPercentileFactories.ArrayPercentile agg = 
        new ArrayPercentileFactories.ArrayPercentile(false, NON_POOLED, PercentileType.EP999R3);
    for (int i = 0; i < DOUBLES.length; i++) {
      agg.accumulate(DOUBLES[i]);
    }
    
    assertFalse(agg.isInteger());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertNull(agg.longArray());
    assertArrayEquals(new double[] { 1022.0, 1023.0, 1024.0, 1025.0 }, 
        agg.doubleArray(), 0.001);
  }
  
  @Test
  public void EP99R3() throws Exception {
    ArrayPercentileFactories.ArrayPercentile agg = 
        new ArrayPercentileFactories.ArrayPercentile(false, NON_POOLED, PercentileType.EP99R3);
    for (int i = 0; i < DOUBLES.length; i++) {
      agg.accumulate(DOUBLES[i]);
    }
    
    assertFalse(agg.isInteger());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertNull(agg.longArray());
    assertArrayEquals(new double[] { 1013.0, 1014.0, 1015.0, 1016.0 }, 
        agg.doubleArray(), 0.001);
  }
  
  @Test
  public void EP95R3() throws Exception {
    ArrayPercentileFactories.ArrayPercentile agg = 
        new ArrayPercentileFactories.ArrayPercentile(false, NON_POOLED, PercentileType.EP95R3);
    for (int i = 0; i < DOUBLES.length; i++) {
      agg.accumulate(DOUBLES[i]);
    }
    
    assertFalse(agg.isInteger());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertNull(agg.longArray());
    assertArrayEquals(new double[] { 972.0, 973.0, 974.0, 975.0 }, 
        agg.doubleArray(), 0.001);
  }
  
  @Test
  public void EP90R3() throws Exception {
    ArrayPercentileFactories.ArrayPercentile agg = 
        new ArrayPercentileFactories.ArrayPercentile(false, NON_POOLED, PercentileType.EP90R3);
    for (int i = 0; i < DOUBLES.length; i++) {
      agg.accumulate(DOUBLES[i]);
    }
    
    assertFalse(agg.isInteger());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertNull(agg.longArray());
    assertArrayEquals(new double[] { 921.0, 922.0, 923.0, 924.0 }, 
        agg.doubleArray(), 0.001);
  }
  
  @Test
  public void EP75R3() throws Exception {
    ArrayPercentileFactories.ArrayPercentile agg = 
        new ArrayPercentileFactories.ArrayPercentile(false, NON_POOLED, PercentileType.EP75R3);
    for (int i = 0; i < DOUBLES.length; i++) {
      agg.accumulate(DOUBLES[i]);
    }
    
    assertFalse(agg.isInteger());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertNull(agg.longArray());
    assertArrayEquals(new double[] { 767.0, 768.0, 769.0, 770.0 }, 
        agg.doubleArray(), 0.001);
  }
  
  @Test
  public void EP999R7() throws Exception {
    ArrayPercentileFactories.ArrayPercentile agg = 
        new ArrayPercentileFactories.ArrayPercentile(false, NON_POOLED, PercentileType.EP999R7);
    for (int i = 0; i < DOUBLES.length; i++) {
      agg.accumulate(DOUBLES[i]);
    }
    
    assertFalse(agg.isInteger());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertNull(agg.longArray());
    assertArrayEquals(new double[] { 1021.977, 1022.977, 1023.978, 1024.977 }, 
        agg.doubleArray(), 0.001);
  }
  
  @Test
  public void EP99R7() throws Exception {
    ArrayPercentileFactories.ArrayPercentile agg = 
        new ArrayPercentileFactories.ArrayPercentile(false, NON_POOLED, PercentileType.EP99R7);
    for (int i = 0; i < DOUBLES.length; i++) {
      agg.accumulate(DOUBLES[i]);
    }
    
    assertFalse(agg.isInteger());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertNull(agg.longArray());
    assertArrayEquals(new double[] { 1012.77, 1013.77, 1014.78, 1015.77 }, 
        agg.doubleArray(), 0.001);
  }
  
  @Test
  public void EP95R7() throws Exception {
    ArrayPercentileFactories.ArrayPercentile agg = 
        new ArrayPercentileFactories.ArrayPercentile(false, NON_POOLED, PercentileType.EP95R7);
    for (int i = 0; i < DOUBLES.length; i++) {
      agg.accumulate(DOUBLES[i]);
    }
    
    assertFalse(agg.isInteger());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertNull(agg.longArray());
    assertArrayEquals(new double[] { 971.849, 972.849, 973.9, 974.849 }, 
        agg.doubleArray(), 0.001);
  }
  
  @Test
  public void EP90R7() throws Exception {
    ArrayPercentileFactories.ArrayPercentile agg = 
        new ArrayPercentileFactories.ArrayPercentile(false, NON_POOLED, PercentileType.EP90R7);
    for (int i = 0; i < DOUBLES.length; i++) {
      agg.accumulate(DOUBLES[i]);
    }
    
    assertFalse(agg.isInteger());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertNull(agg.longArray());
    assertArrayEquals(new double[] { 920.7, 921.7, 922.8, 923.7 }, 
        agg.doubleArray(), 0.001);
  }
  
  @Test
  public void EP75R7() throws Exception {
    ArrayPercentileFactories.ArrayPercentile agg = 
        new ArrayPercentileFactories.ArrayPercentile(false, NON_POOLED, PercentileType.EP75R7);
    for (int i = 0; i < DOUBLES.length; i++) {
      agg.accumulate(DOUBLES[i]);
    }
    
    assertFalse(agg.isInteger());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertNull(agg.longArray());
    assertArrayEquals(new double[] { 767.25, 768.25, 769.5, 770.25 }, 
        agg.doubleArray(), 0.001);
  }
  
  @Test
  public void EP50R7() throws Exception {
    ArrayPercentileFactories.ArrayPercentile agg = 
        new ArrayPercentileFactories.ArrayPercentile(false, NON_POOLED, PercentileType.EP50R7);
    for (int i = 0; i < DOUBLES.length; i++) {
      agg.accumulate(DOUBLES[i]);
    }
    
    assertFalse(agg.isInteger());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertNull(agg.longArray());
    assertArrayEquals(new double[] { 511.5, 512.5, 514, 514.5 }, 
        agg.doubleArray(), 0.001);
  }
  
}