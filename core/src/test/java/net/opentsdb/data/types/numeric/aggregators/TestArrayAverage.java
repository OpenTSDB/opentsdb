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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import org.junit.Test;

public class TestArrayAverage {
  @Test
  public void longs() {
    ArrayAverageFactory.ArrayAverage agg = new ArrayAverageFactory.ArrayAverage(false);
    agg.accumulate(new long[] { 42, -24, 0, 1 });
    agg.accumulate(new long[] { 3, -13, 5, -1 });
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertArrayEquals(new double[] { 22.5, -18.5, 2.5, 0 }, agg.doubleArray(), 0.001);
    
    agg = new ArrayAverageFactory.ArrayAverage(false);
    agg.accumulate(new long[] { });
    agg.accumulate(new long[] { });
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(0, agg.end());
    assertArrayEquals(new double[] { }, agg.doubleArray(), 0.001);
    
    // bad length
    agg = new ArrayAverageFactory.ArrayAverage(false);
    agg.accumulate(new long[] { 42, -24, 0, 1 });
    try {
      agg.accumulate(new long[] { 1 });
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void doubles() throws Exception {
    ArrayAverageFactory.ArrayAverage agg = new ArrayAverageFactory.ArrayAverage(false);
    agg.accumulate(new double[] { 42, -24, 0, 1 });
    agg.accumulate(new double[] { 3, -13, 5, -1 });
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertArrayEquals(new double[] { 22.5, -18.5, 2.5, 0 }, agg.doubleArray(), 0.001);
    
    // non-infectious nans
    agg = new ArrayAverageFactory.ArrayAverage(false);
    agg.accumulate(new double[] { 42, -24, 0, Double.NaN });
    agg.accumulate(new double[] { 3, Double.NaN, 5, -1 });
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertArrayEquals(new double[] { 22.5, -24, 2.5, -1 }, agg.doubleArray(), 0.001);
    
    // infectious nans
    agg = new ArrayAverageFactory.ArrayAverage(true);
    agg.accumulate(new double[] { 42, -24, 0, Double.NaN });
    agg.accumulate(new double[] { 3, Double.NaN, 5, -1 });
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertArrayEquals(new double[] { 22.5, Double.NaN, 2.5, Double.NaN }, 
        agg.doubleArray(), 0.001);
    
    // bad length
    agg = new ArrayAverageFactory.ArrayAverage(false);
    agg.accumulate(new double[] { 42, -24, 0, 1 });
    try {
      agg.accumulate(new double[] { 1 });
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void mixed() throws Exception {
    ArrayAverageFactory.ArrayAverage agg = new ArrayAverageFactory.ArrayAverage(false);
    agg.accumulate(new long[] { 42, -24, 0, 1 });
    agg.accumulate(new double[] { 3, -13, 5, -1 });
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertArrayEquals(new double[] { 22.5, -18.5, 2.5, 0 }, agg.doubleArray(), 0.001);
    
    agg = new ArrayAverageFactory.ArrayAverage(false);
    agg.accumulate(new double[] { 3, -13, 5, -1 });
    agg.accumulate(new long[] { 42, -24, 0, 1 });
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertArrayEquals(new double[] { 22.5, -18.5, 2.5, 0 }, agg.doubleArray(), 0.001);
  }
  
  @Test
  public void offsets() throws Exception {
    ArrayAverageFactory.ArrayAverage agg = new ArrayAverageFactory.ArrayAverage(false);
    agg.accumulate(new long[] { 42, -24, 0, 1 }, 1, 3);
    agg.accumulate(new long[] { 3, -13, 5, -1 }, 1, 3);
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(2, agg.end());
    assertArrayEquals(new double[] { -18.5, 2.5 }, agg.doubleArray(), 0.001);
    
    agg = new ArrayAverageFactory.ArrayAverage(false);
    agg.accumulate(new double[] { 42, -24, 0, 1 }, 1, 3);
    agg.accumulate(new double[] { 3, -13, 5, -1 }, 1, 3);
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(2, agg.end());
    assertArrayEquals(new double[] { -18.5, 2.5 }, agg.doubleArray(), 0.001);
    
    agg = new ArrayAverageFactory.ArrayAverage(false);
    agg.accumulate(new long[] { 42, -24, 0, 1 }, 1, 3);
    agg.accumulate(new double[] { 3, -13, 5, -1 }, 1, 3);
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(2, agg.end());
    assertArrayEquals(new double[] { -18.5, 2.5 }, agg.doubleArray(), 0.001);
  }
}
