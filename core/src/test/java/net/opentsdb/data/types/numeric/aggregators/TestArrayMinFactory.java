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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

public class TestArrayMinFactory {
  
  @Test
  public void longs() {
    ArrayMinFactory.ArrayMin agg = new ArrayMinFactory.ArrayMin(false);
    agg.accumulate(new long[] { 42, -24, 0, 1 });
    agg.accumulate(new long[] { 3, -13, 5, -1 });
    
    assertTrue(agg.isInteger());
    assertNull(agg.doubleArray());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertArrayEquals(new long[] { 3, -24, 0, -1 }, agg.longArray());
    
    agg = new ArrayMinFactory.ArrayMin(false);
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
  public void doubles() throws Exception {
    ArrayMinFactory.ArrayMin agg = new ArrayMinFactory.ArrayMin(false);
    agg.accumulate(new double[] { 42, -24, 0, 1 });
    agg.accumulate(new double[] { 3, -13, 5, -1 });
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertArrayEquals(new double[] { 3, -24, 0, -1 }, agg.doubleArray(), 0.001);
    
    // non-infectious nans
    agg = new ArrayMinFactory.ArrayMin(false);
    agg.accumulate(new double[] { 42, -24, 0, Double.NaN });
    agg.accumulate(new double[] { 3, Double.NaN, 5, -1 });
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertArrayEquals(new double[] { 3, -24, 0, -1 }, agg.doubleArray(), 0.001);
    
    // infectious nans
    agg = new ArrayMinFactory.ArrayMin(true);
    agg.accumulate(new double[] { 42, -24, 0, Double.NaN });
    agg.accumulate(new double[] { 3, Double.NaN, 5, -1 });
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertArrayEquals(new double[] { 3, Double.NaN, 0, Double.NaN }, 
        agg.doubleArray(), 0.001);
    
    // bad length
    try {
      agg.accumulate(new double[] { 1 });
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void mixed() throws Exception {
    ArrayMinFactory.ArrayMin agg = new ArrayMinFactory.ArrayMin(false);
    agg.accumulate(new long[] { 42, -24, 0, 1 });
    agg.accumulate(new double[] { 3, -13, 5, -1 });
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertArrayEquals(new double[] { 3, -24, 0, -1 }, agg.doubleArray(), 0.001);
    
    agg = new ArrayMinFactory.ArrayMin(false);
    agg.accumulate(new double[] { 3, -13, 5, -1 });
    agg.accumulate(new long[] { 42, -24, 0, 1 });
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertArrayEquals(new double[] { 3, -24, 0, -1 }, agg.doubleArray(), 0.001);
  }
  
  @Test
  public void offsets() throws Exception {
    ArrayMinFactory.ArrayMin agg = new ArrayMinFactory.ArrayMin(false);
    agg.accumulate(new long[] { 42, -24, 0, 1 }, 1, 3);
    agg.accumulate(new long[] { 3, -13, 5, -1 }, 1, 3);
    
    assertTrue(agg.isInteger());
    assertNull(agg.doubleArray());
    assertEquals(0, agg.offset());
    assertEquals(2, agg.end());
    assertArrayEquals(new long[] { -24, 0 }, agg.longArray());
    
    agg = new ArrayMinFactory.ArrayMin(false);
    agg.accumulate(new double[] { 42, -24, 0, 1 }, 1, 3);
    agg.accumulate(new double[] { 3, -13, 5, -1 }, 1, 3);
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(2, agg.end());
    assertArrayEquals(new double[] { -24, 0 }, agg.doubleArray(), 0.001);
    
    agg = new ArrayMinFactory.ArrayMin(false);
    agg.accumulate(new long[] { 42, -24, 0, 1 }, 1, 3);
    agg.accumulate(new double[] { 3, -13, 5, -1 }, 1, 3);
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(2, agg.end());
    assertArrayEquals(new double[] { -24, 0 }, agg.doubleArray(), 0.001);
  }
  
}
