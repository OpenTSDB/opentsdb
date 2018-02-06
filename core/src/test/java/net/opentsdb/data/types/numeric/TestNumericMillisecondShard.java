// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.junit.Before;
import org.junit.Test;

import net.opentsdb.data.BaseTimeSeriesId;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.types.numeric.NumericType;

public class TestNumericMillisecondShard {
  private TimeSeriesId id;
  private TimeStamp start;
  private TimeStamp end;
  
  @Before
  public void before() throws Exception {
    id = BaseTimeSeriesId.newBuilder()
        .setMetric("Samwell")
        .build();
    start = new MillisecondTimeStamp(0L);
    end = new MillisecondTimeStamp(3600000);
  }
  
  @Test
  public void ctor() throws Exception {
    NumericMillisecondShard shard = new NumericMillisecondShard(id, start, end);
    assertEquals(4, shard.encodeOn());
    assertEquals(4, shard.offsets().length);
    assertEquals(4, shard.values().length);
    assertEquals(0L, shard.startTime().msEpoch());
    assertEquals(3600000, shard.endTime().msEpoch());
    assertEquals(-1, shard.order());
    assertEquals(1, shard.types().size());
    assertEquals(NumericType.TYPE, shard.types().iterator().next());
    try {
      shard.iterator().next();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
    
    shard = new NumericMillisecondShard(id, start, end, 42);
    assertEquals(4, shard.encodeOn());
    assertEquals(4, shard.offsets().length);
    assertEquals(4, shard.values().length);
    assertEquals(0L, shard.startTime().msEpoch());
    assertEquals(3600000, shard.endTime().msEpoch());
    assertEquals(42, shard.order());
    try {
      shard.iterator().next();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
    
    shard = new NumericMillisecondShard(id, start, end, 42, 100);
    assertEquals(4, shard.encodeOn());
    assertEquals(400, shard.offsets().length);
    assertEquals(400, shard.values().length);
    assertEquals(0L, shard.startTime().msEpoch());
    assertEquals(3600000, shard.endTime().msEpoch());
    assertEquals(42, shard.order());
    try {
      shard.iterator().next();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
    
    end = new MillisecondTimeStamp(1L);
    shard = new NumericMillisecondShard(id, start, end, 42, 0);
    end = new MillisecondTimeStamp(86400000L);
    assertEquals(1, shard.encodeOn());
    assertEquals(0, shard.offsets().length);
    assertEquals(0, shard.values().length);
    assertEquals(0L, shard.startTime().msEpoch());
    assertEquals(1L, shard.endTime().msEpoch());
    assertEquals(42, shard.order());
    try {
      shard.iterator().next();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
    
    try {
      shard = new NumericMillisecondShard(null, start, end, 42, 100);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      shard = new NumericMillisecondShard(id, null, end, 42, 100);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      shard = new NumericMillisecondShard(id, start, null, 42, 100);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      shard = new NumericMillisecondShard(id, start, end, 42, -1);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
 
  @Test
  public void add() throws Exception {
    start = new MillisecondTimeStamp(1486045800000L);
    end = new MillisecondTimeStamp(1486045900000L);
    NumericMillisecondShard shard = new NumericMillisecondShard(id, start, end);
    assertEquals(3, shard.encodeOn());
    assertEquals(3, shard.offsets().length);
    assertEquals(4, shard.values().length);
    assertEquals(1486045800000L, shard.startTime().msEpoch());
    assertEquals(1486045900000L, shard.endTime().msEpoch());
    
    assertArrayEquals(new byte[] { 0, 0, 0 }, shard.offsets());
    assertArrayEquals(new byte[] { 0, 0, 0, 0 }, shard.values());
    
    shard.add(1486045801000L, 42);
    assertEquals(6, shard.offsets().length); // expanded
    assertEquals(4, shard.values().length);
    assertArrayEquals(new byte[] { 1, -12, 0, 0, 0, 0 }, shard.offsets());
    assertArrayEquals(new byte[] { 42, 0, 0, 0 }, shard.values());
    
    shard.add(1486045871000L, 9866.854);
    assertEquals(12, shard.offsets().length); // expanded
    assertEquals(16, shard.values().length);
    assertArrayEquals(new byte[] { 1, -12, 0, -118, -84, 15, 0, 0, 0, 0, 0, 0 }, 
        shard.offsets());
    assertArrayEquals(new byte[] { 42, 64, -61, 69, 109, 79, -33, 59, 
        100, 0, 0, 0, 0, 0, 0, 0 }, shard.values());
    
    // less than not allowed
    try {
      shard.add(1486045800000L, 1);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }

    // same not allowed
    try {
      shard.add(1486045871000L, 9866.854);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // too late
    try {
      shard.add(1486045900001L, 1);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // too early
    shard = new NumericMillisecondShard(id, start, end);
    try {
      shard.add(1486045799999L, 1);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // Ugly to test, so use the next() tests for more checks.
  }
  
  @Test
  public void iterators() throws Exception {
    start = new MillisecondTimeStamp(1486045800000L);
    end = new MillisecondTimeStamp(1486046000000L);
    NumericMillisecondShard shard = new NumericMillisecondShard(id, start, end);
    shard.add(1486045801000L, 42);
    shard.add(1486045871000L, 9866.854);
    shard.add(1486045881000L, -128);
    
    Iterator<TimeSeriesValue<?>> iterator = shard.iterator();
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(42, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1486045871000L, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertEquals(9866.854, v.value().doubleValue(), 0.00001);
    
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1486045881000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(-128, v.value().longValue());
    
    // add after is ok BUT we need a new iterator!
    shard.add(1486045891000L, Long.MAX_VALUE);
    assertFalse(iterator.hasNext());
    
    // add more of diff values for testing
    shard.add(1486045891050L, Long.MIN_VALUE);
    shard.add(1486045901571L, Double.MAX_VALUE);
    shard.add(1486045901572L, Double.MIN_VALUE);
    shard.add(1486045902000L, 0);
    shard.add(1486045903000L, 0f);
    shard.add(1486045904000L, Double.POSITIVE_INFINITY);
    shard.add(1486045905000L, Double.NaN);
    
    iterator = shard.iterator();
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(42, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1486045871000L, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertEquals(9866.854, v.value().doubleValue(), 0.00001);
    
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1486045881000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(-128, v.value().longValue());
    
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1486045891000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(Long.MAX_VALUE, v.value().longValue());
        
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1486045891050L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(Long.MIN_VALUE, v.value().longValue());
        
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1486045901571L, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertEquals(Double.MAX_VALUE, v.value().doubleValue(), 0.00001);
        
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1486045901572L, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertEquals(Double.MIN_NORMAL, v.value().doubleValue(), 0.00001);
        
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1486045902000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(0, v.value().longValue());
        
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1486045903000L, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertEquals(0, v.value().doubleValue(), 0.0001);
        
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1486045904000L, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertTrue(Double.isInfinite(v.value().doubleValue()));
        
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1486045905000L, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    try {
      v = (TimeSeriesValue<NumericType>) iterator.next();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
  }

}
