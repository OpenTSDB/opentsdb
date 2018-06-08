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
package net.opentsdb.query.interpolation.types.numeric;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.NoSuchElementException;

import org.junit.Before;
import org.junit.Test;

import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.types.numeric.BaseNumericFillPolicy;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;

public class TestReadAheadNumericInterpolator {

  private NumericInterpolatorConfig config;
  private BaseNumericFillPolicy fill_policy;
  
  @Before
  public void before() throws Exception {
    config = (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setRealFillPolicy(FillWithRealPolicy.NONE)
        .setType(NumericType.TYPE.toString())
        .build();
    fill_policy = new BaseNumericFillPolicy(config);
  }
  
  @Test
  public void ctor() throws Exception {
    ReadAheadNumericInterpolator interpolator = 
        new ReadAheadNumericInterpolator(fill_policy);
    assertSame(fill_policy, interpolator.fill_policy);
    assertTrue(interpolator.previous.isEmpty());
    assertNull(interpolator.next);
    assertEquals(0, interpolator.response.value().longValue());
    assertFalse(interpolator.has_next);
    
    try {
      new ReadAheadNumericInterpolator(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void setNext() throws Exception {
    ReadAheadNumericInterpolator interpolator = 
        new ReadAheadNumericInterpolator(fill_policy);
    
    // check a null add is a no-op
    interpolator.setNext(null);
    assertTrue(interpolator.previous.isEmpty());
    assertNull(interpolator.next);
    assertFalse(interpolator.has_next);
    
    MutableNumericValue v = new MutableNumericValue();
    v.resetTimestamp(new MillisecondTimeStamp(1000));
    v.resetValue(42);
    
    // first dp
    interpolator.setNext(v);
    
    assertTrue(interpolator.previous.isEmpty());
    assertNotSame(v, interpolator.next);
    assertEquals(1000, interpolator.next.timestamp().msEpoch());
    assertEquals(42, interpolator.next.value().longValue());
    assertTrue(interpolator.has_next);
    
    v = new MutableNumericValue();
    v.resetTimestamp(new MillisecondTimeStamp(2000));
    v.resetValue(24);
    
    // next dp, shifts next to previous and resets next.
    interpolator.setNext(v);
    
    assertEquals(1, interpolator.previous.size());
    assertEquals(1000, interpolator.previous.getLast().timestamp().msEpoch());
    assertEquals(42, interpolator.previous.getLast().value().longValue());
    assertNotSame(v, interpolator.next);
    assertEquals(2000, interpolator.next.timestamp().msEpoch());
    assertEquals(24, interpolator.next.value().longValue());
    assertTrue(interpolator.has_next);
    
    v = new MutableNumericValue();
    v.resetTimestamp(new MillisecondTimeStamp(1500)); // note time order is not honored
    v.resetValue(3);
    
    // next dp, shifts next to previous and resets next.
    interpolator.setNext(v);
    
    assertEquals(2, interpolator.previous.size());
    assertEquals(1000, interpolator.previous.getFirst().timestamp().msEpoch());
    assertEquals(42, interpolator.previous.getFirst().value().longValue());
    assertEquals(2000, interpolator.previous.getLast().timestamp().msEpoch());
    assertEquals(24, interpolator.previous.getLast().value().longValue());
    assertNotSame(v, interpolator.next);
    assertEquals(1500, interpolator.next.timestamp().msEpoch());
    assertEquals(3, interpolator.next.value().longValue());
    assertTrue(interpolator.has_next);
    
    // null insert
    interpolator.setNext(null);
    
    assertEquals(3, interpolator.previous.size());
    assertEquals(1000, interpolator.previous.getFirst().timestamp().msEpoch());
    assertEquals(42, interpolator.previous.getFirst().value().longValue());
    assertEquals(2000, interpolator.previous.get(1).timestamp().msEpoch());
    assertEquals(24, interpolator.previous.get(1).value().longValue());
    assertEquals(1500, interpolator.previous.getLast().timestamp().msEpoch());
    assertEquals(3, interpolator.previous.getLast().value().longValue());
    assertNull(interpolator.next);
    assertFalse(interpolator.has_next);
    
    v = new MutableNumericValue();
    v.resetTimestamp(new MillisecondTimeStamp(3000)); // note time order is not honored
    v.resetValue(89);
    
    // start inserting again
    interpolator.setNext(v);
    
    assertEquals(3, interpolator.previous.size());
    assertEquals(1000, interpolator.previous.getFirst().timestamp().msEpoch());
    assertEquals(42, interpolator.previous.getFirst().value().longValue());
    assertEquals(2000, interpolator.previous.get(1).timestamp().msEpoch());
    assertEquals(24, interpolator.previous.get(1).value().longValue());
    assertEquals(1500, interpolator.previous.getLast().timestamp().msEpoch());
    assertEquals(3, interpolator.previous.getLast().value().longValue());
    assertNotSame(v, interpolator.next);
    assertEquals(3000, interpolator.next.timestamp().msEpoch());
    assertEquals(89, interpolator.next.value().longValue());
    assertTrue(interpolator.has_next);
    
    v = new MutableNumericValue();
    v.resetNull(new MillisecondTimeStamp(4000));
    
    // dp with nulled value is ok.
    interpolator.setNext(v);
    
    assertEquals(4, interpolator.previous.size());
    assertEquals(1000, interpolator.previous.getFirst().timestamp().msEpoch());
    assertEquals(42, interpolator.previous.getFirst().value().longValue());
    assertEquals(2000, interpolator.previous.get(1).timestamp().msEpoch());
    assertEquals(24, interpolator.previous.get(1).value().longValue());
    assertEquals(1500, interpolator.previous.get(2).timestamp().msEpoch());
    assertEquals(3, interpolator.previous.get(2).value().longValue());
    assertEquals(3000, interpolator.previous.getLast().timestamp().msEpoch());
    assertEquals(89, interpolator.previous.getLast().value().longValue());
    assertNotSame(v, interpolator.next);
    assertEquals(4000, interpolator.next.timestamp().msEpoch());
    assertNull(interpolator.next.value());
    assertTrue(interpolator.has_next);
  }

  @Test
  public void nextReal() throws Exception {
    ReadAheadNumericInterpolator interpolator = 
        new ReadAheadNumericInterpolator(fill_policy);
    
    try {
      interpolator.nextReal();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
    
    MutableNumericValue v = new MutableNumericValue();
    v.resetTimestamp(new MillisecondTimeStamp(1000));
    v.resetValue(42);
    
    interpolator.setNext(v);
    
    assertEquals(v.timestamp(), interpolator.nextReal());
  }

  @Test
  public void fillRealsNone() throws Exception {
    config = (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setRealFillPolicy(FillWithRealPolicy.NONE)
        .setType(NumericType.TYPE.toString())
        .build();
    fill_policy = new BaseNumericFillPolicy(config);
    
    ReadAheadNumericInterpolator interpolator = 
        new ReadAheadNumericInterpolator(fill_policy);
    final TimeStamp ts = new MillisecondTimeStamp(2000);
    
    // empty interpolator.
    TimeSeriesValue<NumericType> value = interpolator.fill(ts);
    assertEquals(value.timestamp(), ts);
    assertTrue(Double.isNaN(value.value().doubleValue()));
    
    // previous only
    MutableNumericValue prev = new MutableNumericValue();
    prev.resetTimestamp(new MillisecondTimeStamp(1000));
    prev.resetValue(42);
    interpolator.previous.add(prev);
    
    value = interpolator.fill(ts);
    assertEquals(value.timestamp(), ts);
    assertTrue(Double.isNaN(value.value().doubleValue()));
    
    // next only
    MutableNumericValue prev2 = new MutableNumericValue();
    prev2.resetTimestamp(new MillisecondTimeStamp(3000));
    prev2.resetValue(24);
    interpolator.previous.clear();
    interpolator.next = prev2;
    
    value = interpolator.fill(ts);
    assertEquals(value.timestamp(), ts);
    assertTrue(Double.isNaN(value.value().doubleValue()));
    
    // previous and next
    interpolator.previous.add(prev);
    
    value = interpolator.fill(ts);
    assertEquals(value.timestamp(), ts);
    assertTrue(Double.isNaN(value.value().doubleValue()));
    
    // between two prev
    interpolator.previous.add(prev2);
    interpolator.next = null;
    
    value = interpolator.fill(ts);
    assertEquals(value.timestamp(), ts);
    assertTrue(Double.isNaN(value.value().doubleValue()));
  }
  
  @Test
  public void fillRealsPreviousOnly() throws Exception {
    config = (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setRealFillPolicy(FillWithRealPolicy.PREVIOUS_ONLY)
        .setType(NumericType.TYPE.toString())
        .build();
    fill_policy = new BaseNumericFillPolicy(config);
    
    ReadAheadNumericInterpolator interpolator = 
        new ReadAheadNumericInterpolator(fill_policy);
    final TimeStamp ts = new MillisecondTimeStamp(2000);
    
    // empty interpolator.
    TimeSeriesValue<NumericType> value = interpolator.fill(ts);
    assertEquals(value.timestamp(), ts);
    assertTrue(Double.isNaN(value.value().doubleValue()));
    
    // previous only
    MutableNumericValue prev = new MutableNumericValue();
    prev.resetTimestamp(new MillisecondTimeStamp(1000));
    prev.resetValue(42);
    interpolator.previous.add(prev);
    
    value = interpolator.fill(ts);
    assertEquals(value.timestamp(), ts);
    assertEquals(42, value.value().longValue());
    
    // next only
    MutableNumericValue prev2 = new MutableNumericValue();
    prev2.resetTimestamp(new MillisecondTimeStamp(3000));
    prev2.resetValue(24);
    interpolator.previous.clear();
    interpolator.next = prev2;
    
    value = interpolator.fill(ts);
    assertEquals(value.timestamp(), ts);
    assertTrue(Double.isNaN(value.value().doubleValue()));
    
    // previous and next
    interpolator.previous.add(prev);
    
    value = interpolator.fill(ts);
    assertEquals(value.timestamp(), ts);
    assertEquals(42, value.value().longValue());
    
    // between two prev
    interpolator.previous.add(prev2);
    interpolator.next = null;
    
    value = interpolator.fill(ts);
    assertEquals(value.timestamp(), ts);
    assertEquals(42, value.value().longValue());
  }
  
  @Test
  public void fillRealsPreferPrevious() throws Exception {
    config = (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setRealFillPolicy(FillWithRealPolicy.PREFER_PREVIOUS)
        .setType(NumericType.TYPE.toString())
        .build();
    fill_policy = new BaseNumericFillPolicy(config);
    
    ReadAheadNumericInterpolator interpolator = 
        new ReadAheadNumericInterpolator(fill_policy);
    final TimeStamp ts = new MillisecondTimeStamp(2000);
    
    // empty interpolator.
    TimeSeriesValue<NumericType> value = interpolator.fill(ts);
    assertEquals(value.timestamp(), ts);
    assertTrue(Double.isNaN(value.value().doubleValue()));
    
    // previous only
    MutableNumericValue prev = new MutableNumericValue();
    prev.resetTimestamp(new MillisecondTimeStamp(1000));
    prev.resetValue(42);
    interpolator.previous.add(prev);
    
    value = interpolator.fill(ts);
    assertEquals(value.timestamp(), ts);
    assertEquals(42, value.value().longValue());
    
    // next only
    MutableNumericValue prev2 = new MutableNumericValue();
    prev2.resetTimestamp(new MillisecondTimeStamp(3000));
    prev2.resetValue(24);
    interpolator.previous.clear();
    interpolator.next = prev2;
    
    value = interpolator.fill(ts);
    assertEquals(value.timestamp(), ts);
    assertEquals(24, value.value().longValue());
    
    // previous and next
    interpolator.previous.add(prev);
    
    value = interpolator.fill(ts);
    assertEquals(value.timestamp(), ts);
    assertEquals(42, value.value().longValue());
    
    // between two prev
    interpolator.previous.add(prev2);
    interpolator.next = null;
    
    value = interpolator.fill(ts);
    assertEquals(value.timestamp(), ts);
    assertEquals(42, value.value().longValue());
  }
  
  @Test
  public void fillRealsNextOnly() throws Exception {
    config = (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setRealFillPolicy(FillWithRealPolicy.NEXT_ONLY)
        .setType(NumericType.TYPE.toString())
        .build();
    fill_policy = new BaseNumericFillPolicy(config);
    
    ReadAheadNumericInterpolator interpolator = 
        new ReadAheadNumericInterpolator(fill_policy);
    final TimeStamp ts = new MillisecondTimeStamp(2000);
    
    // empty interpolator.
    TimeSeriesValue<NumericType> value = interpolator.fill(ts);
    assertEquals(value.timestamp(), ts);
    assertTrue(Double.isNaN(value.value().doubleValue()));
    
    // previous only
    MutableNumericValue prev = new MutableNumericValue();
    prev.resetTimestamp(new MillisecondTimeStamp(1000));
    prev.resetValue(42);
    interpolator.previous.add(prev);
    
    value = interpolator.fill(ts);
    assertEquals(value.timestamp(), ts);
    assertTrue(Double.isNaN(value.value().doubleValue()));
    
    // next only
    MutableNumericValue prev2 = new MutableNumericValue();
    prev2.resetTimestamp(new MillisecondTimeStamp(3000));
    prev2.resetValue(24);
    interpolator.previous.clear();
    interpolator.next = prev2;
    
    value = interpolator.fill(ts);
    assertEquals(value.timestamp(), ts);
    assertEquals(24, value.value().longValue());
    
    // previous and next
    interpolator.previous.add(prev);
    
    value = interpolator.fill(ts);
    assertEquals(value.timestamp(), ts);
    assertEquals(24, value.value().longValue());
    
    // between two prev
    interpolator.previous.add(prev2);
    interpolator.next = null;
    
    value = interpolator.fill(ts);
    assertEquals(value.timestamp(), ts);
    assertEquals(24, value.value().longValue());
  }
  
  @Test
  public void fillRealsPreviousNext() throws Exception {
    config = (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
        .setType(NumericType.TYPE.toString())
        .build();
    fill_policy = new BaseNumericFillPolicy(config);
    
    ReadAheadNumericInterpolator interpolator = 
        new ReadAheadNumericInterpolator(fill_policy);
    final TimeStamp ts = new MillisecondTimeStamp(2000);
    
    // empty interpolator.
    TimeSeriesValue<NumericType> value = interpolator.fill(ts);
    assertEquals(value.timestamp(), ts);
    assertTrue(Double.isNaN(value.value().doubleValue()));
    
    // previous only
    MutableNumericValue prev = new MutableNumericValue();
    prev.resetTimestamp(new MillisecondTimeStamp(1000));
    prev.resetValue(42);
    interpolator.previous.add(prev);
    
    value = interpolator.fill(ts);
    assertEquals(value.timestamp(), ts);
    assertEquals(42, value.value().longValue());
    
    // next only
    MutableNumericValue prev2 = new MutableNumericValue();
    prev2.resetTimestamp(new MillisecondTimeStamp(3000));
    prev2.resetValue(24);
    interpolator.previous.clear();
    interpolator.next = prev2;
    
    value = interpolator.fill(ts);
    assertEquals(value.timestamp(), ts);
    assertEquals(24, value.value().longValue());
    
    // previous and next
    interpolator.previous.add(prev);
    
    value = interpolator.fill(ts);
    assertEquals(value.timestamp(), ts);
    assertEquals(24, value.value().longValue());
    
    // between two prev
    interpolator.previous.add(prev2);
    interpolator.next = null;
    
    value = interpolator.fill(ts);
    assertEquals(value.timestamp(), ts);
    assertEquals(24, value.value().longValue());
  }

  @Test
  public void nextEmpty() throws Exception {
    ReadAheadNumericInterpolator interpolator = 
        new ReadAheadNumericInterpolator(fill_policy);
    
    TimeSeriesValue<NumericType> value = 
        interpolator.next(new MillisecondTimeStamp(2000));
    assertEquals(2000, value.timestamp().msEpoch());
    assertTrue(Double.isNaN(value.value().doubleValue()));
  }
  
  @Test
  public void nextNextOnly() throws Exception {
    ReadAheadNumericInterpolator interpolator = 
        new ReadAheadNumericInterpolator(fill_policy);
    
    MutableNumericValue v = new MutableNumericValue();
    v.resetTimestamp(new MillisecondTimeStamp(2000));
    v.resetValue(42);
    interpolator.setNext(v);
    
    TimeSeriesValue<NumericType> value = 
        interpolator.next(new MillisecondTimeStamp(1000));
    assertEquals(1000, value.timestamp().msEpoch());
    assertTrue(Double.isNaN(value.value().doubleValue()));
    assertTrue(interpolator.previous.isEmpty());
    assertTrue(interpolator.hasNext());
    
    value = interpolator.next(new MillisecondTimeStamp(2000));
    assertEquals(2000, value.timestamp().msEpoch());
    assertEquals(42, value.value().longValue());
    assertFalse(interpolator.hasNext());
    assertEquals(1, interpolator.previous.size());
    
    value = interpolator.next(new MillisecondTimeStamp(3000));
    assertEquals(3000, value.timestamp().msEpoch());
    assertTrue(Double.isNaN(value.value().doubleValue()));
    assertFalse(interpolator.hasNext());
    assertEquals(1, interpolator.previous.size());
  }
  
  @Test
  public void nextPreviousOnly() throws Exception {
    ReadAheadNumericInterpolator interpolator = 
        new ReadAheadNumericInterpolator(fill_policy);
    
    MutableNumericValue v = new MutableNumericValue();
    v.resetTimestamp(new MillisecondTimeStamp(500));
    v.resetValue(42);
    interpolator.setNext(v);
    interpolator.setNext(null); // shifts to prev, no more data now
    
    TimeSeriesValue<NumericType> value = 
        interpolator.next(new MillisecondTimeStamp(1000));
    assertEquals(1000, value.timestamp().msEpoch());
    assertTrue(Double.isNaN(value.value().doubleValue()));
    assertFalse(interpolator.hasNext());
    assertEquals(1, interpolator.previous.size());
    
    value = interpolator.next(new MillisecondTimeStamp(2000));
    assertEquals(2000, value.timestamp().msEpoch());
    assertTrue(Double.isNaN(value.value().doubleValue()));
    assertFalse(interpolator.hasNext());
    assertEquals(1, interpolator.previous.size());
    
    value = interpolator.next(new MillisecondTimeStamp(3000));
    assertEquals(3000, value.timestamp().msEpoch());
    assertTrue(Double.isNaN(value.value().doubleValue()));
    assertFalse(interpolator.hasNext());
    assertEquals(1, interpolator.previous.size());
  }

  @Test
  public void nextNextAndPrevious() throws Exception {
    ReadAheadNumericInterpolator interpolator = 
        new ReadAheadNumericInterpolator(fill_policy);
    
    MutableNumericValue v = new MutableNumericValue();
    v.resetTimestamp(new MillisecondTimeStamp(1000));
    v.resetValue(42);
    interpolator.setNext(v);
    
    v = new MutableNumericValue();
    v.resetTimestamp(new MillisecondTimeStamp(2000));
    v.resetValue(24);
    interpolator.setNext(v);
    
    TimeSeriesValue<NumericType> value = 
        interpolator.next(new MillisecondTimeStamp(500));
    assertEquals(500, value.timestamp().msEpoch());
    assertTrue(Double.isNaN(value.value().doubleValue()));
    assertTrue(interpolator.hasNext());
    assertEquals(1, interpolator.previous.size());
    assertEquals(42, interpolator.previous.getLast().value().longValue());
    assertEquals(24, interpolator.next.value().longValue());
    
    value = 
        interpolator.next(new MillisecondTimeStamp(1000));
    assertEquals(1000, value.timestamp().msEpoch());
    assertEquals(42, value.value().longValue());
    assertTrue(interpolator.hasNext());
    assertEquals(1, interpolator.previous.size());
    assertEquals(42, interpolator.previous.getLast().value().longValue());
    assertEquals(24, interpolator.next.value().longValue());
    
    value = interpolator.next(new MillisecondTimeStamp(1500));
    assertEquals(1500, value.timestamp().msEpoch());
    assertTrue(Double.isNaN(value.value().doubleValue()));
    assertTrue(interpolator.hasNext());
    assertEquals(1, interpolator.previous.size());
    assertEquals(42, interpolator.previous.getLast().value().longValue());
    assertEquals(24, interpolator.next.value().longValue());
    
    value = interpolator.next(new MillisecondTimeStamp(2000));
    assertEquals(2000, value.timestamp().msEpoch());
    assertEquals(24, value.value().longValue());
    assertFalse(interpolator.hasNext());
    assertEquals(1, interpolator.previous.size());
    assertEquals(24, interpolator.previous.getLast().value().longValue());
    assertNull(interpolator.next);
  }
  
  @Test
  public void nextOnlyPrevious() throws Exception {
    ReadAheadNumericInterpolator interpolator = 
        new ReadAheadNumericInterpolator(fill_policy);
    
    MutableNumericValue v = new MutableNumericValue();
    v.resetTimestamp(new MillisecondTimeStamp(1000));
    v.resetValue(42);
    interpolator.setNext(v);
    
    v = new MutableNumericValue();
    v.resetTimestamp(new MillisecondTimeStamp(2000));
    v.resetValue(24);
    interpolator.setNext(v);
    
    v = new MutableNumericValue();
    v.resetTimestamp(new MillisecondTimeStamp(3000));
    v.resetValue(1);
    interpolator.setNext(v);
    interpolator.setNext(null); // no more
    
    TimeSeriesValue<NumericType> value = 
        interpolator.next(new MillisecondTimeStamp(500));
    assertEquals(500, value.timestamp().msEpoch());
    assertTrue(Double.isNaN(value.value().doubleValue()));
    assertFalse(interpolator.hasNext());
    assertEquals(3, interpolator.previous.size());
    assertEquals(42, interpolator.previous.getFirst().value().longValue());
    assertEquals(24, interpolator.previous.get(1).value().longValue());
    assertEquals(1, interpolator.previous.getLast().value().longValue());
    assertNull(interpolator.next);
    
    value = interpolator.next(new MillisecondTimeStamp(1000));
    assertEquals(1000, value.timestamp().msEpoch());
    assertEquals(42, value.value().longValue());
    assertFalse(interpolator.hasNext());
    assertEquals(3, interpolator.previous.size());
    assertEquals(42, interpolator.previous.getFirst().value().longValue());
    assertEquals(24, interpolator.previous.get(1).value().longValue());
    assertEquals(1, interpolator.previous.getLast().value().longValue());
    assertNull(interpolator.next);
    
    value = interpolator.next(new MillisecondTimeStamp(1500));
    assertEquals(1500, value.timestamp().msEpoch());
    assertTrue(Double.isNaN(value.value().doubleValue()));
    assertFalse(interpolator.hasNext());
    assertEquals(3, interpolator.previous.size());
    assertEquals(42, interpolator.previous.getFirst().value().longValue());
    assertEquals(24, interpolator.previous.get(1).value().longValue());
    assertEquals(1, interpolator.previous.getLast().value().longValue());
    assertNull(interpolator.next);
    
    value = interpolator.next(new MillisecondTimeStamp(2000));
    assertEquals(2000, value.timestamp().msEpoch());
    assertEquals(24, value.value().longValue());
    assertFalse(interpolator.hasNext());
    assertEquals(2, interpolator.previous.size()); // pops the first value
    assertEquals(24, interpolator.previous.getFirst().value().longValue());
    assertEquals(1, interpolator.previous.getLast().value().longValue());
    assertNull(interpolator.next);
    
    value = interpolator.next(new MillisecondTimeStamp(3000));
    assertEquals(3000, value.timestamp().msEpoch());
    assertEquals(1, value.value().longValue());
    assertFalse(interpolator.hasNext());
    assertEquals(1, interpolator.previous.size()); // pops the first value
    assertEquals(1, interpolator.previous.getLast().value().longValue());
    assertNull(interpolator.next);
    
    value = interpolator.next(new MillisecondTimeStamp(4000));
    assertEquals(4000, value.timestamp().msEpoch());
    assertTrue(Double.isNaN(value.value().doubleValue()));
    assertFalse(interpolator.hasNext());
    assertEquals(1, interpolator.previous.size()); // pops the first value
    assertEquals(1, interpolator.previous.getLast().value().longValue());
    assertNull(interpolator.next);
  }
}