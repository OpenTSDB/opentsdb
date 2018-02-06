// This file is part of OpenTSDB.
// Copyright (C) 017  The OpenTSDB Authors.
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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.BaseTimeSeriesId;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.types.numeric.NumericMillisecondShard;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.interpolation.types.numeric.NumericLERP;
import net.opentsdb.query.interpolation.types.numeric.ScalarNumericInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;

public class TestNumericLERP {
  
  private NumericInterpolatorConfig config;
  
  @Before
  public void before() throws Exception {
    config = NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setRealFillPolicy(FillWithRealPolicy.NONE)
        .build();
  }
  
  @Test
  public void ctor() throws Exception {
    NumericMillisecondShard source = new NumericMillisecondShard(
        BaseTimeSeriesId.newBuilder()
        .setMetric("foo")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(5000));
    source.add(1000, 1000);
    
    NumericLERP lerp = new NumericLERP(source, config);
    assertTrue(lerp.has_next);
    assertEquals(1000, lerp.nextReal().msEpoch());
    
    // empty source
    source = new NumericMillisecondShard(
        BaseTimeSeriesId.newBuilder()
        .setMetric("foo")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(5000));
    lerp = new NumericLERP(source, config);
    assertFalse(lerp.has_next);
    
    // no such type in source
    TimeSeries mock_source = mock(TimeSeries.class);
    when(mock_source.iterator(any(TypeToken.class)))
        .thenReturn(Optional.<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>>empty());
    lerp = new NumericLERP(source, config);
    assertFalse(lerp.has_next);
    
    try {
      new NumericLERP(null, config);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new NumericLERP(source, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void lerpIntegers() throws Exception {
    NumericMillisecondShard source = new NumericMillisecondShard(
        BaseTimeSeriesId.newBuilder()
        .setMetric("foo")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(5000));
    source.add(1000, 1);
    source.add(3000, 10);
    
    final NumericLERP lerp = new NumericLERP(source, config);
    assertTrue(lerp.has_next);
    TimeSeriesValue<NumericType> v = lerp.next(new MillisecondTimeStamp(500));
    assertEquals(500, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    assertEquals(1000, lerp.nextReal().msEpoch());
    
    assertTrue(lerp.has_next);
    v = lerp.next(new MillisecondTimeStamp(1000));
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    assertEquals(3000, lerp.nextReal().msEpoch());
    
    assertTrue(lerp.has_next);
    v = lerp.next(new MillisecondTimeStamp(2000));
    assertEquals(2000, v.timestamp().msEpoch());
    assertEquals(5, v.value().longValue());
    assertEquals(3000, lerp.nextReal().msEpoch());
    
    assertTrue(lerp.has_next);
    v = lerp.next(new MillisecondTimeStamp(3000));
    assertEquals(3000, v.timestamp().msEpoch());
    assertEquals(10, v.value().longValue());
    try {
      lerp.nextReal();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
    
    assertFalse(lerp.has_next);
    v = lerp.next(new MillisecondTimeStamp(3500));
    assertEquals(3500, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
  }
  
  @Test
  public void lerpIntegersPrecise() throws Exception {
    NumericMillisecondShard source = new NumericMillisecondShard(
        BaseTimeSeriesId.newBuilder()
        .setMetric("foo")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(5000));
    source.add(1000, 1000);
    source.add(3000, 3001);
    
    final NumericLERP lerp = new NumericLERP(source, config);
    assertTrue(lerp.has_next);
    TimeSeriesValue<NumericType> v = lerp.next(new MillisecondTimeStamp(500));
    assertEquals(500, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    assertEquals(1000, lerp.nextReal().msEpoch());
    
    assertTrue(lerp.has_next);
    v = lerp.next(new MillisecondTimeStamp(1000));
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1000, v.value().longValue());
    assertEquals(3000, lerp.nextReal().msEpoch());
    
    assertTrue(lerp.has_next);
    v = lerp.next(new MillisecondTimeStamp(2000));
    assertEquals(2000, v.timestamp().msEpoch());
    assertEquals(2000, v.value().longValue());
    assertEquals(3000, lerp.nextReal().msEpoch());
    
    assertTrue(lerp.has_next);
    v = lerp.next(new MillisecondTimeStamp(3000));
    assertEquals(3000, v.timestamp().msEpoch());
    assertEquals(3001, v.value().longValue());
    try {
      lerp.nextReal();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
    
    assertFalse(lerp.has_next);
    v = lerp.next(new MillisecondTimeStamp(3500));
    assertEquals(3500, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
  }
  
  @Test
  public void lerpIntegersAlmostMax() throws Exception {
    NumericMillisecondShard source = new NumericMillisecondShard(
        BaseTimeSeriesId.newBuilder()
        .setMetric("foo")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(5000));
    source.add(1000, Long.MAX_VALUE - 19);
    source.add(3000, Long.MAX_VALUE - 10);
    
    final NumericLERP lerp = new NumericLERP(source, config);
    assertTrue(lerp.has_next);
    TimeSeriesValue<NumericType> v = lerp.next(new MillisecondTimeStamp(500));
    assertEquals(500, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    assertEquals(1000, lerp.nextReal().msEpoch());
    
    assertTrue(lerp.has_next);
    v = lerp.next(new MillisecondTimeStamp(1000));
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(Long.MAX_VALUE - 19, v.value().longValue());
    assertEquals(3000, lerp.nextReal().msEpoch());
    
    assertTrue(lerp.has_next);
    v = lerp.next(new MillisecondTimeStamp(2000));
    assertEquals(2000, v.timestamp().msEpoch());
    assertEquals(Long.MAX_VALUE - 15, v.value().longValue());
    assertEquals(3000, lerp.nextReal().msEpoch());
    
    assertTrue(lerp.has_next);
    v = lerp.next(new MillisecondTimeStamp(3000));
    assertEquals(3000, v.timestamp().msEpoch());
    assertEquals(Long.MAX_VALUE - 10, v.value().longValue());
    try {
      lerp.nextReal();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
    
    assertFalse(lerp.has_next);
    v = lerp.next(new MillisecondTimeStamp(3500));
    assertEquals(3500, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
  }
  
  @Test
  public void lerpIntegerThenFloat() throws Exception {
    NumericMillisecondShard source = new NumericMillisecondShard(
        BaseTimeSeriesId.newBuilder()
        .setMetric("foo")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(5000));
    source.add(1000, 1);
    source.add(3000, 10.5);
    
    final NumericLERP lerp = new NumericLERP(source, config);
    assertTrue(lerp.has_next);
    TimeSeriesValue<NumericType> v = lerp.next(new MillisecondTimeStamp(500));
    assertEquals(500, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    assertEquals(1000, lerp.nextReal().msEpoch());
    
    assertTrue(lerp.has_next);
    v = lerp.next(new MillisecondTimeStamp(1000));
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    assertEquals(3000, lerp.nextReal().msEpoch());
    
    assertTrue(lerp.has_next);
    v = lerp.next(new MillisecondTimeStamp(2000));
    assertEquals(2000, v.timestamp().msEpoch());
    assertEquals(5.75, v.value().doubleValue(), 0.001);
    assertEquals(3000, lerp.nextReal().msEpoch());
    
    assertTrue(lerp.has_next);
    v = lerp.next(new MillisecondTimeStamp(3000));
    assertEquals(3000, v.timestamp().msEpoch());
    assertEquals(10.5, v.value().doubleValue(), 0.001);
    try {
      lerp.nextReal();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
    
    assertFalse(lerp.has_next);
    v = lerp.next(new MillisecondTimeStamp(3500));
    assertEquals(3500, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
  }
  
  @Test
  public void lerpFloats() throws Exception {
    NumericMillisecondShard source = new NumericMillisecondShard(
        BaseTimeSeriesId.newBuilder()
        .setMetric("foo")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(5000));
    source.add(1000, 1.5);
    source.add(3000, 10.5);
    
    final NumericLERP lerp = new NumericLERP(source, config);
    assertTrue(lerp.has_next);
    TimeSeriesValue<NumericType> v = lerp.next(new MillisecondTimeStamp(500));
    assertEquals(500, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    assertEquals(1000, lerp.nextReal().msEpoch());
    
    assertTrue(lerp.has_next);
    v = lerp.next(new MillisecondTimeStamp(1000));
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1.5, v.value().doubleValue(), 0.001);
    assertEquals(3000, lerp.nextReal().msEpoch());
    
    assertTrue(lerp.has_next);
    v = lerp.next(new MillisecondTimeStamp(2000));
    assertEquals(2000, v.timestamp().msEpoch());
    assertEquals(6.0, v.value().doubleValue(), 0.001);
    assertEquals(3000, lerp.nextReal().msEpoch());
    
    assertTrue(lerp.has_next);
    v = lerp.next(new MillisecondTimeStamp(3000));
    assertEquals(3000, v.timestamp().msEpoch());
    assertEquals(10.5, v.value().doubleValue(), 0.001);
    try {
      lerp.nextReal();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
    
    assertFalse(lerp.has_next);
    v = lerp.next(new MillisecondTimeStamp(3500));
    assertEquals(3500, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
  }
  
  @Test
  public void previousOnly() throws Exception {
    config = NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setRealFillPolicy(FillWithRealPolicy.PREVIOUS_ONLY)
        .build();
    NumericMillisecondShard source = new NumericMillisecondShard(
        BaseTimeSeriesId.newBuilder()
        .setMetric("foo")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(5000));
    source.add(1000, 1);
    source.add(3000, 10);
    
    final NumericLERP lerp = new NumericLERP(source, config);
    assertTrue(lerp.has_next);
    TimeSeriesValue<NumericType> v = lerp.next(new MillisecondTimeStamp(500));
    assertEquals(500, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    assertEquals(1000, lerp.nextReal().msEpoch());
    
    assertTrue(lerp.has_next);
    v = lerp.next(new MillisecondTimeStamp(1000));
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    assertEquals(3000, lerp.nextReal().msEpoch());
    
    assertTrue(lerp.has_next);
    v = lerp.next(new MillisecondTimeStamp(2000));
    assertEquals(2000, v.timestamp().msEpoch());
    assertEquals(5, v.value().longValue());
    assertEquals(3000, lerp.nextReal().msEpoch());
    
    assertTrue(lerp.has_next);
    v = lerp.next(new MillisecondTimeStamp(3000));
    assertEquals(3000, v.timestamp().msEpoch());
    assertEquals(10, v.value().longValue());
    try {
      lerp.nextReal();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
    
    assertFalse(lerp.has_next);
    v = lerp.next(new MillisecondTimeStamp(3500));
    assertEquals(3500, v.timestamp().msEpoch());
    assertEquals(10, v.value().longValue());
  }
  
  @Test
  public void nextOnly() throws Exception {
    config = NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setRealFillPolicy(FillWithRealPolicy.NEXT_ONLY)
        .build();
    NumericMillisecondShard source = new NumericMillisecondShard(
        BaseTimeSeriesId.newBuilder()
        .setMetric("foo")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(5000));
    source.add(1000, 1);
    source.add(3000, 10);
    
    final NumericLERP lerp = new NumericLERP(source, config);
    assertTrue(lerp.has_next);
    TimeSeriesValue<NumericType> v = lerp.next(new MillisecondTimeStamp(500));
    assertEquals(500, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    assertEquals(1000, lerp.nextReal().msEpoch());
    
    assertTrue(lerp.has_next);
    v = lerp.next(new MillisecondTimeStamp(1000));
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    assertEquals(3000, lerp.nextReal().msEpoch());
    
    assertTrue(lerp.has_next);
    v = lerp.next(new MillisecondTimeStamp(2000));
    assertEquals(2000, v.timestamp().msEpoch());
    assertEquals(5, v.value().longValue());
    assertEquals(3000, lerp.nextReal().msEpoch());
    
    assertTrue(lerp.has_next);
    v = lerp.next(new MillisecondTimeStamp(3000));
    assertEquals(3000, v.timestamp().msEpoch());
    assertEquals(10, v.value().longValue());
    try {
      lerp.nextReal();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
    
    assertFalse(lerp.has_next);
    v = lerp.next(new MillisecondTimeStamp(3500));
    assertEquals(3500, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
  }

  @Test
  public void preferPrevious() throws Exception {
    config = NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setRealFillPolicy(FillWithRealPolicy.PREFER_PREVIOUS)
        .build();
    NumericMillisecondShard source = new NumericMillisecondShard(
        BaseTimeSeriesId.newBuilder()
        .setMetric("foo")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(5000));
    source.add(1000, 1);
    source.add(3000, 10);
    
    final NumericLERP lerp = new NumericLERP(source, config);
    assertTrue(lerp.has_next);
    TimeSeriesValue<NumericType> v = lerp.next(new MillisecondTimeStamp(500));
    assertEquals(500, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    assertEquals(1000, lerp.nextReal().msEpoch());
    
    assertTrue(lerp.has_next);
    v = lerp.next(new MillisecondTimeStamp(1000));
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    assertEquals(3000, lerp.nextReal().msEpoch());
    
    assertTrue(lerp.has_next);
    v = lerp.next(new MillisecondTimeStamp(2000));
    assertEquals(2000, v.timestamp().msEpoch());
    assertEquals(5, v.value().longValue());
    assertEquals(3000, lerp.nextReal().msEpoch());
    
    assertTrue(lerp.has_next);
    v = lerp.next(new MillisecondTimeStamp(3000));
    assertEquals(3000, v.timestamp().msEpoch());
    assertEquals(10, v.value().longValue());
    try {
      lerp.nextReal();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
    
    assertFalse(lerp.has_next);
    v = lerp.next(new MillisecondTimeStamp(3500));
    assertEquals(3500, v.timestamp().msEpoch());
    assertEquals(10, v.value().longValue());
  }
  
  @Test
  public void preferNext() throws Exception {
    config = NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
        .build();
    NumericMillisecondShard source = new NumericMillisecondShard(
        BaseTimeSeriesId.newBuilder()
        .setMetric("foo")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(5000));
    source.add(1000, 1);
    source.add(3000, 10);
    
    final NumericLERP lerp = new NumericLERP(source, config);
    assertTrue(lerp.has_next);
    TimeSeriesValue<NumericType> v = lerp.next(new MillisecondTimeStamp(500));
    assertEquals(500, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    assertEquals(1000, lerp.nextReal().msEpoch());
    
    assertTrue(lerp.has_next);
    v = lerp.next(new MillisecondTimeStamp(1000));
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    assertEquals(3000, lerp.nextReal().msEpoch());
    
    assertTrue(lerp.has_next);
    v = lerp.next(new MillisecondTimeStamp(2000));
    assertEquals(2000, v.timestamp().msEpoch());
    assertEquals(5, v.value().longValue());
    assertEquals(3000, lerp.nextReal().msEpoch());
    
    assertTrue(lerp.has_next);
    v = lerp.next(new MillisecondTimeStamp(3000));
    assertEquals(3000, v.timestamp().msEpoch());
    assertEquals(10, v.value().longValue());
    try {
      lerp.nextReal();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
    
    assertFalse(lerp.has_next);
    v = lerp.next(new MillisecondTimeStamp(3500));
    assertEquals(3500, v.timestamp().msEpoch());
    assertEquals(10, v.value().longValue());
  }
  
  @Test
  public void fillNone() throws Exception {
    config = NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(FillPolicy.NONE)
        .setRealFillPolicy(FillWithRealPolicy.NONE)
        .build();
    NumericMillisecondShard source = new NumericMillisecondShard(
        BaseTimeSeriesId.newBuilder()
        .setMetric("foo")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(5000));
    source.add(1000, 1);
    source.add(3000, 10);
    
    final NumericLERP lerp = new NumericLERP(source, config);
    assertTrue(lerp.has_next);
    TimeSeriesValue<NumericType> v = lerp.next(new MillisecondTimeStamp(500));
    assertEquals(500, v.timestamp().msEpoch());
    assertNull(v.value());
    assertEquals(1000, lerp.nextReal().msEpoch());
    
    assertTrue(lerp.has_next);
    v = lerp.next(new MillisecondTimeStamp(1000));
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    assertEquals(3000, lerp.nextReal().msEpoch());
    
    assertTrue(lerp.has_next);
    v = lerp.next(new MillisecondTimeStamp(2000));
    assertEquals(2000, v.timestamp().msEpoch());
    assertEquals(5, v.value().longValue());
    assertEquals(3000, lerp.nextReal().msEpoch());
    
    assertTrue(lerp.has_next);
    v = lerp.next(new MillisecondTimeStamp(3000));
    assertEquals(3000, v.timestamp().msEpoch());
    assertEquals(10, v.value().longValue());
    try {
      lerp.nextReal();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
    
    assertFalse(lerp.has_next);
    v = lerp.next(new MillisecondTimeStamp(3500));
    assertEquals(3500, v.timestamp().msEpoch());
    assertNull(v.value());
  }
  
  @Test
  public void fillNull() throws Exception {
    config = NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(FillPolicy.NONE)
        .setRealFillPolicy(FillWithRealPolicy.NONE)
        .build();
    NumericMillisecondShard source = new NumericMillisecondShard(
        BaseTimeSeriesId.newBuilder()
        .setMetric("foo")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(5000));
    source.add(1000, 1);
    source.add(3000, 10);
    
    final NumericLERP lerp = new NumericLERP(source, config);
    assertTrue(lerp.has_next);
    TimeSeriesValue<NumericType> v = lerp.next(new MillisecondTimeStamp(500));
    assertEquals(500, v.timestamp().msEpoch());
    assertNull(v.value());
    assertEquals(1000, lerp.nextReal().msEpoch());
    
    assertTrue(lerp.has_next);
    v = lerp.next(new MillisecondTimeStamp(1000));
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    assertEquals(3000, lerp.nextReal().msEpoch());
    
    assertTrue(lerp.has_next);
    v = lerp.next(new MillisecondTimeStamp(2000));
    assertEquals(2000, v.timestamp().msEpoch());
    assertEquals(5, v.value().longValue());
    assertEquals(3000, lerp.nextReal().msEpoch());
    
    assertTrue(lerp.has_next);
    v = lerp.next(new MillisecondTimeStamp(3000));
    assertEquals(3000, v.timestamp().msEpoch());
    assertEquals(10, v.value().longValue());
    try {
      lerp.nextReal();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
    
    assertFalse(lerp.has_next);
    v = lerp.next(new MillisecondTimeStamp(3500));
    assertEquals(3500, v.timestamp().msEpoch());
    assertNull(v.value());
  }
  
  @Test
  public void fillZero() throws Exception {
    config = NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(FillPolicy.ZERO)
        .setRealFillPolicy(FillWithRealPolicy.NONE)
        .build();
    NumericMillisecondShard source = new NumericMillisecondShard(
        BaseTimeSeriesId.newBuilder()
        .setMetric("foo")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(5000));
    source.add(1000, 1);
    source.add(3000, 10);
    
    final NumericLERP lerp = new NumericLERP(source, config);
    assertTrue(lerp.has_next);
    TimeSeriesValue<NumericType> v = lerp.next(new MillisecondTimeStamp(500));
    assertEquals(500, v.timestamp().msEpoch());
    assertEquals(0, v.value().longValue());
    
    assertTrue(lerp.has_next);
    v = lerp.next(new MillisecondTimeStamp(1000));
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    assertEquals(3000, lerp.nextReal().msEpoch());
    
    assertTrue(lerp.has_next);
    v = lerp.next(new MillisecondTimeStamp(2000));
    assertEquals(2000, v.timestamp().msEpoch());
    assertEquals(5, v.value().longValue());
    assertEquals(3000, lerp.nextReal().msEpoch());
    
    assertTrue(lerp.has_next);
    v = lerp.next(new MillisecondTimeStamp(3000));
    assertEquals(3000, v.timestamp().msEpoch());
    assertEquals(10, v.value().longValue());
    try {
      lerp.nextReal();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
    
    assertFalse(lerp.has_next);
    v = lerp.next(new MillisecondTimeStamp(3500));
    assertEquals(3500, v.timestamp().msEpoch());
    assertEquals(0, v.value().longValue());
  }
  
  @Test
  public void fillScalar() throws Exception {
    config = ScalarNumericInterpolatorConfig.newBuilder()
        .setValue(42)
        .setFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setRealFillPolicy(FillWithRealPolicy.NONE)
        .build();
    NumericMillisecondShard source = new NumericMillisecondShard(
        BaseTimeSeriesId.newBuilder()
        .setMetric("foo")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(5000));
    source.add(1000, 1);
    source.add(3000, 10);
    
    final NumericLERP lerp = new NumericLERP(source, config);
    assertTrue(lerp.has_next);
    TimeSeriesValue<NumericType> v = lerp.next(new MillisecondTimeStamp(500));
    assertEquals(500, v.timestamp().msEpoch());
    assertEquals(42, v.value().longValue());
    
    assertTrue(lerp.has_next);
    v = lerp.next(new MillisecondTimeStamp(1000));
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().longValue());
    assertEquals(3000, lerp.nextReal().msEpoch());
    
    assertTrue(lerp.has_next);
    v = lerp.next(new MillisecondTimeStamp(2000));
    assertEquals(2000, v.timestamp().msEpoch());
    assertEquals(5, v.value().longValue());
    assertEquals(3000, lerp.nextReal().msEpoch());
    
    assertTrue(lerp.has_next);
    v = lerp.next(new MillisecondTimeStamp(3000));
    assertEquals(3000, v.timestamp().msEpoch());
    assertEquals(10, v.value().longValue());
    try {
      lerp.nextReal();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
    
    assertFalse(lerp.has_next);
    v = lerp.next(new MillisecondTimeStamp(3500));
    assertEquals(3500, v.timestamp().msEpoch());
    assertEquals(42, v.value().longValue());
  }
  
  @Test
  public void emptySource() throws Exception {
    NumericMillisecondShard source = new NumericMillisecondShard(
        BaseTimeSeriesId.newBuilder()
        .setMetric("foo")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(5000));
    
    final NumericLERP lerp = new NumericLERP(source, config);
    assertFalse(lerp.has_next);
    TimeSeriesValue<NumericType> v = lerp.next(new MillisecondTimeStamp(500));
    assertEquals(500, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    try {
      lerp.nextReal();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
    
    assertFalse(lerp.has_next);
    v = lerp.next(new MillisecondTimeStamp(1000));
    assertEquals(1000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    try {
      lerp.nextReal();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
    
    assertFalse(lerp.has_next);
    v = lerp.next(new MillisecondTimeStamp(2000));
    assertEquals(2000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    try {
      lerp.nextReal();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
  }

  @Test
  public void noIterator() throws Exception {
    final TimeSeries mock_source = mock(TimeSeries.class);
    when(mock_source.iterator(any(TypeToken.class)))
        .thenReturn(Optional.<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>>empty());
    
    final NumericLERP lerp = new NumericLERP(mock_source, config);
    assertFalse(lerp.has_next);
    TimeSeriesValue<NumericType> v = lerp.next(new MillisecondTimeStamp(500));
    assertEquals(500, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    try {
      lerp.nextReal();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
    
    assertFalse(lerp.has_next);
    v = lerp.next(new MillisecondTimeStamp(1000));
    assertEquals(1000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    try {
      lerp.nextReal();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
    
    assertFalse(lerp.has_next);
    v = lerp.next(new MillisecondTimeStamp(2000));
    assertEquals(2000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    try {
      lerp.nextReal();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
  }

}
