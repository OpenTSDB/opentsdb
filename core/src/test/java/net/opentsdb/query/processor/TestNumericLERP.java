// This file is part of OpenTSDB.
// Copyright (C) 017  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.query.processor;

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
import net.opentsdb.data.types.numeric.BaseNumericFillPolicy;
import net.opentsdb.data.types.numeric.NumericMillisecondShard;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.data.types.numeric.ScalarNumericFillPolicy;
import net.opentsdb.query.QueryFillPolicy;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.pojo.FillPolicy;

public class TestNumericLERP {
  private QueryFillPolicy<NumericType> fill;
  
  @Before
  public void before() throws Exception {
    fill = new BaseNumericFillPolicy(FillPolicy.NOT_A_NUMBER);
  }
  
  @Test
  public void ctor() throws Exception {
    NumericMillisecondShard source = new NumericMillisecondShard(
        BaseTimeSeriesId.newBuilder()
        .setMetric("foo")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(5000));
    source.add(1000, 1000);
    
    NumericLERP lerp = new NumericLERP(source, fill, FillWithRealPolicy.NONE);
    assertTrue(lerp.has_next);
    assertEquals(1000, lerp.nextReal().msEpoch());
    
    // empty source
    source = new NumericMillisecondShard(
        BaseTimeSeriesId.newBuilder()
        .setMetric("foo")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(5000));
    lerp = new NumericLERP(source, fill, FillWithRealPolicy.NONE);
    assertFalse(lerp.has_next);
    
    // no such type in source
    TimeSeries mock_source = mock(TimeSeries.class);
    when(mock_source.iterator(any(TypeToken.class)))
        .thenReturn(Optional.<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>>empty());
    lerp = new NumericLERP(source, fill, FillWithRealPolicy.NONE);
    assertFalse(lerp.has_next);
    
    try {
      new NumericLERP(null, fill, FillWithRealPolicy.NONE);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new NumericLERP(source, null, FillWithRealPolicy.NONE);
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
    
    final NumericLERP lerp = new NumericLERP(source, fill, FillWithRealPolicy.NONE);
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
    
    final NumericLERP lerp = new NumericLERP(source, fill, FillWithRealPolicy.NONE);
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
    
    final NumericLERP lerp = new NumericLERP(source, fill, FillWithRealPolicy.NONE);
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
    
    final NumericLERP lerp = new NumericLERP(source, fill, FillWithRealPolicy.NONE);
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
    
    final NumericLERP lerp = new NumericLERP(source, fill, FillWithRealPolicy.NONE);
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
    NumericMillisecondShard source = new NumericMillisecondShard(
        BaseTimeSeriesId.newBuilder()
        .setMetric("foo")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(5000));
    source.add(1000, 1);
    source.add(3000, 10);
    
    final NumericLERP lerp = new NumericLERP(source, fill, FillWithRealPolicy.PREVIOUS_ONLY);
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
    NumericMillisecondShard source = new NumericMillisecondShard(
        BaseTimeSeriesId.newBuilder()
        .setMetric("foo")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(5000));
    source.add(1000, 1);
    source.add(3000, 10);
    
    final NumericLERP lerp = new NumericLERP(source, fill, FillWithRealPolicy.NEXT_ONLY);
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
    NumericMillisecondShard source = new NumericMillisecondShard(
        BaseTimeSeriesId.newBuilder()
        .setMetric("foo")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(5000));
    source.add(1000, 1);
    source.add(3000, 10);
    
    final NumericLERP lerp = new NumericLERP(source, fill, FillWithRealPolicy.PREFER_PREVIOUS);
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
    NumericMillisecondShard source = new NumericMillisecondShard(
        BaseTimeSeriesId.newBuilder()
        .setMetric("foo")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(5000));
    source.add(1000, 1);
    source.add(3000, 10);
    
    final NumericLERP lerp = new NumericLERP(source, fill, FillWithRealPolicy.PREFER_NEXT);
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
    fill = new BaseNumericFillPolicy(FillPolicy.NONE);
    NumericMillisecondShard source = new NumericMillisecondShard(
        BaseTimeSeriesId.newBuilder()
        .setMetric("foo")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(5000));
    source.add(1000, 1);
    source.add(3000, 10);
    
    final NumericLERP lerp = new NumericLERP(source, fill, FillWithRealPolicy.NONE);
    assertTrue(lerp.has_next);
    TimeSeriesValue<NumericType> v = lerp.next(new MillisecondTimeStamp(500));
    assertNull(v);
    
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
    assertNull(v);
  }
  
  @Test
  public void fillNull() throws Exception {
    fill = new BaseNumericFillPolicy(FillPolicy.NULL);
    NumericMillisecondShard source = new NumericMillisecondShard(
        BaseTimeSeriesId.newBuilder()
        .setMetric("foo")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(5000));
    source.add(1000, 1);
    source.add(3000, 10);
    
    final NumericLERP lerp = new NumericLERP(source, fill, FillWithRealPolicy.NONE);
    assertTrue(lerp.has_next);
    TimeSeriesValue<NumericType> v = lerp.next(new MillisecondTimeStamp(500));
    assertNull(v);
    
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
    assertNull(v);
  }
  
  @Test
  public void fillZero() throws Exception {
    fill = new BaseNumericFillPolicy(FillPolicy.ZERO);
    NumericMillisecondShard source = new NumericMillisecondShard(
        BaseTimeSeriesId.newBuilder()
        .setMetric("foo")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(5000));
    source.add(1000, 1);
    source.add(3000, 10);
    
    final NumericLERP lerp = new NumericLERP(source, fill, FillWithRealPolicy.NONE);
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
    fill = new ScalarNumericFillPolicy(42);
    NumericMillisecondShard source = new NumericMillisecondShard(
        BaseTimeSeriesId.newBuilder()
        .setMetric("foo")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(5000));
    source.add(1000, 1);
    source.add(3000, 10);
    
    final NumericLERP lerp = new NumericLERP(source, fill, FillWithRealPolicy.NONE);
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
    
    final NumericLERP lerp = new NumericLERP(source, fill, FillWithRealPolicy.NONE);
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
    
    final NumericLERP lerp = new NumericLERP(mock_source, fill, FillWithRealPolicy.NONE);
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
