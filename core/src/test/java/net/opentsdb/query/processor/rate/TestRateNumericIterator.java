// This file is part of OpenTSDB.
// Copyright (C) 2014-2017 The OpenTSDB Authors.
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
package net.opentsdb.query.processor.rate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;

import net.opentsdb.common.Const;
import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.MockTimeSeries;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.ZonedNanoTimeStamp;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericMillisecondShard;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.TimeSeriesQuery;
import net.opentsdb.query.pojo.RateOptions;

import org.junit.Test;

import com.google.common.collect.Lists;

public class TestRateNumericIterator {
  private TimeSeries source;
  private TimeSeriesQuery query;
  private RateOptions config;
  private QueryNode node;
  private QueryContext query_context;
  private QueryPipelineContext pipeline_context;
  
  private static final long BASE_TIME = 1356998400000L;
  
  @Test
  public void oneSecondLongs() {
    source = new NumericMillisecondShard(BaseTimeSeriesStringId.newBuilder()
          .setMetric("a")
          .build(), 
        new MillisecondTimeStamp(BASE_TIME), 
        new MillisecondTimeStamp(BASE_TIME + 10000000));
    ((NumericMillisecondShard) source).add(BASE_TIME, 40);
    ((NumericMillisecondShard) source).add(BASE_TIME + 2000000L, 50);
    ((NumericMillisecondShard) source).add(BASE_TIME + 3600000L, 40);
    ((NumericMillisecondShard) source).add(BASE_TIME + 3605000L, 50);
    ((NumericMillisecondShard) source).add(BASE_TIME + 7200000L, 40);
    ((NumericMillisecondShard) source).add(BASE_TIME + 9200000L, 50);
    
    config = RateOptions.newBuilder()
        .setId("foo")
        .setInterval("1s")
        .setCounter(true)
        .build();
    
    setupMock();
    RateNumericIterator it = new RateNumericIterator(node, source);
    
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 2000000L, v.timestamp().msEpoch());
    assertEquals(0.005, v.value().doubleValue(), 0.001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 3600000L, v.timestamp().msEpoch());
    assertEquals(5.764607523034235E15, v.value().doubleValue(), 0.001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 3605000L, v.timestamp().msEpoch());
    assertEquals(2.0, v.value().doubleValue(), 0.001);

    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 7200000L, v.timestamp().msEpoch());
    assertEquals(2.5656111368163495E15, v.value().doubleValue(), 0.001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 9200000L, v.timestamp().msEpoch());
    assertEquals(0.005, v.value().doubleValue(), 0.001);
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void oneSecondDoubles() {
    source = new NumericMillisecondShard(BaseTimeSeriesStringId.newBuilder()
          .setMetric("a")
          .build(), 
        new MillisecondTimeStamp(BASE_TIME), 
        new MillisecondTimeStamp(BASE_TIME + 10000000));
    ((NumericMillisecondShard) source).add(BASE_TIME, 40.5);
    ((NumericMillisecondShard) source).add(BASE_TIME + 2000000L, 50.5);
    ((NumericMillisecondShard) source).add(BASE_TIME + 3600000L, 40.5);
    ((NumericMillisecondShard) source).add(BASE_TIME + 3605000L, 50.5);
    ((NumericMillisecondShard) source).add(BASE_TIME + 7200000L, 40.5);
    ((NumericMillisecondShard) source).add(BASE_TIME + 9200000L, 50.5);
    
    config = RateOptions.newBuilder()
        .setId("foo")
        .setInterval("1s")
        .setCounter(true)
        .build();
    
    setupMock();
    RateNumericIterator it = new RateNumericIterator(node, source);
    
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 2000000L, v.timestamp().msEpoch());
    assertEquals(0.005, v.value().doubleValue(), 0.001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 3600000L, v.timestamp().msEpoch());
    assertEquals(5.764607523034235E15, v.value().doubleValue(), 0.001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 3605000L, v.timestamp().msEpoch());
    assertEquals(2.0, v.value().doubleValue(), 0.001);

    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 7200000L, v.timestamp().msEpoch());
    assertEquals(2.5656111368163495E15, v.value().doubleValue(), 0.001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 9200000L, v.timestamp().msEpoch());
    assertEquals(0.005, v.value().doubleValue(), 0.001);
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void oneSecondMix() {
    source = new NumericMillisecondShard(BaseTimeSeriesStringId.newBuilder()
          .setMetric("a")
          .build(), 
        new MillisecondTimeStamp(BASE_TIME), 
        new MillisecondTimeStamp(BASE_TIME + 10000000));
    ((NumericMillisecondShard) source).add(BASE_TIME, 40);
    ((NumericMillisecondShard) source).add(BASE_TIME + 2000000L, 50.5);
    ((NumericMillisecondShard) source).add(BASE_TIME + 3600000L, 40);
    ((NumericMillisecondShard) source).add(BASE_TIME + 3605000L, 50.5);
    ((NumericMillisecondShard) source).add(BASE_TIME + 7200000L, 40);
    ((NumericMillisecondShard) source).add(BASE_TIME + 9200000L, 50.5);
    
    config = RateOptions.newBuilder()
        .setId("foo")
        .setInterval("1s")
        .setCounter(true)
        .build();
    
    setupMock();
    RateNumericIterator it = new RateNumericIterator(node, source);
    
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 2000000L, v.timestamp().msEpoch());
    assertEquals(0.005, v.value().doubleValue(), 0.001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 3600000L, v.timestamp().msEpoch());
    assertEquals(5.764607523034235E15, v.value().doubleValue(), 0.001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 3605000L, v.timestamp().msEpoch());
    assertEquals(2.1, v.value().doubleValue(), 0.001);

    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 7200000L, v.timestamp().msEpoch());
    assertEquals(2.5656111368163495E15, v.value().doubleValue(), 0.001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 9200000L, v.timestamp().msEpoch());
    assertEquals(0.005, v.value().doubleValue(), 0.001);
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void oneNano() {
    source = new NumericMillisecondShard(BaseTimeSeriesStringId.newBuilder()
          .setMetric("a")
          .build(), 
        new MillisecondTimeStamp(BASE_TIME), 
        new MillisecondTimeStamp(BASE_TIME + 10000000));
    ((NumericMillisecondShard) source).add(BASE_TIME, 40);
    ((NumericMillisecondShard) source).add(BASE_TIME + 2000000L, 50);
    ((NumericMillisecondShard) source).add(BASE_TIME + 3600000L, 40);
    ((NumericMillisecondShard) source).add(BASE_TIME + 3605000L, 50);
    ((NumericMillisecondShard) source).add(BASE_TIME + 7200000L, 40);
    ((NumericMillisecondShard) source).add(BASE_TIME + 9200000L, 50);
    
    config = RateOptions.newBuilder()
        .setId("foo")
        .setInterval("1ns")
        .setCounter(true)
        .build();
    
    setupMock();
    RateNumericIterator it = new RateNumericIterator(node, source);
    
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 2000000L, v.timestamp().msEpoch());
    assertEquals(5.0E-12, v.value().doubleValue(), 0.001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 3600000L, v.timestamp().msEpoch());
    assertEquals(5764607.5230342345, v.value().doubleValue(), 0.001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 3605000L, v.timestamp().msEpoch());
    assertEquals(2.0E-9, v.value().doubleValue(), 0.001);

    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 7200000L, v.timestamp().msEpoch());
    assertEquals(2565611.1368163493, v.value().doubleValue(), 0.001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 9200000L, v.timestamp().msEpoch());
    assertEquals(5.0E-12, v.value().doubleValue(), 0.001);
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void tenSeconds() {
    source = new NumericMillisecondShard(BaseTimeSeriesStringId.newBuilder()
          .setMetric("a")
          .build(), 
        new MillisecondTimeStamp(BASE_TIME), 
        new MillisecondTimeStamp(BASE_TIME + 10000000));
    ((NumericMillisecondShard) source).add(BASE_TIME, 40);
    ((NumericMillisecondShard) source).add(BASE_TIME + 2000000L, 50);
    ((NumericMillisecondShard) source).add(BASE_TIME + 3600000L, 40);
    ((NumericMillisecondShard) source).add(BASE_TIME + 3605000L, 50);
    ((NumericMillisecondShard) source).add(BASE_TIME + 7200000L, 40);
    ((NumericMillisecondShard) source).add(BASE_TIME + 9200000L, 50);
    
    config = RateOptions.newBuilder()
        .setId("foo")
        .setInterval("10s")
        .setCounter(true)
        .build();
    
    setupMock();
    RateNumericIterator it = new RateNumericIterator(node, source);
    
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 2000000L, v.timestamp().msEpoch());
    assertEquals(0.05, v.value().doubleValue(), 0.001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 3600000L, v.timestamp().msEpoch());
    assertEquals(5.7646075230342352E16, v.value().doubleValue(), 0.001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 3605000L, v.timestamp().msEpoch());
    assertEquals(20.0, v.value().doubleValue(), 0.001);

    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 7200000L, v.timestamp().msEpoch());
    assertEquals(2.5656111368163492E16, v.value().doubleValue(), 0.001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 9200000L, v.timestamp().msEpoch());
    assertEquals(0.05, v.value().doubleValue(), 0.001);
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void oneMinute() {
    source = new NumericMillisecondShard(BaseTimeSeriesStringId.newBuilder()
          .setMetric("a")
          .build(), 
        new MillisecondTimeStamp(BASE_TIME), 
        new MillisecondTimeStamp(BASE_TIME + 10000000));
    ((NumericMillisecondShard) source).add(BASE_TIME, 40);
    ((NumericMillisecondShard) source).add(BASE_TIME + 2000000L, 50);
    ((NumericMillisecondShard) source).add(BASE_TIME + 3600000L, 40);
    ((NumericMillisecondShard) source).add(BASE_TIME + 3605000L, 50);
    ((NumericMillisecondShard) source).add(BASE_TIME + 7200000L, 40);
    ((NumericMillisecondShard) source).add(BASE_TIME + 9200000L, 50);
    
    config = RateOptions.newBuilder()
        .setId("foo")
        .setInterval("1m")
        .setCounter(true)
        .build();
    
    setupMock();
    RateNumericIterator it = new RateNumericIterator(node, source);
    
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 2000000L, v.timestamp().msEpoch());
    assertEquals(0.3, v.value().doubleValue(), 0.001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 3600000L, v.timestamp().msEpoch());
    assertEquals(3.4587645138205408E17, v.value().doubleValue(), 0.001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 3605000L, v.timestamp().msEpoch());
    assertEquals(120.0, v.value().doubleValue(), 0.001);

    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 7200000L, v.timestamp().msEpoch());
    assertEquals(1.5393666820898096E17, v.value().doubleValue(), 0.001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 9200000L, v.timestamp().msEpoch());
    assertEquals(0.3, v.value().doubleValue(), 0.001);
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void nullsAtStart() {
    source = new MockTimeSeries(BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    MutableNumericValue nully = new MutableNumericValue();
    nully.resetNull(new MillisecondTimeStamp(BASE_TIME ));
    ((MockTimeSeries) source).addValue(nully);
    //((MockTimeSeries) source).addValue(new MutableNumericType(
    //    new MillisecondTimeStamp(BASE_TIME ), 40));
    nully = new MutableNumericValue();
    nully.resetNull(new MillisecondTimeStamp(BASE_TIME + 2000000L));
    ((MockTimeSeries) source).addValue(nully);
    //((MockTimeSeries) source).addValue(new MutableNumericType(
    //    new MillisecondTimeStamp(BASE_TIME + 2000000L), 50));
    ((MockTimeSeries) source).addValue(new MutableNumericValue(
        new MillisecondTimeStamp(BASE_TIME + 3600000L), 40));
    ((MockTimeSeries) source).addValue(new MutableNumericValue(
        new MillisecondTimeStamp(BASE_TIME + 3605000L), 50));
    ((MockTimeSeries) source).addValue(new MutableNumericValue(
        new MillisecondTimeStamp(BASE_TIME + 7200000L), 40));
    ((MockTimeSeries) source).addValue(new MutableNumericValue(
        new MillisecondTimeStamp(BASE_TIME + 9200000L), 50));
    
    config = RateOptions.newBuilder()
        .setId("foo")
        .setInterval("1s")
        .setCounter(true)
        .setCounterMax(70)
        .build();
    
    setupMock();
    RateNumericIterator it = new RateNumericIterator(node, source);

    assertTrue(it.hasNext());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 3605000L, v.timestamp().msEpoch());
    assertEquals(2.0, v.value().doubleValue(), 0.001);

    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 7200000L, v.timestamp().msEpoch());
    assertEquals(0.016689847009735744, v.value().doubleValue(), 0.001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 9200000L, v.timestamp().msEpoch());
    assertEquals(0.005, v.value().doubleValue(), 0.001);
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void nullsInMiddle() {
    source = new MockTimeSeries(BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    ((MockTimeSeries) source).addValue(new MutableNumericValue(
        new MillisecondTimeStamp(BASE_TIME ), 40));
    ((MockTimeSeries) source).addValue(new MutableNumericValue(
        new MillisecondTimeStamp(BASE_TIME + 2000000L), 50));
    MutableNumericValue nully = new MutableNumericValue();
    nully.resetNull(new MillisecondTimeStamp(BASE_TIME + 3600000L));
    ((MockTimeSeries) source).addValue(nully);
    //((MockTimeSeries) source).addValue(new MutableNumericType(
    //    new MillisecondTimeStamp(BASE_TIME + 3600000L), 40));
    nully = new MutableNumericValue();
    nully.resetNull(new MillisecondTimeStamp(BASE_TIME + 3605000L));
    ((MockTimeSeries) source).addValue(nully);
    //((MockTimeSeries) source).addValue(new MutableNumericType(
    //    new MillisecondTimeStamp(BASE_TIME + 3605000L), 50));
    ((MockTimeSeries) source).addValue(new MutableNumericValue(
        new MillisecondTimeStamp(BASE_TIME + 7200000L), 40));
    ((MockTimeSeries) source).addValue(new MutableNumericValue(
        new MillisecondTimeStamp(BASE_TIME + 9200000L), 50));
    
    config = RateOptions.newBuilder()
        .setId("foo")
        .setInterval("1s")
        .setCounter(true)
        .setCounterMax(70)
        .build();
    
    setupMock();
    RateNumericIterator it = new RateNumericIterator(node, source);

    assertTrue(it.hasNext());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 2000000L, v.timestamp().msEpoch());
    assertEquals(0.005, v.value().doubleValue(), 0.001);

    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 7200000L, v.timestamp().msEpoch());
    assertEquals(0.011538461538461539, v.value().doubleValue(), 0.001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 9200000L, v.timestamp().msEpoch());
    assertEquals(0.005, v.value().doubleValue(), 0.001);
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void nullsAtEnd() {
    source = new MockTimeSeries(BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    ((MockTimeSeries) source).addValue(new MutableNumericValue(
        new MillisecondTimeStamp(BASE_TIME ), 40));
    ((MockTimeSeries) source).addValue(new MutableNumericValue(
        new MillisecondTimeStamp(BASE_TIME + 2000000L), 50));
    ((MockTimeSeries) source).addValue(new MutableNumericValue(
        new MillisecondTimeStamp(BASE_TIME + 3600000L), 40));
    ((MockTimeSeries) source).addValue(new MutableNumericValue(
        new MillisecondTimeStamp(BASE_TIME + 3605000L), 50));
    MutableNumericValue nully = new MutableNumericValue();
    nully.resetNull(new MillisecondTimeStamp(BASE_TIME + 7200000L));
    ((MockTimeSeries) source).addValue(nully);
    //((MockTimeSeries) source).addValue(new MutableNumericType(
    //    new MillisecondTimeStamp(BASE_TIME + 7200000L), 40));
    nully = new MutableNumericValue();
    nully.resetNull(new MillisecondTimeStamp(BASE_TIME + 9200000L));
    ((MockTimeSeries) source).addValue(nully);
    //((MockTimeSeries) source).addValue(new MutableNumericType(
    //    new MillisecondTimeStamp(BASE_TIME + 9200000L), 50));
    
    config = RateOptions.newBuilder()
        .setId("foo")
        .setInterval("1s")
        .setCounter(true)
        .setCounterMax(70)
        .build();
    
    setupMock();
    RateNumericIterator it = new RateNumericIterator(node, source);

    assertTrue(it.hasNext());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 2000000L, v.timestamp().msEpoch());
    assertEquals(0.005, v.value().doubleValue(), 0.001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 3600000L, v.timestamp().msEpoch());
    assertEquals(0.0375, v.value().doubleValue(), 0.001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 3605000L, v.timestamp().msEpoch());
    assertEquals(2.0, v.value().doubleValue(), 0.001);
    
    assertFalse(it.hasNext());
  }
  
  // TODO - test greater intervals once supported.
  
  @Test
  public void oneValue() throws Exception {
    source = new NumericMillisecondShard(BaseTimeSeriesStringId.newBuilder()
          .setMetric("a")
          .build(), 
        new MillisecondTimeStamp(BASE_TIME), 
        new MillisecondTimeStamp(BASE_TIME + 10000000));
    ((NumericMillisecondShard) source).add(BASE_TIME, 40);
    
    config = RateOptions.newBuilder()
        .setId("foo")
        .setInterval("1s")
        .build();
    
    setupMock();
    RateNumericIterator it = new RateNumericIterator(node, source);
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void noData() throws Exception {
    source = new NumericMillisecondShard(BaseTimeSeriesStringId.newBuilder()
          .setMetric("a")
          .build(), 
        new MillisecondTimeStamp(BASE_TIME), 
        new MillisecondTimeStamp(BASE_TIME + 10000000));
    
    config = RateOptions.newBuilder()
        .setId("foo")
        .setInterval("1s")
        .build();
    
    setupMock();
    RateNumericIterator it = new RateNumericIterator(node, source);
    
    assertFalse(it.hasNext());
  }

  @Test(expected = IllegalStateException.class)
  public void decreasingTimestamps() {
    source = mock(TimeSeries.class);
    final List<TimeSeriesValue<? extends TimeSeriesDataType>> dps = 
        Lists.newArrayListWithCapacity(2);
    dps.add(new MutableNumericValue(new MillisecondTimeStamp(2000L), 42));
    dps.add(new MutableNumericValue(new MillisecondTimeStamp(1000L), 24));
    when(source.iterator(NumericType.TYPE)).thenReturn(Optional.of(dps.iterator()));
    config = RateOptions.newBuilder()
        .setId("foo")
        .setInterval("1s")
        .build();
    
    setupMock();
    new RateNumericIterator(node, source);
  }

  @Test(expected = IllegalStateException.class)
  public void duplicatedTimestamps() {
    source = mock(TimeSeries.class);
    final List<TimeSeriesValue<? extends TimeSeriesDataType>> dps = 
        Lists.newArrayListWithCapacity(2);
    dps.add(new MutableNumericValue(new MillisecondTimeStamp(2000L), 42));
    dps.add(new MutableNumericValue(new MillisecondTimeStamp(2000L), 24));
    when(source.iterator(NumericType.TYPE)).thenReturn(Optional.of(dps.iterator()));
    config = RateOptions.newBuilder()
        .setId("foo")
        .setInterval("1s")
        .build();
    
    setupMock();
    new RateNumericIterator(node, source);
  }

  @Test
  public void bigLongValues() {
    source = new NumericMillisecondShard(BaseTimeSeriesStringId.newBuilder()
          .setMetric("a")
          .build(), 
        new MillisecondTimeStamp(BASE_TIME), 
        new MillisecondTimeStamp(BASE_TIME + 10000000));
    ((NumericMillisecondShard) source).add(BASE_TIME, Long.MAX_VALUE - 100);
    ((NumericMillisecondShard) source).add(BASE_TIME + 100000L, Long.MAX_VALUE - 20);
    
    config = RateOptions.newBuilder()
        .setId("foo")
        .setInterval("1s")
        .build();
    
    setupMock();
    RateNumericIterator it = new RateNumericIterator(node, source);
    
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 100000L, v.timestamp().msEpoch());
    assertEquals(0.8, v.value().doubleValue(), 0.001);
    
    assertFalse(it.hasNext());
  }

  @Test
  public void counter() {
    source = new NumericMillisecondShard(BaseTimeSeriesStringId.newBuilder()
          .setMetric("a")
          .build(), 
        new MillisecondTimeStamp(BASE_TIME), 
        new MillisecondTimeStamp(BASE_TIME + 10000000));
    ((NumericMillisecondShard) source).add(BASE_TIME, 0.000000029476822);
    ((NumericMillisecondShard) source).add(BASE_TIME + 2000000L, 0.005);
    ((NumericMillisecondShard) source).add(BASE_TIME + 3600000L, 0.0375);
    ((NumericMillisecondShard) source).add(BASE_TIME + 3605000L, 2);
    ((NumericMillisecondShard) source).add(BASE_TIME + 7200000L, 0.016689847009736);
    ((NumericMillisecondShard) source).add(BASE_TIME + 9200000L, 0.005);
    
    config = RateOptions.newBuilder()
        .setId("foo")
        .setInterval("1s")
        .setCounter(true)
        .setCounterMax(70)
        .build();
    
    setupMock();
    RateNumericIterator it = new RateNumericIterator(node, source);
    
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 2000000L, v.timestamp().msEpoch());
    assertEquals(2.499985261589E-6, v.value().doubleValue(), 0.001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 3600000L, v.timestamp().msEpoch());
    assertEquals(2.03125E-5, v.value().doubleValue(), 0.001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 3605000L, v.timestamp().msEpoch());
    assertEquals(0.39249999999999996, v.value().doubleValue(), 0.001);

    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 7200000L, v.timestamp().msEpoch());
    assertEquals(0.01891980246092065, v.value().doubleValue(), 0.001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 9200000L, v.timestamp().msEpoch());
    assertEquals(0.034994155076495136, v.value().doubleValue(), 0.001);
    
    assertFalse(it.hasNext());
  }

  @Test
  public void counterLongMax() {
    source = new NumericMillisecondShard(BaseTimeSeriesStringId.newBuilder()
          .setMetric("a")
          .build(), 
        new MillisecondTimeStamp(BASE_TIME), 
        new MillisecondTimeStamp(BASE_TIME + 10000000));
    ((NumericMillisecondShard) source).add(BASE_TIME, Long.MAX_VALUE - 55);
    ((NumericMillisecondShard) source).add(BASE_TIME + 30000, Long.MAX_VALUE - 25);
    ((NumericMillisecondShard) source).add(BASE_TIME + 60000, 5);
    
    config = RateOptions.newBuilder()
        .setId("foo")
        .setInterval("1s")
        .setCounter(true)
        .build();
    
    setupMock();
    RateNumericIterator it = new RateNumericIterator(node, source);
    
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 30000, v.timestamp().msEpoch());
    assertEquals(1.0, v.value().doubleValue(), 0.001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 60000, v.timestamp().msEpoch());
    assertEquals(1.0, v.value().doubleValue(), 0.001);
    
    assertFalse(it.hasNext());
  }

  @Test
  public void counterWithResetValue() {
    source = new NumericMillisecondShard(BaseTimeSeriesStringId.newBuilder()
          .setMetric("a")
          .build(), 
        new MillisecondTimeStamp(BASE_TIME), 
        new MillisecondTimeStamp(BASE_TIME + 10000000));
    ((NumericMillisecondShard) source).add(BASE_TIME, 40);
    ((NumericMillisecondShard) source).add(BASE_TIME + 1000, 50);
    ((NumericMillisecondShard) source).add(BASE_TIME + 2000, 40);
    
    config = RateOptions.newBuilder()
        .setId("foo")
        .setInterval("1s")
        .setCounter(true)
        .setCounterMax(70)
        .setResetValue(1)
        .build();
    
    setupMock();
    RateNumericIterator it = new RateNumericIterator(node, source);
    
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 1000, v.timestamp().msEpoch());
    assertEquals(10.0, v.value().doubleValue(), 0.001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 2000, v.timestamp().msEpoch());
    assertEquals(0.0, v.value().doubleValue(), 0.001);
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void counterDropResets() {
    source = new NumericMillisecondShard(BaseTimeSeriesStringId.newBuilder()
          .setMetric("a")
          .build(), 
        new MillisecondTimeStamp(BASE_TIME), 
        new MillisecondTimeStamp(BASE_TIME + 10000000));
    ((NumericMillisecondShard) source).add(BASE_TIME, 40);
    ((NumericMillisecondShard) source).add(BASE_TIME + 1000, 50);
    ((NumericMillisecondShard) source).add(BASE_TIME + 2000, 40);
    ((NumericMillisecondShard) source).add(BASE_TIME + 3000, 50);
    
    config = RateOptions.newBuilder()
        .setId("foo")
        .setInterval("1s")
        .setCounter(true)
        .setDropResets(true)
        .build();
    
    setupMock();
    RateNumericIterator it = new RateNumericIterator(node, source);
    
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 1000, v.timestamp().msEpoch());
    assertEquals(10.0, v.value().doubleValue(), 0.001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 3000, v.timestamp().msEpoch());
    assertEquals(10.0, v.value().doubleValue(), 0.001);
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void counterDroResetsNothingAfter() {
    source = new NumericMillisecondShard(BaseTimeSeriesStringId.newBuilder()
          .setMetric("a")
          .build(), 
        new MillisecondTimeStamp(BASE_TIME), 
        new MillisecondTimeStamp(BASE_TIME + 10000000));
    ((NumericMillisecondShard) source).add(BASE_TIME, 40);
    ((NumericMillisecondShard) source).add(BASE_TIME + 1000, 50);
    ((NumericMillisecondShard) source).add(BASE_TIME + 2000, 40);
    
    config = RateOptions.newBuilder()
        .setId("foo")
        .setInterval("1s")
        .setCounter(true)
        .setDropResets(true)
        .build();
    
    setupMock();
    RateNumericIterator it = new RateNumericIterator(node, source);

    assertTrue(it.hasNext());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 1000, v.timestamp().msEpoch());
    assertEquals(10.0, v.value().doubleValue(), 0.001);
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void counterDroResetsFirst() {
    source = new NumericMillisecondShard(BaseTimeSeriesStringId.newBuilder()
          .setMetric("a")
          .build(), 
        new MillisecondTimeStamp(BASE_TIME), 
        new MillisecondTimeStamp(BASE_TIME + 10000000));
    ((NumericMillisecondShard) source).add(BASE_TIME, 50);
    ((NumericMillisecondShard) source).add(BASE_TIME + 1000, 40);
    ((NumericMillisecondShard) source).add(BASE_TIME + 2000, 50);
    
    config = RateOptions.newBuilder()
        .setId("foo")
        .setInterval("1s")
        .setCounter(true)
        .setDropResets(true)
        .build();
    
    setupMock();
    RateNumericIterator it = new RateNumericIterator(node, source);

    assertTrue(it.hasNext());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 2000, v.timestamp().msEpoch());
    assertEquals(10.0, v.value().doubleValue(), 0.001);
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void counterDroResetsOnly() {
    source = new NumericMillisecondShard(BaseTimeSeriesStringId.newBuilder()
          .setMetric("a")
          .build(), 
        new MillisecondTimeStamp(BASE_TIME), 
        new MillisecondTimeStamp(BASE_TIME + 10000000));
    ((NumericMillisecondShard) source).add(BASE_TIME, 50);
    ((NumericMillisecondShard) source).add(BASE_TIME + 1000, 40);
    
    config = RateOptions.newBuilder()
        .setId("foo")
        .setInterval("1s")
        .setCounter(true)
        .setDropResets(true)
        .build();
    
    setupMock();
    RateNumericIterator it = new RateNumericIterator(node, source);
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void nanoRollover() {
    source = new MockTimeSeries(BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    
    ((MockTimeSeries) source).addValue(new MutableNumericValue(
        new ZonedNanoTimeStamp(BASE_TIME / 1000, 5000, Const.UTC), 40));
    ((MockTimeSeries) source).addValue(new MutableNumericValue(
        new ZonedNanoTimeStamp((BASE_TIME / 1000) + 1L, 1000, Const.UTC), 50));
    
    config = RateOptions.newBuilder()
        .setId("foo")
        .setInterval("1m")
        .setCounter(true)
        .setCounterMax(70)
        .build();
    
    setupMock();
    RateNumericIterator it = new RateNumericIterator(node, source);
    
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals((BASE_TIME / 1000) + 1L, v.timestamp().epoch());
    assertEquals(1000, v.timestamp().nanos());
    assertEquals(0.600000003, v.value().doubleValue(), 0.001);
    
    assertFalse(it.hasNext());
  }
  
  private void setupMock() {
    node = mock(QueryNode.class);
    when(node.config()).thenReturn(config);
    query_context = mock(QueryContext.class);
    pipeline_context = mock(QueryPipelineContext.class);
    when(pipeline_context.queryContext()).thenReturn(query_context);
    when(query_context.query()).thenReturn(query);
    when(node.pipelineContext()).thenReturn(pipeline_context);
  }
}
