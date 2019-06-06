// This file is part of OpenTSDB.
// Copyright (C) 2014-2018 The OpenTSDB Authors.
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

import net.opentsdb.common.Const;
import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.MockTimeSeries;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.ZonedNanoTimeStamp;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericMillisecondShard;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.TimeSeriesQuery;
import net.opentsdb.query.pojo.RateOptions;

import org.junit.Test;

import com.google.common.collect.Lists;

public class TestRateNumericIterator {
  private TimeSeries source;
  private TimeSeriesQuery query;
  private RateConfig config;
  private QueryNode node;
  private QueryResult result;
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
    
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1s")
        .setCounter(true)
        .setId("foo")
        .build();
    
    setupMock();
    RateNumericIterator it = new RateNumericIterator(node, result,
        Lists.newArrayList(source));
    
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
  public void oneSecondLongsDelta() {
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
    
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1s")
        .setCounter(true)
        .setDeltaOnly(true)
        .setId("foo")
        .build();
    
    setupMock();
    RateNumericIterator it = new RateNumericIterator(node, result,
        Lists.newArrayList(source));
    
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 2000000L, v.timestamp().msEpoch());
    assertEquals(10, v.value().longValue());
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 3600000L, v.timestamp().msEpoch());
    assertEquals(-10, v.value().longValue());
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 3605000L, v.timestamp().msEpoch());
    assertEquals(10, v.value().longValue());

    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 7200000L, v.timestamp().msEpoch());
    assertEquals(-10, v.value().longValue());
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 9200000L, v.timestamp().msEpoch());
    assertEquals(10, v.value().longValue());
    
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
    
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1s")
        .setCounter(true)
        .setId("foo")
        .build();
    
    setupMock();
    RateNumericIterator it = new RateNumericIterator(node, result,
         Lists.newArrayList(source));
    
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
  public void oneSecondDoublesDelta() {
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
    
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1s")
        .setCounter(true)
        .setDeltaOnly(true)
        .setId("foo")
        .build();
    
    setupMock();
    RateNumericIterator it = new RateNumericIterator(node, result,
         Lists.newArrayList(source));
    
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 2000000L, v.timestamp().msEpoch());
    assertEquals(10.0, v.value().doubleValue(), 0.001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 3600000L, v.timestamp().msEpoch());
    assertEquals(-10.0, v.value().doubleValue(), 0.001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 3605000L, v.timestamp().msEpoch());
    assertEquals(10.0, v.value().doubleValue(), 0.001);

    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 7200000L, v.timestamp().msEpoch());
    assertEquals(-10.0, v.value().doubleValue(), 0.001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 9200000L, v.timestamp().msEpoch());
    assertEquals(10.0, v.value().doubleValue(), 0.001);
    
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
    
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1s")
        .setCounter(true)
        .setId("foo")
        .build();
    
    setupMock();
    RateNumericIterator it = new RateNumericIterator(node, result,
         Lists.newArrayList(source));
    
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
  public void oneSecondMixDelta() {
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
    
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1s")
        .setCounter(true)
        .setDeltaOnly(true)
        .setId("foo")
        .build();
    
    setupMock();
    RateNumericIterator it = new RateNumericIterator(node, result,
         Lists.newArrayList(source));
    
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 2000000L, v.timestamp().msEpoch());
    assertEquals(10.5, v.value().doubleValue(), 0.001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 3600000L, v.timestamp().msEpoch());
    assertEquals(-10.5, v.value().doubleValue(), 0.001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 3605000L, v.timestamp().msEpoch());
    assertEquals(10.5, v.value().doubleValue(), 0.001);

    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 7200000L, v.timestamp().msEpoch());
    assertEquals(-10.5, v.value().doubleValue(), 0.001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 9200000L, v.timestamp().msEpoch());
    assertEquals(10.5, v.value().doubleValue(), 0.001);
    
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
    
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1ns")
        .setCounter(true)
        .setId("foo")
        .build();
    
    setupMock();
    RateNumericIterator it = new RateNumericIterator(node, result,
         Lists.newArrayList(source));
    
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
    
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("10s")
        .setCounter(true)
        .setId("foo")
        .build();
    
    setupMock();
    RateNumericIterator it = new RateNumericIterator(node, result,
         Lists.newArrayList(source));
    
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
    
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1m")
        .setCounter(true)
        .setId("foo")
        .build();
    
    setupMock();
    RateNumericIterator it = new RateNumericIterator(node, result,
         Lists.newArrayList(source));
    
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
    
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1s")
        .setCounter(true)
        .setCounterMax(70)
        .setId("foo")
        .build();
    
    setupMock();
    RateNumericIterator it = new RateNumericIterator(node, result,
         Lists.newArrayList(source));
    
    
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME, v.timestamp().msEpoch());
    assertTrue(v.value() == null);

    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 2000000L, v.timestamp().msEpoch());
    assertTrue(v.value() == null);

    assertTrue(it.hasNext());
     v = (TimeSeriesValue<NumericType>) it.next();
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
  public void nullsAtStartDelta() {
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
    
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1s")
        .setCounter(true)
        .setCounterMax(70)
        .setDeltaOnly(true)
        .setId("foo")
        .build();
    
    setupMock();
    RateNumericIterator it = new RateNumericIterator(node, result,
         Lists.newArrayList(source));
    
    
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME, v.timestamp().msEpoch());
    assertTrue(v.value() == null);

    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 2000000L, v.timestamp().msEpoch());
    assertTrue(v.value() == null);

    assertTrue(it.hasNext());
     v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 3605000L, v.timestamp().msEpoch());
    assertEquals(10, v.value().longValue());

    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 7200000L, v.timestamp().msEpoch());
    assertEquals(-10, v.value().longValue());
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 9200000L, v.timestamp().msEpoch());
    assertEquals(10, v.value().longValue());
    
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
    
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1s")
        .setCounter(true)
        .setCounterMax(70)
        .setId("foo")
        .build();
    
    setupMock();
    RateNumericIterator it = new RateNumericIterator(node, result,
         Lists.newArrayList(source));

    assertTrue(it.hasNext());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 2000000L, v.timestamp().msEpoch());
    assertEquals(0.005, v.value().doubleValue(), 0.001);

    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 3600000L, v.timestamp().msEpoch());
    assertTrue(v.value() == null);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 3605000L, v.timestamp().msEpoch());
    assertTrue(v.value() == null);
    
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
  public void nullsInMiddleDelta() {
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
    
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1s")
        .setCounter(true)
        .setCounterMax(70)
        .setDeltaOnly(true)
        .setId("foo")
        .build();
    
    setupMock();
    RateNumericIterator it = new RateNumericIterator(node, result,
         Lists.newArrayList(source));

    assertTrue(it.hasNext());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 2000000L, v.timestamp().msEpoch());
    assertEquals(10, v.value().longValue());

    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 3600000L, v.timestamp().msEpoch());
    assertTrue(v.value() == null);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 3605000L, v.timestamp().msEpoch());
    assertTrue(v.value() == null);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 7200000L, v.timestamp().msEpoch());
    assertEquals(-10, v.value().longValue());
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 9200000L, v.timestamp().msEpoch());
    assertEquals(10, v.value().longValue());
    
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
    
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1s")
        .setCounter(true)
        .setCounterMax(70)
        .setId("foo")
        .build();
    
    setupMock();
    RateNumericIterator it = new RateNumericIterator(node, result,
         Lists.newArrayList(source));

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
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertFalse(it.hasNext());
  }
  
  @Test
  public void nullsAtEndDelta() {
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
    
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1s")
        .setCounter(true)
        .setCounterMax(70)
        .setDeltaOnly(true)
        .setId("foo")
        .build();
    
    setupMock();
    RateNumericIterator it = new RateNumericIterator(node, result,
         Lists.newArrayList(source));

    assertTrue(it.hasNext());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 2000000L, v.timestamp().msEpoch());
    assertEquals(10, v.value().longValue());
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 3600000L, v.timestamp().msEpoch());
    assertEquals(-10, v.value().longValue());
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 3605000L, v.timestamp().msEpoch());
    assertEquals(10, v.value().longValue());
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
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
    
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1s")
        .setId("foo")
        .build();
    
    setupMock();
    RateNumericIterator it = new RateNumericIterator(node, result,
         Lists.newArrayList(source));
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void noData() throws Exception {
    source = new NumericMillisecondShard(BaseTimeSeriesStringId.newBuilder()
          .setMetric("a")
          .build(), 
        new MillisecondTimeStamp(BASE_TIME), 
        new MillisecondTimeStamp(BASE_TIME + 10000000));
    
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1s")
        .setId("foo")
        .build();
    
    setupMock();
    RateNumericIterator it = new RateNumericIterator(node, result,
         Lists.newArrayList(source));
    
    assertFalse(it.hasNext());
  }

  @Test(expected = IllegalStateException.class)
  public void decreasingTimestamps() {
    source = new MockTimeSeries(BaseTimeSeriesStringId.newBuilder()
        .setMetric("foo")
        .build());
    ((MockTimeSeries) source).addValue(new MutableNumericValue(
        new MillisecondTimeStamp(2000L), 42));
    ((MockTimeSeries) source).addValue(new MutableNumericValue(
        new MillisecondTimeStamp(1000L), 24));
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1s")
        .setId("foo")
        .build();
    
    setupMock();
    new RateNumericIterator(node, result,
         Lists.newArrayList(source));
  }

  @Test(expected = IllegalStateException.class)
  public void duplicatedTimestamps() {
    source = new MockTimeSeries(BaseTimeSeriesStringId.newBuilder()
        .setMetric("foo")
        .build());
    ((MockTimeSeries) source).addValue(new MutableNumericValue(
        new MillisecondTimeStamp(2000L), 42));
    ((MockTimeSeries) source).addValue(new MutableNumericValue(
        new MillisecondTimeStamp(2000L), 24));
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1s")
        .setId("foo")
        .build();
    
    setupMock();
    new RateNumericIterator(node, result,
         Lists.newArrayList(source));
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
    
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1s")
        .setId("foo")
        .build();
    
    setupMock();
    RateNumericIterator it = new RateNumericIterator(node, result,
         Lists.newArrayList(source));
    
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
    
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1s")
        .setCounter(true)
        .setCounterMax(70)
        .setId("foo")
        .build();
    
    setupMock();
    RateNumericIterator it = new RateNumericIterator(node, result,
         Lists.newArrayList(source));
    
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
    
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1s")
        .setCounter(true)
        .setId("foo")
        .build();
    
    setupMock();
    RateNumericIterator it = new RateNumericIterator(node, result,
         Lists.newArrayList(source));
    
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
    
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1s")
        .setCounter(true)
        .setCounterMax(70)
        .setResetValue(10)
        .setId("foo")
        .build();
    
    setupMock();
    RateNumericIterator it = new RateNumericIterator(node, result,
         Lists.newArrayList(source));
    
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
  public void counterWithResetValuePositive() {
    source = new NumericMillisecondShard(BaseTimeSeriesStringId.newBuilder()
          .setMetric("a")
          .build(), 
        new MillisecondTimeStamp(BASE_TIME), 
        new MillisecondTimeStamp(BASE_TIME + 10000000));
    ((NumericMillisecondShard) source).add(BASE_TIME, 40);
    ((NumericMillisecondShard) source).add(BASE_TIME + 1000, 50);
    ((NumericMillisecondShard) source).add(BASE_TIME + 2000, 80);
    
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1s")
        .setCounter(true)
        .setCounterMax(70)
        .setResetValue(10)
        .setId("foo")
        .build();
    
    setupMock();
    RateNumericIterator it = new RateNumericIterator(node, result,
         Lists.newArrayList(source));
    
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
    
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1s")
        .setCounter(true)
        .setDropResets(true)
        .setId("foo")
        .build();
    
    setupMock();
    RateNumericIterator it = new RateNumericIterator(node, result,
         Lists.newArrayList(source));
    
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
  public void counterDropResetsDelta() {
    source = new NumericMillisecondShard(BaseTimeSeriesStringId.newBuilder()
          .setMetric("a")
          .build(), 
        new MillisecondTimeStamp(BASE_TIME), 
        new MillisecondTimeStamp(BASE_TIME + 10000000));
    ((NumericMillisecondShard) source).add(BASE_TIME, 40);
    ((NumericMillisecondShard) source).add(BASE_TIME + 1000, 50);
    ((NumericMillisecondShard) source).add(BASE_TIME + 2000, 40);
    ((NumericMillisecondShard) source).add(BASE_TIME + 3000, 50);
    
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1s")
        .setCounter(true)
        .setDropResets(true)
        .setDeltaOnly(true)
        .setId("foo")
        .build();
    
    setupMock();
    RateNumericIterator it = new RateNumericIterator(node, result,
         Lists.newArrayList(source));
    
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 1000, v.timestamp().msEpoch());
    assertEquals(10, v.value().longValue());
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 3000, v.timestamp().msEpoch());
    assertEquals(10, v.value().longValue());
    
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
    
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1s")
        .setCounter(true)
        .setDropResets(true)
        .setId("foo")
        .build();
    
    setupMock();
    RateNumericIterator it = new RateNumericIterator(node, result,
         Lists.newArrayList(source));

    assertTrue(it.hasNext());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 1000, v.timestamp().msEpoch());
    assertEquals(10.0, v.value().doubleValue(), 0.001);
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void counterDroResetsNothingAfterDelta() {
    source = new NumericMillisecondShard(BaseTimeSeriesStringId.newBuilder()
          .setMetric("a")
          .build(), 
        new MillisecondTimeStamp(BASE_TIME), 
        new MillisecondTimeStamp(BASE_TIME + 10000000));
    ((NumericMillisecondShard) source).add(BASE_TIME, 40);
    ((NumericMillisecondShard) source).add(BASE_TIME + 1000, 50);
    ((NumericMillisecondShard) source).add(BASE_TIME + 2000, 40);
    
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1s")
        .setCounter(true)
        .setDropResets(true)
        .setDeltaOnly(true)
        .setId("foo")
        .build();
    
    setupMock();
    RateNumericIterator it = new RateNumericIterator(node, result,
         Lists.newArrayList(source));

    assertTrue(it.hasNext());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 1000, v.timestamp().msEpoch());
    assertEquals(10, v.value().longValue());
    
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
    
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1s")
        .setCounter(true)
        .setDropResets(true)
        .setId("foo")
        .build();
    
    setupMock();
    RateNumericIterator it = new RateNumericIterator(node, result,
         Lists.newArrayList(source));

    assertTrue(it.hasNext());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 2000, v.timestamp().msEpoch());
    assertEquals(10.0, v.value().doubleValue(), 0.001);
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void counterDroResetsFirstDelta() {
    source = new NumericMillisecondShard(BaseTimeSeriesStringId.newBuilder()
          .setMetric("a")
          .build(), 
        new MillisecondTimeStamp(BASE_TIME), 
        new MillisecondTimeStamp(BASE_TIME + 10000000));
    ((NumericMillisecondShard) source).add(BASE_TIME, 50);
    ((NumericMillisecondShard) source).add(BASE_TIME + 1000, 40);
    ((NumericMillisecondShard) source).add(BASE_TIME + 2000, 50);
    
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1s")
        .setCounter(true)
        .setDropResets(true)
        .setDeltaOnly(true)
        .setId("foo")
        .build();
    
    setupMock();
    RateNumericIterator it = new RateNumericIterator(node, result,
         Lists.newArrayList(source));

    assertTrue(it.hasNext());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(BASE_TIME + 2000, v.timestamp().msEpoch());
    assertEquals(10, v.value().longValue());
    
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
    
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1s")
        .setCounter(true)
        .setDropResets(true)
        .setId("foo")
        .build();
    
    setupMock();
    RateNumericIterator it = new RateNumericIterator(node, result,
         Lists.newArrayList(source));
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void counterDroResetsOnlyDelta() {
    source = new NumericMillisecondShard(BaseTimeSeriesStringId.newBuilder()
          .setMetric("a")
          .build(), 
        new MillisecondTimeStamp(BASE_TIME), 
        new MillisecondTimeStamp(BASE_TIME + 10000000));
    ((NumericMillisecondShard) source).add(BASE_TIME, 50);
    ((NumericMillisecondShard) source).add(BASE_TIME + 1000, 40);
    
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1s")
        .setCounter(true)
        .setDropResets(true)
        .setDeltaOnly(true)
        .setId("foo")
        .build();
    
    setupMock();
    RateNumericIterator it = new RateNumericIterator(node, result,
         Lists.newArrayList(source));
    
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
    
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1m")
        .setCounter(true)
        .setCounterMax(70)
        .setId("foo")
        .build();
    
    setupMock();
    RateNumericIterator it = new RateNumericIterator(node, result,
         Lists.newArrayList(source));
    
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals((BASE_TIME / 1000) + 1L, v.timestamp().epoch());
    assertEquals(1000, v.timestamp().nanos());
    assertEquals(0.600000003, v.value().doubleValue(), 0.001);
    
    assertFalse(it.hasNext());
  }
  
  private void setupMock() {
    node = mock(QueryNode.class);
    result = mock(QueryResult.class);
    when(node.config()).thenReturn(config);
    query_context = mock(QueryContext.class);
    pipeline_context = mock(QueryPipelineContext.class);
    when(pipeline_context.queryContext()).thenReturn(query_context);
    when(query_context.query()).thenReturn(query);
    when(node.pipelineContext()).thenReturn(pipeline_context);
  }
}
