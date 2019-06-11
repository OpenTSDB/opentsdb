// This file is part of OpenTSDB.
// Copyright (C) 2018 The OpenTSDB Authors.
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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;

import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericArrayTimeSeries;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericMillisecondShard;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.TimeSeriesQuery;
import net.opentsdb.query.pojo.RateOptions;

import org.junit.Test;

import com.google.common.collect.Lists;

public class TestRateNumericArrayIterator {
  private TimeSeries source;
  private TimeSeriesQuery query;
  private RateConfig config;
  private QueryNode node;
  private QueryResult result;
  private TimeSpecification time_spec;
  private QueryContext query_context;
  private QueryPipelineContext pipeline_context;
  
  private static final long BASE_TIME = 1356998400000L;
  
  @Test
  public void longs() {
    setSource(new MutableNumericValue(new SecondTimeStamp(0L), 40),
        new MutableNumericValue(new SecondTimeStamp(60L), 50),
        new MutableNumericValue(new SecondTimeStamp(60L * 2), 40),
        new MutableNumericValue(new SecondTimeStamp(60L * 3), 50),
        new MutableNumericValue(new SecondTimeStamp(60L * 4), 40),
        new MutableNumericValue(new SecondTimeStamp(60L * 5), 50));
    
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1s")
        .setId("foo")
        .build();
    
    setupMock();
    RateNumericArrayIterator it = new RateNumericArrayIterator(node, result,
        Lists.newArrayList(source));
    
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) it.next();
    
    assertArrayEquals(new double[] { Double.NaN, 0.166, -0.166, 
        0.166, -0.166, 0.166 }, 
        value.value().doubleArray(), 0.001);
    
    assertFalse(it.hasNext());
    
    setSource(new MutableNumericValue(new SecondTimeStamp(0L), 0),
        new MutableNumericValue(new SecondTimeStamp(60L), 60),
        new MutableNumericValue(new SecondTimeStamp(60L * 2), 0),
        new MutableNumericValue(new SecondTimeStamp(60L * 3), 60),
        new MutableNumericValue(new SecondTimeStamp(60L * 4), 0),
        new MutableNumericValue(new SecondTimeStamp(60L * 5), 60));
    
    it = new RateNumericArrayIterator(node, result,
        Lists.newArrayList(source));
    
    assertTrue(it.hasNext());
    value = (TimeSeriesValue<NumericArrayType>) it.next();
    
    assertArrayEquals(new double[] { Double.NaN, 1, -1, 1, -1, 1 }, 
        value.value().doubleArray(), 0.001);
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void longsDelta() {
    setSource(new MutableNumericValue(new SecondTimeStamp(0L), 40),
        new MutableNumericValue(new SecondTimeStamp(60L), 50),
        new MutableNumericValue(new SecondTimeStamp(60L * 2), 40),
        new MutableNumericValue(new SecondTimeStamp(60L * 3), 50),
        new MutableNumericValue(new SecondTimeStamp(60L * 4), 40),
        new MutableNumericValue(new SecondTimeStamp(60L * 5), 50));
    
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1s")
        .setDeltaOnly(true)
        .setId("foo")
        .build();
    
    setupMock();
    RateNumericArrayIterator it = new RateNumericArrayIterator(node, result,
        Lists.newArrayList(source));
    
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) it.next();
    
    assertArrayEquals(new long[] { 0, 10, -10, 10, -10, 10 }, 
        value.value().longArray());
    
    assertFalse(it.hasNext());
    
    setSource(new MutableNumericValue(new SecondTimeStamp(0L), 0),
        new MutableNumericValue(new SecondTimeStamp(60L), 60),
        new MutableNumericValue(new SecondTimeStamp(60L * 2), 0),
        new MutableNumericValue(new SecondTimeStamp(60L * 3), 60),
        new MutableNumericValue(new SecondTimeStamp(60L * 4), 0),
        new MutableNumericValue(new SecondTimeStamp(60L * 5), 60));
    
    it = new RateNumericArrayIterator(node, result,
        Lists.newArrayList(source));
    
    assertTrue(it.hasNext());
    value = (TimeSeriesValue<NumericArrayType>) it.next();
    
    assertArrayEquals(new long[] { 0, 60, -60, 60, -60, 60 }, 
        value.value().longArray());
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void doubles() {
    setSource(new MutableNumericValue(new SecondTimeStamp(0L), 40.5),
        new MutableNumericValue(new SecondTimeStamp(60L), 50.5),
        new MutableNumericValue(new SecondTimeStamp(60L * 2), 40.5),
        new MutableNumericValue(new SecondTimeStamp(60L * 3), 50.5),
        new MutableNumericValue(new SecondTimeStamp(60L * 4), 40.5),
        new MutableNumericValue(new SecondTimeStamp(60L * 5), 50.5));
    
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1s")
        .setId("foo")
        .build();
    
    setupMock();
    RateNumericArrayIterator it = new RateNumericArrayIterator(node, result,
        Lists.newArrayList(source));
    
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) it.next();
    
    assertArrayEquals(new double[] { Double.NaN, 0.166, -0.166, 
        0.166, -0.166, 0.166 }, 
        value.value().doubleArray(), 0.001);
    
    assertFalse(it.hasNext());
    
    setSource(new MutableNumericValue(new SecondTimeStamp(0L), 0),
        new MutableNumericValue(new SecondTimeStamp(60L), 60),
        new MutableNumericValue(new SecondTimeStamp(60L * 2), 0),
        new MutableNumericValue(new SecondTimeStamp(60L * 3), 60),
        new MutableNumericValue(new SecondTimeStamp(60L * 4), 0),
        new MutableNumericValue(new SecondTimeStamp(60L * 5), 60));
    
    it = new RateNumericArrayIterator(node, result,
        Lists.newArrayList(source));
    
    assertTrue(it.hasNext());
    value = (TimeSeriesValue<NumericArrayType>) it.next();
    
    assertArrayEquals(new double[] { Double.NaN, 1, -1, 1, -1, 1 }, 
        value.value().doubleArray(), 0.001);
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void doublesDelta() {
    setSource(new MutableNumericValue(new SecondTimeStamp(0L), 40.5),
        new MutableNumericValue(new SecondTimeStamp(60L), 50.5),
        new MutableNumericValue(new SecondTimeStamp(60L * 2), 40.5),
        new MutableNumericValue(new SecondTimeStamp(60L * 3), 50.5),
        new MutableNumericValue(new SecondTimeStamp(60L * 4), 40.5),
        new MutableNumericValue(new SecondTimeStamp(60L * 5), 50.5));
    
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1s")
        .setDeltaOnly(true)
        .setId("foo")
        .build();
    
    setupMock();
    RateNumericArrayIterator it = new RateNumericArrayIterator(node, result,
        Lists.newArrayList(source));
    
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) it.next();
    
    assertArrayEquals(new double[] { 0, 10, -10, 10, -10, 10 }, 
        value.value().doubleArray(), 0.001);
    
    assertFalse(it.hasNext());
    
    setSource(new MutableNumericValue(new SecondTimeStamp(0L), 0),
        new MutableNumericValue(new SecondTimeStamp(60L), 60.5),
        new MutableNumericValue(new SecondTimeStamp(60L * 2), 0),
        new MutableNumericValue(new SecondTimeStamp(60L * 3), 60.5),
        new MutableNumericValue(new SecondTimeStamp(60L * 4), 0),
        new MutableNumericValue(new SecondTimeStamp(60L * 5), 60.5));
    
    it = new RateNumericArrayIterator(node, result,
        Lists.newArrayList(source));
    
    assertTrue(it.hasNext());
    value = (TimeSeriesValue<NumericArrayType>) it.next();
    
    assertArrayEquals(new double[] { 0, 60.5, -60.5, 60.5, -60.5, 60.5 }, 
        value.value().doubleArray(), 0.001);
    
    assertFalse(it.hasNext());
  }
  
  // TODO - test greater intervals once supported.
  
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
    RateNumericArrayIterator it = new RateNumericArrayIterator(node, result,
         Lists.newArrayList(source));
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void noDataDelta() throws Exception {
    source = new NumericMillisecondShard(BaseTimeSeriesStringId.newBuilder()
          .setMetric("a")
          .build(), 
        new MillisecondTimeStamp(BASE_TIME), 
        new MillisecondTimeStamp(BASE_TIME + 10000000));
    
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1s")
        .setDeltaOnly(true)
        .setId("foo")
        .build();
    
    setupMock();
    RateNumericArrayIterator it = new RateNumericArrayIterator(node, result,
         Lists.newArrayList(source));
    
    assertFalse(it.hasNext());
  }

  @Test
  public void counter() {
    setSource(new MutableNumericValue(new SecondTimeStamp(0L), 0),
        new MutableNumericValue(new SecondTimeStamp(60L), 60),
        new MutableNumericValue(new SecondTimeStamp(60L * 2), 120),
        new MutableNumericValue(new SecondTimeStamp(60L * 3), 5),
        new MutableNumericValue(new SecondTimeStamp(60L * 4), 65),
        new MutableNumericValue(new SecondTimeStamp(60L * 5), 60));
    
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1s")
        .setCounter(true)
        .setCounterMax(2)
        .setId("foo")
        .build();
    
    setupMock();
    RateNumericArrayIterator it = new RateNumericArrayIterator(node, result,
         Lists.newArrayList(source));
    
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) it.next();
    
    assertArrayEquals(new double[] { Double.NaN, 1, 1, -1.883, 1, -0.05 }, 
        value.value().doubleArray(), 0.001);
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void counterWithResetValue() {
    setSource(new MutableNumericValue(new SecondTimeStamp(0L), 0),
        new MutableNumericValue(new SecondTimeStamp(60L), 60),
        new MutableNumericValue(new SecondTimeStamp(60L * 2), 170),
        new MutableNumericValue(new SecondTimeStamp(60L * 3), 5),
        new MutableNumericValue(new SecondTimeStamp(60L * 4), 65),
        new MutableNumericValue(new SecondTimeStamp(60L * 5), 60));
    
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1s")
        .setCounter(true)
        .setCounterMax(2)
        .setResetValue(2)
        .setId("foo")
        .build();
    
    setupMock();
    RateNumericArrayIterator it = new RateNumericArrayIterator(node, result,
         Lists.newArrayList(source));
    
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) it.next();
    
    assertArrayEquals(new double[] { Double.NaN, 1, 1.833, -2.716, 1, -0.05 }, 
        value.value().doubleArray(), 0.001);
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void counterDropResets() {
    setSource(new MutableNumericValue(new SecondTimeStamp(0L), 0),
        new MutableNumericValue(new SecondTimeStamp(60L), 60),
        new MutableNumericValue(new SecondTimeStamp(60L * 2), 170),
        new MutableNumericValue(new SecondTimeStamp(60L * 3), 5),
        new MutableNumericValue(new SecondTimeStamp(60L * 4), 65),
        new MutableNumericValue(new SecondTimeStamp(60L * 5), 60));
    
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1s")
        .setCounter(true)
        .setDropResets(true)
        .setId("foo")
        .build();
    
    setupMock();
    RateNumericArrayIterator it = new RateNumericArrayIterator(node, result,
         Lists.newArrayList(source));
    
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) it.next();
    
    assertArrayEquals(new double[] { Double.NaN, 1, 1.833, 0, 1, 0 }, 
        value.value().doubleArray(), 0.001);
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void counterDropResetsDelta() {
    setSource(new MutableNumericValue(new SecondTimeStamp(0L), 0),
        new MutableNumericValue(new SecondTimeStamp(60L), 60),
        new MutableNumericValue(new SecondTimeStamp(60L * 2), 170),
        new MutableNumericValue(new SecondTimeStamp(60L * 3), 5),
        new MutableNumericValue(new SecondTimeStamp(60L * 4), 65),
        new MutableNumericValue(new SecondTimeStamp(60L * 5), 60));
    
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1s")
        .setCounter(true)
        .setDropResets(true)
        .setDeltaOnly(true)
        .setId("foo")
        .build();
    
    setupMock();
    RateNumericArrayIterator it = new RateNumericArrayIterator(node, result,
         Lists.newArrayList(source));
    
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) it.next();
    
    assertArrayEquals(new long[] { 0, 60, 110, 0, 60, 0 }, 
        value.value().longArray());
    
    assertFalse(it.hasNext());
  }
  
  private void setupMock() {
    node = mock(QueryNode.class);
    result = mock(QueryResult.class);
    time_spec = mock(TimeSpecification.class);
    query_context = mock(QueryContext.class);
    pipeline_context = mock(QueryPipelineContext.class);
    
    when(node.config()).thenReturn(config);
    when(pipeline_context.queryContext()).thenReturn(query_context);
    when(query_context.query()).thenReturn(query);
    when(node.pipelineContext()).thenReturn(pipeline_context);
    when(result.timeSpecification()).thenReturn(time_spec);
    when(time_spec.interval()).thenReturn(Duration.ofSeconds(60));
  }

  void setSource(final MutableNumericValue ...values) {
    source = new NumericArrayTimeSeries(BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build(), 
        new SecondTimeStamp(0));
    for (final MutableNumericValue value : values) {
      if (value.isInteger()) {
        ((NumericArrayTimeSeries) source).add(value.longValue());
      } else {
        ((NumericArrayTimeSeries) source).add(value.doubleValue());
      }
    }
  }
}
