// This file is part of OpenTSDB.
// Copyright (C) 2019  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.movingaverage;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;

import net.opentsdb.core.MockTSDB;
import net.opentsdb.core.MockTSDBDefault;
import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.MockTimeSeries;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericArrayTimeSeries;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.SemanticQuery;

public class TestMovingAverageNumericArrayIterator {

  public static MockTSDB TSDB;
  
  private SemanticQuery query;
  private QueryPipelineContext context;
  private QueryResult result;
  private TimeSpecification time_spec;
  private MovingAverage node;
  private MovingAverageConfig config;
  private TimeSeriesStringId id;
  private TimeSeries source;
  
  @BeforeClass
  public static void beforeClass() {
    TSDB = MockTSDBDefault.getMockTSDB();
  }
  
  @Before
  public void before() throws Exception {
    result = mock(QueryResult.class);
    time_spec = mock(TimeSpecification.class);
    context = mock(QueryPipelineContext.class);
    query = mock(SemanticQuery.class);
    node = mock(MovingAverage.class);
    id = BaseTimeSeriesStringId.newBuilder()
        .setMetric("sys.cpu.user")
        .build();
    
    config = (MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setSamples(5)
        .setId("win")
        .build();
    
    when(node.pipelineContext()).thenReturn(context);
    when(context.query()).thenReturn(query);
    when(context.tsdb()).thenReturn(TSDB);
    
    when(query.startTime()).thenReturn(new SecondTimeStamp(0));
    when(node.config()).thenReturn(config);
    when(result.timeSpecification()).thenReturn(time_spec);
    
    when(time_spec.interval()).thenReturn(Duration.ofSeconds(60));
  }
  
  @Test
  public void consistentLongsSamplesAvg() throws Exception {
    setSource(buildLongSeries());
    
    MovingAverageNumericArrayIterator iterator = new MovingAverageNumericArrayIterator(
        node, result, Lists.newArrayList(source));
    assertTrue(iterator.hasNext());
    
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new double[] { 8.0, 7.0, 5.666, 4.75, 5.6, 5.0, 4.4, 5.8, 6.6 },
        value.value().doubleArray(), 0.001);
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void consistentLongsSamplesWMA() throws Exception {
    setSource(buildLongSeries());
    
    config = (MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setSamples(5)
        .setWeighted(true)
        .setId("win")
        .build();
    when(node.config()).thenReturn(config);
    
    MovingAverageNumericArrayIterator iterator = new MovingAverageNumericArrayIterator(
        node, result, Lists.newArrayList(source));
    assertTrue(iterator.hasNext());
    
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new double[] { 8.0, 6.666, 4.833, 3.7, 5.466, 5.2666, 4.6, 6.466, 6.533 },
        value.value().doubleArray(), 0.001);
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void consistentLongsSamplesEWMA() throws Exception {
    setSource(buildLongSeries());
    
    config = (MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setSamples(5)
        .setExponential(true)
        .setId("win")
        .build();
    when(node.config()).thenReturn(config);
    
    MovingAverageNumericArrayIterator iterator = new MovingAverageNumericArrayIterator(
        node, result, Lists.newArrayList(source));
    assertTrue(iterator.hasNext());
    
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new double[] { 8.0, 6.5, 4.680, 3.629, 5.499, 5.170, 4.543, 6.645, 6.431 },
        value.value().doubleArray(), 0.001);
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void consistentLongsTimesAvg() throws Exception {
    setSource(buildLongSeries());
    
    config = (MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setInterval("5m")
        .setId("win")
        .build();
    when(node.config()).thenReturn(config);
    
    MovingAverageNumericArrayIterator iterator = new MovingAverageNumericArrayIterator(
        node, result, Lists.newArrayList(source));
    assertTrue(iterator.hasNext());
    
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new double[] { 8.0, 7.0, 5.666, 4.75, 5.6, 5.0, 4.4, 5.8, 6.6 },
        value.value().doubleArray(), 0.001);
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void consistentLongsTimeWMA() throws Exception {
    setSource(buildLongSeries());
    
    config = (MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setInterval("5m")
        .setWeighted(true)
        .setId("win")
        .build();
    when(node.config()).thenReturn(config);
    
    MovingAverageNumericArrayIterator iterator = new MovingAverageNumericArrayIterator(
        node, result, Lists.newArrayList(source));
    assertTrue(iterator.hasNext());
    
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new double[] { 8.0, 6.666, 4.833, 3.7, 5.466, 5.2666, 4.6, 6.466, 6.533 },
        value.value().doubleArray(), 0.001);
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void consistentLongsTimeEWMA() throws Exception {
    setSource(buildLongSeries());
    
    config = (MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setInterval("5m")
        .setExponential(true)
        .setId("win")
        .build();
    when(node.config()).thenReturn(config);
    
    MovingAverageNumericArrayIterator iterator = new MovingAverageNumericArrayIterator(
        node, result, Lists.newArrayList(source));
    assertTrue(iterator.hasNext());
    
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new double[] { 8.0, 6.5, 4.680, 3.629, 5.499, 5.170, 4.543, 6.645, 6.431 },
        value.value().doubleArray(), 0.001);
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void consistentDoublesSamplesAvg() throws Exception {
    setSource(buildDoubleSeries());
    
    MovingAverageNumericArrayIterator iterator = new MovingAverageNumericArrayIterator(
        node, result, Lists.newArrayList(source));
    
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertEquals(0, value.timestamp().epoch());
    assertArrayEquals(new double[] { 1.0, 4.6, 5.3, 4.1, 3.54, 5.24, 4.96, 5.26, 5.84 },
        value.value().doubleArray(), 0.001);
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void consistentDoublesSamplesWMA() throws Exception {
    setSource(buildDoubleSeries());
    
    config = (MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setSamples(5)
        .setWeighted(true)
        .setId("win")
        .build();
    when(node.config()).thenReturn(config);
    
    MovingAverageNumericArrayIterator iterator = new MovingAverageNumericArrayIterator(
        node, result, Lists.newArrayList(source));
    
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertEquals(0, value.timestamp().epoch());
    assertArrayEquals(new double[] { 1.0, 5.799, 6.25, 3.95, 3.066, 5.0533, 5.573, 6.653, 6.033 },
        value.value().doubleArray(), 0.001);
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void consistentDoublesSamplesEWMA() throws Exception {
    setSource(buildDoubleSeries());
    
    config = (MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setSamples(5)
        .setExponential(true)
        .setId("win")
        .build();
    when(node.config()).thenReturn(config);
    
    MovingAverageNumericArrayIterator iterator = new MovingAverageNumericArrayIterator(
        node, result, Lists.newArrayList(source));
    
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertEquals(0, value.timestamp().epoch());
    assertArrayEquals(new double[] { 1.0, 6.999, 6.725, 3.874, 3.046, 5.226, 5.599, 6.819, 6.054 },
        value.value().doubleArray(), 0.001);
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void consistentDoublesTimeAvg() throws Exception {
    setSource(buildDoubleSeries());
    
    config = (MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setInterval("5m")
        .setId("win")
        .build();
    when(node.config()).thenReturn(config);
    
    MovingAverageNumericArrayIterator iterator = new MovingAverageNumericArrayIterator(
        node, result, Lists.newArrayList(source));
    
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertEquals(0, value.timestamp().epoch());
    assertArrayEquals(new double[] { 1.0, 4.6, 5.3, 4.1, 3.54, 5.24, 4.96, 5.26, 5.84 },
        value.value().doubleArray(), 0.001);
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void consistentDoublesTimeWMA() throws Exception {
    setSource(buildDoubleSeries());
    
    config = (MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setInterval("5m")
        .setWeighted(true)
        .setId("win")
        .build();
    when(node.config()).thenReturn(config);
    
    MovingAverageNumericArrayIterator iterator = new MovingAverageNumericArrayIterator(
        node, result, Lists.newArrayList(source));
    
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertEquals(0, value.timestamp().epoch());
    assertArrayEquals(new double[] { 1.0, 5.799, 6.25, 3.95, 3.066, 5.0533, 5.573, 6.653, 6.033 },
        value.value().doubleArray(), 0.001);
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void consistentDoublesTimeEWMA() throws Exception {
    setSource(buildDoubleSeries());
    
    config = (MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setInterval("5m")
        .setExponential(true)
        .setId("win")
        .build();
    when(node.config()).thenReturn(config);
    
    MovingAverageNumericArrayIterator iterator = new MovingAverageNumericArrayIterator(
        node, result, Lists.newArrayList(source));
    
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertEquals(0, value.timestamp().epoch());
    assertArrayEquals(new double[] { 1.0, 6.999, 6.725, 3.874, 3.046, 5.226, 5.599, 6.819, 6.054 },
        value.value().doubleArray(), 0.001);
    assertFalse(iterator.hasNext());
  }
    
  @Test
  public void emptyIterator() throws Exception {
    MockTimeSeries ts = new MockTimeSeries(id);
    MovingAverageNumericArrayIterator iterator = new MovingAverageNumericArrayIterator(
        node, result, Lists.newArrayList(ts));
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nansSamplesAvg() throws Exception {
    setSource(buildSomeNans());
    
    MovingAverageNumericArrayIterator iterator = new MovingAverageNumericArrayIterator(
        node, result, Lists.newArrayList(source));
    
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertEquals(0, value.timestamp().epoch());
    assertArrayEquals(new double[] { 1.0, 4.6, 5.3, 5.3, 4.3, 6.425, 5.833, 6.333, 6.333 },
        value.value().doubleArray(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nansSamplesWMA() throws Exception {
    setSource(buildSomeNans());
    
    config = (MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setSamples(5)
        .setWeighted(true)
        .setId("win")
        .build();
    when(node.config()).thenReturn(config);
    
    MovingAverageNumericArrayIterator iterator = new MovingAverageNumericArrayIterator(
        node, result, Lists.newArrayList(source));
    
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertEquals(0, value.timestamp().epoch());
    assertArrayEquals(new double[] { 1.0, 5.799, 6.25, 6.249, 3.999, 6.191, 6.075, 7.209, 7.585 },
        value.value().doubleArray(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nansSamplesEWMA() throws Exception {
    setSource(buildSomeNans());
    
    config = (MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setSamples(5)
        .setExponential(true)
        .setId("win")
        .build();
    when(node.config()).thenReturn(config);
    
    MovingAverageNumericArrayIterator iterator = new MovingAverageNumericArrayIterator(
        node, result, Lists.newArrayList(source));
    
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertEquals(0, value.timestamp().epoch());
    assertArrayEquals(new double[] { 1.0, 6.999, 6.725, 6.725, 4.237, 6.464, 6.533, 7.115, 8.058 },
        value.value().doubleArray(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  // same for time now.
  
  @Test
  public void allNanSampleAvg() throws Exception {
    setSource(buildAllNan());
    MovingAverageNumericArrayIterator iterator = new MovingAverageNumericArrayIterator(
        node, result, Lists.newArrayList(source));
    
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertEquals(0, value.timestamp().epoch());
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, Double.NaN, 
        Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN },
        value.value().doubleArray(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void allNanSampleMWA() throws Exception {
    setSource(buildAllNan());
    
    config = (MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setSamples(5)
        .setWeighted(true)
        .setId("win")
        .build();
    when(node.config()).thenReturn(config);
    
    MovingAverageNumericArrayIterator iterator = new MovingAverageNumericArrayIterator(
        node, result, Lists.newArrayList(source));
    
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertEquals(0, value.timestamp().epoch());
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, Double.NaN, 
        Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN },
        value.value().doubleArray(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void allNanSampleEWMA() throws Exception {
    setSource(buildAllNan());
    
    config = (MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setSamples(5)
        .setExponential(true)
        .setId("win")
        .build();
    when(node.config()).thenReturn(config);
    
    MovingAverageNumericArrayIterator iterator = new MovingAverageNumericArrayIterator(
        node, result, Lists.newArrayList(source));
    
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertEquals(0, value.timestamp().epoch());
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, Double.NaN, 
        Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN },
        value.value().doubleArray(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  void setSource(final MockTimeSeries series) {
    source = new NumericArrayTimeSeries(id, new SecondTimeStamp(0));
    for (final TimeSeriesValue<?> value : series.data().get(NumericType.TYPE)) {
      if (((TimeSeriesValue<NumericType>) value).value().isInteger()) {
        ((NumericArrayTimeSeries) source).add(
            ((TimeSeriesValue<NumericType>) value).value().longValue());
      } else {
        ((NumericArrayTimeSeries) source).add(
            ((TimeSeriesValue<NumericType>) value).value().doubleValue());
      }
    }
  }
  
  private MockTimeSeries buildLongSeries() {
    MockTimeSeries ts = new MockTimeSeries(id);
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(0L), 8L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L), 6L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 2), 3L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 3), 2L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 4), 9L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 5), 5L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 6), 3L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 7), 10L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 8), 6L));
    return ts;
  }
  
  private MockTimeSeries buildDoubleSeries() {
    MockTimeSeries ts = new MockTimeSeries(id);
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(0L), 1.0));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L), 8.2));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 2), 6.7));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 3), 0.5));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 4), 1.3));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 5), 9.5));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 6), 6.8));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 7), 8.2));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 8), 3.4));
    return ts;
  }

  private MockTimeSeries buildSomeNans() {
    MockTimeSeries ts = new MockTimeSeries(id);
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(0L), 1.0));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L), 8.2));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 2), 6.7));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 3), Double.NaN));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 4), 1.3));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 5), 9.5));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 6), Double.NaN));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 7), 8.2));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 8), Double.NaN));
    return ts;
  }
  
  private MockTimeSeries buildAllNan() {
    MockTimeSeries ts = new MockTimeSeries(id);
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(0L), Double.NaN));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L), Double.NaN));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 2), Double.NaN));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 3), Double.NaN));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 4), Double.NaN));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 5), Double.NaN));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 6), Double.NaN));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 7), Double.NaN));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 8), Double.NaN));
    return ts;
  }
}
