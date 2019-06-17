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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;

import net.opentsdb.core.MockTSDB;
import net.opentsdb.core.MockTSDBDefault;
import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.MockTimeSeries;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.SemanticQuery;

public class TestMovingAverageNumericIterator {

  public static MockTSDB TSDB;
  
  private SemanticQuery query;
  private QueryPipelineContext context;
  private QueryResult result;
  private MovingAverage node;
  private MovingAverageConfig config;
  private TimeSeriesStringId id;
  
  @BeforeClass
  public static void beforeClass() {
    TSDB = MockTSDBDefault.getMockTSDB();
  }
  
  @Before
  public void before() throws Exception {
    result = mock(QueryResult.class);
    context = mock(QueryPipelineContext.class);
    query = mock(SemanticQuery.class);
    id = BaseTimeSeriesStringId.newBuilder()
        .setMetric("sys.cpu.user")
        .build();
    
    buildNode((MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setSamples(5)
        .setId("win")
        .build());
    
    when(context.query()).thenReturn(query);
    when(context.tsdb()).thenReturn(TSDB);
    
    when(query.startTime()).thenReturn(new SecondTimeStamp(60L * 5));
  }
  
  @Test
  public void consistentLongsSamplesAvg() throws Exception {
    MockTimeSeries ts = buildLongSeries();
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));

    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(5, value.value().longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(4.4, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(5.8, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(6.6, value.value().doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void consistentLongsTimeAvg() throws Exception {
    MockTimeSeries ts = buildLongSeries();
    
    buildNode((MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setInterval("5m")
        .setId("win")
        .build());

    
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));
    
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(5, value.value().longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(4.4, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(5.8, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(6.6, value.value().doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void consistentLongsSamplesWMA() throws Exception {
    MockTimeSeries ts = buildLongSeries();
    
    buildNode((MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setSamples(5)
        .setWeighted(true)
        .setId("win")
        .build());

    
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));

    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(5.266, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(4.6, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(6.466, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(6.534, value.value().doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void consistentLongsSamplesMM() throws Exception {
    MockTimeSeries ts = buildLongSeries();
    
    buildNode((MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setSamples(5)
        .setMedian(true)
        .setId("win")
        .build());

    
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));

    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(5, value.value().longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(5, value.value().longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(6, value.value().longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(6, value.value().longValue());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void consistentLongsSamplesEWMA() throws Exception {
    MockTimeSeries ts = buildLongSeries();
    
    buildNode((MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setSamples(5)
        .setExponential(true)
        .setId("win")
        .build());

    
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));

    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(5.170, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(4.543, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(6.645, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(6.431, value.value().doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void consistentLongsSamplesEWMADiffAlpha() throws Exception {
    MockTimeSeries ts = buildLongSeries();
    
    buildNode((MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setSamples(5)
        .setExponential(true)
        .setAlpha(0.1)
        .setId("win")
        .build());

    
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));
    
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(4.971, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(4.511, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(6.136, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(6.437, value.value().doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void consistentLongsSamplesEWMANoAvg() throws Exception {
    MockTimeSeries ts = buildLongSeries();
    
    buildNode((MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setSamples(5)
        .setExponential(true)
        .setAverageInitial(false)
        .setId("win")
        .build());

    
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));
    
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(5.431, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(4.178, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(5.655, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(7.055, value.value().doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void consistentDoublesSamplesAvg() throws Exception {
    MockTimeSeries ts = buildDoubleSeries();
    
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(5.24, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(4.96, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(5.26, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(5.84, value.value().doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void consistentDoublesTimeAvg() throws Exception {
    MockTimeSeries ts = buildDoubleSeries();
    buildNode((MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setInterval("5m")
        .setId("win")
        .build());

    
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(5.24, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(4.96, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(5.26, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(5.84, value.value().doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void consistentDoublesSamplesWMA() throws Exception {
    MockTimeSeries ts = buildDoubleSeries();
    
    buildNode((MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setSamples(5)
        .setWeighted(true)
        .setId("win")
        .build());

    
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(5.053, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(5.573, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(6.653, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(6.033, value.value().doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void consistentDoublesTimeAMM() throws Exception {
    MockTimeSeries ts = buildDoubleSeries();
    buildNode((MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setSamples(5)
        .setMedian(true)
        .setId("win")
        .build());

    
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(6.7, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(6.8, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(8.2, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(8.2, value.value().doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void consistentDoublesSamplesEWMA() throws Exception {
    MockTimeSeries ts = buildDoubleSeries();
    
    buildNode((MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setSamples(5)
        .setExponential(true)
        .setId("win")
        .build());

    
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(5.226, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(5.599, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(6.819, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(6.054, value.value().doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  // don't worry about alphas and such for doubles.
  
  @Test
  public void consistentMixedHasAvg() throws Exception {
    MockTimeSeries ts = buildMixedSeries();
    
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(3.8, value.value().doubleValue(), 0.01);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(4.86, value.value().doubleValue(), 0.01);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(6.26, value.value().doubleValue(), 0.01);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(6.840, value.value().doubleValue(), 0.01);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void consistentMixedHasWMA() throws Exception {
    MockTimeSeries ts = buildMixedSeries();
    
    buildNode((MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setSamples(5)
        .setWeighted(true)
        .setId("win")
        .build());

    
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(4.666, value.value().doubleValue(), 0.01);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(5.666, value.value().doubleValue(), 0.01);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(7.38, value.value().doubleValue(), 0.01);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(6.426, value.value().doubleValue(), 0.01);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void consistentMixedHasEWMA() throws Exception {
    MockTimeSeries ts = buildMixedSeries();
    
    buildNode((MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setSamples(5)
        .setExponential(true)
        .setId("win")
        .build());

    
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(4.787, value.value().doubleValue(), 0.01);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(5.720, value.value().doubleValue(), 0.01);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(7.710, value.value().doubleValue(), 0.01);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(6.207, value.value().doubleValue(), 0.01);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void gapsSamples() throws Exception {
    MockTimeSeries ts = buildGappySeries();
    
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(4.6, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(4.6, value.value().doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void gapsSamplesWMA() throws Exception {
    MockTimeSeries ts = buildGappySeries();
    
    buildNode((MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setSamples(5)
        .setWeighted(true)
        .setId("win")
        .build());

    
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(4.6, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(5.066, value.value().doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void gapsSamplesEWMA() throws Exception {
    MockTimeSeries ts = buildGappySeries();
    
    buildNode((MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setSamples(5)
        .setExponential(true)
        .setId("win")
        .build());

    
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(4.495, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(5.044, value.value().doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void gapsTime() throws Exception {
    MockTimeSeries ts = buildGappySeries();
    
    buildNode((MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setInterval("5m")
        .setId("win")
        .build());

    
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(4.25, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(6, value.value().longValue());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void gapsTimeWMA() throws Exception {
    MockTimeSeries ts = buildGappySeries();
    
    buildNode((MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setInterval("5m")
        .setWeighted(true)
        .setId("win")
        .build());

    
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(4.6, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(5.5, value.value().doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void gapsTimeEWMA() throws Exception {
    MockTimeSeries ts = buildGappySeries();
    
    buildNode((MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setInterval("5m")
        .setExponential(true)
        .setId("win")
        .build());

    
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(4.555, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(5.28, value.value().doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void firstAfterQueryStartTimestampSamplesAvg() throws Exception {
    MockTimeSeries ts = buildNullAtStart();
    
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(3, value.value().longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(6.5, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(6.333, value.value().doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void firstAfterQueryStartTimestampSamplesWMA() throws Exception {
    MockTimeSeries ts = buildNullAtStart();
    
    buildNode((MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setSamples(5)
        .setWeighted(true)
        .setId("win")
        .build());

    
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(3, value.value().longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(7.666, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(6.833, value.value().doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void firstAfterQueryStartTimestampTimeAvg() throws Exception {
    MockTimeSeries ts = buildNullAtStart();
    
    buildNode((MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setInterval("5m")
        .setId("win")
        .build());

    
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(3, value.value().longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(6.5, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(6.333, value.value().doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void firstAfterQueryStartTimestampTimeWMA() throws Exception {
    MockTimeSeries ts = buildNullAtStart();
    
    buildNode((MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setInterval("5m")
        .setWeighted(true)
        .setId("win")
        .build());

    
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(3, value.value().longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(7.666, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(6.833, value.value().doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nullInQueryRangeSamplesAvg() throws Exception {
    MockTimeSeries ts = buildNullInRange();
    
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(5, value.value().longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(5.8, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(6.4, value.value().doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nullInQueryRangeSamplesWMA() throws Exception {
    MockTimeSeries ts = buildNullInRange();
    
    buildNode((MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setSamples(5)
        .setWeighted(true)
        .setId("win")
        .build());

    
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(5.266, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(6.933, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(7.0, value.value().doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nullInQueryRangeTimeAvg() throws Exception {
    MockTimeSeries ts = buildNullInRange();
    
    buildNode((MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setInterval("5m")
        .setId("win")
        .build());

    
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(5, value.value().longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(6.5, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(7.5, value.value().doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nullInQueryRangeTimeWMA() throws Exception {
    MockTimeSeries ts = buildNullInRange();
    
    buildNode((MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setInterval("5m")
        .setWeighted(true)
        .setId("win")
        .build());

    
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(5.266, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(7.5, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(7.300, value.value().doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nullsAtEndSamplesAvg() throws Exception {
    MockTimeSeries ts = buildNullsAtEnd();
    
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(5, value.value().longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(4.4, value.value().doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nullsAtEndSamplesWMA() throws Exception {
    MockTimeSeries ts = buildNullsAtEnd();
    
    buildNode((MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setSamples(5)
        .setWeighted(true)
        .setId("win")
        .build());

    
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(5.266, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(4.6, value.value().doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nullsAtEndTimeAvg() throws Exception {
    MockTimeSeries ts = buildNullsAtEnd();
    
    buildNode((MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setInterval("5m")
        .setId("win")
        .build());

    
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(5, value.value().longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(4.4, value.value().doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nullsAtEndTimeWMA() throws Exception {
    MockTimeSeries ts = buildNullsAtEnd();
    
    buildNode((MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setInterval("5m")
        .setWeighted(true)
        .setId("win")
        .build());

    
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(5.266, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(4.6, value.value().doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nullsPreQueryStartSamplesAvg() throws Exception {
    MockTimeSeries ts = buildNullsPreQuery();
    
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(5.5, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(5, value.value().longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(5.8, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(6.6, value.value().doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nullsPreQueryStartSamplesWMA() throws Exception {
    MockTimeSeries ts = buildNullsPreQuery();
    
    buildNode((MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setSamples(5)
        .setWeighted(true)
        .setId("win")
        .build());

    
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(5.699, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(4.8, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(6.466, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(6.533, value.value().doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nullsPreQueryStartTimeAvg() throws Exception {
    MockTimeSeries ts = buildNullsPreQuery();
    
    buildNode((MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setInterval("5m")
        .setId("win")
        .build());

    
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(5.5, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(4.75, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(5.8, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(6.6, value.value().doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nullsPreQueryStartTimeWMA() throws Exception {
    MockTimeSeries ts = buildNullsPreQuery();
    
    buildNode((MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setInterval("5m")
        .setWeighted(true)
        .setId("win")
        .build());

    
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(5.699, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(4.7, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(6.466, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(6.533, value.value().doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void allNullSamplesAvg() throws Exception {
    MockTimeSeries ts = buildNulls();
    
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void allNullSamplesWMA() throws Exception {
    MockTimeSeries ts = buildNulls();

    buildNode((MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setSamples(5)
        .setWeighted(true)
        .setId("win")
        .build());

    
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void allNullTimeAvg() throws Exception {
    MockTimeSeries ts = buildNulls();
    
    buildNode((MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setInterval("5m")
        .setId("win")
        .build());

    
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void allNullTimeWMA() throws Exception {
    MockTimeSeries ts = buildNulls();
    
    buildNode((MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setInterval("5m")
        .setWeighted(true)
        .setId("win")
        .build());

    
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void emptyIterator() throws Exception {
    MockTimeSeries ts = new MockTimeSeries(id);
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nansBeforeStartSamplesAvg() throws Exception {
    MockTimeSeries ts = buildNansPreQuery();
    
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(5.833, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(6.075, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(6.45, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(5.84, value.value().doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nansBeforeStartSamplesWMA() throws Exception {
    MockTimeSeries ts = buildNansPreQuery();
    
    buildNode((MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setSamples(5)
        .setWeighted(true)
        .setId("win")
        .build());

    
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(6.009, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(6.353, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(7.092, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(6.033, value.value().doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nansBeforeStartSamplesEWMA() throws Exception {
    MockTimeSeries ts = buildNansPreQuery();
    
    buildNode((MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setSamples(5)
        .setExponential(true)
        .setId("win")
        .build());

    
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(6.3368, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(6.4994, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(7.054, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(6.054, value.value().doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nansBeforeStartTimeAvg() throws Exception {
    MockTimeSeries ts = buildNansPreQuery();
    
    buildNode((MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setInterval("5m")
        .setId("win")
        .build());

    
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(5.833, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(6.075, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(6.45, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(5.84, value.value().doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nansBeforeStartTimeWMA() throws Exception {
    MockTimeSeries ts = buildNansPreQuery();
    
    buildNode((MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setInterval("5m")
        .setWeighted(true)
        .setId("win")
        .build());

    
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(6.009, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(6.353, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(7.092, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(6.033, value.value().doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nansBeforeStartTimeEWMA() throws Exception {
    MockTimeSeries ts = buildNansPreQuery();
    
    buildNode((MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setInterval("5m")
        .setExponential(true)
        .setId("win")
        .build());

    
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(6.3368, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(6.4994, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(7.054, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(6.054, value.value().doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nansInQueryRangeSamplesAvg() throws Exception {
    MockTimeSeries ts = buildNansInRange();
    
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(5.24, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(4.5, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(4.875, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(6.333, value.value().doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nansInQueryRangeSamplesWMA() throws Exception {
    MockTimeSeries ts = buildNansInRange();
    
    buildNode((MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setSamples(5)
        .setWeighted(true)
        .setId("win")
        .build());

    
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(5.053, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(4.960, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(6.6, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(7.585, value.value().doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nansInQueryRangeSamplesEWMA() throws Exception {
    MockTimeSeries ts = buildNansInRange();
    
    buildNode((MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setSamples(5)
        .setExponential(true)
        .setId("win")
        .build());

    
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(5.226, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(5.156, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(6.800, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(8.058, value.value().doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nansInQueryRangeTimeAvg() throws Exception {
    MockTimeSeries ts = buildNansInRange();
    
    buildNode((MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setInterval("5m")
        .setId("win")
        .build());

    
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(5.24, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(4.5, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(4.875, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(6.333, value.value().doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nansInQueryRangeTimesWMA() throws Exception {
    MockTimeSeries ts = buildNansInRange();
    
    buildNode((MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setInterval("5m")
        .setWeighted(true)
        .setId("win")
        .build());

    
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(5.053, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(4.960, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(6.6, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(7.585, value.value().doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nansInQueryRangeTimeEWMA() throws Exception {
    MockTimeSeries ts = buildNansInRange();
    
    buildNode((MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setInterval("5m")
        .setExponential(true)
        .setId("win")
        .build());

    
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(5.226, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(5.156, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(6.800, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(8.058, value.value().doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void allNanSampleAvg() throws Exception {
    MockTimeSeries ts = buildAllNan();
    
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertTrue(Double.isNaN(value.value().doubleValue()));
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertTrue(Double.isNaN(value.value().doubleValue()));
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertTrue(Double.isNaN(value.value().doubleValue()));
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertTrue(Double.isNaN(value.value().doubleValue()));
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void allNanSampleWMA() throws Exception {
    MockTimeSeries ts = buildAllNan();
    
    buildNode((MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setSamples(5)
        .setWeighted(true)
        .setId("win")
        .build());

    
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertTrue(Double.isNaN(value.value().doubleValue()));
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertTrue(Double.isNaN(value.value().doubleValue()));
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertTrue(Double.isNaN(value.value().doubleValue()));
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertTrue(Double.isNaN(value.value().doubleValue()));
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void allNanSampleEWMA() throws Exception {
    MockTimeSeries ts = buildAllNan();
    
    buildNode((MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setSamples(5)
        .setExponential(true)
        .setId("win")
        .build());

    
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertTrue(Double.isNaN(value.value().doubleValue()));
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertTrue(Double.isNaN(value.value().doubleValue()));
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertTrue(Double.isNaN(value.value().doubleValue()));
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertTrue(Double.isNaN(value.value().doubleValue()));
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void allNanTimeAvg() throws Exception {
    MockTimeSeries ts = buildAllNan();
    
    buildNode((MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setInterval("5m")
        .setId("win")
        .build());

    
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertTrue(Double.isNaN(value.value().doubleValue()));
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertTrue(Double.isNaN(value.value().doubleValue()));
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertTrue(Double.isNaN(value.value().doubleValue()));
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertTrue(Double.isNaN(value.value().doubleValue()));
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void allNanTimeWMA() throws Exception {
    MockTimeSeries ts = buildAllNan();
    
    buildNode((MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setInterval("5m")
        .setWeighted(true)
        .setId("win")
        .build());

    
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertTrue(Double.isNaN(value.value().doubleValue()));
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertTrue(Double.isNaN(value.value().doubleValue()));
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertTrue(Double.isNaN(value.value().doubleValue()));
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertTrue(Double.isNaN(value.value().doubleValue()));
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void allNanTimeEWMA() throws Exception {
    MockTimeSeries ts = buildAllNan();
    
    buildNode((MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setInterval("5m")
        .setExponential(true)
        .setId("win")
        .build());

    
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertTrue(Double.isNaN(value.value().doubleValue()));
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertTrue(Double.isNaN(value.value().doubleValue()));
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertTrue(Double.isNaN(value.value().doubleValue()));
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertTrue(Double.isNaN(value.value().doubleValue()));
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void shiftArraySamples() throws Exception {
    MockTimeSeries ts = new MockTimeSeries(id);
    for (int i = 0; i < 48; i++) {
      ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * i), 1L));
    }
    
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));
    int i = 5;
    while (iterator.hasNext()) {
      TimeSeriesValue<NumericType> value = 
          (TimeSeriesValue<NumericType>) iterator.next();
      assertEquals(60L * i, value.timestamp().epoch());
      assertEquals(1, value.value().longValue());
      i++;
    }
    assertEquals(32, iterator.arrayLength());
  }
  
  @Test
  public void shiftArrayTime() throws Exception {
    MockTimeSeries ts = new MockTimeSeries(id);
    for (int i = 0; i < 48; i++) {
      ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * i), 1L));
    }
    
    buildNode((MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setInterval("5m")
        .setId("win")
        .build());

    
    MovingAverageNumericIterator iterator = new MovingAverageNumericIterator(
        node, result, Lists.newArrayList(ts));
    int i = 5;
    while (iterator.hasNext()) {
      TimeSeriesValue<NumericType> value = 
          (TimeSeriesValue<NumericType>) iterator.next();
      assertEquals(60L * i, value.timestamp().epoch());
      assertEquals(1, value.value().longValue());
      i++;
    }
    assertEquals(32, iterator.arrayLength());
  }

  private MockTimeSeries buildLongSeries() {
    MockTimeSeries ts = new MockTimeSeries(id);
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(0L), 8L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L), 6L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 2), 3L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 3), 2L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 4), 9L));
    // start of query
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
    // start of query
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 5), 9.5));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 6), 6.8));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 7), 8.2));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 8), 3.4));
    return ts;
  }

  private MockTimeSeries buildMixedSeries() {
    MockTimeSeries ts = new MockTimeSeries(id);
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(0L), 1L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L), 1.5));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 2), 3L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 3), 0.5));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 4), 9L));
    // start of query
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 5), 5L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 6), 6.8));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 7), 10L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 8), 3.4));
    return ts;
  }

  private MockTimeSeries buildGappySeries() {
    MockTimeSeries ts = new MockTimeSeries(id);
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(0L), 8L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L), 6L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 2), 3L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 3), 2L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 4), 9L));
    // start of query
    //ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 5), 5L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 6), 3L));
    //ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 7), 10L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 8), 6L));
    return ts;
  }

  private MockTimeSeries buildNullAtStart() {
    MockTimeSeries ts = new MockTimeSeries(id);
    // start of query
    //ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 5), 5L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 6), 3L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 7), 10L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 8), 6L));
    return ts;
  }
  
  private MockTimeSeries buildNullInRange() {
    MockTimeSeries ts = new MockTimeSeries(id);
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(0L), 8L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L), 6L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 2), 3L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 3), 2L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 4), 9L));
    // start of query
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 5), 5L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 6), null));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 7), 10L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 8), 6L));
    return ts;
  }

  private MockTimeSeries buildNullsAtEnd() {
    MockTimeSeries ts = new MockTimeSeries(id);
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(0L), 8L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L), 6L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 2), 3L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 3), 2L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 4), 9L));
    // start of query
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 5), 5L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 6), 3L));
    //ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 7), 10L));
    //ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 8), 6L));
    return ts;
  }
  
  private MockTimeSeries buildNullsPreQuery() {
    MockTimeSeries ts = new MockTimeSeries(id);
    //ts.addValue(new MutableNumericValue(new SecondTimeStamp(0L), 8L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L), 6L));
    //ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 2), 3L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 3), 2L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 4), 9L));
    // start of query
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 5), 5L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 6), 3L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 7), 10L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 8), 6L));
    return ts;
  }

  private MockTimeSeries buildNulls() {
    MockTimeSeries ts = new MockTimeSeries(id);
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 4), null));
    
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 5), null));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 6), null));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 7), null));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 8), null));
    return ts;
  }
  
  private MockTimeSeries buildNansPreQuery() {
    MockTimeSeries ts = new MockTimeSeries(id);
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(0L), 1.0));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L), Double.NaN));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 2), 6.7));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 3), Double.NaN));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 4), 1.3));
    // start of query
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 5), 9.5));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 6), 6.8));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 7), 8.2));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 8), 3.4));
    return ts;
  }
  
  private MockTimeSeries buildNansInRange() {
    MockTimeSeries ts = new MockTimeSeries(id);
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(0L), 1.0));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L), 8.2));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 2), 6.7));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 3), 0.5));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 4), 1.3));
    // start of query
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
    // start of query
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 5), Double.NaN));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 6), Double.NaN));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 7), Double.NaN));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 8), Double.NaN));
    return ts;
  }

  void buildNode(final MovingAverageConfig config) {
    node = new MovingAverage(mock(MovingAverageFactory.class), context, config); 
  }
  
}
