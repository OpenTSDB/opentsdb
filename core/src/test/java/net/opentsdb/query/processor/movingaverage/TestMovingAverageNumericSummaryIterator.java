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
import net.opentsdb.data.types.numeric.MutableNumericSummaryValue;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.SemanticQuery;

public class TestMovingAverageNumericSummaryIterator {

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
    
    when(query.startTime()).thenReturn(new SecondTimeStamp(60L * 5));
    when(node.config()).thenReturn(config);
  }
  
  @Test
  public void consistentLongsSamplesAvg() throws Exception {
    MockTimeSeries ts = buildLongs();
    
    MovingAverageNumericSummaryIterator iterator = 
        new MovingAverageNumericSummaryIterator(node, result, 
            Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(5, value.value().value(0).longValue());
    assertEquals(1, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(5.8, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(6.6, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(6.2, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).longValue());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void consistentLongsSamplesWMA() throws Exception {
    MockTimeSeries ts = buildLongs();
    
    config = (MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setSamples(5)
        .setWeighted(true)
        .setId("win")
        .build();
    when(node.config()).thenReturn(config);
    
    MovingAverageNumericSummaryIterator iterator = 
        new MovingAverageNumericSummaryIterator(node, result, 
            Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(4.8, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(6.466, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(6.533, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(6.666, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void consistentLongsSamplesEWMA() throws Exception {
    MockTimeSeries ts = buildLongs();
    
    config = (MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setSamples(5)
        .setExponential(true)
        .setId("win")
        .build();
    when(node.config()).thenReturn(config);
    
    MovingAverageNumericSummaryIterator iterator = 
        new MovingAverageNumericSummaryIterator(node, result, 
            Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(4.699, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(6.645, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(6.431, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(6.608, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void consistentLongsTimeAvg() throws Exception {
    MockTimeSeries ts = buildLongs();
    
    config = (MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setInterval("5m")
        .setId("win")
        .build();
    when(node.config()).thenReturn(config);
    
    MovingAverageNumericSummaryIterator iterator = 
        new MovingAverageNumericSummaryIterator(node, result, 
            Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(5, value.value().value(0).longValue());
    assertEquals(1, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(5.8, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(6.6, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(6.2, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).longValue());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void consistentLongsTimeWMA() throws Exception {
    MockTimeSeries ts = buildLongs();
    
    config = (MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setInterval("5m")
        .setWeighted(true)
        .setId("win")
        .build();
    when(node.config()).thenReturn(config);
    
    MovingAverageNumericSummaryIterator iterator = 
        new MovingAverageNumericSummaryIterator(node, result, 
            Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(4.8, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(6.466, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(6.533, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(6.666, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void consistentLongsTimeEWMA() throws Exception {
    MockTimeSeries ts = buildLongs();
    
    config = (MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setInterval("5m")
        .setExponential(true)
        .setId("win")
        .build();
    when(node.config()).thenReturn(config);
    
    MovingAverageNumericSummaryIterator iterator = 
        new MovingAverageNumericSummaryIterator(node, result, 
            Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(4.699, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(6.645, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(6.431, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(6.608, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void consistentDoublesSamplesAvg() throws Exception {
    MockTimeSeries ts = buildDoubles();
    
    MovingAverageNumericSummaryIterator iterator = 
        new MovingAverageNumericSummaryIterator(node, result, 
            Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(5.24, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(4.96, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(5.26, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(5.84, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).longValue());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void consistentDoublesSamplesWMA() throws Exception {
    MockTimeSeries ts = buildDoubles();
    
    config = (MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setSamples(5)
        .setWeighted(true)
        .setId("win")
        .build();
    when(node.config()).thenReturn(config);
    
    MovingAverageNumericSummaryIterator iterator = 
        new MovingAverageNumericSummaryIterator(node, result, 
            Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(5.053, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(5.573, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(6.653, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(6.033, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void consistentDoublesSamplesEWMA() throws Exception {
    MockTimeSeries ts = buildDoubles();
    
    config = (MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setSamples(5)
        .setExponential(true)
        .setId("win")
        .build();
    when(node.config()).thenReturn(config);
    
    MovingAverageNumericSummaryIterator iterator = 
        new MovingAverageNumericSummaryIterator(node, result, 
            Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(5.226, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(5.599, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(6.819, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(6.054, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void consistentDoublesTimeAvg() throws Exception {
    MockTimeSeries ts = buildDoubles();
    
    config = (MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setInterval("5m")
        .setId("win")
        .build();
    when(node.config()).thenReturn(config);
    
    MovingAverageNumericSummaryIterator iterator = 
        new MovingAverageNumericSummaryIterator(node, result, 
            Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(5.24, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(4.96, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(5.26, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(5.84, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).longValue());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void consistentDoublesTimeWMA() throws Exception {
    MockTimeSeries ts = buildDoubles();
    
    config = (MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setInterval("5m")
        .setWeighted(true)
        .setId("win")
        .build();
    when(node.config()).thenReturn(config);
    
    MovingAverageNumericSummaryIterator iterator = 
        new MovingAverageNumericSummaryIterator(node, result, 
            Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(5.053, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(5.573, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(6.653, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(6.033, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void consistentDoublesTimeEWMA() throws Exception {
    MockTimeSeries ts = buildDoubles();
    
    config = (MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setInterval("5m")
        .setExponential(true)
        .setId("win")
        .build();
    when(node.config()).thenReturn(config);
    
    MovingAverageNumericSummaryIterator iterator = 
        new MovingAverageNumericSummaryIterator(node, result, 
            Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(5.226, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(5.599, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(6.819, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(6.054, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void consistentMixedSamplesAvg() throws Exception {
    MockTimeSeries ts = buildMixed();
    
    MovingAverageNumericSummaryIterator iterator = 
        new MovingAverageNumericSummaryIterator(node, result, 
            Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(5.04, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(4.8, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(5.060, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(5.64, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).longValue());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void consistentMixedSamplesWMA() throws Exception {
    MockTimeSeries ts = buildMixed();
    
    config = (MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setSamples(5)
        .setWeighted(true)
        .setId("win")
        .build();
    when(node.config()).thenReturn(config);
    
    MovingAverageNumericSummaryIterator iterator = 
        new MovingAverageNumericSummaryIterator(node, result, 
            Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(4.793, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(5.380, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(6.446, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(5.893, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void consistentMixedSamplesEWMA() throws Exception {
    MockTimeSeries ts = buildMixed();
    
    config = (MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setSamples(5)
        .setExponential(true)
        .setId("win")
        .build();
    when(node.config()).thenReturn(config);
    
    MovingAverageNumericSummaryIterator iterator = 
        new MovingAverageNumericSummaryIterator(node, result, 
            Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(4.953, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(5.412, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(6.609, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(5.921, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void gapsAcrossBothSamples() throws Exception {
    MockTimeSeries ts = buildMissingBoth();
    
    MovingAverageNumericSummaryIterator iterator = 
        new MovingAverageNumericSummaryIterator(node, result, 
            Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(6.4, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(6.6, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).longValue());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void gapsAcrossBothTime() throws Exception {
    MockTimeSeries ts = buildMissingBoth();
    
    config = (MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setInterval("5m")
        .setId("win")
        .build();
    when(node.config()).thenReturn(config);
    
    MovingAverageNumericSummaryIterator iterator = 
        new MovingAverageNumericSummaryIterator(node, result, 
            Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(6.5, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(7.333, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).longValue());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void gapsOffsetSamples() throws Exception {
    MockTimeSeries ts = buildMissingOffset();
    
    MovingAverageNumericSummaryIterator iterator = 
        new MovingAverageNumericSummaryIterator(node, result, 
            Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(5.75, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(5.666, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(5.75, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(4.666, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).longValue());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void gapsOffsetTime() throws Exception {
    MockTimeSeries ts = buildMissingOffset();
    
    config = (MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setInterval("5m")
        .setId("win")
        .build();
    when(node.config()).thenReturn(config);
    
    MovingAverageNumericSummaryIterator iterator = 
        new MovingAverageNumericSummaryIterator(node, result, 
            Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(5.75, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(5.666, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(5.75, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(4.666, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).longValue());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void firstAfterQueryStartTimestamp() throws Exception {
    MockTimeSeries ts = buildMissingQueryStart();
    
    MovingAverageNumericSummaryIterator iterator = 
        new MovingAverageNumericSummaryIterator(node, result, 
            Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(10, value.value().value(0).longValue());
    assertEquals(1, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(8, value.value().value(0).longValue());
    assertEquals(1, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(7.666, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).longValue());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nullInQueryRangeSamples() throws Exception {
    MockTimeSeries ts = buildNullInRange();
    
    MovingAverageNumericSummaryIterator iterator = 
        new MovingAverageNumericSummaryIterator(node, result, 
            Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(5, value.value().value(0).longValue());
    assertEquals(1, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(5, value.value().value(0).longValue());
    assertEquals(1, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(6, value.value().value(0).longValue());
    assertEquals(1, value.value().value(2).longValue());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nullInQueryRangeTime() throws Exception {
    MockTimeSeries ts = buildNullInRange();
    
    config = (MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setInterval("5m")
        .setId("win")
        .build();
    when(node.config()).thenReturn(config);
    
    MovingAverageNumericSummaryIterator iterator = 
        new MovingAverageNumericSummaryIterator(node, result, 
            Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(5, value.value().value(0).longValue());
    assertEquals(1, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(5.75, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(5.25, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).longValue());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nullsAtEndSamples() throws Exception {
    MockTimeSeries ts = buildNullsAtEnd();
    
    MovingAverageNumericSummaryIterator iterator = 
        new MovingAverageNumericSummaryIterator(node, result, 
            Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(5, value.value().value(0).longValue());
    assertEquals(1, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(5.8, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).longValue());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nullsAtEndTime() throws Exception {
    MockTimeSeries ts = buildNullsAtEnd();
    
    config = (MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setInterval("5m")
        .setId("win")
        .build();
    when(node.config()).thenReturn(config);
    
    MovingAverageNumericSummaryIterator iterator = 
        new MovingAverageNumericSummaryIterator(node, result, 
            Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(5, value.value().value(0).longValue());
    assertEquals(1, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(5.8, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).longValue());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nullsPreQueryStartSamples() throws Exception {
    MockTimeSeries ts = buildNullsBeforeStart();
    
    MovingAverageNumericSummaryIterator iterator = 
        new MovingAverageNumericSummaryIterator(node, result, 
            Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(4.875, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(5.26, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(5.26, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(5.84, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).longValue());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nullsPreQueryStartTime() throws Exception {
    MockTimeSeries ts = buildNullsBeforeStart();
    
    config = (MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setInterval("5m")
        .setId("win")
        .build();
    when(node.config()).thenReturn(config);
    
    MovingAverageNumericSummaryIterator iterator = 
        new MovingAverageNumericSummaryIterator(node, result, 
            Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(4.875, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(4.525, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(5.26, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(5.84, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).longValue());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void allNull() throws Exception {
    MockTimeSeries ts = new MockTimeSeries(id);
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 4))
        .setNull().build());
    // start of query
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 5))
        .setNull().build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 6))
        .setNull().build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 7))
        .setNull().build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 8))
        .setNull().build());
    
    MovingAverageNumericSummaryIterator iterator = 
        new MovingAverageNumericSummaryIterator(node, result, 
            Lists.newArrayList(ts));
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void emptyIterator() throws Exception {
    MockTimeSeries ts = new MockTimeSeries(id);
    MovingAverageNumericSummaryIterator iterator = 
        new MovingAverageNumericSummaryIterator(node, result, 
            Lists.newArrayList(ts));
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nansBeforeStartSamples() throws Exception {
    MockTimeSeries ts = buildNansBeforeStart();
    
    MovingAverageNumericSummaryIterator iterator = 
        new MovingAverageNumericSummaryIterator(node, result, 
            Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(5.833, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(6.075, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(6.45, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(5.84, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).longValue());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nansBeforeStartTime() throws Exception {
    MockTimeSeries ts = buildNansBeforeStart();
    
    config = (MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setInterval("5m")
        .setId("win")
        .build();
    when(node.config()).thenReturn(config);
    
    MovingAverageNumericSummaryIterator iterator = 
        new MovingAverageNumericSummaryIterator(node, result, 
            Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(5.833, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(6.075, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(6.45, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(5.84, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).longValue());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nansForFullWindowSamples() throws Exception {
    MockTimeSeries ts = buildNansForFullWindow();
    
    MovingAverageNumericSummaryIterator iterator = 
        new MovingAverageNumericSummaryIterator(node, result, 
            Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertTrue(Double.isNaN(value.value().value(0).doubleValue()));
    assertEquals(1, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(6.8, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(7.5, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(6.133, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).longValue());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nansForFullWindowTime() throws Exception {
    MockTimeSeries ts = buildNansForFullWindow();
    
    config = (MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setInterval("5m")
        .setId("win")
        .build();
    when(node.config()).thenReturn(config);
    
    
    MovingAverageNumericSummaryIterator iterator = 
        new MovingAverageNumericSummaryIterator(node, result, 
            Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertTrue(Double.isNaN(value.value().value(0).doubleValue()));
    assertEquals(1, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(6.8, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(7.5, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(6.133, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).longValue());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void allNan() throws Exception {
    MockTimeSeries ts = new MockTimeSeries(id);
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 0))
        .addValue(0, Double.NaN).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 1))
        .addValue(0, Double.NaN).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 2))
        .addValue(0, Double.NaN).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 3))
        .addValue(0, Double.NaN).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 4))
        .addValue(0, Double.NaN).addValue(2, 1L).build());
    // start of query
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 5))
        .addValue(0, Double.NaN).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 6))
        .addValue(0, Double.NaN).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 7))
        .addValue(0, Double.NaN).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 8))
        .addValue(0, Double.NaN).addValue(2, 1L).build());
    
    MovingAverageNumericSummaryIterator iterator = 
        new MovingAverageNumericSummaryIterator(node, result, 
            Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertTrue(Double.isNaN(value.value().value(0).doubleValue()));
    assertEquals(1, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertTrue(Double.isNaN(value.value().value(0).doubleValue()));
    assertEquals(1, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertTrue(Double.isNaN(value.value().value(0).doubleValue()));
    assertEquals(1, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertTrue(Double.isNaN(value.value().value(0).doubleValue()));
    assertEquals(1, value.value().value(2).longValue());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void shiftArray() throws Exception {
    MockTimeSeries ts = new MockTimeSeries(id);
    for (int i = 0; i < 48; i++) {
      ts.addValue(MutableNumericSummaryValue.newBuilder()
          .setTimeStamp(new SecondTimeStamp(60L * i))
          .addValue(0, 2L).addValue(2, 1L).build());
    }
    
    MovingAverageNumericSummaryIterator iterator = 
        new MovingAverageNumericSummaryIterator(node, result, 
            Lists.newArrayList(ts));
    int i = 5;
    while (iterator.hasNext()) {
      TimeSeriesValue<NumericSummaryType> value = 
          (TimeSeriesValue<NumericSummaryType>) iterator.next();
      assertEquals(2, value.value().value(0).longValue());
      assertEquals(1, value.value().value(2).longValue());
      
      i++;
    }
    assertEquals(32, iterator.arrayLength());
  }
  
  private MockTimeSeries buildLongs() {
    MockTimeSeries ts = new MockTimeSeries(id);
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 0))
        .addValue(0, 8L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 1))
        .addValue(0, 6L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 2))
        .addValue(0, 2L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 3))
        .addValue(0, 9L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 4))
        .addValue(0, 5L).addValue(2, 1L).build());
    // start of query
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 5))
        .addValue(0, 3L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 6))
        .addValue(0, 10L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 7))
        .addValue(0, 6L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 8))
        .addValue(0, 7L).addValue(2, 1L).build());
    return ts;
  }
  
  private MockTimeSeries buildDoubles() {
    MockTimeSeries ts = new MockTimeSeries(id);
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 0))
        .addValue(0, 1.0).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 1))
        .addValue(0, 8.2).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 2))
        .addValue(0, 6.7).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 3))
        .addValue(0, 0.5).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 4))
        .addValue(0, 1.3).addValue(2, 1L).build());
    // start of query
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 5))
        .addValue(0, 9.5).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 6))
        .addValue(0, 6.8).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 7))
        .addValue(0, 8.2).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 8))
        .addValue(0, 3.4).addValue(2, 1L).build());
    return ts;
  }
  
  private MockTimeSeries buildMixed() {
    MockTimeSeries ts = new MockTimeSeries(id);
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 0))
        .addValue(0, 1.0).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 1))
        .addValue(0, 8L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 2))
        .addValue(0, 6.7).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 3))
        .addValue(0, 0.5).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 4))
        .addValue(0, 1L).addValue(2, 1L).build());
    // start of query
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 5))
        .addValue(0, 9L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 6))
        .addValue(0, 6.8).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 7))
        .addValue(0, 8L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 8))
        .addValue(0, 3.4).addValue(2, 1L).build());
    return ts;
  }

  private MockTimeSeries buildMissingBoth() {
    MockTimeSeries ts = new MockTimeSeries(id);
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 0))
        .addValue(0, 8L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 1))
        .addValue(0, 6L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 2))
        .addValue(0, 2L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 3))
        .addValue(0, 9L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 4))
        .addValue(0, 5L).addValue(2, 1L).build());
    // start of query
//    ts.addValue(MutableNumericSummaryValue.newBuilder()
//        .setTimeStamp(new SecondTimeStamp(60L * 5))
//        .addValue(0, 3L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 6))
        .addValue(0, 10L).addValue(2, 1L).build());
//    ts.addValue(MutableNumericSummaryValue.newBuilder()
//        .setTimeStamp(new SecondTimeStamp(60L * 7))
//        .addValue(0, 6L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 8))
        .addValue(0, 7L).addValue(2, 1L).build());
    return ts;
  }
  
  private MockTimeSeries buildMissingOffset() {
    MockTimeSeries ts = new MockTimeSeries(id);
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 0))
        .addValue(0, 8L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 1))
        .addValue(0, 6L)/*.addValue(2, 1L)*/.build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 2))
        /*.addValue(0, 2L)*/.addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 3))
        .addValue(0, 9L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 4))
        .addValue(0, 5L).addValue(2, 1L).build());
    // start of query
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 5))
        .addValue(0, 3L)/*.addValue(2, 1L)*/.build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 6))
        /*.addValue(0, 10L)*/.addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 7))
        .addValue(0, 6L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 8))
        /*.addValue(0, 7L)*/.addValue(2, 1L).build());
    return ts;
  }

  private MockTimeSeries buildMissingQueryStart() {
    MockTimeSeries ts = new MockTimeSeries(id);
    // start of query
//    ts.addValue(MutableNumericSummaryValue.newBuilder()
//        .setTimeStamp(new SecondTimeStamp(60L * 5))
//        .addValue(0, 3L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 6))
        .addValue(0, 10L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 7))
        .addValue(0, 6L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 8))
        .addValue(0, 7L).addValue(2, 1L).build());
    return ts;
  }

  private MockTimeSeries buildNullInRange() {
    MockTimeSeries ts = new MockTimeSeries(id);
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 0))
        .addValue(0, 8L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 1))
        .addValue(0, 6L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 2))
        .addValue(0, 2L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 3))
        .addValue(0, 9L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 4))
        .addValue(0, 5L).addValue(2, 1L).build());
    // start of query
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 5))
        .addValue(0, 3L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 6))
        .setNull().build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 7))
        .addValue(0, 6L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 8))
        .addValue(0, 7L).addValue(2, 1L).build());
    return ts;
  }
  
  private MockTimeSeries buildNullsAtEnd() {
    MockTimeSeries ts = new MockTimeSeries(id);
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 0))
        .addValue(0, 8L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 1))
        .addValue(0, 6L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 2))
        .addValue(0, 2L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 3))
        .addValue(0, 9L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 4))
        .addValue(0, 5L).addValue(2, 1L).build());
    // start of query
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 5))
        .addValue(0, 3L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 6))
        .addValue(0, 10L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 7))
        .setNull().build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 8))
        .setNull().build());
    return ts;
  }

  private MockTimeSeries buildNullsBeforeStart() {
    MockTimeSeries ts = new MockTimeSeries(id);
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 0))
        .setNull().addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 1))
        .addValue(0, 8.2).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 2))
        .setNull().addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 3))
        .addValue(0, 0.5).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 4))
        .addValue(0, 1.3).addValue(2, 1L).build());
    // start of query
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 5))
        .addValue(0, 9.5).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 6))
        .addValue(0, 6.8).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 7))
        .addValue(0, 8.2).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 8))
        .addValue(0, 3.4).addValue(2, 1L).build());
    return ts;
  }
  
  private MockTimeSeries buildNansBeforeStart() {
    MockTimeSeries ts = new MockTimeSeries(id);
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 0))
        .addValue(0, 1.0).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 1))
        .addValue(0, Double.NaN).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 2))
        .addValue(0, 6.7).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 3))
        .addValue(0, Double.NaN).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 4))
        .addValue(0, 1.3).addValue(2, 1L).build());
    // start of query
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 5))
        .addValue(0, 9.5).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 6))
        .addValue(0, 6.8).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 7))
        .addValue(0, 8.2).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 8))
        .addValue(0, 3.4).addValue(2, 1L).build());
    return ts;
  }
  
  private MockTimeSeries buildNansForFullWindow() {
    MockTimeSeries ts = new MockTimeSeries(id);
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 0))
        .addValue(0, Double.NaN).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 1))
        .addValue(0, Double.NaN).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 2))
        .addValue(0, Double.NaN).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 3))
        .addValue(0, Double.NaN).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 4))
        .addValue(0, Double.NaN).addValue(2, 1L).build());
    // start of query
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 5))
        .addValue(0, Double.NaN).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 6))
        .addValue(0, 6.8).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 7))
        .addValue(0, 8.2).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 8))
        .addValue(0, 3.4).addValue(2, 1L).build());
    return ts;
  }
  
}
