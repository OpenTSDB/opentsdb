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
package net.opentsdb.query.processor.slidingwindow;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;

import net.opentsdb.core.DefaultRegistry;
import net.opentsdb.core.MockTSDB;
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

public class TestSlidingWindowNumericSummaryIterator {

  public static MockTSDB TSDB;
  
  private SemanticQuery query;
  private QueryPipelineContext context;
  private QueryResult result;
  private SlidingWindow node;
  private SlidingWindowConfig config;
  private TimeSeriesStringId id;
  
  @BeforeClass
  public static void beforeClass() {
    TSDB = new MockTSDB();
    TSDB.registry = new DefaultRegistry(TSDB);
    ((DefaultRegistry) TSDB.registry).initialize(true);
  }
  
  @Before
  public void before() throws Exception {
    result = mock(QueryResult.class);
    context = mock(QueryPipelineContext.class);
    query = mock(SemanticQuery.class);
    node = mock(SlidingWindow.class);
    id = BaseTimeSeriesStringId.newBuilder()
        .setMetric("sys.cpu.user")
        .build();
    
    config = (SlidingWindowConfig) SlidingWindowConfig.newBuilder()
        .setAggregator("sum")
        .setWindowSize("5m")
        .setId("win")
        .build();
    
    when(node.pipelineContext()).thenReturn(context);
    when(context.query()).thenReturn(query);
    when(context.tsdb()).thenReturn(TSDB);
    
    when(query.startTime()).thenReturn(new SecondTimeStamp(60L * 5));
    when(node.config()).thenReturn(config);
  }
  
  @Test
  public void consistentLongs() throws Exception {
    MockTimeSeries ts = new MockTimeSeries(id);
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 0))
        .addValue(0, 2L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 1))
        .addValue(0, 2L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 2))
        .addValue(0, 2L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 3))
        .addValue(0, 2L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 4))
        .addValue(0, 2L).addValue(2, 1L).build());
    // start of query
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 5))
        .addValue(0, 2L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 6))
        .addValue(0, 2L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 7))
        .addValue(0, 2L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 8))
        .addValue(0, 2L).addValue(2, 1L).build());
    
    SlidingWindowNumericSummaryIterator iterator = 
        new SlidingWindowNumericSummaryIterator(node, result, 
            Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(10, value.value().value(0).longValue());
    assertEquals(5, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(10, value.value().value(0).longValue());
    assertEquals(5, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(10, value.value().value(0).longValue());
    assertEquals(5, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(10, value.value().value(0).longValue());
    assertEquals(5, value.value().value(2).longValue());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void consistentDoubles() throws Exception {
    MockTimeSeries ts = new MockTimeSeries(id);
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 0))
        .addValue(0, 2.0).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 1))
        .addValue(0, 2.0).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 2))
        .addValue(0, 2.0).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 3))
        .addValue(0, 2.0).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 4))
        .addValue(0, 2.0).addValue(2, 1L).build());
    // start of query
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 5))
        .addValue(0, 2.0).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 6))
        .addValue(0, 2.0).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 7))
        .addValue(0, 2.0).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 8))
        .addValue(0, 2.0).addValue(2, 1L).build());
    
    SlidingWindowNumericSummaryIterator iterator = 
        new SlidingWindowNumericSummaryIterator(node, result, 
            Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(10, value.value().value(0).doubleValue(), 0.001);
    assertEquals(5, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(10, value.value().value(0).doubleValue(), 0.001);
    assertEquals(5, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(10, value.value().value(0).doubleValue(), 0.001);
    assertEquals(5, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(10, value.value().value(0).doubleValue(), 0.001);
    assertEquals(5, value.value().value(2).longValue());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void consistentPreviousHasFloat() throws Exception {
    MockTimeSeries ts = new MockTimeSeries(id);
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 0))
        .addValue(0, 2L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 1))
        .addValue(0, 2.5).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 2))
        .addValue(0, 2L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 3))
        .addValue(0, 2L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 4))
        .addValue(0, 2L).addValue(2, 1L).build());
    // start of query
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 5))
        .addValue(0, 2L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 6))
        .addValue(0, 2L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 7))
        .addValue(0, 2L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 8))
        .addValue(0, 2L).addValue(2, 1L).build());
    
    SlidingWindowNumericSummaryIterator iterator = 
        new SlidingWindowNumericSummaryIterator(node, result, 
            Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(10.5, value.value().value(0).doubleValue(), 0.001);
    assertEquals(5, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(10, value.value().value(0).longValue());
    assertEquals(5, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(10, value.value().value(0).longValue());
    assertEquals(5, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(10, value.value().value(0).longValue());
    assertEquals(5, value.value().value(2).longValue());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void gapsOverTime() throws Exception {
    MockTimeSeries ts = new MockTimeSeries(id);
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 0))
        .addValue(0, 2L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 1))
        .addValue(0, 2L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 2))
        .addValue(0, 2L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 3))
        .addValue(0, 2L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 4))
        .addValue(0, 2L).addValue(2, 1L).build());
    // start of query
    //ts.addValue(MutableNumericSummaryValue.newBuilder()
    //    .setTimeStamp(new SecondTimeStamp(60L * 5))
    //    .addValue(0, 2L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 6))
        .addValue(0, 2L).addValue(2, 1L).build());
    //ts.addValue(MutableNumericSummaryValue.newBuilder()
    //    .setTimeStamp(new SecondTimeStamp(60L * 7))
    //    .addValue(0, 2L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 8))
        .addValue(0, 2L).addValue(2, 1L).build());
    
    SlidingWindowNumericSummaryIterator iterator = 
        new SlidingWindowNumericSummaryIterator(node, result, 
            Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(8, value.value().value(0).longValue());
    assertEquals(4, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(6, value.value().value(0).longValue());
    assertEquals(3, value.value().value(2).longValue());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void firstAfterQueryStartTimestamp() throws Exception {
    MockTimeSeries ts = new MockTimeSeries(id);
    // start of query
    //ts.addValue(MutableNumericSummaryValue.newBuilder()
    //    .setTimeStamp(new SecondTimeStamp(60L * 5))
    //    .addValue(0, 2L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 6))
        .addValue(0, 2L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 7))
        .addValue(0, 2L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 8))
        .addValue(0, 2L).addValue(2, 1L).build());
    
    SlidingWindowNumericSummaryIterator iterator = 
        new SlidingWindowNumericSummaryIterator(node, result, 
            Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(2, value.value().value(0).longValue());
    assertEquals(1, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(4, value.value().value(0).longValue());
    assertEquals(2, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(6, value.value().value(0).longValue());
    assertEquals(3, value.value().value(2).longValue());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nullInQueryRange() throws Exception {
    MockTimeSeries ts = new MockTimeSeries(id);
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 0))
        .addValue(0, 2L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 1))
        .addValue(0, 2L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 2))
        .addValue(0, 2L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 3))
        .addValue(0, 2L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 4))
        .addValue(0, 2L).addValue(2, 1L).build());
    // start of query
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 5))
        .addValue(0, 2L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 6))
        .setNull().build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 7))
        .addValue(0, 2L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 8))
        .addValue(0, 2L).addValue(2, 1L).build());
    
    SlidingWindowNumericSummaryIterator iterator = 
        new SlidingWindowNumericSummaryIterator(node, result, 
            Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(10, value.value().value(0).longValue());
    assertEquals(5, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(8, value.value().value(0).longValue());
    assertEquals(4, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(8, value.value().value(0).longValue());
    assertEquals(4, value.value().value(2).longValue());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nullsAtEnd() throws Exception {
    MockTimeSeries ts = new MockTimeSeries(id);
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 0))
        .addValue(0, 2L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 1))
        .addValue(0, 2L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 2))
        .addValue(0, 2L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 3))
        .addValue(0, 2L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 4))
        .addValue(0, 2L).addValue(2, 1L).build());
    // start of query
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 5))
        .addValue(0, 2L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 6))
        .addValue(0, 2L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 7))
        .setNull().build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 8))
        .setNull().build());
    
    SlidingWindowNumericSummaryIterator iterator = 
        new SlidingWindowNumericSummaryIterator(node, result, 
            Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(10, value.value().value(0).longValue());
    assertEquals(5, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(10, value.value().value(0).longValue());
    assertEquals(5, value.value().value(2).longValue());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nullsPreQueryStart() throws Exception {
    MockTimeSeries ts = new MockTimeSeries(id);
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 0))
        .setNull().build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 1))
        .addValue(0, 2L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 2))
        .setNull().build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 3))
        .addValue(0, 2L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 4))
        .addValue(0, 2L).addValue(2, 1L).build());
    // start of query
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 5))
        .addValue(0, 2L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 6))
        .addValue(0, 2L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 7))
        .addValue(0, 2L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 8))
        .addValue(0, 2L).addValue(2, 1L).build());
    
    SlidingWindowNumericSummaryIterator iterator = 
        new SlidingWindowNumericSummaryIterator(node, result, 
            Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(8, value.value().value(0).longValue());
    assertEquals(4, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(8, value.value().value(0).longValue());
    assertEquals(4, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(10, value.value().value(0).longValue());
    assertEquals(5, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(10, value.value().value(0).longValue());
    assertEquals(5, value.value().value(2).longValue());
    
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
    
    SlidingWindowNumericSummaryIterator iterator = 
        new SlidingWindowNumericSummaryIterator(node, result, 
            Lists.newArrayList(ts));
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void emptyIterator() throws Exception {
    MockTimeSeries ts = new MockTimeSeries(id);
    SlidingWindowNumericSummaryIterator iterator = 
        new SlidingWindowNumericSummaryIterator(node, result, 
            Lists.newArrayList(ts));
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nansBeforeStart() throws Exception {
    MockTimeSeries ts = new MockTimeSeries(id);
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 0))
        .addValue(0, 2.0).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 1))
        .addValue(0, Double.NaN).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 2))
        .addValue(0, 2.0).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 3))
        .addValue(0, Double.NaN).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 4))
        .addValue(0, 2.0).addValue(2, 1L).build());
    // start of query
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 5))
        .addValue(0, 2.0).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 6))
        .addValue(0, 2.0).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 7))
        .addValue(0, 2.0).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 8))
        .addValue(0, 2.0).addValue(2, 1L).build());
    
    SlidingWindowNumericSummaryIterator iterator = 
        new SlidingWindowNumericSummaryIterator(node, result, 
            Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(6, value.value().value(0).doubleValue(), 0.001);
    assertEquals(5, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(8, value.value().value(0).doubleValue(), 0.001);
    assertEquals(5, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(8, value.value().value(0).doubleValue(), 0.001);
    assertEquals(5, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(10, value.value().value(0).doubleValue(), 0.001);
    assertEquals(5, value.value().value(2).longValue());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nansForFullWindow() throws Exception {
    MockTimeSeries ts = new MockTimeSeries(id);
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 0))
        .addValue(0, 2.0).addValue(2, 1L).build());
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
        .addValue(0, 2.0).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 7))
        .addValue(0, 2.0).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 8))
        .addValue(0, 2.0).addValue(2, 1L).build());
    
    SlidingWindowNumericSummaryIterator iterator = 
        new SlidingWindowNumericSummaryIterator(node, result, 
            Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertTrue(Double.isNaN(value.value().value(0).doubleValue()));
    assertEquals(5, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(2, value.value().value(0).doubleValue(), 0.001);
    assertEquals(5, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(4, value.value().value(0).doubleValue(), 0.001);
    assertEquals(5, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(6, value.value().value(0).doubleValue(), 0.001);
    assertEquals(5, value.value().value(2).longValue());
    
    assertFalse(iterator.hasNext());
  }

  @Test
  public void nansBeforeStartInfectious() throws Exception {
    config = (SlidingWindowConfig) SlidingWindowConfig.newBuilder()
        .setAggregator("sum")
        .setWindowSize("5m")
        .setInfectiousNan(true)
        .setId("win")
        .build();
    when(node.config()).thenReturn(config);
    
    MockTimeSeries ts = new MockTimeSeries(id);
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 0))
        .addValue(0, 2.0).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 1))
        .addValue(0, Double.NaN).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 2))
        .addValue(0, 2.0).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 3))
        .addValue(0, Double.NaN).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 4))
        .addValue(0, 2.0).addValue(2, 1L).build());
    // start of query
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 5))
        .addValue(0, 2.0).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 6))
        .addValue(0, 2.0).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 7))
        .addValue(0, 2.0).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 8))
        .addValue(0, 2.0).addValue(2, 1L).build());
    
    SlidingWindowNumericSummaryIterator iterator = 
        new SlidingWindowNumericSummaryIterator(node, result, 
            Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertTrue(Double.isNaN(value.value().value(0).doubleValue()));
    assertEquals(5, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertTrue(Double.isNaN(value.value().value(0).doubleValue()));
    assertEquals(5, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertTrue(Double.isNaN(value.value().value(0).doubleValue()));
    assertEquals(5, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(10, value.value().value(0).doubleValue(), 0.001);
    assertEquals(5, value.value().value(2).longValue());
    
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
    
    SlidingWindowNumericSummaryIterator iterator = 
        new SlidingWindowNumericSummaryIterator(node, result, 
            Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertTrue(Double.isNaN(value.value().value(0).doubleValue()));
    assertEquals(5, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertTrue(Double.isNaN(value.value().value(0).doubleValue()));
    assertEquals(5, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertTrue(Double.isNaN(value.value().value(0).doubleValue()));
    assertEquals(5, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertTrue(Double.isNaN(value.value().value(0).doubleValue()));
    assertEquals(5, value.value().value(2).longValue());
    
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
    
    SlidingWindowNumericSummaryIterator iterator = 
        new SlidingWindowNumericSummaryIterator(node, result, 
            Lists.newArrayList(ts));
    int i = 5;
    while (iterator.hasNext()) {
      TimeSeriesValue<NumericSummaryType> value = 
          (TimeSeriesValue<NumericSummaryType>) iterator.next();
      assertEquals(10, value.value().value(0).longValue());
      assertEquals(5, value.value().value(2).longValue());
      
      i++;
    }
    assertEquals(32, iterator.arrayLength());
  }
  
  @Test
  public void offsetValues() throws Exception {
    MockTimeSeries ts = new MockTimeSeries(id);
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 0))
        .addValue(0, 2L)/*.addValue(2, 1L)*/.build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 1))
        .addValue(0, 2L)/*.addValue(2, 1L)*/.build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 2))
        /*.addValue(0, 2L)*/.addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 3))
        .addValue(0, 2L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 4))
        .addValue(0, 2L)/*.addValue(2, 1L)*/.build());
    // start of query
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 5))
        .addValue(0, 2L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 6))
        /*.addValue(0, 2L)*/.addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 7))
        .addValue(0, 2L).addValue(2, 1L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 8))
        .addValue(0, 2L).addValue(2, 1L).build());
    
    SlidingWindowNumericSummaryIterator iterator = 
        new SlidingWindowNumericSummaryIterator(node, result, 
            Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(8, value.value().value(0).longValue());
    assertEquals(3, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(6, value.value().value(0).longValue());
    assertEquals(4, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(8, value.value().value(0).longValue());
    assertEquals(4, value.value().value(2).longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(8, value.value().value(0).longValue());
    assertEquals(4, value.value().value(2).longValue());
    
    assertFalse(iterator.hasNext());
  }
  
}
