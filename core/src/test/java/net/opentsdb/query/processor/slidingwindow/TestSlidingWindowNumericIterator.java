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
import org.junit.Test;

import com.google.common.collect.Lists;

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

public class TestSlidingWindowNumericIterator {

  private SemanticQuery query;
  private QueryPipelineContext context;
  private QueryResult result;
  private SlidingWindow node;
  private SlidingWindowConfig config;
  private TimeSeriesStringId id;
  
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
        .build();
    
    when(node.pipelineContext()).thenReturn(context);
    when(context.query()).thenReturn(query);
    
    when(query.startTime()).thenReturn(new SecondTimeStamp(60L * 5));
    when(node.config()).thenReturn(config);
  }
  
  @Test
  public void consistentLongs() throws Exception {
    MockTimeSeries ts = new MockTimeSeries(id);
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(0L), 1L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L), 1L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 2), 1L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 3), 1L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 4), 1L));
    // start of query
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 5), 1L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 6), 1L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 7), 1L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 8), 1L));
    
    SlidingWindowNumericIterator iterator = new SlidingWindowNumericIterator(
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
    assertEquals(5, value.value().longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(5, value.value().longValue());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void consistentDoubles() throws Exception {
    MockTimeSeries ts = new MockTimeSeries(id);
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(0L), 1.0));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L), 1.0));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 2), 1.0));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 3), 1.0));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 4), 1.0));
    // start of query
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 5), 1.0));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 6), 1.0));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 7), 1.0));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 8), 1.0));
    
    SlidingWindowNumericIterator iterator = new SlidingWindowNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(5.0, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(5.0, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(5.0, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(5.0, value.value().doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void consistentPreviousHasFloat() throws Exception {
    MockTimeSeries ts = new MockTimeSeries(id);
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(0L), 1L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L), 1.5));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 2), 1L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 3), 1L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 4), 1L));
    // start of query
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 5), 1L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 6), 1L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 7), 1L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 8), 1L));
    
    SlidingWindowNumericIterator iterator = new SlidingWindowNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(5.5, value.value().doubleValue(), 0.01);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(5.0, value.value().doubleValue(), 0.01);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(5.0, value.value().doubleValue(), 0.01);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(5.0, value.value().doubleValue(), 0.01);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void gaps() throws Exception {
    MockTimeSeries ts = new MockTimeSeries(id);
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(0L), 1L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L), 1L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 2), 1L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 3), 1L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 4), 1L));
    // start of query
    //ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 5), 1L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 6), 1L));
    //ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 7), 1L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 8), 1L));
    
    SlidingWindowNumericIterator iterator = new SlidingWindowNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(4, value.value().longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(3, value.value().longValue());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void firstAfterQueryStartTimestamp() throws Exception {
    MockTimeSeries ts = new MockTimeSeries(id);
    // start of query
    //ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 5), 1L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 6), 1L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 7), 1L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 8), 1L));
    
    SlidingWindowNumericIterator iterator = new SlidingWindowNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(1, value.value().longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(2, value.value().longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(3, value.value().longValue());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nullInQueryRange() throws Exception {
    MockTimeSeries ts = new MockTimeSeries(id);
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(0L), 1L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L), 1L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 2), 1L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 3), 1L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 4), 1L));
    
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 5), 1L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 6), null));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 7), 1L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 8), 1L));
    
    SlidingWindowNumericIterator iterator = new SlidingWindowNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(5, value.value().longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(4, value.value().longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(4, value.value().longValue());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nullsAtEnd() throws Exception {
    MockTimeSeries ts = new MockTimeSeries(id);
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(0L), 1L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L), 1L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 2), 1L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 3), 1L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 4), 1L));
    
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 5), 1L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 6), 1L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 7), null));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 8), null));
    
    SlidingWindowNumericIterator iterator = new SlidingWindowNumericIterator(
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
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nullsPreQueryStart() throws Exception {
    MockTimeSeries ts = new MockTimeSeries(id);
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(0L), null));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L), 1L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 2), null));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 3), 1L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 4), 1L));
    
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 5), 1L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 6), 1L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 7), 1L));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 8), 1L));
    
    SlidingWindowNumericIterator iterator = new SlidingWindowNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(4, value.value().longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(4, value.value().longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(5, value.value().longValue());
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(5, value.value().longValue());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void allNull() throws Exception {
    MockTimeSeries ts = new MockTimeSeries(id);
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 4), null));
    
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 5), null));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 6), null));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 7), null));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 8), null));
    
    SlidingWindowNumericIterator iterator = new SlidingWindowNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void emptyIterator() throws Exception {
    MockTimeSeries ts = new MockTimeSeries(id);
    SlidingWindowNumericIterator iterator = new SlidingWindowNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nansBeforeStart() throws Exception {
    MockTimeSeries ts = new MockTimeSeries(id);
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(0L), 1.0));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L), Double.NaN));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 2), 1.0));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 3), Double.NaN));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 4), 1.0));
    // start of query
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 5), 1.0));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 6), 1.0));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 7), 1.0));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 8), 1.0));
    
    SlidingWindowNumericIterator iterator = new SlidingWindowNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertEquals(3.0, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(4.0, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(4.0, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(5.0, value.value().doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nansForFullWindow() throws Exception {
    MockTimeSeries ts = new MockTimeSeries(id);
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(0L), 1.0));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L), Double.NaN));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 2), Double.NaN));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 3), Double.NaN));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 4), Double.NaN));
    // start of query
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 5), Double.NaN));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 6), 1.0));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 7), 1.0));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 8), 1.0));
    
    SlidingWindowNumericIterator iterator = new SlidingWindowNumericIterator(
        node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 5, value.timestamp().epoch());
    assertTrue(Double.isNaN(value.value().doubleValue()));
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 6, value.timestamp().epoch());
    assertEquals(1.0, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 7, value.timestamp().epoch());
    assertEquals(2.0, value.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(60L * 8, value.timestamp().epoch());
    assertEquals(3.0, value.value().doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nansBeforeStartInfectious() throws Exception {
    config = (SlidingWindowConfig) SlidingWindowConfig.newBuilder()
        .setAggregator("sum")
        .setWindowSize("5m")
        .setInfectiousNan(true)
        .build();
    when(node.config()).thenReturn(config);
    
    MockTimeSeries ts = new MockTimeSeries(id);
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(0L), 1.0));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L), Double.NaN));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 2), 1.0));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 3), Double.NaN));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 4), 1.0));
    // start of query
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 5), 1.0));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 6), 1.0));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 7), 1.0));
    ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * 8), 1.0));
    
    SlidingWindowNumericIterator iterator = new SlidingWindowNumericIterator(
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
    assertEquals(5.0, value.value().doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void allNan() throws Exception {
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
    
    SlidingWindowNumericIterator iterator = new SlidingWindowNumericIterator(
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
  public void shiftArray() throws Exception {
    MockTimeSeries ts = new MockTimeSeries(id);
    for (int i = 0; i < 48; i++) {
      ts.addValue(new MutableNumericValue(new SecondTimeStamp(60L * i), 1L));
    }
    
    SlidingWindowNumericIterator iterator = new SlidingWindowNumericIterator(
        node, result, Lists.newArrayList(ts));
    int i = 5;
    while (iterator.hasNext()) {
      TimeSeriesValue<NumericType> value = 
          (TimeSeriesValue<NumericType>) iterator.next();
      assertEquals(60L * i, value.timestamp().epoch());
      assertEquals(5, value.value().longValue());
      
      i++;
    }
    assertEquals(32, iterator.arrayLength());
  }
}
