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
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.SemanticQuery;

public class TestSlidingWindowNumericArrayIterator {

  public static MockTSDB TSDB;
  
  private SemanticQuery query;
  private QueryPipelineContext context;
  private QueryResult result;
  private TimeSpecification time_spec;
  private SlidingWindow node;
  private SlidingWindowConfig config;
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
    
    when(query.startTime()).thenReturn(new SecondTimeStamp(0));
    when(node.config()).thenReturn(config);
    when(result.timeSpecification()).thenReturn(time_spec);
    
    when(time_spec.interval()).thenReturn(Duration.ofSeconds(60));
  }
  
  @Test
  public void consistentLongs() throws Exception {
    setSource(new MutableNumericValue(new SecondTimeStamp(0L), 1L),
              new MutableNumericValue(new SecondTimeStamp(60L), 1L),
              new MutableNumericValue(new SecondTimeStamp(60L * 2), 1L),
              new MutableNumericValue(new SecondTimeStamp(60L * 3), 1L),
              new MutableNumericValue(new SecondTimeStamp(60L * 4), 1L),
              new MutableNumericValue(new SecondTimeStamp(60L * 5), 1L),
              new MutableNumericValue(new SecondTimeStamp(60L * 6), 1L),
              new MutableNumericValue(new SecondTimeStamp(60L * 7), 1L),
              new MutableNumericValue(new SecondTimeStamp(60L * 8), 1L));
    
    SlidingWindowNumericArrayIterator iterator = new SlidingWindowNumericArrayIterator(
        node, result, Lists.newArrayList(source));
    assertTrue(iterator.hasNext());
    
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertEquals(0, value.timestamp().epoch());
    assertArrayEquals(new long[] { 1, 2, 3, 4, 5, 5, 5, 5, 5 },
        value.value().longArray());
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void consistentDoubles() throws Exception {
    setSource(new MutableNumericValue(new SecondTimeStamp(0L), 1.0),
        new MutableNumericValue(new SecondTimeStamp(60L), 1.0),
        new MutableNumericValue(new SecondTimeStamp(60L * 2), 1.0),
        new MutableNumericValue(new SecondTimeStamp(60L * 3), 1.0),
        new MutableNumericValue(new SecondTimeStamp(60L * 4), 1.0),
        new MutableNumericValue(new SecondTimeStamp(60L * 5), 1.0),
        new MutableNumericValue(new SecondTimeStamp(60L * 6), 1.0),
        new MutableNumericValue(new SecondTimeStamp(60L * 7), 1.0),
        new MutableNumericValue(new SecondTimeStamp(60L * 8), 1.0));
    
    SlidingWindowNumericArrayIterator iterator = new SlidingWindowNumericArrayIterator(
        node, result, Lists.newArrayList(source));
    
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertEquals(0, value.timestamp().epoch());
    assertArrayEquals(new double[] { 1, 2, 3, 4, 5, 5, 5, 5, 5 },
        value.value().doubleArray(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void emptyIterator() throws Exception {
    MockTimeSeries ts = new MockTimeSeries(id);
    SlidingWindowNumericArrayIterator iterator = new SlidingWindowNumericArrayIterator(
        node, result, Lists.newArrayList(ts));
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nans() throws Exception {
    setSource(new MutableNumericValue(new SecondTimeStamp(0L), Double.NaN),
        new MutableNumericValue(new SecondTimeStamp(60L), 1.0),
        new MutableNumericValue(new SecondTimeStamp(60L * 2), 1.0),
        new MutableNumericValue(new SecondTimeStamp(60L * 3), Double.NaN),
        new MutableNumericValue(new SecondTimeStamp(60L * 4), 1.0),
        new MutableNumericValue(new SecondTimeStamp(60L * 5), 1.0),
        new MutableNumericValue(new SecondTimeStamp(60L * 6), Double.NaN),
        new MutableNumericValue(new SecondTimeStamp(60L * 7), 1.0),
        new MutableNumericValue(new SecondTimeStamp(60L * 8), 1.0));
    
    SlidingWindowNumericArrayIterator iterator = new SlidingWindowNumericArrayIterator(
        node, result, Lists.newArrayList(source));
    
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertEquals(0, value.timestamp().epoch());
    assertArrayEquals(new double[] { Double.NaN, 1, 2, 2, 3, 4, 3, 3, 4 },
        value.value().doubleArray(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nansInfectious() throws Exception {
    config = (SlidingWindowConfig) SlidingWindowConfig.newBuilder()
        .setAggregator("sum")
        .setWindowSize("5m")
        .setInfectiousNan(true)
        .setId("win")
        .build();
    when(node.config()).thenReturn(config);
    
    setSource(new MutableNumericValue(new SecondTimeStamp(0L), Double.NaN),
        new MutableNumericValue(new SecondTimeStamp(60L), 1.0),
        new MutableNumericValue(new SecondTimeStamp(60L * 2), 1.0),
        new MutableNumericValue(new SecondTimeStamp(60L * 3), Double.NaN),
        new MutableNumericValue(new SecondTimeStamp(60L * 4), 1.0),
        new MutableNumericValue(new SecondTimeStamp(60L * 5), 1.0),
        new MutableNumericValue(new SecondTimeStamp(60L * 6), 1.0),
        new MutableNumericValue(new SecondTimeStamp(60L * 7), 1.0),
        new MutableNumericValue(new SecondTimeStamp(60L * 8), 1.0));
    
    SlidingWindowNumericArrayIterator iterator = new SlidingWindowNumericArrayIterator(
        node, result, Lists.newArrayList(source));
    
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertEquals(0, value.timestamp().epoch());
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, Double.NaN, 
        Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, 5 },
        value.value().doubleArray(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void allNan() throws Exception {
    setSource(new MutableNumericValue(new SecondTimeStamp(0L), Double.NaN),
        new MutableNumericValue(new SecondTimeStamp(60L), Double.NaN),
        new MutableNumericValue(new SecondTimeStamp(60L * 2), Double.NaN),
        new MutableNumericValue(new SecondTimeStamp(60L * 3), Double.NaN),
        new MutableNumericValue(new SecondTimeStamp(60L * 4), Double.NaN),
        new MutableNumericValue(new SecondTimeStamp(60L * 5), Double.NaN),
        new MutableNumericValue(new SecondTimeStamp(60L * 6), Double.NaN),
        new MutableNumericValue(new SecondTimeStamp(60L * 7), Double.NaN),
        new MutableNumericValue(new SecondTimeStamp(60L * 8), Double.NaN));
    SlidingWindowNumericArrayIterator iterator = new SlidingWindowNumericArrayIterator(
        node, result, Lists.newArrayList(source));
    
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertEquals(0, value.timestamp().epoch());
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, Double.NaN, 
        Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN },
        value.value().doubleArray(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  void setSource(final MutableNumericValue ...values) {
    source = new NumericArrayTimeSeries(id, new SecondTimeStamp(0));
    for (final MutableNumericValue value : values) {
      if (value.isInteger()) {
        ((NumericArrayTimeSeries) source).add(value.longValue());
      } else {
        ((NumericArrayTimeSeries) source).add(value.doubleValue());
      }
    }
  }
}
