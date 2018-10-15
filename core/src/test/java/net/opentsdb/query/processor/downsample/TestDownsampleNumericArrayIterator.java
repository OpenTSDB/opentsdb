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
package net.opentsdb.query.processor.downsample;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import net.opentsdb.core.DefaultRegistry;
import net.opentsdb.core.MockTSDB;
import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.types.numeric.NumericArrayTimeSeries;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.processor.downsample.Downsample.DownsampleResult;

public class TestDownsampleNumericArrayIterator {
  public static MockTSDB TSDB;
  
  private NumericInterpolatorConfig numeric_config;
  private TimeSeries source;
  private DownsampleConfig config;
  private QueryNode node;
  private DownsampleResult result;
  private QueryResult downstream;
  private TimeSpecification time_spec;
  private TimeStamp start;
  
  @BeforeClass
  public static void beforeClass() {
    TSDB = new MockTSDB();
    TSDB.registry = new DefaultRegistry(TSDB);
    ((DefaultRegistry) TSDB.registry).initialize(true);
  }
  
  @Before
  public void before() throws Exception {
    node = mock(QueryNode.class);
    result = mock(DownsampleResult.class);
    downstream = mock(QueryResult.class);
    time_spec = mock(TimeSpecification.class);
    start = mock(TimeStamp.class);
    QueryPipelineContext context = mock(QueryPipelineContext.class);
    when(context.tsdb()).thenReturn(TSDB);
    when(node.pipelineContext()).thenReturn(context);
    
    when(downstream.timeSpecification()).thenReturn(time_spec);
    when(result.downstreamResult()).thenReturn(downstream);
    when(result.start()).thenReturn(start);
    
    numeric_config = (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(FillPolicy.NONE)
        .setRealFillPolicy(FillWithRealPolicy.NONE)
        .setDataType(NumericType.TYPE.toString())
        .build();
  }
  
  @Test
  public void ctor() throws Exception {
    source = new NumericArrayTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build(), new MillisecondTimeStamp(1000));
    ((NumericArrayTimeSeries) source).add(1);
    ((NumericArrayTimeSeries) source).add(5);
    ((NumericArrayTimeSeries) source).add(2);
    ((NumericArrayTimeSeries) source).add(1);
    
    config = (DownsampleConfig) DownsampleConfig.newBuilder()
        .setAggregator("avg")
        .setId("foo")
        .setInterval("2m")
        .setStart("1514764800")
        .setEnd("1514765040")
        .addInterpolatorConfig(numeric_config)
        .build();
    when(node.config()).thenReturn(config);
    
    DownsampleNumericArrayIterator iterator = 
        new DownsampleNumericArrayIterator(node, result, source);
    assertEquals(2, iterator.intervals);
    assertTrue(iterator.hasNext());
    
    // empty
    source = new NumericArrayTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build(), new MillisecondTimeStamp(1000));
    iterator = new DownsampleNumericArrayIterator(node, result, source);
    assertEquals(2, iterator.intervals);
    assertFalse(iterator.hasNext());
    
    try {
      new DownsampleNumericArrayIterator(null, result, source);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new DownsampleNumericArrayIterator(node, null, source);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new DownsampleNumericArrayIterator(node, result, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void iterateLongSameSameRangeFull() throws Exception {
    when(time_spec.start()).thenReturn(new SecondTimeStamp(1514764800));
    when(time_spec.interval()).thenReturn(Duration.ofSeconds(60));
    
    source = new NumericArrayTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build(), new MillisecondTimeStamp(1000));
    ((NumericArrayTimeSeries) source).add(1);
    ((NumericArrayTimeSeries) source).add(5);
    ((NumericArrayTimeSeries) source).add(2);
    ((NumericArrayTimeSeries) source).add(1);
    
    config = (DownsampleConfig) DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setId("foo")
        .setInterval("1m")
        .setStart("1514764800")
        .setEnd("1514765040")
        .addInterpolatorConfig(numeric_config)
        .build();
    when(node.config()).thenReturn(config);
    
    DownsampleNumericArrayIterator iterator = 
        new DownsampleNumericArrayIterator(node, result, source);
    assertEquals(4, iterator.intervals);
    assertTrue(iterator.hasNext());
    
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertSame(start, value.timestamp());
    assertTrue(value.value().isInteger());
    assertEquals(8, value.value().longArray().length);
    assertEquals(1, value.value().longArray()[0]);
    assertEquals(5, value.value().longArray()[1]);
    assertEquals(2, value.value().longArray()[2]);
    assertEquals(1, value.value().longArray()[3]);
    assertEquals(0, value.value().offset());
    assertEquals(4, value.value().end());
  }
  
  @Test
  public void iterateLongLowToHighSameRangeFull() throws Exception {
    when(time_spec.start()).thenReturn(new SecondTimeStamp(1514764800));
    when(time_spec.interval()).thenReturn(Duration.ofSeconds(60));
    
    source = new NumericArrayTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build(), new MillisecondTimeStamp(1000));
    ((NumericArrayTimeSeries) source).add(1);
    ((NumericArrayTimeSeries) source).add(5);
    ((NumericArrayTimeSeries) source).add(2);
    ((NumericArrayTimeSeries) source).add(1);
    
    config = (DownsampleConfig) DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setId("foo")
        .setInterval("2m")
        .setStart("1514764800")
        .setEnd("1514765040")
        .addInterpolatorConfig(numeric_config)
        .build();
    when(node.config()).thenReturn(config);
    
    DownsampleNumericArrayIterator iterator = 
        new DownsampleNumericArrayIterator(node, result, source);
    assertEquals(2, iterator.intervals);
    assertTrue(iterator.hasNext());
    
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertSame(start, value.timestamp());
    assertTrue(value.value().isInteger());
    assertEquals(2, value.value().longArray().length);
    assertEquals(6, value.value().longArray()[0]);
    assertEquals(3, value.value().longArray()[1]);
    assertEquals(0, value.value().offset());
    assertEquals(2, value.value().end());
  }
  
  @Test
  public void iterateLongHighToLowSameRangeFull() throws Exception {
    when(time_spec.start()).thenReturn(new SecondTimeStamp(1514764800));
    when(time_spec.interval()).thenReturn(Duration.ofSeconds(60));
    
    source = new NumericArrayTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build(), new MillisecondTimeStamp(1000));
    ((NumericArrayTimeSeries) source).add(1);
    ((NumericArrayTimeSeries) source).add(5);
    ((NumericArrayTimeSeries) source).add(2);
    ((NumericArrayTimeSeries) source).add(1);
    
    config = (DownsampleConfig) DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setId("foo")
        .setInterval("30s")
        .setStart("1514764800")
        .setEnd("1514765040")
        .addInterpolatorConfig(numeric_config)
        .build();
    when(node.config()).thenReturn(config);
    
    DownsampleNumericArrayIterator iterator = 
        new DownsampleNumericArrayIterator(node, result, source);
    assertEquals(8, iterator.intervals);
    assertTrue(iterator.hasNext());
    
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertSame(start, value.timestamp());
    assertFalse(value.value().isInteger());
    assertEquals(8, value.value().doubleArray().length);
    assertEquals(1, value.value().doubleArray()[0], 0.001);
    assertTrue(Double.isNaN(value.value().doubleArray()[1]));
    assertEquals(5, value.value().doubleArray()[2], 0.001);
    assertTrue(Double.isNaN(value.value().doubleArray()[3]));
    assertEquals(2, value.value().doubleArray()[4], 0.001);
    assertTrue(Double.isNaN(value.value().doubleArray()[5]));
    assertEquals(1, value.value().doubleArray()[6], 0.001);
    assertTrue(Double.isNaN(value.value().doubleArray()[7]));
    assertEquals(0, value.value().offset());
    assertEquals(8, value.value().end());
  }
  
  @Test
  public void iterateDoubleSameSameRangeFull() throws Exception {
    when(time_spec.start()).thenReturn(new SecondTimeStamp(1514764800));
    when(time_spec.interval()).thenReturn(Duration.ofSeconds(60));
    
    source = new NumericArrayTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build(), new MillisecondTimeStamp(1000));
    ((NumericArrayTimeSeries) source).add(1.5);
    ((NumericArrayTimeSeries) source).add(5.75);
    ((NumericArrayTimeSeries) source).add(2.3);
    ((NumericArrayTimeSeries) source).add(1.0);
    
    config = (DownsampleConfig) DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setId("foo")
        .setInterval("1m")
        .setStart("1514764800")
        .setEnd("1514765040")
        .addInterpolatorConfig(numeric_config)
        .build();
    when(node.config()).thenReturn(config);
    
    DownsampleNumericArrayIterator iterator = 
        new DownsampleNumericArrayIterator(node, result, source);
    assertEquals(4, iterator.intervals);
    assertTrue(iterator.hasNext());
    
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertSame(start, value.timestamp());
    assertFalse(value.value().isInteger());
    assertEquals(8, value.value().doubleArray().length);
    assertEquals(1.5, value.value().doubleArray()[0], 0.001);
    assertEquals(5.75, value.value().doubleArray()[1], 0.001);
    assertEquals(2.3, value.value().doubleArray()[2], 0.001);
    assertEquals(1.0, value.value().doubleArray()[3], 0.001);
    assertEquals(0, value.value().offset());
    assertEquals(4, value.value().end());
  }
  
  @Test
  public void iterateDoubleLowToHighSameRangeFull() throws Exception {
    when(time_spec.start()).thenReturn(new SecondTimeStamp(1514764800));
    when(time_spec.interval()).thenReturn(Duration.ofSeconds(60));
    
    source = new NumericArrayTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build(), new MillisecondTimeStamp(1000));
    ((NumericArrayTimeSeries) source).add(1.5);
    ((NumericArrayTimeSeries) source).add(5.75);
    ((NumericArrayTimeSeries) source).add(2.3);
    ((NumericArrayTimeSeries) source).add(1.0);
    
    config = (DownsampleConfig) DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setId("foo")
        .setInterval("2m")
        .setStart("1514764800")
        .setEnd("1514765040")
        .addInterpolatorConfig(numeric_config)
        .build();
    when(node.config()).thenReturn(config);
    
    DownsampleNumericArrayIterator iterator = 
        new DownsampleNumericArrayIterator(node, result, source);
    assertEquals(2, iterator.intervals);
    assertTrue(iterator.hasNext());
    
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertSame(start, value.timestamp());
    assertFalse(value.value().isInteger());
    assertEquals(2, value.value().doubleArray().length);
    assertEquals(7.25, value.value().doubleArray()[0], 0.001);
    assertEquals(3.3, value.value().doubleArray()[1], 0.001);
    assertEquals(0, value.value().offset());
    assertEquals(2, value.value().end());
  }
  
  @Test
  public void iterateDoubleHighToLowSameRangeFull() throws Exception {
    when(time_spec.start()).thenReturn(new SecondTimeStamp(1514764800));
    when(time_spec.interval()).thenReturn(Duration.ofSeconds(60));
    
    source = new NumericArrayTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build(), new MillisecondTimeStamp(1000));
    ((NumericArrayTimeSeries) source).add(1.5);
    ((NumericArrayTimeSeries) source).add(5.75);
    ((NumericArrayTimeSeries) source).add(2.3);
    ((NumericArrayTimeSeries) source).add(1.0);
    
    config = (DownsampleConfig) DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setId("foo")
        .setInterval("30s")
        .setStart("1514764800")
        .setEnd("1514765040")
        .addInterpolatorConfig(numeric_config)
        .build();
    when(node.config()).thenReturn(config);
    
    DownsampleNumericArrayIterator iterator = 
        new DownsampleNumericArrayIterator(node, result, source);
    assertEquals(8, iterator.intervals);
    assertTrue(iterator.hasNext());
    
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertSame(start, value.timestamp());
    assertFalse(value.value().isInteger());
    assertEquals(8, value.value().doubleArray().length);
    assertEquals(1.5, value.value().doubleArray()[0], 0.001);
    assertTrue(Double.isNaN(value.value().doubleArray()[1]));
    assertEquals(5.75, value.value().doubleArray()[2], 0.001);
    assertTrue(Double.isNaN(value.value().doubleArray()[3]));
    assertEquals(2.3, value.value().doubleArray()[4], 0.001);
    assertTrue(Double.isNaN(value.value().doubleArray()[5]));
    assertEquals(1.0, value.value().doubleArray()[6], 0.001);
    assertTrue(Double.isNaN(value.value().doubleArray()[7]));
    assertEquals(0, value.value().offset());
    assertEquals(8, value.value().end());
  }

  @Test
  public void noData() throws Exception {
    when(time_spec.start()).thenReturn(new SecondTimeStamp(1514764800));
    when(time_spec.interval()).thenReturn(Duration.ofSeconds(60));
    
    source = new NumericArrayTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build(), new MillisecondTimeStamp(1000));
    
    config = (DownsampleConfig) DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setId("foo")
        .setInterval("1m")
        .setStart("1514764800")
        .setEnd("1514765040")
        .addInterpolatorConfig(numeric_config)
        .build();
    when(node.config()).thenReturn(config);
    
    DownsampleNumericArrayIterator iterator = 
        new DownsampleNumericArrayIterator(node, result, source);
    assertEquals(4, iterator.intervals);
    assertFalse(iterator.hasNext());
  }
}
