// This file is part of OpenTSDB.
// Copyright (C)2019  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.summarizer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;

import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.MockTimeSeries;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeSpecification;
import org.junit.Before;
import org.junit.Test;

import net.opentsdb.data.types.numeric.MutableNumericSummaryValue;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.query.DefaultTimeSeriesDataSourceConfig;
import net.opentsdb.query.QueryMode;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.filter.MetricLiteralFilter;

public class TestSummarizerPassThroughNumericSummaryIterator {
private static final int BASE_TIME = 1546300800;
  
  private SemanticQuery query;
  private QueryPipelineContext context;
  private SummarizerPassThroughResult result;
  
  @Before
  public void before() throws Exception {
    context = mock(QueryPipelineContext.class);
    query = SemanticQuery.newBuilder()
        .setStart(Integer.toString(BASE_TIME))
        .setEnd(Integer.toString(BASE_TIME + (3600 * 4)))
        .setMode(QueryMode.SINGLE)
        .addExecutionGraphNode(DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setId("m1")
            .build())
        .addExecutionGraphNode(SummarizerConfig.newBuilder()
            .addSummary("sum")
            .addSource("m1")
            .setId("summarizer")
            .build())
        .build();
    when(context.query()).thenReturn(query);
    result = mock(SummarizerPassThroughResult.class);
    Summarizer node = mock(Summarizer.class);
    when(node.pipelineContext()).thenReturn(context);
    when(result.summarizerNode()).thenReturn(node);
    
    TimeSpecification time_spec = mock(TimeSpecification.class);
    when(time_spec.start()).thenReturn(new SecondTimeStamp(BASE_TIME));
    when(time_spec.interval()).thenReturn(Duration.ofSeconds(60));
    when(result.timeSpecification()).thenReturn(time_spec);
  }
  
  @Test
  public void longs() throws Exception {
    TimeSeries series = new MockTimeSeries(new BaseTimeSeriesStringId.Builder()
        .setMetric("foo")
        .build());
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
    v.resetTimestamp(new SecondTimeStamp(BASE_TIME));
    v.resetValue(1, -3);
    ((MockTimeSeries) series).addValue(v);

    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new SecondTimeStamp(BASE_TIME + (3600 * 1L)));
    v.resetValue(1, 15);
    ((MockTimeSeries) series).addValue(v);

    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new SecondTimeStamp(BASE_TIME + (3600 * 2L)));
    v.resetValue(1, 12);
    ((MockTimeSeries) series).addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new SecondTimeStamp(BASE_TIME + (3600 * 3L)));
    v.resetValue(0, 12);
    ((MockTimeSeries) series).addValue(v);
    SummarizedTimeSeries sts = spy(new SummarizedTimeSeries(result, series));
    SummarizerPassThroughNumericSummaryIterator iterator = new
        SummarizerPassThroughNumericSummaryIterator(sts);
    
    assertTrue(iterator.hasNext());
    
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(BASE_TIME, value.timestamp().epoch());
    assertEquals(-3, value.value().value(1).longValue());
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(BASE_TIME + (3600), value.timestamp().epoch());
    assertEquals(15, value.value().value(1).longValue());
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(BASE_TIME + (3600 * 2), value.timestamp().epoch());
    assertEquals(12, value.value().value(1).longValue());
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(BASE_TIME + (3600 * 3), value.timestamp().epoch());
    assertEquals(12, value.value().value(0).longValue());
    
    verify(sts, never()).summarize(any(long[].class), anyInt(), anyInt());
    assertFalse(iterator.hasNext());
    
    verify(sts, times(1)).summarize(eq(new long[] { -3, 15, 12, 0, 0, 0, 0, 0 }), 
        eq(0), eq(3));
  }
  
  @Test
  public void doubles() throws Exception {
    TimeSeries series = new MockTimeSeries(new BaseTimeSeriesStringId.Builder()
        .setMetric("foo")
        .build());
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
    v.resetTimestamp(new SecondTimeStamp(BASE_TIME));
    v.resetValue(1, -3.5);
    ((MockTimeSeries) series).addValue(v);

    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new SecondTimeStamp(BASE_TIME + (3600 * 1L)));
    v.resetValue(1, 15.75);
    ((MockTimeSeries) series).addValue(v);

    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new SecondTimeStamp(BASE_TIME + (3600 * 2L)));
    v.resetValue(1, 12.13);
    ((MockTimeSeries) series).addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new SecondTimeStamp(BASE_TIME + (3600 * 3L)));
    v.resetValue(0, 12.99);
    ((MockTimeSeries) series).addValue(v);
    SummarizedTimeSeries sts = spy(new SummarizedTimeSeries(result, series));
    SummarizerPassThroughNumericSummaryIterator iterator = new
        SummarizerPassThroughNumericSummaryIterator(sts);
    
    assertTrue(iterator.hasNext());
    
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(BASE_TIME, value.timestamp().epoch());
    assertEquals(-3.5, value.value().value(1).doubleValue(), 0.001);
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(BASE_TIME + (3600), value.timestamp().epoch());
    assertEquals(15.75, value.value().value(1).doubleValue(), 0.001);
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(BASE_TIME + (3600 * 2), value.timestamp().epoch());
    assertEquals(12.13, value.value().value(1).doubleValue(), 0.001);
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(BASE_TIME + (3600 * 3), value.timestamp().epoch());
    assertEquals(12.99, value.value().value(0).doubleValue(), 0.001);
    
    verify(sts, never()).summarize(any(double[].class), anyInt(), anyInt());
    assertFalse(iterator.hasNext());
    
    verify(sts, times(1)).summarize(eq(new double[] { -3.5, 15.75, 12.13, 0, 0, 0, 0, 0 }), 
        eq(0), eq(3));
  }
  
  @Test
  public void mixed() throws Exception {
    TimeSeries series = new MockTimeSeries(new BaseTimeSeriesStringId.Builder()
        .setMetric("foo")
        .build());
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
    v.resetTimestamp(new SecondTimeStamp(BASE_TIME));
    v.resetValue(1, -3.5);
    ((MockTimeSeries) series).addValue(v);

    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new SecondTimeStamp(BASE_TIME + (3600 * 1L)));
    v.resetValue(1, 15);
    ((MockTimeSeries) series).addValue(v);

    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new SecondTimeStamp(BASE_TIME + (3600 * 2L)));
    v.resetValue(1, 12.13);
    ((MockTimeSeries) series).addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new SecondTimeStamp(BASE_TIME + (3600 * 3L)));
    v.resetValue(0, 12.99);
    ((MockTimeSeries) series).addValue(v);
    SummarizedTimeSeries sts = spy(new SummarizedTimeSeries(result, series));
    SummarizerPassThroughNumericSummaryIterator iterator = new
        SummarizerPassThroughNumericSummaryIterator(sts);
    
    assertTrue(iterator.hasNext());
    
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(BASE_TIME, value.timestamp().epoch());
    assertEquals(-3.5, value.value().value(1).doubleValue(), 0.001);
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(BASE_TIME + (3600), value.timestamp().epoch());
    assertEquals(15, value.value().value(1).longValue());
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(BASE_TIME + (3600 * 2), value.timestamp().epoch());
    assertEquals(12.13, value.value().value(1).doubleValue(), 0.001);
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(BASE_TIME + (3600 * 3), value.timestamp().epoch());
    assertEquals(12.99, value.value().value(0).doubleValue(), 0.001);
    
    verify(sts, never()).summarize(any(double[].class), anyInt(), anyInt());
    assertFalse(iterator.hasNext());
    
    verify(sts, times(1)).summarize(eq(new double[] { -3.5, 15, 12.13, 0, 0, 0, 0, 0 }), 
        eq(0), eq(3));
  }
  
  @Test
  public void average() throws Exception {
    TimeSeries series = new MockTimeSeries(new BaseTimeSeriesStringId.Builder()
        .setMetric("foo")
        .build());
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
    v.resetTimestamp(new SecondTimeStamp(BASE_TIME));
    v.resetValue(0, 6);
    v.resetValue(1, 2);
    ((MockTimeSeries) series).addValue(v);

    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new SecondTimeStamp(BASE_TIME + (3600 * 1L)));
    v.resetValue(0, 4.8);
    v.resetValue(1, 1);
    ((MockTimeSeries) series).addValue(v);

    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new SecondTimeStamp(BASE_TIME + (3600 * 2L)));
    v.resetValue(0, 8);
    v.resetValue(1, 2);
    ((MockTimeSeries) series).addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new SecondTimeStamp(BASE_TIME + (3600 * 3L)));
    v.resetValue(0, 12);
    ((MockTimeSeries) series).addValue(v);
    SummarizedTimeSeries sts = spy(new SummarizedTimeSeries(result, series));
    SummarizerPassThroughNumericSummaryIterator iterator = new
        SummarizerPassThroughNumericSummaryIterator(sts);
    
    assertTrue(iterator.hasNext());
    
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(BASE_TIME, value.timestamp().epoch());
    assertEquals(6, value.value().value(0).longValue());
    assertEquals(2, value.value().value(1).longValue());
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(BASE_TIME + (3600), value.timestamp().epoch());
    assertEquals(4.8, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(1).longValue());
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(BASE_TIME + (3600 * 2), value.timestamp().epoch());
    assertEquals(8, value.value().value(0).longValue());
    assertEquals(2, value.value().value(1).longValue());
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(BASE_TIME + (3600 * 3), value.timestamp().epoch());
    assertEquals(12, value.value().value(0).longValue());
    
    verify(sts, never()).summarize(any(double[].class), anyInt(), anyInt());
    assertFalse(iterator.hasNext());
    
    verify(sts, times(1)).summarize(eq(new double[] { 3, 4.8, 4, 0, 0, 0, 0, 0 }), 
        eq(0), eq(3));
  }

  @Test
  public void filterMiddle() throws Exception {
    query = SemanticQuery.newBuilder()
        .setStart(Integer.toString(BASE_TIME + 3600))
        .setEnd(Integer.toString(BASE_TIME + (3600 * 3)))
        .setMode(QueryMode.SINGLE)
        .addExecutionGraphNode(DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setId("m1")
            .build())
        .addExecutionGraphNode(SummarizerConfig.newBuilder()
            .addSummary("sum")
            .addSource("m1")
            .setId("summarizer")
            .build())
        .build();
    when(context.query()).thenReturn(query);
    
    TimeSeries series = new MockTimeSeries(new BaseTimeSeriesStringId.Builder()
        .setMetric("foo")
        .build());
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
    v.resetTimestamp(new SecondTimeStamp(BASE_TIME));
    v.resetValue(1, -3);
    ((MockTimeSeries) series).addValue(v);

    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new SecondTimeStamp(BASE_TIME + (3600 * 1L)));
    v.resetValue(1, 15);
    ((MockTimeSeries) series).addValue(v);

    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new SecondTimeStamp(BASE_TIME + (3600 * 2L)));
    v.resetValue(1, 12);
    ((MockTimeSeries) series).addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new SecondTimeStamp(BASE_TIME + (3600 * 3L)));
    v.resetValue(0, 12);
    ((MockTimeSeries) series).addValue(v);
    SummarizedTimeSeries sts = spy(new SummarizedTimeSeries(result, series));
    SummarizerPassThroughNumericSummaryIterator iterator = new
        SummarizerPassThroughNumericSummaryIterator(sts);
    
    assertTrue(iterator.hasNext());
    
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(BASE_TIME, value.timestamp().epoch());
    assertEquals(-3, value.value().value(1).longValue());
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(BASE_TIME + (3600), value.timestamp().epoch());
    assertEquals(15, value.value().value(1).longValue());
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(BASE_TIME + (3600 * 2), value.timestamp().epoch());
    assertEquals(12, value.value().value(1).longValue());
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(BASE_TIME + (3600 * 3), value.timestamp().epoch());
    assertEquals(12, value.value().value(0).longValue());
    
    verify(sts, never()).summarize(any(long[].class), anyInt(), anyInt());
    assertFalse(iterator.hasNext());
    
    verify(sts, times(1)).summarize(eq(new long[] { 15, 12, 0, 0, 0, 0, 0, 0 }), 
        eq(0), eq(2));
  }
  
  @Test
  public void filterEarly() throws Exception {
    query = SemanticQuery.newBuilder()
        .setStart(Integer.toString(BASE_TIME + (3600 * 4)))
        .setEnd(Integer.toString(BASE_TIME + (3600 * 8)))
        .setMode(QueryMode.SINGLE)
        .addExecutionGraphNode(DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setId("m1")
            .build())
        .addExecutionGraphNode(SummarizerConfig.newBuilder()
            .addSummary("sum")
            .addSource("m1")
            .setId("summarizer")
            .build())
        .build();
    when(context.query()).thenReturn(query);
    
    TimeSeries series = new MockTimeSeries(new BaseTimeSeriesStringId.Builder()
        .setMetric("foo")
        .build());
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
    v.resetTimestamp(new SecondTimeStamp(BASE_TIME));
    v.resetValue(1, -3);
    ((MockTimeSeries) series).addValue(v);

    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new SecondTimeStamp(BASE_TIME + (3600 * 1L)));
    v.resetValue(1, 15);
    ((MockTimeSeries) series).addValue(v);

    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new SecondTimeStamp(BASE_TIME + (3600 * 2L)));
    v.resetValue(1, 12);
    ((MockTimeSeries) series).addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new SecondTimeStamp(BASE_TIME + (3600 * 3L)));
    v.resetValue(0, 12);
    ((MockTimeSeries) series).addValue(v);
    SummarizedTimeSeries sts = spy(new SummarizedTimeSeries(result, series));
    SummarizerPassThroughNumericSummaryIterator iterator = new
        SummarizerPassThroughNumericSummaryIterator(sts);
    
    assertTrue(iterator.hasNext());
    
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(BASE_TIME, value.timestamp().epoch());
    assertEquals(-3, value.value().value(1).longValue());
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(BASE_TIME + (3600), value.timestamp().epoch());
    assertEquals(15, value.value().value(1).longValue());
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(BASE_TIME + (3600 * 2), value.timestamp().epoch());
    assertEquals(12, value.value().value(1).longValue());
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(BASE_TIME + (3600 * 3), value.timestamp().epoch());
    assertEquals(12, value.value().value(0).longValue());
    
    verify(sts, never()).summarize(any(long[].class), anyInt(), anyInt());
    assertFalse(iterator.hasNext());
    
    verify(sts, never()).summarize(any(long[].class), anyInt(), anyInt());
  }
  
  @Test
  public void filterLate() throws Exception {
    query = SemanticQuery.newBuilder()
        .setStart(Integer.toString(BASE_TIME - (3600 * 8)))
        .setEnd(Integer.toString(BASE_TIME - 3600))
        .setMode(QueryMode.SINGLE)
        .addExecutionGraphNode(DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setId("m1")
            .build())
        .addExecutionGraphNode(SummarizerConfig.newBuilder()
            .addSummary("sum")
            .addSource("m1")
            .setId("summarizer")
            .build())
        .build();
    when(context.query()).thenReturn(query);
    
    TimeSeries series = new MockTimeSeries(new BaseTimeSeriesStringId.Builder()
        .setMetric("foo")
        .build());
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
    v.resetTimestamp(new SecondTimeStamp(BASE_TIME));
    v.resetValue(1, -3);
    ((MockTimeSeries) series).addValue(v);

    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new SecondTimeStamp(BASE_TIME + (3600 * 1L)));
    v.resetValue(1, 15);
    ((MockTimeSeries) series).addValue(v);

    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new SecondTimeStamp(BASE_TIME + (3600 * 2L)));
    v.resetValue(1, 12);
    ((MockTimeSeries) series).addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new SecondTimeStamp(BASE_TIME + (3600 * 3L)));
    v.resetValue(0, 12);
    ((MockTimeSeries) series).addValue(v);
    SummarizedTimeSeries sts = spy(new SummarizedTimeSeries(result, series));
    SummarizerPassThroughNumericSummaryIterator iterator = new
        SummarizerPassThroughNumericSummaryIterator(sts);
    
    assertTrue(iterator.hasNext());
    
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(BASE_TIME, value.timestamp().epoch());
    assertEquals(-3, value.value().value(1).longValue());
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(BASE_TIME + (3600), value.timestamp().epoch());
    assertEquals(15, value.value().value(1).longValue());
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(BASE_TIME + (3600 * 2), value.timestamp().epoch());
    assertEquals(12, value.value().value(1).longValue());
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(BASE_TIME + (3600 * 3), value.timestamp().epoch());
    assertEquals(12, value.value().value(0).longValue());
    
    verify(sts, never()).summarize(any(long[].class), anyInt(), anyInt());
    assertFalse(iterator.hasNext());
    
    verify(sts, never()).summarize(any(long[].class), anyInt(), anyInt());
  }
  
}
