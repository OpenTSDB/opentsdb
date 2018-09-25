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
package net.opentsdb.query.serdes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Iterator;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.collect.Lists;

import net.opentsdb.core.DefaultRegistry;
import net.opentsdb.core.MockTSDB;
import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.MockTimeSeries;
import net.opentsdb.data.PBufQueryResult;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.types.numeric.MutableNumericSummaryValue;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.exceptions.SerdesException;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.TimeSeriesQuery;
import net.opentsdb.query.execution.serdes.BaseSerdesOptions;
import net.opentsdb.query.pojo.Metric;
import net.opentsdb.query.pojo.Timespan;
import net.opentsdb.utils.UnitTestException;

public class TestPBufSerdes {

  private static MockTSDB TSDB;
  
  private PBufSerdesFactory factory;
  private QueryContext context;
  private SerdesOptions options;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    TSDB = new MockTSDB();
    TSDB.registry = new DefaultRegistry(TSDB);
    ((DefaultRegistry) TSDB.registry).initialize(true);
  }
  
  @Before
  public void before() throws Exception {
    factory = new PBufSerdesFactory();
    context = mock(QueryContext.class);
    options = mock(SerdesOptions.class);
  }
  
  @Test
  public void serdes() throws Exception {
    options = BaseSerdesOptions.newBuilder()
        .setStart(new MillisecondTimeStamp(1525824000000L))
        .setEnd(new MillisecondTimeStamp(1525827600000L))
        .setId("pbuf")
        .build();
    
    TimeSeriesQuery q = net.opentsdb.query.pojo.TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("1525824000")
            .setEnd("1525827600")
            .setAggregator("sum")
            .build())
        .addMetric(Metric.newBuilder()
            .setId("m1")
            .setMetric("sys.cpu.user"))
        .build().convert().build();
    
    when(context.query()).thenReturn(q);
    
    MockTimeSeries ts = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("metric.foo")
        .addTags("host", "web01")
        .build());
    
    MutableNumericValue v = new MutableNumericValue();
    v.reset(new MillisecondTimeStamp(1525824000000L), 42);
    ts.addValue(v);
    
    v = new MutableNumericValue();
    v.reset(new MillisecondTimeStamp(1525824060000L), 25.6);
    ts.addValue(v);
    
    MutableNumericSummaryValue sv = new MutableNumericSummaryValue();
    sv.resetTimestamp(new MillisecondTimeStamp(1525824000000L));
    sv.resetValue(0, 42.5);
    sv.resetValue(2, 4);
    ts.addValue(sv);
    
    sv = new MutableNumericSummaryValue();
    sv.resetTimestamp(new MillisecondTimeStamp(1525824060000L));
    sv.resetValue(0, 8);
    sv.resetValue(2, 1);
    ts.addValue(sv);
    
    // second series
    MockTimeSeries ts2 = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("metric.bar")
        .addTags("host", "web02")
        .addTags("dc", "phx")
        .build());
    
    v = new MutableNumericValue();
    v.reset(new MillisecondTimeStamp(1525824000000L), 128);
    ts2.addValue(v);
    
    v = new MutableNumericValue();
    v.reset(new MillisecondTimeStamp(1525824060000L), 888.997968);
    ts2.addValue(v);
    
    sv = new MutableNumericSummaryValue();
    sv.resetTimestamp(new MillisecondTimeStamp(1525824000000L));
    sv.resetValue(0, -5);
    sv.resetValue(2, 16);
    ts2.addValue(sv);
    
    sv = new MutableNumericSummaryValue();
    sv.resetTimestamp(new MillisecondTimeStamp(1525824120000L));
    sv.resetValue(0, 24.75);
    sv.resetValue(2, 9);
    ts2.addValue(sv);
    
    net.opentsdb.data.TimeSpecification spec = 
        mock(net.opentsdb.data.TimeSpecification.class);
    when(spec.start()).thenReturn(new SecondTimeStamp(1514764800));
    when(spec.end()).thenReturn(new SecondTimeStamp(1514768400));
    when(spec.stringInterval()).thenReturn("1m");
    when(spec.timezone()).thenReturn(ZoneId.of("America/Denver"));
    
    QueryResult result = mock(QueryResult.class);
    when(result.dataSource()).thenReturn("UT");
    when(result.resolution()).thenReturn(ChronoUnit.SECONDS);
    when(result.timeSeries()).thenReturn(Lists.newArrayList(ts, ts2));
    when(result.timeSpecification()).thenReturn(spec);
    
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PBufSerdes serdes = (PBufSerdes) factory.newInstance(context, options, baos);
    serdes.serialize(result, null);
    
    final byte[] serialized = baos.toByteArray();
    assertNotNull(serialized);
    
    // now deserialize
    final ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
    QueryNode node = mock(QueryNode.class);
    QueryPipelineContext ctx = mock(QueryPipelineContext.class);
    when(node.pipelineContext()).thenReturn(ctx);
    when(ctx.tsdb()).thenReturn(TSDB);
    final QueryResult[] results = new QueryResult[1];
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        results[0] = (QueryResult) invocation.getArguments()[0];
        return null;
      }
    }).when(node).onNext(any(QueryResult.class));
    
    serdes = (PBufSerdes) factory.newInstance(context, options, bais);
    serdes.deserialize(node, null);
    assertNotNull(results[0]);
    assertEquals(1514764800, results[0].timeSpecification().start().epoch());
    assertEquals(1514768400, results[0].timeSpecification().end().epoch());
    assertEquals("1m", results[0].timeSpecification().stringInterval());
    assertEquals(ZoneId.of("America/Denver"), results[0].timeSpecification().timezone());
    
    for (final TimeSeries series : results[0].timeSeries()) {
      if (((TimeSeriesStringId) series.id()).metric().equals("metric.foo")) {
        validateFoo(series);
      } else {
        validateBar(series);
      }
    }
  }
  
  @Test
  public void serdesEmpty() throws Exception {
    SerdesOptions options = BaseSerdesOptions.newBuilder()
        .setStart(new MillisecondTimeStamp(1525824000000L))
        .setEnd(new MillisecondTimeStamp(1525827600000L))
        .setId("pbuf")
        .build();
    
    TimeSeriesQuery q = net.opentsdb.query.pojo.TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("1525824000")
            .setEnd("1525827600")
            .setAggregator("sum")
            .build())
        .addMetric(Metric.newBuilder()
            .setId("m1")
            .setMetric("sys.cpu.user"))
        .build().convert().build();
    
    QueryResult result = mock(QueryResult.class);
    when(result.dataSource()).thenReturn("UT");
    when(result.resolution()).thenReturn(ChronoUnit.SECONDS);
    when(result.timeSeries()).thenReturn(Collections.emptyList());
    
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PBufSerdes serdes = (PBufSerdes) factory.newInstance(context, options, baos);
    serdes.serialize(result, null);
    
    final byte[] serialized = baos.toByteArray();
    assertNotNull(serialized);
    
    // now deserialize
    final ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
    QueryNode node = mock(QueryNode.class);
    final boolean[] validated = new boolean[1];
    doAnswer(new Answer<Void>() {

      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        final QueryResult parsed = (QueryResult) invocation.getArguments()[0];
        assertTrue(parsed.timeSeries().isEmpty());
        validated[0] = true;
        return null;
      }
      
    }).when(node).onNext(any(QueryResult.class));
    
    serdes = (PBufSerdes) factory.newInstance(context, options, bais);
    serdes.deserialize(node, null);
    assertTrue(validated[0]);
  }

  @Test
  public void serdesErrors() throws Exception {
    SerdesOptions options = BaseSerdesOptions.newBuilder()
        .setStart(new MillisecondTimeStamp(1525824000000L))
        .setEnd(new MillisecondTimeStamp(1525827600000L))
        .setId("pbuf")
        .build();

    QueryResult result = mock(QueryResult.class);
    when(result.dataSource()).thenReturn("UT");
    when(result.resolution()).thenReturn(ChronoUnit.SECONDS);
    when(result.timeSeries()).thenReturn(Collections.emptyList());
    
    try {
      factory.newInstance(null, options, mock(ByteArrayOutputStream.class));
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      factory.newInstance(context, null, mock(ByteArrayOutputStream.class));
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      factory.newInstance(context, options, (InputStream) null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    MockTimeSeries ts = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("metric.foo")
        .addTags("host", "web01")
        .build());
    when(result.timeSeries()).thenReturn(Lists.newArrayList(ts));
    
    ByteArrayOutputStream baos = mock(ByteArrayOutputStream.class);
    doThrow(new UnitTestException()).when(baos).write(any(byte[].class), 
        anyInt(), anyInt());
    PBufSerdes serdes = (PBufSerdes) factory.newInstance(context, options, baos);
    try {
      serdes.serialize(result, null);
      fail("Expected SerdesException");
    } catch (SerdesException e) { }
    
    // empty returns an empty result.
    QueryNode node = mock(QueryNode.class);
    ByteArrayInputStream bais = new ByteArrayInputStream(new byte[0]);
    serdes = (PBufSerdes) factory.newInstance(context, options, bais);
    serdes.deserialize(node, null);
    verify(node, times(1)).onNext(any(PBufQueryResult.class));
    
    // corrupt.
    node = mock(QueryNode.class);
    bais = new ByteArrayInputStream(new byte[] { 42, 0, 1, 1, 4 });
    serdes = (PBufSerdes) factory.newInstance(context, options, bais);
    serdes.deserialize(node, null);
    verify(node, times(1)).onError(any(Throwable.class));
  }
  
  void validateFoo(final TimeSeries series) {
    assertEquals("metric.foo", ((TimeSeriesStringId) series.id()).metric());
    assertEquals("web01", ((TimeSeriesStringId) series.id()).tags().get("host"));
    
    Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> it = 
        series.iterator(NumericType.TYPE).get();
    assertTrue(it.hasNext());
    
    TimeSeriesValue<NumericType> val = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(1525824000000L, val.timestamp().msEpoch());
    assertEquals(42, val.value().longValue());
    
    val = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(1525824060000L, val.timestamp().msEpoch());
    assertEquals(25.6, val.value().doubleValue(), 0.001);
    
    assertFalse(it.hasNext());
    
    it = series.iterator(NumericSummaryType.TYPE).get();
    assertTrue(it.hasNext());
    
    TimeSeriesValue<NumericSummaryType> sv = 
        (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(1525824000000L, sv.timestamp().msEpoch());
    assertEquals(42.5, sv.value().value(0).doubleValue(), 0.001);
    assertEquals(4, sv.value().value(2).longValue());
    
    sv = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(1525824060000L, sv.timestamp().msEpoch());
    assertEquals(8, sv.value().value(0).longValue());
    assertEquals(1, sv.value().value(2).longValue());
    
    assertFalse(it.hasNext());
  }
  
  void validateBar(final TimeSeries series) {
    assertEquals("metric.bar", ((TimeSeriesStringId) series.id()).metric());
    assertEquals("web02", ((TimeSeriesStringId) series.id()).tags().get("host"));
    assertEquals("phx", ((TimeSeriesStringId) series.id()).tags().get("dc"));
    
    Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> it = 
        series.iterator(NumericType.TYPE).get();
    assertTrue(it.hasNext());
    
    TimeSeriesValue<NumericType> val = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(1525824000000L, val.timestamp().msEpoch());
    assertEquals(128, val.value().longValue());
    
    val = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(1525824060000L, val.timestamp().msEpoch());
    assertEquals(888.997968, val.value().doubleValue(), 0.001);
    
    assertFalse(it.hasNext());
    
    it = series.iterator(NumericSummaryType.TYPE).get();
    assertTrue(it.hasNext());
    
    TimeSeriesValue<NumericSummaryType> sv = 
        (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(1525824000000L, sv.timestamp().msEpoch());
    assertEquals(-5, sv.value().value(0).longValue());
    assertEquals(16, sv.value().value(2).longValue());
    
    sv = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(1525824120000L, sv.timestamp().msEpoch());
    assertEquals(24.75, sv.value().value(0).doubleValue(), 0.001);
    assertEquals(9, sv.value().value(2).longValue());
    
    assertFalse(it.hasNext());
  }
  
}
