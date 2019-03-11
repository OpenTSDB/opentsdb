// This file is part of OpenTSDB.
// Copyright (C) 2017-2018  The OpenTSDB Authors.
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
package net.opentsdb.query.execution.serdes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.time.Duration;
import java.util.Collections;

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.common.Const;
import net.opentsdb.core.Registry;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.MockTimeSeries;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesDataSourceFactory;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.BaseTimeSeriesByteId;
import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.types.numeric.MutableNumericSummaryValue;
import net.opentsdb.data.types.numeric.NumericArrayTimeSeries;
import net.opentsdb.data.types.numeric.NumericMillisecondShard;
import net.opentsdb.data.types.numeric.aggregators.NumericAggregatorFactory;
import net.opentsdb.data.types.numeric.aggregators.SumFactory;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.TimeSeriesQuery;
import net.opentsdb.query.pojo.Metric;
import net.opentsdb.query.pojo.Timespan;
import net.opentsdb.query.serdes.SerdesOptions;
import net.opentsdb.utils.UnitTestException;

public class TestJsonV2QuerySerdes {

  private TSDB tsdb;
  private QueryContext context;
  private QueryResult result;
  private TimeSeriesQuery query;
  private TimeSeries ts1;
  private TimeSeriesId id1;
  private TimeSeries ts2;
  private TimeSeriesId id2;
  private TimeSeriesDataSourceFactory store;
  private JsonV2QuerySerdesOptions options;
  
  @Before
  public void before() throws Exception {
    tsdb = mock(TSDB.class);
    Registry registry = mock(Registry.class);
    when(tsdb.getRegistry()).thenReturn(registry);
    when(tsdb.getRegistry().getPlugin(eq(NumericAggregatorFactory.class), anyString()))
      .thenReturn(new SumFactory());
    
    context = mock(QueryContext.class);
    result = mock(QueryResult.class);
    store = mock(TimeSeriesDataSourceFactory.class);
    query = net.opentsdb.query.pojo.TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("1486045800")
            .setEnd("1486046000")
            .setAggregator("sum"))
        .addMetric(Metric.newBuilder()
            .setId("m1")
            .setMetric("sys.cpu.user"))
        .build()
        .convert(tsdb).build();
    when(context.query()).thenReturn(query);
    
    id1 = BaseTimeSeriesStringId.newBuilder()
        .setMetric("sys.cpu.user")
        .addTags("host", "web01")
        .addTags("dc", "phx")
        .addAggregatedTag("owner")
        .build();
    
    ts1 = new NumericMillisecondShard(id1, 
        new MillisecondTimeStamp(1486045800000L), 
        new MillisecondTimeStamp(1486046000000L));
    ((NumericMillisecondShard) ts1).add(1486045800000L, 1);
    ((NumericMillisecondShard) ts1).add(1486045860000L, 5.75);
    
    id2 = BaseTimeSeriesStringId.newBuilder()
        .setMetric("sys.cpu.user")
        .addTags("host", "web02")
        .addTags("dc", "phx")
        .addAggregatedTag("owner")
        .build();
    
    ts2 = new NumericMillisecondShard(id2,
        new MillisecondTimeStamp(1486045800000L), 
        new MillisecondTimeStamp(1486046000000L));
    ((NumericMillisecondShard) ts2).add(1486045800000L, 4);
    ((NumericMillisecondShard) ts2).add(1486045860000L, 0.0015);
    
    when(result.timeSeries()).thenReturn(Lists.newArrayList(ts1, ts2));
    
    options = (JsonV2QuerySerdesOptions) JsonV2QuerySerdesOptions.newBuilder()
        .setId("json")
        .build();
  }
  
  @Test
  public void serializeFull() throws Exception {
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    final JsonV2QuerySerdes serdes = new JsonV2QuerySerdes(context, options, output);
    serdes.serialize(result, null);

    String json = new String(output.toByteArray(), Const.UTF8_CHARSET);
    
    assertTrue(json.startsWith("["));
    assertFalse(json.endsWith("]"));
    assertTrue(json.contains("\"metric\":\"sys.cpu.user\""));
    assertTrue(json.contains("\"tags\":{"));
    assertTrue(json.contains("\"dc\":\"phx\""));
    assertTrue(json.contains("\"host\":\"web01\""));
    assertTrue(json.contains("\"host\":\"web02\""));
    assertTrue(json.contains("\"aggregateTags\":[\"owner\"]"));
    assertTrue(json.contains("\"dps\":{"));
    
    assertTrue(json.contains("\"1486045800\":1"));
    assertTrue(json.contains("\"1486045860\":5.75"));
    assertTrue(json.contains("\"1486045800\":4"));
    assertTrue(json.contains("\"1486045860\":0.0015"));
    
    // close it.
    serdes.serializeComplete(null);
    output.close();
    json = new String(output.toByteArray(), Const.UTF8_CHARSET);
    assertTrue(json.endsWith("]"));
  }
  
  @Test
  public void serializeWithMilliseconds() throws Exception {
    options = (JsonV2QuerySerdesOptions) JsonV2QuerySerdesOptions.newBuilder()
        .setMsResolution(true)
        .setId("json")
        .build();
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    final JsonV2QuerySerdes serdes = new JsonV2QuerySerdes(context, options, output);
    serdes.serialize( result, null);
    output.close();
    final String json = new String(output.toByteArray(), Const.UTF8_CHARSET);
    
    assertTrue(json.contains("\"metric\":\"sys.cpu.user\""));
    assertTrue(json.contains("\"tags\":{"));
    assertTrue(json.contains("\"dc\":\"phx\""));
    assertTrue(json.contains("\"host\":\"web01\""));
    assertTrue(json.contains("\"host\":\"web02\""));
    assertTrue(json.contains("\"aggregateTags\":[\"owner\"]"));
    assertTrue(json.contains("\"dps\":{"));
    
    assertTrue(json.contains("\"1486045800000\":1"));
    assertTrue(json.contains("\"1486045860000\":5.75"));
    assertTrue(json.contains("\"1486045800000\":4"));
    assertTrue(json.contains("\"1486045860000\":0.0015"));
  }
  
  @Test
  public void serializeFilterEarlyValues() throws Exception {
    query = net.opentsdb.query.pojo.TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("1486045860")
            .setEnd("1486046000")
            .setAggregator("sum"))
        .addMetric(Metric.newBuilder()
            .setId("m1")
            .setMetric("sys.cpu.user"))
        .build()
        .convert(tsdb).build();
    when(context.query()).thenReturn(query);
    
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    final JsonV2QuerySerdes serdes = new JsonV2QuerySerdes(context, options, output);
    serdes.serialize(result, null);
    output.close();
    final String json = new String(output.toByteArray(), Const.UTF8_CHARSET);
    
    assertTrue(json.contains("\"metric\":\"sys.cpu.user\""));
    assertTrue(json.contains("\"tags\":{"));
    assertTrue(json.contains("\"dc\":\"phx\""));
    assertTrue(json.contains("\"host\":\"web01\""));
    assertTrue(json.contains("\"host\":\"web02\""));
    assertTrue(json.contains("\"aggregateTags\":[\"owner\"]"));
    assertTrue(json.contains("\"dps\":{"));
    
    assertFalse(json.contains("\"1486045800\":1"));
    assertTrue(json.contains("\"1486045860\":5.75"));
    assertFalse(json.contains("\"1486045800\":4"));
    assertTrue(json.contains("\"1486045860\":0.0015"));
  }
  
  @Test
  public void serializeFilterLateValues() throws Exception {
    query = net.opentsdb.query.pojo.TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("1486045200")
            .setEnd("1486045800")
            .setAggregator("sum"))
        .addMetric(Metric.newBuilder()
            .setId("m1")
            .setMetric("sys.cpu.user"))
        .build()
        .convert(tsdb).build();
    when(context.query()).thenReturn(query);
    
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    final JsonV2QuerySerdes serdes = new JsonV2QuerySerdes(context, options, output);
    serdes.serialize(result, null);
    output.close();
    final String json = new String(output.toByteArray(), Const.UTF8_CHARSET);
    assertTrue(json.contains("\"metric\":\"sys.cpu.user\""));
    assertTrue(json.contains("\"tags\":{"));
    assertTrue(json.contains("\"dc\":\"phx\""));
    assertTrue(json.contains("\"host\":\"web01\""));
    assertTrue(json.contains("\"host\":\"web02\""));
    assertTrue(json.contains("\"aggregateTags\":[\"owner\"]"));
    assertTrue(json.contains("\"dps\":{"));
    
    assertTrue(json.contains("\"1486045800\":1"));
    assertFalse(json.contains("\"1486045860\":5.75"));
    assertTrue(json.contains("\"1486045800\":4"));
    assertFalse(json.contains("\"1486045860\":0.0015"));
  }
  
  @Test
  public void serializeFilterOOBEarlyValues() throws Exception {
    query = net.opentsdb.query.pojo.TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("1486046100")
            .setEnd("1486046400")
            .setAggregator("sum"))
        .addMetric(Metric.newBuilder()
            .setId("m1")
            .setMetric("sys.cpu.user"))
        .build()
        .convert(tsdb).build();
    when(context.query()).thenReturn(query);
    
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    final JsonV2QuerySerdes serdes = new JsonV2QuerySerdes(context, options, output);
    serdes.serialize(result, null);
    serdes.serializeComplete(null);
    output.close();
    final String json = new String(output.toByteArray(), Const.UTF8_CHARSET);
    assertEquals("[]", json);
  }
  
  @Test
  public void serializeFilterOOBLateValues() throws Exception {
    query = net.opentsdb.query.pojo.TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("1486045200")
            .setEnd("1486045500")
            .setAggregator("sum"))
        .addMetric(Metric.newBuilder()
            .setId("m1")
            .setMetric("sys.cpu.user"))
        .build()
        .convert(tsdb).build();
    when(context.query()).thenReturn(query);
    
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    final JsonV2QuerySerdes serdes = new JsonV2QuerySerdes(context, options, output);
    serdes.serialize(result, null);
    serdes.serializeComplete(null);
    output.close();
    final String json = new String(output.toByteArray(), Const.UTF8_CHARSET);
    assertEquals("[]", json);
  }
  
  @Test
  public void serializeEmpty() throws Exception {
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    final JsonV2QuerySerdes serdes = new JsonV2QuerySerdes(context, options, output);
    when(result.timeSeries()).thenReturn(Collections.emptyList());
    serdes.serialize(result, null);
    serdes.serializeComplete(null);
    output.close();
    final String json = new String(output.toByteArray(), Const.UTF8_CHARSET);
    assertEquals("[]", json);
  }

  @Test
  public void serializeExceptions() throws Exception {
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    final JsonV2QuerySerdes serdes = new JsonV2QuerySerdes(context, options, output);
    
    try {
      serdes.serialize((QueryResult) null, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    QueryNode node = mock(QueryNode.class);
    serdes.deserialize(node, null);
    verify(node, times(1)).onError(any(Throwable.class));
    
    try {
      new JsonV2QuerySerdes(context, null, output);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new JsonV2QuerySerdes(context, mock(SerdesOptions.class), output);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new JsonV2QuerySerdes(context, options, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void serializeBinaryIds() throws Exception {
    when(result.idType()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return Const.TS_BYTE_ID;
      }
    });
    
    TimeSeriesByteId byte_id1 = BaseTimeSeriesByteId.newBuilder(store)
        .setMetric(new byte[] { 0, 0, 1 })
      .build();
    TimeSeriesByteId byte_id2 = BaseTimeSeriesByteId.newBuilder(store)
        .setMetric(new byte[] { 0, 0, 2 })
      .build();
    
    when(store.resolveByteId(byte_id1, null)).thenReturn(
        Deferred.fromResult((TimeSeriesStringId) id1));
    
    when(store.resolveByteId(byte_id2, null)).thenReturn(
        Deferred.fromResult((TimeSeriesStringId) id2));
    
    ts1 = new NumericMillisecondShard(byte_id1,
        new MillisecondTimeStamp(1486045800000L), 
        new MillisecondTimeStamp(1486046000000L));
    ((NumericMillisecondShard) ts1).add(1486045800000L, 1);
    ((NumericMillisecondShard) ts1).add(1486045860000L, 5.75);
    
    ts2 = new NumericMillisecondShard(byte_id2,
        new MillisecondTimeStamp(1486045800000L), 
        new MillisecondTimeStamp(1486046000000L));
    ((NumericMillisecondShard) ts2).add(1486045800000L, 4);
    ((NumericMillisecondShard) ts2).add(1486045860000L, 0.0015);
    
    when(result.timeSeries()).thenReturn(Lists.newArrayList(ts1, ts2));
    
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    final JsonV2QuerySerdes serdes = new JsonV2QuerySerdes(context, options, output);
    serdes.serialize(result, null);
    output.close();
    final String json = new String(output.toByteArray(), Const.UTF8_CHARSET);
    
    assertTrue(json.contains("\"metric\":\"sys.cpu.user\""));
    assertTrue(json.contains("\"tags\":{"));
    assertTrue(json.contains("\"dc\":\"phx\""));
    assertTrue(json.contains("\"host\":\"web01\""));
    assertTrue(json.contains("\"host\":\"web02\""));
    assertTrue(json.contains("\"aggregateTags\":[\"owner\"]"));
    assertTrue(json.contains("\"dps\":{"));
    
    assertTrue(json.contains("\"1486045800\":1"));
    assertTrue(json.contains("\"1486045860\":5.75"));
    assertTrue(json.contains("\"1486045800\":4"));
    assertTrue(json.contains("\"1486045860\":0.0015"));
    
    verify(store, times(2)).resolveByteId(any(TimeSeriesByteId.class), any());
  }
  
  @Test
  public void serializeBinaryIdsResolveException() throws Exception {
    when(result.idType()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return Const.TS_BYTE_ID;
      }
    });
    
    TimeSeriesByteId byte_id1 = BaseTimeSeriesByteId.newBuilder(store)
        .setMetric(new byte[] { 0, 0, 1 })
      .build();
    TimeSeriesByteId byte_id2 = BaseTimeSeriesByteId.newBuilder(store)
        .setMetric(new byte[] { 0, 0, 2 })
      .build();
    
    when(store.resolveByteId(byte_id1, null)).thenReturn(
        Deferred.fromResult((TimeSeriesStringId) id1));
    
    when(store.resolveByteId(byte_id2, null)).thenReturn(
        Deferred.fromError(new UnitTestException()));
    
    ts1 = new NumericMillisecondShard(byte_id1,
        new MillisecondTimeStamp(1486045800000L), 
        new MillisecondTimeStamp(1486046000000L));
    ((NumericMillisecondShard) ts1).add(1486045800000L, 1);
    ((NumericMillisecondShard) ts1).add(1486045860000L, 5.75);
    
    ts2 = new NumericMillisecondShard(byte_id2,
        new MillisecondTimeStamp(1486045800000L), 
        new MillisecondTimeStamp(1486046000000L));
    ((NumericMillisecondShard) ts2).add(1486045800000L, 4);
    ((NumericMillisecondShard) ts2).add(1486045860000L, 0.0015);
    
    when(result.timeSeries()).thenReturn(Lists.newArrayList(ts1, ts2));
    
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    final JsonV2QuerySerdes serdes = new JsonV2QuerySerdes(context, options, output);
    try {
      serdes.serialize(result, null).join();
      fail("Expected UnitTestException");
    } catch (UnitTestException e) { }
    output.close();
  }

  @Test
  public void serializeSummaryFull() throws Exception {
    ts1 = new MockTimeSeries((TimeSeriesStringId) id1);
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
    v.resetTimestamp(new SecondTimeStamp(1486045800));
    v.resetValue(0, 42);
    ((MockTimeSeries) ts1).addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new SecondTimeStamp(1486045860));
    v.resetValue(0, 24.3);
    ((MockTimeSeries) ts1).addValue(v);
    
    ts2 = new MockTimeSeries((TimeSeriesStringId) id2);
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new SecondTimeStamp(1486045800));
    v.resetValue(0, -1.75);
    ((MockTimeSeries) ts2).addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new SecondTimeStamp(1486045860));
    v.resetValue(0, 1024);
    ((MockTimeSeries) ts2).addValue(v);
    
    when(result.timeSeries()).thenReturn(Lists.newArrayList(ts1, ts2));
    
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    final JsonV2QuerySerdes serdes = new JsonV2QuerySerdes(context, options, output);
    serdes.serialize(result, null);
    output.close();
    final String json = new String(output.toByteArray(), Const.UTF8_CHARSET);
    
    assertTrue(json.contains("\"metric\":\"sys.cpu.user\""));
    assertTrue(json.contains("\"tags\":{"));
    assertTrue(json.contains("\"dc\":\"phx\""));
    assertTrue(json.contains("\"host\":\"web01\""));
    assertTrue(json.contains("\"host\":\"web02\""));
    assertTrue(json.contains("\"aggregateTags\":[\"owner\"]"));
    assertTrue(json.contains("\"dps\":{"));
    
    assertTrue(json.contains("\"1486045800\":42"));
    assertTrue(json.contains("\"1486045860\":24.3"));
    assertTrue(json.contains("\"1486045800\":-1.75"));
    assertTrue(json.contains("\"1486045860\":1024"));
  }

  @Test
  public void serializeArrayFull() throws Exception {
    TimeSpecification spec = mock(TimeSpecification.class);
    when(spec.start()).thenReturn(new SecondTimeStamp(1486045800));
    when(spec.interval()).thenReturn(Duration.ofSeconds(60));
    when(result.timeSpecification()).thenReturn(spec);
    
    ts1 = new NumericArrayTimeSeries(id1, new SecondTimeStamp(1486045800));
    ((NumericArrayTimeSeries) ts1).add(1);
    ((NumericArrayTimeSeries) ts1).add(5);
    
    ts2 = new NumericArrayTimeSeries(id2, new SecondTimeStamp(1486045800));
    ((NumericArrayTimeSeries) ts2).add(0.009);
    ((NumericArrayTimeSeries) ts2).add(-2.45);
    
    when(result.timeSeries()).thenReturn(Lists.newArrayList(ts1, ts2));
    
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    final JsonV2QuerySerdes serdes = new JsonV2QuerySerdes(context, options, output);
    serdes.serialize(result, null);
    output.close();
    final String json = new String(output.toByteArray(), Const.UTF8_CHARSET);
    assertTrue(json.contains("\"metric\":\"sys.cpu.user\""));
    assertTrue(json.contains("\"tags\":{"));
    assertTrue(json.contains("\"dc\":\"phx\""));
    assertTrue(json.contains("\"host\":\"web01\""));
    assertTrue(json.contains("\"host\":\"web02\""));
    assertTrue(json.contains("\"aggregateTags\":[\"owner\"]"));
    assertTrue(json.contains("\"dps\":{"));
    
    assertTrue(json.contains("\"1486045800\":1"));
    assertTrue(json.contains("\"1486045860\":5"));
    assertTrue(json.contains("\"1486045800\":0.009"));
    assertTrue(json.contains("\"1486045860\":-2.45"));
  }
}
