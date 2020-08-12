// This file is part of OpenTSDB.
// Copyright (C) 2017-2020  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.Collections;

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.common.Const;
import net.opentsdb.configuration.UnitTestConfiguration;
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
import net.opentsdb.data.BaseTimeSeriesByteId;
import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.types.numeric.MutableNumericSummaryValue;
import net.opentsdb.data.types.numeric.NumericMillisecondShard;
import net.opentsdb.data.types.numeric.aggregators.NumericAggregatorFactory;
import net.opentsdb.data.types.numeric.aggregators.SumFactory;
import net.opentsdb.query.DefaultQueryResultId;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.TimeSeriesQuery;
import net.opentsdb.query.pojo.Metric;
import net.opentsdb.query.pojo.Timespan;
import net.opentsdb.utils.UnitTestException;

public class TestJsonV3QuerySerdes {

  private TSDB tsdb;
  private QueryContext context;
  private QueryResult result;
  private TimeSeriesQuery query;
  private TimeSeries ts1;
  private TimeSeriesId id1;
  private TimeSeries ts2;
  private TimeSeriesId id2;
  private TimeSeriesDataSourceFactory store;
  private JsonV3QuerySerdesOptions options;

  @Before
  public void before() throws Exception {
    tsdb = mock(TSDB.class);
    when(tsdb.getConfig()).thenReturn(UnitTestConfiguration.getConfiguration());
    Registry registry = mock(Registry.class);
    when(tsdb.getRegistry()).thenReturn(registry);
    when(tsdb.getRegistry().getPlugin(eq(NumericAggregatorFactory.class), anyString()))
        .thenReturn(new SumFactory());

    context = mock(QueryContext.class);
    result = mock(QueryResult.class);
    when(result.dataSource()).thenReturn(new DefaultQueryResultId("s1", "s1"));
    store = mock(TimeSeriesDataSourceFactory.class);
    query = net.opentsdb.query.pojo.TimeSeriesQuery.newBuilder()
        .setTime(
            Timespan.newBuilder().setStart("1486045800").setEnd("1486046000").setAggregator("sum"))
        .addMetric(Metric.newBuilder().setId("m1").setMetric("sys.cpu.user")).build().convert(tsdb)
        .build();
    when(context.query()).thenReturn(query);

    id1 = BaseTimeSeriesStringId.newBuilder().setMetric("sys.cpu.user").addTags("host", "web01")
        .addTags("dc", "phx").addAggregatedTag("owner").build();

    ts1 = new NumericMillisecondShard(id1, new MillisecondTimeStamp(1486045800000L),
        new MillisecondTimeStamp(1486046000000L));
    ((NumericMillisecondShard) ts1).add(1486045800000L, 1);
    ((NumericMillisecondShard) ts1).add(1486045860000L, 5.75);

    id2 = BaseTimeSeriesStringId.newBuilder().setMetric("sys.cpu.user").addTags("host", "web02")
        .addTags("dc", "phx").addAggregatedTag("owner").build();

    ts2 = new NumericMillisecondShard(id2, new MillisecondTimeStamp(1486045800000L),
        new MillisecondTimeStamp(1486046000000L));
    ((NumericMillisecondShard) ts2).add(1486045800000L, 4);
    ((NumericMillisecondShard) ts2).add(1486045860000L, 0.0015);

    when(result.timeSeries()).thenReturn(Lists.newArrayList(ts1, ts2));
    QueryPipelineContext ctx = mock(QueryPipelineContext.class);
    QueryNode src = mock(QueryNode.class);
    when(src.pipelineContext()).thenReturn(ctx);
    QueryNodeConfig c1 = mock(QueryNodeConfig.class);
    when(c1.getId()).thenReturn("s1");
    when(src.config()).thenReturn(c1);
    when(ctx.downstream(any(QueryNode.class))).thenReturn(Lists.newArrayList());
    when(result.source()).thenReturn(src);

    options =
        (JsonV3QuerySerdesOptions) JsonV3QuerySerdesOptions.newBuilder().setId("json").build();
  }

  @Test
  public void serializeFull() throws Exception {
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    final JsonV3QuerySerdes serdes = new JsonV3QuerySerdes(context, options, output);
    serdes.serialize(result, null);
    serdes.serializeComplete(null);
    String json = new String(output.toByteArray(), Const.UTF8_CHARSET);

    assertTrue(json.startsWith("{"));
    assertTrue(json.endsWith("}"));
    assertTrue(json.contains("\"metric\":\"sys.cpu.user\""));
    assertTrue(json.contains("\"tags\":{"));
    assertTrue(json.contains("\"dc\":\"phx\""));
    assertTrue(json.contains("\"host\":\"web01\""));
    assertTrue(json.contains("\"host\":\"web02\""));
    assertTrue(json.contains("\"aggregateTags\":[\"owner\"]"));
    assertFalse(json.contains("\"dps\":{"));

    assertTrue(json.contains("\"1486045800\":1"));
    assertTrue(json.contains("\"1486045860\":5.75"));
    assertTrue(json.contains("\"1486045800\":4"));
    assertTrue(json.contains("\"1486045860\":0.0015"));

    output.close();
    json = new String(output.toByteArray(), Const.UTF8_CHARSET);
  }

  @Test
  public void serializeWithMilliseconds() throws Exception {
    options = (JsonV3QuerySerdesOptions) JsonV3QuerySerdesOptions.newBuilder().setMsResolution(true)
        .setId("json").build();
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    final JsonV3QuerySerdes serdes = new JsonV3QuerySerdes(context, options, output);
    serdes.serialize(result, null);
    serdes.serializeComplete(null);
    output.close();
    final String json = new String(output.toByteArray(), Const.UTF8_CHARSET);

    assertTrue(json.contains("\"metric\":\"sys.cpu.user\""));
    assertTrue(json.contains("\"tags\":{"));
    assertTrue(json.contains("\"dc\":\"phx\""));
    assertTrue(json.contains("\"host\":\"web01\""));
    assertTrue(json.contains("\"host\":\"web02\""));
    assertTrue(json.contains("\"aggregateTags\":[\"owner\"]"));
    assertFalse(json.contains("\"dps\":{"));

    assertTrue(json.contains("\"1486045800000\":1"));
    assertTrue(json.contains("\"1486045860000\":5.75"));
    assertTrue(json.contains("\"1486045800000\":4"));
    assertTrue(json.contains("\"1486045860000\":0.0015"));
  }

  @Test
  public void serializeEmpty() throws Exception {
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    final JsonV3QuerySerdes serdes = new JsonV3QuerySerdes(context, options, output);
    when(result.timeSeries()).thenReturn(Collections.emptyList());
    serdes.serialize(result, null);
    serdes.serializeComplete(null);
    output.close();
    final String json = new String(output.toByteArray(), Const.UTF8_CHARSET);
    assertEquals(json, "{\"results\":[{\"source\":\"s1:s1\",\"data\":[]}],\"log\":[]}");
  }

  @Test
  public void serializeBinaryIds() throws Exception {
    when(result.idType()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return Const.TS_BYTE_ID;
      }
    });

    TimeSeriesByteId byte_id1 =
        BaseTimeSeriesByteId.newBuilder(store).setMetric(new byte[] {0, 0, 1}).build();
    TimeSeriesByteId byte_id2 =
        BaseTimeSeriesByteId.newBuilder(store).setMetric(new byte[] {0, 0, 2}).build();

    when(store.resolveByteId(byte_id1, null))
        .thenReturn(Deferred.fromResult((TimeSeriesStringId) id1));

    when(store.resolveByteId(byte_id2, null))
        .thenReturn(Deferred.fromResult((TimeSeriesStringId) id2));

    ts1 = new NumericMillisecondShard(byte_id1, new MillisecondTimeStamp(1486045800000L),
        new MillisecondTimeStamp(1486046000000L));
    ((NumericMillisecondShard) ts1).add(1486045800000L, 1);
    ((NumericMillisecondShard) ts1).add(1486045860000L, 5.75);

    ts2 = new NumericMillisecondShard(byte_id2, new MillisecondTimeStamp(1486045800000L),
        new MillisecondTimeStamp(1486046000000L));
    ((NumericMillisecondShard) ts2).add(1486045800000L, 4);
    ((NumericMillisecondShard) ts2).add(1486045860000L, 0.0015);

    when(result.timeSeries()).thenReturn(Lists.newArrayList(ts1, ts2));

    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    final JsonV3QuerySerdes serdes = new JsonV3QuerySerdes(context, options, output);
    serdes.serialize(result, null);
    serdes.serializeComplete(null);
    output.close();
    final String json = new String(output.toByteArray(), Const.UTF8_CHARSET);

    assertTrue(json.contains("\"metric\":\"sys.cpu.user\""));
    assertTrue(json.contains("\"tags\":{"));
    assertTrue(json.contains("\"dc\":\"phx\""));
    assertTrue(json.contains("\"host\":\"web01\""));
    assertTrue(json.contains("\"host\":\"web02\""));
    assertTrue(json.contains("\"aggregateTags\":[\"owner\"]"));
    assertFalse(json.contains("\"dps\":{"));

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
    final JsonV3QuerySerdes serdes = new JsonV3QuerySerdes(context, options, output);
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
    final JsonV3QuerySerdes serdes = new JsonV3QuerySerdes(context, options, output);
    serdes.serialize(result, null);
    serdes.serializeComplete(null);
    output.close();
    final String json = new String(output.toByteArray(), Const.UTF8_CHARSET);
    
    assertTrue(json.contains("\"metric\":\"sys.cpu.user\""));
    assertTrue(json.contains("\"tags\":{"));
    assertTrue(json.contains("\"dc\":\"phx\""));
    assertTrue(json.contains("\"host\":\"web01\""));
    assertTrue(json.contains("\"host\":\"web02\""));
    assertTrue(json.contains("\"aggregateTags\":[\"owner\"]"));
    assertFalse(json.contains("\"dps\":{"));
    
    assertTrue(json.contains("\"1486045800\":42"));
    assertTrue(json.contains("\"1486045860\":24.3"));
    assertTrue(json.contains("\"1486045800\":-1.75"));
    assertTrue(json.contains("\"1486045860\":1024"));
  }

}
