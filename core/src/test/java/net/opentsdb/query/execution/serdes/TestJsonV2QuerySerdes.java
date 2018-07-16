// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.BaseTimeSeriesByteId;
import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.types.numeric.NumericMillisecondShard;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.query.pojo.Timespan;
import net.opentsdb.query.serdes.SerdesOptions;
import net.opentsdb.storage.ReadableTimeSeriesDataStore;
import net.opentsdb.utils.UnitTestException;

public class TestJsonV2QuerySerdes {

  private QueryContext context;
  private QueryResult result;
  private TimeSeriesQuery query;
  private NumericMillisecondShard ts1;
  private NumericMillisecondShard ts2;
  private ReadableTimeSeriesDataStore store;
  private JsonV2QuerySerdesOptions options;
  
  @Before
  public void before() throws Exception {
    context = mock(QueryContext.class);
    result = mock(QueryResult.class);
    store = mock(ReadableTimeSeriesDataStore.class);
    query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("1486045800000")
            .setEnd("1486046000000"))
        .build();
    when(context.query()).thenReturn(query);
    
    ts1 = new NumericMillisecondShard(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("sys.cpu.user")
        .addTags("host", "web01")
        .addTags("dc", "phx")
        .addAggregatedTag("owner")
        .build(), 
        new MillisecondTimeStamp(1486045800000L), 
        new MillisecondTimeStamp(1486046000000L));
    ts1.add(1486045800000L, 1);
    ts1.add(1486045860000L, 5.75);
    
    ts2 = new NumericMillisecondShard(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("sys.cpu.user")
        .addTags("host", "web02")
        .addTags("dc", "phx")
        .addAggregatedTag("owner")
        .build(),
        new MillisecondTimeStamp(1486045800000L), 
        new MillisecondTimeStamp(1486046000000L));
    ts2.add(1486045800000L, 4);
    ts2.add(1486045860000L, 0.0015);
    
    when(result.timeSeries()).thenReturn(Lists.newArrayList(ts1, ts2));
    
    options = (JsonV2QuerySerdesOptions) JsonV2QuerySerdesOptions.newBuilder()
        .setStart(new MillisecondTimeStamp(1486045800000L))
        .setEnd(new MillisecondTimeStamp(1486046000000L))
        .setId("json")
        .build();
  }
  
  @Test
  public void serializeFull() throws Exception {
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    final JsonV2QuerySerdes serdes = new JsonV2QuerySerdes();
    serdes.serialize(context, options, output, result, null);
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
  }
  
  @Test
  public void serializeWithMilliseconds() throws Exception {
    final SerdesOptions conf = JsonV2QuerySerdesOptions.newBuilder()
        .setMsResolution(true)
        .setStart(new MillisecondTimeStamp(1486045800000L))
        .setEnd(new MillisecondTimeStamp(1486045860000L))
        .setId("json")
        .build();
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    final JsonV2QuerySerdes serdes = new JsonV2QuerySerdes();
    serdes.serialize(context, conf, output, result, null);
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
    options = (JsonV2QuerySerdesOptions) JsonV2QuerySerdesOptions.newBuilder()
        .setStart(new MillisecondTimeStamp(1486045860000L))
        .setEnd(new MillisecondTimeStamp(1486046100000L))
        .setId("json")
        .build();
    
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    final JsonV2QuerySerdes serdes = new JsonV2QuerySerdes();
    serdes.serialize(context, options, output, result, null);
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
    options = (JsonV2QuerySerdesOptions) JsonV2QuerySerdesOptions.newBuilder()
        .setStart(new MillisecondTimeStamp(1486045200000L))
        .setEnd(new MillisecondTimeStamp(1486045800000L))
        .setId("json")
        .build();
    
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    final JsonV2QuerySerdes serdes = new JsonV2QuerySerdes();
    serdes.serialize(context, options, output, result, null);
    output.close();
    final String json = new String(output.toByteArray(), Const.UTF8_CHARSET);
    System.out.println(json);
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
    options = (JsonV2QuerySerdesOptions) JsonV2QuerySerdesOptions.newBuilder()
        .setStart(new MillisecondTimeStamp(1486046100000L))
        .setEnd(new MillisecondTimeStamp(1486046400000L))
        .setId("json")
        .build();
    
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    final JsonV2QuerySerdes serdes = new JsonV2QuerySerdes();
    serdes.serialize(context, options, output, result, null);
    output.close();
    final String json = new String(output.toByteArray(), Const.UTF8_CHARSET);
    assertEquals("[]", json);
  }
  
  @Test
  public void serializeFilterOOBLateValues() throws Exception {
    options = (JsonV2QuerySerdesOptions) JsonV2QuerySerdesOptions.newBuilder()
        .setStart(new MillisecondTimeStamp(1486045200000L))
        .setEnd(new MillisecondTimeStamp(1486045500000L))
        .setId("json")
        .build();
    
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    final JsonV2QuerySerdes serdes = new JsonV2QuerySerdes();
    serdes.serialize(context, options, output, result, null);
    output.close();
    final String json = new String(output.toByteArray(), Const.UTF8_CHARSET);
    assertEquals("[]", json);
  }
  
  @Test
  public void serializeEmpty() throws Exception {
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    final JsonV2QuerySerdes serdes = new JsonV2QuerySerdes();
    when(result.timeSeries()).thenReturn(Collections.emptyList());
    serdes.serialize(context, options, output, result, null);
    output.close();
    final String json = new String(output.toByteArray(), Const.UTF8_CHARSET);
    assertEquals("[]", json);
  }

  @Test
  public void serializeExceptions() throws Exception {
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    final JsonV2QuerySerdes serdes = new JsonV2QuerySerdes();
    
    try {
      serdes.serialize(context, options, null, result, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      serdes.serialize(context, options, output, null, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    QueryNode node = mock(QueryNode.class);
    serdes.deserialize(options, null, node, null);
    verify(node, times(1)).onError(any(Throwable.class));
  }

  @Test
  public void serializeBinaryIds() throws Exception {
    when(result.idType()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return Const.TS_BYTE_ID;
      }
    });
    
    TimeSeriesByteId id1 = BaseTimeSeriesByteId.newBuilder(store)
        .setMetric(new byte[] { 0, 0, 1 })
      .build();
    TimeSeriesByteId id2 = BaseTimeSeriesByteId.newBuilder(store)
        .setMetric(new byte[] { 0, 0, 2 })
      .build();
    
    when(store.resolveByteId(id1, null)).thenReturn(
        Deferred.fromResult(BaseTimeSeriesStringId.newBuilder()
            .setMetric("sys.cpu.user")
            .addTags("host", "web01")
            .addTags("dc", "phx")
            .addAggregatedTag("owner")
            .build()));
    
    when(store.resolveByteId(id2, null)).thenReturn(
        Deferred.fromResult(BaseTimeSeriesStringId.newBuilder()
            .setMetric("sys.cpu.user")
            .addTags("host", "web02")
            .addTags("dc", "phx")
            .addAggregatedTag("owner")
            .build()));
    
    ts1 = new NumericMillisecondShard(id1,
        new MillisecondTimeStamp(1486045800000L), 
        new MillisecondTimeStamp(1486046000000L));
    ts1.add(1486045800000L, 1);
    ts1.add(1486045860000L, 5.75);
    
    ts2 = new NumericMillisecondShard(id2,
        new MillisecondTimeStamp(1486045800000L), 
        new MillisecondTimeStamp(1486046000000L));
    ts2.add(1486045800000L, 4);
    ts2.add(1486045860000L, 0.0015);
    
    when(result.timeSeries()).thenReturn(Lists.newArrayList(ts1, ts2));
    
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    final JsonV2QuerySerdes serdes = new JsonV2QuerySerdes();
    serdes.serialize(context, options, output, result, null);
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
    
    TimeSeriesByteId id1 = BaseTimeSeriesByteId.newBuilder(store)
        .setMetric(new byte[] { 0, 0, 1 })
      .build();
    TimeSeriesByteId id2 = BaseTimeSeriesByteId.newBuilder(store)
        .setMetric(new byte[] { 0, 0, 2 })
      .build();
    
    when(store.resolveByteId(id1, null)).thenReturn(
        Deferred.fromResult(BaseTimeSeriesStringId.newBuilder()
            .setMetric("sys.cpu.user")
            .addTags("host", "web01")
            .addTags("dc", "phx")
            .addAggregatedTag("owner")
            .build()));
    
    when(store.resolveByteId(id2, null)).thenReturn(
        Deferred.fromError(new UnitTestException()));
    
    ts1 = new NumericMillisecondShard(id1,
        new MillisecondTimeStamp(1486045800000L), 
        new MillisecondTimeStamp(1486046000000L));
    ts1.add(1486045800000L, 1);
    ts1.add(1486045860000L, 5.75);
    
    ts2 = new NumericMillisecondShard(id2,
        new MillisecondTimeStamp(1486045800000L), 
        new MillisecondTimeStamp(1486046000000L));
    ts2.add(1486045800000L, 4);
    ts2.add(1486045860000L, 0.0015);
    
    when(result.timeSeries()).thenReturn(Lists.newArrayList(ts1, ts2));
    
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    final JsonV2QuerySerdes serdes = new JsonV2QuerySerdes();
    try {
      serdes.serialize(context, options, output, result, null).join();
      fail("Expected UnitTestException");
    } catch (UnitTestException e) { }
    output.close();
  }
}
