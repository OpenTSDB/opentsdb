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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.util.Collections;

import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.collect.Lists;

import net.opentsdb.common.Const;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.BaseTimeSeriesId;
import net.opentsdb.data.types.numeric.NumericMillisecondShard;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.query.pojo.Timespan;
import net.opentsdb.utils.JSON;

public class TestJsonV2QuerySerdes {

  private QueryContext context;
  private QueryResult result;
  private TimeSeriesQuery query;
  private NumericMillisecondShard ts1;
  private NumericMillisecondShard ts2;
  
  @Before
  public void before() throws Exception {
    context = mock(QueryContext.class);
    result = mock(QueryResult.class);
    query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("1486045800000")
            .setEnd("1486046000000"))
        .build();
    when(context.query()).thenReturn(query);
    
    ts1 = new NumericMillisecondShard(
        BaseTimeSeriesId.newBuilder()
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
        BaseTimeSeriesId.newBuilder()
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
  }
  
  @Test
  public void fullSerdes() throws Exception {
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    final JsonGenerator generator = JSON.getFactory().createGenerator(output);
    final JsonV2QuerySerdes serdes = new JsonV2QuerySerdes(generator);
    serdes.serialize(context, null, output, result);
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
  public void fullSerdesWithMilliseconds() throws Exception {
    final SerdesOptions conf = JsonV2QuerySerdesOptions.newBuilder()
        .setMsResolution(true)
        .build();
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    final JsonGenerator generator = JSON.getFactory().createGenerator(output);
    final JsonV2QuerySerdes serdes = new JsonV2QuerySerdes(generator);
    serdes.serialize(context, conf, output, result);
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
  public void serdesFilterEarlyValues() throws Exception {
    query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("1486045860000")
            .setEnd("1486046100000"))
        .build();
    when(context.query()).thenReturn(query);
    
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    final JsonGenerator generator = JSON.getFactory().createGenerator(output);
    final JsonV2QuerySerdes serdes = new JsonV2QuerySerdes(generator);
    serdes.serialize(context, null, output, result);
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
  public void serdesFilterLateValues() throws Exception {
    query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("1486045200000")
            .setEnd("1486045800000"))
        .build();
    when(context.query()).thenReturn(query);
    
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    final JsonGenerator generator = JSON.getFactory().createGenerator(output);
    final JsonV2QuerySerdes serdes = new JsonV2QuerySerdes(generator);
    serdes.serialize(context, null, output, result);
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
  public void serdesFilterOOBEarlyValues() throws Exception {
    query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("1486046100000")
            .setEnd("1486046400000"))
        .build();
    when(context.query()).thenReturn(query);
    
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    final JsonGenerator generator = JSON.getFactory().createGenerator(output);
    final JsonV2QuerySerdes serdes = new JsonV2QuerySerdes(generator);
    serdes.serialize(context, null, output, result);
    output.close();
    final String json = new String(output.toByteArray(), Const.UTF8_CHARSET);
    assertTrue(json.isEmpty());
  }
  
  @Test
  public void serdesFilterOOBLateValues() throws Exception {
    query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("1486045200000")
            .setEnd("1486045500000"))
        .build();
    when(context.query()).thenReturn(query);
    
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    final JsonGenerator generator = JSON.getFactory().createGenerator(output);
    final JsonV2QuerySerdes serdes = new JsonV2QuerySerdes(generator);
    serdes.serialize(context, null, output, result);
    output.close();
    final String json = new String(output.toByteArray(), Const.UTF8_CHARSET);
    assertTrue(json.isEmpty());
  }
  
  @Test
  public void empty() throws Exception {
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    final JsonGenerator generator = JSON.getFactory().createGenerator(output);
    final JsonV2QuerySerdes serdes = new JsonV2QuerySerdes(generator);
    when(result.timeSeries()).thenReturn(Collections.emptyList());
    serdes.serialize(context, null, output, result);
    output.close();
    final String json = new String(output.toByteArray(), Const.UTF8_CHARSET);
    assertEquals("", json);
  }

  @Test
  public void exceptions() throws Exception {
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    final JsonGenerator generator = JSON.getFactory().createGenerator(output);
    
    try {
      new JsonV2QuerySerdes(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    final JsonV2QuerySerdes serdes = new JsonV2QuerySerdes(generator);
    
    try {
      serdes.serialize(context, null, null, result);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      serdes.serialize(context, null, output, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      serdes.deserialize(null, null);
      fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) { }
  }
}
