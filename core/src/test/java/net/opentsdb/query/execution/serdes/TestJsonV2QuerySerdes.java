// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.query.execution.serdes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;

import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonGenerator;

import net.opentsdb.common.Const;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.SimpleStringGroupId;
import net.opentsdb.data.BaseTimeSeriesId;
import net.opentsdb.data.TimeSeriesGroupId;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.iterators.DefaultIteratorGroups;
import net.opentsdb.data.iterators.IteratorGroups;
import net.opentsdb.data.types.numeric.NumericMillisecondShard;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.query.pojo.Timespan;
import net.opentsdb.utils.JSON;

public class TestJsonV2QuerySerdes {

  private TimeSeriesQuery query;
  private TimeStamp start;
  private TimeStamp end;
  
  @Before
  public void before() throws Exception {
    query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("1486045800000")
            .setEnd("1486046000000"))
        .build();
    start = new MillisecondTimeStamp(1486045800000L);
    end = new MillisecondTimeStamp(1486046000000L);
  }
  
  @Test
  public void fullSerdes() throws Exception {
    final IteratorGroups results = new DefaultIteratorGroups();
    
    final TimeSeriesGroupId group_id_a = new SimpleStringGroupId("a");
    final TimeSeriesId id_a = BaseTimeSeriesId.newBuilder()
        .setMetric("sys.cpu.user")
        .addTags("host", "web01")
        .addTags("dc", "phx")
    .build();
    
    NumericMillisecondShard shard = 
        new NumericMillisecondShard(id_a, start, end);
    shard.add(1486045801000L, 42);
    shard.add(1486045871000L, 9866.854);
    shard.add(1486045881000L, -128);
    results.addIterator(group_id_a, shard);
    
    final TimeSeriesId id_b = BaseTimeSeriesId.newBuilder()
        .setMetric("sys.cpu.user")
        .addTags("host", "web02")
        .addTags("dc", "phx")
    .build();
    shard = new NumericMillisecondShard(id_b, start, end);
    shard.add(1486045801000L, 8);
    shard.add(1486045871000L, Double.NaN);
    shard.add(1486045881000L, 5000);
    results.addIterator(group_id_a, shard);
    
    final TimeSeriesGroupId group_id_b = new SimpleStringGroupId("b");
    shard = new NumericMillisecondShard(id_a, start, end);
    shard.add(1486045801000L, 5);
    shard.add(1486045871000L, Double.NaN);
    shard.add(1486045881000L, 2);
    results.addIterator(group_id_b, shard);
    
    shard = new NumericMillisecondShard(id_b, start, end);
    shard.add(1486045801000L, 20);
    shard.add(1486045871000L, Double.NaN);
    shard.add(1486045881000L, 13);
    results.addIterator(group_id_b, shard);
    
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    final JsonGenerator generator = JSON.getFactory().createGenerator(output);
    final JsonV2QuerySerdes serdes = new JsonV2QuerySerdes(generator);
    serdes.serialize(query, null, output, results);
    output.close();
    final String json = new String(output.toByteArray(), Const.UTF8_CHARSET);
    
    assertTrue(json.contains("\"metric\":\"sys.cpu.user\""));
    assertTrue(json.contains("\"tags\":{"));
    assertTrue(json.contains("\"dc\":\"phx\""));
    assertTrue(json.contains("\"host\":\"web01\""));
    assertTrue(json.contains("\"host\":\"web02\""));
    assertTrue(json.contains("\"dps\":{"));
    
    assertTrue(json.contains("\"1486045801\":42"));
    assertTrue(json.contains("\"1486045871\":9866.854"));
    assertTrue(json.contains("\"1486045881\":-128"));
    
    assertTrue(json.contains("\"1486045801\":8"));
    assertTrue(json.contains("\"1486045871\":\"NaN\""));
    assertTrue(json.contains("\"1486045881\":5000"));
    
    assertTrue(json.contains("\"1486045801\":5"));
    assertTrue(json.contains("\"1486045881\":2"));
    
    assertTrue(json.contains("\"1486045801\":20"));
    assertTrue(json.contains("\"1486045881\":13"));
  }
  
  @Test
  public void fullSerdesWithMilliseconds() throws Exception {
    final IteratorGroups results = new DefaultIteratorGroups();
    
    final TimeSeriesGroupId group_id_a = new SimpleStringGroupId("a");
    final TimeSeriesId id_a = BaseTimeSeriesId.newBuilder()
        .setMetric("sys.cpu.user")
        .addTags("host", "web01")
        .addTags("dc", "phx")
    .build();
    
    NumericMillisecondShard shard = 
        new NumericMillisecondShard(id_a, start, end);
    shard.add(1486045801000L, 42);
    shard.add(1486045871000L, 9866.854);
    shard.add(1486045881000L, -128);
    results.addIterator(group_id_a, shard);
    
    final TimeSeriesId id_b = BaseTimeSeriesId.newBuilder()
        .setMetric("sys.cpu.user")
        .addTags("host", "web02")
        .addTags("dc", "phx")
    .build();
    shard = new NumericMillisecondShard(id_b, start, end);
    shard.add(1486045801000L, 8);
    shard.add(1486045871000L, Double.NaN);
    shard.add(1486045881000L, 5000);
    results.addIterator(group_id_a, shard);
    
    final TimeSeriesGroupId group_id_b = new SimpleStringGroupId("b");
    shard = new NumericMillisecondShard(id_a, start, end);
    shard.add(1486045801000L, 5);
    shard.add(1486045871000L, Double.NaN);
    shard.add(1486045881000L, 2);
    results.addIterator(group_id_b, shard);
    
    shard = new NumericMillisecondShard(id_b, start, end);
    shard.add(1486045801000L, 20);
    shard.add(1486045871000L, Double.NaN);
    shard.add(1486045881000L, 13);
    results.addIterator(group_id_b, shard);
    
    final SerdesOptions conf = JsonV2QuerySerdesOptions.newBuilder()
        .setMsResolution(true)
        .build();
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    final JsonGenerator generator = JSON.getFactory().createGenerator(output);
    final JsonV2QuerySerdes serdes = new JsonV2QuerySerdes(generator);
    serdes.serialize(query, conf, output, results);
    output.close();
    final String json = new String(output.toByteArray(), Const.UTF8_CHARSET);
    
    assertTrue(json.contains("\"metric\":\"sys.cpu.user\""));
    assertTrue(json.contains("\"tags\":{"));
    assertTrue(json.contains("\"dc\":\"phx\""));
    assertTrue(json.contains("\"host\":\"web01\""));
    assertTrue(json.contains("\"host\":\"web02\""));
    assertTrue(json.contains("\"dps\":{"));
    
    assertTrue(json.contains("\"1486045801000\":42"));
    assertTrue(json.contains("\"1486045871000\":9866.854"));
    assertTrue(json.contains("\"1486045881000\":-128"));
    
    assertTrue(json.contains("\"1486045801000\":8"));
    assertTrue(json.contains("\"1486045871000\":\"NaN\""));
    assertTrue(json.contains("\"1486045881000\":5000"));
    
    assertTrue(json.contains("\"1486045801000\":5"));
    assertTrue(json.contains("\"1486045881000\":2"));
    
    assertTrue(json.contains("\"1486045801000\":20"));
    assertTrue(json.contains("\"1486045881000\":13"));
  }
  
  @Test
  public void serdesFilterOnTime() throws Exception {
    query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("1486045881000")
            .setEnd("1486046000000"))
        .build();
    
    final IteratorGroups results = new DefaultIteratorGroups();
    
    final TimeSeriesGroupId group_id_a = new SimpleStringGroupId("a");
    final TimeSeriesId id_a = BaseTimeSeriesId.newBuilder()
        .setMetric("sys.cpu.user")
        .addTags("host", "web01")
        .addTags("dc", "phx")
    .build();
    
    NumericMillisecondShard shard = 
        new NumericMillisecondShard(id_a, start, end);
    shard.add(1486045801000L, 42);
    shard.add(1486045871000L, 9866.854);
    shard.add(1486045881000L, -128);
    results.addIterator(group_id_a, shard);
    
    final TimeSeriesId id_b = BaseTimeSeriesId.newBuilder()
        .setMetric("sys.cpu.user")
        .addTags("host", "web02")
        .addTags("dc", "phx")
    .build();
    shard = new NumericMillisecondShard(id_b, start, end);
    shard.add(1486045801000L, 8);
    shard.add(1486045871000L, Double.NaN);
    shard.add(1486045881000L, 5000);
    results.addIterator(group_id_a, shard);
    
    final TimeSeriesGroupId group_id_b = new SimpleStringGroupId("b");
    shard = new NumericMillisecondShard(id_a, start, end);
    shard.add(1486045801000L, 5);
    shard.add(1486045871000L, Double.NaN);
    shard.add(1486045881000L, 2);
    results.addIterator(group_id_b, shard);
    
    shard = new NumericMillisecondShard(id_b, start, end);
    shard.add(1486045801000L, 20);
    shard.add(1486045871000L, Double.NaN);
    shard.add(1486045881000L, 13);
    results.addIterator(group_id_b, shard);
    
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    final JsonGenerator generator = JSON.getFactory().createGenerator(output);
    final JsonV2QuerySerdes serdes = new JsonV2QuerySerdes(generator);
    serdes.serialize(query, null, output, results);
    output.close();
    final String json = new String(output.toByteArray(), Const.UTF8_CHARSET);
    
    assertTrue(json.contains("\"metric\":\"sys.cpu.user\""));
    assertTrue(json.contains("\"tags\":{"));
    assertTrue(json.contains("\"dc\":\"phx\""));
    assertTrue(json.contains("\"host\":\"web01\""));
    assertTrue(json.contains("\"host\":\"web02\""));
    assertTrue(json.contains("\"dps\":{"));
    
    assertFalse(json.contains("\"1486045801\""));
    assertTrue(json.contains("\"1486045881\":-128"));
    assertTrue(json.contains("\"1486045881\":5000"));
    assertTrue(json.contains("\"1486045881\":2"));
    assertTrue(json.contains("\"1486045881\":13"));
  }
  
  @Test
  public void empty() throws Exception {
    final IteratorGroups results = new DefaultIteratorGroups();
    
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    final JsonGenerator generator = JSON.getFactory().createGenerator(output);
    final JsonV2QuerySerdes serdes = new JsonV2QuerySerdes(generator);
    serdes.serialize(query, null, output, results);
    output.close();
    final String json = new String(output.toByteArray(), Const.UTF8_CHARSET);
    assertEquals("", json);
  }

  @Test
  public void exceptions() throws Exception {
    final IteratorGroups results = new DefaultIteratorGroups();
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    final JsonGenerator generator = JSON.getFactory().createGenerator(output);
    
    try {
      new JsonV2QuerySerdes(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    final JsonV2QuerySerdes serdes = new JsonV2QuerySerdes(generator);
    
    try {
      serdes.serialize(query, null, null, results);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      serdes.serialize(query, null, output, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      serdes.deserialize(null, null);
      fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) { }
  }
}
