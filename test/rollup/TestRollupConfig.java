// This file is part of OpenTSDB.
// Copyright (C) 2015-2017  The OpenTSDB Authors.
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
package net.opentsdb.rollup;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.hbase.async.HBaseClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.utils.JSON;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ TSDB.class, HBaseClient.class })
public class TestRollupConfig {
  private final static String tsdb_table = "tsdb";
  private final static String rollup_table = "tsdb-rollup-10m";
  private final static String preagg_table = "tsdb-rollup-agg-10m";
  
  private TSDB tsdb;
  private HBaseClient client;
  private RollupConfig.Builder builder;
  private RollupInterval raw;
  private RollupInterval tenmin;
  
  @Before
  public void before() throws Exception {
    tsdb = PowerMockito.mock(TSDB.class);
    client = PowerMockito.mock(HBaseClient.class);
    when(tsdb.getClient()).thenReturn(client);
    
    raw = RollupInterval.builder()
        .setTable(tsdb_table)
        .setPreAggregationTable(tsdb_table)
        .setInterval("1m")
        .setRowSpan("1h")
        .setDefaultInterval(true)
        .build();
    
    tenmin = RollupInterval.builder()
        .setTable(rollup_table)
        .setPreAggregationTable(preagg_table)
        .setInterval("10m")
        .setRowSpan("1d")
        .build();
    
    builder = RollupConfig.builder()
        .addAggregationId("Sum", 0)
        .addAggregationId("Max", 1)
        .addInterval(raw)
        .addInterval(tenmin);
  }
  
  @Test
  public void ctor() throws Exception {
    RollupConfig config = builder.build();
    assertEquals(2, config.forward_intervals.size());
    assertSame(raw, config.forward_intervals.get("1m"));
    assertSame(tenmin, config.forward_intervals.get("10m"));
    
    assertEquals(3, config.reverse_intervals.size());
    assertSame(raw, config.reverse_intervals.get(tsdb_table));
    assertSame(tenmin, config.reverse_intervals.get(rollup_table));
    assertSame(tenmin, config.reverse_intervals.get(preagg_table));
    
    assertEquals(2, config.aggregations_to_ids.size());
    assertEquals(2, config.ids_to_aggregations.size());
    
    assertEquals(0, (int) config.aggregations_to_ids.get("sum"));
    assertEquals(1, (int) config.aggregations_to_ids.get("max"));
    
    assertEquals("sum", config.ids_to_aggregations.get(0));
    assertEquals("max", config.ids_to_aggregations.get(1));
    
    // missing aggregations
    builder = RollupConfig.builder()
        .addInterval(raw)
        .addInterval(tenmin);
    try {
      builder.build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // duplicate aggregation id
    builder = RollupConfig.builder()
        .addAggregationId("Sum", 1)
        .addAggregationId("Max", 1)
        .addInterval(raw)
        .addInterval(tenmin);
    try {
      builder.build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // invalid ID
    builder = RollupConfig.builder()
        .addAggregationId("Sum", 0)
        .addAggregationId("Max", 128)
        .addInterval(raw)
        .addInterval(tenmin);
    try {
      builder.build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // empty intervals
    builder = RollupConfig.builder()
        .addAggregationId("Sum", 0)
        .addAggregationId("Max", 1);
    try {
      builder.build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // dupe intervals
    builder = RollupConfig.builder()
        .addAggregationId("Sum", 0)
        .addAggregationId("Max", 1)
        .addInterval(raw)
        .addInterval(raw);
    try {
      builder.build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // two defaults
    tenmin = RollupInterval.builder()
        .setTable(rollup_table)
        .setPreAggregationTable(preagg_table)
        .setInterval("10m")
        .setRowSpan("1d")
        .setDefaultInterval(true)
        .build();
    builder = RollupConfig.builder()
        .addAggregationId("Sum", 0)
        .addAggregationId("Max", 1)
        .addInterval(raw)
        .addInterval(raw);
    try {
      builder.build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void getRollupIntervalString() throws Exception {
    final RollupConfig config = builder.build();
    
    assertSame(raw, config.getRollupInterval("1m"));
    assertSame(tenmin, config.getRollupInterval("10m"));
    
    try {
      config.getRollupInterval("5m");
      fail("Expected NoSuchRollupForIntervalException");
    } catch (NoSuchRollupForIntervalException e) { }
    
    try {
      config.getRollupInterval(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      config.getRollupInterval("");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void getRollupIntervalForTable() throws Exception {
    final RollupConfig config = builder.build();
    
    assertSame(raw, config.getRollupIntervalForTable(tsdb_table));
    assertSame(tenmin, config.getRollupIntervalForTable(rollup_table));
    assertSame(tenmin, config.getRollupIntervalForTable(preagg_table));
    
    try {
      config.getRollupIntervalForTable("nosuchtable");
      fail("Expected NoSuchRollupForTableException");
    } catch (NoSuchRollupForTableException e) { }
    
    try {
      config.getRollupIntervalForTable(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      config.getRollupIntervalForTable("");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void serdes() throws Exception {
    RollupConfig config = builder.build();
    String json = JSON.serializeToString(config);
    
    assertTrue(json.contains("\"intervals\":["));
    assertTrue(json.contains("\"interval\":\"1m\""));
    assertTrue(json.contains("interval\":\"10m\""));
    assertTrue(json.contains("\"aggregationIds\":{"));
    assertTrue(json.contains("\"sum\":0"));
    assertTrue(json.contains("\"max\":1"));
    
    json = "{\"intervals\":[{\"interval\":\"1m\",\"table\":\"tsdb\","
        + "\"preAggregationTable\":\"tsdb\",\"defaultInterval\":true,"
        + "\"rowSpan\":\"1h\"},{\"interval\":\"10m\",\"table\":"
        + "\"tsdb-rollup-10m\",\"preAggregationTable\":\"tsdb-rollup-agg-10m\","
        + "\"defaultInterval\":false,\"rowSpan\":\"1d\"}],\"aggregationIds\":"
        + "{\"sum\":0,\"max\":1}}";
    config = JSON.parseToObject(json, RollupConfig.class);
    assertEquals(2, config.forward_intervals.size());
    assertNotNull(config.forward_intervals.get("1m"));
    assertNotNull(config.forward_intervals.get("10m"));
    
    assertEquals(3, config.reverse_intervals.size());
    assertNotNull(config.reverse_intervals.get(tsdb_table));
    assertNotNull(config.reverse_intervals.get(rollup_table));
    assertNotNull(config.reverse_intervals.get(preagg_table));
    
    assertEquals(2, config.aggregations_to_ids.size());
    assertEquals(2, config.ids_to_aggregations.size());
    
    assertEquals(0, (int) config.aggregations_to_ids.get("sum"));
    assertEquals(1, (int) config.aggregations_to_ids.get("max"));
    
    assertEquals("sum", config.ids_to_aggregations.get(0));
    assertEquals("max", config.ids_to_aggregations.get(1));
  }

  @Test
  public void ensureTablesExist() throws Exception {
    when(client.ensureTableExists(any(byte[].class)))
      .thenAnswer(new Answer<Deferred<Object>>() {
      @Override
      public Deferred<Object> answer(InvocationOnMock invocation)
          throws Throwable {
        return Deferred.fromResult(null);
      }
    });
    
    final RollupConfig config = builder.build();
    config.ensureTablesExist(tsdb);
    verify(client, times(2)).ensureTableExists(tsdb_table.getBytes());
    verify(client, times(1)).ensureTableExists(rollup_table.getBytes());
    verify(client, times(1)).ensureTableExists(preagg_table.getBytes());
  }
}
