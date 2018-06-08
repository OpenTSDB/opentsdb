// This file is part of OpenTSDB.
// Copyright (C) 2015-2018 The OpenTSDB Authors.
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
package net.opentsdb.rollup;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import net.opentsdb.core.TSDB;
import net.opentsdb.exceptions.IllegalDataException;
import net.opentsdb.utils.JSON;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ TSDB.class })
public class TestRollupConfig {
  private final static String tsdb_table = "tsdb";
  private final static String rollup_table = "tsdb-rollup-10m";
  private final static String preagg_table = "tsdb-rollup-agg-10m";
  
  private DefaultRollupConfig.Builder builder;
  private RollupInterval raw;
  private RollupInterval tenmin;
  
  @Before
  public void before() throws Exception {
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
    
    builder = DefaultRollupConfig.builder()
        .addAggregationId("Sum", 0)
        .addAggregationId("Max", 1)
        .addAggregationId("count", 2)
        .addAggregationId("min", 3)
        .addInterval(raw)
        .addInterval(tenmin);
  }
  
  @Test
  public void ctor() throws Exception {
    DefaultRollupConfig config = builder.build();
    assertEquals(2, config.forward_intervals.size());
    assertSame(raw, config.forward_intervals.get("1m"));
    assertSame(config, config.forward_intervals.get("1m").rollupConfig());
    assertSame(tenmin, config.forward_intervals.get("10m"));
    assertSame(config, config.forward_intervals.get("10m").rollupConfig());
    
    assertEquals(3, config.reverse_intervals.size());
    assertSame(raw, config.reverse_intervals.get(tsdb_table));
    assertSame(tenmin, config.reverse_intervals.get(rollup_table));
    assertSame(tenmin, config.reverse_intervals.get(preagg_table));
    
    assertEquals(4, config.aggregations_to_ids.size());
    assertEquals(4, config.ids_to_aggregations.size());
    
    assertEquals(0, (int) config.aggregations_to_ids.get("sum"));
    assertEquals(1, (int) config.aggregations_to_ids.get("max"));
    assertEquals(2, (int) config.aggregations_to_ids.get("count"));
    assertEquals(3, (int) config.aggregations_to_ids.get("min"));
    
    assertEquals("sum", config.ids_to_aggregations.get(0));
    assertEquals("max", config.ids_to_aggregations.get(1));
    assertEquals("count", config.ids_to_aggregations.get(2));
    assertEquals("min", config.ids_to_aggregations.get(3));
    
    // missing aggregations
    builder = DefaultRollupConfig.builder()
        .addInterval(raw)
        .addInterval(tenmin);
    try {
      builder.build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // duplicate aggregation id
    builder = DefaultRollupConfig.builder()
        .addAggregationId("Sum", 1)
        .addAggregationId("Max", 1)
        .addInterval(raw)
        .addInterval(tenmin);
    try {
      builder.build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // invalid ID
    builder = DefaultRollupConfig.builder()
        .addAggregationId("Sum", 0)
        .addAggregationId("Max", 128)
        .addInterval(raw)
        .addInterval(tenmin);
    try {
      builder.build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // empty intervals
    builder = DefaultRollupConfig.builder()
        .addAggregationId("Sum", 0)
        .addAggregationId("Max", 1);
    try {
      builder.build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // dupe intervals
    builder = DefaultRollupConfig.builder()
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
    builder = DefaultRollupConfig.builder()
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
    final DefaultRollupConfig config = builder.build();
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
    final DefaultRollupConfig config = builder.build();
    
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
  public void getRollupIntervals() throws Exception {
    DefaultRollupConfig config = builder.build();
    
    List<RollupInterval> intervals = config.getRollupIntervals(60, "1m");
    assertEquals(1, intervals.size());
    assertSame(raw, intervals.get(0));
    
    intervals = config.getRollupIntervals(600, "10m");
    assertEquals(2, intervals.size());
    assertSame(tenmin, intervals.get(0));
    assertSame(raw, intervals.get(1));
    
    intervals = config.getRollupIntervals(1200, "20m");
    assertEquals(2, intervals.size());
    assertSame(tenmin, intervals.get(0));
    assertSame(raw, intervals.get(1));
    
    intervals = config.getRollupIntervals(720, "12m");
    assertEquals(1, intervals.size());
    assertSame(raw, intervals.get(0));
    
    try {
      config.getRollupIntervals(1, "1s");
      fail("Expected NoSuchRollupForIntervalException");
    } catch (NoSuchRollupForIntervalException e) { }
    
    intervals = config.getRollupIntervals(60, "1m", true);
    assertEquals(0, intervals.size());
    
    intervals = config.getRollupIntervals(600, "10m", true);
    assertEquals(1, intervals.size());
    assertSame(tenmin, intervals.get(0));
    
    intervals = config.getRollupIntervals(1200, "20m", true);
    assertEquals(1, intervals.size());
    assertSame(tenmin, intervals.get(0));
    
    intervals = config.getRollupIntervals(720, "12m", true);
    assertEquals(0, intervals.size());
    
    // str_interval is used for logging
    intervals = config.getRollupIntervals(60, null);
    assertEquals(1, intervals.size());
    assertSame(raw, intervals.get(0));
    
    intervals = config.getRollupIntervals(60, "");
    assertEquals(1, intervals.size());
    assertSame(raw, intervals.get(0));
    
    try {
      config.getRollupIntervals(0, "1m");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void serdes() throws Exception {
    DefaultRollupConfig config = builder.build();
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
    config = JSON.parseToObject(json, DefaultRollupConfig.class);
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
  public void getIdForAggregatorString() throws Exception {
    DefaultRollupConfig config = builder.build();
    assertEquals(0, config.getIdForAggregator("sum"));
    assertEquals(0, config.getIdForAggregator("ZimSum"));
    assertEquals(1, config.getIdForAggregator("max"));
    assertEquals(1, config.getIdForAggregator("MimMax"));
    assertEquals(2, config.getIdForAggregator("Count"));
    assertEquals(3, config.getIdForAggregator("min"));
    try {
      config.getIdForAggregator("avg");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    try {
      config.getIdForAggregator((String) null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    try {
      config.getIdForAggregator("");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void getIdForAggregatorQualifier() throws Exception {
    DefaultRollupConfig config = builder.build();
    assertEquals(0, config.getIdForAggregator(new byte[] { 's', 'u', 'm', ':', 0, 0 }));
    assertEquals(0, config.getIdForAggregator(new byte[] { 'S', 'U', 'M', ':', 0, 0 }));
    assertEquals(1, config.getIdForAggregator(new byte[] { 'm', 'a', 'x', ':', 0, 0 }));
    assertEquals(1, config.getIdForAggregator(new byte[] { 'M', 'A', 'X', ':', 0, 0 }));
    assertEquals(3, config.getIdForAggregator(new byte[] { 'm', 'i', 'n', ':', 0, 0 }));
    assertEquals(3, config.getIdForAggregator(new byte[] { 'M', 'I', 'N', ':', 0, 0 }));
    assertEquals(2, config.getIdForAggregator(new byte[] { 'c', 'o', 'u', 'n', 't', ':', 0, 0 }));
    assertEquals(2, config.getIdForAggregator(new byte[] { 'C', 'O', 'U', 'N', 'T', ':', 0, 0 }));
    try {
      config.getIdForAggregator(new byte[] { 's', 'u', 'm' });
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    try {
      config.getIdForAggregator(new byte[] { });
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    try {
      config.getIdForAggregator((byte[]) null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    try {
      config.getIdForAggregator(new byte[] { 'a', 'v', 'g', ':', 0, 0 });
      fail("Expected IllegalDataException");
    } catch (IllegalDataException e) { }
  }
  
  @Test
  public void getOffsetStartFromQualifier() throws Exception {
    DefaultRollupConfig config = builder.build();
    assertEquals(4, config.getOffsetStartFromQualifier(new byte[] { 's', 'u', 'm', ':', 0, 0 }));
    assertEquals(4, config.getOffsetStartFromQualifier(new byte[] { 'S', 'U', 'M', ':', 0, 0 }));
    assertEquals(4, config.getOffsetStartFromQualifier(new byte[] { 'm', 'a', 'x', ':', 0, 0 }));
    assertEquals(4, config.getOffsetStartFromQualifier(new byte[] { 'M', 'A', 'X', ':', 0, 0 }));
    assertEquals(4, config.getOffsetStartFromQualifier(new byte[] { 'm', 'i', 'n', ':', 0, 0 }));
    assertEquals(4, config.getOffsetStartFromQualifier(new byte[] { 'M', 'I', 'N', ':', 0, 0 }));
    // meh, shouldn't matter if this sneaks through.
    assertEquals(4, config.getOffsetStartFromQualifier(new byte[] { 'M', 'u', 'm', ':', 0, 0 }));
    assertEquals(6, config.getOffsetStartFromQualifier(new byte[] { 'c', 'o', 'u', 'n', 't', ':', 0, 0 }));
    assertEquals(6, config.getOffsetStartFromQualifier(new byte[] { 'C', 'O', 'U', 'N', 'T', ':', 0, 0 }));
    try {
      config.getOffsetStartFromQualifier(new byte[] { 's', 'u', 'm' });
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    try {
      config.getOffsetStartFromQualifier(new byte[] { });
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    try {
      config.getOffsetStartFromQualifier(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    try {
      config.getOffsetStartFromQualifier(new byte[] { 'a', 'v', 'g', ':', 0, 0 });
      fail("Expected IllegalDataException");
    } catch (IllegalDataException e) { }
  }
  
  @Test
  public void queryToRollupAggregation() throws Exception {
    assertEquals("sum", DefaultRollupConfig.queryToRollupAggregation("ZimSum"));
    assertEquals("sum", DefaultRollupConfig.queryToRollupAggregation("sum"));
    assertEquals("max", DefaultRollupConfig.queryToRollupAggregation("MimMax"));
    assertEquals("min", DefaultRollupConfig.queryToRollupAggregation("MimMin"));
    assertEquals("avg", DefaultRollupConfig.queryToRollupAggregation("Avg"));
    assertEquals("", DefaultRollupConfig.queryToRollupAggregation(""));
    try {
      DefaultRollupConfig.queryToRollupAggregation(null);
      fail("Expected NullPointerException");
    } catch (NullPointerException e) { }
  }
}
