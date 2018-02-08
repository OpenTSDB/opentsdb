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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.nio.charset.Charset;

import org.hbase.async.Bytes;
import org.junit.Test;

import net.opentsdb.utils.JSON;

public class TestRollupInterval {
  private final static Charset CHARSET = Charset.forName("ISO-8859-1");
  private final static String rollup_table = "tsdb-rollup-10m";
  private final static String preagg_table = "tsdb-rollup-agg-10m";
  private final static byte[] table = rollup_table.getBytes(CHARSET);
  private final static byte[] agg_table = preagg_table.getBytes(CHARSET);
  
  @Test
  public void ctor1SecondHour() throws Exception {
    final RollupInterval interval = RollupInterval.builder()
        .setTable(rollup_table)
        .setPreAggregationTable(preagg_table)
        .setInterval("1s")
        .setRowSpan("1h")
        .build();
    assertEquals('h', interval.getUnits());
    assertEquals("1s", interval.getInterval());
    assertEquals('s', interval.getIntervalUnits());
    assertEquals(3600, interval.getIntervals());
    assertEquals(1, interval.getIntervalSeconds());
    assertEquals(rollup_table, interval.getTable());
    assertEquals(preagg_table, interval.getPreAggregationTable());
    assertEquals(0, Bytes.memcmp(table, interval.getTemporalTable()));
    assertEquals(0, Bytes.memcmp(agg_table, interval.getGroupbyTable()));
  }
  
  // test odd boundaries
  @Test
  public void ctor7SecondHour() throws Exception {
    final RollupInterval interval = RollupInterval.builder()
        .setTable(rollup_table)
        .setPreAggregationTable(preagg_table)
        .setInterval("7s")
        .setRowSpan("1h")
        .build();
    assertEquals('h', interval.getUnits());
    assertEquals("7s", interval.getInterval());
    assertEquals('s', interval.getIntervalUnits());
    assertEquals(514, interval.getIntervals());
    assertEquals(7, interval.getIntervalSeconds());
    assertEquals(rollup_table, interval.getTable());
    assertEquals(preagg_table, interval.getPreAggregationTable());
    assertEquals(0, Bytes.memcmp(table, interval.getTemporalTable()));
    assertEquals(0, Bytes.memcmp(agg_table, interval.getGroupbyTable()));
  }
  
  @Test
  public void ctor15SecondsHour() throws Exception {
    final RollupInterval interval = RollupInterval.builder()
        .setTable(rollup_table)
        .setPreAggregationTable(preagg_table)
        .setInterval("15s")
        .setRowSpan("1h")
        .build();
    assertEquals('h', interval.getUnits());
    assertEquals("15s", interval.getInterval());
    assertEquals('s', interval.getIntervalUnits());
    assertEquals(240, interval.getIntervals());
    assertEquals(15, interval.getIntervalSeconds());
    assertEquals(rollup_table, interval.getTable());
    assertEquals(preagg_table, interval.getPreAggregationTable());
    assertEquals(0, Bytes.memcmp(table, interval.getTemporalTable()));
    assertEquals(0, Bytes.memcmp(agg_table, interval.getGroupbyTable()));
  }
  
  @Test
  public void ctor30SecondsHour() throws Exception {
    final RollupInterval interval = RollupInterval.builder()
        .setTable(rollup_table)
        .setPreAggregationTable(preagg_table)
        .setInterval("30s")
        .setRowSpan("1h")
        .build();
    assertEquals('h', interval.getUnits());
    assertEquals("30s", interval.getInterval());
    assertEquals('s', interval.getIntervalUnits());
    assertEquals(120, interval.getIntervals());
    assertEquals(30, interval.getIntervalSeconds());
    assertEquals(rollup_table, interval.getTable());
    assertEquals(preagg_table, interval.getPreAggregationTable());
    assertEquals(0, Bytes.memcmp(table, interval.getTemporalTable()));
    assertEquals(0, Bytes.memcmp(agg_table, interval.getGroupbyTable()));
  }
  
  @Test
  public void ctor1MinuteDay() throws Exception {
    final RollupInterval interval = RollupInterval.builder()
        .setTable(rollup_table)
        .setPreAggregationTable(preagg_table)
        .setInterval("1m")
        .setRowSpan("1d")
        .build();
    assertEquals('d', interval.getUnits());
    assertEquals("1m", interval.getInterval());
    assertEquals('m', interval.getIntervalUnits());
    assertEquals(1440, interval.getIntervals());
    assertEquals(60, interval.getIntervalSeconds());
    assertEquals(rollup_table, interval.getTable());
    assertEquals(preagg_table, interval.getPreAggregationTable());
    assertEquals(0, Bytes.memcmp(table, interval.getTemporalTable()));
    assertEquals(0, Bytes.memcmp(agg_table, interval.getGroupbyTable()));
  }
  
  @Test
  public void ctor10MinuteDay() throws Exception {
    final RollupInterval interval = RollupInterval.builder()
        .setTable(rollup_table)
        .setPreAggregationTable(preagg_table)
        .setInterval("10m")
        .setRowSpan("1d")
        .build();
    assertEquals('d', interval.getUnits());
    assertEquals("10m", interval.getInterval());
    assertEquals('m', interval.getIntervalUnits());
    assertEquals(144, interval.getIntervals());
    assertEquals(600, interval.getIntervalSeconds());
    assertEquals(rollup_table, interval.getTable());
    assertEquals(preagg_table, interval.getPreAggregationTable());
    assertEquals(0, Bytes.memcmp(table, interval.getTemporalTable()));
    assertEquals(0, Bytes.memcmp(agg_table, interval.getGroupbyTable()));
  }
  
  @Test
  public void ctor10Minute6Hours() throws Exception {
    final RollupInterval interval = RollupInterval.builder()
        .setTable(rollup_table)
        .setPreAggregationTable(preagg_table)
        .setInterval("10m")
        .setRowSpan("6h")
        .build();
    assertEquals('h', interval.getUnits());
    assertEquals("10m", interval.getInterval());
    assertEquals('m', interval.getIntervalUnits());
    assertEquals(36, interval.getIntervals());
    assertEquals(600, interval.getIntervalSeconds());
    assertEquals(rollup_table, interval.getTable());
    assertEquals(preagg_table, interval.getPreAggregationTable());
    assertEquals(0, Bytes.memcmp(table, interval.getTemporalTable()));
    assertEquals(0, Bytes.memcmp(agg_table, interval.getGroupbyTable()));
  }
  
  @Test
  public void ctor10Minute12Hours() throws Exception {
    final RollupInterval interval = RollupInterval.builder()
        .setTable(rollup_table)
        .setPreAggregationTable(preagg_table)
        .setInterval("10m")
        .setRowSpan("12h")
        .build();
    assertEquals('h', interval.getUnits());
    assertEquals("10m", interval.getInterval());
    assertEquals('m', interval.getIntervalUnits());
    assertEquals(72, interval.getIntervals());
    assertEquals(600, interval.getIntervalSeconds());
    assertEquals(rollup_table, interval.getTable());
    assertEquals(preagg_table, interval.getPreAggregationTable());
    assertEquals(0, Bytes.memcmp(table, interval.getTemporalTable()));
    assertEquals(0, Bytes.memcmp(agg_table, interval.getGroupbyTable()));
  }
  
  @Test
  public void ctor15MinuteDay() throws Exception {
    final RollupInterval interval = RollupInterval.builder()
        .setTable(rollup_table)
        .setPreAggregationTable(preagg_table)
        .setInterval("15m")
        .setRowSpan("1d")
        .build();
    assertEquals('d', interval.getUnits());
    assertEquals("15m", interval.getInterval());
    assertEquals('m', interval.getIntervalUnits());
    assertEquals(96, interval.getIntervals());
    assertEquals(900, interval.getIntervalSeconds());
    assertEquals(rollup_table, interval.getTable());
    assertEquals(preagg_table, interval.getPreAggregationTable());
    assertEquals(0, Bytes.memcmp(table, interval.getTemporalTable()));
    assertEquals(0, Bytes.memcmp(agg_table, interval.getGroupbyTable()));
  }
  
  @Test
  public void ctor30MinuteDay() throws Exception {
    final RollupInterval interval = RollupInterval.builder()
        .setTable(rollup_table)
        .setPreAggregationTable(preagg_table)
        .setInterval("30m")
        .setRowSpan("1d")
        .build();
    assertEquals('d', interval.getUnits());
    assertEquals("30m", interval.getInterval());
    assertEquals('m', interval.getIntervalUnits());
    assertEquals(48, interval.getIntervals());
    assertEquals(1800, interval.getIntervalSeconds());
    assertEquals(rollup_table, interval.getTable());
    assertEquals(preagg_table, interval.getPreAggregationTable());
    assertEquals(0, Bytes.memcmp(table, interval.getTemporalTable()));
    assertEquals(0, Bytes.memcmp(agg_table, interval.getGroupbyTable()));
  }
  
  @Test
  public void ctor1HourDay() throws Exception {
    final RollupInterval interval = RollupInterval.builder()
        .setTable(rollup_table)
        .setPreAggregationTable(preagg_table)
        .setInterval("1h")
        .setRowSpan("1d")
        .build();
    assertEquals('d', interval.getUnits());
    assertEquals("1h", interval.getInterval());
    assertEquals('h', interval.getIntervalUnits());
    assertEquals(24, interval.getIntervals());
    assertEquals(3600, interval.getIntervalSeconds());
    assertEquals(rollup_table, interval.getTable());
    assertEquals(preagg_table, interval.getPreAggregationTable());
    assertEquals(0, Bytes.memcmp(table, interval.getTemporalTable()));
    assertEquals(0, Bytes.memcmp(agg_table, interval.getGroupbyTable()));
  }
  
  @Test
  public void ctor1HourMonth() throws Exception {
    final RollupInterval interval = RollupInterval.builder()
        .setTable(rollup_table)
        .setPreAggregationTable(preagg_table)
        .setInterval("1h")
        .setRowSpan("1n")
        .build();
    assertEquals('n', interval.getUnits());
    assertEquals("1h", interval.getInterval());
    assertEquals('h', interval.getIntervalUnits());
    assertEquals(768, interval.getIntervals());
    assertEquals(3600, interval.getIntervalSeconds());
    assertEquals(rollup_table, interval.getTable());
    assertEquals(preagg_table, interval.getPreAggregationTable());
    assertEquals(0, Bytes.memcmp(table, interval.getTemporalTable()));
    assertEquals(0, Bytes.memcmp(agg_table, interval.getGroupbyTable()));
  }
  
  @Test
  public void ctor3HourMonth() throws Exception {
    final RollupInterval interval = RollupInterval.builder()
        .setTable(rollup_table)
        .setPreAggregationTable(preagg_table)
        .setInterval("3h")
        .setRowSpan("1n")
        .build();
    assertEquals('n', interval.getUnits());
    assertEquals("3h", interval.getInterval());
    assertEquals('h', interval.getIntervalUnits());
    assertEquals(256, interval.getIntervals());
    assertEquals(10800, interval.getIntervalSeconds());
    assertEquals(rollup_table, interval.getTable());
    assertEquals(preagg_table, interval.getPreAggregationTable());
    assertEquals(0, Bytes.memcmp(table, interval.getTemporalTable()));
    assertEquals(0, Bytes.memcmp(agg_table, interval.getGroupbyTable()));
  }
  
  @Test
  public void ctor6HourMonth() throws Exception {
    final RollupInterval interval = RollupInterval.builder()
        .setTable(rollup_table)
        .setPreAggregationTable(preagg_table)
        .setInterval("6h")
        .setRowSpan("1n")
        .build();
    assertEquals('n', interval.getUnits());
    assertEquals("6h", interval.getInterval());
    assertEquals('h', interval.getIntervalUnits());
    assertEquals(128, interval.getIntervals());
    assertEquals(21600, interval.getIntervalSeconds());
    assertEquals(rollup_table, interval.getTable());
    assertEquals(preagg_table, interval.getPreAggregationTable());
    assertEquals(0, Bytes.memcmp(table, interval.getTemporalTable()));
    assertEquals(0, Bytes.memcmp(agg_table, interval.getGroupbyTable()));
  }
  
  @Test
  public void ctor6HourYear() throws Exception {
    final RollupInterval interval = RollupInterval.builder()
        .setTable(rollup_table)
        .setPreAggregationTable(preagg_table)
        .setInterval("6h")
        .setRowSpan("1y")
        .build();
    assertEquals('y', interval.getUnits());
    assertEquals("6h", interval.getInterval());
    assertEquals('h', interval.getIntervalUnits());
    assertEquals(1464, interval.getIntervals());
    assertEquals(21600, interval.getIntervalSeconds());
    assertEquals(rollup_table, interval.getTable());
    assertEquals(preagg_table, interval.getPreAggregationTable());
    assertEquals(0, Bytes.memcmp(table, interval.getTemporalTable()));
    assertEquals(0, Bytes.memcmp(agg_table, interval.getGroupbyTable()));
  }
  
  @Test
  public void ctor12HourYear() throws Exception {
    final RollupInterval interval = RollupInterval.builder()
        .setTable(rollup_table)
        .setPreAggregationTable(preagg_table)
        .setInterval("12h")
        .setRowSpan("1y")
        .build();
    assertEquals('y', interval.getUnits());
    assertEquals("12h", interval.getInterval());
    assertEquals('h', interval.getIntervalUnits());
    assertEquals(732, interval.getIntervals());
    assertEquals(43200, interval.getIntervalSeconds());
    assertEquals(rollup_table, interval.getTable());
    assertEquals(preagg_table, interval.getPreAggregationTable());
    assertEquals(0, Bytes.memcmp(table, interval.getTemporalTable()));
    assertEquals(0, Bytes.memcmp(agg_table, interval.getGroupbyTable()));
  }
  
  @Test
  public void ctor1DayYear() throws Exception {
    final RollupInterval interval = RollupInterval.builder()
        .setTable(rollup_table)
        .setPreAggregationTable(preagg_table)
        .setInterval("1d")
        .setRowSpan("1y")
        .build();
    assertEquals('y', interval.getUnits());
    assertEquals("1d", interval.getInterval());
    assertEquals('d', interval.getIntervalUnits());
    assertEquals(366, interval.getIntervals());
    assertEquals(86400, interval.getIntervalSeconds());
    assertEquals(rollup_table, interval.getTable());
    assertEquals(preagg_table, interval.getPreAggregationTable());
    assertEquals(0, Bytes.memcmp(table, interval.getTemporalTable()));
    assertEquals(0, Bytes.memcmp(agg_table, interval.getGroupbyTable()));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorUnknownNullRollupTable() throws Exception {
    RollupInterval.builder()
        .setTable(null)
        .setPreAggregationTable(preagg_table)
        .setInterval("1h")
        .setRowSpan("1d")
        .build();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorUnknownEmptyRollupTable() throws Exception {
    RollupInterval.builder()
    .setTable("")
    .setPreAggregationTable(preagg_table)
    .setInterval("1h")
    .setRowSpan("1d")
    .build();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorUnknownNullPreAggTable() throws Exception {
    RollupInterval.builder()
    .setTable(rollup_table)
    .setPreAggregationTable(null)
    .setInterval("1h")
    .setRowSpan("1d")
    .build();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorUnknownEmptyPreAggTable() throws Exception {
    RollupInterval.builder()
    .setTable(rollup_table)
    .setPreAggregationTable("")
    .setInterval("1h")
    .setRowSpan("1d")
    .build();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorUnknownSpan() throws Exception {
    RollupInterval.builder()
    .setTable(rollup_table)
    .setPreAggregationTable(preagg_table)
    .setInterval("1h")
    .setRowSpan("1s")
    .build();
  }
  
  @Test (expected = NullPointerException.class)
  public void ctorNullInterval() throws Exception {
    RollupInterval.builder()
    .setTable(rollup_table)
    .setPreAggregationTable(preagg_table)
    .setInterval(null)
    .setRowSpan("1d")
    .build();
  }
  
  @Test (expected = StringIndexOutOfBoundsException.class)
  public void ctorEmptyInterval() throws Exception {
    RollupInterval.builder()
    .setTable(rollup_table)
    .setPreAggregationTable(preagg_table)
    .setInterval("")
    .setRowSpan("1d")
    .build();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorBigDuration() throws Exception {
    RollupInterval.builder()
    .setTable(rollup_table)
    .setPreAggregationTable(preagg_table)
    .setInterval("365y")
    .setRowSpan("1d")
    .build();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorTooManyIntervals() throws Exception {
    RollupInterval.builder()
    .setTable(rollup_table)
    .setPreAggregationTable(preagg_table)
    .setInterval("1s")
    .setRowSpan("17")
    .build();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorDurationTooBigForSpan() throws Exception {
    RollupInterval.builder()
    .setTable(rollup_table)
    .setPreAggregationTable(preagg_table)
    .setInterval("36500s")
    .setRowSpan("1h")
    .build();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorDurationEqualToSpan() throws Exception {
    RollupInterval.builder()
    .setTable(rollup_table)
    .setPreAggregationTable(preagg_table)
    .setInterval("3600s")
    .setRowSpan("1h")
    .build();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorTooFewIntervals() throws Exception {
    RollupInterval.builder()
    .setTable(rollup_table)
    .setPreAggregationTable(preagg_table)
    .setInterval("3000s")
    .setRowSpan("1h")
    .build();
  }

  @Test (expected = IllegalArgumentException.class)
  public void ctorNoUnitsInSpan() throws Exception {
    RollupInterval.builder()
    .setTable(rollup_table)
    .setPreAggregationTable(preagg_table)
    .setInterval("365y")
    .setRowSpan("1")
    .build();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorNoIntervalInSpan() throws Exception {
    RollupInterval.builder()
    .setTable(rollup_table)
    .setPreAggregationTable(preagg_table)
    .setInterval("365y")
    .setRowSpan("d")
    .build();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorNoMs() throws Exception {
    RollupInterval.builder()
    .setTable(rollup_table)
    .setPreAggregationTable(preagg_table)
    .setInterval("365y")
    .setRowSpan("1000ms")
    .build();
  }

  @Test (expected = IllegalArgumentException.class)
  public void ctor15Minute7Days() throws Exception {
    RollupInterval.builder()
    .setTable(rollup_table)
    .setPreAggregationTable(preagg_table)
    .setInterval("15m")
    .setRowSpan("7d")
    .build();
  }

  @Test
  public void serdes() throws Exception {
    RollupInterval interval = RollupInterval.builder()
        .setTable(rollup_table)
        .setPreAggregationTable(preagg_table)
        .setInterval("1s")
        .setRowSpan("1h")
        .setDefaultInterval(true)
        .build();
    
    String json = JSON.serializeToString(interval);
    assertTrue(json.contains("\"interval\":\"1s\""));
    assertTrue(json.contains("\"table\":\"tsdb-rollup-10m\""));
    assertTrue(json.contains("\"defaultInterval\":true"));
    assertTrue(json.contains("\"rowSpan\":\"1h\""));
    assertTrue(json.contains("\"preAggregationTable\":\"tsdb-rollup-agg-10m\""));
    
    json = "{\"interval\":\"1s\",\"table\":\"tsdb-rollup-10m\","
        + "\"defaultRollupInterval\":true,\"rowSpan\":\"1h\","
        + "\"preAggregationTable\":\"tsdb-rollup-agg-10m\"}";
    interval = JSON.parseToObject(json, RollupInterval.class);
    assertEquals('h', interval.getUnits());
    assertEquals("1s", interval.getInterval());
    assertEquals('s', interval.getIntervalUnits());
    assertEquals(3600, interval.getIntervals());
    assertEquals(1, interval.getIntervalSeconds());
    assertEquals(rollup_table, interval.getTable());
    assertEquals(preagg_table, interval.getPreAggregationTable());
    assertEquals(0, Bytes.memcmp(table, interval.getTemporalTable()));
    assertEquals(0, Bytes.memcmp(agg_table, interval.getGroupbyTable()));
  }
  
  @Test
  public void testHashCodeAndEquals() throws Exception {
    final RollupInterval interval_a = RollupInterval.builder()
        .setTable(rollup_table)
        .setPreAggregationTable(preagg_table)
        .setInterval("1s")
        .setRowSpan("1h")
        .setDefaultInterval(true)
        .build();
    RollupInterval interval_b = RollupInterval.builder()
        .setTable(rollup_table)
        .setPreAggregationTable(preagg_table)
        .setInterval("1s")
        .setRowSpan("1h")
        .setDefaultInterval(true)
        .build();
    assertEquals(interval_a.hashCode(), interval_b.hashCode());
    assertEquals(interval_a, interval_b);
    
    interval_b = RollupInterval.builder()
        .setTable("nothertable")  // <-- DIFF
        .setPreAggregationTable(preagg_table)
        .setInterval("1s")
        .setRowSpan("1h")
        .setDefaultInterval(true)
        .build();
    assertNotEquals(interval_a.hashCode(), interval_b.hashCode());
    assertNotEquals(interval_a, interval_b);
    
    interval_b = RollupInterval.builder()
        .setTable(rollup_table)
        .setPreAggregationTable("nothertable")  // <-- DIFF
        .setInterval("1s")
        .setRowSpan("1h")
        .setDefaultInterval(true)
        .build();
    assertNotEquals(interval_a.hashCode(), interval_b.hashCode());
    assertNotEquals(interval_a, interval_b);
    
    interval_b = RollupInterval.builder()
        .setTable(rollup_table)
        .setPreAggregationTable(preagg_table)
        .setInterval("30s")  // <-- DIFF
        .setRowSpan("1h")
        .setDefaultInterval(true)
        .build();
    assertNotEquals(interval_a.hashCode(), interval_b.hashCode());
    assertNotEquals(interval_a, interval_b);
    
    interval_b = RollupInterval.builder()
        .setTable(rollup_table)
        .setPreAggregationTable(preagg_table)
        .setInterval("1s")
        .setRowSpan("2h")  // <-- DIFF
        .setDefaultInterval(true)
        .build();
    assertNotEquals(interval_a.hashCode(), interval_b.hashCode());
    assertNotEquals(interval_a, interval_b);
    
    interval_b = RollupInterval.builder()
        .setTable(rollup_table)
        .setPreAggregationTable(preagg_table)
        .setInterval("1s")
        .setRowSpan("1h")
        //.setIsDefault(true)  // <-- DIFF
        .build();
    assertNotEquals(interval_a.hashCode(), interval_b.hashCode());
    assertNotEquals(interval_a, interval_b);
  }
}
