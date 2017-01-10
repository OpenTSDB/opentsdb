// This file is part of OpenTSDB.
// Copyright (C) 2010-2015  The OpenTSDB Authors.
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.charset.Charset;

import org.hbase.async.Bytes;
import org.junit.Test;

public class TestRollupInterval {
  private final static Charset CHARSET = Charset.forName("ISO-8859-1");
  private final static String rollup_table = "tsdb-rollup-10m";
  private final static String preagg_table = "tsdb-rollup-agg-10m";
  private final static byte[] table = rollup_table.getBytes(CHARSET);
  private final static byte[] agg_table = preagg_table.getBytes(CHARSET);
  
  @Test
  public void ctor1SecondHour() throws Exception {
    final RollupInterval interval = new RollupInterval(
        rollup_table, preagg_table, "1s", "1h");
    assertEquals('h', interval.getUnits());
    assertEquals("1s", interval.getStringInterval());
    assertEquals('s', interval.getIntervalUnits());
    assertEquals(3600, interval.getIntervals());
    assertEquals(1, interval.getInterval());
    assertEquals(rollup_table, interval.getTemporalTableName());
    assertEquals(preagg_table, interval.getGroupbyTableName());
    assertEquals(0, Bytes.memcmp(table, interval.getTemporalTable()));
    assertEquals(0, Bytes.memcmp(agg_table, interval.getGroupbyTable()));
  }
  
  // test odd boundaries
  @Test
  public void ctor7SecondHour() throws Exception {
    final RollupInterval interval = new RollupInterval(
        rollup_table, preagg_table, "7s", "1h");
    assertEquals('h', interval.getUnits());
    assertEquals("7s", interval.getStringInterval());
    assertEquals('s', interval.getIntervalUnits());
    assertEquals(514, interval.getIntervals());
    assertEquals(7, interval.getInterval());
    assertEquals(rollup_table, interval.getTemporalTableName());
    assertEquals(preagg_table, interval.getGroupbyTableName());
    assertEquals(0, Bytes.memcmp(table, interval.getTemporalTable()));
    assertEquals(0, Bytes.memcmp(agg_table, interval.getGroupbyTable()));
  }
  
  @Test
  public void ctor15SecondsHour() throws Exception {
    final RollupInterval interval = new RollupInterval(
        rollup_table, preagg_table, "15s", "1h");
    assertEquals('h', interval.getUnits());
    assertEquals("15s", interval.getStringInterval());
    assertEquals('s', interval.getIntervalUnits());
    assertEquals(240, interval.getIntervals());
    assertEquals(15, interval.getInterval());
    assertEquals(rollup_table, interval.getTemporalTableName());
    assertEquals(preagg_table, interval.getGroupbyTableName());
    assertEquals(0, Bytes.memcmp(table, interval.getTemporalTable()));
    assertEquals(0, Bytes.memcmp(agg_table, interval.getGroupbyTable()));
  }
  
  @Test
  public void ctor30SecondsHour() throws Exception {
    final RollupInterval interval = new RollupInterval(
        rollup_table, preagg_table, "30s", "1h");
    assertEquals('h', interval.getUnits());
    assertEquals("30s", interval.getStringInterval());
    assertEquals('s', interval.getIntervalUnits());
    assertEquals(120, interval.getIntervals());
    assertEquals(30, interval.getInterval());
    assertEquals(rollup_table, interval.getTemporalTableName());
    assertEquals(preagg_table, interval.getGroupbyTableName());
    assertEquals(0, Bytes.memcmp(table, interval.getTemporalTable()));
    assertEquals(0, Bytes.memcmp(agg_table, interval.getGroupbyTable()));
  }
  
  @Test
  public void ctor1MinuteDay() throws Exception {
    final RollupInterval interval = new RollupInterval(
        rollup_table, preagg_table, "1m", "1d");
    assertEquals('d', interval.getUnits());
    assertEquals("1m", interval.getStringInterval());
    assertEquals('m', interval.getIntervalUnits());
    assertEquals(1440, interval.getIntervals());
    assertEquals(60, interval.getInterval());
    assertEquals(rollup_table, interval.getTemporalTableName());
    assertEquals(preagg_table, interval.getGroupbyTableName());
    assertEquals(0, Bytes.memcmp(table, interval.getTemporalTable()));
    assertEquals(0, Bytes.memcmp(agg_table, interval.getGroupbyTable()));
  }
  
  @Test
  public void ctor10MinuteDay() throws Exception {
    final RollupInterval interval = new RollupInterval(
        rollup_table, preagg_table, "10m", "1d");
    assertEquals('d', interval.getUnits());
    assertEquals("10m", interval.getStringInterval());
    assertEquals('m', interval.getIntervalUnits());
    assertEquals(144, interval.getIntervals());
    assertEquals(600, interval.getInterval());
    assertEquals(rollup_table, interval.getTemporalTableName());
    assertEquals(preagg_table, interval.getGroupbyTableName());
    assertEquals(0, Bytes.memcmp(table, interval.getTemporalTable()));
    assertEquals(0, Bytes.memcmp(agg_table, interval.getGroupbyTable()));
  }
  
  @Test
  public void ctor10Minute6Hours() throws Exception {
    final RollupInterval interval = new RollupInterval(
        rollup_table, preagg_table, "10m", "6h");
    assertEquals('h', interval.getUnits());
    assertEquals("10m", interval.getStringInterval());
    assertEquals('m', interval.getIntervalUnits());
    assertEquals(36, interval.getIntervals());
    assertEquals(600, interval.getInterval());
    assertEquals(rollup_table, interval.getTemporalTableName());
    assertEquals(preagg_table, interval.getGroupbyTableName());
    assertEquals(0, Bytes.memcmp(table, interval.getTemporalTable()));
    assertEquals(0, Bytes.memcmp(agg_table, interval.getGroupbyTable()));
  }
  
  @Test
  public void ctor10Minute12Hours() throws Exception {
    final RollupInterval interval = new RollupInterval(
        rollup_table, preagg_table, "10m", "12h");
    assertEquals('h', interval.getUnits());
    assertEquals("10m", interval.getStringInterval());
    assertEquals('m', interval.getIntervalUnits());
    assertEquals(72, interval.getIntervals());
    assertEquals(600, interval.getInterval());
    assertEquals(rollup_table, interval.getTemporalTableName());
    assertEquals(preagg_table, interval.getGroupbyTableName());
    assertEquals(0, Bytes.memcmp(table, interval.getTemporalTable()));
    assertEquals(0, Bytes.memcmp(agg_table, interval.getGroupbyTable()));
  }
  
  @Test
  public void ctor15MinuteDay() throws Exception {
    final RollupInterval interval = new RollupInterval(
        rollup_table, preagg_table, "15m", "1d");
    assertEquals('d', interval.getUnits());
    assertEquals("15m", interval.getStringInterval());
    assertEquals('m', interval.getIntervalUnits());
    assertEquals(96, interval.getIntervals());
    assertEquals(900, interval.getInterval());
    assertEquals(rollup_table, interval.getTemporalTableName());
    assertEquals(preagg_table, interval.getGroupbyTableName());
    assertEquals(0, Bytes.memcmp(table, interval.getTemporalTable()));
    assertEquals(0, Bytes.memcmp(agg_table, interval.getGroupbyTable()));
  }
  
  @Test
  public void ctor30MinuteDay() throws Exception {
    final RollupInterval interval = new RollupInterval(
        rollup_table, preagg_table, "30m", "1d");
    assertEquals('d', interval.getUnits());
    assertEquals("30m", interval.getStringInterval());
    assertEquals('m', interval.getIntervalUnits());
    assertEquals(48, interval.getIntervals());
    assertEquals(1800, interval.getInterval());
    assertEquals(rollup_table, interval.getTemporalTableName());
    assertEquals(preagg_table, interval.getGroupbyTableName());
    assertEquals(0, Bytes.memcmp(table, interval.getTemporalTable()));
    assertEquals(0, Bytes.memcmp(agg_table, interval.getGroupbyTable()));
  }
  
  @Test
  public void ctor1HourDay() throws Exception {
    final RollupInterval interval = new RollupInterval(
        rollup_table, preagg_table, "1h", "1d");
    assertEquals('d', interval.getUnits());
    assertEquals("1h", interval.getStringInterval());
    assertEquals('h', interval.getIntervalUnits());
    assertEquals(24, interval.getIntervals());
    assertEquals(3600, interval.getInterval());
    assertEquals(rollup_table, interval.getTemporalTableName());
    assertEquals(preagg_table, interval.getGroupbyTableName());
    assertEquals(0, Bytes.memcmp(table, interval.getTemporalTable()));
    assertEquals(0, Bytes.memcmp(agg_table, interval.getGroupbyTable()));
  }
  
  @Test
  public void ctor1HourMonth() throws Exception {
    final RollupInterval interval = new RollupInterval(
        rollup_table, preagg_table, "1h", "1m");
    assertEquals('m', interval.getUnits());
    assertEquals("1h", interval.getStringInterval());
    assertEquals('h', interval.getIntervalUnits());
    assertEquals(768, interval.getIntervals());
    assertEquals(3600, interval.getInterval());
    assertEquals(rollup_table, interval.getTemporalTableName());
    assertEquals(preagg_table, interval.getGroupbyTableName());
    assertEquals(0, Bytes.memcmp(table, interval.getTemporalTable()));
    assertEquals(0, Bytes.memcmp(agg_table, interval.getGroupbyTable()));
  }
  
  @Test
  public void ctor3HourMonth() throws Exception {
    final RollupInterval interval = new RollupInterval(
        rollup_table, preagg_table, "3h", "1m");
    assertEquals('m', interval.getUnits());
    assertEquals("3h", interval.getStringInterval());
    assertEquals('h', interval.getIntervalUnits());
    assertEquals(256, interval.getIntervals());
    assertEquals(10800, interval.getInterval());
    assertEquals(rollup_table, interval.getTemporalTableName());
    assertEquals(preagg_table, interval.getGroupbyTableName());
    assertEquals(0, Bytes.memcmp(table, interval.getTemporalTable()));
    assertEquals(0, Bytes.memcmp(agg_table, interval.getGroupbyTable()));
  }
  
  @Test
  public void ctor6HourMonth() throws Exception {
    final RollupInterval interval = new RollupInterval(
        rollup_table, preagg_table, "6h", "1m");
    assertEquals('m', interval.getUnits());
    assertEquals("6h", interval.getStringInterval());
    assertEquals('h', interval.getIntervalUnits());
    assertEquals(128, interval.getIntervals());
    assertEquals(21600, interval.getInterval());
    assertEquals(rollup_table, interval.getTemporalTableName());
    assertEquals(preagg_table, interval.getGroupbyTableName());
    assertEquals(0, Bytes.memcmp(table, interval.getTemporalTable()));
    assertEquals(0, Bytes.memcmp(agg_table, interval.getGroupbyTable()));
  }
  
  @Test
  public void ctor6HourYear() throws Exception {
    final RollupInterval interval = new RollupInterval(
        rollup_table, preagg_table, "6h", "1y");
    assertEquals('y', interval.getUnits());
    assertEquals("6h", interval.getStringInterval());
    assertEquals('h', interval.getIntervalUnits());
    assertEquals(1464, interval.getIntervals());
    assertEquals(21600, interval.getInterval());
    assertEquals(rollup_table, interval.getTemporalTableName());
    assertEquals(preagg_table, interval.getGroupbyTableName());
    assertEquals(0, Bytes.memcmp(table, interval.getTemporalTable()));
    assertEquals(0, Bytes.memcmp(agg_table, interval.getGroupbyTable()));
  }
  
  @Test
  public void ctor12HourYear() throws Exception {
    final RollupInterval interval = new RollupInterval(
        rollup_table, preagg_table, "12h", "1y");
    assertEquals('y', interval.getUnits());
    assertEquals("12h", interval.getStringInterval());
    assertEquals('h', interval.getIntervalUnits());
    assertEquals(732, interval.getIntervals());
    assertEquals(43200, interval.getInterval());
    assertEquals(rollup_table, interval.getTemporalTableName());
    assertEquals(preagg_table, interval.getGroupbyTableName());
    assertEquals(0, Bytes.memcmp(table, interval.getTemporalTable()));
    assertEquals(0, Bytes.memcmp(agg_table, interval.getGroupbyTable()));
  }
  
  @Test
  public void ctor1DayYear() throws Exception {
    final RollupInterval interval = new RollupInterval(
        rollup_table, preagg_table, "1d", "1y");
    assertEquals('y', interval.getUnits());
    assertEquals("1d", interval.getStringInterval());
    assertEquals('d', interval.getIntervalUnits());
    assertEquals(366, interval.getIntervals());
    assertEquals(86400, interval.getInterval());
    assertEquals(rollup_table, interval.getTemporalTableName());
    assertEquals(preagg_table, interval.getGroupbyTableName());
    assertEquals(0, Bytes.memcmp(table, interval.getTemporalTable()));
    assertEquals(0, Bytes.memcmp(agg_table, interval.getGroupbyTable()));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorUnknownNullRollupTable() throws Exception {
    new RollupInterval(null, preagg_table, "1d", "1h");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorUnknownEmptyRollupTable() throws Exception {
    new RollupInterval("", preagg_table, "1d", "1h");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorUnknownNullPreAggTable() throws Exception {
    new RollupInterval(rollup_table, null, "1d", "1h");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorUnknownEmptyPreAggTable() throws Exception {
    new RollupInterval(rollup_table, "", "1d", "1h");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorUnknownSpan() throws Exception {
    new RollupInterval(rollup_table, preagg_table, "1d", "1s");
  }
  
  @Test (expected = NullPointerException.class)
  public void ctorNullInterval() throws Exception {
    new RollupInterval(rollup_table, preagg_table, null, "1d");
  }
  
  @Test (expected = StringIndexOutOfBoundsException.class)
  public void ctorEmptyInterval() throws Exception {
    new RollupInterval(rollup_table, preagg_table, "", "1d");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorBigDuration() throws Exception {
    new RollupInterval(rollup_table, preagg_table, "365y", "1d");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorTooManyIntervals() throws Exception {
    new RollupInterval(rollup_table, preagg_table, "1s", "17");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorDurationTooBigForSpan() throws Exception {
    new RollupInterval(rollup_table, preagg_table, "36500s", "1h");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorDurationEqualToSpan() throws Exception {
    new RollupInterval(rollup_table, preagg_table, "3600s", "1h");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorTooFewIntervals() throws Exception {
    new RollupInterval(rollup_table, preagg_table, "3000s", "1h");
  }

  @Test (expected = IllegalArgumentException.class)
  public void ctorNoUnitsInSpan() throws Exception {
    new RollupInterval(rollup_table, preagg_table, "365y", "1");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorNoIntervalInSpan() throws Exception {
    new RollupInterval(rollup_table, preagg_table, "365y", "d");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorNoMs() throws Exception {
    new RollupInterval(rollup_table, preagg_table, "365y", "1000ms");
  }

  @Test (expected = IllegalArgumentException.class)
  public void ctor15Minute7Days() throws Exception {
    new RollupInterval(rollup_table, preagg_table, "15m", "7d");
  }

  @Test
  public void testHashCodeAndEquals() throws Exception {
    RollupInterval interval_a = new RollupInterval(
        rollup_table, preagg_table, "7s", "1h");
    int hash_a = interval_a.hashCode();
    RollupInterval interval_b = new RollupInterval(
        rollup_table, preagg_table, "7s", "1h");
    int hash_b = interval_b.hashCode();
    
    assertEquals(interval_a, interval_b);
    assertTrue(interval_a != interval_b);
    assertEquals(hash_a, hash_b);
    
    interval_b = new RollupInterval(
        rollup_table, preagg_table, "18s", "1h");
    hash_b = interval_b.hashCode();
    assertFalse(interval_a.equals(interval_b));
    assertFalse(hash_a == hash_b);
    
    interval_b = new RollupInterval(
        rollup_table, preagg_table, "7s", "2h");
    hash_b = interval_b.hashCode();
    assertFalse(interval_a.equals(interval_b));
    assertFalse(hash_a == hash_b);
    
    interval_b = new RollupInterval(
        "tsdb-quirm", preagg_table, "7s", "1h");
    hash_b = interval_b.hashCode();
    assertFalse(interval_a.equals(interval_b));
    assertFalse(hash_a == hash_b);
    
    interval_b = new RollupInterval(
        rollup_table, "tsdb-klatch", "7s", "1h");
    hash_b = interval_b.hashCode();
    assertFalse(interval_a.equals(interval_b));
    assertFalse(hash_a == hash_b);
  }
}
