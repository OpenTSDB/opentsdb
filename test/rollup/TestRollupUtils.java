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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

import net.opentsdb.core.Const;

public class TestRollupUtils {
  private static final byte[] SUM_COL = "sum:".getBytes(Const.ASCII_CHARSET);
  private static final String temporal_table = "tsdb-rollup-10m";
  private static final String groupby_table = "tsdb-rollup-agg-10m";
  
  private RollupInterval hour_interval;
  private RollupInterval tenmin_oneday;
  private RollupInterval month_interval;
  
  @Before
  public void before() {
    hour_interval = RollupInterval.builder()
        .setTable(temporal_table)
        .setPreAggregationTable(groupby_table)
        .setInterval("1s")
        .setRowSpan("1h")
        .build();
    tenmin_oneday = RollupInterval.builder()
        .setTable(temporal_table)
        .setPreAggregationTable(groupby_table)
        .setInterval("10m")
        .setRowSpan("1d")
        .build();
    month_interval = RollupInterval.builder()
        .setTable(temporal_table)
        .setPreAggregationTable(groupby_table)
        .setInterval("1h")
        .setRowSpan("1n")
        .build();
  }
  
  @Test
  public void getRollupBasetimeHourSecondsTop() throws Exception {
    // Thu, 06 Jun 2013 15:00:00 GMT
    assertEquals(1370530800, RollupUtils.getRollupBasetime(1370530800L,
        hour_interval));
  }
  
  @Test
  public void getRollupBasetimeHourMilliSecondsTop() throws Exception {
    // Thu, 06 Jun 2013 15:00:00.154 GMT
    assertEquals(1370530800, RollupUtils.getRollupBasetime(1370530800154L, 
        hour_interval));
  }
  
  @Test
  public void getRollupBasetimeHourSecondsMid() throws Exception {
    // Thu, 06 Jun 2013 15:35:25 GMT
    assertEquals(1370530800, RollupUtils.getRollupBasetime(1370532925L, 
        hour_interval));
  }
  
  @Test
  public void getRollupBasetimeHourMilliSecondsMid() throws Exception {
    // Thu, 06 Jun 2013 15:35:25.154 GMT
    assertEquals(1370530800, RollupUtils.getRollupBasetime(1370532925154L, 
        hour_interval));
  }
  
  @Test
  public void getRollupBasetimeHourSecondsEnd() throws Exception {
    // Thu, 06 Jun 2013 15:59:59 GMT
    assertEquals(1370530800, RollupUtils.getRollupBasetime(1370534399L, 
        hour_interval));
  }
  
  @Test
  public void getRollupBasetimeHourMilliSecondsEnd() throws Exception {
    // Thu, 06 Jun 2013 15:59:59.999 GMT
    assertEquals(1370530800, RollupUtils.getRollupBasetime(1370534399999L, 
        hour_interval));
  }

  @Test
  public void getRollupBasetime6HourSecondsTop() throws Exception {
    // Thu, 06 Jun 2013 12:00:00 GMT
    final RollupInterval interval = RollupInterval.builder()
        .setTable(temporal_table)
        .setPreAggregationTable(groupby_table)
        .setInterval("10m")
        .setRowSpan("6h")
        .build();
    assertEquals(1370520000, RollupUtils.getRollupBasetime(1370520000L, interval));
  }
  
  @Test
  public void getRollupBasetime6HourSecondsMid() throws Exception {
    // Thu, 06 Jun 2013 15:00:00 GMT
    final RollupInterval interval = RollupInterval.builder()
        .setTable(temporal_table)
        .setPreAggregationTable(groupby_table)
        .setInterval("10m")
        .setRowSpan("6h")
        .build();
    assertEquals(1370520000, RollupUtils.getRollupBasetime(1370530800L, interval));
  }
  
  @Test
  public void getRollupBasetime6HourSecondsEnd() throws Exception {
    // Thu, 06 Jun 2013 23:59:59 GMT
    final RollupInterval interval = RollupInterval.builder()
        .setTable(temporal_table)
        .setPreAggregationTable(groupby_table)
        .setInterval("10m")
        .setRowSpan("6h")
        .build();
    assertEquals(1370520000, RollupUtils.getRollupBasetime(1370541599L, interval));
  }
  
  @Test
  public void getRollupBasetime2HourSecondsTop() throws Exception {
    // Thu, 06 Jun 2013 12:00:00 GMT
    final RollupInterval interval = RollupInterval.builder()
        .setTable(temporal_table)
        .setPreAggregationTable(groupby_table)
        .setInterval("10m")
        .setRowSpan("2h")
        .build();
    assertEquals(1370520000, RollupUtils.getRollupBasetime(1370520000L, interval));
  }
  
  @Test
  public void getRollupBasetime2HourSecondsMid() throws Exception {
    // Thu, 06 Jun 2013 13:01:00 GMT
    final RollupInterval interval = RollupInterval.builder()
        .setTable(temporal_table)
        .setPreAggregationTable(groupby_table)
        .setInterval("10m")
        .setRowSpan("2h")
        .build();
    assertEquals(1370520000, RollupUtils.getRollupBasetime(1370523660L, interval));
  }
  
  @Test
  public void getRollupBasetime2HourSecondsEnd() throws Exception {
    // Thu, 06 Jun 2013 13:59:59 GMT
    final RollupInterval interval = RollupInterval.builder()
        .setTable(temporal_table)
        .setPreAggregationTable(groupby_table)
        .setInterval("10m")
        .setRowSpan("2h")
        .build();
    assertEquals(1370520000, RollupUtils.getRollupBasetime(1370527199L, interval));
  }
  
  @Test
  public void getRollupBasetimeDaySecondsTop() throws Exception {
    // Thu, 06 Jun 2013 00:00:00 GMT
    assertEquals(1370476800, RollupUtils.getRollupBasetime(1370476800L, 
        tenmin_oneday));
  }
  
  @Test
  public void getRollupBasetimeDayMilliSecondsTop() throws Exception {
    // Thu, 06 Jun 2013 00:00:00.154 GMT
    assertEquals(1370476800, RollupUtils.getRollupBasetime(1370476800154L, 
        tenmin_oneday));
  }
  
  @Test
  public void getRollupBasetimeDaySecondsMid() throws Exception {
    // Thu, 06 Jun 2013 15:35:25 GMT
    assertEquals(1370476800, RollupUtils.getRollupBasetime(1370532925L, 
        tenmin_oneday));
  }
  
  @Test
  public void getRollupBasetimeDayMilliSecondsMid() throws Exception {
    // Thu, 06 Jun 2013 15:35:25.154 GMT
    assertEquals(1370476800, RollupUtils.getRollupBasetime(1370532925154L, 
        tenmin_oneday));
  }
  
  @Test
  public void getRollupBasetimeDaySecondsEnd() throws Exception {
    // Thu, 06 Jun 2013 23:59:59 GMT
    assertEquals(1370476800, RollupUtils.getRollupBasetime(1370563199L, 
        tenmin_oneday));
  }
  
  @Test
  public void getRollupBasetimeDayMilliSecondsEnd() throws Exception {
    // Thu, 06 Jun 2013 23:59:59.999 GMT
    assertEquals(1370476800, RollupUtils.getRollupBasetime(1370563199999L, 
        tenmin_oneday));
  }

  @Test
  public void getRollupBasetimeMonthSecondsTop() throws Exception {
    // Sat, 01 Jun 2013 00:00:00 GMT
    assertEquals(1370044800, RollupUtils.getRollupBasetime(1370044800L, 
        month_interval));
  }
  
  @Test
  public void getRollupBasetimeMonthMilliSecondsTop() throws Exception {
    // Thu, 01 Jun 2013 00:00:00.154 GMT
    assertEquals(1370044800, RollupUtils.getRollupBasetime(1370044800154L, 
        month_interval));
  }
  
  @Test
  public void getRollupBasetimeMonthSecondsMid() throws Exception {
    // Thu, 06 Jun 2013 15:35:25 GMT
    assertEquals(1370044800, RollupUtils.getRollupBasetime(1370532925L, 
        month_interval));
  }
  
  @Test
  public void getRollupBasetimeMonthMilliSecondsMid() throws Exception {
    // Thu, 06 Jun 2013 15:35:25.154 GMT
    assertEquals(1370044800, RollupUtils.getRollupBasetime(1370532925154L, 
        month_interval));
  }
  
  @Test
  public void getRollupBasetimeMonthSecondsEnd30days() throws Exception {
    // Thu, 30 Jun 2013 23:59:59 GMT
    assertEquals(1370044800, RollupUtils.getRollupBasetime(1372636799L, 
        month_interval));
  }
  
  @Test
  public void getRollupBasetimeMonthMilliSecondsEnd30days() throws Exception {
    // Thu, 30 Jun 2013 23:59:59.999 GMT
    assertEquals(1370044800, RollupUtils.getRollupBasetime(1372636799999L, 
        month_interval));
  }
  
  @Test
  public void getRollupBasetimeMonthSecondsEnd31days() throws Exception {
    // Wed, 31 Jul 2013 23:59:59 GMT
    assertEquals(1372636800, RollupUtils.getRollupBasetime(1375315199L, 
        month_interval));
  }
  
  @Test
  public void getRollupBasetimeMonthMilliSecondsEnd31days() throws Exception {
    // Wed, 31 Jul 2013 23:59:59.999 GMT
    assertEquals(1372636800, RollupUtils.getRollupBasetime(1375315199999L, 
        month_interval));
  }
  
  @Test
  public void getRollupBasetimeMonthSecondsEndFebruary() throws Exception {
    // Thu, 28 Feb 2013 23:59:59 GMT
    assertEquals(1359676800, RollupUtils.getRollupBasetime(1362095999L, 
        month_interval));
  }
  
  @Test
  public void getRollupBasetimeMonthMilliSecondsEndFebruary() throws Exception {
    // Thu, 28 Feb 2013 23:59:59 GMT
    assertEquals(1359676800, RollupUtils.getRollupBasetime(1362095999999L, 
        month_interval));
  }
  
  @Test
  public void getRollupBasetimeMonthSecondsEndLeapFebruary() throws Exception {
    // Wed, 29 Feb 2012 23:59:59 GMT
    assertEquals(1328054400, RollupUtils.getRollupBasetime(1330559999L, 
        month_interval));
  }
  
  @Test
  public void getRollupBasetimeMonthMilliSecondsEndLeapFebruary() throws Exception {
    // Wed, 29 Feb 2012 23:59:59 GMT
    assertEquals(1328054400, RollupUtils.getRollupBasetime(1330559999999L, 
        month_interval));
  }

  // NOTE: This is system dependent and leap seconds will usually just bump
  // to the next month and overwrite any offset == 0 value there. If this unit
  // test fails, that's actually a GOOD thing!
  @Test
  public void getRollupBasetimeMonthSecondsLeapSecond() throws Exception {
    // Tue, 30 Jun 2015 23:59:60 GMT
    assertEquals(1435708800, RollupUtils.getRollupBasetime(1435708800L, 
        month_interval));
  }
  
  @Test
  public void getRollupBasetimeMonthSecondsLeapMilliSecond() throws Exception {
    // Tue, 30 Jun 2015 23:59:60.154 GMT
    assertEquals(1435708800, RollupUtils.getRollupBasetime(1435708800154L, 
        month_interval));
  }
  
  @Test
  public void getRollupBasetimeYearSecondsTop() throws Exception {
    // Tue, 01 Jan 2013 00:00:00 GMT
    final RollupInterval interval = RollupInterval.builder()
        .setTable(temporal_table)
        .setPreAggregationTable(groupby_table)
        .setInterval("24h")
        .setRowSpan("1y")
        .build();
    assertEquals(1356998400, RollupUtils.getRollupBasetime(1356998400L, 
        interval));
  }
  
  @Test
  public void getRollupBasetimeYearMilliSecondsTop() throws Exception {
    // Tue, 01 Jan 2013 00:00:00.154 GMT
    final RollupInterval interval = RollupInterval.builder()
        .setTable(temporal_table)
        .setPreAggregationTable(groupby_table)
        .setInterval("24h")
        .setRowSpan("1y")
        .build();
    assertEquals(1356998400, RollupUtils.getRollupBasetime(1356998400154L, 
        interval));
  }
  
  @Test
  public void getRollupBasetimeYearSecondsMid() throws Exception {
    // Thu, 06 Jun 2013 15:35:25 GMT
    final RollupInterval interval = RollupInterval.builder()
        .setTable(temporal_table)
        .setPreAggregationTable(groupby_table)
        .setInterval("24h")
        .setRowSpan("1y")
        .build();
    assertEquals(1356998400, RollupUtils.getRollupBasetime(1370532925L, 
        interval));
  }
  
  @Test
  public void getRollupBasetimeYearMilliSecondsMid() throws Exception {
    // Thu, 06 Jun 2013 15:35:25.154 GMT
    final RollupInterval interval = RollupInterval.builder()
        .setTable(temporal_table)
        .setPreAggregationTable(groupby_table)
        .setInterval("24h")
        .setRowSpan("1y")
        .build();
    assertEquals(1356998400, RollupUtils.getRollupBasetime(1370532925154L, 
        interval));
  }
  
  @Test
  public void getRollupBasetimeYearSecondsEnd() throws Exception {
    // Tue, 31 Dec 2013 23:59:59 GMT
    final RollupInterval interval = RollupInterval.builder()
        .setTable(temporal_table)
        .setPreAggregationTable(groupby_table)
        .setInterval("24h")
        .setRowSpan("1y")
        .build();
    assertEquals(1356998400, RollupUtils.getRollupBasetime(1388534399L, 
        interval));
  }
  
  @Test
  public void getRollupBasetimeYearMilliSecondsEnd() throws Exception {
    // Tue, 31 Dec 2013 23:59:59.999 GMT
    final RollupInterval interval = RollupInterval.builder()
        .setTable(temporal_table)
        .setPreAggregationTable(groupby_table)
        .setInterval("24h")
        .setRowSpan("1y")
        .build();
    assertEquals(1356998400, RollupUtils.getRollupBasetime(1388534399999L, 
        interval));
  }
  
  @Test
  public void getRollupBasetimeHourZero() throws Exception {
    // Thu, 01 Jan 1970 00:00:00 GMT
    assertEquals(0, RollupUtils.getRollupBasetime(0L, hour_interval));
  }
  
  @Test
  public void getRollupBasetimeDayZero() throws Exception {
    // Thu, 01 Jan 1970 00:00:00 GMT
    final RollupInterval interval = RollupInterval.builder()
        .setTable(temporal_table)
        .setPreAggregationTable(groupby_table)
        .setInterval("10m")
        .setRowSpan("1d")
        .build();
    assertEquals(0, RollupUtils.getRollupBasetime(0L, interval));
  }
  
  @Test
  public void getRollupBasetimeMonthZero() throws Exception {
    // Thu, 01 Jan 1970 00:00:00 GMT
    final RollupInterval interval = RollupInterval.builder()
        .setTable(temporal_table)
        .setPreAggregationTable(groupby_table)
        .setInterval("1h")
        .setRowSpan("1n")
        .build();
    assertEquals(0, RollupUtils.getRollupBasetime(0L, interval));
  }
  
  @Test
  public void getRollupBasetimeYearZero() throws Exception {
    // Thu, 01 Jan 1970 00:00:00 GMT
    final RollupInterval interval = RollupInterval.builder()
        .setTable(temporal_table)
        .setPreAggregationTable(groupby_table)
        .setInterval("24h")
        .setRowSpan("1y")
        .build();
    assertEquals(0, RollupUtils.getRollupBasetime(0L, interval));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getRollupBasetimeNegativeTimestamp() throws Exception {
    // Thu, 06 Jun 2013 15:00:00 GMT
    RollupUtils.getRollupBasetime(-1370530800L, hour_interval);
  }
  
  @Test (expected = NullPointerException.class)
  public void getRollupBasetimeNullInterval() throws Exception {
    // Tue, 31 Dec 2013 23:59:59.999 GMT
    assertEquals(1356998400, RollupUtils.getRollupBasetime(1388534399999L, 
        null));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getRollupBasetimeBadSpan() throws Exception {
    // Tue, 31 Dec 2013 23:59:59.999 GMT
    final RollupInterval interval = RollupInterval.builder()
        .setTable(temporal_table)
        .setPreAggregationTable(groupby_table)
        .setInterval("1s")
        .setRowSpan("1w")
        .build();
    assertEquals(1356998400, RollupUtils.getRollupBasetime(1388534399999L, 
        interval));
  }
  
  @Test
  public void buildRollupQualifier1SecondInHourTop() {
    final byte[] offset = {0, (byte)0x07};
    byte[] expected_qual = new byte[SUM_COL.length + 2];
    System.arraycopy(SUM_COL, 0, expected_qual, 0, SUM_COL.length);
    System.arraycopy(offset, 0, expected_qual, SUM_COL.length, 2);

    //Thu, 06 Jun 2013 15:00:00 GMT
    final byte[] q = RollupUtils.buildRollupQualifier(1370530800L, 1370530800, 
        (byte)7, "sum", hour_interval);

    assertArrayEquals(expected_qual, q);
  }
  
  @Test
  public void buildRollupQualifier1SecondInHourMid() {
    final byte[] offset = {(byte) 0x84, (byte)0xD7};
    byte[] expected_qual = new byte[SUM_COL.length + 2];
    System.arraycopy(SUM_COL, 0, expected_qual, 0, SUM_COL.length);
    System.arraycopy(offset, 0, expected_qual, SUM_COL.length, 2);

    //Thu, 06 Jun 2013 15:35:25 GMT
    final byte[] q = RollupUtils.buildRollupQualifier(1370532925L, 1370530800, 
        (byte)7, "sum", hour_interval);

    assertArrayEquals(expected_qual, q);
  }
  
  @Test
  public void buildRollupQualifier1SecondInHourEnd() {
    final byte[] offset = {(byte) 0xE0, (byte)0xF7};
    byte[] expected_qual = new byte[SUM_COL.length + 2];
    System.arraycopy(SUM_COL, 0, expected_qual, 0, SUM_COL.length);
    System.arraycopy(offset, 0, expected_qual, SUM_COL.length, 2);

    //Thu, 06 Jun 2013 15:59:59 GMT
    final byte[] q = RollupUtils.buildRollupQualifier(1370534399L, 1370530800, 
        (byte)7, "sum", hour_interval);

    assertArrayEquals(expected_qual, q);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void buildRollupQualifier1SecondInHourOver() {
    //Thu, 06 Jun 2013 16:00:00 GMT
    RollupUtils.buildRollupQualifier(1370534400L, 1370530800, (byte)7, 
        "sum", hour_interval);
  }
  
  @Test
  public void buildRollupQualifier30SecondInHourTop() {
    final RollupInterval interval = RollupInterval.builder()
        .setTable(temporal_table)
        .setPreAggregationTable(groupby_table)
        .setInterval("30s")
        .setRowSpan("1h")
        .build();
    
    final byte[] offset = {0, (byte)0x07};
    byte[] expected_qual = new byte[SUM_COL.length + 2];
    System.arraycopy(SUM_COL, 0, expected_qual, 0, SUM_COL.length);
    System.arraycopy(offset, 0, expected_qual, SUM_COL.length, 2);

    //Thu, 06 Jun 2013 15:00:00 GMT
    final byte[] q = RollupUtils.buildRollupQualifier(1370530800L, 1370530800, 
        (byte)7, "sum", interval);

    assertArrayEquals(expected_qual, q);
  }
  
  @Test
  public void buildRollupQualifier30SecondInHourMid() {
    final RollupInterval interval = RollupInterval.builder()
        .setTable(temporal_table)
        .setPreAggregationTable(groupby_table)
        .setInterval("30s")
        .setRowSpan("1h")
        .build();
    
    final byte[] offset = {4, (byte)0x67};
    byte[] expected_qual = new byte[SUM_COL.length + 2];
    System.arraycopy(SUM_COL, 0, expected_qual, 0, SUM_COL.length);
    System.arraycopy(offset, 0, expected_qual, SUM_COL.length, 2);

    //Thu, 06 Jun 2013 15:35:25 GMT
    final byte[] q = RollupUtils.buildRollupQualifier(1370532925L, 1370530800, 
        (byte)7, "sum", interval);

    assertArrayEquals(expected_qual, q);
  }
  
  @Test
  public void buildRollupQualifier30SecondInHourEnd() {
    final RollupInterval interval = RollupInterval.builder()
        .setTable(temporal_table)
        .setPreAggregationTable(groupby_table)
        .setInterval("30s")
        .setRowSpan("1h")
        .build();
    
    final byte[] offset = {7, (byte)0x77};
    byte[] expected_qual = new byte[SUM_COL.length + 2];
    System.arraycopy(SUM_COL, 0, expected_qual, 0, SUM_COL.length);
    System.arraycopy(offset, 0, expected_qual, SUM_COL.length, 2);

    //Thu, 06 Jun 2013 15:59:59 GMT
    final byte[] q = RollupUtils.buildRollupQualifier(1370534399L, 1370530800, 
        (byte)7, "sum", interval);

    assertArrayEquals(expected_qual, q);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void buildRollupQualifier30SecondInHourOver() {
    final RollupInterval interval = RollupInterval.builder()
        .setTable(temporal_table)
        .setPreAggregationTable(groupby_table)
        .setInterval("30s")
        .setRowSpan("1h")
        .build();

    //Thu, 06 Jun 2013 16:00:00 GMT
    RollupUtils.buildRollupQualifier(1370534400L, 1370530800, (byte)7, 
        "sum", interval);
  }
  
  @Test
  public void buildRollupQualifier1MinuteInHourTop() {
    final RollupInterval interval = RollupInterval.builder()
        .setTable(temporal_table)
        .setPreAggregationTable(groupby_table)
        .setInterval("1m")
        .setRowSpan("1h")
        .build();
    
    final byte[] offset = {0, (byte)0x07};
    byte[] expected_qual = new byte[SUM_COL.length + 2];
    System.arraycopy(SUM_COL, 0, expected_qual, 0, SUM_COL.length);
    System.arraycopy(offset, 0, expected_qual, SUM_COL.length, 2);

    //Thu, 06 Jun 2013 15:00:00 GMT
    final byte[] q = RollupUtils.buildRollupQualifier(1370530800L, 1370530800, 
        (byte)7, "sum", interval);

    assertArrayEquals(expected_qual, q);
  }
  
  @Test
  public void buildRollupQualifier1MinuteInHourMid() {
    final RollupInterval interval = RollupInterval.builder()
        .setTable(temporal_table)
        .setPreAggregationTable(groupby_table)
        .setInterval("1m")
        .setRowSpan("1h")
        .build();
    
    final byte[] offset = {2, (byte)0x37};
    byte[] expected_qual = new byte[SUM_COL.length + 2];
    System.arraycopy(SUM_COL, 0, expected_qual, 0, SUM_COL.length);
    System.arraycopy(offset, 0, expected_qual, SUM_COL.length, 2);

    //Thu, 06 Jun 2013 15:35:25 GMT
    final byte[] q = RollupUtils.buildRollupQualifier(1370532925L, 1370530800, 
        (byte)7, "sum", interval);

    assertArrayEquals(expected_qual, q);
  }
  
  @Test
  public void buildRollupQualifier1MinuteInHourEnd() {
    final RollupInterval interval = RollupInterval.builder()
        .setTable(temporal_table)
        .setPreAggregationTable(groupby_table)
        .setInterval("1m")
        .setRowSpan("1h")
        .build();
    
    final byte[] offset = {3, (byte)0xB7};
    byte[] expected_qual = new byte[SUM_COL.length + 2];
    System.arraycopy(SUM_COL, 0, expected_qual, 0, SUM_COL.length);
    System.arraycopy(offset, 0, expected_qual, SUM_COL.length, 2);

    //Thu, 06 Jun 2013 15:35:25 GMT
    final byte[] q = RollupUtils.buildRollupQualifier(1370534399L, 1370530800, 
        (byte)7, "sum", interval);

    assertArrayEquals(expected_qual, q);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void buildRollupQualifier1MinuteInHourOver() {
    final RollupInterval interval = RollupInterval.builder()
        .setTable(temporal_table)
        .setPreAggregationTable(groupby_table)
        .setInterval("30s")
        .setRowSpan("1h")
        .build();

    //Thu, 06 Jun 2013 16:00:00 GMT
    RollupUtils.buildRollupQualifier(1370534400L, 1370530800, (byte)7, 
        "sum", interval);
  }
  
  @Test
  public void buildRollupQualifier15MinutesInDayTop() {
    final RollupInterval interval = RollupInterval.builder()
        .setTable(temporal_table)
        .setPreAggregationTable(groupby_table)
        .setInterval("15m")
        .setRowSpan("1d")
        .build();
    
    final byte[] offset = {0, (byte)0x07};
    byte[] expected_qual = new byte[SUM_COL.length + 2];
    System.arraycopy(SUM_COL, 0, expected_qual, 0, SUM_COL.length);
    System.arraycopy(offset, 0, expected_qual, SUM_COL.length, 2);

    //Thu, 06 Jun 2013 00:00:00 GMT
    final byte[] q = RollupUtils.buildRollupQualifier(1370476800L, 1370476800, 
        (byte)7, "sum", interval);

    assertArrayEquals(expected_qual, q);
  }
  
  @Test
  public void buildRollupQualifier15MinutesInDayMid() {
    final RollupInterval interval = RollupInterval.builder()
        .setTable(temporal_table)
        .setPreAggregationTable(groupby_table)
        .setInterval("15m")
        .setRowSpan("1d")
        .build();
    
    final byte[] offset = {3, (byte)0xE7};
    byte[] expected_qual = new byte[SUM_COL.length + 2];
    System.arraycopy(SUM_COL, 0, expected_qual, 0, SUM_COL.length);
    System.arraycopy(offset, 0, expected_qual, SUM_COL.length, 2);

    //Thu, 06 Jun 2013 15:35:25 GMT
    final byte[] q = RollupUtils.buildRollupQualifier(1370532925L, 1370476800, 
        (byte)7, "sum", interval);

    assertArrayEquals(expected_qual, q);
  }
  
  @Test
  public void buildRollupQualifier15MinutesInDayEnd() {
    final RollupInterval interval = RollupInterval.builder()
        .setTable(temporal_table)
        .setPreAggregationTable(groupby_table)
        .setInterval("15m")
        .setRowSpan("1d")
        .build();
    
    final byte[] offset = {5, (byte)0xF7};
    byte[] expected_qual = new byte[SUM_COL.length + 2];
    System.arraycopy(SUM_COL, 0, expected_qual, 0, SUM_COL.length);
    System.arraycopy(offset, 0, expected_qual, SUM_COL.length, 2);

    //Thu, 06 Jun 2013 23:59:59 GMT
    final byte[] q = RollupUtils.buildRollupQualifier(1370563199L, 1370476800, 
        (byte)7, "sum", interval);

    assertArrayEquals(expected_qual, q);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void buildRollupQualifier15MinutesInDayOver() {
    final RollupInterval interval = RollupInterval.builder()
        .setTable(temporal_table)
        .setPreAggregationTable(groupby_table)
        .setInterval("15m")
        .setRowSpan("1d")
        .build();

    //Thu, 07 Jun 2013 00:00:00 GMT
    RollupUtils.buildRollupQualifier(1370563200L, 1370476800, (byte)7, "sum", 
        interval);
  }
  
  @Test
  public void buildRollupQualifier60MinutesInDayTop() {
    final RollupInterval interval = RollupInterval.builder()
        .setTable(temporal_table)
        .setPreAggregationTable(groupby_table)
        .setInterval("60m")
        .setRowSpan("1d")
        .build();
    
    final byte[] offset = {0, (byte)0x07};
    byte[] expected_qual = new byte[SUM_COL.length + 2];
    System.arraycopy(SUM_COL, 0, expected_qual, 0, SUM_COL.length);
    System.arraycopy(offset, 0, expected_qual, SUM_COL.length, 2);

    //Thu, 06 Jun 2013 00:00:00 GMT
    final byte[] q = RollupUtils.buildRollupQualifier(1370476800L, 1370476800, 
        (byte)7, "sum", interval);

    assertArrayEquals(expected_qual, q);
  }
  
  @Test
  public void buildRollupQualifier60MinutesInDayMid() {
    final RollupInterval interval = RollupInterval.builder()
        .setTable(temporal_table)
        .setPreAggregationTable(groupby_table)
        .setInterval("60m")
        .setRowSpan("1d")
        .build();
    
    final byte[] offset = {0, (byte)0xF7};
    byte[] expected_qual = new byte[SUM_COL.length + 2];
    System.arraycopy(SUM_COL, 0, expected_qual, 0, SUM_COL.length);
    System.arraycopy(offset, 0, expected_qual, SUM_COL.length, 2);

    //Thu, 06 Jun 2013 15:35:25 GMT
    final byte[] q = RollupUtils.buildRollupQualifier(1370532925L, 1370476800, 
        (byte)7, "sum", interval);

    assertArrayEquals(expected_qual, q);
  }
  
  @Test
  public void buildRollupQualifier60MinutesInDayEnd() {
    final RollupInterval interval = RollupInterval.builder()
        .setTable(temporal_table)
        .setPreAggregationTable(groupby_table)
        .setInterval("60m")
        .setRowSpan("1d")
        .build();
    
    final byte[] offset = {1, (byte)0x77};
    byte[] expected_qual = new byte[SUM_COL.length + 2];
    System.arraycopy(SUM_COL, 0, expected_qual, 0, SUM_COL.length);
    System.arraycopy(offset, 0, expected_qual, SUM_COL.length, 2);

    //Thu, 06 Jun 2013 23:59:59 GMT
    final byte[] q = RollupUtils.buildRollupQualifier(1370563199L, 1370476800, 
        (byte)7, "sum", interval);

    assertArrayEquals(expected_qual, q);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void buildRollupQualifier60MinutesInDayOver() {
    final RollupInterval interval = RollupInterval.builder()
        .setTable(temporal_table)
        .setPreAggregationTable(groupby_table)
        .setInterval("60m")
        .setRowSpan("1d")
        .build();

    //Thu, 07 Jun 2013 00:00:00 GMT
    RollupUtils.buildRollupQualifier(1370563200L, 1370476800, (byte)7, "sum", 
        interval);
  }
  
  @Test
  public void buildRollupQualifier3HoursInMonthTop() {
    final RollupInterval interval = RollupInterval.builder()
        .setTable(temporal_table)
        .setPreAggregationTable(groupby_table)
        .setInterval("3h")
        .setRowSpan("1n")
        .build();
    
    final byte[] offset = {0, (byte)0x07};
    byte[] expected_qual = new byte[SUM_COL.length + 2];
    System.arraycopy(SUM_COL, 0, expected_qual, 0, SUM_COL.length);
    System.arraycopy(offset, 0, expected_qual, SUM_COL.length, 2);

    //Sat, 01 Jun 2013 00:00:00 GMT
    final byte[] q = RollupUtils.buildRollupQualifier(1370044800L, 1370044800, 
        (byte)7, "sum", interval);

    assertArrayEquals(expected_qual, q);
  }
  
  @Test
  public void buildRollupQualifier3HoursInMonthMid() {
    final RollupInterval interval = RollupInterval.builder()
        .setTable(temporal_table)
        .setPreAggregationTable(groupby_table)
        .setInterval("3h")
        .setRowSpan("1n")
        .build();
    
    final byte[] offset = {2, (byte)0xD7};
    byte[] expected_qual = new byte[SUM_COL.length + 2];
    System.arraycopy(SUM_COL, 0, expected_qual, 0, SUM_COL.length);
    System.arraycopy(offset, 0, expected_qual, SUM_COL.length, 2);

    //Thu, 06 Jun 2013 15:35:25 GMT
    final byte[] q = RollupUtils.buildRollupQualifier(1370532925L, 1370044800, 
        (byte)7, "sum", interval);

    assertArrayEquals(expected_qual, q);
  }
  
  @Test
  public void buildRollupQualifier3HoursInMonthEnd() {
    final RollupInterval interval = RollupInterval.builder()
        .setTable(temporal_table)
        .setPreAggregationTable(groupby_table)
        .setInterval("3h")
        .setRowSpan("1n")
        .build();
    
    final byte[] offset = {0x0E, (byte)0xF7};
    byte[] expected_qual = new byte[SUM_COL.length + 2];
    System.arraycopy(SUM_COL, 0, expected_qual, 0, SUM_COL.length);
    System.arraycopy(offset, 0, expected_qual, SUM_COL.length, 2);

    //Thu, 30 Jun 2013 23:59:59 GMT
    final byte[] q = RollupUtils.buildRollupQualifier(1372636799L, 1370044800, 
        (byte)7, "sum", interval);

    assertArrayEquals(expected_qual, q);
  }
  
  // NOTE this guy won't overflow since we max our monthlies on 31 days.
  @Test
  public void buildRollupQualifier3HoursInMonthOver30Days() {
    final RollupInterval interval = RollupInterval.builder()
        .setTable(temporal_table)
        .setPreAggregationTable(groupby_table)
        .setInterval("3h")
        .setRowSpan("1n")
        .build();
    
    final byte[] offset = {0x0F, (byte)0x07};
    byte[] expected_qual = new byte[SUM_COL.length + 2];
    System.arraycopy(SUM_COL, 0, expected_qual, 0, SUM_COL.length);
    System.arraycopy(offset, 0, expected_qual, SUM_COL.length, 2);

    //Thu, 1 July 2013 00:00:00 GMT
    final byte[] q = RollupUtils.buildRollupQualifier(1372636800L, 1370044800, 
        (byte)7, "sum", interval);

    assertArrayEquals(expected_qual, q);
  }
  
  // Still only overflows 3 days later
  @Test (expected = IllegalArgumentException.class)
  public void buildRollupQualifier3HoursInMonthOver() {
    final RollupInterval interval = RollupInterval.builder()
        .setTable(temporal_table)
        .setPreAggregationTable(groupby_table)
        .setInterval("3h")
        .setRowSpan("1n")
        .build();

    //Wed, 03 Jul 2013 23:59:59 GMT
    RollupUtils.buildRollupQualifier(1372895999L, 1370044800, (byte)7, "sum", 
        interval);
  }
  
  @Test
  public void buildRollupQualifier6HoursInYearTop() {
    final RollupInterval interval = RollupInterval.builder()
        .setTable(temporal_table)
        .setPreAggregationTable(groupby_table)
        .setInterval("6h")
        .setRowSpan("1y")
        .build();
    
    final byte[] offset = {0, (byte)0x07};
    byte[] expected_qual = new byte[SUM_COL.length + 2];
    System.arraycopy(SUM_COL, 0, expected_qual, 0, SUM_COL.length);
    System.arraycopy(offset, 0, expected_qual, SUM_COL.length, 2);

    //Tue, 01 Jan 2013 00:00:00 GMT
    final byte[] q = RollupUtils.buildRollupQualifier(1356998400L, 1356998400, 
        (byte)7, "sum", interval);

    assertArrayEquals(expected_qual, q);
  }
  
  @Test
  public void buildRollupQualifier6HoursInYearMid() {
    final RollupInterval interval = RollupInterval.builder()
        .setTable(temporal_table)
        .setPreAggregationTable(groupby_table)
        .setInterval("6h")
        .setRowSpan("1y")
        .build();
    
    final byte[] offset = {0x27, (byte)0x27};
    byte[] expected_qual = new byte[SUM_COL.length + 2];
    System.arraycopy(SUM_COL, 0, expected_qual, 0, SUM_COL.length);
    System.arraycopy(offset, 0, expected_qual, SUM_COL.length, 2);

    //Thu, 06 Jun 2013 15:35:25 GMT
    final byte[] q = RollupUtils.buildRollupQualifier(1370532925L, 1356998400, 
        (byte)7, "sum", interval);

    assertArrayEquals(expected_qual, q);
  }
  
  @Test
  public void buildRollupQualifier6HoursInYearEnd() {
    final RollupInterval interval = RollupInterval.builder()
        .setTable(temporal_table)
        .setPreAggregationTable(groupby_table)
        .setInterval("6h")
        .setRowSpan("1y")
        .build();
    
    final byte[] offset = {0x5B, (byte)0x37};
    byte[] expected_qual = new byte[SUM_COL.length + 2];
    System.arraycopy(SUM_COL, 0, expected_qual, 0, SUM_COL.length);
    System.arraycopy(offset, 0, expected_qual, SUM_COL.length, 2);

    //Tue, 31 Dec 2013 23:59:59 GMT
    final byte[] q = RollupUtils.buildRollupQualifier(1388534399, 1356998400, 
        (byte)7, "sum", interval);

    assertArrayEquals(expected_qual, q);
  }
  
  // overflows since our max years are a little larger
  @Test (expected = IllegalArgumentException.class)
  public void buildRollupQualifier6HoursInYearOver() {
    final RollupInterval interval = RollupInterval.builder()
        .setTable(temporal_table)
        .setPreAggregationTable(groupby_table)
        .setInterval("6h")
        .setRowSpan("1y")
        .build();

    //Wed, 01 Jan 2014 00:00:00 GMT
    RollupUtils.buildRollupQualifier(1388620800, 1356998400, (byte)7, "sum", 
        interval);
  }
  
  // Flag tests ------------------
  @Test
  public void buildRollupQualifier8BytesLong() {
    final byte[] offset = {(byte) 0x84, (byte)0xD7};
    byte[] expected_qual = new byte[SUM_COL.length + 2];
    System.arraycopy(SUM_COL, 0, expected_qual, 0, SUM_COL.length);
    System.arraycopy(offset, 0, expected_qual, SUM_COL.length, 2);

    final byte[] q = RollupUtils.buildRollupQualifier(1370532925L, 1370530800, 
        (byte)7, "sum", hour_interval);

    assertArrayEquals(expected_qual, q);
  }
  
  @Test
  public void buildRollupQualifierBytesLong() {
    final byte[] offset = {(byte) 0x84, (byte)0xD3};
    byte[] expected_qual = new byte[SUM_COL.length + 2];
    System.arraycopy(SUM_COL, 0, expected_qual, 0, SUM_COL.length);
    System.arraycopy(offset, 0, expected_qual, SUM_COL.length, 2);

    final byte[] q = RollupUtils.buildRollupQualifier(1370532925L, 1370530800, 
        (byte) 3, "sum", hour_interval);

    assertArrayEquals(expected_qual, q);
  }
  
  @Test
  public void buildRollupQualifier2BytesLong() {
    final byte[] offset = {(byte) 0x84, (byte)0xD1};
    byte[] expected_qual = new byte[SUM_COL.length + 2];
    System.arraycopy(SUM_COL, 0, expected_qual, 0, SUM_COL.length);
    System.arraycopy(offset, 0, expected_qual, SUM_COL.length, 2);

    final byte[] q = RollupUtils.buildRollupQualifier(1370532925L, 1370530800,
         (byte) 1, "sum", hour_interval);

    assertArrayEquals(expected_qual, q);
  }
  
  @Test
  public void buildRollupQualifierByteLong() {
    final byte[] offset = {(byte) 0x84, (byte)0xD0};
    byte[] expected_qual = new byte[SUM_COL.length + 2];
    System.arraycopy(SUM_COL, 0, expected_qual, 0, SUM_COL.length);
    System.arraycopy(offset, 0, expected_qual, SUM_COL.length, 2);

    final byte[] q = RollupUtils.buildRollupQualifier(1370532925L, 1370530800,
        (byte) 0, "sum", hour_interval);

    assertArrayEquals(expected_qual, q);
  }

  @Test
  public void buildRollupQualifierTenMin8ByteFloat() {
    final byte[] offset = {(byte) 0x84, (byte)0xDF};
    byte[] expected_qual = new byte[SUM_COL.length + 2];
    System.arraycopy(SUM_COL, 0, expected_qual, 0, SUM_COL.length);
    System.arraycopy(offset, 0, expected_qual, SUM_COL.length, 2);

    final byte[] q = RollupUtils.buildRollupQualifier(1370532925L, 1370530800, 
        (byte) ( 7 | Const.FLAG_FLOAT), "sum", hour_interval);

    assertArrayEquals(expected_qual, q);
  }

  @Test
  public void buildRollupQualifier4ByteFloat() {
    final byte[] offset = {(byte) 0x84, (byte)0xDB};
    byte[] expected_qual = new byte[SUM_COL.length + 2];
    System.arraycopy(SUM_COL, 0, expected_qual, 0, SUM_COL.length);
    System.arraycopy(offset, 0, expected_qual, SUM_COL.length, 2);

    final byte[] q = RollupUtils.buildRollupQualifier(1370532925L, 1370530800, 
        (byte) ( 3 | Const.FLAG_FLOAT), "sum", hour_interval);

    assertArrayEquals(expected_qual, q);
  }
  
  @Test
  public void buildRollupQualifierTenMinZeroTime() {
    final byte[] offset = {0x0, 0x0};
    byte[] expected_qual = new byte[SUM_COL.length + 2];
    System.arraycopy(SUM_COL, 0, expected_qual, 0, SUM_COL.length);
    System.arraycopy(offset, 0, expected_qual, SUM_COL.length, 2);

    final byte[] q = 
        RollupUtils.buildRollupQualifier(0, 0, (byte) 0, "sum", hour_interval);

    assertArrayEquals(expected_qual, q);
  }
  
  // this one will be really goofy because we don't really account for negative
  // values in the offset when applying the mask to the timestamp to determine if
  // it's in millisecond s or not. Fix this up some day.
  @Test
  public void buildRollupQualifierNegativeTime() {
    final byte[] offset = {(byte) 0xF1, 0};
    byte[] expected_qual = new byte[SUM_COL.length + 2];
    System.arraycopy(SUM_COL, 0, expected_qual, 0, SUM_COL.length);
    System.arraycopy(offset, 0, expected_qual, SUM_COL.length, 2);

    final byte[] q = RollupUtils.buildRollupQualifier(1420062000L, -1420063200, 
        (byte) 0, "sum", hour_interval);

    assertArrayEquals(expected_qual, q);
  }
  
  @Test
  public void buildRollupQualifierAggCase() {
    final byte[] offset = {0, (byte)0x07};
    byte[] expected_qual = new byte[SUM_COL.length + 2];
    System.arraycopy(SUM_COL, 0, expected_qual, 0, SUM_COL.length);
    System.arraycopy(offset, 0, expected_qual, SUM_COL.length, 2);

    //Thu, 06 Jun 2013 15:00:00 GMT
    final byte[] q = RollupUtils.buildRollupQualifier(1370530800L, 1370530800, 
        (byte)7, "Sum", hour_interval);

    assertArrayEquals(expected_qual, q);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void buildRollupQualifierNullAggregator() {
    RollupUtils.buildRollupQualifier(1370532925L, 1370530800, 
        (byte)7, null, hour_interval);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void buildRollupQualifierEmptyAggregator() {
    RollupUtils.buildRollupQualifier(1370532925L, 1370530800, 
        (byte)7, "", hour_interval);
  }
  
  // verify we truncate the milliseconds
  @Test
  public void buildRollupQualifierMillisecond() {
    final byte[] offset = {(byte) 0x84, (byte)0xD7};
    byte[] expected_qual = new byte[SUM_COL.length + 2];
    System.arraycopy(SUM_COL, 0, expected_qual, 0, SUM_COL.length);
    System.arraycopy(offset, 0, expected_qual, SUM_COL.length, 2);

    //Thu, 06 Jun 2013 15:35:25 GMT
    final byte[] q = RollupUtils.buildRollupQualifier(1370532925154L, 1370530800, 
        (byte)7, "sum", hour_interval);

    assertArrayEquals(expected_qual, q);
  }
  
}
