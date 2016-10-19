// This file is part of OpenTSDB.
// Copyright (C) 2015  The OpenTSDB Authors.
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
package net.opentsdb.core;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyShort;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import net.opentsdb.rollup.NoSuchRollupForIntervalException;
import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.rollup.RollupInterval;
import net.opentsdb.rollup.RollupUtils;
import net.opentsdb.storage.MockBase;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.uid.UniqueId.UniqueIdType;

import org.hbase.async.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import com.stumbleupon.async.Deferred;

public class TestTSDBAddAggregatePoint extends BaseTsdbTest {
  private final static byte[] TSDB_TABLE = "tsdb".getBytes(MockBase.ASCII());
  private final static byte[] AGG_TABLE = "tsdb-agg".getBytes(MockBase.ASCII());
  private final static byte[] FAMILY = "t".getBytes(MockBase.ASCII());
  private HashMap<String, String> tags;
  private RollupConfig rollup_config;
  
  @Before
  public void beforeLocal() {
    tags = new HashMap<String, String>(1);
    tags.put(TAGK_STRING, TAGV_STRING);
    
    storage = new MockBase(tsdb, client, true, true, true, true);
    final List<byte[]> families = new ArrayList<byte[]>();
    families.add(FAMILY);
    
    storage.addTable("tsdb-rollup-10m".getBytes(), families);
    storage.addTable("tsdb-rollup-agg-10m".getBytes(), families);
    storage.addTable("tsdb-rollup-1h".getBytes(), families);
    storage.addTable("tsdb-rollup-agg-1h".getBytes(), families);
    storage.addTable("tsdb-rollup-1d".getBytes(), families);
    storage.addTable("tsdb-rollup-agg-1d".getBytes(), families);
    storage.addTable(AGG_TABLE, families);
    
    final List<RollupInterval> rollups = new ArrayList<RollupInterval>();
    rollups.add(new RollupInterval(
        "tsdb", "tsdb-agg", "1m", "1h", true));
    rollups.add(new RollupInterval(
        "tsdb-rollup-10m", "tsdb-rollup-agg-10m", "10m", "1d"));
    rollups.add(new RollupInterval(
        "tsdb-rollup-1h", "tsdb-rollup-agg-1h", "1h", "1m"));
    rollups.add(new RollupInterval(
        "tsdb-rollup-1d", "tsdb-rollup-agg-1d", "1d", "1y"));
    
    rollup_config = new RollupConfig(rollups);
    Whitebox.setInternalState(tsdb, "rollup_config", rollup_config);
    Whitebox.setInternalState(tsdb, "default_interval", rollups.get(0));
    
    mockUID(UniqueIdType.TAGK, "_aggregate", new byte[] { 0, 0, 42 });
    mockUID(UniqueIdType.TAGV, "SUM", new byte[] { 0, 0, 42 });
    mockUID(UniqueIdType.TAGV, "MAX", new byte[] { 0, 0, 43 });
    mockUID(UniqueIdType.TAGV, "MIN", new byte[] { 0, 0, 44 });
    mockUID(UniqueIdType.TAGV, "COUNT", new byte[] { 0, 0, 45 });
    mockUID(UniqueIdType.TAGV, "AVG", new byte[] { 0, 0, 46 });
    mockUID(UniqueIdType.TAGV, "NOSUCHAGG", new byte[] { 0, 0, 47 });
  }
  
  @Test
  public void addAggregatePointLong1Byte() throws Exception {
    final byte[] row = new byte[] {  0, 0, 1, 
        0x50, (byte) 0xE2, 0x27, 0, 0, 0, 1, 0, 0, 1};
    RowKey.prefixKeyWithSalt(row);
    final byte[] qualifier = new byte[] {0x73, 0x75, 0x6D, 0x3A, 0, 0};

    tsdb.addAggregatePoint(METRIC_STRING, 1356998400L, 42, tags, false,
        "10m", "sum").joinUninterruptibly();

    final byte[] value = storage.getColumn(
        rollup_config.getRollupInterval("10m").getTemporalTable(), 
        row, FAMILY, qualifier);
    final byte[] expected = {0x2A};
    assertArrayEquals(expected, value);
  }
  
  @Test
  public void addAggregatePointLong1ByteNegative() throws Exception {
    final byte[] row = new byte[] { 0, 0, 1, 
        0x50, (byte) 0xE2, 0x27, 0, 0, 0, 1, 0, 0, 1};
    RowKey.prefixKeyWithSalt(row);
    final byte[] qualifier = new byte[] {0x73, 0x75, 0x6D, 0x3A, 0, 0};

    tsdb.addAggregatePoint(METRIC_STRING, 1356998400L, -42, tags, false,
        "10m", "sum").joinUninterruptibly();

    final byte[] value = storage.getColumn(
        rollup_config.getRollupInterval("10m").getTemporalTable(), 
        row, FAMILY, qualifier);
    final byte[] expected = {(byte) 0xD6};
    assertArrayEquals(expected, value);
  }
  
  @Test
  public void addAggregatePointLong2Bytes() throws Exception {
    final byte[] row = new byte[] { 0, 0, 1,
        0x50, (byte) 0xE2, 0x27, 0, 0, 0, 1, 0, 0, 1};
    RowKey.prefixKeyWithSalt(row);
    final byte[] qualifier = new byte[] {0x73, 0x75, 0x6D, 0x3A, 0, 1};

    tsdb.addAggregatePoint(METRIC_STRING, 1356998400L, 257, tags, false,
        "10m", "sum").joinUninterruptibly();

    final byte[] value = storage.getColumn(
        rollup_config.getRollupInterval("10m").getTemporalTable(), 
        row, FAMILY, qualifier);
    final byte[] expected = {1, 1};
    assertArrayEquals(expected, value);
  }
  
  @Test
  public void addAggregatePointLong2BytesNegative() throws Exception {
    final byte[] row = new byte[] { 0, 0, 1,
        0x50, (byte) 0xE2, 0x27, 0, 0, 0, 1, 0, 0, 1};
    RowKey.prefixKeyWithSalt(row);
    final byte[] qualifier = new byte[] {0x73, 0x75, 0x6D, 0x3A, 0, 1};

    tsdb.addAggregatePoint(METRIC_STRING, 1356998400L, -257, tags, false,
        "10m", "sum").joinUninterruptibly();

    final byte[] value = storage.getColumn(
        rollup_config.getRollupInterval("10m").getTemporalTable(), 
        row, FAMILY, qualifier);
    final byte[] expected = {(byte) 0xFE, (byte) 0xFF};
    assertArrayEquals(expected, value);
  }
  
  @Test
  public void addAggregatePointLong4Bytes() throws Exception {
    final byte[] row = new byte[] { 0, 0, 1,
        0x50, (byte) 0xE2, 0x27, 0, 0, 0, 1, 0, 0, 1};
    RowKey.prefixKeyWithSalt(row);
    final byte[] qualifier = new byte[] {0x73, 0x75, 0x6D, 0x3A, 0, 3};

    tsdb.addAggregatePoint(METRIC_STRING, 1356998400L, 65537, tags, false,
        "10m", "sum").joinUninterruptibly();

    final byte[] value = storage.getColumn(
        rollup_config.getRollupInterval("10m").getTemporalTable(), 
        row, FAMILY, qualifier);
    final byte[] expected = {0, 1, 0, 1};
    assertArrayEquals(expected, value);
  }
  
  @Test
  public void addAggregatePointLong4BytesNegative() throws Exception {
    final byte[] row = new byte[] { 0, 0, 1,
        0x50, (byte) 0xE2, 0x27, 0, 0, 0, 1, 0, 0, 1};
    RowKey.prefixKeyWithSalt(row);
    final byte[] qualifier = new byte[] {0x73, 0x75, 0x6D, 0x3A, 0, 3};

    tsdb.addAggregatePoint(METRIC_STRING, 1356998400L, -65537, tags, false,
        "10m", "sum").joinUninterruptibly();

    final byte[] value = storage.getColumn(
        rollup_config.getRollupInterval("10m").getTemporalTable(), 
        row, FAMILY, qualifier);
    final byte[] expected = 
      {(byte) 0xFF, (byte) 0xFE, (byte) 0xFF, (byte) 0xFF};
    assertArrayEquals(expected, value);
  }
  
  @Test
  public void addAggregatePointLong8Bytes() throws Exception {
    final byte[] row = new byte[] { 0, 0, 1,
        0x50, (byte) 0xE2, 0x27, 0, 0, 0, 1, 0, 0, 1};
    RowKey.prefixKeyWithSalt(row);
    final byte[] qualifier = new byte[] {0x73, 0x75, 0x6D, 0x3A, 0, 7};

    tsdb.addAggregatePoint(METRIC_STRING, 1356998400L, 4294967296L, tags, false,
        "10m", "sum").joinUninterruptibly();

    final byte[] value = storage.getColumn(
        rollup_config.getRollupInterval("10m").getTemporalTable(), 
        row, FAMILY, qualifier);
    final byte[] expected = {0, 0, 0, 1, 0, 0, 0, 0};
    assertArrayEquals(expected, value);
  }
  
  @Test
  public void addAggregatePointLong8BytesNegative() throws Exception {
    final byte[] row = new byte[] { 0, 0, 1,
        0x50, (byte) 0xE2, 0x27, 0, 0, 0, 1, 0, 0, 1};
    RowKey.prefixKeyWithSalt(row);
    final byte[] qualifier = new byte[] {0x73, 0x75, 0x6D, 0x3A, 0, 7};

    tsdb.addAggregatePoint(METRIC_STRING, 1356998400L, -4294967296L, tags, false,
        "10m", "sum").joinUninterruptibly();

    final byte[] value = storage.getColumn(
        rollup_config.getRollupInterval("10m").getTemporalTable(), 
        row, FAMILY, qualifier);
    final byte[] expected = 
      {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0, 0, 0, 0};
    assertArrayEquals(expected, value);
  }
  
  @Test
  public void addAggregatePointFloat4Bytes() throws Exception {
    final byte[] row = new byte[] { 0, 0, 1,
        0x50, (byte) 0xE2, 0x27, 0, 0, 0, 1, 0, 0, 1};
    RowKey.prefixKeyWithSalt(row);
    final byte[] qualifier = new byte[] {0x73, 0x75, 0x6D, 0x3A, 0, 0x0B};

    tsdb.addAggregatePoint(METRIC_STRING, 1356998400L, 42.5F, tags, false,
        "10m", "sum").joinUninterruptibly();

    final byte[] value = storage.getColumn(
        rollup_config.getRollupInterval("10m").getTemporalTable(), 
        row, FAMILY, qualifier);
    final float read = 
        Float.intBitsToFloat(Bytes.getInt(Arrays.copyOf(value, 4)));
    assertEquals(42.5F, read, 0.0000001);
  }
  
  @Test
  public void addAggregatePointFloat4BytesNegative() throws Exception {
    final byte[] row = new byte[] { 0, 0, 1,
        0x50, (byte) 0xE2, 0x27, 0, 0, 0, 1, 0, 0, 1};
    RowKey.prefixKeyWithSalt(row);
    final byte[] qualifier = new byte[] {0x73, 0x75, 0x6D, 0x3A, 0, 0x0B};

    tsdb.addAggregatePoint(METRIC_STRING, 1356998400L, -42.5F, tags, false,
        "10m", "sum").joinUninterruptibly();

    final byte[] value = storage.getColumn(
        rollup_config.getRollupInterval("10m").getTemporalTable(), 
        row, FAMILY, qualifier);
    final float read = 
        Float.intBitsToFloat(Bytes.getInt(Arrays.copyOf(value, 4)));
    assertEquals(-42.5F, read, 0.0000001);
  }
  
  @Test
  public void addAggregatePointFloat4BytesPrecision() throws Exception {
    final byte[] row = new byte[] { 0, 0, 1,
        0x50, (byte) 0xE2, 0x27, 0, 0, 0, 1, 0, 0, 1};
    RowKey.prefixKeyWithSalt(row);
    final byte[] qualifier = new byte[] {0x73, 0x75, 0x6D, 0x3A, 0, 0x0B};

    tsdb.addAggregatePoint(METRIC_STRING, 1356998400L, 42.5123459999F, tags, false,
        "10m", "sum").joinUninterruptibly();

    final byte[] value = storage.getColumn(
        rollup_config.getRollupInterval("10m").getTemporalTable(), 
        row, FAMILY, qualifier);
    final float read = 
        Float.intBitsToFloat(Bytes.getInt(Arrays.copyOf(value, 4)));
    assertEquals(42.5123459999F, read, 0.0000001);
  }
  
  @Test
  public void addAggregatePointFloat4BytesPrecisionNegative() throws Exception {
    final byte[] row = new byte[] { 0, 0, 1,
        0x50, (byte) 0xE2, 0x27, 0, 0, 0, 1, 0, 0, 1};
    RowKey.prefixKeyWithSalt(row);
    final byte[] qualifier = new byte[] {0x73, 0x75, 0x6D, 0x3A, 0, 0x0B};

    tsdb.addAggregatePoint(METRIC_STRING, 1356998400L, -42.5123459999F, tags, false,
        "10m", "sum").joinUninterruptibly();

    final byte[] value = storage.getColumn(
        rollup_config.getRollupInterval("10m").getTemporalTable(), 
        row, FAMILY, qualifier);
    final float read = 
        Float.intBitsToFloat(Bytes.getInt(Arrays.copyOf(value, 4)));
    assertEquals(-42.5123459999F, read, 0.0000001);
  }

  // not allowing rollups with millisecond precision
  @Test (expected = IllegalArgumentException.class)
  public void addAggregatePointMilliseconds() throws Exception {
    tsdb.addAggregatePoint(METRIC_STRING, 1419992400000L, 42, tags, false,
        "10m", "sum").joinUninterruptibly();
  }
 
  @Test (expected = NoSuchRollupForIntervalException.class)
  public void addAggregatePointNoSuchRollup() throws Exception {
    tsdb.addAggregatePoint(METRIC_STRING, 1356998400L, 42, tags, false,
        "11m", "sum").joinUninterruptibly();
  }

  @Test
  public void addAggregatePoint10mInDayTop() throws Exception {
    final byte[] row = new byte[] { 0, 0, 1,
        0x51, (byte) 0xAF, (byte) 0xD1, 0, 0, 0, 1, 0, 0, 1};
    RowKey.prefixKeyWithSalt(row);
    final byte[] qualifier = new byte[] {0x73, 0x75, 0x6D, 0x3A, 0, 0};

    tsdb.addAggregatePoint(METRIC_STRING, 1370476800, 42, tags, false,
        "10m", "sum").joinUninterruptibly();

    final byte[] value = storage.getColumn(
        rollup_config.getRollupInterval("10m").getTemporalTable(), 
        row, FAMILY, qualifier);
    final byte[] expected = {0x2A};
    assertArrayEquals(expected, value);
  }
  
  @Test
  public void addAggregatePoint10mInDayMid() throws Exception {
    final byte[] row = new byte[] { 0, 0, 1,
        0x51, (byte) 0xAF, (byte) 0xD1, 0, 0, 0, 1, 0, 0, 1};
    RowKey.prefixKeyWithSalt(row);
    final byte[] qualifier = new byte[] {0x73, 0x75, 0x6D, 0x3A, 5, (byte) 0xD0};

    tsdb.addAggregatePoint(METRIC_STRING, 1370532925L, 42, tags, false,
        "10m", "sum").joinUninterruptibly();

    final byte[] value = storage.getColumn(
        rollup_config.getRollupInterval("10m").getTemporalTable(), 
        row, FAMILY, qualifier);
    final byte[] expected = {0x2A};
    assertArrayEquals(expected, value);
  }
  
  @Test
  public void addAggregatePoint10mInDayEnd() throws Exception {
    final byte[] row = new byte[] { 0, 0, 1,
        0x51, (byte) 0xAF, (byte) 0xD1, 0, 0, 0, 1, 0, 0, 1};
    RowKey.prefixKeyWithSalt(row);
    final byte[] qualifier = new byte[] {0x73, 0x75, 0x6D, 0x3A, 5, (byte) 0xF0};

    tsdb.addAggregatePoint(METRIC_STRING, 1370534399L, 42, tags, false,
        "10m", "sum").joinUninterruptibly();

    final byte[] value = storage.getColumn(
        rollup_config.getRollupInterval("10m").getTemporalTable(), 
        row, FAMILY, qualifier);
    final byte[] expected = {0x2A};
    assertArrayEquals(expected, value);
  }
  
  @Test
  public void addAggregatePoint10mInDayOver() throws Exception {
    final byte[] row = new byte[] { 0, 0, 1,
        0x51, (byte) 0xB1, (byte) 0x22, (byte) 0x80, 0, 0, 1, 0, 0, 1};
    RowKey.prefixKeyWithSalt(row);
    final byte[] qualifier = new byte[] {0x73, 0x75, 0x6D, 0x3A, 0, 0};

    tsdb.addAggregatePoint(METRIC_STRING, 1370563200L, 42, tags, false,
        "10m", "sum").joinUninterruptibly();

    final byte[] value = storage.getColumn(
        rollup_config.getRollupInterval("10m").getTemporalTable(), 
        row, FAMILY, qualifier);
    final byte[] expected = {0x2A};
    assertArrayEquals(expected, value);
  }
  
  @Test
  public void addAggregatePoint1hInMonthTop() throws Exception {
    final byte[] row = new byte[] { 0, 0, 1,
        0x51, (byte) 0xA9, (byte) 0x39, (byte) 0x80, 0, 0, 1, 0, 0, 1};
    RowKey.prefixKeyWithSalt(row);
    final byte[] qualifier = new byte[] {0x73, 0x75, 0x6D, 0x3A, 0, 0};

    tsdb.addAggregatePoint(METRIC_STRING, 1370044800L, 42, tags, false,
        "1h", "sum").joinUninterruptibly();

    final byte[] value = storage.getColumn(
        rollup_config.getRollupInterval("1h").getTemporalTable(), 
        row, FAMILY, qualifier);
    final byte[] expected = {0x2A};
    assertArrayEquals(expected, value);
  }
  
  @Test
  public void addAggregatePoint1hInMonthMid() throws Exception {
    final byte[] row = new byte[] { 0, 0, 1,
        0x51, (byte) 0xA9, (byte) 0x39, (byte) 0x80, 0, 0, 1, 0, 0, 1};
    RowKey.prefixKeyWithSalt(row);
    final byte[] qualifier = new byte[] {0x73, 0x75, 0x6D, 0x3A, 0x2C, (byte) 0xF0};

    tsdb.addAggregatePoint(METRIC_STRING, 1372636799L, 42, tags, false,
        "1h", "sum").joinUninterruptibly();

    final byte[] value = storage.getColumn(
        rollup_config.getRollupInterval("1h").getTemporalTable(), 
        row, FAMILY, qualifier);
    final byte[] expected = {0x2A};
    assertArrayEquals(expected, value);
  }
  
  @Test
  public void addAggregatePoint1hInMonthOver() throws Exception {
    final byte[] row = new byte[] { 0, 0, 1,
        0x51, (byte) 0xD0, (byte) 0xC6, (byte) 0x80, 0, 0, 1, 0, 0, 1};
    RowKey.prefixKeyWithSalt(row);
    final byte[] qualifier = new byte[] {0x73, 0x75, 0x6D, 0x3A, 0, 0};

    tsdb.addAggregatePoint(METRIC_STRING, 1372636800L, 42, tags, false,
        "1h", "sum").joinUninterruptibly();

    final byte[] value = storage.getColumn(
        rollup_config.getRollupInterval("1h").getTemporalTable(), 
        row, FAMILY, qualifier);
    final byte[] expected = {0x2A};
    assertArrayEquals(expected, value);
  }
  
  @Test
  public void addAggregatePoint1dInYearTop() throws Exception {
    final byte[] row = new byte[] { 0, 0, 1,
        0x50, (byte) 0xE2, (byte) 0x27, (byte) 0x00, 0, 0, 1, 0, 0, 1};
    RowKey.prefixKeyWithSalt(row);
    final byte[] qualifier = new byte[] {0x73, 0x75, 0x6D, 0x3A, 0, 0};

    tsdb.addAggregatePoint(METRIC_STRING, 1356998400L, 42, tags, false,
        "1d", "sum").joinUninterruptibly();

    final byte[] value = storage.getColumn(
        rollup_config.getRollupInterval("1d").getTemporalTable(), 
        row, FAMILY, qualifier);
    final byte[] expected = {0x2A};
    assertArrayEquals(expected, value);
  }
  
  @Test
  public void addAggregatePoint1dInYearMid() throws Exception {
    final byte[] row = new byte[] { 0, 0, 1,
        0x50, (byte) 0xE2, (byte) 0x27, (byte) 0x00, 0, 0, 1, 0, 0, 1};
    RowKey.prefixKeyWithSalt(row);
    final byte[] qualifier = new byte[] {0x73, 0x75, 0x6D, 0x3A, 9, (byte) 0xC0};

    tsdb.addAggregatePoint(METRIC_STRING, 1370532925L, 42, tags, false,
        "1d", "sum").joinUninterruptibly();

    final byte[] value = storage.getColumn(
        rollup_config.getRollupInterval("1d").getTemporalTable(), 
        row, FAMILY, qualifier);
    final byte[] expected = {0x2A};
    assertArrayEquals(expected, value);
  }
  
  @Test
  public void addAggregatePoint1dInYearEnd() throws Exception {
    final byte[] row = new byte[] { 0, 0, 1,
        0x50, (byte) 0xE2, (byte) 0x27, (byte) 0x00, 0, 0, 1, 0, 0, 1};
    RowKey.prefixKeyWithSalt(row);
    final byte[] qualifier = new byte[] {0x73, 0x75, 0x6D, 0x3A, 0x16, (byte) 0xC0};

    tsdb.addAggregatePoint(METRIC_STRING, 1388534399L, 42, tags, false,
        "1d", "sum").joinUninterruptibly();

    final byte[] value = storage.getColumn(
        rollup_config.getRollupInterval("1d").getTemporalTable(), 
        row, FAMILY, qualifier);
    final byte[] expected = {0x2A};
    assertArrayEquals(expected, value);
  }
  
  @Test
  public void addAggregatePoint1dInYearOver() throws Exception {
    final byte[] row = new byte[] { 0, 0, 1,
        0x52, (byte) 0xC3, (byte) 0x5A, (byte) 0x80, 0, 0, 1, 0, 0, 1};
    RowKey.prefixKeyWithSalt(row);
    final byte[] qualifier = new byte[] {0x73, 0x75, 0x6D, 0x3A, 0, 0};

    tsdb.addAggregatePoint(METRIC_STRING, 1388534400L, 42, tags, false,
        "1d", "sum").joinUninterruptibly();

    final byte[] value = storage.getColumn(
        rollup_config.getRollupInterval("1d").getTemporalTable(), 
        row, FAMILY, qualifier);
    final byte[] expected = {0x2A};
    assertArrayEquals(expected, value);
  }
  
  @Test (expected = NoSuchUniqueName.class)
  public void addAggregatePointNSUNMetric() throws Exception {
    tsdb.addAggregatePoint(NSUN_METRIC, 1388534400L, 42, tags, false,
        "1d", "sum").joinUninterruptibly();
  }
  
  @Test (expected = NoSuchUniqueName.class)
  public void addAggregatePointNSUNTagK() throws Exception {
    tags.clear();
    tags.put(NSUN_TAGK, TAGV_STRING);
    tsdb.addAggregatePoint(METRIC_STRING, 1388534400L, 42, tags, false,
        "1d", "sum").joinUninterruptibly();
  }
  
  @Test (expected = NoSuchUniqueName.class)
  public void addAggregatePointNSUNTagV() throws Exception {
    tags.put(TAGK_STRING, NSUN_TAGV);
    tsdb.addAggregatePoint(METRIC_STRING, 1388534400L, 42, tags, false,
        "1d", "sum").joinUninterruptibly();
  }
  
  // This is allowed, we don't check the aggregation function in this method.
  // It's up to the RPC level to check
  @Test
  public void addAggregatePointRollupNoSuchAgg() throws Exception {

    tsdb.addAggregatePoint(METRIC_STRING, 1356998400, 42, tags, false, "10m", 
        "nosuchagg").joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1 };
    RowKey.prefixKeyWithSalt(row);
    final RollupInterval interval = rollup_config.getRollupInterval("10m");
    assertEquals(42, storage.getColumn(interval.getTemporalTable(), 
        row, FAMILY, RollupUtils.buildRollupQualifier(1356998400, (short)0, 
            "nosuchagg", interval))[0]);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void addAggregatePointRollupsNotConfigured() throws Exception {
    Whitebox.setInternalState(tsdb, "rollup_config", (RollupConfig)null);
    tsdb.addAggregatePoint(METRIC_STRING, 1356998400, 42, tags, false, "10m", 
        "nosuchagg").joinUninterruptibly();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void addAggregatePointNegativeTimestamp() throws Exception {
    tsdb.addAggregatePoint(METRIC_STRING, -1356998400, 42, tags, false, "10m", 
        "sum").joinUninterruptibly();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void addAggregatePointEmptyTags() throws Exception {
    tags.put(TAGK_STRING, "");
    tsdb.addAggregatePoint(METRIC_STRING, 1356998400, 42, tags, false, "10m", 
        "nosuchagg").joinUninterruptibly();
  }
  
  // not allowed at this time
  @Test (expected = IllegalArgumentException.class)
  public void addAggregatePointMS() throws Exception {
    tsdb.addAggregatePoint(METRIC_STRING, 1356998400000L, 42, tags, false, "10m", 
        "nosuchagg").joinUninterruptibly();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void addAggregatePointNullInterval() throws Exception {

    tsdb.addAggregatePoint(METRIC_STRING, 1356998400, 42, tags, false, null, 
        "sum").joinUninterruptibly();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void addAggregatePointEmptyInterval() throws Exception {
    tsdb.addAggregatePoint(METRIC_STRING, 1356998400, 42, tags, false, "", 
        "sum").joinUninterruptibly();
  }
  
  @Test (expected = NoSuchRollupForIntervalException.class)
  public void addAggregatePointIntervalNotConfigured() throws Exception {
    tsdb.addAggregatePoint(METRIC_STRING, 1356998400, 42, tags, false, "6h", 
        "sum").joinUninterruptibly();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void addAggregatePointNullAggregator() throws Exception {
    tsdb.addAggregatePoint(METRIC_STRING, 1356998400, 42, tags, false, "10m", 
        null).joinUninterruptibly();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void addAggregatePointEmptyAggregator() throws Exception {
    tsdb.addAggregatePoint(METRIC_STRING, 1356998400, 42, tags, false, "10m", 
        "").joinUninterruptibly();
  }
  
  @Test
  public void addAggregatePointRollupRouting() throws Exception {
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1 };
    RowKey.prefixKeyWithSalt(row);
    
    RollupInterval interval = rollup_config.getRollupInterval("10m");
    tsdb.addAggregatePoint(METRIC_STRING, 1356998400, 42, tags, false, "10m", 
        "sum").joinUninterruptibly();
    
    assertEquals(42, storage.getColumn(interval.getTemporalTable(), 
        row, FAMILY, RollupUtils.buildRollupQualifier(1356998400, (short)0, "sum", 
            interval))[0]);
    // make sure it didn't get into the tsdb table
    assertNull(storage.getColumn(TSDB_TABLE, 
        row, FAMILY, RollupUtils.buildRollupQualifier(1356998400, (short)0, "sum", 
            interval)));
    
    storage.flushStorage();
    
    interval = rollup_config.getRollupInterval("1h");
    tsdb.addAggregatePoint(METRIC_STRING, 1356998400, 42, tags, false, "1h", 
        "sum").joinUninterruptibly();
    
    assertEquals(42, storage.getColumn(interval.getTemporalTable(), 
        row, FAMILY, RollupUtils.buildRollupQualifier(1356998400, (short)0, "sum", 
            interval))[0]);
    assertNull(storage.getColumn(TSDB_TABLE, 
        row, FAMILY, RollupUtils.buildRollupQualifier(1356998400, (short)0, "sum", 
            interval)));
    assertNull(storage.getColumn(
        rollup_config.getRollupInterval("10m").getTemporalTable(), 
        row, FAMILY, RollupUtils.buildRollupQualifier(1356998400, (short)0, "sum", 
            rollup_config.getRollupInterval("10m"))));
    
    storage.flushStorage();
    
    interval = rollup_config.getRollupInterval("1d");
    tsdb.addAggregatePoint(METRIC_STRING, 1356998400, 42, tags, false, "1d", 
        "sum").joinUninterruptibly();
    
    assertEquals(42, storage.getColumn(interval.getTemporalTable(), 
        row, FAMILY, RollupUtils.buildRollupQualifier(1356998400, (short)0, "sum", 
            interval))[0]);
    assertNull(storage.getColumn(TSDB_TABLE, 
        row, FAMILY, RollupUtils.buildRollupQualifier(1356998400, (short)0, "sum", 
            interval)));
    assertNull(storage.getColumn(
        rollup_config.getRollupInterval("1h").getTemporalTable(), 
        row, FAMILY, RollupUtils.buildRollupQualifier(1356998400, (short)0, "sum", 
            rollup_config.getRollupInterval("1h"))));
    
    storage.flushStorage();
    // other aggs
    interval = rollup_config.getRollupInterval("1h");
    tsdb.addAggregatePoint(METRIC_STRING, 1356998400, 42, tags, false, "1h", 
        "max").joinUninterruptibly();
    assertEquals(42, storage.getColumn(interval.getTemporalTable(), 
        row, FAMILY, RollupUtils.buildRollupQualifier(1356998400, (short)0, "max", 
            interval))[0]);
    
    tsdb.addAggregatePoint(METRIC_STRING, 1356998400, 42, tags, false, "1h", 
        "min").joinUninterruptibly();
    assertEquals(42, storage.getColumn(interval.getTemporalTable(), 
        row, FAMILY, RollupUtils.buildRollupQualifier(1356998400, (short)0, "min", 
            interval))[0]);
    
    tsdb.addAggregatePoint(METRIC_STRING, 1356998400, 42, tags, false, "1h", 
        "count").joinUninterruptibly();
    assertEquals(42, storage.getColumn(interval.getTemporalTable(), 
        row, FAMILY, RollupUtils.buildRollupQualifier(1356998400, (short)0, "count", 
            interval))[0]);
    
    tsdb.addAggregatePoint(METRIC_STRING, 1356998400, 42, tags, false, "1h", 
        "avg").joinUninterruptibly();
    assertEquals(42, storage.getColumn(interval.getTemporalTable(), 
        row, FAMILY, RollupUtils.buildRollupQualifier(1356998400, (short)0, "avg", 
            interval))[0]);
  }
  
  @Test
  public void addAggregatePointLongs() throws Exception {
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1 };
    RowKey.prefixKeyWithSalt(row);
    final RollupInterval interval = rollup_config.getRollupInterval("10m");
    
    // 1 byte
    tsdb.addAggregatePoint(METRIC_STRING, 1356998400, 0, tags, false, "10m", 
        "sum").joinUninterruptibly();
    assertEquals(0, storage.getColumn(interval.getTemporalTable(), 
        row, FAMILY, RollupUtils.buildRollupQualifier(1356998400, (short)0, "sum", 
            interval))[0]);
    
    tsdb.addAggregatePoint(METRIC_STRING, 1356998400, 42, tags, false, "10m", 
        "sum").joinUninterruptibly();
    assertEquals(42, storage.getColumn(interval.getTemporalTable(), 
        row, FAMILY, RollupUtils.buildRollupQualifier(1356998400, (short)0, "sum", 
            interval))[0]);
    
    tsdb.addAggregatePoint(METRIC_STRING, 1356998400, -42, tags, false, "10m", 
        "sum").joinUninterruptibly();
    assertEquals(-42, storage.getColumn(interval.getTemporalTable(), 
        row, FAMILY, RollupUtils.buildRollupQualifier(1356998400, (short)0, "sum", 
            interval))[0]);
    
    // 2 bytes
    tsdb.addAggregatePoint(METRIC_STRING, 1356998400, 257, tags, false, "10m", 
        "sum").joinUninterruptibly();
    assertEquals(257, Bytes.getShort(storage.getColumn(interval.getTemporalTable(), 
        row, FAMILY, RollupUtils.buildRollupQualifier(1356998400, (short)1, "sum", 
            interval))));
    
    tsdb.addAggregatePoint(METRIC_STRING, 1356998400, -257, tags, false, "10m", 
        "sum").joinUninterruptibly();
    assertEquals(-257, Bytes.getShort(storage.getColumn(interval.getTemporalTable(), 
        row, FAMILY, RollupUtils.buildRollupQualifier(1356998400, (short)1, "sum", 
            interval))));
    
    // 4 bytes
    tsdb.addAggregatePoint(METRIC_STRING, 1356998400, 65537, tags, false, "10m", 
        "sum").joinUninterruptibly();
    assertEquals(65537, Bytes.getInt(storage.getColumn(interval.getTemporalTable(), 
        row, FAMILY, RollupUtils.buildRollupQualifier(1356998400, (short)3, "sum", 
            interval))));
    
    tsdb.addAggregatePoint(METRIC_STRING, 1356998400, -65537, tags, false, "10m", 
        "sum").joinUninterruptibly();
    assertEquals(-65537, Bytes.getInt(storage.getColumn(interval.getTemporalTable(), 
        row, FAMILY, RollupUtils.buildRollupQualifier(1356998400, (short)3, "sum", 
            interval))));
    
    // 8 bytes
    tsdb.addAggregatePoint(METRIC_STRING, 1356998400, 4294967296L, tags, false, "10m", 
        "sum").joinUninterruptibly();
    assertEquals(4294967296L, Bytes.getLong(storage.getColumn(interval.getTemporalTable(), 
        row, FAMILY, RollupUtils.buildRollupQualifier(1356998400, (short)7, "sum", 
            interval))));
    
    tsdb.addAggregatePoint(METRIC_STRING, 1356998400, -4294967296L, tags, false, "10m", 
        "sum").joinUninterruptibly();
    assertEquals(-4294967296L, Bytes.getLong(storage.getColumn(interval.getTemporalTable(), 
        row, FAMILY, RollupUtils.buildRollupQualifier(1356998400, (short)7, "sum", 
            interval))));
  }
  
  @Test
  public void addAggregatePointFloats() throws Exception {
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1 };
    RowKey.prefixKeyWithSalt(row);
    final RollupInterval interval = rollup_config.getRollupInterval("10m");
    
    tsdb.addAggregatePoint(METRIC_STRING, 1356998400, 0.0F, tags, false, "10m", 
        "sum").joinUninterruptibly();
    assertEquals(0.0, Float.intBitsToFloat(Bytes.getInt(
        storage.getColumn(interval.getTemporalTable(), 
        row, FAMILY, RollupUtils.buildRollupQualifier(1356998400, (short)11, "sum", 
            interval)))), 0.0001);
    
    tsdb.addAggregatePoint(METRIC_STRING, 1356998400, 42.5F, tags, false, "10m", 
        "sum").joinUninterruptibly();
    assertEquals(42.5, Float.intBitsToFloat(Bytes.getInt(
        storage.getColumn(interval.getTemporalTable(), 
        row, FAMILY, RollupUtils.buildRollupQualifier(1356998400, (short)11, "sum", 
            interval)))), 0.0001);
    
    tsdb.addAggregatePoint(METRIC_STRING, 1356998400, -42.5F, tags, false, "10m", 
        "sum").joinUninterruptibly();
    assertEquals(-42.5, Float.intBitsToFloat(Bytes.getInt(
        storage.getColumn(interval.getTemporalTable(), 
        row, FAMILY, RollupUtils.buildRollupQualifier(1356998400, (short)11, "sum", 
            interval)))), 0.0001);
    
    tsdb.addAggregatePoint(METRIC_STRING, 1356998400, 42.5123459999F, tags, false, "10m", 
        "sum").joinUninterruptibly();
    assertEquals(42.5123459999F, Float.intBitsToFloat(Bytes.getInt(
        storage.getColumn(interval.getTemporalTable(), 
        row, FAMILY, RollupUtils.buildRollupQualifier(1356998400, (short)11, "sum", 
            interval)))), 0.0000001);
  }
  
  @Test
  public void addAggregatePointGroupByRollupRouting() throws Exception {
    byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1, 0, 0, 0x2A, 0, 0, 0x2A };
    RowKey.prefixKeyWithSalt(row);
    
    RollupInterval interval = rollup_config.getRollupInterval("10m");
    tsdb.addAggregatePoint(METRIC_STRING, 1356998400, 42, tags, true, "10m", 
        "sum").joinUninterruptibly();

    assertEquals(42, storage.getColumn(interval.getGroupbyTable(), 
        row, FAMILY, RollupUtils.buildRollupQualifier(1356998400, (short)0, "sum", 
            interval))[0]);
    // make sure it didn't get into the tsdb table OR rollup table
    assertNull(storage.getColumn(TSDB_TABLE, 
        row, FAMILY, RollupUtils.buildRollupQualifier(1356998400, (short)0, "sum", 
            interval)));
    assertNull(storage.getColumn(interval.getTemporalTable(), 
        row, FAMILY, RollupUtils.buildRollupQualifier(1356998400, (short)0, "sum", 
            interval)));
    
    storage.flushStorage();
    
    interval = rollup_config.getRollupInterval("1h");
    tsdb.addAggregatePoint(METRIC_STRING, 1356998400, 42, tags, true, "1h", 
        "sum").joinUninterruptibly();
    
    assertEquals(42, storage.getColumn(interval.getGroupbyTable(), 
        row, FAMILY, RollupUtils.buildRollupQualifier(1356998400, (short)0, "sum", 
            interval))[0]);
    assertNull(storage.getColumn(TSDB_TABLE, 
        row, FAMILY, RollupUtils.buildRollupQualifier(1356998400, (short)0, "sum", 
            interval)));
    assertNull(storage.getColumn(interval.getTemporalTable(), 
        row, FAMILY, RollupUtils.buildRollupQualifier(1356998400, (short)0, "sum", 
            interval)));
    
    storage.flushStorage();
    
    interval = rollup_config.getRollupInterval("1d");
    tsdb.addAggregatePoint(METRIC_STRING, 1356998400, 42, tags, true, "1d", 
        "sum").joinUninterruptibly();
    
    assertEquals(42, storage.getColumn(interval.getGroupbyTable(), 
        row, FAMILY, RollupUtils.buildRollupQualifier(1356998400, (short)0, "sum", 
            interval))[0]);
    assertNull(storage.getColumn(TSDB_TABLE, 
        row, FAMILY, RollupUtils.buildRollupQualifier(1356998400, (short)0, "sum", 
            interval)));
    assertNull(storage.getColumn(interval.getTemporalTable(), 
        row, FAMILY, RollupUtils.buildRollupQualifier(1356998400, (short)0, "sum", 
            interval)));
    
    storage.flushStorage();
    // other aggs
    row[row.length-1] = 0x2B;
    RowKey.prefixKeyWithSalt(row);
    interval = rollup_config.getRollupInterval("1h");
    tsdb.addAggregatePoint(METRIC_STRING, 1356998400, 42, tags, true, "1h", 
        "max").joinUninterruptibly();
    assertEquals(42, storage.getColumn(interval.getGroupbyTable(), 
        row, FAMILY, RollupUtils.buildRollupQualifier(1356998400, (short)0, "max", 
            interval))[0]);
    
    row[row.length-1] = 0x2C;
    RowKey.prefixKeyWithSalt(row);
    tsdb.addAggregatePoint(METRIC_STRING, 1356998400, 42, tags, true, "1h", 
        "min").joinUninterruptibly();
    assertEquals(42, storage.getColumn(interval.getGroupbyTable(), 
        row, FAMILY, RollupUtils.buildRollupQualifier(1356998400, (short)0, "min", 
            interval))[0]);
    
    row[row.length-1] = 0x2D;
    RowKey.prefixKeyWithSalt(row);
    tsdb.addAggregatePoint(METRIC_STRING, 1356998400, 42, tags, true, "1h", 
        "count").joinUninterruptibly();
    assertEquals(42, storage.getColumn(interval.getGroupbyTable(), 
        row, FAMILY, RollupUtils.buildRollupQualifier(1356998400, (short)0, "count", 
            interval))[0]);
    
    row[row.length-1] = 0x2E;
    RowKey.prefixKeyWithSalt(row);
    tsdb.addAggregatePoint(METRIC_STRING, 1356998400, 42, tags, true, "1h", 
        "avg").joinUninterruptibly();
    assertEquals(42, storage.getColumn(interval.getGroupbyTable(), 
        row, FAMILY, RollupUtils.buildRollupQualifier(1356998400, (short)0, "avg", 
            interval))[0]);
  }
  
  //This is allowed, we don't check the aggregation function in this method.
  // It's up to the RPC level to check
  @Test
  public void addAggregatePointGroupByRollupNoSuchAgg() throws Exception {
    tsdb.addAggregatePoint(METRIC_STRING, 1356998400, 42, tags, true, "10m", 
        "nosuchagg").joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1, 0, 0, 0x2A, 0, 0, 0x2F };
    RowKey.prefixKeyWithSalt(row);
    final RollupInterval interval = rollup_config.getRollupInterval("10m");
    assertEquals(42, storage.getColumn(interval.getGroupbyTable(), 
        row, FAMILY, RollupUtils.buildRollupQualifier(1356998400, (short)0, 
            "nosuchagg", interval))[0]);
  }
 
  //This is allowed, we don't check the aggregation function in this method.
  // It's up to the RPC level to check
  @Test
  public void addAggregatePointGroupByNoSuchAgg() throws Exception {
    tsdb.addAggregatePoint(METRIC_STRING, 1356998400, 42, tags, true, null, 
        "nosuchagg").joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1, 0, 0, 0x2A, 0, 0, 0x2F  };
    RowKey.prefixKeyWithSalt(row);
    assertEquals(42, storage.getColumn(AGG_TABLE, row, FAMILY, 
        new byte[] { 0, 0 })[0]);
  }
  
  @Test
  public void addAggregatePointGroupBy() throws Exception {
    tsdb.addAggregatePoint(METRIC_STRING, 1356998400, 42, tags, true, null, 
        "sum").joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1, 0, 0, 0x2A, 0, 0, 0x2A };
    RowKey.prefixKeyWithSalt(row);
    assertEquals(42, storage.getColumn(AGG_TABLE, row, FAMILY, 
        new byte[] { 0, 0 })[0]);
    assertNull(storage.getColumn(TSDB_TABLE, row, FAMILY, 
        new byte[] { 0, 0 }));
    assertNull(storage.getColumn(
        rollup_config.getRollupInterval("10m").getGroupbyTable(), row, FAMILY, 
        new byte[] { 0, 0 }));
    
    tsdb.addAggregatePoint(METRIC_STRING, 1356998400, 24, tags, true, null, 
        "count").joinUninterruptibly();
    row[row.length-1] = 0x2D;
    RowKey.prefixKeyWithSalt(row);
    assertEquals(24, storage.getColumn(AGG_TABLE, row, FAMILY, 
        new byte[] { 0, 0 })[0]);
    assertNull(storage.getColumn(TSDB_TABLE, row, FAMILY, 
        new byte[] { 0, 0 }));
    assertNull(storage.getColumn(
        rollup_config.getRollupInterval("10m").getGroupbyTable(), row, FAMILY, 
        new byte[] { 0, 0 }));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void addAggregatePointGroupByRollupsDisabled() throws Exception {
    Whitebox.setInternalState(tsdb, "rollup_config", (Object) null);
    tsdb.addAggregatePoint(METRIC_STRING, 1356998400L, 42, tags, false,
        "10m", "sum").joinUninterruptibly();
  }
  
  @Test
  public void dpFilterOK() throws Exception {
    final WriteableDataPointFilterPlugin filter = 
        mock(WriteableDataPointFilterPlugin.class);
    when(filter.filterDataPoints()).thenReturn(true);
    when(filter.allowDataPoint(eq(METRIC_STRING), anyLong(), 
        any(byte[].class), eq(tags), anyShort()))
      .thenReturn(Deferred.<Boolean>fromResult(true));
    Whitebox.setInternalState(tsdb, "ts_filter", filter);
    
    tsdb.addAggregatePoint(METRIC_STRING, 1356998400L, 42, tags, false,
        "10m", "sum").joinUninterruptibly();
    
    final byte[] row = new byte[] { 0, 0, 1, 
        0x50, (byte) 0xE2, 0x27, 0, 0, 0, 1, 0, 0, 1};
    RowKey.prefixKeyWithSalt(row);
    final byte[] qualifier = new byte[] {0x73, 0x75, 0x6D, 0x3A, 0, 0};
    final byte[] value = storage.getColumn(
        rollup_config.getRollupInterval("10m").getTemporalTable(), 
        row, FAMILY, qualifier);
    final byte[] expected = {0x2A};
    assertArrayEquals(expected, value);
  }
  
  @Test
  public void uidFilterBlocked() throws Exception {
    final WriteableDataPointFilterPlugin filter = 
        mock(WriteableDataPointFilterPlugin.class);
    when(filter.filterDataPoints()).thenReturn(true);
    when(filter.allowDataPoint(eq(METRIC_STRING), anyLong(), 
        any(byte[].class), eq(tags), anyShort()))
      .thenReturn(Deferred.<Boolean>fromResult(false));
    Whitebox.setInternalState(tsdb, "ts_filter", filter);
    
    tsdb.addAggregatePoint(METRIC_STRING, 1356998400L, 42, tags, false,
        "10m", "sum").joinUninterruptibly();
    
    final byte[] row = new byte[] { 0, 0, 1, 
        0x50, (byte) 0xE2, 0x27, 0, 0, 0, 1, 0, 0, 1};
    RowKey.prefixKeyWithSalt(row);
    final byte[] qualifier = new byte[] {0x73, 0x75, 0x6D, 0x3A, 0, 0};
    final byte[] value = storage.getColumn(
        rollup_config.getRollupInterval("10m").getTemporalTable(), 
        row, FAMILY, qualifier);
    assertNull(value);
  }
  
  @Test
  public void dpFilterReturnsException() throws Exception {
    final WriteableDataPointFilterPlugin filter = 
        mock(WriteableDataPointFilterPlugin.class);
    when(filter.filterDataPoints()).thenReturn(true);
    when(filter.allowDataPoint(eq(METRIC_STRING), anyLong(), 
        any(byte[].class), eq(tags), anyShort()))
      .thenReturn(Deferred.<Boolean>fromError(new UnitTestException("Boo!")));
    Whitebox.setInternalState(tsdb, "ts_filter", filter);
    
    final Deferred<Object> deferred = tsdb.addAggregatePoint(METRIC_STRING, 
        1356998400L, 42, tags, false, "10m", "sum");
    
    try {
      deferred.join();
      fail("Expected an UnitTestException");
    } catch (UnitTestException e) { };
    final byte[] row = new byte[] { 0, 0, 1, 
        0x50, (byte) 0xE2, 0x27, 0, 0, 0, 1, 0, 0, 1};
    RowKey.prefixKeyWithSalt(row);
    final byte[] qualifier = new byte[] {0x73, 0x75, 0x6D, 0x3A, 0, 0};
    final byte[] value = storage.getColumn(
        rollup_config.getRollupInterval("10m").getTemporalTable(), 
        row, FAMILY, qualifier);
    assertNull(value);
  }
  
  @Test
  public void dpFilterThrowsException() throws Exception {
    final WriteableDataPointFilterPlugin filter = 
        mock(WriteableDataPointFilterPlugin.class);
    when(filter.filterDataPoints()).thenReturn(true);
    when(filter.allowDataPoint(eq(METRIC_STRING), anyLong(), 
        any(byte[].class), eq(tags), anyShort()))
      .thenThrow(new UnitTestException("Boo!"));
    Whitebox.setInternalState(tsdb, "ts_filter", filter);
    
    try {
      tsdb.addAggregatePoint(METRIC_STRING, 
          1356998400L, 42, tags, false, "10m", "sum");
      fail("Expected an UnitTestException");
    } catch (UnitTestException e) { };
    final byte[] row = new byte[] { 0, 0, 1, 
        0x50, (byte) 0xE2, 0x27, 0, 0, 0, 1, 0, 0, 1};
    RowKey.prefixKeyWithSalt(row);
    final byte[] qualifier = new byte[] {0x73, 0x75, 0x6D, 0x3A, 0, 0};
    final byte[] value = storage.getColumn(
        rollup_config.getRollupInterval("10m").getTemporalTable(), 
        row, FAMILY, qualifier);
    assertNull(value);
  }
}
