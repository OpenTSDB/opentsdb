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
package net.opentsdb.rollup;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import net.opentsdb.core.Aggregators;
import net.opentsdb.core.Const;
import net.opentsdb.core.DataPoint;
import net.opentsdb.core.IllegalDataException;
import net.opentsdb.core.Internal;
import net.opentsdb.core.RowKey;
import net.opentsdb.core.SeekableView;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.TestRowSeq;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.Config;

import org.hbase.async.Bytes;
import org.hbase.async.KeyValue;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import com.stumbleupon.async.Deferred;

import java.util.Arrays;

@RunWith(PowerMockRunner.class)
//"Classloader hell"...  It's real.  Tell PowerMock to ignore these classes
//because they fiddle with the class loader.  We don't test them anyway.
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
               "ch.qos.*", "org.slf4j.*",
               "com.sum.*", "org.xml.*"})
@PrepareForTest({ RollupSeq.class, TSDB.class, UniqueId.class, KeyValue.class, 
  Config.class, RowKey.class, Const.class })
public final class TestRollupSeq {
  private TSDB tsdb = mock(TSDB.class);
  private Config config = mock(Config.class);
  private UniqueId metrics = mock(UniqueId.class);
  private static final byte[] TABLE = { 't', 'a', 'b', 'l', 'e' };
  private static final byte[] KEY = 
      new byte[] { 0, 0, 1, 0x50, (byte)0xE2, 0x27, 0, 0, 0, 1, 0, 0, 2 };
  public static final byte[] FAMILY = { 't' };
  private static final RollupQuery rollup_query_sum = 
    new RollupQuery(new RollupInterval("tsdb", "tsdb-agg", "1s", "1h"), 
    Aggregators.SUM, 1000);
  private static final RollupQuery rollup_query_avg = 
      new RollupQuery(new RollupInterval("tsdb", "tsdb-agg", "1s", "1h"), 
      Aggregators.AVG, 1000);
  private static final RollupQuery rollup_query_sum_mimmax = 
    new RollupQuery(new RollupInterval("tsdb", "tsdb-agg", "1s", "1h"), 
    Aggregators.MIMMAX, 1000);
  private static final RollupQuery rollup_query_10m_sum = 
    new RollupQuery(new RollupInterval("tsdb", "tsdb-agg", "10m", "6h"), 
    Aggregators.SUM, 600000);
  private static final RollupQuery rollup_query_10m_avg = 
      new RollupQuery(new RollupInterval("tsdb", "tsdb-agg", "10m", "6h"), 
      Aggregators.AVG, 600000);
  private static final RollupQuery rollup_query_10m_count = 
      new RollupQuery(new RollupInterval("tsdb", "tsdb-agg", "10m", "6h"), 
      Aggregators.COUNT, 600000);
  private static final RollupQuery rollup_query_1h_sum = 
    new RollupQuery(new RollupInterval("tsdb", "tsdb-agg", "1h", "1d"), 
    Aggregators.SUM, 3600000);
  private static final RollupQuery rollup_query_1h_avg = 
      new RollupQuery(new RollupInterval("tsdb", "tsdb-agg", "1h", "1d"), 
      Aggregators.AVG, 3600000);
  private static final RollupQuery rollup_query_1h_count = 
      new RollupQuery(new RollupInterval("tsdb", "tsdb-agg", "1h", "1d"), 
      Aggregators.COUNT, 3600000);
  
  @Before
  public void before() throws Exception {
    // Inject the attributes we need into the "tsdb" object.
    Whitebox.setInternalState(tsdb, "metrics", metrics);
    Whitebox.setInternalState(tsdb, "table", TABLE);
    Whitebox.setInternalState(tsdb, "config", config);
    when(tsdb.getConfig()).thenReturn(config);
    when(RowKey.metricNameAsync(tsdb, TestRowSeq.KEY))
      .thenReturn(Deferred.fromResult("sys.cpu.user"));
  }
  
  @Test
  public void setRow() throws Exception {
    final KeyValue kv = getRollupKeyValue(1356998400000L, 4L, rollup_query_sum);
    final RollupSeq rs = new RollupSeq(tsdb, rollup_query_sum);
    rs.setRow(kv);
    assertEquals(1, rs.size());
    final SeekableView it = rs.iterator();
    assertTrue(it.hasNext());
    DataPoint dp = it.next();
    assertTrue(dp.isInteger());
    assertEquals(4, dp.longValue());
    assertEquals(1356998400000L, dp.timestamp());
    assertEquals(-1, dp.valueCount());
    assertFalse(it.hasNext());
  }
   
  @Test (expected = IllegalStateException.class)
  public void setRowAlreadySet() throws Exception {
    final KeyValue kv = getRollupKeyValue(1356998400000L, 4L, rollup_query_sum);
    
    final RollupSeq rs = new RollupSeq(tsdb, rollup_query_sum);
    rs.setRow(kv);
    assertEquals(1, rs.size());
    //Expects an IllegalStateException
    final KeyValue kv1 = getRollupKeyValue(1356998500000L, 5L, rollup_query_sum);
    rs.setRow(kv1);
  }
  
  @Test
  public void addRow() throws Exception {
    final KeyValue kv1 = getRollupKeyValue(1356998400000L, 4L, rollup_query_sum);
    final KeyValue kv2 = getRollupKeyValue(1356998500000L, 5L, rollup_query_sum);

    final RollupSeq rs = new RollupSeq(tsdb, rollup_query_sum);
    rs.setRow(kv1);
    assertEquals(1, rs.size());
    
    rs.addRow(kv2);
    assertEquals(2, rs.size());
    
    final SeekableView it = rs.iterator();
    assertTrue(it.hasNext());
    DataPoint dp = it.next();
    assertTrue(dp.isInteger());
    assertEquals(1356998400000L, dp.timestamp());
    assertEquals(4, dp.longValue());
    assertEquals(-1, dp.valueCount());
    
    assertTrue(it.hasNext());
    dp = it.next();
    assertTrue(dp.isInteger());
    assertEquals(1356998500000L, dp.timestamp());
    assertEquals(5, dp.longValue());
    assertEquals(-1, dp.valueCount());
    assertFalse(it.hasNext());
  }
  
  // This should never happen
  @Test
  public void addRowMergeDifferentSalt() throws Exception {
    PowerMockito.mockStatic(Const.class);
    PowerMockito.when(Const.SALT_WIDTH()).thenReturn(1);
    PowerMockito.when(Const.SALT_BUCKETS()).thenReturn(20);
     
    final byte[] key = new byte[TestRowSeq.KEY.length + 1];
    key[0] = 1;
    System.arraycopy(TestRowSeq.KEY, 0, key, 1, TestRowSeq.KEY.length);
    final byte[] qual1 = { 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x17 };
    final byte[] val2 = Bytes.fromLong(5L);
    final RollupSeq rs = new RollupSeq(tsdb, rollup_query_sum);
    rs.setRow(TestRowSeq.makekv(qual1, val1));
    rs.addRow(TestRowSeq.makekv(qual2, val2));
    assertEquals(2, rs.size());
    
    final byte[] qual3 = { 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x27 };
    final byte[] val3 = Bytes.fromLong(6L);
    final byte[] qual4 = { 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x37 };
    final byte[] val4 = Bytes.fromLong(7L);
    final byte[] key2 = Arrays.copyOf(key, key.length);
    key2[0] = 2;
    rs.addRow(TestRowSeq.makekv(key2, qual3, val3));
    rs.addRow(TestRowSeq.makekv(key2, qual4, val4));

    assertEquals(4, rs.size());
    
    final SeekableView it = rs.iterator();
    long value = 4;
    long ts = 1356998400000L;
    while (it.hasNext()) {
      final DataPoint dp = it.next();
      assertEquals(ts, dp.timestamp());
      assertTrue(dp.isInteger());
      assertEquals(value, dp.longValue());
      assertEquals(-1, dp.valueCount());
      ++value;
      ts += 1000;
    }
  }
  
  @Test
  public void addRowMergeLater() throws Exception {
    // this happens if the same row key is used for the addRow call
    final byte[] qual1 = { 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x17 };
    final byte[] val2 = Bytes.fromLong(5L);
    final RollupSeq rs = new RollupSeq(tsdb, rollup_query_sum);
    rs.setRow(TestRowSeq.makekv(qual1, val1));
    rs.addRow(TestRowSeq.makekv(qual2, val2));
    assertEquals(2, rs.size());
    
    final byte[] qual3 = { 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x27 };
    final byte[] val3 = Bytes.fromLong(6L);
    final byte[] qual4 = { 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x37 };
    final byte[] val4 = Bytes.fromLong(7L);
    rs.addRow(TestRowSeq.makekv(qual3, val3));
    rs.addRow(TestRowSeq.makekv(qual4, val4));
    
    assertEquals(4, rs.size());
    final SeekableView it = rs.iterator();
    long value = 4;
    long ts = 1356998400000L;
    while (it.hasNext()) {
      final DataPoint dp = it.next();
      assertEquals(ts, dp.timestamp());
      assertTrue(dp.isInteger());
      assertEquals(value, dp.longValue());
      assertEquals(-1, dp.valueCount());
      ++value;
      ts += 1000;
    }
  }
  
  @Test (expected = IllegalDataException.class)
  public void addRowMergeEarlier() throws Exception {
    // this happens if the same row key is used for the addRow call
    final byte[] qual1 = { 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x27 };
    final byte[] val1 = Bytes.fromLong(6L);
    final byte[] qual2 = { 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x37 };
    final byte[] val2 = Bytes.fromLong(7L);
    final RollupSeq rs = new RollupSeq(tsdb, rollup_query_sum);
    rs.setRow(TestRowSeq.makekv(qual1, val1));
    rs.addRow(TestRowSeq.makekv(qual2, val2));
    assertEquals(2, rs.size());
    
    final byte[] qual3 = { 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x07 };
    final byte[] val3 = Bytes.fromLong(4L);
    rs.addRow(TestRowSeq.makekv( qual3, val3));
  }
  
  @Test (expected = IllegalDataException.class)
  public void addRowMergeMiddle() throws Exception {
    // this happens if the same row key is used for the addRow call
    final byte[] qual1 = { 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x17 };
    final byte[] val2 = Bytes.fromLong(5L);
    final RollupSeq rs = new RollupSeq(tsdb, rollup_query_sum);
    rs.setRow(TestRowSeq.makekv(qual1, val1));
    rs.addRow(TestRowSeq.makekv(qual2, val2));
    assertEquals(2, rs.size());
    
    final byte[] qual3 = { 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x47 };
    final byte[] val3 = Bytes.fromLong(8L);
    final byte[] qual4 = { 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x57 };
    final byte[] val4 = Bytes.fromLong(9L);
    rs.addRow(TestRowSeq.makekv( qual3, val3));
    rs.addRow(TestRowSeq.makekv(qual4, val4));
    assertEquals(4, rs.size());
    
    final byte[] qual5 = { 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x27 };
    final byte[] val5 = Bytes.fromLong(6L);
    rs.addRow(TestRowSeq.makekv( qual5, val5));
  }
  
  @Test (expected = IllegalDataException.class)
  public void addRowMergeDuplicateLater() throws Exception {
    // this happens if the same row key is used for the addRow call
    final byte[] qual1 = { 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x17 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual3 = { 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x27 };
    final byte[] val3 = Bytes.fromLong(6L);
    final RollupSeq rs = new RollupSeq(tsdb, rollup_query_sum);
    rs.setRow(TestRowSeq.makekv(qual1, val1));
    rs.addRow(TestRowSeq.makekv(qual2, val2));
    rs.addRow(TestRowSeq.makekv(qual3, val3));
    assertEquals(3, rs.size());
    rs.addRow(TestRowSeq.makekv(qual3, val3));
  }
  
  @Test
  public void addRowMergeDuplicateLaterRepair() throws Exception {
    when(config.fix_duplicates()).thenReturn(true);
    
    // this happens if the same row key is used for the addRow call
    final byte[] qual1 = { 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x17 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual3 = { 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x27 };
    final byte[] val3 = Bytes.fromLong(6L);
    final RollupSeq rs = new RollupSeq(tsdb, rollup_query_sum);
    rs.setRow(new KeyValue(KEY, FAMILY, qual1, 1, val1));
    rs.addRow(new KeyValue(KEY, FAMILY, qual2, 2, val2));
    rs.addRow(new KeyValue(KEY, FAMILY, qual3, 3, val3));
    assertEquals(3, rs.size());
    SeekableView it = rs.iterator();
    long ts = 1356998400000L;
    double value = 4.0;
    while(it.hasNext()) {
      final DataPoint dp = it.next();
      assertEquals(ts, dp.timestamp());
      assertEquals(value, dp.toDouble(), 0.0001);
      ts += 1000;
      ++value;
    }
    rs.addRow(new KeyValue(KEY, FAMILY, qual3, 8, Bytes.fromLong(7L)));
    assertEquals(3, rs.size());

    it = rs.iterator();
    ts = 1356998400000L;
    value = 4.0;
    while(it.hasNext()) {
      final DataPoint dp = it.next();
      assertEquals(ts, dp.timestamp());
      assertEquals(value, dp.toDouble(), 0.0001);
      ts += 1000;
      if (value >= 5) {
        value = 7.0;
      } else {
        ++value;
      }
    }
  }
  
  @Test (expected = IllegalDataException.class)
  public void addRowMergeDuplicateEarlier() throws Exception {
    // this happens if the same row key is used for the addRow call
    final byte[] qual4 = { 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x17 };
    final byte[] val4 = Bytes.fromLong(5L);
    final byte[] qual1 = { 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x27 };
    final byte[] val1 = Bytes.fromLong(6L);
    final byte[] qual2 = { 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x37 };
    final byte[] val2 = Bytes.fromLong(7L);
    final RollupSeq rs = new RollupSeq(tsdb, rollup_query_sum);
    rs.setRow(TestRowSeq.makekv(qual4, val4));
    rs.addRow(TestRowSeq.makekv(qual1, val1));
    rs.addRow(TestRowSeq.makekv(qual2, val2));
    assertEquals(3, rs.size());
    
    final byte[] qual3 = { 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x07 };
    final byte[] val3 = Bytes.fromLong(4L);
    rs.addRow(TestRowSeq.makekv(qual3, val3));
  }
  
  @Test
  public void addRowMergeDuplicateEarlierRepair() throws Exception {
    when(config.fix_duplicates()).thenReturn(true);
    // this happens if the same row key is used for the addRow call
    final byte[] qual1 = { 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x17 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual3 = { 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x27 };
    final byte[] val3 = Bytes.fromLong(6L);
    final RollupSeq rs = new RollupSeq(tsdb, rollup_query_sum);
    rs.setRow(new KeyValue(KEY, FAMILY, qual1, 1, val1));
    rs.addRow(new KeyValue(KEY, FAMILY, qual2, 2, val2));
    rs.addRow(new KeyValue(KEY, FAMILY, qual3, 3, val3));
    assertEquals(3, rs.size());
    SeekableView it = rs.iterator();
    long ts = 1356998400000L;
    double value = 4.0;
    while(it.hasNext()) {
      final DataPoint dp = it.next();
      assertEquals(ts, dp.timestamp());
      assertEquals(value, dp.toDouble(), 0.0001);
      ts += 1000;
      ++value;
    }
    
    rs.addRow(new KeyValue(KEY, FAMILY, qual3, 1, Bytes.fromLong(7L)));
    assertEquals(3, rs.size());
    
    it = rs.iterator();
    ts = 1356998400000L;
    value = 4.0;
    while(it.hasNext()) {
      final DataPoint dp = it.next();
      assertEquals(ts, dp.timestamp());
      assertEquals(value, dp.toDouble(), 0.0001);
      ts += 1000;
      ++value;
    }
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void addRowMergeDuplicateCountLater() throws Exception {
    // this happens if the same row key is used for the addRow call
    final byte[] qual1 = { 0x63, 0x6F, 0x75, 0x6E, 0x74, 0x3A, 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x63, 0x6F, 0x75, 0x6E, 0x74, 0x3A, 0x00, 0x17 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual3 = { 0x63, 0x6F, 0x75, 0x6E, 0x74,  0x3A, 0x00, 0x27 };
    final byte[] val3 = Bytes.fromLong(6L);
    final RollupSeq rs = new RollupSeq(tsdb, rollup_query_avg);
    rs.setRow(TestRowSeq.makekv(qual1, val1));
    rs.addRow(TestRowSeq.makekv(qual2, val2));
    rs.addRow(TestRowSeq.makekv(qual3, val3));
    assertEquals(0, rs.size());
    rs.addRow(TestRowSeq.makekv(qual3, val3));
  }
  
  @Test
  public void addRowMergeDuplicateCountLaterRepair() throws Exception {
    when(config.fix_duplicates()).thenReturn(true);
    // this happens if the same row key is used for the addRow call
    final byte[] qual1 = { 0x63, 0x6F, 0x75, 0x6E, 0x74, 0x3A, 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x63, 0x6F, 0x75, 0x6E, 0x74, 0x3A, 0x00, 0x17 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual3 = { 0x63, 0x6F, 0x75, 0x6E, 0x74,  0x3A, 0x00, 0x27 };
    final byte[] val3 = Bytes.fromLong(6L);
    final RollupSeq rs = new RollupSeq(tsdb, rollup_query_avg);
    rs.setRow(new KeyValue(KEY, FAMILY, qual1, 1, val1));
    rs.addRow(new KeyValue(KEY, FAMILY, qual2, 2, val2));
    rs.addRow(new KeyValue(KEY, FAMILY, qual3, 3, val3));
    assertEquals(6, rs.count_values[23]);
    assertEquals(0, rs.size());
    
    rs.addRow(new KeyValue(KEY, FAMILY, qual3, 8, Bytes.fromLong(7L)));
    assertEquals(7, rs.count_values[23]);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void addRowMergeDuplicateCountEarlier() throws Exception {
    // this happens if the same row key is used for the addRow call
    final byte[] qual4 = { 0x63, 0x6F, 0x75, 0x6E, 0x74, 0x3A, 0x00, 0x17 };
    final byte[] val4 = Bytes.fromLong(5L);
    final byte[] qual1 = { 0x63, 0x6F, 0x75, 0x6E, 0x74, 0x3A, 0x00, 0x27 };
    final byte[] val1 = Bytes.fromLong(6L);
    final byte[] qual2 = { 0x63, 0x6F, 0x75, 0x6E, 0x74, 0x3A, 0x00, 0x37 };
    final byte[] val2 = Bytes.fromLong(7L);
    final RollupSeq rs = new RollupSeq(tsdb, rollup_query_avg);
    rs.setRow(TestRowSeq.makekv(qual4, val4));
    rs.addRow(TestRowSeq.makekv(qual1, val1));
    rs.addRow(TestRowSeq.makekv(qual2, val2));
    assertEquals(0, rs.size());
    
    final byte[] qual3 = { 0x63, 0x6F, 0x75, 0x6E, 0x74, 0x3A, 0x00, 0x07 };
    final byte[] val3 = Bytes.fromLong(4L);
    rs.addRow(TestRowSeq.makekv(qual3, val3));
  }
  
  @Test
  public void addRowMergeDuplicateCountEarlierRepair() throws Exception {
    when(config.fix_duplicates()).thenReturn(true);
    // this happens if the same row key is used for the addRow call
    final byte[] qual1 = { 0x63, 0x6F, 0x75, 0x6E, 0x74, 0x3A, 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x63, 0x6F, 0x75, 0x6E, 0x74, 0x3A, 0x00, 0x17 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual3 = { 0x63, 0x6F, 0x75, 0x6E, 0x74, 0x3A, 0x00, 0x27 };
    final byte[] val3 = Bytes.fromLong(6L);
    final RollupSeq rs = new RollupSeq(tsdb, rollup_query_avg);
    rs.setRow(new KeyValue(KEY, FAMILY, qual1, 1, val1));
    rs.addRow(new KeyValue(KEY, FAMILY, qual2, 2, val2));
    rs.addRow(new KeyValue(KEY, FAMILY, qual3, 3, val3));
    assertEquals(0, rs.size());
    assertEquals(6, rs.count_values[23]);
    
    rs.addRow(new KeyValue(KEY, FAMILY, qual3, 1, Bytes.fromLong(7L)));
    assertEquals(6, rs.count_values[23]);
  }
  
  @Test (expected = IllegalDataException.class)
  public void addRowDiffBaseTime() throws Exception {
    final byte[] qual1 = { 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final RollupSeq rs = new RollupSeq(tsdb, rollup_query_sum);
    rs.setRow(TestRowSeq.makekv(qual1, val1));
    rs.addRow(TestRowSeq.makekv(qual2, val2));
    assertEquals(2, rs.size());
    
    final byte[] qual3 = { 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x37 };
    final byte[] val3 = Bytes.fromLong(6L);
    final byte[] qual4 = { 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x47 };
    final byte[] val4 = Bytes.fromLong(7L);
    final byte[] row2 = { 0, 0, 1, 0x50, (byte)0xE2, 0x35, 0x10, 0, 0, 1, 0, 0, 2 };
    rs.addRow(new KeyValue(row2, TestRowSeq.FAMILY, qual3, val3));
    rs.addRow(new KeyValue(row2, TestRowSeq.FAMILY, qual4, val4));
  }

  @Test (expected = IllegalStateException.class)
  public void addRowNotSet() throws Exception {
    final byte[] qual1 = { 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final RollupSeq rs = new RollupSeq(tsdb, rollup_query_sum);
    rs.addRow(TestRowSeq.makekv(qual1, val1));
    rs.addRow(TestRowSeq.makekv(qual2, val2));
    
  }
  
  @Test (expected = IllegalDataException.class)
  public void addRowMergeDifferentKey() throws Exception {
    final byte[] qual1 = { 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final RollupSeq rs = new RollupSeq(tsdb, rollup_query_sum);
    rs.setRow(TestRowSeq.makekv(qual1, val1));
    rs.addRow(TestRowSeq.makekv(qual2, val2));
    assertEquals(2, rs.size());
    
    final byte[] qual3 = { 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x37 };
    final byte[] val3 = Bytes.fromLong(6L);
    final byte[] qual4 = { 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x47 };
    final byte[] val4 = Bytes.fromLong(7L);
    final byte[] key2 = Arrays.copyOf(TestRowSeq.KEY, TestRowSeq.KEY.length);
    key2[key2.length - 1] = 3;
    rs.addRow(TestRowSeq.makekv(key2, qual3, val3));
    rs.addRow(TestRowSeq.makekv(key2, qual4, val4)); 
  }
  
  @Test (expected = IllegalDataException.class)
  public void addRowMergeDifferentKeyAndSalt() throws Exception {
    final byte[] qual1 = { 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final RollupSeq rs = new RollupSeq(tsdb, rollup_query_sum);
    rs.setRow(TestRowSeq.makekv(qual1, val1));
    rs.addRow(TestRowSeq.makekv(qual2, val2));
    assertEquals(2, rs.size());
    
    final byte[] qual3 = { 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x37 };
    final byte[] val3 = Bytes.fromLong(6L);
    final byte[] qual4 = { 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x47 };
    final byte[] val4 = Bytes.fromLong(7L);
    final byte[] key2 = Arrays.copyOf(TestRowSeq.KEY, TestRowSeq.KEY.length);
    key2[0] = 2;
    key2[key2.length - 1] = 3;
    rs.addRow(TestRowSeq.makekv(key2, qual3, val3));
    rs.addRow(TestRowSeq.makekv(key2, qual4, val4)); 
  }
  
  @Test (expected = IllegalDataException.class)
  public void addRowMergeDifferentTime() throws Exception {
    final byte[] qual1 = { 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final RollupSeq rs = new RollupSeq(tsdb, rollup_query_sum);
    rs.setRow(TestRowSeq.makekv(qual1, val1));
    rs.addRow(TestRowSeq.makekv(qual2, val2));
    assertEquals(2, rs.size());
    
    final byte[] qual3 = { 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x37 };
    final byte[] val3 = Bytes.fromLong(6L);
    final byte[] qual4 = { 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x47 };
    final byte[] val4 = Bytes.fromLong(7L);
    final byte[] key2 = Arrays.copyOf(TestRowSeq.KEY, TestRowSeq.KEY.length);
    key2[7] = 3;
    rs.addRow(TestRowSeq.makekv(key2, qual3, val3));
    rs.addRow(TestRowSeq.makekv(key2, qual4, val4)); 
  }
 
  @Test
  public void timestamp() throws Exception {
    final KeyValue kv = getRollupKeyValue(1356998400000L, 7L, rollup_query_sum_mimmax);
    
    final RollupSeq rs = new RollupSeq(tsdb, rollup_query_sum_mimmax);
    rs.setRow(kv);
    
    assertEquals(1, rs.size());
    final SeekableView it = rs.iterator();
    assertTrue(it.hasNext());
    DataPoint dp = it.next();
    assertTrue(dp.isInteger());
    assertEquals(7, dp.longValue());
    assertEquals(1356998400000L, dp.timestamp());
    assertEquals(-1, dp.valueCount());
    assertFalse(it.hasNext());
  }
  // NOTE: many of the tests below also test RollupSeq.size()
  @Test
  public void rollup10m() throws Exception {
    byte[] key = Arrays.copyOf(KEY, KEY.length);
    Internal.setBaseTime(key, 1420070400);
    final RollupSeq rs = new RollupSeq(tsdb, rollup_query_10m_sum);
    rs.setRow(getRollupKeyValue(key, 1420070400, 1L, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420071000, 2L, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420071600, 3L, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420072200, 4L, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420072800, 5L, rollup_query_10m_sum));
    
    assertEquals(5, rs.size());
    final SeekableView it = rs.iterator();
    long value = 1;
    long ts = 1420070400000L;
    while (it.hasNext()) {
      final DataPoint dp = it.next();
      assertEquals(ts, dp.timestamp());
      assertTrue(dp.isInteger());
      assertEquals(value, dp.longValue());
      assertEquals(-1, dp.valueCount());
      ++value;
      ts += 600000;
    }
  }

  @Test
  public void rollup10mDouble() throws Exception {
    byte[] key = Arrays.copyOf(KEY, KEY.length);
    Internal.setBaseTime(key, 1420070400);
    final RollupSeq rs = new RollupSeq(tsdb, rollup_query_10m_sum);
    rs.setRow(getRollupKeyValue(key, 1420070400, 0.25, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420071000, 0.50, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420071600, 0.75, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420072200, 1.00, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420072800, 1.25, rollup_query_10m_sum));
    
    assertEquals(5, rs.size());
    final SeekableView it = rs.iterator();
    double value = 0.25;
    long ts = 1420070400000L;
    while (it.hasNext()) {
      final DataPoint dp = it.next();
      assertEquals(ts, dp.timestamp());
      assertFalse(dp.isInteger());
      assertEquals(value, dp.doubleValue(), 0.0001);
      assertEquals(-1, dp.valueCount());
      value += 0.25;
      ts += 600000;
    }
  }

  @Test
  public void rollup10mFloat() throws Exception {
    byte[] key = Arrays.copyOf(KEY, KEY.length);
    Internal.setBaseTime(key, 1420070400);
    final RollupSeq rs = new RollupSeq(tsdb, rollup_query_10m_sum);
    rs.setRow(getRollupKeyValue(key, 1420070400, 10.25F, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420071000, 10.75F, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420071600, 11.25F, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420072200, 11.75F, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420072800, 12.25F, rollup_query_10m_sum));
    
    assertEquals(5, rs.size());
    final SeekableView it = rs.iterator();
    double value = 10.25;
    long ts = 1420070400000L;
    while (it.hasNext()) {
      final DataPoint dp = it.next();
      assertEquals(ts, dp.timestamp());
      assertFalse(dp.isInteger());
      assertEquals(value, dp.doubleValue(), 0.0001);
      assertEquals(-1, dp.valueCount());
      value += 0.50;
      ts += 600000;
    }
  }

  @Test
  public void rollup10mMixFloatAndLong() throws Exception {
    byte[] key = Arrays.copyOf(KEY, KEY.length);
    Internal.setBaseTime(key, 1420070400);
    final RollupSeq rs = new RollupSeq(tsdb, rollup_query_10m_sum);
    rs.setRow(getRollupKeyValue(key, 1420070400, 20, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420071000, 10.50F, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420071600, 21, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420072200, 11.50F, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420072800, 22, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420073400, 12.50F, rollup_query_10m_sum));
    
    assertEquals(6, rs.size());
    final SeekableView it = rs.iterator();
    double dvalue = 10.50;
    long lvalue = 20;
    long ts = 1420070400000L;
    boolean toggle = false;
    while (it.hasNext()) {
      final DataPoint dp = it.next();
      assertEquals(ts, dp.timestamp());
      if (dp.isInteger()) {
        assertEquals(lvalue, dp.longValue());
      } else {
        assertEquals(dvalue, dp.doubleValue(), 0.0001);
      }
      assertEquals(-1, dp.valueCount());
      if (toggle) {
        dvalue += 1;
      } else {
        ++lvalue;
      }
      toggle = !toggle;
      ts += 600000;
    }
  }

  @Test
  public void rollupAvg10mWithCount() throws Exception {
    byte[] key = Arrays.copyOf(KEY, KEY.length);
    Internal.setBaseTime(key, 1420070400);
    final RollupSeq rs = new RollupSeq(tsdb, rollup_query_10m_avg);
    rs.setRow(getRollupKeyValue(key, 1420070400, 20, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420070400, 2, rollup_query_10m_count));
    rs.addRow(getRollupKeyValue(key, 1420071000, 21, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420071000, 2, rollup_query_10m_count));
    rs.addRow(getRollupKeyValue(key, 1420071600, 22, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420071600, 2, rollup_query_10m_count));
    rs.addRow(getRollupKeyValue(key, 1420072200, 23, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420072200, 2, rollup_query_10m_count));

    assertEquals(4, rs.size());
    final SeekableView it = rs.iterator();
    long value = 20;
    long ts = 1420070400000L;
    while (it.hasNext()) {
      final DataPoint dp = it.next();
      assertEquals(ts, dp.timestamp());
      assertEquals(value, dp.longValue());
      assertEquals(2, dp.valueCount());
      ++value;
      ts += 600000;
    }
  }
  
  @Test
  public void rollupAvg10mWithCountFirst() throws Exception {
    byte[] key = Arrays.copyOf(KEY, KEY.length);
    Internal.setBaseTime(key, 1420070400);
    final RollupSeq rs = new RollupSeq(tsdb, rollup_query_10m_avg);
    rs.setRow(getRollupKeyValue(key, 1420070400, 2, rollup_query_10m_count));
    rs.addRow(getRollupKeyValue(key, 1420070400, 20, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420071000, 2, rollup_query_10m_count));
    rs.addRow(getRollupKeyValue(key, 1420071000, 21, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420071600, 2, rollup_query_10m_count));
    rs.addRow(getRollupKeyValue(key, 1420071600, 22, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420072200, 2, rollup_query_10m_count));
    rs.addRow(getRollupKeyValue(key, 1420072200, 23, rollup_query_10m_sum));

    assertEquals(4, rs.size());
    final SeekableView it = rs.iterator();
    long value = 20;
    long ts = 1420070400000L;
    while (it.hasNext()) {
      final DataPoint dp = it.next();
      assertEquals(ts, dp.timestamp());
      assertEquals(value, dp.longValue());
      assertEquals(2, dp.valueCount());
      ++value;
      ts += 600000;
    }
  }

  @Test
  public void rollupAvg10mMissingCount() throws Exception {
    byte[] key = Arrays.copyOf(KEY, KEY.length);
    Internal.setBaseTime(key, 1420070400);
    final RollupSeq rs = new RollupSeq(tsdb, rollup_query_10m_avg);
    rs.setRow(getRollupKeyValue(key, 1420070400, 20, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420071000, 21, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420071600, 22, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420072200, 23, rollup_query_10m_sum));

    assertEquals(0, rs.size());
    final SeekableView it = rs.iterator();
    assertFalse(it.hasNext());
  }
  
  @Test
  public void rollupAvg10mSkipFirstCount() throws Exception {
    byte[] key = Arrays.copyOf(KEY, KEY.length);
    Internal.setBaseTime(key, 1420070400);
    final RollupSeq rs = new RollupSeq(tsdb, rollup_query_10m_avg);
    rs.setRow(getRollupKeyValue(key, 1420070400, 20, rollup_query_10m_sum));
    //rs.addRow(getRollupKeyValue(key, 1420070400, 2, rollup_query_10m_count));
    rs.addRow(getRollupKeyValue(key, 1420071000, 21, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420071000, 2, rollup_query_10m_count));
    rs.addRow(getRollupKeyValue(key, 1420071600, 22, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420071600, 2, rollup_query_10m_count));
    rs.addRow(getRollupKeyValue(key, 1420072200, 23, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420072200, 2, rollup_query_10m_count));

    assertEquals(3, rs.size());
    final SeekableView it = rs.iterator();
    long value = 21;
    long ts = 1420071000000L;
    while (it.hasNext()) {
      final DataPoint dp = it.next();
      assertEquals(ts, dp.timestamp());
      assertEquals(value, dp.longValue());
      assertEquals(2, dp.valueCount());
      ++value;
      ts += 600000;
    }
  }
  
  @Test
  public void rollupAvg10mSkipLastCount() throws Exception {
    byte[] key = Arrays.copyOf(KEY, KEY.length);
    Internal.setBaseTime(key, 1420070400);
    final RollupSeq rs = new RollupSeq(tsdb, rollup_query_10m_avg);
    rs.setRow(getRollupKeyValue(key, 1420070400, 20, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420070400, 2, rollup_query_10m_count));
    rs.addRow(getRollupKeyValue(key, 1420071000, 21, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420071000, 2, rollup_query_10m_count));
    rs.addRow(getRollupKeyValue(key, 1420071600, 22, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420071600, 2, rollup_query_10m_count));
    rs.addRow(getRollupKeyValue(key, 1420072200, 23, rollup_query_10m_sum));
    //rs.addRow(getRollupKeyValue(key, 1420072200, 2, rollup_query_10m_count));

    assertEquals(3, rs.size());
    final SeekableView it = rs.iterator();
    long value = 20;
    long ts = 1420070400000L;
    while (it.hasNext()) {
      final DataPoint dp = it.next();
      assertEquals(ts, dp.timestamp());
      assertEquals(value, dp.longValue());
      assertEquals(2, dp.valueCount());
      ++value;
      ts += 600000;
    }
  }
  
  @Test
  public void rollupAvg10mSkipMiddleCount() throws Exception {
    byte[] key = Arrays.copyOf(KEY, KEY.length);
    Internal.setBaseTime(key, 1420070400);
    final RollupSeq rs = new RollupSeq(tsdb, rollup_query_10m_avg);
    rs.setRow(getRollupKeyValue(key, 1420070400, 20, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420070400, 2, rollup_query_10m_count));
    rs.addRow(getRollupKeyValue(key, 1420071000, 21, rollup_query_10m_sum));
    //rs.addRow(getRollupKeyValue(key, 1420071000, 2, rollup_query_10m_count));
    rs.addRow(getRollupKeyValue(key, 1420071600, 22, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420071600, 2, rollup_query_10m_count));
    rs.addRow(getRollupKeyValue(key, 1420072200, 23, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420072200, 2, rollup_query_10m_count));

    assertEquals(3, rs.size());
    final SeekableView it = rs.iterator();
    long value = 20;
    long ts = 1420070400000L;
    while (it.hasNext()) {
      final DataPoint dp = it.next();
      assertEquals(ts, dp.timestamp());
      assertEquals(value, dp.longValue());
      assertEquals(2, dp.valueCount());
      if (ts == 1420070400000L) {
        ts = 1420071600000L;
        value += 2;
      } else {
        ts += 600000;
        ++value;
      }
    }
  }
  
  @Test
  public void rollupAvg10mMissingSum() throws Exception {
    byte[] key = Arrays.copyOf(KEY, KEY.length);
    Internal.setBaseTime(key, 1420070400);
    final RollupSeq rs = new RollupSeq(tsdb, rollup_query_10m_avg);
    rs.setRow(getRollupKeyValue(key, 1420070400, 2, rollup_query_10m_count));
    rs.addRow(getRollupKeyValue(key, 1420071000, 2, rollup_query_10m_count));
    rs.addRow(getRollupKeyValue(key, 1420071600, 2, rollup_query_10m_count));
    rs.addRow(getRollupKeyValue(key, 1420072200, 2, rollup_query_10m_count));

    assertEquals(0, rs.size());
    final SeekableView it = rs.iterator();
    assertFalse(it.hasNext());
  }
  
  public void rollupAvg10mSkipFirstSum() throws Exception {
    byte[] key = Arrays.copyOf(KEY, KEY.length);
    Internal.setBaseTime(key, 1420070400);
    final RollupSeq rs = new RollupSeq(tsdb, rollup_query_10m_avg);
    //rs.setRow(getRollupKeyValue(key, 1420070400, 20, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420070400, 2, rollup_query_10m_count));
    rs.addRow(getRollupKeyValue(key, 1420071000, 21, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420071000, 2, rollup_query_10m_count));
    rs.addRow(getRollupKeyValue(key, 1420071600, 22, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420071600, 2, rollup_query_10m_count));
    rs.addRow(getRollupKeyValue(key, 1420072200, 23, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420072200, 2, rollup_query_10m_count));

    assertEquals(3, rs.size());
    final SeekableView it = rs.iterator();
    long value = 21;
    long ts = 1420071000000L;
    while (it.hasNext()) {
      final DataPoint dp = it.next();
      assertEquals(ts, dp.timestamp());
      assertEquals(value, dp.longValue());
      assertEquals(2, dp.valueCount());
      ++value;
      ts += 600000;
    }
  }
  
  @Test
  public void rollupAvg10mSkipLastSum() throws Exception {
    byte[] key = Arrays.copyOf(KEY, KEY.length);
    Internal.setBaseTime(key, 1420070400);
    final RollupSeq rs = new RollupSeq(tsdb, rollup_query_10m_avg);
    rs.setRow(getRollupKeyValue(key, 1420070400, 20, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420070400, 2, rollup_query_10m_count));
    rs.addRow(getRollupKeyValue(key, 1420071000, 21, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420071000, 2, rollup_query_10m_count));
    rs.addRow(getRollupKeyValue(key, 1420071600, 22, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420071600, 2, rollup_query_10m_count));
    //rs.addRow(getRollupKeyValue(key, 1420072200, 23, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420072200, 2, rollup_query_10m_count));

    assertEquals(3, rs.size());
    final SeekableView it = rs.iterator();
    long value = 20;
    long ts = 1420070400000L;
    while (it.hasNext()) {
      final DataPoint dp = it.next();
      assertEquals(ts, dp.timestamp());
      assertEquals(value, dp.longValue());
      assertEquals(2, dp.valueCount());
      ++value;
      ts += 600000;
    }
  }
  
  @Test
  public void rollupAvg10mSkipMiddleSum() throws Exception {
    byte[] key = Arrays.copyOf(KEY, KEY.length);
    Internal.setBaseTime(key, 1420070400);
    final RollupSeq rs = new RollupSeq(tsdb, rollup_query_10m_avg);
    rs.setRow(getRollupKeyValue(key, 1420070400, 20, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420070400, 2, rollup_query_10m_count));
    //rs.addRow(getRollupKeyValue(key, 1420071000, 21, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420071000, 2, rollup_query_10m_count));
    rs.addRow(getRollupKeyValue(key, 1420071600, 22, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420071600, 2, rollup_query_10m_count));
    rs.addRow(getRollupKeyValue(key, 1420072200, 23, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420072200, 2, rollup_query_10m_count));

    assertEquals(3, rs.size());
    final SeekableView it = rs.iterator();
    long value = 20;
    long ts = 1420070400000L;
    while (it.hasNext()) {
      final DataPoint dp = it.next();
      assertEquals(ts, dp.timestamp());
      assertEquals(value, dp.longValue());
      assertEquals(2, dp.valueCount());
      if (ts == 1420070400000L) {
        ts = 1420071600000L;
        value += 2;
      } else {
        ts += 600000;
        ++value;
      }
    }
  }
  
  @Test
  public void rollupAvg10mUnaligned() throws Exception {
    byte[] key = Arrays.copyOf(KEY, KEY.length);
    Internal.setBaseTime(key, 1420070400);
    final RollupSeq rs = new RollupSeq(tsdb, rollup_query_10m_avg);
    rs.setRow(getRollupKeyValue(key, 1420070400, 20, rollup_query_10m_sum));
    //rs.addRow(getRollupKeyValue(key, 1420070400, 2, rollup_query_10m_count));
    //rs.addRow(getRollupKeyValue(key, 1420071000, 21, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420071000, 2, rollup_query_10m_count));
    rs.addRow(getRollupKeyValue(key, 1420071600, 22, rollup_query_10m_sum));
    //rs.addRow(getRollupKeyValue(key, 1420071600, 2, rollup_query_10m_count));
    //rs.addRow(getRollupKeyValue(key, 1420072200, 23, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420072200, 2, rollup_query_10m_count));

    assertEquals(0, rs.size());
    final SeekableView it = rs.iterator();
    assertFalse(it.hasNext());
  }
  
  @Test
  public void endOfArrayDivergence() throws Exception {
    final RollupSeq rs = new RollupSeq(tsdb, rollup_query_10m_avg);
    byte[] key = Arrays.copyOf(KEY, KEY.length);
    Internal.setBaseTime(key, 1420070400);
    
    int[]indices = new int[4];
    byte[] b = new byte[] { 0, 11, 0, 27, 0, 43, 0, 59, 0, 75, 0, 91, 0, 107, 0, 123, 0, -117, 0, -101, 0, -85, 0, -69, 0, -53, 0, -37, 0, -21, 0, -5, 1, 11, 1, 27, 1, 43, 1, 59, 1, 75, 1, 91, 1, 123, 0, 0 };
    Whitebox.setInternalState(rs, "key", key);
    Whitebox.setInternalState(rs, "qualifiers", b);
    Whitebox.setInternalState(rs, "indices", indices);
    indices[0] = b.length;
    Whitebox.setInternalState(rs, "values", new byte[] { 67, 10, 71, -82, 66, -10, -108, 123, 66, -52, -52, -51, 66, -56, 97, 72, 66, -78, 5, 31, 66, -79, -31, 72, 66, 101, 0, 0, 67, 1, 99, -41, 66, -17, -77, 51, 66, -48, 10, 61, 66, -48, -118, 61, 66, -77, -47, -20, 66, -79, 81, -20, 66, -33, -118, 61, 67, 6, -31, 72, 66, -22, -26, 102, 66, -51, -103, -102, 66, -65, 51, 51, 66, -73, 112, -92, 66, -71, -47, -20, 66, -25, -6, -31, 67, 10, 10, 61, 65, -115, 92, 41, 0, 0, 0, 0 });
    b = new byte[] { 0, 11, 0, 27, 0, 43, 0, 59, 0, 75, 0, 91, 0, 107, 0, 123, 0, -117, 0, -101, 0, -85, 0, -69, 0, -53, 0, -37, 0, -21, 0, -5, 1, 11, 1, 27, 1, 43, 1, 59, 1, 75, 1, 91, 1, 107, 0, 0 };
    indices[2] = b.length;
    Whitebox.setInternalState(rs, "count_qualifiers", b);
    Whitebox.setInternalState(rs, "count_values", new byte[] { 66, 112, 0, 0, 66, 112, 0, 0, 66, 112, 0, 0, 66, 112, 0, 0, 66, 112, 0, 0, 66, 112, 0, 0, 65, -16, 0, 0, 66, 100, 0, 0, 66, 112, 0, 0, 66, 112, 0, 0, 66, 112, 0, 0, 66, 112, 0, 0, 66, 112, 0, 0, 66, 112, 0, 0, 66, 112, 0, 0, 66, 112, 0, 0, 66, 112, 0, 0, 66, 112, 0, 0, 66, 112, 0, 0, 66, 112, 0, 0, 66, 112, 0, 0, 66, 112, 0, 0, 65, 32, 0, 0, 0, 0, 0, 0 });
    
    final SeekableView it = rs.iterator();
    int count = 0;
    while (it.hasNext()) {
      ++count;
      it.next();
    }
    assertEquals(22, count);   
  }
  
  @Test
  public void arrayOverflowAvoidance() throws Exception {
    final RollupSeq rs = new RollupSeq(tsdb, rollup_query_10m_avg);
    byte[] key = Arrays.copyOf(KEY, KEY.length);
    Internal.setBaseTime(key, 1420070400);
    
    final int[]indices = new int[4];
    byte[] b = new byte[] { 0, 11, 0, 27, 0, 43, 0, 59, 0, 75, 0, 91, 0, 107, 0, 123, 0, -117, 0, -101, 0, -85, 0, -69, 0, -53, 0, -37, 0, -21, 0, -5, 1, 11, 1, 27, 1, 43, 1, 59, 1, 75, 1, 91, 1, 107, 1, 123 };
    Whitebox.setInternalState(rs, "key", key);
    Whitebox.setInternalState(rs, "qualifiers", b);
    Whitebox.setInternalState(rs, "indices", indices);
    indices[0] = b.length;
    Whitebox.setInternalState(rs, "values", new byte[] { 75, 29, -83, 11, 75, 28, -40, -13, 75, 23, -115, 8, 75, 9, -84, 94, 75, 11, 55, -77, 75, 12, -115, 111, 75, 8, -21, -48, 75, 12, 66, 76, 75, 8, 118, -48, 75, 11, -65, 121, 75, 26, 64, -100, 75, 69, -15, -114, 75, 95, 85, -64, 75, 109, 28, 40, 75, 118, 100, -32, 75, 110, -119, 20, 75, 100, -71, -94, 75, 100, -94, 50, 75, 90, -53, 44, 75, 90, 47, 70, 75, 82, 80, 40, 75, 66, 106, 121, 75, 57, -102, -34, 75, 53, 69, 4 });
    indices[2] = b.length;
    Whitebox.setInternalState(rs, "count_qualifiers", b);
    Whitebox.setInternalState(rs, "count_values", new byte[] { 65, 64, 0, 0, 65, 64, 0, 0, 65, 64, 0, 0, 65, 64, 0, 0, 65, 64, 0, 0, 65, 64, 0, 0, 65, 64, 0, 0, 65, 64, 0, 0, 65, 64, 0, 0, 65, 64, 0, 0, 65, 48, 0, 0, 65, 64, 0, 0, 65, 64, 0, 0, 65, 64, 0, 0, 65, 64, 0, 0, 65, 64, 0, 0, 65, 64, 0, 0, 65, 64, 0, 0, 65, 64, 0, 0, 65, 64, 0, 0, 65, 64, 0, 0, 65, 64, 0, 0, 65, 64, 0, 0, 65, 64, 0, 0 });
    
    final SeekableView it = rs.iterator();
    int count = 0;
    while (it.hasNext()) {
      ++count;
      it.next();
    }
    assertEquals(24, count);   
  }
  
  @Test
  public void rollup1hLong() throws Exception {
    byte[] key = Arrays.copyOf(KEY, KEY.length);
    Internal.setBaseTime(key, 1420070400);
    final RollupSeq rs = new RollupSeq(tsdb, rollup_query_1h_sum);
    rs.setRow(getRollupKeyValue(key, 1420070400, 1L, rollup_query_1h_sum));
    rs.addRow(getRollupKeyValue(key, 1420074000, 2L, rollup_query_1h_sum));
    rs.addRow(getRollupKeyValue(key, 1420077600, 3L, rollup_query_1h_sum));
    rs.addRow(getRollupKeyValue(key, 1420081200, 4L, rollup_query_1h_sum));
    rs.addRow(getRollupKeyValue(key, 1420084800, 5L, rollup_query_1h_sum));

    assertEquals(5, rs.size());
    final SeekableView it = rs.iterator();
    long value = 1;
    long ts = 1420070400000L;
    while (it.hasNext()) {
      final DataPoint dp = it.next();
      assertEquals(ts, dp.timestamp());
      assertTrue(dp.isInteger());
      assertEquals(value, dp.longValue());
      assertEquals(-1, dp.valueCount());
      ++value;
      ts += 3600000;
    }
  }

  @Test
  public void rollup1hFloat() throws Exception {
    byte[] key = Arrays.copyOf(KEY, KEY.length);
    Internal.setBaseTime(key, 1420070400);
    final RollupSeq rs = new RollupSeq(tsdb, rollup_query_1h_sum);
    rs.setRow(getRollupKeyValue(key, 1420070400, 0.25, rollup_query_1h_sum));
    rs.addRow(getRollupKeyValue(key, 1420074000, 0.5, rollup_query_1h_sum));
    rs.addRow(getRollupKeyValue(key, 1420077600, 0.75, rollup_query_1h_sum));
    rs.addRow(getRollupKeyValue(key, 1420081200, 1.0, rollup_query_1h_sum));
    rs.addRow(getRollupKeyValue(key, 1420084800, 1.25, rollup_query_1h_sum));

    assertEquals(5, rs.size());
    final SeekableView it = rs.iterator();
    double value = 0.25;
    long ts = 1420070400000L;
    while (it.hasNext()) {
      final DataPoint dp = it.next();
      assertEquals(ts, dp.timestamp());
      assertFalse(dp.isInteger());
      assertEquals(value, dp.doubleValue(), 0.0001);
      assertEquals(-1, dp.valueCount());
      value += 0.25;
      ts += 3600000;
    }
  }

  @Test
  public void rollup1hLongWithCount() throws Exception {
    byte[] key = Arrays.copyOf(KEY, KEY.length);
    Internal.setBaseTime(key, 1420070400);
    final RollupSeq rs = new RollupSeq(tsdb, rollup_query_1h_avg);
    rs.setRow(getRollupKeyValue(key, 1420070400, 1L, rollup_query_1h_sum));
    rs.addRow(getRollupKeyValue(key, 1420070400, 2L, rollup_query_1h_count));
    rs.addRow(getRollupKeyValue(key, 1420074000, 2L, rollup_query_1h_sum));
    rs.addRow(getRollupKeyValue(key, 1420074000, 2L, rollup_query_1h_count));
    rs.addRow(getRollupKeyValue(key, 1420077600, 3L, rollup_query_1h_sum));
    rs.addRow(getRollupKeyValue(key, 1420077600, 2L, rollup_query_1h_count));
    rs.addRow(getRollupKeyValue(key, 1420081200, 4L, rollup_query_1h_sum));
    rs.addRow(getRollupKeyValue(key, 1420081200, 2L, rollup_query_1h_count));
    rs.addRow(getRollupKeyValue(key, 1420084800, 5L, rollup_query_1h_sum));
    rs.addRow(getRollupKeyValue(key, 1420084800, 2L, rollup_query_1h_count));

    assertEquals(5, rs.size());
    final SeekableView it = rs.iterator();
    long value = 1;
    long ts = 1420070400000L;
    while (it.hasNext()) {
      final DataPoint dp = it.next();
      assertEquals(ts, dp.timestamp());
      assertTrue(dp.isInteger());
      assertEquals(value, dp.longValue());
      assertEquals(2, dp.valueCount());
      ++value;
      ts += 3600000;
    }
  }
  
  @Test
  public void rollup1hLongWithCountWithDoubles() throws Exception {
    byte[] key = Arrays.copyOf(KEY, KEY.length);
    Internal.setBaseTime(key, 1420070400);
    final RollupSeq rs = new RollupSeq(tsdb, rollup_query_1h_avg);
    rs.setRow(getRollupKeyValue(key, 1420070400, 1L, rollup_query_1h_sum));
    rs.addRow(getRollupKeyValue(key, 1420070400, 2L, rollup_query_1h_count));
    rs.addRow(getRollupKeyValue(key, 1420074000, 2L, rollup_query_1h_sum));
    rs.addRow(getRollupKeyValue(key, 1420074000, 2D, rollup_query_1h_count));
    rs.addRow(getRollupKeyValue(key, 1420077600, 3L, rollup_query_1h_sum));
    rs.addRow(getRollupKeyValue(key, 1420077600, 2D, rollup_query_1h_count));
    rs.addRow(getRollupKeyValue(key, 1420081200, 4L, rollup_query_1h_sum));
    rs.addRow(getRollupKeyValue(key, 1420081200, 2D, rollup_query_1h_count));
    rs.addRow(getRollupKeyValue(key, 1420084800, 5L, rollup_query_1h_sum));
    rs.addRow(getRollupKeyValue(key, 1420084800, 2L, rollup_query_1h_count));

    assertEquals(5, rs.size());
    final SeekableView it = rs.iterator();
    long value = 1;
    long ts = 1420070400000L;
    while (it.hasNext()) {
      final DataPoint dp = it.next();
      assertEquals(ts, dp.timestamp());
      assertTrue(dp.isInteger());
      assertEquals(value, dp.longValue());
      assertEquals(2, dp.valueCount());
      ++value;
      ts += 3600000;
    }
  }
  
  @Test
  public void rollup10mSeekTop() throws Exception {
    byte[] key = Arrays.copyOf(KEY, KEY.length);
    Internal.setBaseTime(key, 1420070400);
    final RollupSeq rs = new RollupSeq(tsdb, rollup_query_10m_sum);
    rs.setRow(getRollupKeyValue(key, 1420070400, 1L, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420071000, 2L, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420071600, 3L, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420072200, 4L, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420072800, 5L, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420073400, 6L, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420074000, 7L, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420074600, 8L, rollup_query_10m_sum));

    assertEquals(8, rs.size());
    final SeekableView it = rs.iterator();
    it.seek(1420070400000L);
    long value = 1;
    long ts = 1420070400000L;
    while (it.hasNext()) {
      final DataPoint dp = it.next();
      assertEquals(ts, dp.timestamp());
      assertTrue(dp.isInteger());
      assertEquals(value, dp.longValue());
      assertEquals(-1, dp.valueCount());
      ++value;
      ts += 600000;
    }
  }
  
  @Test
  public void rollup10mSeek() throws Exception {
    byte[] key = Arrays.copyOf(KEY, KEY.length);
    Internal.setBaseTime(key, 1420070400);
    final RollupSeq rs = new RollupSeq(tsdb, rollup_query_10m_sum);
    rs.setRow(getRollupKeyValue(key, 1420070400, 1L, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420071000, 2L, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420071600, 3L, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420072200, 4L, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420072800, 5L, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420073400, 6L, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420074000, 7L, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420074600, 8L, rollup_query_10m_sum));

    assertEquals(8, rs.size());
    final SeekableView it = rs.iterator();
    it.seek(1420072200000L);
    long value = 4;
    long ts = 1420072200000L;
    while (it.hasNext()) {
      final DataPoint dp = it.next();
      assertEquals(ts, dp.timestamp());
      assertTrue(dp.isInteger());
      assertEquals(value, dp.longValue());
      assertEquals(-1, dp.valueCount());
      ++value;
      ts += 600000;
    }
  }
  
  @Test
  public void rollup10mSeekOOB() throws Exception {
    byte[] key = Arrays.copyOf(KEY, KEY.length);
    Internal.setBaseTime(key, 1420070400);
    final RollupSeq rs = new RollupSeq(tsdb, rollup_query_10m_sum);
    rs.setRow(getRollupKeyValue(key, 1420070400, 1L, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420071000, 2L, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420071600, 3L, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420072200, 4L, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420072800, 5L, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420073400, 6L, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420074000, 7L, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420074600, 8L, rollup_query_10m_sum));

    assertEquals(8, rs.size());
    final SeekableView it = rs.iterator();
    it.seek(1420075200000L);
    assertFalse(it.hasNext());
  }
  
  @Test
  public void rollup10mSeekSeconds() throws Exception {
    byte[] key = Arrays.copyOf(KEY, KEY.length);
    Internal.setBaseTime(key, 1420070400);
    final RollupSeq rs = new RollupSeq(tsdb, rollup_query_10m_sum);
    rs.setRow(getRollupKeyValue(key, 1420070400, 1L, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420071000, 2L, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420071600, 3L, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420072200, 4L, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420072800, 5L, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420073400, 6L, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420074000, 7L, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420074600, 8L, rollup_query_10m_sum));

    assertEquals(8, rs.size());
    final SeekableView it = rs.iterator();
    it.seek(1420072200L);
    long value = 4;
    long ts = 1420072200000L;
    while (it.hasNext()) {
      final DataPoint dp = it.next();
      assertEquals(ts, dp.timestamp());
      assertTrue(dp.isInteger());
      assertEquals(value, dp.longValue());
      assertEquals(-1, dp.valueCount());
      ++value;
      ts += 600000;
    }
  }
  
  @Test
  public void rollup10mSeekUnaligned() throws Exception {
    byte[] key = Arrays.copyOf(KEY, KEY.length);
    Internal.setBaseTime(key, 1420070400);
    final RollupSeq rs = new RollupSeq(tsdb, rollup_query_10m_sum);
    rs.setRow(getRollupKeyValue(key, 1420070400, 1L, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420071000, 2L, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420071600, 3L, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420072200, 4L, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420072800, 5L, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420073400, 6L, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420074000, 7L, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420074600, 8L, rollup_query_10m_sum));

    assertEquals(8, rs.size());
    final SeekableView it = rs.iterator();
    it.seek(1420072123456L);
    long value = 4;
    long ts = 1420072200000L;
    while (it.hasNext()) {
      final DataPoint dp = it.next();
      assertEquals(ts, dp.timestamp());
      assertTrue(dp.isInteger());
      assertEquals(value, dp.longValue());
      assertEquals(-1, dp.valueCount());
      ++value;
      ts += 600000;
    }
  }
  
  @Test
  public void rollup10mAvgSeekTopAligned() throws Exception {
    byte[] key = Arrays.copyOf(KEY, KEY.length);
    Internal.setBaseTime(key, 1420070400);
    final RollupSeq rs = new RollupSeq(tsdb, rollup_query_10m_avg);
    rs.setRow(getRollupKeyValue(key, 1420070400, 20, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420070400, 2, rollup_query_10m_count));
    rs.addRow(getRollupKeyValue(key, 1420071000, 21, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420071000, 2, rollup_query_10m_count));
    rs.addRow(getRollupKeyValue(key, 1420071600, 22, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420071600, 2, rollup_query_10m_count));
    rs.addRow(getRollupKeyValue(key, 1420072200, 23, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420072200, 2, rollup_query_10m_count));

    assertEquals(4, rs.size());
    final SeekableView it = rs.iterator();
    it.seek(1420070400000L);
    long value = 20;
    long ts = 1420070400000L;
    while (it.hasNext()) {
      final DataPoint dp = it.next();
      assertEquals(ts, dp.timestamp());
      assertTrue(dp.isInteger());
      assertEquals(value, dp.longValue());
      assertEquals(2, dp.valueCount());
      ++value;
      ts += 600000;
    }
    assertEquals(24, value);
  }
  
  @Test
  public void rollup10mAvgSeekTopUnaligned() throws Exception {
    byte[] key = Arrays.copyOf(KEY, KEY.length);
    Internal.setBaseTime(key, 1420070400);
    final RollupSeq rs = new RollupSeq(tsdb, rollup_query_10m_avg);
    rs.setRow(getRollupKeyValue(key, 1420070400, 20, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420070400, 2, rollup_query_10m_count));
    //rs.addRow(getRollupKeyValue(key, 1420071000, 21, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420071000, 2, rollup_query_10m_count));
    rs.addRow(getRollupKeyValue(key, 1420071600, 22, rollup_query_10m_sum));
    //rs.addRow(getRollupKeyValue(key, 1420071600, 2, rollup_query_10m_count));
    rs.addRow(getRollupKeyValue(key, 1420072200, 23, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420072200, 2, rollup_query_10m_count));

    assertEquals(2, rs.size());
    final SeekableView it = rs.iterator();
    it.seek(1420070400000L);
    DataPoint dp = it.next();
    assertEquals(1420070400000L, dp.timestamp());
    assertTrue(dp.isInteger());
    assertEquals(20, dp.longValue());
    assertEquals(2, dp.valueCount());
    dp = it.next();
    assertEquals(1420072200000L, dp.timestamp());
    assertTrue(dp.isInteger());
    assertEquals(23, dp.longValue());
    assertEquals(2, dp.valueCount());
    assertFalse(it.hasNext());
  }
  
  @Test
  public void rollup10mAvgSeekTopUnalignedMissingTop() throws Exception {
    byte[] key = Arrays.copyOf(KEY, KEY.length);
    Internal.setBaseTime(key, 1420070400);
    final RollupSeq rs = new RollupSeq(tsdb, rollup_query_10m_avg);
    rs.setRow(getRollupKeyValue(key, 1420070400, 20, rollup_query_10m_sum));
    //rs.addRow(getRollupKeyValue(key, 1420070400, 2, rollup_query_10m_count));
    rs.addRow(getRollupKeyValue(key, 1420071000, 21, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420071000, 2, rollup_query_10m_count));
    rs.addRow(getRollupKeyValue(key, 1420071600, 22, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420071600, 2, rollup_query_10m_count));
    rs.addRow(getRollupKeyValue(key, 1420072200, 23, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420072200, 2, rollup_query_10m_count));

    assertEquals(3, rs.size());
    final SeekableView it = rs.iterator();
    it.seek(1420070400000L);
    long value = 21;
    long ts = 1420071000000L;
    while (it.hasNext()) {
      final DataPoint dp = it.next();
      assertEquals(ts, dp.timestamp());
      assertTrue(dp.isInteger());
      assertEquals(value, dp.longValue());
      assertEquals(2, dp.valueCount());
      ++value;
      ts += 600000;
    }
    assertEquals(24, value);
  }
  
  @Test
  public void rollup10mAvgSeekAligned() throws Exception {
    byte[] key = Arrays.copyOf(KEY, KEY.length);
    Internal.setBaseTime(key, 1420070400);
    final RollupSeq rs = new RollupSeq(tsdb, rollup_query_10m_avg);
    rs.setRow(getRollupKeyValue(key, 1420070400, 20, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420070400, 2, rollup_query_10m_count));
    rs.addRow(getRollupKeyValue(key, 1420071000, 21, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420071000, 2, rollup_query_10m_count));
    rs.addRow(getRollupKeyValue(key, 1420071600, 22, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420071600, 2, rollup_query_10m_count));
    rs.addRow(getRollupKeyValue(key, 1420072200, 23, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420072200, 2, rollup_query_10m_count));

    assertEquals(4, rs.size());
    final SeekableView it = rs.iterator();
    it.seek(1420071600);
    long value = 22;
    long ts = 1420071600000L;
    while (it.hasNext()) {
      final DataPoint dp = it.next();
      assertEquals(ts, dp.timestamp());
      assertTrue(dp.isInteger());
      assertEquals(value, dp.longValue());
      assertEquals(2, dp.valueCount());
      ++value;
      ts += 600000;
    }
    assertEquals(24, value);
  }
  
  @Test
  public void rollup10mAvgSeekUnaligned() throws Exception {
    byte[] key = Arrays.copyOf(KEY, KEY.length);
    Internal.setBaseTime(key, 1420070400);
    final RollupSeq rs = new RollupSeq(tsdb, rollup_query_10m_avg);
    rs.setRow(getRollupKeyValue(key, 1420070400, 20, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420070400, 2, rollup_query_10m_count));
    //rs.addRow(getRollupKeyValue(key, 1420071000, 21, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420071000, 2, rollup_query_10m_count));
    rs.addRow(getRollupKeyValue(key, 1420071600, 22, rollup_query_10m_sum));
    //rs.addRow(getRollupKeyValue(key, 1420071600, 2, rollup_query_10m_count));
    rs.addRow(getRollupKeyValue(key, 1420072200, 23, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420072200, 2, rollup_query_10m_count));

    assertEquals(2, rs.size());
    final SeekableView it = rs.iterator();
    it.seek(1420071600);
    long value = 23;
    long ts = 1420072200000L;
    while (it.hasNext()) {
      final DataPoint dp = it.next();
      assertEquals(ts, dp.timestamp());
      assertTrue(dp.isInteger());
      assertEquals(value, dp.longValue());
      assertEquals(2, dp.valueCount());
      ++value;
      ts += 600000;
    }
    assertEquals(24, value);
  }
  
  @Test
  public void rollup10mAvgSeekUnalignedEmpty() throws Exception {
    byte[] key = Arrays.copyOf(KEY, KEY.length);
    Internal.setBaseTime(key, 1420070400);
    final RollupSeq rs = new RollupSeq(tsdb, rollup_query_10m_avg);
    rs.setRow(getRollupKeyValue(key, 1420070400, 20, rollup_query_10m_sum));
    //rs.addRow(getRollupKeyValue(key, 1420070400, 2, rollup_query_10m_count));
    //rs.addRow(getRollupKeyValue(key, 1420071000, 21, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420071000, 2, rollup_query_10m_count));
    rs.addRow(getRollupKeyValue(key, 1420071600, 22, rollup_query_10m_sum));
    //rs.addRow(getRollupKeyValue(key, 1420071600, 2, rollup_query_10m_count));
    //rs.addRow(getRollupKeyValue(key, 1420072200, 23, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420072200, 2, rollup_query_10m_count));

    assertEquals(0, rs.size());
    final SeekableView it = rs.iterator();
    it.seek(1420071600);
    assertFalse(it.hasNext());
  }
  
  @Test
  public void rollup10mAvgSeekTopAlignedOOB() throws Exception {
    byte[] key = Arrays.copyOf(KEY, KEY.length);
    Internal.setBaseTime(key, 1420070400);
    final RollupSeq rs = new RollupSeq(tsdb, rollup_query_10m_avg);
    rs.setRow(getRollupKeyValue(key, 1420070400, 20, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420070400, 2, rollup_query_10m_count));
    rs.addRow(getRollupKeyValue(key, 1420071000, 21, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420071000, 2, rollup_query_10m_count));
    rs.addRow(getRollupKeyValue(key, 1420071600, 22, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420071600, 2, rollup_query_10m_count));
    rs.addRow(getRollupKeyValue(key, 1420072200, 23, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420072200, 2, rollup_query_10m_count));

    assertEquals(4, rs.size());
    final SeekableView it = rs.iterator();
    it.seek(1420075200000L);
    assertFalse(it.hasNext());
  }
  
  @Test
  public void rollup10mAvgSeekTopUnalignedOOB() throws Exception {
    byte[] key = Arrays.copyOf(KEY, KEY.length);
    Internal.setBaseTime(key, 1420070400);
    final RollupSeq rs = new RollupSeq(tsdb, rollup_query_10m_avg);
    rs.setRow(getRollupKeyValue(key, 1420070400, 20, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420070400, 2, rollup_query_10m_count));
    //rs.addRow(getRollupKeyValue(key, 1420071000, 21, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420071000, 2, rollup_query_10m_count));
    rs.addRow(getRollupKeyValue(key, 1420071600, 22, rollup_query_10m_sum));
    //rs.addRow(getRollupKeyValue(key, 1420071600, 2, rollup_query_10m_count));
    rs.addRow(getRollupKeyValue(key, 1420072200, 23, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420072200, 2, rollup_query_10m_count));

    assertEquals(2, rs.size());
    final SeekableView it = rs.iterator();
    it.seek(1420075200000L);
    assertFalse(it.hasNext());
  }
  
  @Test
  public void rollup10mAvgSeekTopUnalignedEmptyOOB() throws Exception {
    byte[] key = Arrays.copyOf(KEY, KEY.length);
    Internal.setBaseTime(key, 1420070400);
    final RollupSeq rs = new RollupSeq(tsdb, rollup_query_10m_avg);
    rs.setRow(getRollupKeyValue(key, 1420070400, 20, rollup_query_10m_sum));
    //rs.addRow(getRollupKeyValue(key, 1420070400, 2, rollup_query_10m_count));
    //rs.addRow(getRollupKeyValue(key, 1420071000, 21, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420071000, 2, rollup_query_10m_count));
    rs.addRow(getRollupKeyValue(key, 1420071600, 22, rollup_query_10m_sum));
    //rs.addRow(getRollupKeyValue(key, 1420071600, 2, rollup_query_10m_count));
    //rs.addRow(getRollupKeyValue(key, 1420072200, 23, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420072200, 2, rollup_query_10m_count));

    assertEquals(0, rs.size());
    final SeekableView it = rs.iterator();
    it.seek(1420075200000L);
    assertFalse(it.hasNext());
  }
  
  @Test
  public void rollup10mTimestamp() throws Exception {
    byte[] key = Arrays.copyOf(KEY, KEY.length);
    Internal.setBaseTime(key, 1420070400);
    final RollupSeq rs = new RollupSeq(tsdb, rollup_query_10m_sum);
    rs.setRow(getRollupKeyValue(key, 1420070400, 1L, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420071000, 2L, rollup_query_10m_sum));
    rs.addRow(getRollupKeyValue(key, 1420071600, 3L, rollup_query_10m_sum));
    
    assertEquals(1420070400000L, rs.timestamp(0));
    assertEquals(1420071000000L, rs.timestamp(1));
    assertEquals(1420071600000L, rs.timestamp(2));
    
    try {
      assertEquals(1420071600000L, rs.timestamp(3));
      fail("Excpected an IndexOutOfBoundsException");
    } catch (IndexOutOfBoundsException e) { }
    
    try {
      assertEquals(1420071600000L, rs.timestamp(-1));
      fail("Excpected an IndexOutOfBoundsException");
    } catch (IndexOutOfBoundsException e) { }
  }
  
  private static KeyValue getRollupKeyValue(final long timestamp, 
          final long value, 
          final RollupQuery rollup_query) {
    return getRollupKeyValue(TestRowSeq.KEY, timestamp, value, rollup_query);
  }
  
  private static KeyValue getRollupKeyValue(final byte[] key,
          final long timestamp, 
          final long value, 
          final RollupQuery rollup_query) {
    
    final byte[] val = Internal.vleEncodeLong(value);
    final short flags = (short) (val.length - 1);  // Just the length.
    return new KeyValue(key, TestRowSeq.FAMILY, getQualifier(timestamp, flags, 
            rollup_query), val);
  }

  private static KeyValue getRollupKeyValue(final byte[] key,
          final long timestamp, 
          final float value, 
          final RollupQuery rollup_query) {
    
    final short flags = Const.FLAG_FLOAT | 0x3;  // A float stored on 4 bytes.
    final byte[] val = Bytes.fromInt(Float.floatToRawIntBits(value));
    return new KeyValue(key, TestRowSeq.FAMILY, getQualifier(timestamp, flags, 
            rollup_query), val);
  }
  
  private static KeyValue getRollupKeyValue(final byte[] key,
          final long timestamp, 
          final double value, 
          final RollupQuery rollup_query) {
    
    final short flags = Const.FLAG_FLOAT | 0x7;  // A double stored on 8 bytes.
    final byte[] val = Bytes.fromLong(Double.doubleToRawLongBits(value));
    return new KeyValue(key, TestRowSeq.FAMILY, getQualifier(timestamp, flags, 
            rollup_query), val);
  }

  private static byte[] getQualifier(final long timestamp, 
          final short flags, RollupQuery rollup_query) {
    
    final int base_time = RollupUtils.getRollupBasetime(timestamp, 
            rollup_query.getRollupInterval());
    return RollupUtils.buildRollupQualifier(timestamp, base_time, flags, 
            rollup_query.getRollupAgg().toString(), 
            rollup_query.getRollupInterval());
  }
}
