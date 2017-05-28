// This file is part of OpenTSDB.
// Copyright (C) 2016-2017  The OpenTSDB Authors.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.uid.UniqueId.UniqueIdType;
import net.opentsdb.utils.Config;

import org.hbase.async.Bytes;
import org.hbase.async.KeyValue;
import org.hbase.async.Bytes.ByteMap;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import com.stumbleupon.async.Deferred;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@RunWith(PowerMockRunner.class)
//"Classloader hell"...  It's real.  Tell PowerMock to ignore these classes
//because they fiddle with the class loader.  We don't test them anyway.
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
             "ch.qos.*", "org.slf4j.*",
             "com.sum.*", "org.xml.*"})
@PrepareForTest({ HistogramSpan.class, HistogramRowSeq.class, TSDB.class, 
  UniqueId.class, KeyValue.class, Config.class, RowKey.class })
public final class TestHistogramSpan {
  protected TSDB tsdb = mock(TSDB.class);
  protected Config config = mock(Config.class);
  protected UniqueId metrics = mock(UniqueId.class);
  protected static final byte[] TABLE = { 't', 'a', 'b', 'l', 'e' };
  protected static final byte[] FAMILY = { 't' };
  protected static final byte[] ZERO = { 0 };
  
  protected byte[] hour1 = null;
  protected byte[] hour2 = null;
  protected byte[] hour3 = null;
  
  @Before
  public void before() throws Exception {
    // Inject the attributes we need into the "tsdb" object.
    Whitebox.setInternalState(tsdb, "metrics", metrics);
    Whitebox.setInternalState(tsdb, "table", TABLE);
    Whitebox.setInternalState(tsdb, "config", config);
    when(tsdb.getConfig()).thenReturn(config);
    when(tsdb.metrics.width()).thenReturn((short) 3);
    hour1 = BaseTsdbTest.getRowKey(
        BaseTsdbTest.generateUID(UniqueIdType.METRIC, (byte) 1), 
        1356998400, 
        BaseTsdbTest.generateUID(UniqueIdType.TAGK, (byte) 1),
        BaseTsdbTest.generateUID(UniqueIdType.TAGV, (byte) 1));
    hour2 = BaseTsdbTest.getRowKey(
        BaseTsdbTest.generateUID(UniqueIdType.METRIC, (byte) 1), 
        1357002000, 
        BaseTsdbTest.generateUID(UniqueIdType.TAGK, (byte) 1),
        BaseTsdbTest.generateUID(UniqueIdType.TAGV, (byte) 1));
    hour3 = BaseTsdbTest.getRowKey(
        BaseTsdbTest.generateUID(UniqueIdType.METRIC, (byte) 1), 
        1357005600, 
        BaseTsdbTest.generateUID(UniqueIdType.TAGK, (byte) 1),
        BaseTsdbTest.generateUID(UniqueIdType.TAGV, (byte) 1));
    
    when(RowKey.metricNameAsync(tsdb, hour1))
      .thenReturn(Deferred.fromResult("in.rps.latency"));
  }
  
  @Test
  public void addRow() {
    List<HistogramDataPoint> hdps = new ArrayList<HistogramDataPoint>();
    hdps.add(new LongHistogramDataPointForTest(100L, Bytes.fromLong(0)));
    hdps.add(new LongHistogramDataPointForTest(105L, Bytes.fromLong(1)));
    
    final HistogramSpan histSpan = new HistogramSpan(tsdb);
    histSpan.addRow(hour1, hdps);
    
    assertEquals(2, histSpan.size());
  }
  
  @Test (expected = NullPointerException.class)
  public void addRowNull() {
    final HistogramSpan histSpan = new HistogramSpan(tsdb);
    histSpan.addRow(hour1, null);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void addRowBadKeyLength() {
    List<HistogramDataPoint> row1 = new ArrayList<HistogramDataPoint>();
    row1.add(new LongHistogramDataPointForTest(100L, Bytes.fromLong(0)));
    row1.add(new LongHistogramDataPointForTest(105L, Bytes.fromLong(1)));
    
    final HistogramSpan histSpan = new HistogramSpan(tsdb);
    histSpan.addRow(hour1, row1);
    
    List<HistogramDataPoint> row2 = new ArrayList<HistogramDataPoint>();
    row2.add(new LongHistogramDataPointForTest(110L, Bytes.fromLong(2)));
    row2.add(new LongHistogramDataPointForTest(115L, Bytes.fromLong(3)));
    
    final byte[] bad_key = new byte[] { 0, 0, 0, 1, 0x50, (byte)0xE2, 0x43, 
        0x20, 0, 0, 0, 1 };
    histSpan.addRow(bad_key, row2);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void addRowMissMatchedMetric() {
    List<HistogramDataPoint> row1 = new ArrayList<HistogramDataPoint>();
    row1.add(new LongHistogramDataPointForTest(100L, Bytes.fromLong(0)));
    row1.add(new LongHistogramDataPointForTest(105L, Bytes.fromLong(1)));
    
    final HistogramSpan histSpan = new HistogramSpan(tsdb);
    histSpan.addRow(hour1, row1);
    
    List<HistogramDataPoint> row2 = new ArrayList<HistogramDataPoint>();
    row2.add(new LongHistogramDataPointForTest(110L, Bytes.fromLong(2)));
    row2.add(new LongHistogramDataPointForTest(115L, Bytes.fromLong(3)));
    
    final byte[] not_matched_mitric_key = new byte[] { 0, 0, 0, 2, 0x50, 
        (byte)0xE2, 0x35, 0x10, 0, 0, 0, 1, 0, 0, 0, 2 };
    histSpan.addRow(not_matched_mitric_key, row2);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void addRowMissMatchedTagk() {
    List<HistogramDataPoint> row1 = new ArrayList<HistogramDataPoint>();
    row1.add(new LongHistogramDataPointForTest(100L, Bytes.fromLong(0)));
    row1.add(new LongHistogramDataPointForTest(105L, Bytes.fromLong(1)));
    
    final HistogramSpan histSpan = new HistogramSpan(tsdb);
    histSpan.addRow(hour1, row1);
    
    List<HistogramDataPoint> row2 = new ArrayList<HistogramDataPoint>();
    row2.add(new LongHistogramDataPointForTest(110L, Bytes.fromLong(2)));
    row2.add(new LongHistogramDataPointForTest(115L, Bytes.fromLong(3)));
    
    final byte[] not_matched_tagk_key = new byte[] { 0, 0, 0, 1, 0x50, 
        (byte)0xE2, 0x35, 0x10, 0, 0, 0, 2, 0, 0, 0, 2 };
    histSpan.addRow(not_matched_tagk_key, row2);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void addRowMissMatchedTagv() {
    List<HistogramDataPoint> row1 = new ArrayList<HistogramDataPoint>();
    row1.add(new LongHistogramDataPointForTest(100L, Bytes.fromLong(0)));
    row1.add(new LongHistogramDataPointForTest(105L, Bytes.fromLong(1)));
    
    final HistogramSpan histSpan = new HistogramSpan(tsdb);
    histSpan.addRow(hour1, row1);
    
    List<HistogramDataPoint> row2 = new ArrayList<HistogramDataPoint>();
    row2.add(new LongHistogramDataPointForTest(110L, Bytes.fromLong(2)));
    row2.add(new LongHistogramDataPointForTest(115L, Bytes.fromLong(3)));
    
    final byte[] not_matched_tagv_key = new byte[] { 0, 0, 0, 1, 0x50, 
        (byte)0xE2, 0x35, 0x10, 0, 0, 0, 1, 0, 0, 0, 3 };
    histSpan.addRow(not_matched_tagv_key, row2);
  }
  
  @Test
  public void addRowOutOfOrder() {
    List<HistogramDataPoint> row2 = new ArrayList<HistogramDataPoint>();
    row2.add(new LongHistogramDataPointForTest(110L, Bytes.fromLong(2)));
    row2.add(new LongHistogramDataPointForTest(115L, Bytes.fromLong(3)));
    
    
    final HistogramSpan histSpan = new HistogramSpan(tsdb);
    histSpan.addRow(hour2, row2);
    
    List<HistogramDataPoint> row1 = new ArrayList<HistogramDataPoint>();
    row1.add(new LongHistogramDataPointForTest(100L, Bytes.fromLong(0)));
    row1.add(new LongHistogramDataPointForTest(105L, Bytes.fromLong(1)));
    histSpan.addRow(hour1, row1);
    
    assertEquals(4, histSpan.size());
    
    assertEquals(100L, histSpan.timestamp(0));
    assertEquals(105L, histSpan.timestamp(1));
    assertEquals(110L, histSpan.timestamp(2));
    assertEquals(115L, histSpan.timestamp(3));
  }

  @Test (expected = IllegalArgumentException.class)
  public void addDifferentKey() throws Exception {
    List<HistogramDataPoint> row1 = new ArrayList<HistogramDataPoint>();
    row1.add(new LongHistogramDataPointForTest(100L, Bytes.fromLong(0)));
    row1.add(new LongHistogramDataPointForTest(105L, Bytes.fromLong(1)));
    
    final HistogramSpan histSpan = new HistogramSpan(tsdb);
    histSpan.addRow(hour1, row1);
    
    final byte[] hour1_with_diff_key = Arrays.copyOf(hour1, hour1.length);
    hour1_with_diff_key[hour1_with_diff_key.length - 1] = 3;
    
    List<HistogramDataPoint> row2 = new ArrayList<HistogramDataPoint>();
    row2.add(new LongHistogramDataPointForTest(110L, Bytes.fromLong(2)));
    row2.add(new LongHistogramDataPointForTest(115L, Bytes.fromLong(3)));
    
    histSpan.addRow(hour1_with_diff_key, row2);
  }

  @Test
  public void getTagUids() {
    List<HistogramDataPoint> row1 = new ArrayList<HistogramDataPoint>();
    row1.add(new LongHistogramDataPointForTest(100L, Bytes.fromLong(0)));
    row1.add(new LongHistogramDataPointForTest(105L, Bytes.fromLong(1)));
    
    final HistogramSpan histSpan = new HistogramSpan(tsdb);
    histSpan.addRow(hour1, row1);
    
    final ByteMap<byte[]> uids = histSpan.getTagUids();
    assertEquals(1, uids.size());
    assertEquals(0, Bytes.memcmp(
        BaseTsdbTest.generateUID(UniqueIdType.TAGK, (byte) 1), uids.firstKey()));
    assertEquals(0, Bytes.memcmp(
        BaseTsdbTest.generateUID(UniqueIdType.TAGK, (byte) 1), 
        uids.firstEntry().getValue()));
  }
  
  @Test (expected = IllegalStateException.class)
  public void getTagUidsNotSet() {
    final HistogramSpan histSpan = new HistogramSpan(tsdb);
    histSpan.getTagUids();
  }

  @Test
  public void getAggregatedTagUids() {
    List<HistogramDataPoint> row1 = new ArrayList<HistogramDataPoint>();
    row1.add(new LongHistogramDataPointForTest(100L, Bytes.fromLong(0)));
    row1.add(new LongHistogramDataPointForTest(105L, Bytes.fromLong(1)));
    
    final HistogramSpan histSpan = new HistogramSpan(tsdb);
    histSpan.addRow(hour1, row1);
    
    final List<byte[]> uids = histSpan.getAggregatedTagUids();
    assertEquals(0, uids.size());
  }
  
  @Test
  public void getAggregatedTagUidsNotSet() {
    final HistogramSpan histSpan = new HistogramSpan(tsdb);
    assertTrue(histSpan.getAggregatedTagUids().isEmpty());
  }

  @Test (expected = IndexOutOfBoundsException.class)
  public void iteratorEmpty() {
    final HistogramSpan histSpan = new HistogramSpan(tsdb);
    assertFalse(histSpan.spanIterator().hasNext());
  }
  
}
