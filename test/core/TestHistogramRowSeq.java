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

import java.util.NoSuchElementException;

import net.opentsdb.uid.UniqueId;
import net.opentsdb.uid.UniqueId.UniqueIdType;
import net.opentsdb.utils.Config;

import org.hbase.async.Bytes;
import org.hbase.async.Bytes.ByteMap;
import org.hbase.async.KeyValue;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import com.stumbleupon.async.Deferred;

import java.util.ArrayList;
import java.util.List;

@RunWith(PowerMockRunner.class)
//"Classloader hell"...  It's real.  Tell PowerMock to ignore these classes
//because they fiddle with the class loader.  We don't test them anyway.
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
               "ch.qos.*", "org.slf4j.*",
               "com.sum.*", "org.xml.*"})
@PrepareForTest({ HistogramRowSeq.class, TSDB.class, UniqueId.class, KeyValue.class, 
  Config.class, RowKey.class })
public final class TestHistogramRowSeq {
  private TSDB tsdb = mock(TSDB.class);
  private Config config = mock(Config.class);
  private UniqueId metrics = mock(UniqueId.class);
  private static final byte[] TABLE = { 't', 'a', 'b', 'l', 'e' };
  public byte[] key = null;
  public static final byte[] FAMILY = { 't' };
  public static final byte[] ZERO = { 0 };
  
  @Before
  public void before() throws Exception {
    // Inject the attributes we need into the "tsdb" object.
    Whitebox.setInternalState(tsdb, "metrics", metrics);
    Whitebox.setInternalState(tsdb, "table", TABLE);
    Whitebox.setInternalState(tsdb, "config", config);
    when(tsdb.getConfig()).thenReturn(config);
    when(tsdb.metrics.width()).thenReturn((short) 3);
    key = BaseTsdbTest.getRowKey(
        BaseTsdbTest.generateUID(UniqueIdType.METRIC, (byte) 1), 
        1356998400, 
        BaseTsdbTest.generateUID(UniqueIdType.TAGK, (byte) 1),
        BaseTsdbTest.generateUID(UniqueIdType.TAGV, (byte) 1));
    when(RowKey.metricNameAsync(tsdb, key))
      .thenReturn(Deferred.fromResult("in.rps.latency"));
  }
  
  @Test
  public void setRow() throws Exception {
    List<HistogramDataPoint> hdps = new ArrayList<HistogramDataPoint>();
    hdps.add(new SimpleHistogramDataPointAdapter(
        new LongHistogramDataPointForTest(0, 0), 100L));
    hdps.add(new SimpleHistogramDataPointAdapter(
        new LongHistogramDataPointForTest(0, 1), 105L));
    
    final HistogramRowSeq hrs = new HistogramRowSeq(tsdb);
    hrs.setRow(key, hdps);
    
    assertEquals(2, hrs.size());
    assertEquals(100L, hrs.timestamp(0));
    assertEquals(105L, hrs.timestamp(1));
  }
   
  @Test (expected = IllegalStateException.class)
  public void setRowAlreadySet() throws Exception {
    List<HistogramDataPoint> hdps = new ArrayList<HistogramDataPoint>();
    hdps.add(new SimpleHistogramDataPointAdapter(
        new LongHistogramDataPointForTest(0, 0), 100L));
    hdps.add(new SimpleHistogramDataPointAdapter(
        new LongHistogramDataPointForTest(0, 1), 105L));
    
    final HistogramRowSeq hrs = new HistogramRowSeq(tsdb);
    hrs.setRow(key, hdps);
    hrs.setRow(key, hdps);
  }
  
  @Test
  public void addRowMergeLater() throws Exception {
    List<HistogramDataPoint> hdps = new ArrayList<HistogramDataPoint>();
    hdps.add(new SimpleHistogramDataPointAdapter(
        new LongHistogramDataPointForTest(0, 0), 100L));
    hdps.add(new SimpleHistogramDataPointAdapter(
        new LongHistogramDataPointForTest(0, 1), 105L));
    
    final HistogramRowSeq hrs = new HistogramRowSeq(tsdb);
    hrs.setRow(key, hdps);
    assertEquals(2, hrs.size());
    
    
    List<HistogramDataPoint> hdps2 = new ArrayList<HistogramDataPoint>();
    hdps2.add(new SimpleHistogramDataPointAdapter(
        new LongHistogramDataPointForTest(0, 2), 110L));
    hdps2.add(new SimpleHistogramDataPointAdapter(
        new LongHistogramDataPointForTest(0, 3), 115L));
    hrs.addRow(hdps2);
    assertEquals(4, hrs.size());
    
    assertEquals(100L, hrs.timestamp(0));
    assertEquals(105L, hrs.timestamp(1));
    assertEquals(110L, hrs.timestamp(2));
    assertEquals(115L, hrs.timestamp(3));
  }
  
  @Test
  public void addRowMergeEarlier() throws Exception {
    List<HistogramDataPoint> hdps = new ArrayList<HistogramDataPoint>();
    hdps.add(new SimpleHistogramDataPointAdapter(
        new LongHistogramDataPointForTest(0, 2), 110L));
    hdps.add(new SimpleHistogramDataPointAdapter(
        new LongHistogramDataPointForTest(0, 3), 115L));
    
    final HistogramRowSeq hrs = new HistogramRowSeq(tsdb);
    hrs.setRow(key, hdps);
    assertEquals(2, hrs.size());
    
    
    List<HistogramDataPoint> hdps2 = new ArrayList<HistogramDataPoint>();
    hdps2.add(new SimpleHistogramDataPointAdapter(
        new LongHistogramDataPointForTest(0, 0), 100L));
    hdps2.add(new SimpleHistogramDataPointAdapter(
        new LongHistogramDataPointForTest(0, 1), 105L));
    hrs.addRow(hdps2);
    assertEquals(4, hrs.size());
    
    assertEquals(100L, hrs.timestamp(0));
    assertEquals(105L, hrs.timestamp(1));
    assertEquals(110L, hrs.timestamp(2));
    assertEquals(115L, hrs.timestamp(3));
  }
  
  @Test
  public void addRowMergeMiddle() throws Exception {
    List<HistogramDataPoint> hdps = new ArrayList<HistogramDataPoint>();
    hdps.add(new SimpleHistogramDataPointAdapter(
        new LongHistogramDataPointForTest(0, 4), 120L));
    hdps.add(new SimpleHistogramDataPointAdapter(
        new LongHistogramDataPointForTest(0, 5), 125L));
    
    final HistogramRowSeq hrs = new HistogramRowSeq(tsdb);
    hrs.setRow(key, hdps);
    assertEquals(2, hrs.size());
    
    
    List<HistogramDataPoint> hdps2 = new ArrayList<HistogramDataPoint>();
    hdps2.add(new SimpleHistogramDataPointAdapter(
        new LongHistogramDataPointForTest(0, 0), 100L));
    hdps2.add(new SimpleHistogramDataPointAdapter(
        new LongHistogramDataPointForTest(0, 1), 105L));
    hrs.addRow(hdps2);
    assertEquals(4, hrs.size());
    
    List<HistogramDataPoint> hdps3 = new ArrayList<HistogramDataPoint>();
    hdps3.add(new SimpleHistogramDataPointAdapter(
        new LongHistogramDataPointForTest(0, 2), 110L));
    hdps3.add(new SimpleHistogramDataPointAdapter(
        new LongHistogramDataPointForTest(0, 3), 115L));
    hrs.addRow(hdps3);
    assertEquals(6, hrs.size());
    
    assertEquals(100L, hrs.timestamp(0));
    assertEquals(105L, hrs.timestamp(1));
    assertEquals(110L, hrs.timestamp(2));
    assertEquals(115L, hrs.timestamp(3));
    assertEquals(120L, hrs.timestamp(4));
    assertEquals(125L, hrs.timestamp(5));
  }
  
  @Test
  public void addRowMergeDuplicateLater() throws Exception {
    List<HistogramDataPoint> hdps = new ArrayList<HistogramDataPoint>();
    hdps.add(new SimpleHistogramDataPointAdapter(
        new LongHistogramDataPointForTest(0, 0), 100L));
    hdps.add(new SimpleHistogramDataPointAdapter(
        new LongHistogramDataPointForTest(0, 1), 105L));
    
    final HistogramRowSeq hrs = new HistogramRowSeq(tsdb);
    hrs.setRow(key, hdps);
    assertEquals(2, hrs.size());
    
    
    List<HistogramDataPoint> hdps2 = new ArrayList<HistogramDataPoint>();
    hdps2.add(new SimpleHistogramDataPointAdapter(
        new LongHistogramDataPointForTest(0, 100), 100L));
    hdps2.add(new SimpleHistogramDataPointAdapter(
        new LongHistogramDataPointForTest(0, 2), 110L));
    hrs.addRow(hdps2);
    assertEquals(3, hrs.size());
    
    assertEquals(100L, hrs.timestamp(0));
    assertEquals(105L, hrs.timestamp(1));
    assertEquals(110L, hrs.timestamp(2));
  }
  
  @Test
  public void timestamp() throws Exception {
    List<HistogramDataPoint> hdps = new ArrayList<HistogramDataPoint>();
    hdps.add(new SimpleHistogramDataPointAdapter(
        new LongHistogramDataPointForTest(0, 0), 100L));
    hdps.add(new SimpleHistogramDataPointAdapter(
        new LongHistogramDataPointForTest(0, 1), 105L));
    
    final HistogramRowSeq hrs = new HistogramRowSeq(tsdb);
    hrs.setRow(key, hdps);
    
    assertEquals(2, hrs.size());
    assertEquals(100L, hrs.timestamp(0));
    assertEquals(105L, hrs.timestamp(1));
  }
  
  @Test (expected = IndexOutOfBoundsException.class)
  public void timestampOutofBounds() throws Exception {
    List<HistogramDataPoint> hdps = new ArrayList<HistogramDataPoint>();
    hdps.add(new SimpleHistogramDataPointAdapter(
        new LongHistogramDataPointForTest(0, 0), 100L));
    hdps.add(new SimpleHistogramDataPointAdapter(
        new LongHistogramDataPointForTest(0, 1), 105L));
    
    final HistogramRowSeq hrs = new HistogramRowSeq(tsdb);
    hrs.setRow(key, hdps);
    
    assertEquals(2, hrs.size());
    assertEquals(100L, hrs.timestamp(0));
    assertEquals(105L, hrs.timestamp(1));
    hrs.timestamp(2);
  }
  
  @Test
  public void iterateAllItems() throws Exception {
    List<HistogramDataPoint> hdps = new ArrayList<HistogramDataPoint>();
    hdps.add(new SimpleHistogramDataPointAdapter(
        new LongHistogramDataPointForTest(0, 0), 100L));
    hdps.add(new SimpleHistogramDataPointAdapter(
        new LongHistogramDataPointForTest(0, 1), 105L));
    
    final HistogramRowSeq hrs = new HistogramRowSeq(tsdb);
    hrs.setRow(key, hdps);
    
    assertEquals(2, hrs.size());
    
    final HistogramSeekableView it = hrs.iterator();
    HistogramDataPoint hdp = it.next();
    
    assertEquals(100L, hdp.timestamp());
    assertEquals(0L, Bytes.getLong(hdp.getRawData(false)));
 
    hdp = it.next();    
    assertEquals(105L, hdp.timestamp());
    assertEquals(1L, Bytes.getLong(hdp.getRawData(false)));
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void iterateAfterMergeDuplicate() throws Exception {
    List<HistogramDataPoint> hdps = new ArrayList<HistogramDataPoint>();
    hdps.add(new SimpleHistogramDataPointAdapter(
        new LongHistogramDataPointForTest(0, 2), 110L));
    hdps.add(new SimpleHistogramDataPointAdapter(
        new LongHistogramDataPointForTest(0, 3), 115L));
    
    final HistogramRowSeq hrs = new HistogramRowSeq(tsdb);
    hrs.setRow(key, hdps);
    
    assertEquals(2, hrs.size());
    
    List<HistogramDataPoint> hdps2 = new ArrayList<HistogramDataPoint>();
    hdps2.add(
        new SimpleHistogramDataPointAdapter(
            new LongHistogramDataPointForTest(0, 20), 110L));
    hdps2.add(new SimpleHistogramDataPointAdapter(
        new LongHistogramDataPointForTest(0, 4), 120L));
    
    final HistogramSeekableView it = hrs.iterator();
    HistogramDataPoint hdp = it.next();
    
    assertEquals(110L, hdp.timestamp());
    assertEquals(2L, Bytes.getLong(hdp.getRawData(false)));
 
    hdp = it.next();    
    assertEquals(115L, hdp.timestamp());
    assertEquals(3L, Bytes.getLong(hdp.getRawData(false)));
    
    assertFalse(it.hasNext());
  }

  @Test
  public void iterateLarge() throws Exception {
    long ts = 100L;
    final int limit = 64 * 1000;
    List<HistogramDataPoint> hdps = new ArrayList<HistogramDataPoint>();
    for (int i = 0; i < limit; ++i) {
      hdps.add(new SimpleHistogramDataPointAdapter(
          new LongHistogramDataPointForTest(0, i), ts + 5 * i));
    }
    
    final HistogramRowSeq hrs = new HistogramRowSeq(tsdb);
    hrs.setRow(key, hdps);

    final HistogramSeekableView it = hrs.iterator();
    while (it.hasNext()) {
      assertEquals(ts, it.next().timestamp());
      ts += 5;
    }
    assertFalse(it.hasNext());
  }
  
  @Test
  public void seekStart() throws Exception {
    List<HistogramDataPoint> hdps = new ArrayList<HistogramDataPoint>();
    hdps.add(new SimpleHistogramDataPointAdapter(
        new LongHistogramDataPointForTest(0, 0), 100L));
    hdps.add(new SimpleHistogramDataPointAdapter(
        new LongHistogramDataPointForTest(0, 1), 105L));
    hdps.add(new SimpleHistogramDataPointAdapter(
        new LongHistogramDataPointForTest(0, 2), 110L));
    
    final HistogramRowSeq hrs = new HistogramRowSeq(tsdb);
    hrs.setRow(key, hdps);
    
    assertEquals(3, hrs.size());

    final HistogramSeekableView it = hrs.iterator();
    it.seek(100L);
    HistogramDataPoint hdp = it.next();
    assertEquals(100L, hdp.timestamp());
    assertEquals(0, Bytes.getLong(hdp.getRawData(false)));
    
    assertTrue(it.hasNext());
  }
  
  @Test
  public void seekMsBetween() throws Exception {
    List<HistogramDataPoint> hdps = new ArrayList<HistogramDataPoint>();
    hdps.add(new SimpleHistogramDataPointAdapter(
        new LongHistogramDataPointForTest(0, 0), 100L));
    hdps.add(new SimpleHistogramDataPointAdapter(
        new LongHistogramDataPointForTest(0, 1), 105L));
    hdps.add(new SimpleHistogramDataPointAdapter(
        new LongHistogramDataPointForTest(0, 2), 110L));
    
    final HistogramRowSeq hrs = new HistogramRowSeq(tsdb);
    hrs.setRow(key, hdps);
    
    assertEquals(3, hrs.size());

    final HistogramSeekableView it = hrs.iterator();
    it.seek(105L);
    HistogramDataPoint hdp = it.next();
    assertEquals(105L, hdp.timestamp());
    assertEquals(1, Bytes.getLong(hdp.getRawData(false)));
    
    assertTrue(it.hasNext());
  }
  
  @Test
  public void seekMsEnd() throws Exception {
    List<HistogramDataPoint> hdps = new ArrayList<HistogramDataPoint>();
    hdps.add(new SimpleHistogramDataPointAdapter(
        new LongHistogramDataPointForTest(0, 0), 100L));
    hdps.add(new SimpleHistogramDataPointAdapter(
        new LongHistogramDataPointForTest(0, 1), 105L));
    hdps.add(new SimpleHistogramDataPointAdapter(
        new LongHistogramDataPointForTest(0, 2), 110L));
    
    final HistogramRowSeq hrs = new HistogramRowSeq(tsdb);
    hrs.setRow(key, hdps);
    
    assertEquals(3, hrs.size());

    final HistogramSeekableView it = hrs.iterator();
    it.seek(110L);
    HistogramDataPoint hdp = it.next();
    assertEquals(110L, hdp.timestamp());
    assertEquals(2, Bytes.getLong(hdp.getRawData(false)));
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void seekMsTooEarly() throws Exception {
    List<HistogramDataPoint> hdps = new ArrayList<HistogramDataPoint>();
    hdps.add(new SimpleHistogramDataPointAdapter(
        new LongHistogramDataPointForTest(0, 1), 105L));
    hdps.add(new SimpleHistogramDataPointAdapter(
        new LongHistogramDataPointForTest(0, 2), 110L));
    
    final HistogramRowSeq hrs = new HistogramRowSeq(tsdb);
    hrs.setRow(key, hdps);
    
    assertEquals(2, hrs.size());

    final HistogramSeekableView it = hrs.iterator();
    it.seek(100L);
    HistogramDataPoint hdp = it.next();
    assertEquals(105L, hdp.timestamp());
    assertEquals(1, Bytes.getLong(hdp.getRawData(false)));
    
    assertTrue(it.hasNext());
  }
  
  @Test (expected = NoSuchElementException.class)
  public void seekMsPastLastDp() throws Exception {
    List<HistogramDataPoint> hdps = new ArrayList<HistogramDataPoint>();
    hdps.add(new SimpleHistogramDataPointAdapter(
        new LongHistogramDataPointForTest(0, 0), 100L));
    hdps.add(new SimpleHistogramDataPointAdapter(
        new LongHistogramDataPointForTest(0, 1), 105L));
    hdps.add(new SimpleHistogramDataPointAdapter(
        new LongHistogramDataPointForTest(0, 2), 110L));
    
    final HistogramRowSeq hrs = new HistogramRowSeq(tsdb);
    hrs.setRow(key, hdps);
    
    assertEquals(3, hrs.size());

    final HistogramSeekableView it = hrs.iterator();
    it.seek(200L);
    
    it.next();
  }
  
  @Test
  public void getTagUids() throws Exception {
    List<HistogramDataPoint> hdps = new ArrayList<HistogramDataPoint>();
    hdps.add(new SimpleHistogramDataPointAdapter(
        new LongHistogramDataPointForTest(0, 0), 100L));
    hdps.add(new SimpleHistogramDataPointAdapter(
        new LongHistogramDataPointForTest(0, 1), 105L));
    hdps.add(new SimpleHistogramDataPointAdapter(
        new LongHistogramDataPointForTest(0, 2), 110L));
    
    final HistogramRowSeq hrs = new HistogramRowSeq(tsdb);
    hrs.setRow(key, hdps);
    
    final ByteMap<byte[]> uids = hrs.getTagUids();
    assertEquals(1, uids.size());
    assertEquals(0, Bytes.memcmp(
        BaseTsdbTest.generateUID(UniqueIdType.TAGK, (byte) 1), uids.firstKey()));
    assertEquals(0, Bytes.memcmp(
        BaseTsdbTest.generateUID(UniqueIdType.TAGV, (byte) 1), 
        uids.firstEntry().getValue()));
  }
  
  @Test (expected = NullPointerException.class)
  public void getTagUidsNotSet() throws Exception {
    final HistogramRowSeq hrs = new HistogramRowSeq(tsdb);
    hrs.getTagUids();
  }
}
