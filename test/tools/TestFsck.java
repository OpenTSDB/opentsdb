// This file is part of OpenTSDB.
// Copyright (C) 2013  The OpenTSDB Authors.
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
package net.opentsdb.tools;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import net.opentsdb.core.Query;
import net.opentsdb.core.RowKey;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.Tags;
import net.opentsdb.meta.Annotation;
import net.opentsdb.storage.MockBase;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.Config;

import org.apache.zookeeper.proto.DeleteRequest;
import org.hbase.async.Bytes;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.Scanner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stumbleupon.async.Deferred;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
  "ch.qos.*", "org.slf4j.*",
  "com.sum.*", "org.xml.*"})
@PrepareForTest({TSDB.class, Config.class, UniqueId.class, HBaseClient.class, 
  GetRequest.class, PutRequest.class, KeyValue.class, Fsck.class,
  FsckOptions.class, Scanner.class, DeleteRequest.class, Annotation.class,
  RowKey.class, Tags.class})
public final class TestFsck {
  private final static byte[] ROW = 
    MockBase.stringToBytes("00000150E22700000001000001");
  private final static byte[] ROW2 = 
      MockBase.stringToBytes("00000150E23510000001000001");
  private final static byte[] ROW3 = 
      MockBase.stringToBytes("00000150E24320000001000001");
  private Config config;
  private TSDB tsdb = null;
  private HBaseClient client = mock(HBaseClient.class);
  private UniqueId metrics = mock(UniqueId.class);
  private UniqueId tag_names = mock(UniqueId.class);
  private UniqueId tag_values = mock(UniqueId.class);
  private MockBase storage;
  private FsckOptions options = mock(FsckOptions.class);
  private final static List<byte[]> tags = new ArrayList<byte[]>(1);
  static {
    tags.add(new byte[] { 0, 0, 1, 0, 0, 1});
  }

  @SuppressWarnings("unchecked")
  @Before
  public void before() throws Exception {
    PowerMockito.whenNew(HBaseClient.class)
      .withArguments(anyString(), anyString()).thenReturn(client);
    config = new Config(false);
    tsdb = new TSDB(config);
    when(client.flush()).thenReturn(Deferred.fromResult(null));
    
    storage = new MockBase(tsdb, client, true, true, true, true);
    storage.setFamily("t".getBytes(MockBase.ASCII()));
    
    when(options.fix()).thenReturn(false);
    when(options.compact()).thenReturn(false);
    when(options.resolveDupes()).thenReturn(false);
    when(options.lastWriteWins()).thenReturn(false);
    when(options.deleteOrphans()).thenReturn(false);
    when(options.deleteUnknownColumns()).thenReturn(false);
    when(options.deleteBadValues()).thenReturn(false);
    when(options.deleteBadRows()).thenReturn(false);
    when(options.deleteBadCompacts()).thenReturn(false);
    when(options.threads()).thenReturn(1);

    // replace the "real" field objects with mocks
    Field met = tsdb.getClass().getDeclaredField("metrics");
    met.setAccessible(true);
    met.set(tsdb, metrics);
    
    Field tagk = tsdb.getClass().getDeclaredField("tag_names");
    tagk.setAccessible(true);
    tagk.set(tsdb, tag_names);
    
    Field tagv = tsdb.getClass().getDeclaredField("tag_values");
    tagv.setAccessible(true);
    tagv.set(tsdb, tag_values);
    
    // mock UniqueId
    when(metrics.getId("sys.cpu.user")).thenReturn(new byte[] { 0, 0, 1 });
    when(metrics.getName(new byte[] { 0, 0, 1 })).thenReturn("sys.cpu.user");
    when(metrics.getId("sys.cpu.system"))
      .thenThrow(new NoSuchUniqueName("sys.cpu.system", "metric"));
    when(metrics.getId("sys.cpu.nice")).thenReturn(new byte[] { 0, 0, 2 });
    when(metrics.getName(new byte[] { 0, 0, 2 })).thenReturn("sys.cpu.nice");
    when(tag_names.getId("host")).thenReturn(new byte[] { 0, 0, 1 });
    when(tag_names.getName(new byte[] { 0, 0, 1 })).thenReturn("host");
    when(tag_names.getOrCreateId("host")).thenReturn(new byte[] { 0, 0, 1 });
    when(tag_names.getId("dc")).thenThrow(new NoSuchUniqueName("dc", "metric"));
    when(tag_values.getId("web01")).thenReturn(new byte[] { 0, 0, 1 });
    when(tag_values.getName(new byte[] { 0, 0, 1 })).thenReturn("web01");
    when(tag_values.getOrCreateId("web01")).thenReturn(new byte[] { 0, 0, 1 });
    when(tag_values.getId("web02")).thenReturn(new byte[] { 0, 0, 2 });
    when(tag_values.getName(new byte[] { 0, 0, 2 })).thenReturn("web02");
    when(tag_values.getOrCreateId("web02")).thenReturn(new byte[] { 0, 0, 2 });
    when(tag_values.getId("web03"))
      .thenThrow(new NoSuchUniqueName("web03", "metric"));
    
    PowerMockito.mockStatic(RowKey.class);
    when(RowKey.metricNameAsync((TSDB)any(), (byte[])any()))
      .thenReturn(Deferred.fromResult("sys.cpu.user"));

    PowerMockito.mockStatic(Tags.class);
    when(Tags.resolveIds((TSDB)any(), (ArrayList<byte[]>)any()))
      .thenReturn(null); // don't care
    
//    PowerMockito.mockStatic(Thread.class);
//    PowerMockito.doNothing().when(Thread.class);
//    Thread.sleep(anyLong());
    
    when(metrics.width()).thenReturn((short)3);
    when(tag_names.width()).thenReturn((short)3);
    when(tag_values.width()).thenReturn((short)3);
  }

  @Test
  public void globalAnnotation() throws Exception {
    // make sure we don't catch this during a query. We should start with
    // the first metric (0, 0, 1) whereas globals are on metric (0, 0, 0).
    storage.addColumn(new byte[] {0, 0, 0, 0x52, (byte)0xC3, 0x5A, (byte)0x80}, 
        new byte[] {1, 0, 0}, "{}".getBytes());
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(0, fsck.kvs_processed.get());
    assertEquals(0, fsck.totalErrors());
  }
  
  @Test
  public void noData() throws Exception {
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(0, fsck.kvs_processed.get());
    assertEquals(0, fsck.totalErrors());
  }
  
  @Test
  public void noErrors() throws Exception {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);

    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(1, fsck.rows_processed.get());
    assertEquals(0, fsck.totalErrors());
  }

  @Test
  public void noErrorsMultipleRows() throws Exception {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);

    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    storage.addColumn(ROW2, qual1, val1);
    storage.addColumn(ROW2, qual2, val2);
    storage.addColumn(ROW3, qual1, val1);
    storage.addColumn(ROW3, qual2, val2);
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(6, fsck.kvs_processed.get());
    assertEquals(3, fsck.rows_processed.get());
    assertEquals(0, fsck.totalErrors());
  }
  
  @Test
  public void noErrorsMilliseconds() throws Exception {
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    long timestamp = 1356998400000L;
    for (float i = 1.25F; i <= 76; i += 0.25F) {
      long ts = timestamp += 500;
      if ((ts % 1000) == 0) {
        ts = ts / 1000;
      }
      if (i % 2 == 0) {
        tsdb.addPoint("sys.cpu.user", ts, (long)i, tags).joinUninterruptibly();
      } else {
        tsdb.addPoint("sys.cpu.user", ts, i, tags).joinUninterruptibly();
      }
    }
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(300, fsck.kvs_processed.get());
    assertEquals(0, fsck.totalErrors());
  }

  @Test
  public void noErrorsAnnotation() throws Exception {
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] noteq = { 0x01, 0x00, 0x01 };
    final byte[] notev = "{}".getBytes();
    
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    storage.addColumn(ROW, noteq, notev);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(3, fsck.kvs_processed.get());
    assertEquals(1, fsck.annotations.get());
    assertEquals(0, fsck.totalErrors());
  }
  
  @Test
  public void noErrorsMixedMsAndSeconds() throws Exception {
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    long timestamp = 1356998400000L;
    for (float i = 1.25F; i <= 76; i += 0.25F) {
      long ts = timestamp += 500;
      if ((ts % 1000) == 0) {
        ts = ts / 1000;
      }
      if (i % 2 == 0) {
        tsdb.addPoint("sys.cpu.user", ts, (long)i, tags).joinUninterruptibly();
      } else {
        tsdb.addPoint("sys.cpu.user", ts, i, tags).joinUninterruptibly();
      }
    }
  
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(300, fsck.kvs_processed.get());
    assertEquals(0, fsck.totalErrors());
  }
  
  @Test
  public void noErrorsMixedMsAndSecondsAnnotations() throws Exception {
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    long timestamp = 1356998400000L;
    for (float i = 1.25F; i <= 76; i += 0.25F) {
      long ts = timestamp += 500;
      if ((ts % 1000) == 0) {
        ts = ts / 1000;
      }
      if (i % 2 == 0) {
        tsdb.addPoint("sys.cpu.user", ts, (long)i, tags).joinUninterruptibly();
      } else {
        tsdb.addPoint("sys.cpu.user", ts, i, tags).joinUninterruptibly();
      }
    }
    
    final byte[] noteq = { 0x01, 0x00, 0x01 };
    final byte[] notev = "{}".getBytes();
    storage.addColumn(ROW, noteq, notev);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(301, fsck.kvs_processed.get());
    assertEquals(1, fsck.annotations.get());
    assertEquals(0, fsck.totalErrors());
  }

  @Test
  public void noErrorsCompacted() throws Exception {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    final byte[] val12 = MockBase.concatByteArrays(val1, val2, new byte[] { 0 });
    storage.addColumn(ROW, qual12, val12);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(1, fsck.kvs_processed.get());
    assertEquals(0, fsck.totalErrors());
  }
  
  @Test
  public void noErrorsCompactedMS() throws Exception {
    final byte[] qual1 = { (byte) 0xF0, 0x00, 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x02, 0x07 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual3 = { (byte) 0xF0, 0x00, 0x04, 0x07 };
    final byte[] val3 = Bytes.fromLong(6L);
    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual1, qual2, qual3), 
        MockBase.concatByteArrays(val1, val2, val3, new byte[] { 0 }));
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(1, fsck.kvs_processed.get());
    assertEquals(0, fsck.totalErrors());
  }

  @Test
  public void noErrorsCompactedMix() throws Exception {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x02, 0x07 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    final byte[] val12 = MockBase.concatByteArrays(val1, val2, new byte[] { 0 });
    storage.addColumn(ROW, qual12, val12);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(1, fsck.kvs_processed.get());
    assertEquals(0, fsck.totalErrors());
  }
  
  @Test
  public void noErrorsCompactedMixReverse() throws Exception {
    final byte[] qual1 = { (byte) 0xF0, 0x00, 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    final byte[] val12 = MockBase.concatByteArrays(val1, val2, new byte[] { 0 });
    storage.addColumn(ROW, qual12, val12);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(1, fsck.kvs_processed.get());
    assertEquals(0, fsck.totalErrors());
  }
  
  @Test
  public void singleValueCompacted() throws Exception {
    when(options.fix()).thenReturn(true);
    
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    storage.addColumn(ROW, qual1, MockBase.concatByteArrays(val1, new byte[] { 0 }));
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(1, fsck.kvs_processed.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.correctable());
    assertArrayEquals(MockBase.concatByteArrays(val1, new byte[] { 0 }), 
        storage.getColumn(ROW, qual1));
  }
  
  @Test
  public void singleValueCompactedFix() throws Exception {
    when(options.fix()).thenReturn(true);
    when(options.deleteBadValues()).thenReturn(true);
    
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    storage.addColumn(ROW, qual1, MockBase.concatByteArrays(val1, new byte[] { 0 }));
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(1, fsck.kvs_processed.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.correctable());
    assertNull(storage.getColumn(ROW, qual1));
  }
  
  @Test
  public void noSuchMetricId() throws Exception {
    when(options.fix()).thenReturn(true);
    when(RowKey.metricNameAsync((TSDB)any(), (byte[])any()))
      .thenThrow(new NoSuchUniqueId("metric", new byte[] { 0, 0, 1 }));
    
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = new byte[] { 0, 0, 0, 0, 0, 0, 0,5 };

    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(0, fsck.kvs_processed.get());
    assertEquals(0, fsck.rows_processed.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.correctable());
    assertArrayEquals(val1, storage.getColumn(ROW, qual1));
  }
  
  @Test
  public void noSuchMetricIdFix() throws Exception {
    when(options.fix()).thenReturn(true);
    when(options.deleteOrphans()).thenReturn(true);
    when(RowKey.metricNameAsync((TSDB)any(), (byte[])any()))
      .thenThrow(new NoSuchUniqueId("metric", new byte[] { 0, 0, 1 }));
    
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = new byte[] { 0, 0, 0, 0, 0, 0, 0,5 };

    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(0, fsck.kvs_processed.get());
    assertEquals(0, fsck.rows_processed.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.correctable());
    assertNull(storage.getColumn(ROW, qual1));
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void noSuchTagId() throws Exception {
    when(options.fix()).thenReturn(true);
    when(Tags.resolveIds((TSDB)any(), (ArrayList<byte[]>)any()))
      .thenThrow(new NoSuchUniqueId("tagk", new byte[] { 0, 0, 1 }));
    
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = new byte[] { 0, 0, 0, 0, 0, 0, 0,5 };
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(0, fsck.kvs_processed.get());
    assertEquals(0, fsck.rows_processed.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.correctable());
    assertArrayEquals(val1, storage.getColumn(ROW, qual1));
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void noSuchTagIdFix() throws Exception {
    when(options.fix()).thenReturn(true);
    when(options.deleteOrphans()).thenReturn(true);
    when(Tags.resolveIds((TSDB)any(), (ArrayList<byte[]>)any()))
      .thenThrow(new NoSuchUniqueId("tagk", new byte[] { 0, 0, 1 }));
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = new byte[] { 0, 0, 0, 0, 0, 0, 0,5 };

    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(0, fsck.kvs_processed.get());
    assertEquals(0, fsck.rows_processed.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.correctable());
    assertNull(storage.getColumn(ROW, qual1));
  }
  
  @Test
  public void badRowKey() throws Exception {
    when(options.fix()).thenReturn(true);
    
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] bad_key = { 0x00, 0x00, 0x01 };
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    storage.addColumn(bad_key, qual1, val1);
    storage.addColumn(bad_key, qual2, val2);
    storage.addColumn(ROW3, qual1, val1);
    storage.addColumn(ROW3, qual2, val2);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(4, fsck.kvs_processed.get());
    assertEquals(3, fsck.rows_processed.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(2, storage.numColumns(ROW));
    assertEquals(2, storage.numColumns(bad_key));
    assertEquals(2, storage.numColumns(ROW3));
  }
  
  @Test
  public void badRowKeyFix() throws Exception {
    when(options.fix()).thenReturn(true);
    when(options.deleteBadRows()).thenReturn(true);
    
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] bad_key = { 0x00, 0x00, 0x01 };
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    storage.addColumn(bad_key, qual1, val1);
    storage.addColumn(bad_key, qual2, val2);
    storage.addColumn(ROW3, qual1, val1);
    storage.addColumn(ROW3, qual2, val2);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(4, fsck.kvs_processed.get());
    assertEquals(3, fsck.rows_processed.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(2, storage.numColumns(ROW));
    assertEquals(-1, storage.numColumns(bad_key));
    assertEquals(2, storage.numColumns(ROW3));
  }
  
  @Test
  public void lastCompactedByteNotZero() throws Exception {
    when(options.fix()).thenReturn(true);
    when(options.deleteBadCompacts()).thenReturn(true);
    
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    final byte[] val12 = MockBase.concatByteArrays(val1, val2);
    storage.addColumn(ROW, qual12, val12);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(1, fsck.kvs_processed.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.correctable());
    assertArrayEquals(val12, storage.getColumn(ROW,  qual12));
  }
  
  @Test
  public void oneByteQualifier() throws Exception {
    when(options.fix()).thenReturn(true);
    
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x01 };
    final byte[] val2 = new byte[] { 5 };
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.correctable());
    assertEquals(1, fsck.unknown.get());
  }
  
  @Test
  public void oneByteQualifierFix() throws Exception {
    when(options.fix()).thenReturn(true);
    when(options.deleteUnknownColumns()).thenReturn(true);
    
    final byte[] qual1 = { 0x00, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { 0x01 };
    final byte[] val2 = new byte[] { 5 };
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);

    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.correctable());
    assertEquals(1, fsck.unknown.get());
    assertArrayEquals(val1, storage.getColumn(ROW, qual1));
    assertNull(storage.getColumn(ROW, qual2));
  }
  
  @Test
  public void valueTooLong() throws Exception {
    when(options.fix()).thenReturn(true);
    
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 5 };
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.correctable());
    assertEquals(1, fsck.bad_values.get());
  }
  
  @Test
  public void valueTooLongFix() throws Exception {
    when(options.fix()).thenReturn(true);
    when(options.deleteBadValues()).thenReturn(true);
    
    final byte[] qual1 = { 0x00, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 5 };
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.correctable());
    assertEquals(1, fsck.bad_values.get());
    assertArrayEquals(val1, storage.getColumn(ROW, qual1));
    assertNull(storage.getColumn(ROW, qual2));
  }
  
  @Test
  public void valueTooLongMS() throws Exception {
    when(options.fix()).thenReturn(true);
    
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x02, 0x07 };
    final byte[] val2 = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 5 };
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.correctable());
    assertEquals(1, fsck.bad_values.get());
  }

  @Test
  public void valueTooLongMSFix() throws Exception {
    when(options.fix()).thenReturn(true);
    when(options.deleteBadValues()).thenReturn(true);
    
    final byte[] qual1 = { 0x00, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x02, 0x07 };
    final byte[] val2 = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 5 };
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.correctable());
    assertEquals(1, fsck.bad_values.get());
    assertArrayEquals(val1, storage.getColumn(ROW, qual1));
    assertNull(storage.getColumn(ROW, qual2));
  }
  
  @Test
  public void valueTooShort() throws Exception {
    when(options.fix()).thenReturn(true);
    
    final byte[] qual1 = { 0x00, 0x00 };
    final byte[] val1 = { 4 } ;
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = new byte[] { 0, 0, 0, 5 };
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.correctable());
    assertEquals(1, fsck.bad_values.get());
  }
  
  @Test
  public void valueTooShortFix() throws Exception {
    when(options.fix()).thenReturn(true);
    when(options.deleteBadValues()).thenReturn(true);
    
    final byte[] qual1 = { 0x00, 0x00 };
    final byte[] val1 = { 4 } ;
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = new byte[] { 0, 0, 0, 5 };
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.correctable());
    assertEquals(1, fsck.bad_values.get());
    assertArrayEquals(val1, storage.getColumn(ROW, qual1));
    assertNull(storage.getColumn(ROW, qual2));
  }
  
  @Test
  public void valueTooShortMS() throws Exception {
    when(options.fix()).thenReturn(true);
    
    final byte[] qual1 = { 0x00, 0x00 };
    final byte[] val1 =  { 4 };
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x02, 0x03 };
    final byte[] val2 = new byte[] { 0, 0, 5 };
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.correctable());
    assertEquals(1, fsck.bad_values.get());
  }
  
  @Test
  public void valueTooShortMSFix() throws Exception {
    when(options.fix()).thenReturn(true);
    when(options.deleteBadValues()).thenReturn(true);
    
    final byte[] qual1 = { 0x00, 0x00 };
    final byte[] val1 =  { 4 };
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x02, 0x03 };
    final byte[] val2 = new byte[] { 0, 0, 5 };
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.correctable());
    assertEquals(1, fsck.bad_values.get());
    assertArrayEquals(val1, storage.getColumn(ROW, qual1));
    assertNull(storage.getColumn(ROW, qual2));
  }
  
  @Test
  public void float8byteVal4byteQual() throws Exception {
    final byte[] qual1 = { 0x00, 0x0B };
    final byte[] val1 = Bytes.fromLong(Float.floatToRawIntBits(4.2F));
    final byte[] qual2 = { 0x00, 0x2B };
    final byte[] val2 = Bytes.fromLong(Float.floatToRawIntBits(500.8F));
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(2, fsck.totalErrors());
    assertEquals(2, fsck.value_encoding.get());
    assertEquals(2, fsck.correctable());
    assertArrayEquals(val1, storage.getColumn(ROW, qual1));
    assertArrayEquals(val2, storage.getColumn(ROW, qual2));
  }
  
  @Test
  public void float8byteVal4byteQualFix() throws Exception {
    when(options.fix()).thenReturn(true);

    final byte[] qual1 = { 0x00, 0x0B };
    final byte[] val1 = Bytes.fromLong(Float.floatToRawIntBits(4.2F));
    final byte[] fixed_val1 = Bytes.fromInt(Float.floatToRawIntBits(4.2F));
    final byte[] qual2 = { 0x00, 0x2B };
    final byte[] val2 = Bytes.fromLong(Float.floatToRawIntBits(500.8F));
    final byte[] fixed_val2 = Bytes.fromInt(Float.floatToRawIntBits(500.8F));
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(2, fsck.totalErrors());
    assertEquals(2, fsck.totalFixed());
    assertEquals(2, fsck.correctable());
    assertEquals(2, fsck.value_encoding.get());
    assertArrayEquals(fixed_val1, storage.getColumn(ROW, qual1));
    assertArrayEquals(fixed_val2, storage.getColumn(ROW, qual2));
  }
  
  @Test
  public void float8byteVal8byteQual() throws Exception {
    final byte[] qual1 = { 0x00, 0x0F };
    final byte[] val1 = Bytes.fromLong(Double.doubleToRawLongBits(4.2F));
    final byte[] qual2 = { 0x00, 0x2F };
    final byte[] val2 = Bytes.fromLong(Double.doubleToRawLongBits(500.8F));
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(0, fsck.totalErrors());
    assertEquals(0, fsck.value_encoding.get());
    assertEquals(0, fsck.correctable());
    assertArrayEquals(val1, storage.getColumn(ROW, qual1));
    assertArrayEquals(val2, storage.getColumn(ROW, qual2));
  }
  
  @Test
  public void float8byteVal4byteQualSignExtensionBug() throws Exception {
    final byte[] qual1 = { 0x00, 0x0B };
    final byte[] val1 = Bytes.fromInt(Float.floatToRawIntBits(4.2F));
    final byte[] qual2 = { 0x00, 0x2B };
    final byte[] bug = { (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF };
    final byte[] val2 = Bytes.fromInt(Float.floatToRawIntBits(500.8F));
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, MockBase.concatByteArrays(bug, val2));
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(0, fsck.totalFixed());
    assertEquals(1, fsck.correctable());
    assertEquals(1, fsck.value_encoding.get());
  }
  
  @Test
  public void float8byteVal4byteQualSignExtensionBugFix() throws Exception {
    when(options.fix()).thenReturn(true);
    
    final byte[] qual1 = { 0x00, 0x0B };
    final byte[] val1 = Bytes.fromInt(Float.floatToRawIntBits(4.2F));
    final byte[] qual2 = { 0x00, 0x2B };
    final byte[] bug = { (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF };
    final byte[] val2 = Bytes.fromInt(Float.floatToRawIntBits(500.8F));
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, MockBase.concatByteArrays(bug, val2));
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.totalFixed());
    assertEquals(1, fsck.correctable());
    assertEquals(1, fsck.value_encoding.get());
    assertArrayEquals(val2, storage.getColumn(ROW, qual2));
  }

  @Test
  public void float8byteVal4byteQualSignExtensionBugCompacted() 
      throws Exception {
    when(options.fix()).thenReturn(true);
    
    final byte[] qual1 = { 0x00, 0x0B };
    final byte[] val1 = Bytes.fromLong(Float.floatToRawIntBits(4.2F));
    final byte[] qual2 = { 0x00, 0x2B };
    final byte[] bug = { (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF };
    final byte[] val2 = Bytes.fromInt(Float.floatToRawIntBits(500.8F));
    storage.addColumn(ROW, MockBase.concatByteArrays(qual1, qual2), 
        MockBase.concatByteArrays(val1, bug, val2, new byte[] { 0 }));
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(1, fsck.kvs_processed.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(0, fsck.totalFixed());
    assertEquals(1, fsck.correctable());
    assertEquals(1, fsck.bad_compacted_columns.get());
  }
  
  @Test
  public void float8byteVal4byteQualSignExtensionBugCompactedFix() 
      throws Exception {
    when(options.fix()).thenReturn(true);
    when(options.deleteBadCompacts()).thenReturn(true);
    
    final byte[] qual1 = { 0x00, 0x0B };
    final byte[] val1 = Bytes.fromLong(Float.floatToRawIntBits(4.2F));
    final byte[] qual2 = { 0x00, 0x2B };
    final byte[] bug = { (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF };
    final byte[] val2 = Bytes.fromInt(Float.floatToRawIntBits(500.8F));
    storage.addColumn(ROW, MockBase.concatByteArrays(qual1, qual2), 
        MockBase.concatByteArrays(val1, bug, val2, new byte[] { 0 }));
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(1, fsck.kvs_processed.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(0, fsck.totalFixed());
    assertEquals(1, fsck.correctable());
    assertEquals(1, fsck.bad_compacted_columns.get());
    assertEquals(1, fsck.bad_compacted_columns_deleted.get());
    assertNull(storage.getColumn(ROW, MockBase.concatByteArrays(qual1, qual2)));
  }
  
  @Test
  public void float8byteVal4byteQualMessedUp() throws Exception {
    when(options.fix()).thenReturn(true);
    
    final byte[] qual1 = { 0x00, 0x0B };
    final byte[] val1 = Bytes.fromInt(Float.floatToRawIntBits(4.2F));
    final byte[] qual2 = { 0x00, 0x2B };
    final byte[] bug = { (byte) 0xFB, (byte) 0x02, (byte) 0xF4, (byte) 0x0F };
    final byte[] val2 = Bytes.fromInt(Float.floatToRawIntBits(500.8F));
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, MockBase.concatByteArrays(bug, val2));
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(0, fsck.totalFixed());
    assertEquals(1, fsck.correctable());
    assertEquals(1, fsck.bad_values.get());
    assertArrayEquals(val1, storage.getColumn(ROW, qual1));
    assertArrayEquals(MockBase.concatByteArrays(bug, val2), 
        storage.getColumn(ROW, qual2));
  }

  @Test
  public void float8byteVal4byteQualMessedUpFix() throws Exception {
    when(options.fix()).thenReturn(true);
    when(options.deleteBadValues()).thenReturn(true);
    
    final byte[] qual1 = { 0x00, 0x0B };
    final byte[] val1 = Bytes.fromInt(Float.floatToRawIntBits(4.2F));
    final byte[] qual2 = { 0x00, 0x2B };
    final byte[] bug = { (byte) 0xFB, (byte) 0x02, (byte) 0xF4, (byte) 0x0F };
    final byte[] val2 = Bytes.fromInt(Float.floatToRawIntBits(500.8F));
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, MockBase.concatByteArrays(bug, val2));
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.totalFixed());
    assertEquals(1, fsck.correctable());
    assertEquals(1, fsck.bad_values.get());
    assertEquals(1, fsck.bad_values_deleted.get());
    assertArrayEquals(val1, storage.getColumn(ROW, qual1));
    assertNull(storage.getColumn(ROW, qual2));
  }
  
  @Test
  public void float4byteVal8byteQual() throws Exception {
    when(options.fix()).thenReturn(true);
    
    final byte[] qual1 = { 0x00, 0x0F };
    final byte[] val1 = Bytes.fromInt(Float.floatToRawIntBits(42.5F));
    final byte[] qual2 = { 0x00, 0x2F };
    final byte[] val2 = Bytes.fromInt(Float.floatToRawIntBits(500.8F));
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(2, fsck.totalErrors());
    assertEquals(2, fsck.bad_values.get());
    assertEquals(2, fsck.correctable());
    assertArrayEquals(val1, storage.getColumn(ROW, qual1));
    assertArrayEquals(val2, storage.getColumn(ROW, qual2));
  }
  
  @Test
  public void float4byteVal8byteQualFix() throws Exception {
    when(options.fix()).thenReturn(true);
    when(options.deleteBadValues()).thenReturn(true);
    
    final byte[] qual1 = { 0x00, 0x0F };
    final byte[] val1 = Bytes.fromInt(Float.floatToRawIntBits(42.5F));
    final byte[] qual2 = { 0x00, 0x2F };
    final byte[] val2 = Bytes.fromInt(Float.floatToRawIntBits(500.8F));
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(2, fsck.totalErrors());
    assertEquals(2, fsck.bad_values.get());
    assertEquals(2, fsck.correctable());
    assertNull(storage.getColumn(ROW, qual1));
    assertNull(storage.getColumn(ROW, qual2));
  }
  
  @Test
  public void floatBadVal4ByteQual() throws Exception {
    when(options.fix()).thenReturn(true);
    
    final byte[] qual1 = { 0x00, 0x0B };
    final byte[] val1 = Bytes.fromInt(Float.floatToRawIntBits(4.2F));
    final byte[] qual2 = { 0x00, 0x2B };
    final byte[] val2 = { 1 };
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(0, fsck.totalFixed());
    assertEquals(1, fsck.correctable());
    assertEquals(1, fsck.bad_values.get());
    assertArrayEquals(val1, storage.getColumn(ROW, qual1));
    assertArrayEquals(val2, storage.getColumn(ROW, qual2));
  }

  @Test
  public void floatBadVal4ByteQualFix() throws Exception {
    when(options.fix()).thenReturn(true);
    when(options.deleteBadValues()).thenReturn(true);
    
    final byte[] qual1 = { 0x00, 0x0B };
    final byte[] val1 = Bytes.fromInt(Float.floatToRawIntBits(4.2F));
    final byte[] qual2 = { 0x00, 0x2B };
    final byte[] val2 = { 1 };
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.totalFixed());
    assertEquals(1, fsck.correctable());
    assertEquals(1, fsck.bad_values.get());
    assertEquals(1, fsck.bad_values_deleted.get());
    assertArrayEquals(val1, storage.getColumn(ROW, qual1));
    assertNull(storage.getColumn(ROW, qual2));
  }
  
  @Test
  public void floatBadVal8ByteQual() throws Exception {
    when(options.fix()).thenReturn(true);
    
    final byte[] qual1 = { 0x00, 0x0B };
    final byte[] val1 = Bytes.fromInt(Float.floatToRawIntBits(4.2F));
    final byte[] qual2 = { 0x00, 0x2F };
    final byte[] val2 = { 1 };
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(0, fsck.totalFixed());
    assertEquals(1, fsck.correctable());
    assertEquals(1, fsck.bad_values.get());
    assertArrayEquals(val1, storage.getColumn(ROW, qual1));
    assertArrayEquals(val2, storage.getColumn(ROW, qual2));
  }

  @Test
  public void floatBadVal8ByteQualFix() throws Exception {
    when(options.fix()).thenReturn(true);
    when(options.deleteBadValues()).thenReturn(true);
    
    final byte[] qual1 = { 0x00, 0x0B };
    final byte[] val1 = Bytes.fromInt(Float.floatToRawIntBits(4.2F));
    final byte[] qual2 = { 0x00, 0x2F };
    final byte[] val2 = { 1 };
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.totalFixed());
    assertEquals(1, fsck.correctable());
    assertEquals(1, fsck.bad_values.get());
    assertEquals(1, fsck.bad_values_deleted.get());
    assertArrayEquals(val1, storage.getColumn(ROW, qual1));
    assertNull(storage.getColumn(ROW, qual2));
  }
  
  @Test
  public void unknownObject() throws Exception {
    when(options.fix()).thenReturn(true);
    
    final byte[] qual1 = { 0x00, 0x07};
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27, 0x04, 0x01, 0x01, 0x01, 0x01 };
    final byte[] val2 = Bytes.fromLong(5L);
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(0, fsck.totalFixed());
    assertEquals(1, fsck.correctable());
    assertEquals(1, fsck.unknown.get());
  }
  
  @Test
  public void unknownObjectFix() throws Exception {
    when(options.fix()).thenReturn(true);
    when(options.deleteUnknownColumns()).thenReturn(true);
    
    final byte[] qual1 = { 0x00, 0x00};
    final byte[] val1 = { 4 };
    final byte[] qual2 = { 0x00, 0x27, 0x04, 0x01, 0x01, 0x01, 0x01 };
    final byte[] val2 = Bytes.fromLong(5L);
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.totalFixed());
    assertEquals(1, fsck.correctable());
    assertEquals(1, fsck.unknown.get());
    assertEquals(1, fsck.unknown_fixed.get());
    assertArrayEquals(val1, storage.getColumn(ROW, qual1));
    assertNull(storage.getColumn(ROW, qual2));
  }
  
  @Test
  public void futureObject() throws Exception {
    when(options.fix()).thenReturn(true);
    
    final byte[] qual1 = { 0x00, 0x07};
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x04, 0x27, 0x04 };
    final byte[] val2 = "Future Object".getBytes();
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(0, fsck.totalErrors());
    assertEquals(0, fsck.totalFixed());
    assertEquals(0, fsck.correctable());
    assertEquals(1, fsck.future.get());
    assertArrayEquals(val2, storage.getColumn(ROW, qual2));
  }
  
  @Test
  public void futureObjectShouldNotDelete() throws Exception {
    when(options.fix()).thenReturn(true);
    when(options.deleteUnknownColumns()).thenReturn(true);
    
    final byte[] qual1 = { 0x00, 0x07};
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x04, 0x27, 0x04 };
    final byte[] val2 = "Future Object".getBytes();
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(0, fsck.totalErrors());
    assertEquals(0, fsck.totalFixed());
    assertEquals(0, fsck.correctable());
    assertEquals(1, fsck.future.get());
    assertArrayEquals(val2, storage.getColumn(ROW, qual2));
  }
  
  @Test
  public void integerWrongLength() throws Exception {
    when(options.fix()).thenReturn(true);
    
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x23 };
    final byte[] val2 = Bytes.fromLong(5L);
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(0, fsck.totalFixed());
    assertEquals(1, fsck.correctable());
    assertEquals(1, fsck.bad_values.get());
  }
  
  @Test
  public void integerWrongLengthFix() throws Exception {
    when(options.fix()).thenReturn(true);
    when(options.deleteBadValues()).thenReturn(true);
    
    final byte[] qual1 = { 0x00, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { 0x00, 0x23 };
    final byte[] val2 = Bytes.fromLong(5L);
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.totalFixed());
    assertEquals(1, fsck.correctable());
    assertEquals(1, fsck.bad_values.get());
    assertArrayEquals(val1, storage.getColumn(ROW, qual1));
    assertNull(storage.getColumn(ROW, qual2));
  }

  @Test
  public void compactOutOfOrder() throws Exception {   
    final byte[] qual1 = { 0x0, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { 0x0, 0x20 };
    final byte[] val2 = { 5 };
    final byte[] qual3 = { 0x0, 0x30 };
    final byte[] val3 = { 6 };
    storage.addColumn(ROW, MockBase.concatByteArrays(qual1, qual3, qual2), 
        MockBase.concatByteArrays(val1, val3, val2, new byte[] { 0 }));

    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(1, fsck.kvs_processed.get());
    assertEquals(1, fsck.fixable_compacted_columns.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.correctable());
    assertArrayEquals(MockBase.concatByteArrays(val1, val3, val2, new byte[] { 0 }), 
        storage.getColumn(ROW, MockBase.concatByteArrays(qual1, qual3, qual2)));
  }
  
  @Test
  public void compactOutOfOrderFix() throws Exception {
    when(options.fix()).thenReturn(true);
    
    final byte[] qual1 = { 0x0, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { 0x0, 0x20 };
    final byte[] val2 = { 5 };
    final byte[] qual3 = { 0x0, 0x30 };
    final byte[] val3 = { 6 };
    storage.addColumn(ROW, MockBase.concatByteArrays(qual1, qual3, qual2), 
        MockBase.concatByteArrays(val1, val3, val2, new byte[] { 0 }));

    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(1, fsck.kvs_processed.get());
    assertEquals(1, fsck.fixable_compacted_columns.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.correctable());
    assertArrayEquals(MockBase.concatByteArrays(val1, val2, val3, new byte[] { 0 }), 
        storage.getColumn(ROW, MockBase.concatByteArrays(qual1, qual2, qual3)));
  }
  
  @Test
  public void compactWithoutFix() throws Exception {
    when(options.compact()).thenReturn(true);
    
    final byte[] qual1 = { 0x0, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { 0x0, 0x20 };
    final byte[] val2 = { 5 };
    final byte[] qual3 = { 0x0, 0x30 };
    final byte[] val3 = { 6 };
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    storage.addColumn(ROW, qual3, val3);

    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(3, fsck.kvs_processed.get());
    assertEquals(0, fsck.vle.get());
    assertEquals(0, fsck.totalErrors());
    assertEquals(0, fsck.correctable());
    assertArrayEquals(val1, storage.getColumn(ROW, qual1));
    assertArrayEquals(val2, storage.getColumn(ROW, qual2));
    assertArrayEquals(val3, storage.getColumn(ROW, qual3));
  }
  
  @Test
  public void compactFix() throws Exception {
    when(options.compact()).thenReturn(true);
    when(options.fix()).thenReturn(true);
    
    final byte[] qual1 = { 0x0, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { 0x0, 0x20 };
    final byte[] val2 = { 5 };
    final byte[] qual3 = { 0x0, 0x30 };
    final byte[] val3 = { 6 };
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    storage.addColumn(ROW, qual3, val3);

    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(3, fsck.kvs_processed.get());
    assertEquals(0, fsck.vle.get());
    assertEquals(0, fsck.totalErrors());
    assertEquals(0, fsck.correctable());
    assertArrayEquals(MockBase.concatByteArrays(val1, val2, val3, new byte[] {0}), 
        storage.getColumn(ROW, MockBase.concatByteArrays(qual1, qual2, qual3)));
    assertNull(storage.getColumn(ROW, qual1));
    assertNull(storage.getColumn(ROW, qual2));
    assertNull(storage.getColumn(ROW, qual3));
  }
  
  @Test
  public void compactTwoRowsWithoutFix() throws Exception {
    when(options.compact()).thenReturn(true);
    
    final byte[] qual1 = { 0x0, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { 0x0, 0x20 };
    final byte[] val2 = { 5 };
    final byte[] qual3 = { 0x0, 0x30 };
    final byte[] val3 = { 6 };
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    storage.addColumn(ROW2, qual2, val2);
    storage.addColumn(ROW2, qual3, val3);

    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(4, fsck.kvs_processed.get());
    assertEquals(0, fsck.vle.get());
    assertEquals(0, fsck.totalErrors());
    assertEquals(0, fsck.correctable());
    assertArrayEquals(val1, storage.getColumn(ROW, qual1));
    assertArrayEquals(val2, storage.getColumn(ROW, qual2));
    assertArrayEquals(val2, storage.getColumn(ROW2, qual2));
    assertArrayEquals(val3, storage.getColumn(ROW2, qual3));
  }
  
  @Test
  public void compactTwoRowsFix() throws Exception {
    when(options.compact()).thenReturn(true);
    when(options.fix()).thenReturn(true);
    
    final byte[] qual1 = { 0x0, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { 0x0, 0x20 };
    final byte[] val2 = { 5 };
    final byte[] qual3 = { 0x0, 0x30 };
    final byte[] val3 = { 6 };
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    storage.addColumn(ROW2, qual2, val2);
    storage.addColumn(ROW2, qual3, val3);

    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(4, fsck.kvs_processed.get());
    assertEquals(0, fsck.vle.get());
    assertEquals(0, fsck.totalErrors());
    assertEquals(0, fsck.correctable());
    assertArrayEquals(MockBase.concatByteArrays(val1, val2, new byte[] {0}), 
        storage.getColumn(ROW, MockBase.concatByteArrays(qual1, qual2)));
    assertArrayEquals(MockBase.concatByteArrays(val2, val3, new byte[] {0}), 
        storage.getColumn(ROW2, MockBase.concatByteArrays(qual2, qual3)));
    assertNull(storage.getColumn(ROW, qual1));
    assertNull(storage.getColumn(ROW, qual2));
    assertNull(storage.getColumn(ROW2, qual2));
    assertNull(storage.getColumn(ROW2, qual3));
  }
  
  @Test
  public void badCompactTooShort() throws Exception {
    when(options.fix()).thenReturn(true);
    
    final byte[] qual1 = { 0x0, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x0, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual3 = { 0x0, 0x37 };
    final byte[] val3 = Bytes.fromInt(6);
    storage.addColumn(ROW, MockBase.concatByteArrays(qual1, qual2, qual3), 
        MockBase.concatByteArrays(val1, val2, val3, new byte[] {0}));

    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(1, fsck.kvs_processed.get());
    assertEquals(1, fsck.bad_compacted_columns.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.correctable());
    assertArrayEquals(MockBase.concatByteArrays(val1, val2, val3, new byte[] {0}), 
        storage.getColumn(ROW, MockBase.concatByteArrays(qual1, qual2, qual3)));
  }
  
  @Test
  public void badCompactTooShortFix() throws Exception {
    when(options.fix()).thenReturn(true);
    when(options.deleteBadCompacts()).thenReturn(true);
    
    final byte[] qual1 = { 0x0, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x0, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual3 = { 0x0, 0x37 };
    final byte[] val3 = Bytes.fromInt(6);
    storage.addColumn(ROW, MockBase.concatByteArrays(qual1, qual2, qual3), 
        MockBase.concatByteArrays(val1, val2, val3, new byte[] {0}));

    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(1, fsck.kvs_processed.get());
    assertEquals(1, fsck.bad_compacted_columns.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.correctable());
    assertEquals(-1, storage.numColumns(ROW));
  }
  
  @Test
  public void badCompactTooLong() throws Exception {
    when(options.fix()).thenReturn(true);
    
    final byte[] qual1 = { 0x0, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x0, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual3 = { 0x0, 0x33 };
    final byte[] val3 = Bytes.fromLong(6);
    storage.addColumn(ROW, MockBase.concatByteArrays(qual1, qual2, qual3), 
        MockBase.concatByteArrays(val1, val2, val3, new byte[] {0}));

    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(1, fsck.kvs_processed.get());
    assertEquals(1, fsck.bad_compacted_columns.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.correctable());
    assertArrayEquals(MockBase.concatByteArrays(val1, val2, val3, new byte[] {0}), 
        storage.getColumn(ROW, MockBase.concatByteArrays(qual1, qual2, qual3)));
  }
  
  @Test
  public void badCompactTooLongFix() throws Exception {
    when(options.fix()).thenReturn(true);
    when(options.deleteBadCompacts()).thenReturn(true);
    
    final byte[] qual1 = { 0x0, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x0, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual3 = { 0x0, 0x33 };
    final byte[] val3 = Bytes.fromLong(6);
    storage.addColumn(ROW, MockBase.concatByteArrays(qual1, qual2, qual3), 
        MockBase.concatByteArrays(val1, val2, val3, new byte[] {0}));

    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(1, fsck.kvs_processed.get());
    assertEquals(1, fsck.bad_compacted_columns.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.correctable());
    assertEquals(-1, storage.numColumns(ROW));
  }
  
  // VLE --------------------------------------------
  
  @Test
  public void integerVle1Byte() throws Exception {
    final byte[] qual1 = { 0x00, 0x00 };
    final byte[] val1 = new byte[] { 1 };
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(2L);
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(0, fsck.totalErrors());
    assertEquals(0, fsck.totalFixed());
    assertEquals(0, fsck.correctable());
    assertEquals(1, fsck.vle.get());
    assertEquals(7, fsck.vle_bytes.get());
    assertArrayEquals(val1, storage.getColumn(ROW, qual1));
    assertArrayEquals(val2, storage.getColumn(ROW, qual2));
  }
  
  @Test
  public void integerVle1ByteFix() throws Exception {
    when(options.fix()).thenReturn(true);
    
    final byte[] qual1 = { 0x00, 0x00 };
    final byte[] val1 = new byte[] { 1 };
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(2L);
    final byte[] fixed_qual2 = { 0x00, 0x20 };
    final byte[] fixed_val2 = new byte[] { 2 };
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(0, fsck.totalErrors());
    assertEquals(0, fsck.totalFixed());
    assertEquals(0, fsck.correctable());
    assertEquals(1, fsck.vle.get());
    assertEquals(7, fsck.vle_bytes.get());
    assertEquals(1, fsck.vle_fixed.get());
    assertArrayEquals(val1, storage.getColumn(ROW, qual1));
    assertNull(storage.getColumn(ROW, qual2));
    assertArrayEquals(fixed_val2, storage.getColumn(ROW, fixed_qual2));
  }
  
  @Test
  public void integerVle1ByteNegative() throws Exception {
    final byte[] qual1 = { 0x00, 0x00 };
    final byte[] val1 = new byte[] { 1 };
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(-2L);
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(0, fsck.totalErrors());
    assertEquals(0, fsck.totalFixed());
    assertEquals(0, fsck.correctable());
    assertEquals(1, fsck.vle.get());
    assertEquals(7, fsck.vle_bytes.get());
    assertArrayEquals(val1, storage.getColumn(ROW, qual1));
    assertArrayEquals(val2, storage.getColumn(ROW, qual2));
  }
  
  @Test
  public void integerVle1ByteNegativeFix() throws Exception {
    when(options.fix()).thenReturn(true);

    final byte[] qual1 = { 0x00, 0x00 };
    final byte[] val1 = new byte[] { 1 };
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(-2L);
    final byte[] fixed_qual2 = { 0x00, 0x20 };
    final byte[] fixed_val2 = new byte[] { -2 };
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(0, fsck.totalErrors());
    assertEquals(0, fsck.totalFixed());
    assertEquals(0, fsck.correctable());
    assertEquals(1, fsck.vle.get());
    assertEquals(7, fsck.vle_bytes.get());
    assertEquals(1, fsck.vle_fixed.get());
    assertArrayEquals(val1, storage.getColumn(ROW, qual1));
    assertNull(storage.getColumn(ROW, qual2));
    assertArrayEquals(fixed_val2, storage.getColumn(ROW, fixed_qual2));
  }
  
  @Test
  public void integerVle2Bytes() throws Exception {
    final byte[] qual1 = { 0x00, 0x00 };
    final byte[] val1 = new byte[] { 1 };
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(257L);
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(0, fsck.totalErrors());
    assertEquals(0, fsck.totalFixed());
    assertEquals(0, fsck.correctable());
    assertEquals(1, fsck.vle.get());
    assertEquals(6, fsck.vle_bytes.get());
    assertArrayEquals(val1, storage.getColumn(ROW, qual1));
    assertArrayEquals(val2, storage.getColumn(ROW, qual2));
  }
  
  @Test
  public void integerVle2BytesFix() throws Exception {
    when(options.fix()).thenReturn(true);
    
    final byte[] qual1 = { 0x00, 0x00 };
    final byte[] val1 = new byte[] { 1 };
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(257L);
    final byte[] fixed_qual2 = { 0x00, 0x21 };
    final byte[] fixed_val2 = Bytes.fromShort((short)257);
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(0, fsck.totalErrors());
    assertEquals(0, fsck.totalFixed());
    assertEquals(0, fsck.correctable());
    assertEquals(1, fsck.vle.get());
    assertEquals(6, fsck.vle_bytes.get());
    assertEquals(1, fsck.vle_fixed.get());
    assertArrayEquals(val1, storage.getColumn(ROW, qual1));
    assertNull(storage.getColumn(ROW, qual2));
    assertArrayEquals(fixed_val2, storage.getColumn(ROW, fixed_qual2));
  }
  
  @Test
  public void integerVle2BytesNegative() throws Exception {
    final byte[] qual1 = { 0x00, 0x00 };
    final byte[] val1 = new byte[] { 1 };
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(-257L);
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(0, fsck.totalErrors());
    assertEquals(0, fsck.totalFixed());
    assertEquals(0, fsck.correctable());
    assertEquals(1, fsck.vle.get());
    assertEquals(6, fsck.vle_bytes.get());
    assertArrayEquals(val1, storage.getColumn(ROW, qual1));
    assertArrayEquals(val2, storage.getColumn(ROW, qual2));
  }
  
  @Test
  public void integerVle2BytesNegativeFix() throws Exception {
    when(options.fix()).thenReturn(true);
    
    final byte[] qual1 = { 0x00, 0x00 };
    final byte[] val1 = new byte[] { 1 };
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(-257L);
    final byte[] fixed_qual2 = { 0x00, 0x21 };
    final byte[] fixed_val2 = Bytes.fromShort((short)-257);
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(0, fsck.totalErrors());
    assertEquals(0, fsck.totalFixed());
    assertEquals(0, fsck.correctable());
    assertEquals(1, fsck.vle.get());
    assertEquals(6, fsck.vle_bytes.get());
    assertEquals(1, fsck.vle_fixed.get());
    assertArrayEquals(val1, storage.getColumn(ROW, qual1));
    assertNull(storage.getColumn(ROW, qual2));
    assertArrayEquals(fixed_val2, storage.getColumn(ROW, fixed_qual2));
  }
  
  @Test
  public void integerVle4Bytes() throws Exception {
    final byte[] qual1 = { 0x00, 0x00 };
    final byte[] val1 = new byte[] { 1 };
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(65537L);
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(0, fsck.totalErrors());
    assertEquals(0, fsck.totalFixed());
    assertEquals(0, fsck.correctable());
    assertEquals(1, fsck.vle.get());
    assertEquals(4, fsck.vle_bytes.get());
    assertArrayEquals(val1, storage.getColumn(ROW, qual1));
    assertArrayEquals(val2, storage.getColumn(ROW, qual2));
  }
  
  @Test
  public void integerVle4BytesFix() throws Exception {
    when(options.fix()).thenReturn(true);
    
    final byte[] qual1 = { 0x00, 0x00 };
    final byte[] val1 = new byte[] { 1 };
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(65537L);
    final byte[] fixed_qual2 = { 0x00, 0x23 };
    final byte[] fixed_val2 = Bytes.fromInt(65537);
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(0, fsck.totalErrors());
    assertEquals(0, fsck.totalFixed());
    assertEquals(0, fsck.correctable());
    assertEquals(1, fsck.vle.get());
    assertEquals(4, fsck.vle_bytes.get());
    assertEquals(1, fsck.vle_fixed.get());
    assertArrayEquals(val1, storage.getColumn(ROW, qual1));
    assertNull(storage.getColumn(ROW, qual2));
    assertArrayEquals(fixed_val2, storage.getColumn(ROW, fixed_qual2));
  }
  
  @Test
  public void integerVle4BytesNegative() throws Exception {
    final byte[] qual1 = { 0x00, 0x00 };
    final byte[] val1 = new byte[] { 1 };
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(-65537L);
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(0, fsck.totalErrors());
    assertEquals(0, fsck.totalFixed());
    assertEquals(0, fsck.correctable());
    assertEquals(1, fsck.vle.get());
    assertEquals(4, fsck.vle_bytes.get());
    assertArrayEquals(val1, storage.getColumn(ROW, qual1));
    assertArrayEquals(val2, storage.getColumn(ROW, qual2));
  }
  
  @Test
  public void integerVle4BytesNegativeFix() throws Exception {
    when(options.fix()).thenReturn(true);
    
    final byte[] qual1 = { 0x00, 0x00 };
    final byte[] val1 = new byte[] { 1 };
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(-65537L);
    final byte[] fixed_qual2 = { 0x00, 0x23 };
    final byte[] fixed_val2 = Bytes.fromInt(-65537);
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(0, fsck.totalErrors());
    assertEquals(0, fsck.totalFixed());
    assertEquals(0, fsck.correctable());
    assertEquals(1, fsck.vle.get());
    assertEquals(4, fsck.vle_bytes.get());
    assertEquals(1, fsck.vle_fixed.get());
    assertArrayEquals(val1, storage.getColumn(ROW, qual1));
    assertNull(storage.getColumn(ROW, qual2));
    assertArrayEquals(fixed_val2, storage.getColumn(ROW, fixed_qual2));
  }
  
  @Test
  public void compactedVLE() throws Exception {
    final byte[] qual1 = { 0x0, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x0, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual1, qual2), 
        MockBase.concatByteArrays(val1, val2, new byte[] { 0 }));

    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(1, fsck.kvs_processed.get());
    assertEquals(2, fsck.vle.get());
    assertEquals(0, fsck.totalErrors());
    assertEquals(0, fsck.correctable());
    assertArrayEquals(MockBase.concatByteArrays(val1, val2, new byte[] { 0 }), 
        storage.getColumn(ROW, MockBase.concatByteArrays(qual1, qual2)));
  }
  
  @Test
  public void compactedVLEFix() throws Exception {
    when(options.fix()).thenReturn(true);
    
    final byte[] qual1 = { 0x0, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x0, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual1, qual2), 
        MockBase.concatByteArrays(val1, val2, new byte[] { 0 }));
    final byte[] compacted_qual = { 0x0, 0x0, 0x0, 0x20 };
    final byte[] compacted_value = { 0x4, 0x5, 0x0 };
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(1, fsck.kvs_processed.get());
    assertEquals(2, fsck.vle.get());
    assertEquals(0, fsck.totalErrors());
    assertEquals(0, fsck.correctable());
    assertNull(storage.getColumn(ROW, MockBase.concatByteArrays(qual1, qual2)));
    assertArrayEquals(compacted_value, 
        storage.getColumn(ROW, compacted_qual));
  }
  
  @Test
  public void compactFixWithVLE() throws Exception {
    when(options.compact()).thenReturn(true);
    when(options.fix()).thenReturn(true);
    
    final byte[] qual1 = { 0x0, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x0, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual3 = { 0x0, 0x37 };
    final byte[] val3 = Bytes.fromLong(6L);
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    storage.addColumn(ROW, qual3, val3);
    final byte[] vle_qual = { 0x0, 0x0, 0x0, 0x20, 0x0, 0x30 };
    final byte[] vle_value = { 0x04, 0x05, 0x06, 0x0 };
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(3, fsck.kvs_processed.get());
    assertEquals(3, fsck.vle.get());
    assertEquals(0, fsck.totalErrors());
    assertEquals(0, fsck.correctable());
    assertArrayEquals(vle_value, storage.getColumn(ROW, vle_qual));
    assertNull(storage.getColumn(ROW, qual1));
    assertNull(storage.getColumn(ROW, qual2));
    assertNull(storage.getColumn(ROW, qual3));
  }
  
  // DUPLICATE DATA POINTS ---------------------------------
  
  @Test
  public void dupesSinglesSeconds() throws Exception {    
    final byte[] qual1 = { 0x00, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { 0x00, 0x0B };
    final byte[] val2 = Bytes.fromInt(Float.floatToRawIntBits(500.8F));
    final byte[] qual3 = { 0x00, 0x20 };
    final byte[] val3 = { 5 };
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    storage.addColumn(ROW, qual3, val3);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(3, fsck.kvs_processed.get());
    assertEquals(1, fsck.duplicates.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.correctable());
    assertArrayEquals(val1, storage.getColumn(ROW, qual1));
    assertArrayEquals(val2, storage.getColumn(ROW, qual2));
    assertArrayEquals(val3, storage.getColumn(ROW, qual3));
  }
  
  @Test
  public void dupesSinglesSecondsFix() throws Exception {
    when(options.fix()).thenReturn(true);
    when(options.resolveDupes()).thenReturn(true);
    
    final byte[] qual1 = { 0x00, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { 0x00, 0x0B };
    final byte[] val2 = Bytes.fromInt(Float.floatToRawIntBits(500.8F));
    final byte[] qual3 = { 0x00, 0x20 };
    final byte[] val3 = { 5 };
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    storage.addColumn(ROW, qual3, val3);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(3, fsck.kvs_processed.get());
    assertEquals(1, fsck.duplicates.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.correctable());
    assertEquals(1, fsck.totalFixed());
    assertArrayEquals(val1, storage.getColumn(ROW, qual1));
    assertNull(storage.getColumn(ROW, qual2));
    assertArrayEquals(val3, storage.getColumn(ROW, qual3));
  }
  
  @Test
  public void dupesSinglesSecondsLWW() throws Exception {
    when(options.lastWriteWins()).thenReturn(true);
    
    final byte[] qual1 = { 0x00, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { 0x00, 0x0B };
    final byte[] val2 = Bytes.fromInt(Float.floatToRawIntBits(500.8F));
    final byte[] qual3 = { 0x00, 0x20 };
    final byte[] val3 = { 5 };
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    storage.addColumn(ROW, qual3, val3);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(3, fsck.kvs_processed.get());
    assertEquals(1, fsck.duplicates.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.correctable());
    assertEquals(0, fsck.totalFixed());
    assertArrayEquals(val1, storage.getColumn(ROW, qual1));
    assertArrayEquals(val2, storage.getColumn(ROW, qual2));
    assertArrayEquals(val3, storage.getColumn(ROW, qual3));
  }
  
  @Test
  public void dupesSinglesSecondsLWWFix() throws Exception {
    when(options.lastWriteWins()).thenReturn(true);
    when(options.fix()).thenReturn(true);
    when(options.resolveDupes()).thenReturn(true);
    
    final byte[] qual1 = { 0x00, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { 0x00, 0x0B };
    final byte[] val2 = Bytes.fromInt(Float.floatToRawIntBits(500.8F));
    final byte[] qual3 = { 0x00, 0x20 };
    final byte[] val3 = { 5 };
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    storage.addColumn(ROW, qual3, val3);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(3, fsck.kvs_processed.get());
    assertEquals(1, fsck.duplicates.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.correctable());
    assertEquals(1, fsck.totalFixed());
    assertNull(storage.getColumn(ROW, qual1));
    assertArrayEquals(val2, storage.getColumn(ROW, qual2));
    assertArrayEquals(val3, storage.getColumn(ROW, qual3));
  }
  
  @Test
  public void dupeTimestampsMultipleSinglesSeconds() throws Exception {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x0B };
    final byte[] val2 = Bytes.fromInt(Float.floatToRawIntBits(500.8F));
    final byte[] qual3 = { 0x00, 0x03 };
    final byte[] val3 = Bytes.fromInt(42);
    final byte[] qual4 = { 0x00, 0x01 };
    final byte[] val4 = Bytes.fromShort((short)24);
    final byte[] qual5 = { 0x00, 0x21 };
    final byte[] val5 = Bytes.fromShort((short)24);
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    storage.addColumn(ROW, qual3, val3);
    storage.addColumn(ROW, qual4, val4);
    storage.addColumn(ROW, qual5, val5);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(5, fsck.kvs_processed.get());
    assertEquals(3, fsck.duplicates.get());
    assertEquals(3, fsck.totalErrors());
    assertEquals(3, fsck.correctable());
    assertArrayEquals(val1, storage.getColumn(ROW, qual1));
    assertArrayEquals(val2, storage.getColumn(ROW, qual2));
    assertArrayEquals(val3, storage.getColumn(ROW, qual3));
    assertArrayEquals(val4, storage.getColumn(ROW, qual4));
    assertArrayEquals(val5, storage.getColumn(ROW, qual5));
  }
  
  @Test
  public void dupeTimestampsMultipleSinglesSecondsFix() throws Exception {
    when(options.fix()).thenReturn(true);
    when(options.resolveDupes()).thenReturn(true);
    
    final byte[] qual1 = { 0x00, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { 0x00, 0x0B };
    final byte[] val2 = Bytes.fromInt(Float.floatToRawIntBits(500.8F));
    final byte[] qual3 = { 0x00, 0x03 };
    final byte[] val3 = Bytes.fromInt(42);
    final byte[] qual4 = { 0x00, 0x01 };
    final byte[] val4 = Bytes.fromShort((short)24);
    final byte[] qual5 = { 0x00, 0x21 };
    final byte[] val5 = Bytes.fromShort((short)24);
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    storage.addColumn(ROW, qual3, val3);
    storage.addColumn(ROW, qual4, val4);
    storage.addColumn(ROW, qual5, val5);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(5, fsck.kvs_processed.get());
    assertEquals(3, fsck.duplicates.get());
    assertEquals(3, fsck.totalErrors());
    assertEquals(3, fsck.correctable());
    assertArrayEquals(val1, storage.getColumn(ROW, qual1));
    assertNull(storage.getColumn(ROW, qual2));
    assertNull(storage.getColumn(ROW, qual3));
    assertNull(storage.getColumn(ROW, qual4));
    assertArrayEquals(val5, storage.getColumn(ROW, qual5));
  }
  
  @Test
  public void dupeTimestampsMultipleSinglesSecondsLWW() throws Exception {
    when(options.lastWriteWins()).thenReturn(true);
    
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x0B };
    final byte[] val2 = Bytes.fromInt(Float.floatToRawIntBits(500.8F));
    final byte[] qual3 = { 0x00, 0x03 };
    final byte[] val3 = Bytes.fromInt(42);
    final byte[] qual4 = { 0x00, 0x01 };
    final byte[] val4 = Bytes.fromShort((short)24);
    final byte[] qual5 = { 0x00, 0x21 };
    final byte[] val5 = Bytes.fromShort((short)24);
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    storage.addColumn(ROW, qual3, val3);
    storage.addColumn(ROW, qual4, val4);
    storage.addColumn(ROW, qual5, val5);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(5, fsck.kvs_processed.get());
    assertEquals(3, fsck.duplicates.get());
    assertEquals(3, fsck.totalErrors());
    assertEquals(3, fsck.correctable());
    assertArrayEquals(val1, storage.getColumn(ROW, qual1));
    assertArrayEquals(val2, storage.getColumn(ROW, qual2));
    assertArrayEquals(val3, storage.getColumn(ROW, qual3));
    assertArrayEquals(val4, storage.getColumn(ROW, qual4));
    assertArrayEquals(val5, storage.getColumn(ROW, qual5));
  }
  
  @Test
  public void dupeTimestampsMultipleSinglesSecondsLWWFix() throws Exception {
    when(options.lastWriteWins()).thenReturn(true);
    when(options.fix()).thenReturn(true);
    when(options.resolveDupes()).thenReturn(true);
    
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x0B };
    final byte[] val2 = Bytes.fromInt(Float.floatToRawIntBits(500.8F));
    final byte[] qual3 = { 0x00, 0x03 };
    final byte[] val3 = Bytes.fromInt(42);
    final byte[] qual4 = { 0x00, 0x01 };
    final byte[] val4 = Bytes.fromShort((short)24);
    final byte[] qual5 = { 0x00, 0x21 };
    final byte[] val5 = Bytes.fromShort((short)24);
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    storage.addColumn(ROW, qual3, val3);
    storage.addColumn(ROW, qual4, val4);
    storage.addColumn(ROW, qual5, val5);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(5, fsck.kvs_processed.get());
    assertEquals(3, fsck.duplicates.get());
    assertEquals(3, fsck.totalErrors());
    assertEquals(3, fsck.correctable());
    assertNull(storage.getColumn(ROW, qual1));
    assertNull(storage.getColumn(ROW, qual2));
    assertNull(storage.getColumn(ROW, qual3));
    assertArrayEquals(val4, storage.getColumn(ROW, qual4));
    assertArrayEquals(val5, storage.getColumn(ROW, qual5));
  }
  
  @Test
  public void dupeSinglesTimestampsMs() throws Exception {
    final byte[] qual1 = { (byte) 0xF0, 0x00, 0x00, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x02, 0x00 };
    final byte[] val2 = { 5 };
    final byte[] qual3 = { (byte) 0xF0, 0x00, 0x02, 0x0B };
    final byte[] val3 = Bytes.fromInt(Float.floatToRawIntBits(500.8F));
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    storage.addColumn(ROW, qual3, val3);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(3, fsck.kvs_processed.get());
    assertEquals(1, fsck.duplicates.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.correctable());
    assertArrayEquals(val1, storage.getColumn(ROW, qual1));
    assertArrayEquals(val2, storage.getColumn(ROW, qual2));
    assertArrayEquals(val3, storage.getColumn(ROW, qual3));
  }

  @Test
  public void dupeSinglesTimestampsMsFix() throws Exception {
    when(options.fix()).thenReturn(true);
    when(options.resolveDupes()).thenReturn(true);
    
    final byte[] qual1 = { (byte) 0xF0, 0x00, 0x00, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x02, 0x00 };
    final byte[] val2 = { 5 };
    final byte[] qual3 = { (byte) 0xF0, 0x00, 0x02, 0x0B };
    final byte[] val3 = Bytes.fromInt(Float.floatToRawIntBits(500.8F));
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    storage.addColumn(ROW, qual3, val3);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(3, fsck.kvs_processed.get());
    assertEquals(1, fsck.duplicates.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.correctable());
    assertArrayEquals(val1, storage.getColumn(ROW, qual1));
    assertArrayEquals(val2, storage.getColumn(ROW, qual2));
    assertNull(storage.getColumn(ROW, qual3));
  }
  
  @Test
  public void dupeSinglesTimestampsMsLWW() throws Exception {
    when(options.lastWriteWins()).thenReturn(true);
    
    final byte[] qual1 = { (byte) 0xF0, 0x00, 0x00, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x02, 0x00 };
    final byte[] val2 = { 5 };
    final byte[] qual3 = { (byte) 0xF0, 0x00, 0x02, 0x0B };
    final byte[] val3 = Bytes.fromInt(Float.floatToRawIntBits(500.8F));
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    storage.addColumn(ROW, qual3, val3);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(3, fsck.kvs_processed.get());
    assertEquals(1, fsck.duplicates.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.correctable());
    assertArrayEquals(val1, storage.getColumn(ROW, qual1));
    assertArrayEquals(val2, storage.getColumn(ROW, qual2));
    assertArrayEquals(val3, storage.getColumn(ROW, qual3));
  }
  
  @Test
  public void dupeSinglesTimestampsMsFixLWW() throws Exception {
    when(options.fix()).thenReturn(true);
    when(options.resolveDupes()).thenReturn(true);
    when(options.lastWriteWins()).thenReturn(true);
    
    final byte[] qual1 = { (byte) 0xF0, 0x00, 0x00, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x02, 0x00 };
    final byte[] val2 = { 5 };
    final byte[] qual3 = { (byte) 0xF0, 0x00, 0x02, 0x0B };
    final byte[] val3 = Bytes.fromInt(Float.floatToRawIntBits(500.8F));
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    storage.addColumn(ROW, qual3, val3);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(3, fsck.kvs_processed.get());
    assertEquals(1, fsck.duplicates.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.correctable());
    assertArrayEquals(val1, storage.getColumn(ROW, qual1));
    assertNull(storage.getColumn(ROW, qual2));
    assertArrayEquals(val3, storage.getColumn(ROW, qual3));
  }
  
  @Test
  public void dupeTimestampsMultipleSinglesMs() throws Exception {
    final byte[] qual1 = { (byte) 0xF0, 0x00, 0x00, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x02, 0x00 };
    final byte[] val2 = { 5 };
    final byte[] qual3 = { (byte) 0xF0, 0x00, 0x02, 0x03 };
    final byte[] val3 = Bytes.fromInt(6);
    final byte[] qual4 = { (byte) 0xF0, 0x00, 0x02, 0x01 };
    final byte[] val4 = Bytes.fromShort((short)7);
    final byte[] qual5 = { (byte) 0xF0, 0x00, 0x03, 0x00 };
    final byte[] val5 = { 8 };
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    storage.addColumn(ROW, qual3, val3);
    storage.addColumn(ROW, qual4, val4);
    storage.addColumn(ROW, qual5, val5);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(5, fsck.kvs_processed.get());
    assertEquals(2, fsck.duplicates.get());
    assertEquals(2, fsck.totalErrors());
    assertEquals(2, fsck.correctable());
    assertArrayEquals(val1, storage.getColumn(ROW, qual1));
    assertArrayEquals(val2, storage.getColumn(ROW, qual2));
    assertArrayEquals(val3, storage.getColumn(ROW, qual3));
    assertArrayEquals(val4, storage.getColumn(ROW, qual4));
    assertArrayEquals(val5, storage.getColumn(ROW, qual5));
  }
  
  @Test
  public void dupeTimestampsMultipleSinglesMsFix() throws Exception {
    when(options.fix()).thenReturn(true);
    when(options.resolveDupes()).thenReturn(true);
    
    final byte[] qual1 = { (byte) 0xF0, 0x00, 0x00, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x02, 0x00 };
    final byte[] val2 = { 5 };
    final byte[] qual3 = { (byte) 0xF0, 0x00, 0x02, 0x03 };
    final byte[] val3 = Bytes.fromInt(6);
    final byte[] qual4 = { (byte) 0xF0, 0x00, 0x02, 0x01 };
    final byte[] val4 = Bytes.fromShort((short)7);
    final byte[] qual5 = { (byte) 0xF0, 0x00, 0x03, 0x00 };
    final byte[] val5 = { 8 };
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    storage.addColumn(ROW, qual3, val3);
    storage.addColumn(ROW, qual4, val4);
    storage.addColumn(ROW, qual5, val5);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(5, fsck.kvs_processed.get());
    assertEquals(2, fsck.duplicates.get());
    assertEquals(2, fsck.totalErrors());
    assertEquals(2, fsck.correctable());
    assertArrayEquals(val1, storage.getColumn(ROW, qual1));
    assertArrayEquals(val2, storage.getColumn(ROW, qual2));
    assertNull(storage.getColumn(ROW, qual3));
    assertNull(storage.getColumn(ROW, qual4));
    assertArrayEquals(val5, storage.getColumn(ROW, qual5));
  }
  
  @Test
  public void dupeTimestampsMultipleSinglesMsLWW() throws Exception {
    when(options.lastWriteWins()).thenReturn(true);
    
    final byte[] qual1 = { (byte) 0xF0, 0x00, 0x00, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x02, 0x00 };
    final byte[] val2 = { 5 };
    final byte[] qual3 = { (byte) 0xF0, 0x00, 0x02, 0x03 };
    final byte[] val3 = Bytes.fromInt(6);
    final byte[] qual4 = { (byte) 0xF0, 0x00, 0x02, 0x01 };
    final byte[] val4 = Bytes.fromShort((short)7);
    final byte[] qual5 = { (byte) 0xF0, 0x00, 0x03, 0x00 };
    final byte[] val5 = { 8 };
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    storage.addColumn(ROW, qual3, val3);
    storage.addColumn(ROW, qual4, val4);
    storage.addColumn(ROW, qual5, val5);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(5, fsck.kvs_processed.get());
    assertEquals(2, fsck.duplicates.get());
    assertEquals(2, fsck.totalErrors());
    assertEquals(2, fsck.correctable());
    assertArrayEquals(val1, storage.getColumn(ROW, qual1));
    assertArrayEquals(val2, storage.getColumn(ROW, qual2));
    assertArrayEquals(val3, storage.getColumn(ROW, qual3));
    assertArrayEquals(val4, storage.getColumn(ROW, qual4));
    assertArrayEquals(val5, storage.getColumn(ROW, qual5));
  }
  
  @Test
  public void dupeTimestampsMultipleSinglesMsLWWFix() throws Exception {
    when(options.fix()).thenReturn(true);
    when(options.resolveDupes()).thenReturn(true);
    when(options.lastWriteWins()).thenReturn(true);
    
    final byte[] qual1 = { (byte) 0xF0, 0x00, 0x00, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x02, 0x00 };
    final byte[] val2 = { 5 };
    final byte[] qual3 = { (byte) 0xF0, 0x00, 0x02, 0x03 };
    final byte[] val3 = Bytes.fromInt(6);
    final byte[] qual4 = { (byte) 0xF0, 0x00, 0x02, 0x01 };
    final byte[] val4 = Bytes.fromShort((short)7);
    final byte[] qual5 = { (byte) 0xF0, 0x00, 0x03, 0x00 };
    final byte[] val5 = { 8 };
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    storage.addColumn(ROW, qual3, val3);
    storage.addColumn(ROW, qual4, val4);
    storage.addColumn(ROW, qual5, val5);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(5, fsck.kvs_processed.get());
    assertEquals(2, fsck.duplicates.get());
    assertEquals(2, fsck.totalErrors());
    assertEquals(2, fsck.correctable());
    assertArrayEquals(val1, storage.getColumn(ROW, qual1));
    assertNull(storage.getColumn(ROW, qual2));
    assertNull(storage.getColumn(ROW, qual3));
    assertArrayEquals(val4, storage.getColumn(ROW, qual4));
    assertArrayEquals(val5, storage.getColumn(ROW, qual5));
  }
  
  @Test
  public void dupesSinglesMixed() throws Exception {    
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { (byte) 0xF0, 0x01, (byte) 0xF4, 0x07 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual3 = { 0x00, 0x27 };
    final byte[] val3 = Bytes.fromLong(6L);
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    storage.addColumn(ROW, qual3, val3);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(3, fsck.kvs_processed.get());
    assertEquals(1, fsck.duplicates.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.correctable());
    assertArrayEquals(val1, storage.getColumn(ROW, qual1));
    assertArrayEquals(val2, storage.getColumn(ROW, qual2));
    assertArrayEquals(val3, storage.getColumn(ROW, qual3));
  }
  
  @Test
  public void dupesSinglesMixedFix() throws Exception {
    when(options.fix()).thenReturn(true);
    when(options.resolveDupes()).thenReturn(true);
    
    final byte[] qual1 = { 0x00, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { (byte) 0xF0, 0x01, (byte) 0xF4, 0x00 };
    final byte[] val2 = { 5 };
    final byte[] qual3 = { 0x00, 0x23 };
    final byte[] val3 = Bytes.fromInt(6);
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    storage.addColumn(ROW, qual3, val3);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(3, fsck.kvs_processed.get());
    assertEquals(1, fsck.duplicates.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.correctable());
    assertArrayEquals(val1, storage.getColumn(ROW, qual1));
    assertArrayEquals(val2, storage.getColumn(ROW, qual2));
    assertNull(storage.getColumn(ROW, qual3));
  }
  
  @Test
  public void dupesSinglesMixedLWW() throws Exception {
    when(options.lastWriteWins()).thenReturn(true);
    
    final byte[] qual1 = { 0x00, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { (byte) 0xF0, 0x01, (byte) 0xF4, 0x00 };
    final byte[] val2 = { 5 };
    final byte[] qual3 = { 0x00, 0x23 };
    final byte[] val3 = Bytes.fromInt(6);
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    storage.addColumn(ROW, qual3, val3);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(3, fsck.kvs_processed.get());
    assertEquals(1, fsck.duplicates.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.correctable());
    assertArrayEquals(val1, storage.getColumn(ROW, qual1));
    assertArrayEquals(val2, storage.getColumn(ROW, qual2));
    assertArrayEquals(val3, storage.getColumn(ROW, qual3));
  }
  
  @Test
  public void dupesSinglesMixedLWWFix() throws Exception {
    when(options.fix()).thenReturn(true);
    when(options.resolveDupes()).thenReturn(true);
    when(options.lastWriteWins()).thenReturn(true);
    
    final byte[] qual1 = { 0x00, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { (byte) 0xF0, 0x01, (byte) 0xF4, 0x00 };
    final byte[] val2 = { 5 };
    final byte[] qual3 = { 0x00, 0x23 };
    final byte[] val3 = Bytes.fromInt(6);
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    storage.addColumn(ROW, qual3, val3);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(3, fsck.kvs_processed.get());
    assertEquals(1, fsck.duplicates.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.correctable());
    assertArrayEquals(val1, storage.getColumn(ROW, qual1));
    assertNull(storage.getColumn(ROW, qual2));
    assertArrayEquals(val3, storage.getColumn(ROW, qual3));
  }
  
  // DUPLICATE COMPACTED DATA POINTS ----------------------------
  
  @Test
  public void twoCompactedColumnsWSameTS() throws Exception {
    when(options.resolveDupes()).thenReturn(true);
    final byte[] qual1 = { 0x0, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { 0x0, 0x20 };
    final byte[] val2 = { 5 };
    final byte[] qual3 = { 0x0, 0x21 };
    final byte[] val3 = Bytes.fromShort((short)7);
    final byte[] qual4 = { 0x0, 0x30 };
    final byte[] val4 = { 7 };
    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual1, qual2), 
        MockBase.concatByteArrays(val1, val2, new byte[] { 0 }));
    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual3, qual4), 
        MockBase.concatByteArrays(val3, val4, new byte[] { 0 }));

    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(1, fsck.duplicates.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.correctable());
    assertArrayEquals(MockBase.concatByteArrays(val1, val2, new byte[] { 0 }), 
        storage.getColumn(ROW, MockBase.concatByteArrays(qual1, qual2)));
    assertArrayEquals(MockBase.concatByteArrays(val3, val4, new byte[] { 0 }), 
        storage.getColumn(ROW, MockBase.concatByteArrays(qual3, qual4)));
  }
  
  @Test
  public void twoCompactedColumnsWSameTSFix() throws Exception {
    when(options.fix()).thenReturn(true);
    when(options.resolveDupes()).thenReturn(true);
    final byte[] qual1 = { 0x0, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { 0x0, 0x20 };
    final byte[] val2 = { 5 };
    final byte[] qual3 = { 0x0, 0x21 };
    final byte[] val3 = Bytes.fromShort((short)7);
    final byte[] qual4 = { 0x0, 0x30 };
    final byte[] val4 = { 7 };
    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual1, qual2), 
        MockBase.concatByteArrays(val1, val2, new byte[] { 0 }));
    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual3, qual4), 
        MockBase.concatByteArrays(val3, val4, new byte[] { 0 }));

    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(1, fsck.duplicates.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.correctable());
    assertNull(storage.getColumn(ROW, MockBase.concatByteArrays(qual1, qual2)));
    assertNull(storage.getColumn(ROW, MockBase.concatByteArrays(qual3, qual4)));
    assertArrayEquals(MockBase.concatByteArrays(val1, val2, val4, 
        new byte[] { 0 }), storage.getColumn(ROW, 
            MockBase.concatByteArrays(qual1, qual2, qual4)));
  }
  
  @Test
  public void twoCompactedColumnsOneWExtraDP() throws Exception {
    when(options.resolveDupes()).thenReturn(true);
    final byte[] qual1 = { 0x0, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { 0x0, 0x20 };
    final byte[] val2 = { 5 };
    final byte[] qual3 = { 0x0, 0x30 };
    final byte[] val3 = { 7 };
    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual1, qual2), 
        MockBase.concatByteArrays(val1, val2, new byte[] { 0 }));
    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual1, qual2, qual3), 
        MockBase.concatByteArrays(val1, val2, val3, new byte[] { 0 }));

    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(2, fsck.duplicates.get());
    assertEquals(2, fsck.totalErrors());
    assertEquals(2, fsck.correctable());
    assertArrayEquals(MockBase.concatByteArrays(val1, val2, 
        new byte[] { 0 }), storage.getColumn(ROW, 
            MockBase.concatByteArrays(qual1, qual2)));
    assertArrayEquals(MockBase.concatByteArrays(val1, val2, val3, 
        new byte[] { 0 }), storage.getColumn(ROW, 
            MockBase.concatByteArrays(qual1, qual2, qual3)));
  }
  
  @Test
  public void twoCompactedColumnsOneWExtraDPFix() throws Exception {
    when(options.fix()).thenReturn(true);
    when(options.resolveDupes()).thenReturn(true);
    final byte[] qual1 = { 0x0, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { 0x0, 0x20 };
    final byte[] val2 = { 5 };
    final byte[] qual3 = { 0x0, 0x30 };
    final byte[] val3 = { 7 };
    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual1, qual2), 
        MockBase.concatByteArrays(val1, val2, new byte[] { 0 }));
    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual1, qual2, qual3), 
        MockBase.concatByteArrays(val1, val2, val3, new byte[] { 0 }));

    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(2, fsck.duplicates.get());
    assertEquals(2, fsck.totalErrors());
    assertEquals(2, fsck.correctable());
    assertNull(storage.getColumn(ROW, MockBase.concatByteArrays(qual1, qual2)));
    assertArrayEquals(MockBase.concatByteArrays(val1, val2, val3, 
        new byte[] { 0 }), storage.getColumn(ROW, 
            MockBase.concatByteArrays(qual1, qual2, qual3)));
  }
  
  @Test
  public void twoCompactedColumnsWSameTSLWW() throws Exception {
    when(options.lastWriteWins()).thenReturn(true);
    when(options.resolveDupes()).thenReturn(true);
    final byte[] qual1 = { 0x0, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { 0x0, 0x20 };
    final byte[] val2 = { 5 };
    final byte[] qual3 = { 0x0, 0x21 };
    final byte[] val3 = Bytes.fromShort((short)7);
    final byte[] qual4 = { 0x0, 0x30 };
    final byte[] val4 = { 7 };
    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual1, qual2), 
        MockBase.concatByteArrays(val1, val2, new byte[] { 0 }));
    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual3, qual4), 
        MockBase.concatByteArrays(val3, val4, new byte[] { 0 }));

    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(1, fsck.duplicates.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.correctable());
    assertArrayEquals(MockBase.concatByteArrays(val1, val2, new byte[] { 0 }), 
        storage.getColumn(ROW, MockBase.concatByteArrays(qual1, qual2)));
    assertArrayEquals(MockBase.concatByteArrays(val3, val4, new byte[] { 0 }), 
        storage.getColumn(ROW, MockBase.concatByteArrays(qual3, qual4)));
  }
  
  @Test
  public void twoCompactedColumnsWSameTSFixLLW() throws Exception {
    when(options.lastWriteWins()).thenReturn(true);
    when(options.fix()).thenReturn(true);
    when(options.resolveDupes()).thenReturn(true);
    
    final byte[] qual1 = { 0x0, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { 0x0, 0x20 };
    final byte[] val2 = { 5 };
    final byte[] qual3 = { 0x0, 0x21 };
    final byte[] val3 = Bytes.fromShort((short)7);
    final byte[] qual4 = { 0x0, 0x30 };
    final byte[] val4 = { 7 };
    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual1, qual2), 
        MockBase.concatByteArrays(val1, val2, new byte[] { 0 }));
    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual3, qual4), 
        MockBase.concatByteArrays(val3, val4, new byte[] { 0 }));

    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(1, fsck.duplicates.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.correctable());
    assertNull(storage.getColumn(ROW, MockBase.concatByteArrays(qual1, qual2)));
    assertNull(storage.getColumn(ROW, MockBase.concatByteArrays(qual3, qual4)));
    assertArrayEquals(MockBase.concatByteArrays(val1, val3, val4, 
        new byte[] { 0 }), storage.getColumn(ROW, 
            MockBase.concatByteArrays(qual1, qual3, qual4)));
  }

  @Test
  public void twoCompactedColumnsMSWSameTS() throws Exception {
    final byte[] qual1 = { (byte) 0xF0, 0x00, 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x02, 0x03 };
    final byte[] val2 = Bytes.fromInt(5);
    final byte[] qual3 = { (byte) 0xF0, 0x00, 0x02, 0x07 };
    final byte[] val3 = Bytes.fromLong(6L);
    final byte[] qual4 = { (byte) 0xF0, 0x00, 0x04, 0x07 };
    final byte[] val4 = Bytes.fromLong(7L);
    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual1, qual2), 
        MockBase.concatByteArrays(val1, val2, new byte[] { 0 }));
    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual3, qual4), 
        MockBase.concatByteArrays(val3, val4, new byte[] { 0 }));
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(1, fsck.duplicates.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.correctable());
    assertArrayEquals(MockBase.concatByteArrays(val1, val2, new byte[] { 0 }), 
        storage.getColumn(ROW, MockBase.concatByteArrays(qual1, qual2)));
    assertArrayEquals(MockBase.concatByteArrays(val3, val4, new byte[] { 0 }), 
        storage.getColumn(ROW, MockBase.concatByteArrays(qual3, qual4)));
  }
  
  @Test
  public void twoCompactedColumnsMSWSameTSFix() throws Exception {
    when(options.fix()).thenReturn(true);
    when(options.resolveDupes()).thenReturn(true);
    
    final byte[] qual1 = { (byte) 0xF0, 0x00, 0x00, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x02, 0x03 };
    final byte[] val2 = Bytes.fromInt(5);
    final byte[] qual3 = { (byte) 0xF0, 0x00, 0x02, 0x00 };
    final byte[] val3 = { 6 };
    final byte[] qual4 = { (byte) 0xF0, 0x00, 0x04, 0x00 };
    final byte[] val4 = { 7 };
    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual1, qual2), 
        MockBase.concatByteArrays(val1, val2, new byte[] { 0 }));
    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual3, qual4), 
        MockBase.concatByteArrays(val3, val4, new byte[] { 0 }));
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(1, fsck.duplicates.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.correctable());
    assertNull(storage.getColumn(ROW, MockBase.concatByteArrays(qual1, qual2)));
    assertNull(storage.getColumn(ROW, MockBase.concatByteArrays(qual3, qual4)));
    assertArrayEquals(MockBase.concatByteArrays(val1, val2, val4, new byte[] { 0 }), 
        storage.getColumn(ROW, MockBase.concatByteArrays(qual1, qual2, qual4)));
  }
  
  @Test
  public void twoCompactedColumnsMSWSameTSLLW() throws Exception {
    when(options.lastWriteWins()).thenReturn(true);
    
    final byte[] qual1 = { (byte) 0xF0, 0x00, 0x00, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x02, 0x03 };
    final byte[] val2 = Bytes.fromInt(5);
    final byte[] qual3 = { (byte) 0xF0, 0x00, 0x02, 0x00 };
    final byte[] val3 = { 6 };
    final byte[] qual4 = { (byte) 0xF0, 0x00, 0x04, 0x00 };
    final byte[] val4 = { 7 };
    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual1, qual2), 
        MockBase.concatByteArrays(val1, val2, new byte[] { 0 }));
    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual3, qual4), 
        MockBase.concatByteArrays(val3, val4, new byte[] { 0 }));
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(1, fsck.duplicates.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.correctable());
    assertArrayEquals(MockBase.concatByteArrays(val1, val2, new byte[] { 0 }), 
        storage.getColumn(ROW, MockBase.concatByteArrays(qual1, qual2)));
    assertArrayEquals(MockBase.concatByteArrays(val3, val4, new byte[] { 0 }), 
        storage.getColumn(ROW, MockBase.concatByteArrays(qual3, qual4)));
  }
  
  @Test
  public void twoCompactedColumnsMSWSameTSLWWFix() throws Exception {
    when(options.lastWriteWins()).thenReturn(true);
    when(options.fix()).thenReturn(true);
    when(options.resolveDupes()).thenReturn(true);
    
    final byte[] qual1 = { (byte) 0xF0, 0x00, 0x00, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x02, 0x03 };
    final byte[] val2 = Bytes.fromInt(5);
    final byte[] qual3 = { (byte) 0xF0, 0x00, 0x02, 0x00 };
    final byte[] val3 = { 6 };
    final byte[] qual4 = { (byte) 0xF0, 0x00, 0x04, 0x00 };
    final byte[] val4 = { 7 };
    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual1, qual2), 
        MockBase.concatByteArrays(val1, val2, new byte[] { 0 }));
    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual3, qual4), 
        MockBase.concatByteArrays(val3, val4, new byte[] { 0 }));
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(1, fsck.duplicates.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.correctable());
    assertNull(storage.getColumn(ROW, MockBase.concatByteArrays(qual1, qual2)));
    assertNull(storage.getColumn(ROW, MockBase.concatByteArrays(qual3, qual4)));
    assertArrayEquals(MockBase.concatByteArrays(val1, val3, val4, new byte[] { 0 }), 
        storage.getColumn(ROW, MockBase.concatByteArrays(qual1, qual3, qual4)));
  }
  
  @Test
  public void twoCompactedPlusSingleWSameTS() throws Exception {
    when(options.resolveDupes()).thenReturn(true);
    final byte[] qual1 = { 0x0, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { 0x0, 0x20 };
    final byte[] val2 = { 5 };
    final byte[] qual3 = { 0x0, 0x21 };
    final byte[] val3 = Bytes.fromShort((short)6);
    final byte[] qual4 = { 0x0, 0x30 };
    final byte[] val4 = { 7 };
    final byte[] qual5 = { 0x0, 0x23 };
    final byte[] val5 = Bytes.fromInt(8);
    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual1, qual2), 
        MockBase.concatByteArrays(val1, val2, new byte[] { 0 }));
    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual3, qual4), 
        MockBase.concatByteArrays(val3, val4, new byte[] { 0 }));
    storage.addColumn(ROW, qual5, val5);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(3, fsck.kvs_processed.get());
    assertEquals(2, fsck.duplicates.get());
    assertEquals(2, fsck.totalErrors());
    assertEquals(2, fsck.correctable());
    assertArrayEquals(MockBase.concatByteArrays(val1, val2, new byte[] { 0 }), 
        storage.getColumn(ROW, MockBase.concatByteArrays(qual1, qual2)));
    assertArrayEquals(MockBase.concatByteArrays(val3, val4, new byte[] { 0 }), 
        storage.getColumn(ROW, MockBase.concatByteArrays(qual3, qual4)));
    assertArrayEquals(val5, storage.getColumn(ROW, qual5));
  }
  
  @Test
  public void twoCompactedPlusSingleWSameTSFix() throws Exception {
    when(options.resolveDupes()).thenReturn(true);
    when(options.fix()).thenReturn(true);
    
    final byte[] qual1 = { 0x0, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { 0x0, 0x20 };
    final byte[] val2 = { 5 };
    final byte[] qual3 = { 0x0, 0x21 };
    final byte[] val3 = Bytes.fromShort((short)6);
    final byte[] qual4 = { 0x0, 0x30 };
    final byte[] val4 = { 7 };
    final byte[] qual5 = { 0x0, 0x23 };
    final byte[] val5 = Bytes.fromInt(8);
    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual1, qual2), 
        MockBase.concatByteArrays(val1, val2, new byte[] { 0 }));
    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual3, qual4), 
        MockBase.concatByteArrays(val3, val4, new byte[] { 0 }));
    storage.addColumn(ROW, qual5, val5);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(3, fsck.kvs_processed.get());
    assertEquals(2, fsck.duplicates.get());
    assertEquals(2, fsck.totalErrors());
    assertEquals(2, fsck.correctable());
    assertArrayEquals(MockBase.concatByteArrays(val1, val2, val4, 
        new byte[] { 0 }), storage.getColumn(ROW, 
            MockBase.concatByteArrays(qual1, qual2, qual4)));
    assertNull(storage.getColumn(ROW, MockBase.concatByteArrays(qual1, qual2)));
    assertNull(storage.getColumn(ROW, MockBase.concatByteArrays(qual3, qual4)));
    assertNull(storage.getColumn(ROW, qual5));
  }
  
  @Test
  public void twoCompactedPlusSingleWSameTSLWW() throws Exception {
    when(options.resolveDupes()).thenReturn(true);
    when(options.lastWriteWins()).thenReturn(true);
    
    final byte[] qual1 = { 0x0, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { 0x0, 0x20 };
    final byte[] val2 = { 5 };
    final byte[] qual3 = { 0x0, 0x21 };
    final byte[] val3 = Bytes.fromShort((short)6);
    final byte[] qual4 = { 0x0, 0x30 };
    final byte[] val4 = { 7 };
    final byte[] qual5 = { 0x0, 0x23 };
    final byte[] val5 = Bytes.fromInt(8);
    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual1, qual2), 
        MockBase.concatByteArrays(val1, val2, new byte[] { 0 }));
    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual3, qual4), 
        MockBase.concatByteArrays(val3, val4, new byte[] { 0 }));
    storage.addColumn(ROW, qual5, val5);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(3, fsck.kvs_processed.get());
    assertEquals(2, fsck.duplicates.get());
    assertEquals(2, fsck.totalErrors());
    assertEquals(2, fsck.correctable());
    assertArrayEquals(MockBase.concatByteArrays(val1, val2, new byte[] { 0 }), 
        storage.getColumn(ROW, MockBase.concatByteArrays(qual1, qual2)));
    assertArrayEquals(MockBase.concatByteArrays(val3, val4, new byte[] { 0 }), 
        storage.getColumn(ROW, MockBase.concatByteArrays(qual3, qual4)));
    assertArrayEquals(val5, storage.getColumn(ROW, qual5));
  }
  
  @Test
  public void twoCompactedPlusSingleWSameTSLWWFix() throws Exception {
    when(options.resolveDupes()).thenReturn(true);
    when(options.fix()).thenReturn(true);
    when(options.lastWriteWins()).thenReturn(true);
    
    final byte[] qual1 = { 0x0, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { 0x0, 0x20 };
    final byte[] val2 = { 5 };
    final byte[] qual3 = { 0x0, 0x21 };
    final byte[] val3 = Bytes.fromShort((short)6);
    final byte[] qual4 = { 0x0, 0x30 };
    final byte[] val4 = { 7 };
    final byte[] qual5 = { 0x0, 0x23 };
    final byte[] val5 = Bytes.fromInt(8);
    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual1, qual2), 
        MockBase.concatByteArrays(val1, val2, new byte[] { 0 }));
    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual3, qual4), 
        MockBase.concatByteArrays(val3, val4, new byte[] { 0 }));
    storage.addColumn(ROW, qual5, val5);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(3, fsck.kvs_processed.get());
    assertEquals(2, fsck.duplicates.get());
    assertEquals(2, fsck.totalErrors());
    assertEquals(2, fsck.correctable());
    assertArrayEquals(MockBase.concatByteArrays(val1, val5, val4, 
        new byte[] { 0 }), storage.getColumn(ROW, 
            MockBase.concatByteArrays(qual1, qual5, qual4)));
    assertNull(storage.getColumn(ROW, MockBase.concatByteArrays(qual1, qual2)));
    assertNull(storage.getColumn(ROW, MockBase.concatByteArrays(qual3, qual4)));
    assertNull(storage.getColumn(ROW, qual5));
  }
  
  @Test
  public void compactedAndSingleWSameTS() throws Exception {
    final byte[] qual1 = { 0x0, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { 0x0, 0x20 };
    final byte[] val2 = { 5 };
    final byte[] qual3 = { 0x0, 0x30 };
    final byte[] val3 = { 6 };
    final byte[] qual4 = { 0x0, 0x30 };
    final byte[] val4 = { 7 };
    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual1, qual2, qual3), 
        MockBase.concatByteArrays(val1, val2, val3, new byte[] { 0 }));
    storage.addColumn(ROW, qual4, val4);
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(1, fsck.duplicates.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.correctable());
    assertArrayEquals(MockBase.concatByteArrays(val1, val2, val3, new byte[] { 0 }), 
        storage.getColumn(ROW, MockBase.concatByteArrays(qual1, qual2, qual3)));
    assertArrayEquals(val4, storage.getColumn(ROW, qual4));
  }
  
  @Test
  public void compactedAndSingleWSameTSFix() throws Exception {
    when(options.fix()).thenReturn(true);
    when(options.resolveDupes()).thenReturn(true);
    
    final byte[] qual1 = { 0x0, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { 0x0, 0x20 };
    final byte[] val2 = { 5 };
    final byte[] qual3 = { 0x0, 0x30 };
    final byte[] val3 = { 6 };
    final byte[] qual4 = { 0x0, 0x30 };
    final byte[] val4 = { 7 };
    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual1, qual2, qual3), 
        MockBase.concatByteArrays(val1, val2, val3, new byte[] { 0 }));
    storage.addColumn(ROW, qual4, val4);
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(1, fsck.duplicates.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.correctable());
    assertArrayEquals(MockBase.concatByteArrays(val1, val2, val3, new byte[] { 0 }), 
        storage.getColumn(ROW, MockBase.concatByteArrays(qual1, qual2, qual3)));
    assertNull(storage.getColumn(ROW, qual4));
  }
  
  @Test
  public void compactedAndSingleWSameTSLWW() throws Exception {
    when(options.lastWriteWins()).thenReturn(true);
    
    final byte[] qual1 = { 0x0, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { 0x0, 0x20 };
    final byte[] val2 = { 5 };
    final byte[] qual3 = { 0x0, 0x30 };
    final byte[] val3 = { 6 };
    final byte[] qual4 = { 0x0, 0x30 };
    final byte[] val4 = { 7 };
    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual1, qual2, qual3), 
        MockBase.concatByteArrays(val1, val2, val3, new byte[] { 0 }));
    storage.addColumn(ROW, qual4, val4);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(1, fsck.duplicates.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.correctable());
    assertArrayEquals(MockBase.concatByteArrays(val1, val2, val3, new byte[] { 0 }), 
        storage.getColumn(ROW, MockBase.concatByteArrays(qual1, qual2, qual3)));
    assertArrayEquals(val4, storage.getColumn(ROW, qual4));
  }
  
  @Test
  public void compactedAndSingleWSameTSLWWFix() throws Exception {
    when(options.fix()).thenReturn(true);
    when(options.resolveDupes()).thenReturn(true);
    when(options.lastWriteWins()).thenReturn(true);
    
    final byte[] qual1 = { 0x0, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { 0x0, 0x20 };
    final byte[] val2 = { 5 };
    final byte[] qual3 = { 0x0, 0x30 };
    final byte[] val3 = { 6 };
    final byte[] qual4 = { 0x0, 0x30 };
    final byte[] val4 = { 7 };
    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual1, qual2, qual3), 
        MockBase.concatByteArrays(val1, val2, val3, new byte[] { 0 }));
    storage.addColumn(ROW, qual4, val4);
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(1, fsck.duplicates.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.correctable());
    assertArrayEquals(MockBase.concatByteArrays(val1, val2, val4, new byte[] { 0 }), 
        storage.getColumn(ROW, MockBase.concatByteArrays(qual1, qual2, qual4)));
    assertNull(storage.getColumn(ROW, qual3));
  }
  
  @Test
  public void compactedAndSingleMSWSameTS() throws Exception {
    final byte[] qual1 = { (byte) 0xF0, 0x00, 0x00, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x02, 0x00 };
    final byte[] val2 = { 5 };
    final byte[] qual3 = { (byte) 0xF0, 0x00, 0x04, 0x00 };
    final byte[] val3 =  { 6 };
    final byte[] qual4 = { (byte) 0xF0, 0x00, 0x04, 0x00 };
    final byte[] val4 = { 7 };
    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual1, qual2, qual3), 
        MockBase.concatByteArrays(val1, val2, val3, new byte[] { 0 }));
    storage.addColumn(ROW, qual4, val4);

    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(1, fsck.duplicates.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.correctable());
    assertArrayEquals(MockBase.concatByteArrays(val1, val2, val3, new byte[] { 0 }), 
        storage.getColumn(ROW, MockBase.concatByteArrays(qual1, qual2, qual3)));
    assertArrayEquals(val4, storage.getColumn(ROW, qual4));
  }
  
  @Test
  public void compactedAndSingleMSWSameTSFix() throws Exception {
    when(options.fix()).thenReturn(true);
    when(options.resolveDupes()).thenReturn(true);
    
    final byte[] qual1 = { (byte) 0xF0, 0x00, 0x00, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x02, 0x00 };
    final byte[] val2 = { 5 };
    final byte[] qual3 = { (byte) 0xF0, 0x00, 0x04, 0x00 };
    final byte[] val3 =  { 6 };
    final byte[] qual4 = { (byte) 0xF0, 0x00, 0x04, 0x00 };
    final byte[] val4 = { 7 };
    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual1, qual2, qual3), 
        MockBase.concatByteArrays(val1, val2, val3, new byte[] { 0 }));
    storage.addColumn(ROW, qual4, val4);

    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(1, fsck.duplicates.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.correctable());
    assertArrayEquals(MockBase.concatByteArrays(val1, val2, val3, new byte[] { 0 }), 
        storage.getColumn(ROW, MockBase.concatByteArrays(qual1, qual2, qual3)));
    assertNull(storage.getColumn(ROW, qual4));
  }
  
  @Test
  public void compactedAndSingleMSWSameTSLWW() throws Exception {
    when(options.lastWriteWins()).thenReturn(true);
    
    final byte[] qual1 = { (byte) 0xF0, 0x00, 0x00, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x02, 0x00 };
    final byte[] val2 = { 5 };
    final byte[] qual3 = { (byte) 0xF0, 0x00, 0x04, 0x00 };
    final byte[] val3 =  { 6 };
    final byte[] qual4 = { (byte) 0xF0, 0x00, 0x04, 0x00 };
    final byte[] val4 = { 7 };
    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual1, qual2, qual3), 
        MockBase.concatByteArrays(val1, val2, val3, new byte[] { 0 }));
    storage.addColumn(ROW, qual4, val4);

    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(1, fsck.duplicates.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.correctable());
    assertArrayEquals(MockBase.concatByteArrays(val1, val2, val3, new byte[] { 0 }), 
        storage.getColumn(ROW, MockBase.concatByteArrays(qual1, qual2, qual3)));
    assertArrayEquals(val4, storage.getColumn(ROW, qual4));
  }
  
  @Test
  public void compactedAndSingleMSWSameTSLWWFix() throws Exception {
    when(options.fix()).thenReturn(true);
    when(options.resolveDupes()).thenReturn(true);
    when(options.lastWriteWins()).thenReturn(true);
    
    final byte[] qual1 = { (byte) 0xF0, 0x00, 0x00, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x02, 0x00 };
    final byte[] val2 = { 5 };
    final byte[] qual3 = { (byte) 0xF0, 0x00, 0x04, 0x00 };
    final byte[] val3 =  { 6 };
    final byte[] qual4 = { (byte) 0xF0, 0x00, 0x04, 0x00 };
    final byte[] val4 = { 7 };
    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual1, qual2, qual3), 
        MockBase.concatByteArrays(val1, val2, val3, new byte[] { 0 }));
    storage.addColumn(ROW, qual4, val4);

    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(1, fsck.duplicates.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.correctable());
    assertArrayEquals(MockBase.concatByteArrays(val1, val2, val4, new byte[] { 0 }), 
        storage.getColumn(ROW, MockBase.concatByteArrays(qual1, qual2, qual4)));
    assertNull(storage.getColumn(ROW, qual4));
  }
  
  @Test
  public void compactedAndSingleMixedWSameTS() throws Exception {
    final byte[] qual1 = { 0x0, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x02, 0x00 };
    final byte[] val2 = { 5 };
    final byte[] qual3 = { 0x0, 0x30 };
    final byte[] val3 = { 6 };
    final byte[] qual4 = { (byte) 0xF0, 0x00, 0x02, 0x00 };
    final byte[] val4 = { 7 };
    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual1, qual2, qual3), 
        MockBase.concatByteArrays(val1, val2, val3, new byte[] { 1 }));
    storage.addColumn(ROW, qual4, val4);
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(1, fsck.duplicates.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.correctable());
    assertArrayEquals(MockBase.concatByteArrays(val1, val2, val3, new byte[] { 1 }), 
        storage.getColumn(ROW, MockBase.concatByteArrays(qual1, qual2, qual3)));
    assertArrayEquals(val4, storage.getColumn(ROW, qual4));
  }
  
  @Test
  public void compactedAndSingleMixedWSameTSFix() throws Exception {
    when(options.fix()).thenReturn(true);
    when(options.resolveDupes()).thenReturn(true);
    
    final byte[] qual1 = { 0x0, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x02, 0x00 };
    final byte[] val2 = { 5 };
    final byte[] qual3 = { 0x0, 0x30 };
    final byte[] val3 = { 6 };
    final byte[] qual4 = { (byte) 0xF0, 0x00, 0x02, 0x00 };
    final byte[] val4 = { 7 };
    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual1, qual2, qual3), 
        MockBase.concatByteArrays(val1, val2, val3, new byte[] { 1 }));
    storage.addColumn(ROW, qual4, val4);
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(1, fsck.duplicates.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.correctable());
    assertArrayEquals(MockBase.concatByteArrays(val1, val2, val3, new byte[] {1 }), 
        storage.getColumn(ROW, MockBase.concatByteArrays(qual1, qual2, qual3)));
    assertNull(storage.getColumn(ROW, qual4));
  }
  
  @Test
  public void compactedAndSingleMixedWSameTSLWW() throws Exception {
    when(options.lastWriteWins()).thenReturn(true);
    final byte[] qual1 = { 0x0, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x02, 0x00 };
    final byte[] val2 = { 5 };
    final byte[] qual3 = { 0x0, 0x30 };
    final byte[] val3 = { 6 };
    final byte[] qual4 = { (byte) 0xF0, 0x00, 0x02, 0x00 };
    final byte[] val4 = { 7 };
    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual1, qual2, qual3), 
        MockBase.concatByteArrays(val1, val2, val3, new byte[] { 1 }));
    storage.addColumn(ROW, qual4, val4);
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(1, fsck.duplicates.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.correctable());
    assertArrayEquals(MockBase.concatByteArrays(val1, val2, val3, new byte[] { 1 }), 
        storage.getColumn(ROW, MockBase.concatByteArrays(qual1, qual2, qual3)));
    assertArrayEquals(val4, storage.getColumn(ROW, qual4));
  }
  
  @Test
  public void compactedAndSingleMixedWSameTSLWWFix() throws Exception {
    when(options.lastWriteWins()).thenReturn(true);
    when(options.fix()).thenReturn(true);
    when(options.resolveDupes()).thenReturn(true);
    
    final byte[] qual1 = { 0x0, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x02, 0x00 };
    final byte[] val2 = { 5 };
    final byte[] qual3 = { 0x0, 0x30 };
    final byte[] val3 = { 6 };
    final byte[] qual4 = { (byte) 0xF0, 0x00, 0x02, 0x00 };
    final byte[] val4 = { 7 };
    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual1, qual2, qual3), 
        MockBase.concatByteArrays(val1, val2, val3, new byte[] { 1 }));
    storage.addColumn(ROW, qual4, val4);
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(1, fsck.duplicates.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.correctable());
    assertArrayEquals(MockBase.concatByteArrays(val1, val4, val3, new byte[] {1 }), 
        storage.getColumn(ROW, MockBase.concatByteArrays(qual1, qual4, qual3)));
    assertNull(storage.getColumn(ROW, qual4));
  }
  
  @Test
  public void tripleCompactedColumnsWSameTS() throws Exception {
    when(options.resolveDupes()).thenReturn(true);
    final byte[] qual1 = { 0x0, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { 0x0, 0x20 };
    final byte[] val2 = { 5 };
    final byte[] qual3 = { 0x0, 0x21 };
    final byte[] val3 = Bytes.fromShort((short)6);
    final byte[] qual4 = { 0x0, 0x30 };
    final byte[] val4 = { 7 };
    final byte[] qual5 = { 0x0, 0x23 };
    final byte[] val5 = Bytes.fromInt(8);
    final byte[] qual6 = { 0x0, 0x40 };
    final byte[] val6 = { 9 };
    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual1, qual2), 
        MockBase.concatByteArrays(val1, val2, new byte[] { 0 }));
    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual3, qual4), 
        MockBase.concatByteArrays(val3, val4, new byte[] { 0 }));
    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual5, qual6), 
        MockBase.concatByteArrays(val5, val6, new byte[] { 0 }));
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(3, fsck.kvs_processed.get());
    assertEquals(2, fsck.duplicates.get());
    assertEquals(2, fsck.totalErrors());
    assertEquals(2, fsck.correctable());
    assertArrayEquals(MockBase.concatByteArrays(val1, val2, new byte[] { 0 }), 
        storage.getColumn(ROW, MockBase.concatByteArrays(qual1, qual2)));
    assertArrayEquals(MockBase.concatByteArrays(val3, val4, new byte[] { 0 }), 
        storage.getColumn(ROW, MockBase.concatByteArrays(qual3, qual4)));
    assertArrayEquals(MockBase.concatByteArrays(val5, val6, new byte[] { 0 }), 
        storage.getColumn(ROW, MockBase.concatByteArrays(qual5, qual6)));
  }
  
  @Test
  public void tripleCompactedColumnsWSameTSFix() throws Exception {
    when(options.fix()).thenReturn(true);
    when(options.resolveDupes()).thenReturn(true);
    
    final byte[] qual1 = { 0x0, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { 0x0, 0x20 };
    final byte[] val2 = { 5 };
    final byte[] qual3 = { 0x0, 0x21 };
    final byte[] val3 = Bytes.fromShort((short)6);
    final byte[] qual4 = { 0x0, 0x30 };
    final byte[] val4 = { 7 };
    final byte[] qual5 = { 0x0, 0x23 };
    final byte[] val5 = Bytes.fromInt(8);
    final byte[] qual6 = { 0x0, 0x40 };
    final byte[] val6 = { 9 };
    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual1, qual2), 
        MockBase.concatByteArrays(val1, val2, new byte[] { 0 }));
    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual3, qual4), 
        MockBase.concatByteArrays(val3, val4, new byte[] { 0 }));
    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual5, qual6), 
        MockBase.concatByteArrays(val5, val6, new byte[] { 0 }));
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(3, fsck.kvs_processed.get());
    assertEquals(2, fsck.duplicates.get());
    assertEquals(2, fsck.totalErrors());
    assertEquals(2, fsck.correctable());
    assertArrayEquals(MockBase.concatByteArrays(val1, val2, val4, val6, new byte[] { 0 }), 
        storage.getColumn(ROW, MockBase.concatByteArrays(qual1, qual2, qual4, qual6)));
    assertNull(storage.getColumn(ROW, MockBase.concatByteArrays(qual1, qual2)));
    assertNull(storage.getColumn(ROW, MockBase.concatByteArrays(qual3, qual4)));
    assertNull(storage.getColumn(ROW, MockBase.concatByteArrays(qual5, qual6)));
  }
  
  @Test
  public void tripleCompactedColumnsWSameTSLWW() throws Exception {
    when(options.lastWriteWins()).thenReturn(true);
    
    final byte[] qual1 = { 0x0, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { 0x0, 0x20 };
    final byte[] val2 = { 5 };
    final byte[] qual3 = { 0x0, 0x21 };
    final byte[] val3 = Bytes.fromShort((short)6);
    final byte[] qual4 = { 0x0, 0x30 };
    final byte[] val4 = { 7 };
    final byte[] qual5 = { 0x0, 0x23 };
    final byte[] val5 = Bytes.fromInt(8);
    final byte[] qual6 = { 0x0, 0x40 };
    final byte[] val6 = { 9 };
    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual1, qual2), 
        MockBase.concatByteArrays(val1, val2, new byte[] { 0 }));
    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual3, qual4), 
        MockBase.concatByteArrays(val3, val4, new byte[] { 0 }));
    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual5, qual6), 
        MockBase.concatByteArrays(val5, val6, new byte[] { 0 }));
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(3, fsck.kvs_processed.get());
    assertEquals(2, fsck.duplicates.get());
    assertEquals(2, fsck.totalErrors());
    assertEquals(2, fsck.correctable());
    assertArrayEquals(MockBase.concatByteArrays(val1, val2, new byte[] { 0 }), 
        storage.getColumn(ROW, MockBase.concatByteArrays(qual1, qual2)));
    assertArrayEquals(MockBase.concatByteArrays(val3, val4, new byte[] { 0 }), 
        storage.getColumn(ROW, MockBase.concatByteArrays(qual3, qual4)));
    assertArrayEquals(MockBase.concatByteArrays(val5, val6, new byte[] { 0 }), 
        storage.getColumn(ROW, MockBase.concatByteArrays(qual5, qual6)));
  }
  
  @Test
  public void tripleCompactedColumnsWSameTSLWWFix() throws Exception {
    when(options.fix()).thenReturn(true);
    when(options.resolveDupes()).thenReturn(true);
    when(options.lastWriteWins()).thenReturn(true);
    
    final byte[] qual1 = { 0x0, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { 0x0, 0x20 };
    final byte[] val2 = { 5 };
    final byte[] qual3 = { 0x0, 0x21 };
    final byte[] val3 = Bytes.fromShort((short)6);
    final byte[] qual4 = { 0x0, 0x30 };
    final byte[] val4 = { 7 };
    final byte[] qual5 = { 0x0, 0x23 };
    final byte[] val5 = Bytes.fromInt(8);
    final byte[] qual6 = { 0x0, 0x40 };
    final byte[] val6 = { 9 };
    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual1, qual2), 
        MockBase.concatByteArrays(val1, val2, new byte[] { 0 }));
    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual3, qual4), 
        MockBase.concatByteArrays(val3, val4, new byte[] { 0 }));
    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual5, qual6), 
        MockBase.concatByteArrays(val5, val6, new byte[] { 0 }));
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(3, fsck.kvs_processed.get());
    assertEquals(2, fsck.duplicates.get());
    assertEquals(2, fsck.totalErrors());
    assertEquals(2, fsck.correctable());
    assertArrayEquals(MockBase.concatByteArrays(val1, val5, val4, val6, new byte[] { 0 }), 
        storage.getColumn(ROW, MockBase.concatByteArrays(qual1, qual5, qual4, qual6)));
    assertNull(storage.getColumn(ROW, MockBase.concatByteArrays(qual1, qual2)));
    assertNull(storage.getColumn(ROW, MockBase.concatByteArrays(qual3, qual4)));
    assertNull(storage.getColumn(ROW, MockBase.concatByteArrays(qual5, qual6)));
  }

  // MULTIPLE ISSUES ----------------------------
  // check for interactions between flags, e.g. compact + delete bad values
  // + resolve duplicates, etc
  
  @Test
  public void compactAndNotFixDupes() throws Exception {
    when(options.fix()).thenReturn(true);
    when(options.compact()).thenReturn(true);
    
    final byte[] qual1 = { 0x00, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { 0x00, 0x0B };
    final byte[] val2 = Bytes.fromInt(Float.floatToRawIntBits(500.8F));
    final byte[] qual3 = { 0x00, 0x20 };
    final byte[] val3 = { 5 };
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    storage.addColumn(ROW, qual3, val3);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    storage.dumpToSystemOut();
    assertEquals(3, fsck.kvs_processed.get());
    assertEquals(1, fsck.duplicates.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.correctable());
    assertArrayEquals(val1, storage.getColumn(ROW, qual1));
    assertArrayEquals(val2, storage.getColumn(ROW, qual2));
    assertArrayEquals(val3, storage.getColumn(ROW, qual3));
  }
  
  @Test
  public void compactAndFixDupes() throws Exception {
    when(options.fix()).thenReturn(true);
    when(options.compact()).thenReturn(true);
    when(options.resolveDupes()).thenReturn(true);
    
    final byte[] qual1 = { 0x00, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { 0x00, 0x0B };
    final byte[] val2 = Bytes.fromInt(Float.floatToRawIntBits(500.8F));
    final byte[] qual3 = { 0x00, 0x20 };
    final byte[] val3 = { 5 };
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    storage.addColumn(ROW, qual3, val3);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(3, fsck.kvs_processed.get());
    assertEquals(1, fsck.duplicates.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.correctable());
    assertNull(storage.getColumn(ROW, qual1));
    assertNull(storage.getColumn(ROW, qual2));
    assertNull(storage.getColumn(ROW, qual3));
    assertArrayEquals(MockBase.concatByteArrays(val1, val3, new byte[] { 0 }),
        storage.getColumn(ROW,  MockBase.concatByteArrays(qual1, qual3)));
  }

  @Test
  public void vleAndCompact() throws Exception {
    when(options.fix()).thenReturn(true);
    when(options.compact()).thenReturn(true);
    
    final byte[] qual1 = { 0x0, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x0, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    final byte[] compacted_qual = { 0x0, 0x0, 0x0, 0x20 };
    final byte[] compacted_value = { 0x4, 0x5, 0x0 };
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(2, fsck.vle.get());
    assertEquals(0, fsck.totalErrors());
    assertEquals(0, fsck.correctable());
    assertNull(storage.getColumn(ROW, qual1));
    assertNull(storage.getColumn(ROW, qual2));
    assertArrayEquals(compacted_value, storage.getColumn(ROW, compacted_qual));
  }
  
  @Test
  public void compactAndNotFixIntegerWrongLength() throws Exception {
    when(options.fix()).thenReturn(true);
    when(options.compact()).thenReturn(true);

    final byte[] qual1 = { 0x00, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { 0x00, 0x23 };
    final byte[] val2 = Bytes.fromLong(5L);
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    storage.dumpToSystemOut();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(0, fsck.totalFixed());
    assertEquals(1, fsck.correctable());
    assertEquals(1, fsck.bad_values.get());
    assertArrayEquals(val1, storage.getColumn(ROW, qual1));
    assertArrayEquals(val2, storage.getColumn(ROW, qual2));
  }
  
  @Test
  public void compactAndFixIntegerWrongLength() throws Exception {
    when(options.fix()).thenReturn(true);
    when(options.compact()).thenReturn(true);
    when(options.deleteBadValues()).thenReturn(true);
    
    final byte[] qual1 = { 0x00, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { 0x00, 0x23 };
    final byte[] val2 = Bytes.fromLong(5L);
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    storage.dumpToSystemOut();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(1, fsck.totalErrors());
    assertEquals(1, fsck.totalFixed());
    assertEquals(1, fsck.correctable());
    assertEquals(1, fsck.bad_values.get());
    assertArrayEquals(val1, storage.getColumn(ROW, qual1));
    assertNull(storage.getColumn(ROW, qual2));
  }

  @Test
  public void rowOfBadValuesNotFix() throws Exception {
    when(options.fix()).thenReturn(true);
    when(options.compact()).thenReturn(true);
    
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromInt(4);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 5 };
    final byte[] qual3 = { 0x00, 0x47 };
    final byte[] val3 = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 6 };
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    storage.addColumn(ROW, qual3, val3);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(3, fsck.kvs_processed.get());
    assertEquals(3, fsck.totalErrors());
    assertEquals(3, fsck.correctable());
    assertEquals(3, fsck.bad_values.get());
    assertArrayEquals(val1, storage.getColumn(ROW, qual1));
    assertArrayEquals(val2, storage.getColumn(ROW, qual2));
    assertArrayEquals(val3, storage.getColumn(ROW, qual3));
  }
  
  @Test
  public void rowOfBadValuesFix() throws Exception {
    when(options.fix()).thenReturn(true);
    when(options.compact()).thenReturn(true);
    when(options.deleteBadValues()).thenReturn(true);
    
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromInt(4);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 5 };
    final byte[] qual3 = { 0x00, 0x47 };
    final byte[] val3 = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 6 };
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    storage.addColumn(ROW, qual3, val3);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(3, fsck.kvs_processed.get());
    assertEquals(3, fsck.totalErrors());
    assertEquals(3, fsck.correctable());
    assertEquals(3, fsck.bad_values.get());
    assertNull(storage.getColumn(ROW, qual1));
    assertNull(storage.getColumn(ROW, qual2));
    assertNull(storage.getColumn(ROW, qual3));
    assertEquals(-1, storage.numColumns(ROW));
  }

  @Test
  public void compactedAndBadValuesNotFix() throws Exception {
    when(options.fix()).thenReturn(true);
    when(options.compact()).thenReturn(true);
    
    final byte[] qual1 = { 0x00, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { 0x00, 0x20 };
    final byte[] val2 = { 5 };
    final byte[] qual3 = { 0x00, 0x47 };
    final byte[] val3 = { 0, 0, 0, 0, 0, 0, 0, 0, 6 };
    final byte[] qual4 = { 0x00, 0x57 };
    final byte[] val4 = { 0, 0, 0, 0, 0, 0, 0, 0, 7 };
    storage.addColumn(ROW, MockBase.concatByteArrays(qual1, qual2), 
        MockBase.concatByteArrays(val1, val2, new byte[] { 0 }));
    storage.addColumn(ROW, qual3, val3);
    storage.addColumn(ROW, qual4, val4);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(3, fsck.kvs_processed.get());
    assertEquals(2, fsck.totalErrors());
    assertEquals(2, fsck.correctable());
    assertEquals(2, fsck.bad_values.get());
    assertArrayEquals(MockBase.concatByteArrays(val1, val2, new byte[] { 0 }), 
        storage.getColumn(ROW, MockBase.concatByteArrays(qual1, qual2)));
    assertArrayEquals(val3, storage.getColumn(ROW, qual3));
    assertArrayEquals(val4, storage.getColumn(ROW, qual4));
  }
  
  @Test
  public void compactedAndBadValuesFix() throws Exception {
    when(options.fix()).thenReturn(true);
    when(options.compact()).thenReturn(true);
    when(options.deleteBadValues()).thenReturn(true);
    
    final byte[] qual1 = { 0x00, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { 0x00, 0x20 };
    final byte[] val2 = { 5 };
    final byte[] qual3 = { 0x00, 0x47 };
    final byte[] val3 = { 0, 0, 0, 0, 0, 0, 0, 0, 6 };
    final byte[] qual4 = { 0x00, 0x57 };
    final byte[] val4 = { 0, 0, 0, 0, 0, 0, 0, 0, 7 };
    storage.addColumn(ROW, MockBase.concatByteArrays(qual1, qual2), 
        MockBase.concatByteArrays(val1, val2, new byte[] { 0 }));
    storage.addColumn(ROW, qual3, val3);
    storage.addColumn(ROW, qual4, val4);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(3, fsck.kvs_processed.get());
    assertEquals(2, fsck.totalErrors());
    assertEquals(2, fsck.correctable());
    assertEquals(2, fsck.bad_values.get());
    assertArrayEquals(MockBase.concatByteArrays(val1, val2, new byte[] { 0 }), 
        storage.getColumn(ROW, MockBase.concatByteArrays(qual1, qual2)));
    assertNull(storage.getColumn(ROW, qual3));
    assertNull(storage.getColumn(ROW, qual4));
  }
  
  @Test
  public void compactedAndBadValuesNotFixAndDupesNotFix() throws Exception {
    when(options.fix()).thenReturn(true);
    when(options.compact()).thenReturn(true);
    
    final byte[] qual1 = { 0x00, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { 0x00, 0x20 };
    final byte[] val2 = { 5 };
    final byte[] qual3 = { 0x00, 0x47 };
    final byte[] val3 = { 0, 0, 0, 0, 0, 0, 0, 0, 6 };
    final byte[] qual4 = { 0x00, 0x20 };
    final byte[] val4 = { 7 };
    storage.addColumn(ROW, MockBase.concatByteArrays(qual1, qual2), 
        MockBase.concatByteArrays(val1, val2, new byte[] { 0 }));
    storage.addColumn(ROW, qual3, val3);
    storage.addColumn(ROW, qual4, val4);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(3, fsck.kvs_processed.get());
    assertEquals(2, fsck.totalErrors());
    assertEquals(2, fsck.correctable());
    assertEquals(1, fsck.duplicates.get());
    assertEquals(1, fsck.bad_values.get());
    assertArrayEquals(MockBase.concatByteArrays(val1, val2, new byte[] { 0 }), 
        storage.getColumn(ROW, MockBase.concatByteArrays(qual1, qual2)));
    assertArrayEquals(val3, storage.getColumn(ROW, qual3));
    assertArrayEquals(val4, storage.getColumn(ROW, qual4));
  }
  
  @Test
  public void compactedAndBadValuesFixAndDupesNotFix() throws Exception {
    // this will delete the bad value but won't compact
    when(options.fix()).thenReturn(true);
    when(options.compact()).thenReturn(true);
    when(options.deleteBadValues()).thenReturn(true);
    
    final byte[] qual1 = { 0x00, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { 0x00, 0x20 };
    final byte[] val2 = { 5 };
    final byte[] qual3 = { 0x00, 0x47 };
    final byte[] val3 = { 0, 0, 0, 0, 0, 0, 0, 0, 6 };
    final byte[] qual4 = { 0x00, 0x20 };
    final byte[] val4 = { 7 };
    storage.addColumn(ROW, MockBase.concatByteArrays(qual1, qual2), 
        MockBase.concatByteArrays(val1, val2, new byte[] { 0 }));
    storage.addColumn(ROW, qual3, val3);
    storage.addColumn(ROW, qual4, val4);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(3, fsck.kvs_processed.get());
    assertEquals(2, fsck.totalErrors());
    assertEquals(2, fsck.correctable());
    assertEquals(1, fsck.duplicates.get());
    assertEquals(1, fsck.bad_values.get());
    assertArrayEquals(MockBase.concatByteArrays(val1, val2, new byte[] { 0 }), 
        storage.getColumn(ROW, MockBase.concatByteArrays(qual1, qual2)));
    assertNull(storage.getColumn(ROW, qual3));
    assertArrayEquals(val4, storage.getColumn(ROW, qual4));
  }
  
  @Test
  public void compactedAndBadValuesNotFixAndDupesFix() throws Exception {
    // this is a no-op as we don't want to compact a row with a bad value when
    // we weren't told to delete the bad value. Therefore we logit and leave it
    when(options.fix()).thenReturn(true);
    when(options.compact()).thenReturn(true);
    when(options.resolveDupes()).thenReturn(true);
    
    final byte[] qual1 = { 0x00, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { 0x00, 0x20 };
    final byte[] val2 = { 5 };
    final byte[] qual3 = { 0x00, 0x47 };
    final byte[] val3 = { 0, 0, 0, 0, 0, 0, 0, 0, 6 };
    final byte[] qual4 = { 0x00, 0x20 };
    final byte[] val4 = { 7 };
    storage.addColumn(ROW, MockBase.concatByteArrays(qual1, qual2), 
        MockBase.concatByteArrays(val1, val2, new byte[] { 0 }));
    storage.addColumn(ROW, qual3, val3);
    storage.addColumn(ROW, qual4, val4);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(3, fsck.kvs_processed.get());
    assertEquals(2, fsck.totalErrors());
    assertEquals(2, fsck.correctable());
    assertEquals(1, fsck.duplicates.get());
    assertEquals(1, fsck.bad_values.get());
    assertEquals(0, fsck.totalFixed());
    assertArrayEquals(MockBase.concatByteArrays(val1, val2, new byte[] { 0 }), 
        storage.getColumn(ROW, MockBase.concatByteArrays(qual1, qual2)));
    assertArrayEquals(val3, storage.getColumn(ROW, qual3));
    assertArrayEquals(val4, storage.getColumn(ROW, qual4));
  }
  
  @Test
  public void compactedAndBadValuesFixAndDupesFix() throws Exception {
    when(options.fix()).thenReturn(true);
    when(options.compact()).thenReturn(true);
    when(options.resolveDupes()).thenReturn(true);
    when(options.deleteBadValues()).thenReturn(true);
    
    final byte[] qual1 = { 0x00, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { 0x00, 0x20 };
    final byte[] val2 = { 5 };
    final byte[] qual3 = { 0x00, 0x47 };
    final byte[] val3 = { 0, 0, 0, 0, 0, 0, 0, 0, 6 };
    final byte[] qual4 = { 0x00, 0x20 };
    final byte[] val4 = { 7 };
    storage.addColumn(ROW, MockBase.concatByteArrays(qual1, qual2), 
        MockBase.concatByteArrays(val1, val2, new byte[] { 0 }));
    storage.addColumn(ROW, qual3, val3);
    storage.addColumn(ROW, qual4, val4);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(3, fsck.kvs_processed.get());
    assertEquals(2, fsck.totalErrors());
    assertEquals(2, fsck.correctable());
    assertEquals(1, fsck.duplicates.get());
    assertEquals(1, fsck.bad_values.get());
    assertEquals(2, fsck.totalFixed());
    assertArrayEquals(MockBase.concatByteArrays(val1, val2, new byte[] { 0 }), 
        storage.getColumn(ROW, MockBase.concatByteArrays(qual1, qual2)));
    assertNull(storage.getColumn(ROW, qual3));
    assertNull(storage.getColumn(ROW, qual4));
  }
  
  @Test
  public void compactedAndBadValuesFixAndDupesLWWFix() throws Exception {
    when(options.fix()).thenReturn(true);
    when(options.compact()).thenReturn(true);
    when(options.resolveDupes()).thenReturn(true);
    when(options.deleteBadValues()).thenReturn(true);
    when(options.lastWriteWins()).thenReturn(true);
    
    final byte[] qual1 = { 0x00, 0x00 };
    final byte[] val1 = { 4 };
    final byte[] qual2 = { 0x00, 0x20 };
    final byte[] val2 = { 5 };
    final byte[] qual3 = { 0x00, 0x47 };
    final byte[] val3 = { 0, 0, 0, 0, 0, 0, 0, 0, 6 };
    final byte[] qual4 = { 0x00, 0x20 };
    final byte[] val4 = { 7 };
    storage.addColumn(ROW, MockBase.concatByteArrays(qual1, qual2), 
        MockBase.concatByteArrays(val1, val2, new byte[] { 0 }));
    storage.addColumn(ROW, qual3, val3);
    storage.addColumn(ROW, qual4, val4);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(3, fsck.kvs_processed.get());
    assertEquals(2, fsck.totalErrors());
    assertEquals(2, fsck.correctable());
    assertEquals(1, fsck.duplicates.get());
    assertEquals(1, fsck.bad_values.get());
    assertEquals(2, fsck.totalFixed());
    assertArrayEquals(MockBase.concatByteArrays(val1, val4, new byte[] { 0 }), 
        storage.getColumn(ROW, MockBase.concatByteArrays(qual1, qual2)));
    assertNull(storage.getColumn(ROW, qual3));
    assertNull(storage.getColumn(ROW, qual4));
  }

  // QUERIES --------------------------------------------
  
  @Test
  public void runQuery() throws Exception {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);

    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    final String[] args = "1356998400 sum sys.cpu.user".split(" ");
    final ArrayList<Query> queries = new ArrayList<Query>();
    CliQuery.parseCommandLineQuery(args, tsdb, queries, null, null);
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runQueries(queries);
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(1, fsck.rows_processed.get());
    assertEquals(0, fsck.totalErrors());
  }

}
