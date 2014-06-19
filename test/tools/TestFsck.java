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
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;

import net.opentsdb.core.TSDB;
import net.opentsdb.meta.Annotation;
import net.opentsdb.storage.MockBase;
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

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
  "ch.qos.*", "org.slf4j.*",
  "com.sum.*", "org.xml.*"})
@PrepareForTest({TSDB.class, Config.class, UniqueId.class, HBaseClient.class, 
  GetRequest.class, PutRequest.class, KeyValue.class, Fsck.class,
  Scanner.class, DeleteRequest.class, Annotation.class })
public final class TestFsck {
  private final static byte[] ROW = 
    MockBase.stringToBytes("00000150E22700000001000001");
  private Config config;
  private TSDB tsdb = null;
  private HBaseClient client = mock(HBaseClient.class);
  private UniqueId metrics = mock(UniqueId.class);
  private UniqueId tag_names = mock(UniqueId.class);
  private UniqueId tag_values = mock(UniqueId.class);
  private MockBase storage;
 
  private final static Method fsck;
  static {
    try {
      fsck = Fsck.class.getDeclaredMethod("fsck", TSDB.class, HBaseClient.class, 
          byte[].class, boolean.class, String[].class);
      fsck.setAccessible(true);
    } catch (Exception e) {
      throw new RuntimeException("Failed in static initializer", e);
    }
  }
  
  @Before
  public void before() throws Exception {
    PowerMockito.whenNew(HBaseClient.class)
      .withArguments(anyString(), anyString()).thenReturn(client);
    config = new Config(false);
    tsdb = new TSDB(config);
    
    storage = new MockBase(tsdb, client, true, true, true, true);
    storage.setFamily("t".getBytes(MockBase.ASCII()));
    
    PowerMockito.mockStatic(System.class);
    when(System.nanoTime())
      .thenReturn(1357300800000000L)
      .thenReturn(1357300900000000L);
    
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
    
    when(metrics.width()).thenReturn((short)3);
    when(tag_names.width()).thenReturn((short)3);
    when(tag_values.width()).thenReturn((short)3);
  }
  
  @Test
  public void noData() throws Exception {
    int errors = (Integer)fsck.invoke(null, tsdb, client, 
        "tsdb".getBytes(MockBase.ASCII()), false, new String[] { 
        "1356998400", "1357002000", "sum", "sys.cpu.user" });
    assertEquals(0, errors);
  }
  
  @Test
  public void noErrors() throws Exception {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = new byte[] { 0, 0, 0, 0, 0, 0, 0,5 };

    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    int errors = (Integer)fsck.invoke(null, tsdb, client, 
        "tsdb".getBytes(MockBase.ASCII()), false, new String[] { 
        "1356998400", "1357002000", "sum", "sys.cpu.user" });
    assertEquals(0, errors);
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
    int errors = (Integer)fsck.invoke(null, tsdb, client, 
        "tsdb".getBytes(MockBase.ASCII()), false, new String[] { 
        "1356998400", "1357002000", "sum", "sys.cpu.user" });
    assertEquals(0, errors);
  }
  
  @Test
  public void noErrorsAnnotation() throws Exception {
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = new byte[] { 0, 0, 0, 0, 0, 0, 0,5 };

    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);

    final Annotation note = new Annotation();
    note.setTSUID(MockBase.bytesToString(ROW));
    note.setDescription("woot");
    note.setStartTime(1356998460);
    note.syncToStorage(tsdb, true).joinUninterruptibly();
    int errors = (Integer)fsck.invoke(null, tsdb, client, 
        "tsdb".getBytes(MockBase.ASCII()), false, new String[] { 
        "1356998400", "1357002000", "sum", "sys.cpu.user" });
    assertEquals(0, errors);
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
  
    int errors = (Integer)fsck.invoke(null, tsdb, client, 
        "tsdb".getBytes(MockBase.ASCII()), false, new String[] { 
        "1356998400", "1357002000", "sum", "sys.cpu.user" });
    assertEquals(0, errors);
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
    
    final Annotation note = new Annotation();
    note.setTSUID(MockBase.bytesToString(ROW));
    note.setDescription("woot");
    note.setStartTime(1356998460);
    note.syncToStorage(tsdb, true).joinUninterruptibly();
    
    int errors = (Integer)fsck.invoke(null, tsdb, client, 
        "tsdb".getBytes(MockBase.ASCII()), false, new String[] { 
        "1356998400", "1357002000", "sum", "sys.cpu.user" });
    assertEquals(0, errors);
  }

  @Test
  public void NoErrorsCompacted() throws Exception {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    final byte[] val12 = MockBase.concatByteArrays(val1, val2, new byte[] { 0 });
    storage.addColumn(ROW, qual12, val12);
    int errors = (Integer)fsck.invoke(null, tsdb, client, 
        "tsdb".getBytes(MockBase.ASCII()), false, new String[] { 
        "1356998400", "1357002000", "sum", "sys.cpu.user" });
    assertEquals(0, errors);
  }
  
  @Test
  public void NoErrorsCompactedMS() throws Exception {
    final byte[] qual1 = { (byte) 0xF0, 0x00, 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x02, 0x07 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual3 = { (byte) 0xF0, 0x00, 0x04, 0x07 };
    final byte[] val3 = Bytes.fromLong(6L);

    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual1, qual2, qual3), 
        MockBase.concatByteArrays(val1, val2, val3, new byte[] { 0 }));
    int errors = (Integer)fsck.invoke(null, tsdb, client, 
        "tsdb".getBytes(MockBase.ASCII()), false, new String[] { 
        "1356998400", "1357002000", "sum", "sys.cpu.user" });
    assertEquals(0, errors);
    assertEquals(1, storage.numColumns(ROW));
  }
  
  @Test
  public void NoErrorsCompactedMix() throws Exception {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x02, 0x07 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    final byte[] val12 = MockBase.concatByteArrays(val1, val2, new byte[] { 0 });
    storage.addColumn(ROW, qual12, val12);
    int errors = (Integer)fsck.invoke(null, tsdb, client, 
        "tsdb".getBytes(MockBase.ASCII()), false, new String[] { 
        "1356998400", "1357002000", "sum", "sys.cpu.user" });
    assertEquals(0, errors);
    assertEquals(1, storage.numColumns(ROW));
  }
  
  @Test
  public void NoErrorsCompactedMixReverse() throws Exception {
    final byte[] qual1 = { (byte) 0xF0, 0x00, 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    final byte[] val12 = MockBase.concatByteArrays(val1, val2, new byte[] { 0 });
    storage.addColumn(ROW, qual12, val12);
    int errors = (Integer)fsck.invoke(null, tsdb, client, 
        "tsdb".getBytes(MockBase.ASCII()), false, new String[] { 
        "1356998400", "1357002000", "sum", "sys.cpu.user" });
    assertEquals(0, errors);
    assertEquals(1, storage.numColumns(ROW));
  }
  
  @Test
  public void lastCompactedByteNotZero() throws Exception {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    final byte[] val12 = MockBase.concatByteArrays(val1, val2);
    storage.addColumn(ROW, qual12, val12);
    int errors = (Integer)fsck.invoke(null, tsdb, client, 
        "tsdb".getBytes(MockBase.ASCII()), false, new String[] { 
        "1356998400", "1357002000", "sum", "sys.cpu.user" });
    assertEquals(1, errors);
  }
  
  @Test
  public void oneByteQualifier() throws Exception {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x01 };
    final byte[] val2 = new byte[] { 5 };

    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    int errors = (Integer)fsck.invoke(null, tsdb, client, 
        "tsdb".getBytes(MockBase.ASCII()), false, new String[] { 
        "1356998400", "1357002000", "sum", "sys.cpu.user" });
    assertEquals(1, errors);
  }
  
  @Test
  public void valueTooLong() throws Exception {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 5 };

    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    int errors = (Integer)fsck.invoke(null, tsdb, client, 
        "tsdb".getBytes(MockBase.ASCII()), false, new String[] { 
        "1356998400", "1357002000", "sum", "sys.cpu.user" });
    assertEquals(1, errors);
  }
  
  @Test
  public void valueTooLongMS() throws Exception {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x02, 0x0B };
    final byte[] val2 = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 5 };

    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    int errors = (Integer)fsck.invoke(null, tsdb, client, 
        "tsdb".getBytes(MockBase.ASCII()), false, new String[] { 
        "1356998400", "1357002000", "sum", "sys.cpu.user" });
    assertEquals(1, errors);
  }

  @Test
  public void singleByteQual() throws Exception {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);

    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    int errors = (Integer)fsck.invoke(null, tsdb, client, 
        "tsdb".getBytes(MockBase.ASCII()), false, new String[] { 
        "1356998400", "1357002000", "sum", "sys.cpu.user" });
    assertEquals(1, errors);
  }
  
  @Test
  public void OLDfloat8byteVal4byteQualOK() throws Exception {
    final byte[] qual1 = { 0x00, 0x0B };
    final byte[] val1 = Bytes.fromLong(Float.floatToRawIntBits(4.2F));
    final byte[] qual2 = { 0x00, 0x2B };
    final byte[] val2 = Bytes.fromLong(Float.floatToRawIntBits(500.8F));

    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    int errors = (Integer)fsck.invoke(null, tsdb, client, 
        "tsdb".getBytes(MockBase.ASCII()), false, new String[] { 
        "1356998400", "1357002000", "sum", "sys.cpu.user" });
    assertEquals(0, errors);
  }
  
  @Test
  public void OLDfloat8byteVal4byteQualSignExtensionBug() throws Exception {
    final byte[] qual1 = { 0x00, 0x0B };
    final byte[] val1 = Bytes.fromLong(Float.floatToRawIntBits(4.2F));
    final byte[] qual2 = { 0x00, 0x2B };
    final byte[] bug = { (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF };
    final byte[] val2 = Bytes.fromInt(Float.floatToRawIntBits(500.8F));

    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, MockBase.concatByteArrays(bug, val2));
    int errors = (Integer)fsck.invoke(null, tsdb, client, 
        "tsdb".getBytes(MockBase.ASCII()), false, new String[] { 
        "1356998400", "1357002000", "sum", "sys.cpu.user" });
    assertEquals(1, errors);
  }
  
  @Test
  public void OLDfloat8byteVal4byteQualSignExtensionBugCompacted() 
      throws Exception {
    final byte[] qual1 = { 0x00, 0x0B };
    final byte[] val1 = Bytes.fromLong(Float.floatToRawIntBits(4.2F));
    final byte[] qual2 = { 0x00, 0x2B };
    final byte[] bug = { (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF };
    final byte[] val2 = Bytes.fromInt(Float.floatToRawIntBits(500.8F));
    storage.addColumn(ROW, MockBase.concatByteArrays(qual1, qual2), 
        MockBase.concatByteArrays(val1, bug, val2, new byte[] { 0 }));
    int errors = (Integer)fsck.invoke(null, tsdb, client, 
        "tsdb".getBytes(MockBase.ASCII()), false, new String[] { 
        "1356998400", "1357002000", "sum", "sys.cpu.user" });
    assertEquals(1, errors);
  }
  
  @Test
  public void OLDfloat8byteVal4byteQualSignExtensionBugFix() throws Exception {
    final byte[] qual1 = { 0x00, 0x0B };
    final byte[] val1 = Bytes.fromLong(Float.floatToRawIntBits(4.2F));
    final byte[] qual2 = { 0x00, 0x2B };
    final byte[] bug = { (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF };
    final byte[] val2 = Bytes.fromInt(Float.floatToRawIntBits(500.8F));

    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, MockBase.concatByteArrays(bug, val2));
    int errors = (Integer)fsck.invoke(null, tsdb, client, 
        "tsdb".getBytes(MockBase.ASCII()), true, new String[] { 
        "1356998400", "1357002000", "sum", "sys.cpu.user" });
    assertEquals(1, errors);
    final byte[] fixed = storage.getColumn(ROW, qual2);
    assertArrayEquals(MockBase.concatByteArrays(new byte[4], val2), fixed);
  }
  
  @Test
  public void OLDfloat8byteVal4byteQualMessedUp() throws Exception {
    final byte[] qual1 = { 0x00, 0x0B };
    final byte[] val1 = Bytes.fromLong(Float.floatToRawIntBits(4.2F));
    final byte[] qual2 = { 0x00, 0x2B };
    final byte[] bug = { (byte) 0xFB, (byte) 0x02, (byte) 0xF4, (byte) 0x0F };
    final byte[] val2 = Bytes.fromInt(Float.floatToRawIntBits(500.8F));

    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, MockBase.concatByteArrays(bug, val2));
    int errors = (Integer)fsck.invoke(null, tsdb, client, 
        "tsdb".getBytes(MockBase.ASCII()), false, new String[] { 
        "1356998400", "1357002000", "sum", "sys.cpu.user" });
    assertEquals(1, errors);
  }

  @Test
  public void floatNot4Or8Bytes() throws Exception {
    final byte[] qual1 = { 0x00, 0x0B };
    final byte[] val1 = Bytes.fromLong(Float.floatToRawIntBits(4.2F));
    final byte[] qual2 = { 0x00, 0x2B };
    final byte[] bug = { 0 };
    final byte[] val2 = Bytes.fromInt(Float.floatToRawIntBits(500.8F));

    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, MockBase.concatByteArrays(bug, val2));
    int errors = (Integer)fsck.invoke(null, tsdb, client, 
        "tsdb".getBytes(MockBase.ASCII()), false, new String[] { 
        "1356998400", "1357002000", "sum", "sys.cpu.user" });
    assertEquals(1, errors);
  }

  @Test
  public void unknownObject() throws Exception {
    final byte[] qual1 = { 0x00, 0x07};
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27, 0x04, 0x01, 0x01, 0x01, 0x01 };
    final byte[] val2 = Bytes.fromLong(5L);
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    int errors = (Integer)fsck.invoke(null, tsdb, client, 
        "tsdb".getBytes(MockBase.ASCII()), false, new String[] { 
        "1356998400", "1357002000", "sum", "sys.cpu.user" });
    assertEquals(1, errors);
  }
  
  @Test
  public void futureObject() throws Exception {
    final byte[] qual1 = { 0x00, 0x07};
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x04, 0x27, 0x04 };
    final byte[] val2 = Bytes.fromLong(5L);
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    int errors = (Integer)fsck.invoke(null, tsdb, client, 
        "tsdb".getBytes(MockBase.ASCII()), false, new String[] { 
        "1356998400", "1357002000", "sum", "sys.cpu.user" });
    assertEquals(0, errors);
  }
  
  @Test
  public void dupeTimestampsSeconds() throws Exception {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x0B };
    final byte[] val2 = Bytes.fromInt(Float.floatToRawIntBits(500.8F));

    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    int errors = (Integer)fsck.invoke(null, tsdb, client, 
        "tsdb".getBytes(MockBase.ASCII()), false, new String[] { 
        "1356998400", "1357002000", "sum", "sys.cpu.user" });
    assertEquals(1, errors);
  }
  
  @Test
  public void dupeTimestampsSecondsFix() throws Exception {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x0B };
    final byte[] val2 = Bytes.fromInt(Float.floatToRawIntBits(500.8F));

    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    int errors = (Integer)fsck.invoke(null, tsdb, client, 
        "tsdb".getBytes(MockBase.ASCII()), true, new String[] { 
        "1356998400", "1357002000", "sum", "sys.cpu.user" });
    assertEquals(1, errors);
    assertEquals(1, storage.numColumns(ROW));
  }
  
  @Test
  public void dupeTimestampsMs() throws Exception {
    final byte[] qual1 = { (byte) 0xF0, 0x00, 0x02, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x02, 0x0B };
    final byte[] val2 = Bytes.fromInt(Float.floatToRawIntBits(500.8F));

    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    int errors = (Integer)fsck.invoke(null, tsdb, client, 
        "tsdb".getBytes(MockBase.ASCII()), false, new String[] { 
        "1356998400", "1357002000", "sum", "sys.cpu.user" });
    assertEquals(1, errors);
    assertEquals(2, storage.numColumns(ROW));
  }

  @Test
  public void dupeTimestampsMsFix() throws Exception {
    final byte[] qual1 = { (byte) 0xF0, 0x00, 0x02, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x02, 0x0B };
    final byte[] val2 = Bytes.fromInt(Float.floatToRawIntBits(500.8F));

    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    int errors = (Integer)fsck.invoke(null, tsdb, client, 
        "tsdb".getBytes(MockBase.ASCII()), true, new String[] { 
        "1356998400", "1357002000", "sum", "sys.cpu.user" });
    assertEquals(1, errors);
    assertEquals(1, storage.numColumns(ROW));
  }

  @Test
  public void twoCompactedColumnsWSameTS() throws Exception {
    // hopefully this never happens, but if it does, we can't fix it manually
    // easily without splitting up and re-writing the compacted cells.
    final byte[] qual1 = { 0x0, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x0, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual3 = { 0x0, 0x37 };
    final byte[] val3 = Bytes.fromLong(6L);

    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual1, qual2), 
        MockBase.concatByteArrays(val1, val2, new byte[] { 0 }));
    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual2, qual3), 
        MockBase.concatByteArrays(val2, val3, new byte[] { 0 }));
    int errors = (Integer)fsck.invoke(null, tsdb, client, 
        "tsdb".getBytes(MockBase.ASCII()), false, new String[] { 
        "1356998400", "1357002000", "sum", "sys.cpu.user" });
    assertEquals(1, errors);
  }
  
  @Test
  public void compactedWSameTS() throws Exception {
    final byte[] qual1 = { 0x0, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x0, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual3 = { 0x0, 0x37 };
    final byte[] val3 = Bytes.fromLong(6L);

    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual1, qual2, qual3), 
        MockBase.concatByteArrays(val1, val2, val3, new byte[] { 0 }));
    storage.addColumn(ROW, qual3, val3);
    int errors = (Integer)fsck.invoke(null, tsdb, client, 
        "tsdb".getBytes(MockBase.ASCII()), false, new String[] { 
        "1356998400", "1357002000", "sum", "sys.cpu.user" });
    assertEquals(1, errors);
    assertEquals(2, storage.numColumns(ROW));
  }
  
  @Test
  public void compactedMSWSameTS() throws Exception {
    final byte[] qual1 = { (byte) 0xF0, 0x00, 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x02, 0x07 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual3 = { (byte) 0xF0, 0x00, 0x04, 0x07 };
    final byte[] val3 = Bytes.fromLong(6L);

    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual1, qual2, qual3), 
        MockBase.concatByteArrays(val1, val2, val3, new byte[] { 0 }));
    storage.addColumn(ROW, qual3, val3);
    int errors = (Integer)fsck.invoke(null, tsdb, client, 
        "tsdb".getBytes(MockBase.ASCII()), false, new String[] { 
        "1356998400", "1357002000", "sum", "sys.cpu.user" });
    assertEquals(1, errors);
    assertEquals(2, storage.numColumns(ROW));
  }
  
  @Test
  public void compactedWSameTSFix() throws Exception {
    final byte[] qual1 = { 0x0, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x0, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual3 = { 0x0, 0x37 };
    final byte[] val3 = Bytes.fromLong(6L);

    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual1, qual2, qual3), 
        MockBase.concatByteArrays(val1, val2, val3, new byte[] { 0 }));
    storage.addColumn(ROW, qual3, val3);
    int errors = (Integer)fsck.invoke(null, tsdb, client, 
        "tsdb".getBytes(MockBase.ASCII()), true, new String[] { 
        "1356998400", "1357002000", "sum", "sys.cpu.user" });
    assertEquals(1, errors);
    assertEquals(1, storage.numColumns(ROW));
  }
  
  @Test
  public void compactedMSWSameTSFix() throws Exception {
    final byte[] qual1 = { 0x0, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x0, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual3 = { 0x0, 0x37 };
    final byte[] val3 = Bytes.fromLong(6L);

    storage.addColumn(ROW, 
        MockBase.concatByteArrays(qual1, qual2, qual3), 
        MockBase.concatByteArrays(val1, val2, val3, new byte[] { 0 }));
    storage.addColumn(ROW, qual3, val3);
    int errors = (Integer)fsck.invoke(null, tsdb, client, 
        "tsdb".getBytes(MockBase.ASCII()), true, new String[] { 
        "1356998400", "1357002000", "sum", "sys.cpu.user" });
    assertEquals(1, errors);
    assertEquals(1, storage.numColumns(ROW));
  }
}
