// This file is part of OpenTSDB.
// Copyright (C) 2014  The OpenTSDB Authors.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;

import net.opentsdb.core.TSDB;
import net.opentsdb.meta.Annotation;
import net.opentsdb.storage.MockBase;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.Config;

import org.hbase.async.Bytes;
import org.hbase.async.DeleteRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.Scanner;
import org.junit.After;
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
  GetRequest.class, PutRequest.class, KeyValue.class, DumpSeries.class,
  Scanner.class, DeleteRequest.class, Annotation.class })
public class TestDumpSeries {
  private Config config;
  private TSDB tsdb = null;
  private HBaseClient client = mock(HBaseClient.class);
  private UniqueId metrics = mock(UniqueId.class);
  private UniqueId tag_names = mock(UniqueId.class);
  private UniqueId tag_values = mock(UniqueId.class);
  private MockBase storage;
  private ByteArrayOutputStream buffer;
  // the simplest way to test is to capture the System.out.print() data so we
  // need to capture a reference to the original stdout stream here and reset
  // it after each test so a failed unit test doesn't block stdout for 
  // subsequent tests.
  private final PrintStream stdout = System.out;
  
  private final static Method doDump;
  static {
    try {
      doDump = DumpSeries.class.getDeclaredMethod("doDump", TSDB.class, 
          HBaseClient.class, byte[].class, boolean.class, boolean.class, 
          String[].class);
      doDump.setAccessible(true);
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
    
    buffer = new ByteArrayOutputStream();
    System.setOut(new PrintStream(buffer));
    
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
    when(metrics.getNameAsync(new byte[] { 0, 0, 1 })).thenReturn(Deferred.fromResult("sys.cpu.user"));
    when(metrics.getId("sys.cpu.system"))
      .thenThrow(new NoSuchUniqueName("sys.cpu.system", "metric"));
    when(metrics.getId("sys.cpu.nice")).thenReturn(new byte[] { 0, 0, 2 });
    when(metrics.getNameAsync(new byte[] { 0, 0, 2 })).thenReturn(Deferred.fromResult("sys.cpu.nice"));
    when(tag_names.getId("host")).thenReturn(new byte[] { 0, 0, 1 });
    when(tag_names.getNameAsync(new byte[] { 0, 0, 1 })).thenReturn(Deferred.fromResult("host"));
    when(tag_names.getOrCreateId("host")).thenReturn(new byte[] { 0, 0, 1 });
    when(tag_names.getId("dc")).thenThrow(new NoSuchUniqueName("dc", "metric"));
    when(tag_values.getId("web01")).thenReturn(new byte[] { 0, 0, 1 });
    when(tag_values.getNameAsync(new byte[] { 0, 0, 1 })).thenReturn(Deferred.fromResult("web01"));
    when(tag_values.getOrCreateId("web01")).thenReturn(new byte[] { 0, 0, 1 });
    when(tag_values.getId("web02")).thenReturn(new byte[] { 0, 0, 2 });
    when(tag_values.getNameAsync(new byte[] { 0, 0, 2 })).thenReturn(Deferred.fromResult("web02"));
    when(tag_values.getOrCreateId("web02")).thenReturn(new byte[] { 0, 0, 2 });
    when(tag_values.getId("web03"))
      .thenThrow(new NoSuchUniqueName("web03", "metric"));
    
    when(metrics.width()).thenReturn((short)3);
    when(tag_names.width()).thenReturn((short)3);
    when(tag_values.width()).thenReturn((short)3);
  }
  
  @After
  public void after() {
    System.setOut(stdout);
  }
  
  @Test
  public void dumpRaw() throws Exception {
    writeData();    
    doDump.invoke(null, tsdb, client, "tsdb".getBytes(MockBase.ASCII()), false, 
        false, new String[] { "1356998400", "1357002000", "sum", "sys.cpu.user" });
    final String[] log_lines = buffer.toString("ISO-8859-1").split("\n");
    assertNotNull(log_lines);
    // only worry about the immutable. The human readable date format
    // differs per location.
    assertEquals("[0, 0, 1, 80, -30, 39, 0, 0, 0, 1, 0, 0, 1] sys.cpu.user 1356998400", 
        log_lines[0].substring(0, 67));
    assertEquals(
        "  [0, 0]\t[42]\t0\tl\t1356998400",
        log_lines[1].substring(0, 28));
    assertEquals(
        "  [0, 17]\t[1, 1]\t1\tl\t1356998401",
        log_lines[2].substring(0, 31));
    assertEquals(
        "  [0, 35]\t[0, 1, 0, 1]\t2\tl\t1356998402",
        log_lines[3].substring(0, 37));
    assertEquals(
        "  [0, 55]\t[0, 0, 0, 1, 0, 0, 0, 0]\t3\tl\t1356998403",
        log_lines[4].substring(0, 49));
    assertEquals(
        "  [0, 75]\t[66, 42, 0, 0]\t4\tf\t1356998404",
        log_lines[5].substring(0, 39));
    assertEquals(
        "  [0, 91]\t[66, 42, 12, -92]\t5\tf\t1356998405",
        log_lines[6].substring(0, 42));
    assertEquals(
        "  [1, 0, 0]\t[123, 34, 116, 115, 117, 105, 100, 34, 58, 34, "
        + "48, 48, 48, 48, 48, 49, 48, 48, 48, 48, 48, 49, 48, 48, 48, 48, 48, "
        + "49, 34, 44, 34, 115, 116, 97, 114, 116, 84, 105, 109, 101, 34, 58, "
        + "49, 51, 53, 54, 57, 57, 56, 52, 48, 48, 44, 34, 101, 110, 100, 84, "
        + "105, 109, 101, 34, 58, 48, 44, 34, 100, 101, 115, 99, 114, 105, "
        + "112, 116, 105, 111, 110, 34, 58, 34, 65, 110, 110, 111, 116, 97, "
        + "116, 105, 111, 110, 32, 111, 110, 32, 115, 101, 99, 111, 110, 100, "
        + "115, 34, 44, 34, 110, 111, 116, 101, 115, 34, 58, 34, 34, 44, 34, "
        + "99, 117, 115, 116, 111, 109, 34, 58, 110, 117, 108, 108, 125]\t0\t"
        + "{\"tsuid\":\"000001000001000001\",\"startTime\":1356998400,"
        + "\"endTime\":0,\"description\":\"Annotation on seconds\","
        + "\"notes\":\"\",\"custom\":null}\t1356998416000",
        log_lines[7].substring(0, 729));
    assertEquals(
        "[0, 0, 1, 80, -30, 53, 16, 0, 0, 1, 0, 0, 1] sys.cpu.user 1357002000",
        log_lines[8].substring(0, 68));
    assertEquals(
        "  [1, 0, 0]\t[123, 34, 116, 115, 117, 105, 100, "
        + "34, 58, 34, 48, 48, 48, 48, 48, 49, 48, 48, 48, 48, 48, 49, 48, 48, "
        + "48, 48, 48, 49, 34, 44, 34, 115, 116, 97, 114, 116, 84, 105, 109, "
        + "101, 34, 58, 49, 51, 53, 55, 48, 48, 50, 48, 48, 48, 48, 48, 48, "
        + "44, 34, 101, 110, 100, 84, 105, 109, 101, 34, 58, 48, 44, 34, 100, "
        + "101, 115, 99, 114, 105, 112, 116, 105, 111, 110, 34, 58, 34, 65, "
        + "110, 110, 111, 116, 97, 116, 105, 111, 110, 32, 111, 110, 32, 109, "
        + "105, 108, 108, 105, 115, 101, 99, 111, 110, 100, 115, 34, 44, 34, "
        + "110, 111, 116, 101, 115, 34, 58, 34, 34, 44, 34, 99, 117, 115, 116, "
        + "111, 109, 34, 58, 110, 117, 108, 108, 125]\t0\t{\"tsuid\":"
        + "\"000001000001000001\",\"startTime\":1357002000000,\"endTime\":0,"
        + "\"description\":\"Annotation on milliseconds\",\"notes\":\"\","
        + "\"custom\":null}\t1357002016000",
        log_lines[9].substring(0, 774));
    assertEquals(
        "  [-16, 0, 0, 0]\t[42]\t0\tl\t1357002000000",
        log_lines[10].substring(0, 39));
    assertEquals(
        "  [-16, 0, -6, 1]\t[1, 1]\t1000\tl\t1357002001000",
        log_lines[11].substring(0, 45));
    assertEquals(
        "  [-16, 1, -12, 3]\t[0, 1, 0, 1]\t2000\tl"
        + "\t1357002002000",
        log_lines[12].substring(0, 52));
    assertEquals(
        "  [-16, 2, -18, 7]\t[0, 0, 0, 1, 0, 0, 0, 0]\t3000"
        + "\tl\t1357002003000",
        log_lines[13].substring(0, 64));
    assertEquals(
        "  [-16, 3, -24, 11]\t[66, 42, 0, 0]\t4000\tf\t"
        + "1357002004000",
        log_lines[14].substring(0, 55));
    assertEquals(
        "  [-16, 4, -30, 11]\t[66, 42, 12, -92]\t5000\tf\t"
        + "1357002005000",
        log_lines[15].substring(0, 58));
  }
  
  @Test
  public void dumpImport() throws Exception {
    writeData();
    doDump.invoke(null, tsdb, client, "tsdb".getBytes(MockBase.ASCII()), false, 
        true, new String[] { "1356998400", "1357002000", "sum", "sys.cpu.user" });
    final String[] log_lines = buffer.toString("ISO-8859-1").split("\n");
    assertNotNull(log_lines);
    assertEquals("sys.cpu.user 1356998400 42 host=web01", log_lines[0]);
    assertEquals("sys.cpu.user 1356998401 257 host=web01", log_lines[1]);
    assertEquals("sys.cpu.user 1356998402 65537 host=web01", log_lines[2]);
    assertEquals("sys.cpu.user 1356998403 4294967296 host=web01", log_lines[3]);
    assertEquals("sys.cpu.user 1356998404 42.5 host=web01", log_lines[4]);
    assertEquals("sys.cpu.user 1356998405 42.51234436035156 host=web01",
        log_lines[5]);
    assertEquals("sys.cpu.user 1357002000000 42 host=web01", log_lines[6]);
    assertEquals("sys.cpu.user 1357002001000 257 host=web01", log_lines[7]);
    assertEquals("sys.cpu.user 1357002002000 65537 host=web01", log_lines[8]);
    assertEquals("sys.cpu.user 1357002003000 4294967296 host=web01",
        log_lines[9]);
    assertEquals("sys.cpu.user 1357002004000 42.5 host=web01", log_lines[10]);
    assertEquals("sys.cpu.user 1357002005000 42.51234436035156 host=web01",
        log_lines[11]);
  }

  @Test
  public void dumpRawAndDelete() throws Exception {
    writeData();    
    doDump.invoke(null, tsdb, client, "tsdb".getBytes(MockBase.ASCII()), true, 
        false, new String[] { "1356998400", "1357002000", "sum", "sys.cpu.user" });
    final String[] log_lines = buffer.toString("ISO-8859-1").split("\n");
    assertNotNull(log_lines);
    assertEquals(16, log_lines.length);
    assertEquals(-1, storage.numColumns(
        MockBase.stringToBytes("00000150E22700000001000001")));
    assertEquals(-1, storage.numColumns(
        MockBase.stringToBytes("00000150E23510000001000001")));
  }
  
  @Test
  public void dumpImportAndDelete() throws Exception {
    writeData();    
    doDump.invoke(null, tsdb, client, "tsdb".getBytes(MockBase.ASCII()), true, 
        true, new String[] { "1356998400", "1357002000", "sum", "sys.cpu.user" });
    final String[] log_lines = buffer.toString("ISO-8859-1").split("\n");
    assertNotNull(log_lines);
    assertEquals(12, log_lines.length);
    assertEquals(-1, storage.numColumns(
        MockBase.stringToBytes("00000150E22700000001000001")));
    assertEquals(-1, storage.numColumns(
        MockBase.stringToBytes("00000150E23510000001000001")));
  }
  
  @Test
  public void dumpRawCompacted() throws Exception {
    writeCompactedData();
    doDump.invoke(null, tsdb, client, "tsdb".getBytes(MockBase.ASCII()), false, 
        false, new String[] { "1356998400", "1357002000", "sum", "sys.cpu.user" });
    final String[] log_lines = buffer.toString("ISO-8859-1").split("\n");
    assertNotNull(log_lines);
    // only worry about the immutable. The human readable date format
    // differs per location.
    assertEquals(
        "[0, 0, 1, 80, -30, 39, 0, 0, 0, 1, 0, 0, 1] sys.cpu.user 1356998400", 
        log_lines[0].substring(0, 67));
    assertEquals(
        "  [-16, 0, 0, 7, -16, 0, 2, 7, -16, 0, 1, 7]\t[0, 0, 0, 0, 0, 0, 0, "
        + "4, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 6, 0] = 3 values:", 
        log_lines[1]);
    assertEquals(
        "    [-16, 0, 0, 7]\t[0, 0, 0, 0, 0, 0, 0, 4]\t0\tl\t1356998400000", 
        log_lines[2].substring(0, 61));
    assertEquals(
        "    [-16, 0, 1, 7]\t[0, 0, 0, 0, 0, 0, 0, 6]\t4\tl\t1356998400004", 
        log_lines[3].substring(0, 61));
    assertEquals(
        "    [-16, 0, 2, 7]\t[0, 0, 0, 0, 0, 0, 0, 5]\t8\tl\t1356998400008", 
        log_lines[4].substring(0, 61));
  }

  @Test
  public void dumpImportCompacted() throws Exception {
    writeCompactedData();
    doDump.invoke(null, tsdb, client, "tsdb".getBytes(MockBase.ASCII()), false, 
        true, new String[] { "1356998400", "1357002000", "sum", "sys.cpu.user" });
    final String[] log_lines = buffer.toString("ISO-8859-1").split("\n");
    assertNotNull(log_lines);
    // only worry about the immutable. The human readable date format
    // differs per location.
    assertEquals("sys.cpu.user 1356998400000 4 host=web01", log_lines[0]);
    assertEquals("sys.cpu.user 1356998400004 6 host=web01", log_lines[1]);
    assertEquals("sys.cpu.user 1356998400008 5 host=web01", log_lines[2]);
  }
  
  @Test
  public void dumpRawCompactedAndDelete() throws Exception {
    writeCompactedData();
    doDump.invoke(null, tsdb, client, "tsdb".getBytes(MockBase.ASCII()), true, 
        false, new String[] { "1356998400", "1357002000", "sum", "sys.cpu.user" });
    final String[] log_lines = buffer.toString("ISO-8859-1").split("\n");
    assertNotNull(log_lines);
    assertEquals(5, log_lines.length);
    assertEquals(-1, storage.numColumns(
        MockBase.stringToBytes("00000150E22700000001000001")));
  }
  
  @Test
  public void dumpImportCompactedAndDelete() throws Exception {
    writeCompactedData();
    doDump.invoke(null, tsdb, client, "tsdb".getBytes(MockBase.ASCII()), true, 
        true, new String[] { "1356998400", "1357002000", "sum", "sys.cpu.user" });
    final String[] log_lines = buffer.toString("ISO-8859-1").split("\n");
    assertNotNull(log_lines);
    assertEquals(3, log_lines.length);
    assertEquals(-1, storage.numColumns(
        MockBase.stringToBytes("00000150E22700000001000001")));
  }
  
  /**
   * Store some data in MockBase for use in the unit tests. We'll put in a mix
   * of all possible types so that we know they'll come out properly in the end.
   * For that reason we'll use the standard OpenTSDB methods for writing data.
   */
  private void writeData() throws Exception {
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    long timestamp = 1356998400;
    
    Annotation annotation = new Annotation();
    annotation.setStartTime(timestamp);
    annotation.setTSUID("000001000001000001");
    annotation.setDescription("Annotation on seconds");
    annotation.syncToStorage(tsdb, false).joinUninterruptibly();
    
    tsdb.addPoint("sys.cpu.user", timestamp++, 42, tags).joinUninterruptibly();
    tsdb.addPoint("sys.cpu.user", timestamp++, 257, tags).joinUninterruptibly();
    tsdb.addPoint("sys.cpu.user", timestamp++, 65537, tags).joinUninterruptibly();
    tsdb.addPoint("sys.cpu.user", timestamp++, 4294967296L, tags).joinUninterruptibly();
    tsdb.addPoint("sys.cpu.user", timestamp++, 42.5F, tags).joinUninterruptibly();
    tsdb.addPoint("sys.cpu.user", timestamp++, 42.5123459999F, tags)
      .joinUninterruptibly();
    
    timestamp = 1357002000000L;
    
    annotation = new Annotation();
    annotation.setStartTime(timestamp);
    annotation.setTSUID("000001000001000001");
    annotation.setDescription("Annotation on milliseconds");
    annotation.syncToStorage(tsdb, false).joinUninterruptibly();
    
    tsdb.addPoint("sys.cpu.user", timestamp, 42, tags).joinUninterruptibly();
    tsdb.addPoint("sys.cpu.user", timestamp += 1000, 257, tags).joinUninterruptibly();
    tsdb.addPoint("sys.cpu.user", timestamp += 1000, 65537, tags).joinUninterruptibly();
    tsdb.addPoint("sys.cpu.user", timestamp += 1000, 4294967296L, tags).joinUninterruptibly();
    tsdb.addPoint("sys.cpu.user", timestamp += 1000, 42.5F, tags).joinUninterruptibly();
    tsdb.addPoint("sys.cpu.user", timestamp += 1000, 42.5123459999F, tags)
      .joinUninterruptibly();
  }
  
  /**
   * Store a compacted cell in a row so that we can verify the proper raw dump
   * format and that the --import flag will parse it correctly.
   */
  private void writeCompactedData() throws Exception {
    final byte[] qual1 = { (byte) 0xF0, 0x00, 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x02, 0x07 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual3 = { (byte) 0xF0, 0x00, 0x01, 0x07 };
    final byte[] val3 = Bytes.fromLong(6L);
    storage.addColumn(MockBase.stringToBytes("00000150E22700000001000001"), 
        "t".getBytes(MockBase.ASCII()), 
        MockBase.concatByteArrays(qual1, qual2, qual3),
        MockBase.concatByteArrays(val1, val2, val3, new byte[] { 0 }));
//    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
//    kvs.add(makekv(qual12, MockBase.concatByteArrays(val1, val2, ZERO)));

  }
}
