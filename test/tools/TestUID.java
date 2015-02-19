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

import static net.opentsdb.core.StringCoder.toBytes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;

import java.lang.reflect.Method;

import dagger.ObjectGraph;
import net.opentsdb.TestModuleMemoryStore;
import net.opentsdb.storage.MemoryStore;
import net.opentsdb.storage.TsdbStore;

import net.opentsdb.storage.hbase.HBaseStore;
import org.hbase.async.Bytes;
import org.hbase.async.KeyValue;
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
@PrepareForTest({KeyValue.class, Scanner.class})
public class TestUID {
  private MemoryStore tsdb_store;
  
  // names used for testing
  private byte[] NAME_FAMILY = toBytes("name");
  private byte[] ID_FAMILY = toBytes("id");
  private byte[] METRICS = toBytes("metrics");
  private byte[] TAGK = toBytes("tagk");
  private byte[] TAGV = toBytes("tagv");

  private static final Method fsck;
  static {
    try {
      fsck = UidManager.class.getDeclaredMethod("fsck", HBaseStore.class,
          byte[].class, boolean.class, boolean.class);
      fsck.setAccessible(true);
    } catch (Exception e) {
      throw new RuntimeException("Failed in static initializer", e);
    }
  }
  
  @Before
  public void before() throws Exception {
    ObjectGraph objectGraph = ObjectGraph.create(new TestModuleMemoryStore());
    tsdb_store = objectGraph.get(MemoryStore.class);

    // replace the "real" field objects with mocks
//    Field cl = tsdb.getClass().getDeclaredField("tsdb_store");
//    cl.setAccessible(true);
//    cl.set(tsdb, tsdb_store);
//    
//    Field met = tsdb.getClass().getDeclaredField("metrics");
//    met.setAccessible(true);
//    met.set(tsdb, metrics);
//    
//    Field tagk = tsdb.getClass().getDeclaredField("tag_names");
//    tagk.setAccessible(true);
//    tagk.set(tsdb, tag_names);
//    
//    Field tagv = tsdb.getClass().getDeclaredField("tag_values");
//    tagv.setAccessible(true);
//    tagv.set(tsdb, tag_values);
//    
//    // mock UniqueId
//    when(metrics.getId("sys.cpu.user")).thenReturn(new byte[] {0, 0, 1 });
//    when(metrics.getName(new byte[] {0, 0, 1 })).thenReturn("sys.cpu.user");
//    when(metrics.getId("sys.cpu.system"))
//      .thenThrow(new NoSuchUniqueName("sys.cpu.system", "metric"));
//    when(metrics.getId("sys.cpu.nice")).thenReturn(new byte[] {0, 0, 2 });
//    when(metrics.getName(new byte[] {0, 0, 2 })).thenReturn("sys.cpu.nice");
//    when(tag_names.getId("host")).thenReturn(new byte[] {0, 0, 1 });
//    when(tag_names.getName(new byte[] {0, 0, 1 })).thenReturn("host");
//    when(tag_names.getOrCreateId("host")).thenReturn(new byte[] {0, 0, 1 });
//    when(tag_names.getId("dc")).thenThrow(new NoSuchUniqueName("dc", "metric"));
//    when(tag_values.getId("web01")).thenReturn(new byte[] {0, 0, 1 });
//    when(tag_values.getName(new byte[] {0, 0, 1 })).thenReturn("web01");
//    when(tag_values.getOrCreateId("web01")).thenReturn(new byte[] {0, 0, 1 });
//    when(tag_values.getId("web02")).thenReturn(new byte[] {0, 0, 2 });
//    when(tag_values.getName(new byte[] {0, 0, 2 })).thenReturn("web02");
//    when(tag_values.getOrCreateId("web02")).thenReturn(new byte[] {0, 0, 2 });
//    when(tag_values.getId("web03"))
//      .thenThrow(new NoSuchUniqueName("web03", "metric"));
//    
//    when(metrics.width()).thenReturn((short)3);
//    when(tag_names.width()).thenReturn((short)3);
//    when(tag_values.width()).thenReturn((short)3);
    PowerMockito.spy(System.class);
    PowerMockito.when(System.nanoTime())
      .thenReturn(1357300800000000L)
      .thenReturn(1357300801000000L)
      .thenReturn(1357300802000000L)
      .thenReturn(1357300803000000L);
    PowerMockito.when(System.currentTimeMillis())
      .thenReturn(1357300800000L)
      .thenReturn(1357300801000L)
      .thenReturn(1357300802000L)
      .thenReturn(1357300803000L);
  }
  
  /* FSCK --------------------------------------------
   * The UID FSCK is concerned with making sure the UID table is in a clean state.
   * Most important are the forward mappings to UIDs as that's what's used to
   * store time series. 
   * A clean table is below:
   * ---------------------
   * REVERSE   |  FORWARD
   * ---------------------
   * 01 -> foo   foo -> 01
   * 02 -> bar   bar -> 02
   * 
   * The reverse map will have a unique set of UIDs but could, in error, have
   * duplicate names.
   * 
   * The forward map will have a unique set of names but could, in error, have
   * duplicate IDs.
   * 
   * Order of error checking is to loop through the FORWARD map first, then
   * the REVERSE map. Then for each it will check the other map for entries
   */
  
  @Test
  public void fsckNoData() throws Exception {
    setupMockBase();
    tsdb_store.flushStorage();
    int errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), false, false);
    assertEquals(0, errors);
  }
  
  @Test
  public void fsckNoErrors() throws Exception {
    setupMockBase();
    int errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), false, false);
    assertEquals(0, errors);
  }
  
  /*
   * Max UID row values that are higher than the largest assigned UID for their 
   * type are OK and we just warn on them. This is usually caused by a user 
   * removing a name that they no longer need.
   */ 
  @Test
  public void fsckMetricsUIDHigh() throws Exception {
    // currently a warning, not an error
    setupMockBase();
    tsdb_store.addColumn(new byte[]{0}, ID_FAMILY, METRICS, Bytes.fromLong(42L));
    int errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), false, false);
    assertEquals(0, errors);
  }
  
  @Test
  public void fsckTagkUIDHigh() throws Exception {
    // currently a warning, not an error
    setupMockBase();
    tsdb_store.addColumn(new byte[]{0}, ID_FAMILY, TAGK, Bytes.fromLong(42L));
    int errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), false, false);
    assertEquals(0, errors);
  }
  
  @Test
  public void fsckTagvUIDHigh() throws Exception {
    // currently a warning, not an error
    setupMockBase();
    tsdb_store.addColumn(new byte[]{0}, ID_FAMILY, TAGV, Bytes.fromLong(42L));
    int errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), false, false);
    assertEquals(0, errors);
  }
  
  /*
   * Max UID row values that are lower than the largest assigned UID for their 
   * type can be fixed by simply setting the max ID to the largest found UID.
   */  
  @Test
  public void fsckMetricsUIDLow() throws Exception {
    // currently a warning, not an error
    setupMockBase();
    tsdb_store.addColumn(new byte[]{0}, ID_FAMILY, METRICS, Bytes.fromLong(1L));
    int errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), false, false);
    assertEquals(1, errors);
  }
  
  @Test
  public void fsckFIXMetricsUIDLow() throws Exception {
    // currently a warning, not an error
    setupMockBase();
    tsdb_store.addColumn(new byte[]{0}, ID_FAMILY, METRICS, Bytes.fromLong(0L));
    int errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), true, false);
    assertEquals(1, errors);
    errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), false, false);
    assertEquals(0, errors);
  }
  
  @Test
  public void fsckTagkUIDLow() throws Exception {
    setupMockBase();
    tsdb_store.addColumn(new byte[]{0}, ID_FAMILY, TAGK, Bytes.fromLong(1L));
    int errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), false, false);
    assertEquals(1, errors);
  }
  
  @Test
  public void fsckFIXTagkUIDLow() throws Exception {
    setupMockBase();
    tsdb_store.addColumn(new byte[]{0}, ID_FAMILY, TAGK, Bytes.fromLong(1L));
    int errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), true, false);
    assertEquals(1, errors);
    errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), false, false);
    assertEquals(0, errors);
  }
  
  @Test
  public void fsckTagvUIDLow() throws Exception {
    setupMockBase();
    tsdb_store.addColumn(new byte[]{0}, ID_FAMILY, TAGV, Bytes.fromLong(1L));
    int errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), false, false);
    assertEquals(1, errors);
  }
  
  @Test
  public void fsckFIXTagvUIDLow() throws Exception {
    setupMockBase();
    tsdb_store.addColumn(new byte[]{0}, ID_FAMILY, TAGV, Bytes.fromLong(1L));
    int errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), true, false);
    assertEquals(1, errors);
    errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), false, false);
    assertEquals(0, errors);
  }
  
  /*
   * Max UID row values that are != 8 bytes wide are bizzare.
   * TODO - a fix would be to find the max used ID for the type and store that
   * in the max row.
   */
  @Test
  public void fsckMetricsUIDWrongLength() throws Exception {
    setupMockBase();
    tsdb_store.addColumn(new byte[]{0}, ID_FAMILY, METRICS, Bytes.fromInt(3));
    int errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), false, false);
    assertEquals(2, errors);
  }
  
  @Test
  public void fsckTagkUIDWrongLength() throws Exception {
    setupMockBase();
    tsdb_store.addColumn(new byte[]{0}, ID_FAMILY, TAGK, Bytes.fromInt(3));
    int errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), false, false);
    assertEquals(2, errors);
  }
  
  @Test
  public void fsckTagvUIDWrongLength() throws Exception {
    setupMockBase();
    tsdb_store.addColumn(new byte[]{0}, ID_FAMILY, TAGV, Bytes.fromInt(3));
    int errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), false, false);
    assertEquals(2, errors);
  }
  
  /* #1 - Missing Reverse Mapping
   * - Forward mapping is missing reverse: bar -> 02
   * ---------------------
   * 01 -> foo   foo -> 01
   *             bar -> 02
   * 
   * FIX - Restore reverse map 02 -> bar. OK since 02 doesn't map to anything
   */
  @Test
  public void fsckMetricsMissingReverse() throws Exception {
    setupMockBase();
    tsdb_store.flushColumn(new byte[]{0, 0, 1}, NAME_FAMILY, METRICS);
    int errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), false, false);
    assertEquals(1, errors);
  }
  
  @Test
  public void fsckFIXMetricsMissingReverse() throws Exception {
    setupMockBase();
    tsdb_store.flushColumn(new byte[]{0, 0, 1}, NAME_FAMILY, METRICS);
    int errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), true, false);
    assertEquals(1, errors);
    assertArrayEquals(toBytes("foo"),
      tsdb_store.getColumn(new byte[]{0, 0, 1}, NAME_FAMILY, METRICS));
    errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), false, false);
    assertEquals(0, errors);
  }
  
  @Test
  public void fsckTagkMissingReverse() throws Exception {
    setupMockBase();
    tsdb_store.flushColumn(new byte[]{0, 0, 1}, NAME_FAMILY, TAGK);
    int errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), false, false);
    assertEquals(1, errors);
  }
  
  @Test
  public void fsckFIXTagkMissingReverse() throws Exception {
    setupMockBase();
    tsdb_store.flushColumn(new byte[]{0, 0, 1}, NAME_FAMILY, TAGK);
    int errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), true, false);
    assertEquals(1, errors);
    assertArrayEquals(toBytes("host"),
      tsdb_store.getColumn(new byte[]{0, 0, 1}, NAME_FAMILY, TAGK));
    errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), false, false);
    assertEquals(0, errors);
  }
  
  @Test
  public void fsckTagvMissingReverse() throws Exception {
    setupMockBase();
    tsdb_store.flushColumn(new byte[]{0, 0, 1}, NAME_FAMILY, TAGV);
    int errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), false, false);
    assertEquals(1, errors);
  }
  
  @Test
  public void fsckFIXTagvMissingReverse() throws Exception {
    setupMockBase();
    tsdb_store.flushColumn(new byte[]{0, 0, 1}, NAME_FAMILY, TAGV);
    int errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), true, false);
    assertEquals(1, errors);
    assertArrayEquals(toBytes("web01"),
      tsdb_store.getColumn(new byte[]{0, 0, 1}, NAME_FAMILY, TAGV));
    errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), false, false);
    assertEquals(0, errors);
  }
  
  /* #2 - Inconsistent Forward where a name points to a previously assigned UID
   * THIS SHOULD NOT HAPPEN unless there's a major but or someone was messing with
   * the UID table.
   * - Forward mapping wtf -> 01 is diff than reverse 01 -> foo
   * - Inconsistent forward mapping wtf -> 01 vs wtf -> foo / foo -> 01
   * --------------------
   * 01 -> foo   foo -> 01
   * 02 -> bar   bar -> 02
   *             wtf -> 01
   *             ^^^^^^^^^
   * FIX - Since any time series with the "01" UID is now corrupted with data from
   * both foo and wtf, the best solution is to just delete the forward maps for
   * foo and wtf, then create a new name map of "fsck.foo.wtf -> 01" so that the
   * old time series are still accessible.
   */
  @Test
  public void fsckMetricsInconsistentForward() throws Exception {
    setupMockBase();
    tsdb_store.addColumn(toBytes("wtf"), ID_FAMILY,
      METRICS, new byte[]{0, 0, 1});
    tsdb_store.addColumn(new byte[]{0}, ID_FAMILY, METRICS, Bytes.fromLong(3L));
    int errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), false, false);
    assertEquals(2, errors);
  }
  
  @Test
  public void fsckFIXMetricsInconsistentForward() throws Exception {
    setupMockBase();
    tsdb_store.addColumn(toBytes("wtf"), ID_FAMILY,
      METRICS, new byte[]{0, 0, 1});
    tsdb_store.addColumn(new byte[]{0}, ID_FAMILY, METRICS, Bytes.fromLong(3L));
    int errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), true, false);
    assertEquals(2, errors);
    assertArrayEquals(toBytes("fsck.foo.wtf"),
      tsdb_store.getColumn(new byte[]{0, 0, 1}, NAME_FAMILY, METRICS));
    assertNull(tsdb_store.getColumn(toBytes("foo"), ID_FAMILY,
        METRICS));
    assertNull(tsdb_store.getColumn(toBytes("wtf"), ID_FAMILY,
      METRICS));
    errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), false, false);
    assertEquals(0, errors);
  }

  @Test
  public void fsckTagkInconsistentForward() throws Exception {
    setupMockBase();
    tsdb_store.addColumn(toBytes("some.other.value"), ID_FAMILY,
      TAGK, new byte[]{0, 0, 1});
    tsdb_store.addColumn(new byte[]{0}, ID_FAMILY, TAGK, Bytes.fromLong(3L));
    int errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), false, false);
    assertEquals(2, errors);
  }
  
  @Test
  public void fsckFIXTagkInconsistentForward() throws Exception {
    setupMockBase();
    tsdb_store.addColumn(toBytes("wtf"), ID_FAMILY,
      TAGK, new byte[]{0, 0, 1});
    tsdb_store.addColumn(new byte[]{0}, ID_FAMILY, TAGK, Bytes.fromLong(3L));
    int errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), true, false);
    assertEquals(2, errors);
    assertArrayEquals(toBytes("fsck.host.wtf"),
      tsdb_store.getColumn(new byte[]{0, 0, 1}, NAME_FAMILY, TAGK));
    assertNull(tsdb_store.getColumn(toBytes("host"), ID_FAMILY,
        TAGK));
    assertNull(tsdb_store.getColumn(toBytes("wtf"), ID_FAMILY,
      TAGK));
    errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), false, false);
    assertEquals(0, errors);
  }
  
  @Test
  public void fsckTagvInconsistentForward() throws Exception {
    setupMockBase();
    tsdb_store.addColumn(toBytes("some.other.value"), ID_FAMILY,
      TAGV, new byte[]{0, 0, 1});
    tsdb_store.addColumn(new byte[]{0}, ID_FAMILY, TAGV, Bytes.fromLong(3L));
    int errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), false, false);
    assertEquals(2, errors);
  }
  
  @Test
  public void fsckFIXTagvInconsistentForward() throws Exception {
    setupMockBase();
    tsdb_store.addColumn(toBytes("wtf"), ID_FAMILY,
      TAGV, new byte[]{0, 0, 1});
    tsdb_store.addColumn(new byte[]{0}, ID_FAMILY, TAGV, Bytes.fromLong(3L));
    int errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), true, false);
    assertEquals(2, errors);
    assertArrayEquals(toBytes("fsck.web01.wtf"),
      tsdb_store.getColumn(new byte[]{0, 0, 1}, NAME_FAMILY, TAGV));
    assertNull(tsdb_store.getColumn(toBytes("web01"), ID_FAMILY,
        TAGV));
    assertNull(tsdb_store.getColumn(toBytes("wtf"), ID_FAMILY,
      TAGV));
    errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), false, false);
    assertEquals(0, errors);
  }
  
  /* #3 - Duplicate Forward (really inconsistent) where the reverse map dosen't 
   * match the name of the forward map.
   * - Forward mapping bar -> 02 is diff than reverse: 02 -> wtf
   * - Duplicate forward mapping bar -> 02 and null -> wtf
   * - Reverse mapping missing forward mapping bar -> 02
   * ---------------------
   * 01 -> foo   foo -> 01
   * 02 -> wtf   bar -> 02
   *       ^^^
   * FIX - Restore reverse map 02 -> bar. wtf may have been deleted. It will be
   * reassigned the next time it's written.
   */
  @Test
  public void fsckMetricsDuplicateForward() throws Exception {
    setupMockBase();
    tsdb_store.addColumn(new byte[]{0, 0, 2}, NAME_FAMILY,
      METRICS, toBytes("wtf"));
    int errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), false, false);
    assertEquals(2, errors);
  }
  
  @Test
  public void fsckFIXMetricsDuplicateForward() throws Exception {
    setupMockBase();
    tsdb_store.addColumn(new byte[]{0, 0, 2}, NAME_FAMILY,
      METRICS, toBytes("wtf"));
    int errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), true, false);
    assertEquals(2, errors);
    assertArrayEquals(toBytes("bar"),
      tsdb_store.getColumn(new byte[]{0, 0, 2}, NAME_FAMILY, METRICS));
    errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), false, false);
    assertEquals(0, errors);
  }
  
  @Test
  public void fsckTagkDuplicateForward() throws Exception {
    setupMockBase();
    tsdb_store.addColumn(new byte[]{0, 0, 2}, NAME_FAMILY,
      TAGK, toBytes("wtf"));
    int errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), false, false);
    assertEquals(2, errors);
  }
  
  @Test
  public void fsckFIXTagkDuplicateForward() throws Exception {
    setupMockBase();
    tsdb_store.addColumn(new byte[]{0, 0, 2}, NAME_FAMILY,
      TAGK, toBytes("wtf"));
    int errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), true, false);
    assertEquals(2, errors);
    assertArrayEquals(toBytes("dc"),
      tsdb_store.getColumn(new byte[]{0, 0, 2}, NAME_FAMILY, TAGK));
    errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), false, false);
    assertEquals(0, errors);
  }
  
  @Test
  public void fsckTagvDuplicateForward() throws Exception {
    setupMockBase();
    tsdb_store.addColumn(new byte[]{0, 0, 2}, NAME_FAMILY,
      TAGV, toBytes("wtf"));
    int errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), false, false);
    assertEquals(2, errors);
  }
  
  @Test
  public void fsckFIXTagvDuplicateForward() throws Exception {
    setupMockBase();
    tsdb_store.addColumn(new byte[]{0, 0, 2}, NAME_FAMILY,
      TAGV, toBytes("wtf"));
    int errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), true, false);
    assertEquals(2, errors);
    assertArrayEquals(toBytes("web02"),
      tsdb_store.getColumn(new byte[]{0, 0, 2}, NAME_FAMILY, TAGV));
    errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), false, false);
    assertEquals(0, errors);
  }
 
  /* #4 - Missing Forward Mapping
   * - Reverse mapping missing forward mapping bar -> 02
   * ---------------------
   * 01 -> foo   foo -> 01
   * 02 -> bar   
   * 
   * FIX - Restore forward map. OK since "bar" does not map to anything
   */
  @Test
  public void fsckMetricsMissingForward() throws Exception {
    // currently a warning, not an error
    setupMockBase();
    tsdb_store.flushColumn(toBytes("bar"), ID_FAMILY,
      METRICS);
    int errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), false, false);
    assertEquals(0, errors);
  }
  
  @Test
  public void fsckFIXMetricsMissingForward() throws Exception {
    // currently a warning, not an error
    setupMockBase();
    tsdb_store.flushColumn(toBytes("bar"), ID_FAMILY,
      METRICS);
    int errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), true, false);
    assertEquals(0, errors);
    assertNull(tsdb_store.getColumn(new byte[]{0, 0, 2}, NAME_FAMILY, METRICS));
  }
  
  @Test
  public void fsckTagkMissingForward() throws Exception {
    // currently a warning, not an error
    setupMockBase();
    tsdb_store.flushColumn(toBytes("host"), ID_FAMILY, TAGK);
    int errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), false, false);
    assertEquals(0, errors);
  }
  
  @Test
  public void fsckFIXTagkMissingForward() throws Exception {
    // currently a warning, not an error
    setupMockBase();
    tsdb_store.flushColumn(toBytes("host"), ID_FAMILY, TAGK);
    int errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), true, false);
    assertEquals(0, errors);
    assertNull(tsdb_store.getColumn(new byte[]{0, 0, 1}, NAME_FAMILY, TAGK));
  }
  
  @Test
  public void fsckTagvMissingForward() throws Exception {
    // currently a warning, not an error
    setupMockBase();
    tsdb_store.flushColumn(toBytes("web01"), ID_FAMILY, TAGV);
    int errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), false, false);
    assertEquals(0, errors);
  }
  
  @Test
  public void fsckFIXTagvMissingForward() throws Exception {
    // currently a warning, not an error
    setupMockBase();
    tsdb_store.flushColumn(toBytes("web01"), ID_FAMILY, TAGV);
    int errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), true, false);
    assertEquals(0, errors);
    assertNull(tsdb_store.getColumn(new byte[]{0, 0, 1}, NAME_FAMILY, TAGV));
  }
  
  /* #5 - Inconsistent Reverse Mapping
   * - Inconsistent reverse mapping 03 -> foo vs 01 -> foo / foo -> 01 
   * ---------------------
   * 01 -> foo   foo -> 01
   * 02 -> bar   bar -> 02
   * 03 -> foo   
   *       ^^^
   * FIX - Delete 03 reverse map. OK since nothing maps to 02.
   */
  @Test
  public void fsckMetricsInconsistentReverse() throws Exception {
    setupMockBase();
    tsdb_store.addColumn(new byte[]{0, 0, 3}, NAME_FAMILY,
      METRICS, toBytes("foo"));
    tsdb_store.addColumn(new byte[]{0}, ID_FAMILY, METRICS, Bytes.fromLong(3L));
    int errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), false, false);
    assertEquals(1, errors);
  }
  
  @Test
  public void fsckFIXMetricsInconsistentReverse() throws Exception {
    setupMockBase();
    tsdb_store.addColumn(new byte[]{0, 0, 3}, NAME_FAMILY,
      METRICS, toBytes("foo"));
    tsdb_store.addColumn(new byte[]{0}, ID_FAMILY, METRICS, Bytes.fromLong(3L));
    int errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), true, false);
    assertEquals(1, errors);
    assertNull(tsdb_store.getColumn(new byte[]{0, 0, 3}, NAME_FAMILY, METRICS));
    errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), false, false);
    assertEquals(0, errors);
  }
  
  @Test
  public void fsckTagkInconsistentReverse() throws Exception {
    setupMockBase();
    tsdb_store.addColumn(new byte[]{0, 0, 3}, NAME_FAMILY,
      TAGK, toBytes("host"));
    tsdb_store.addColumn(new byte[]{0}, ID_FAMILY, TAGK, Bytes.fromLong(3L));
    int errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), false, false);
    assertEquals(1, errors);
  }
  
  @Test
  public void fsckFIXTagkInconsistentReverse() throws Exception {
    setupMockBase();
    tsdb_store.addColumn(new byte[]{0, 0, 3}, NAME_FAMILY,
      TAGK, toBytes("host"));
    tsdb_store.addColumn(new byte[]{0}, ID_FAMILY, TAGK, Bytes.fromLong(3L));
    int errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), true, false);
    assertEquals(1, errors);
    assertNull(tsdb_store.getColumn(new byte[]{0, 0, 3}, NAME_FAMILY, TAGK));
    errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), false, false);
    assertEquals(0, errors);
  }
  
  @Test
  public void fsckTagvInconsistentReverse() throws Exception {
    setupMockBase();
    tsdb_store.addColumn(new byte[]{0, 0, 3}, NAME_FAMILY,
      TAGV, toBytes("web01"));
    tsdb_store.addColumn(new byte[]{0}, ID_FAMILY, TAGV, Bytes.fromLong(3L));
    int errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), false, false);
    assertEquals(1, errors);
  }
  
  @Test
  public void fsckFIXTagvInconsistentReverse() throws Exception {
    setupMockBase();
    tsdb_store.addColumn(new byte[]{0, 0, 3}, NAME_FAMILY,
      TAGV, toBytes("web01"));
    tsdb_store.addColumn(new byte[]{0}, ID_FAMILY, TAGV, Bytes.fromLong(3L));
    int errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), true, false);
    assertEquals(1, errors);
    assertNull(tsdb_store.getColumn(new byte[]{0, 0, 3}, NAME_FAMILY, TAGV));
    errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), false, false);
    assertEquals(0, errors);
  }
  
  /* #6 - Duplicate Reverse Mapping
   * - Forward mapping is missing reverse mapping: wtf -> 04 
   * - Duplicate reverse mapping 03 -> wtf and 04 -> null
   * ---------------------
   * 01 -> foo   foo -> 01
   * 02 -> bar   bar -> 02
   * 03 -> wtf   wtf -> 04
   *       ^^^
   * FIX - Delete 03 reverse map. wtf -> 04 will be fixed by creating a reverse
   * map for 04 -> wtf
   */
  @Test
  public void fsckMetricsDuplicateReverse() throws Exception {
    setupMockBase();
    tsdb_store.addColumn(new byte[]{0, 0, 3}, NAME_FAMILY,
      METRICS, toBytes("wtf"));
    tsdb_store.addColumn(toBytes("wtf"), ID_FAMILY,
      METRICS, new byte[]{0, 0, 4});
    tsdb_store.addColumn(new byte[]{0}, ID_FAMILY, METRICS, Bytes.fromLong(4L));
    int errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), false, false);
    assertEquals(2, errors);
  }
  
  @Test
  public void fsckFIXMetricsDuplicateReverse() throws Exception {
    setupMockBase();
    tsdb_store.addColumn(new byte[]{0, 0, 3}, NAME_FAMILY,
      METRICS, toBytes("wtf"));
    tsdb_store.addColumn(toBytes("wtf"), ID_FAMILY,
      METRICS, new byte[]{0, 0, 4});
    tsdb_store.addColumn(new byte[]{0}, ID_FAMILY, METRICS, Bytes.fromLong(4L));
    int errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), true, false);
    assertEquals(2, errors);
    assertNull(tsdb_store.getColumn(new byte[]{0, 0, 3}, NAME_FAMILY, METRICS));
    assertArrayEquals(toBytes("wtf"),
        tsdb_store.getColumn(new byte [] {0, 0, 4}, NAME_FAMILY, METRICS));
    errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), false, false);
    assertEquals(0, errors);
  }
  
  @Test
  public void fsckTagkDuplicateReverse() throws Exception {
    setupMockBase();
    tsdb_store.addColumn(new byte[]{0, 0, 3}, NAME_FAMILY,
      TAGK, toBytes("wtf"));
    tsdb_store.addColumn(toBytes("wtf"), ID_FAMILY,
      TAGK, new byte[]{0, 0, 4});
    tsdb_store.addColumn(new byte[]{0}, ID_FAMILY, TAGK, Bytes.fromLong(4L));
    int errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), false, false);
    assertEquals(2, errors);
  }
  
  @Test
  public void fsckFIXTagkDuplicateReverse() throws Exception {
    setupMockBase();
    tsdb_store.addColumn(new byte[]{0, 0, 3}, NAME_FAMILY,
      TAGK, toBytes("wtf"));
    tsdb_store.addColumn(toBytes("wtf"), ID_FAMILY,
      TAGK, new byte[]{0, 0, 4});
    tsdb_store.addColumn(new byte[]{0}, ID_FAMILY, TAGK, Bytes.fromLong(4L));
    int errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), true, false);
    assertEquals(2, errors);
    assertNull(tsdb_store.getColumn(new byte[]{0, 0, 3}, NAME_FAMILY, TAGK));
    assertArrayEquals(toBytes("wtf"),
        tsdb_store.getColumn(new byte [] {0, 0, 4}, NAME_FAMILY, TAGK));
    errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), false, false);
    assertEquals(0, errors);
  }
  
  @Test
  public void fsckTagvDuplicateReverse() throws Exception {
    setupMockBase();
    tsdb_store.addColumn(new byte[]{0, 0, 3}, NAME_FAMILY,
      TAGV, toBytes("wtf"));
    tsdb_store.addColumn(toBytes("wtf"), ID_FAMILY,
      TAGV, new byte[]{0, 0, 4});
    tsdb_store.addColumn(new byte[]{0}, ID_FAMILY, TAGV, Bytes.fromLong(4L));
    int errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), false, false);
    assertEquals(2, errors);
  }
  
  @Test
  public void fsckFIXTagvDuplicateReverse() throws Exception {
    setupMockBase();
    tsdb_store.addColumn(new byte[]{0, 0, 3}, NAME_FAMILY,
      TAGV, toBytes("wtf"));
    tsdb_store.addColumn(toBytes("wtf"), ID_FAMILY,
      TAGV, new byte[]{0, 0, 4});
    tsdb_store.addColumn(new byte[]{0}, ID_FAMILY, TAGV, Bytes.fromLong(4L));
    int errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), true, false);
    assertEquals(2, errors);
    assertNull(tsdb_store.getColumn(new byte[]{0, 0, 3}, NAME_FAMILY, TAGV));
    assertArrayEquals(toBytes("wtf"),
        tsdb_store.getColumn(new byte [] {0, 0, 4}, NAME_FAMILY, TAGV));
    errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), false, false);
    assertEquals(0, errors);
  }
  
  /* ---------------- COMPOUND ERRORS --------------- 
   * (if the base fixes above work for all UID types, we only need to test
   *  one of the types below)
   * 
   * #7 - Inconsistent Forward And Duplicate Reverse
   * - Forward mapping missing reverse mapping: wtf -> 03
   * - Forward mapping bar -> 02 is diff than reverse mapping: 02 -> wtf
   * - Inconsistent forward mapping bar -> 02 vs bar -> wtf / wtf -> 03
   * - Duplicate reverse mapping 02 -> wtf and 03 -> null
   * ---------------------
   * 01 -> foo   foo -> 01
   * 02 -> wtf   bar -> 02
   *       ^^^   wtf -> 01
   *                    ^^
   * FIX - #1 covers the 02 -> wtf / bar -> 02 mismatch. #2 will fix wtf -> 01
   * and foo -> 01
   */
  @Test
  public void fsckMetricsInconsistentFwdAndDupeRev() throws Exception {
    setupMockBase();
    tsdb_store.addColumn(new byte[]{0, 0, 2}, NAME_FAMILY,
      METRICS, toBytes("wtf"));
    tsdb_store.addColumn(toBytes("wtf"), ID_FAMILY,
      METRICS, new byte[]{0, 0, 1});
    tsdb_store.addColumn(new byte[]{0}, ID_FAMILY, METRICS, Bytes.fromLong(3L));
    int errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), false, false);
    assertEquals(5, errors);
  }
  
  @Test
  public void fsckFIXMetricsInconsistentFwdAndDupeRev() throws Exception {
    setupMockBase();
    tsdb_store.addColumn(new byte[]{0, 0, 2}, NAME_FAMILY,
      METRICS, toBytes("wtf"));
    tsdb_store.addColumn(toBytes("wtf"), ID_FAMILY,
      METRICS, new byte[]{0, 0, 1});
    tsdb_store.addColumn(new byte[]{0}, ID_FAMILY, METRICS, Bytes.fromLong(3L));
    int errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), true, false);
    assertEquals(4, errors);
    assertArrayEquals(toBytes("fsck.foo.wtf"),
      tsdb_store.getColumn(new byte[]{0, 0, 1}, NAME_FAMILY, METRICS));
    assertNull(tsdb_store.getColumn(toBytes("foo"), ID_FAMILY,
        METRICS));
    assertNull(tsdb_store.getColumn(toBytes("wtf"), ID_FAMILY,
      METRICS));
    assertArrayEquals(toBytes("bar"),
        tsdb_store.getColumn(new byte [] {0, 0, 2}, NAME_FAMILY, METRICS));
    errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), false, false);
    assertEquals(0, errors);
  }
  
  /* #8 - Inconsistent Forward And Inconsistent Reverse
   * - Forward mapping wtf -> 01 is diff than reverse mapping: 01 -> foo
   * - Inconsistent forward mapping wtf -> 01 vs wtf -> foo / foo -> 01
   * - Forward mapping bar -> 02 is diff than reverse mapping: 02 -> wtf
   * - Inconsistent forward mapping bar -> 02 vs bar -> wtf / wtf -> 01
   * - Inconsistent reverse mapping 02 -> wtf vs 01 -> wtf / foo -> 01
   * - Inconsistent reverse mapping 03 -> foo vs 01 -> foo / foo -> 01
   * ---------------------
   * 01 -> foo   foo -> 01
   * 02 -> wtf   bar -> 02
   *       ^^^
   * 03 -> foo   wtf -> 01
   *       ^^^          ^^
   * 
   * FIX - Same as #2 && #3
   */
  @Test
  public void fsckMetricsInconsistentFwdAndInconsistentRev() throws Exception {
    setupMockBase();
    tsdb_store.addColumn(new byte[]{0, 0, 2}, NAME_FAMILY,
      METRICS, toBytes("wtf"));
    tsdb_store.addColumn(toBytes("wtf"), ID_FAMILY,
      METRICS, new byte[]{0, 0, 1});
    tsdb_store.addColumn(new byte[]{0, 0, 3}, NAME_FAMILY,
      METRICS, toBytes("foo"));
    tsdb_store.addColumn(new byte[]{0}, ID_FAMILY, METRICS, Bytes.fromLong(3L));
    int errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), false, false);
    assertEquals(6, errors);
  }
  
  @Test
  public void fsckFIXMetricsInconsistentFwdAndInconsistentRev() throws Exception {
    setupMockBase();
    tsdb_store.addColumn(new byte[]{0, 0, 2}, NAME_FAMILY,
      METRICS, toBytes("wtf"));
    tsdb_store.addColumn(toBytes("wtf"), ID_FAMILY,
      METRICS, new byte[]{0, 0, 1});
    tsdb_store.addColumn(new byte[]{0, 0, 3}, NAME_FAMILY,
      METRICS, toBytes("foo"));
    tsdb_store.addColumn(new byte[]{0}, ID_FAMILY, METRICS, Bytes.fromLong(3L));
    int errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), true, false);
    assertEquals(4, errors); // diff than above since we remove some forwards early
    assertArrayEquals(toBytes("fsck.foo.wtf"),
      tsdb_store.getColumn(new byte[]{0, 0, 1}, NAME_FAMILY, METRICS));
    assertNull(tsdb_store.getColumn(toBytes("foo"), ID_FAMILY,
        METRICS));
    assertNull(tsdb_store.getColumn(toBytes("wtf"), ID_FAMILY,
      METRICS));
    assertArrayEquals(toBytes("bar"),
        tsdb_store.getColumn(new byte [] {0, 0, 2}, NAME_FAMILY, METRICS));
    errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), false, false);
    assertEquals(0, errors);
  }
  
  /* #9 - Inconsistent Forward, No Dupes
   * - Forward mapping bar -> 02 is different than reverse mapping: 02 -> wtf
   * - Inconsistent forward mapping bar -> 02 vs bar -> wtf / wtf -> 03
   * - Inconsistent reverse mapping 02 -> wtf vs 03 -> wtf / wtf -> 03
   * ---------------------
   * 01 -> foo   foo -> 01
   * 02 -> wtf   bar -> 02
   * 03 -> wtf   wtf -> 03
   * 
   * FIX - Remove reverse 02 -> wtf. Run again and restore 02 -> bar
   */
  @Test
  public void fsckMetricsInconsistentFwdNoDupes() throws Exception {
    setupMockBase();
    tsdb_store.addColumn(new byte[]{0, 0, 2}, NAME_FAMILY,
      METRICS, toBytes("wtf"));
    tsdb_store.addColumn(toBytes("wtf"), ID_FAMILY,
      METRICS, new byte[]{0, 0, 3});
    tsdb_store.addColumn(new byte[]{0, 0, 3}, NAME_FAMILY,
      METRICS, toBytes("wtf"));
    tsdb_store.addColumn(new byte[]{0}, ID_FAMILY, METRICS, Bytes.fromLong(3L));
    int errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), false, false);
    assertEquals(3, errors);
  }
  
  @Test
  public void fsckFixMetricsInconsistentFwdNoDupes() throws Exception {
    setupMockBase();
    tsdb_store.addColumn(new byte[]{0, 0, 2}, NAME_FAMILY,
      METRICS, toBytes("wtf"));
    tsdb_store.addColumn(toBytes("wtf"), ID_FAMILY,
      METRICS, new byte[]{0, 0, 3});
    tsdb_store.addColumn(new byte[]{0, 0, 3}, NAME_FAMILY,
      METRICS, toBytes("wtf"));
    tsdb_store.addColumn(new byte[]{0}, ID_FAMILY, METRICS, Bytes.fromLong(3L));
    int errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), true, false);
    assertEquals(2, errors);
    assertArrayEquals(toBytes("bar"),
      tsdb_store.getColumn(new byte[]{0, 0, 2}, NAME_FAMILY, METRICS));
    errors = (Integer)fsck.invoke(null, tsdb_store,
        toBytes("tsdb"), true, false);
    assertEquals(0, errors);
  }
  
  /**
   * Write clean data to MockBase that can be overridden by individual unit tests
   */
  private void setupMockBase() {
    tsdb_store.addColumn(new byte[] { 0 }, ID_FAMILY, METRICS, Bytes.fromLong(2L));
    tsdb_store.addColumn(new byte[]{0}, ID_FAMILY, TAGK, Bytes.fromLong(2L));
    tsdb_store.addColumn(new byte[]{0}, ID_FAMILY, TAGV, Bytes.fromLong(2L));
    
    // forward mappings
    tsdb_store.addColumn(toBytes("foo"), ID_FAMILY,
      METRICS, new byte[]{0, 0, 1});
    tsdb_store.addColumn(toBytes("host"), ID_FAMILY,
        TAGK, new byte[] {0, 0, 1});
    tsdb_store.addColumn(toBytes("web01"), ID_FAMILY,
      TAGV, new byte[]{0, 0, 1});

    tsdb_store.addColumn(toBytes("bar"), ID_FAMILY,
      METRICS, new byte[]{0, 0, 2});
    tsdb_store.addColumn(toBytes("dc"), ID_FAMILY,
      TAGK, new byte[]{0, 0, 2});
    tsdb_store.addColumn(toBytes("web02"), ID_FAMILY,
      TAGV, new byte[]{0, 0, 2});

    // reverse mappings
    tsdb_store.addColumn(new byte[]{0, 0, 1}, NAME_FAMILY,
      METRICS, toBytes("foo"));
    tsdb_store.addColumn(new byte[]{0, 0, 1}, NAME_FAMILY,
      TAGK, toBytes("host"));
    tsdb_store.addColumn(new byte[] {0, 0, 1}, NAME_FAMILY,
        TAGV, toBytes("web01"));

    tsdb_store.addColumn(new byte[]{0, 0, 2}, NAME_FAMILY,
      METRICS, toBytes("bar"));
    tsdb_store.addColumn(new byte[]{0, 0, 2}, NAME_FAMILY,
      TAGK, toBytes("dc"));
    tsdb_store.addColumn(new byte[]{0, 0, 2}, NAME_FAMILY,
      TAGV, toBytes("web02"));
  }
}
