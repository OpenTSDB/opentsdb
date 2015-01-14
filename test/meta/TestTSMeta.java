// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
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
package net.opentsdb.meta;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import com.google.common.collect.Maps;
import net.opentsdb.core.Const;
import net.opentsdb.storage.MemoryStore;
import net.opentsdb.core.TSDB;
import net.opentsdb.storage.MockBase;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.Config;

import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.Bytes;
import org.hbase.async.DeleteRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.Scanner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.DeferredGroupException;

import java.util.Map;

@PowerMockIgnore({"javax.management.*", "javax.xml.*",
  "ch.qos.*", "org.slf4j.*",
  "com.sum.*", "org.xml.*"})
@RunWith(PowerMockRunner.class)
@PrepareForTest({TSDB.class, Config.class, UniqueId.class,
  GetRequest.class, PutRequest.class, DeleteRequest.class, KeyValue.class,
  Scanner.class, UIDMeta.class, TSMeta.class, AtomicIncrementRequest.class,
  MemoryStore.class})
public final class TestTSMeta {
  private static byte[] NAME_FAMILY = "name".getBytes(Const.CHARSET_ASCII);
  private TSDB tsdb;
  private Config config;
  private MemoryStore tsdb_store;
  private TSMeta meta = new TSMeta();
  
  @Before
  public void before() throws Exception {
    Map<String, String> overrides = Maps.newHashMap();
    overrides.put("tsd.core.meta.enable_tsuid_incrementing", "TRUE");
    overrides.put("tsd.core.meta.enable_realtime_ts", "TRUE");
    config = new Config(false, overrides);

    tsdb_store = new MemoryStore();
    tsdb = new TSDB(tsdb_store, config);

    tsdb_store.addColumn(new byte[]{0, 0, 1},
      NAME_FAMILY,
      "metrics".getBytes(Const.CHARSET_ASCII),
      "sys.cpu.0".getBytes(Const.CHARSET_ASCII));
    tsdb_store.addColumn(new byte[]{0, 0, 1},
      NAME_FAMILY,
      "metric_meta".getBytes(Const.CHARSET_ASCII),
      ("{\"uid\":\"000001\",\"type\":\"METRIC\",\"name\":\"sys.cpu.0\"," +
        "\"description\":\"Description\",\"notes\":\"MyNotes\",\"created\":" +
        "1328140801,\"displayName\":\"System CPU\"}")
        .getBytes(Const.CHARSET_ASCII));

    tsdb_store.addColumn(new byte[]{0, 0, 1},
      NAME_FAMILY,
      "tagk".getBytes(Const.CHARSET_ASCII),
      "host".getBytes(Const.CHARSET_ASCII));
    tsdb_store.addColumn(new byte[]{0, 0, 1},
      NAME_FAMILY,
      "tagk_meta".getBytes(Const.CHARSET_ASCII),
      ("{\"uid\":\"000001\",\"type\":\"TAGK\",\"name\":\"host\"," +
        "\"description\":\"Description\",\"notes\":\"MyNotes\",\"created\":" +
        "1328140801,\"displayName\":\"Host server name\"}")
        .getBytes(Const.CHARSET_ASCII));

    tsdb_store.addColumn(new byte[]{0, 0, 1},
      NAME_FAMILY,
      "tagv".getBytes(Const.CHARSET_ASCII),
      "web01".getBytes(Const.CHARSET_ASCII));
    tsdb_store.addColumn(new byte[]{0, 0, 1},
      NAME_FAMILY,
      "tagv_meta".getBytes(Const.CHARSET_ASCII),
      ("{\"uid\":\"000001\",\"type\":\"TAGV\",\"name\":\"web01\"," +
        "\"description\":\"Description\",\"notes\":\"MyNotes\",\"created\":" +
        "1328140801,\"displayName\":\"Web server 1\"}")
        .getBytes(Const.CHARSET_ASCII));

    tsdb_store.addColumn(new byte[]{0, 0, 1, 0, 0, 1, 0, 0, 1},
      NAME_FAMILY,
      "ts_meta".getBytes(Const.CHARSET_ASCII),
      ("{\"tsuid\":\"000001000001000001\",\"" +
        "description\":\"Description\",\"notes\":\"Notes\",\"created\":1328140800," +
        "\"custom\":null,\"units\":\"\",\"retention\":42,\"max\":1.0,\"min\":" +
        "\"NaN\",\"displayName\":\"Display\",\"dataType\":\"Data\"}")
        .getBytes(Const.CHARSET_ASCII));
    tsdb_store.addColumn(new byte[]{0, 0, 1, 0, 0, 1, 0, 0, 1},
      NAME_FAMILY,
      "ts_ctr".getBytes(Const.CHARSET_ASCII),
      Bytes.fromLong(1L));
  }
  
  @Test
  public void constructor() { 
    assertNotNull(new TSMeta());
  }
 
  @Test
  public void createConstructor() {
    meta = new TSMeta(new byte[] { 0, 0, 1, 0, 0, 2, 0, 0, 3 }, 1357300800000L);
    assertEquals(1357300800000L / 1000, meta.getCreated());
  }
  
  @Test
  public void getTSMeta() throws Exception {
    meta = tsdb.getTSMeta("000001000001000001", true).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(meta);
    assertEquals("000001000001000001", meta.getTSUID());
    assertEquals("sys.cpu.0", meta.getMetric().getName());
    assertEquals(2, meta.getTags().size());
    assertEquals("host", meta.getTags().get(0).getName());
    assertEquals("web01", meta.getTags().get(1).getName());
    assertEquals(1, meta.getTotalDatapoints());
    // no support for timestamps in mockbase yet
    //assertEquals(1328140801L, meta.getLastReceived());
  }
  
  @Test
  public void getTSMetaDoesNotExist() throws Exception {
    meta = tsdb.getTSMeta("000002000001000001", true).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNull(meta);
  }
  
  @Test (expected = NoSuchUniqueId.class)
  public void getTSMetaNSUMetric() throws Throwable {
    tsdb_store.addColumn(new byte[]{0, 0, 2, 0, 0, 1, 0, 0, 1},
      NAME_FAMILY,
      "ts_meta".getBytes(Const.CHARSET_ASCII),
      ("{\"tsuid\":\"000002000001000001\",\"" +
        "description\":\"Description\",\"notes\":\"Notes\",\"created\":1328140800," +
        "\"custom\":null,\"units\":\"\",\"retention\":42,\"max\":1.0,\"min\":" +
        "\"NaN\",\"displayName\":\"Display\",\"dataType\":\"Data\"}")
        .getBytes(Const.CHARSET_ASCII));
    try {
      tsdb.getTSMeta( "000002000001000001", true).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    } catch (DeferredGroupException e) {
      throw e.getCause();
    }
  }
  
  @Test (expected = NoSuchUniqueId.class)
  public void getTSMetaNSUTagk() throws Throwable {
    tsdb_store.addColumn(new byte[]{0, 0, 1, 0, 0, 2, 0, 0, 1},
      NAME_FAMILY,
      "ts_meta".getBytes(Const.CHARSET_ASCII),
      ("{\"tsuid\":\"000001000002000001\",\"" +
        "description\":\"Description\",\"notes\":\"Notes\",\"created\":1328140800," +
        "\"custom\":null,\"units\":\"\",\"retention\":42,\"max\":1.0,\"min\":" +
        "\"NaN\",\"displayName\":\"Display\",\"dataType\":\"Data\"}")
        .getBytes(Const.CHARSET_ASCII));
    try {
      tsdb.getTSMeta( "000001000002000001", true).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    } catch (DeferredGroupException e) {
      throw e.getCause();
    }
  }
  
  @Test (expected = NoSuchUniqueId.class)
  public void getTSMetaNSUTagv() throws Throwable {
    tsdb_store.addColumn(new byte[]{0, 0, 1, 0, 0, 1, 0, 0, 2},
      NAME_FAMILY,
      "ts_meta".getBytes(Const.CHARSET_ASCII),
      ("{\"tsuid\":\"000001000001000002\",\"" +
        "description\":\"Description\",\"notes\":\"Notes\",\"created\":1328140800," +
        "\"custom\":null,\"units\":\"\",\"retention\":42,\"max\":1.0,\"min\":" +
        "\"NaN\",\"displayName\":\"Display\",\"dataType\":\"Data\"}")
        .getBytes(Const.CHARSET_ASCII));
    try {
      tsdb.getTSMeta( "000001000001000002", true).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    } catch (DeferredGroupException e) {
      throw e.getCause();
    }
  }
  
  @Test
  public void delete() throws Exception {
    meta = tsdb.getTSMeta( "000001000001000001", true).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    tsdb.delete(meta);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void deleteNull() throws Exception {
    meta = new TSMeta();
    tsdb.delete(meta);
  }
  
  @Test
  public void syncToStorage() throws Exception {
    meta = new TSMeta(new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 1 }, 1357300800000L);
    meta.setDisplayName("New DN");
    tsdb.syncToStorage(meta, false).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertEquals("New DN", meta.getDisplayName());
    assertEquals(42, meta.getRetention());
  }
  
  @Test
  public void syncToStorageOverwrite() throws Exception {
    meta = new TSMeta(new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 1 }, 1357300800000L);
    meta.setDisplayName("New DN");
    tsdb.syncToStorage(meta, true).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertEquals("New DN", meta.getDisplayName());
    assertEquals(0, meta.getRetention());
  }
  
  @Test (expected = IllegalStateException.class)
  public void syncToStorageNoChanges() throws Exception {
    meta = new TSMeta("ABCD");
    tsdb.syncToStorage(meta, true).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void syncToStorageNullTSUID() throws Exception {
    meta = new TSMeta();
    tsdb.syncToStorage(meta, true).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
  }

  @Test (expected = IllegalArgumentException.class)
  public void syncToStorageDoesNotExist() throws Exception {
    tsdb_store.flushRow(new byte[]{0, 0, 1, 0, 0, 1, 0, 0, 1});
    meta = new TSMeta(new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 1 }, 1357300800000L);
    tsdb.syncToStorage(meta, false).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
  }
  
  @Test
  public void storeNew() throws Exception {
    meta = new TSMeta(new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 1 }, 1357300800000L);
    meta.setDisplayName("New DN");
    tsdb.create(meta);
    assertEquals("New DN", meta.getDisplayName());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void storeNewNull() throws Exception {
    meta = new TSMeta(null);
    tsdb.create(meta);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void storeNewEmpty() throws Exception {
    meta = new TSMeta("");
    tsdb.create(meta);
  }
  
  @Test
  public void metaExistsInStorage() throws Exception {
    assertTrue(tsdb.TSMetaExists("000001000001000001")
        .joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));
  }
  
  @Test
  public void metaExistsInStorageNot() throws Exception {
    tsdb_store.flushRow(new byte[]{0, 0, 1, 0, 0, 1, 0, 0, 1});
    assertFalse(tsdb.TSMetaExists("000001000001000001")
            .joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));
  }
  
  @Test
  public void counterExistsInStorage() throws Exception {
    assertTrue(tsdb.TSMetaCounterExists(
            new byte[]{0, 0, 1, 0, 0, 1, 0, 0, 1}).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));
  }
  
  @Test
  public void counterExistsInStorageNot() throws Exception {
    tsdb_store.flushRow(new byte[]{0, 0, 1, 0, 0, 1, 0, 0, 1});
    assertFalse(tsdb.TSMetaCounterExists(
            new byte[]{0, 0, 1, 0, 0, 1, 0, 0, 1}).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));
  }

  @Test
  public void incrementAndGetCounter() throws Exception {
    final byte[] tsuid = { 0, 0, 1, 0, 0, 1, 0, 0, 1 };
    tsdb.incrementAndGetCounter(tsuid).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    verify(tsdb_store).
            bufferAtomicIncrement((AtomicIncrementRequest) any());
  }
  
  @Test (expected = NoSuchUniqueId.class)
  public void incrementAndGetCounterNSU() throws Exception {
    final byte[] tsuid = { 0, 0, 1, 0, 0, 1, 0, 0, 2 };
    class ErrBack implements Callback<Object, Exception> {
      @Override
      public Object call(Exception e) throws Exception {
        Throwable ex = e;
        while (ex.getClass().equals(DeferredGroupException.class)) {
          ex = ex.getCause();
        }
        throw (Exception)ex;
      }      
    }

    tsdb.incrementAndGetCounter(tsuid).addErrback(new ErrBack())
    .joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
  }
  
  @Test
  public void META_QUALIFIER() throws Exception {
    assertArrayEquals("ts_meta".getBytes(Const.CHARSET_ASCII),
        TSMeta.META_QUALIFIER());
  }
  
  @Test
  public void COUNTER_QUALIFIER() throws Exception {
    assertArrayEquals("ts_ctr".getBytes(Const.CHARSET_ASCII),
        TSMeta.COUNTER_QUALIFIER());
  }

  @Test
  public void parseFromColumn() throws Exception {
    final KeyValue column = mock(KeyValue.class);
    when(column.key()).thenReturn(new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 1 });
    when(column.value()).thenReturn(tsdb_store.getColumn(
      new byte[]{0, 0, 1, 0, 0, 1, 0, 0, 1},
      NAME_FAMILY,
      "ts_meta".getBytes(Const.CHARSET_ASCII)));
    final TSMeta meta = tsdb.parseFromColumn(column.key(), column.value(), false)
      .joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(meta);
    assertEquals("000001000001000001", meta.getTSUID());
    assertNull(meta.getMetric());
  }
  
  @Test
  public void parseFromColumnWithUIDMeta() throws Exception {
    final KeyValue column = mock(KeyValue.class);
    when(column.key()).thenReturn(new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 1 });
    when(column.value()).thenReturn(tsdb_store.getColumn(
      new byte[]{0, 0, 1, 0, 0, 1, 0, 0, 1},
      NAME_FAMILY,
      "ts_meta".getBytes(Const.CHARSET_ASCII)));
    final TSMeta meta = tsdb.parseFromColumn(column.key(), column.value(), true)
      .joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(meta);
    assertEquals("000001000001000001", meta.getTSUID());
    assertNotNull(meta.getMetric());
    assertEquals("sys.cpu.0", meta.getMetric().getName());
  }
  
  @Test (expected = NoSuchUniqueId.class)
  public void parseFromColumnWithUIDMetaNSU() throws Exception {
    class ErrBack implements Callback<Object, Exception> {
      @Override
      public Object call(Exception e) throws Exception {
        Throwable ex = e;
        while (ex.getClass().equals(DeferredGroupException.class)) {
          ex = ex.getCause();
        }
        throw (Exception)ex;
      }      
    }
    
    final KeyValue column = mock(KeyValue.class);
    when(column.key()).thenReturn(new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 2 });
    when(column.value()).thenReturn(("{\"tsuid\":\"000001000001000002\",\"" +
        "description\":\"Description\",\"notes\":\"Notes\",\"created\":1328140800," +
        "\"custom\":null,\"units\":\"\",\"retention\":42,\"max\":1.0,\"min\":" +
        "\"NaN\",\"displayName\":\"Display\",\"dataType\":\"Data\"}")
        .getBytes(Const.CHARSET_ASCII));
    tsdb.parseFromColumn(column.key(), column.value(), true).addErrback(new ErrBack())
      .joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
  }
}
