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
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import net.opentsdb.core.TSDB;
import net.opentsdb.storage.MockBase;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.JSON;

import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.Bytes;
import org.hbase.async.DeleteRequest;
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

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.DeferredGroupException;

@PowerMockIgnore({"javax.management.*", "javax.xml.*",
  "ch.qos.*", "org.slf4j.*",
  "com.sum.*", "org.xml.*"})
@RunWith(PowerMockRunner.class)
@PrepareForTest({TSDB.class, Config.class, UniqueId.class, HBaseClient.class, 
  GetRequest.class, PutRequest.class, DeleteRequest.class, KeyValue.class, 
  Scanner.class, UIDMeta.class, TSMeta.class, AtomicIncrementRequest.class})
public final class TestTSMeta {
  private TSDB tsdb;
  private Config config;
  private HBaseClient client = mock(HBaseClient.class);
  private MockBase storage;
  private TSMeta meta = new TSMeta();
  
  @Before
  public void before() throws Exception {
    config = mock(Config.class);
    when(config.getString("tsd.storage.hbase.data_table")).thenReturn("tsdb");
    when(config.getString("tsd.storage.hbase.uid_table")).thenReturn("tsdb-uid");
    when(config.getString("tsd.storage.hbase.meta_table")).thenReturn("tsdb-meta");
    when(config.getString("tsd.storage.hbase.tree_table")).thenReturn("tsdb-tree");
    when(config.enable_tsuid_incrementing()).thenReturn(true);
    when(config.enable_realtime_ts()).thenReturn(true);
    
    PowerMockito.whenNew(HBaseClient.class)
      .withArguments(anyString(), anyString()).thenReturn(client);
    tsdb = new TSDB(config);
    storage = new MockBase(tsdb, client, true, true, true, true);
    
    storage.addColumn(new byte[] { 0, 0, 1 }, 
        "metrics".getBytes(MockBase.ASCII()),
        "sys.cpu.0".getBytes(MockBase.ASCII()));
    storage.addColumn(new byte[] { 0, 0, 1 }, 
        "metric_meta".getBytes(MockBase.ASCII()), 
        ("{\"uid\":\"000001\",\"type\":\"METRIC\",\"name\":\"sys.cpu.0\"," +
        "\"description\":\"Description\",\"notes\":\"MyNotes\",\"created\":" + 
        "1328140801,\"displayName\":\"System CPU\"}")
        .getBytes(MockBase.ASCII()));
    
    storage.addColumn(new byte[] { 0, 0, 1 }, 
        "tagk".getBytes(MockBase.ASCII()),
        "host".getBytes(MockBase.ASCII()));
    storage.addColumn(new byte[] { 0, 0, 1 }, 
        "tagk_meta".getBytes(MockBase.ASCII()), 
        ("{\"uid\":\"000001\",\"type\":\"TAGK\",\"name\":\"host\"," +
        "\"description\":\"Description\",\"notes\":\"MyNotes\",\"created\":" + 
        "1328140801,\"displayName\":\"Host server name\"}")
        .getBytes(MockBase.ASCII()));

    storage.addColumn(new byte[] { 0, 0, 1 }, 
        "tagv".getBytes(MockBase.ASCII()),
        "web01".getBytes(MockBase.ASCII()));
    storage.addColumn(new byte[] { 0, 0, 1 }, 
        "tagv_meta".getBytes(MockBase.ASCII()), 
        ("{\"uid\":\"000001\",\"type\":\"TAGV\",\"name\":\"web01\"," +
        "\"description\":\"Description\",\"notes\":\"MyNotes\",\"created\":" + 
        "1328140801,\"displayName\":\"Web server 1\"}")
        .getBytes(MockBase.ASCII()));

    storage.addColumn(new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 1 },
        "ts_meta".getBytes(MockBase.ASCII()),
        ("{\"tsuid\":\"000001000001000001\",\"" +
        "description\":\"Description\",\"notes\":\"Notes\",\"created\":1328140800," +
        "\"custom\":null,\"units\":\"\",\"retention\":42,\"max\":1.0,\"min\":" +
        "\"NaN\",\"displayName\":\"Display\",\"dataType\":\"Data\"}")
        .getBytes(MockBase.ASCII()));
    storage.addColumn(new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 1 },
        "ts_ctr".getBytes(MockBase.ASCII()),
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
  public void serialize() throws Exception {
    final String json = JSON.serializeToString(meta);
    assertNotNull(json);
    assertTrue(json.contains("\"created\":0"));
  }
  
  @Test
  public void deserialize() throws Exception {
    String json = "{\"tsuid\":\"ABCD\",\"" +
     "description\":\"Description\",\"notes\":\"Notes\",\"created\":1328140800," +
     "\"custom\":null,\"units\":\"\",\"retention\":42,\"max\":1.0,\"min\":" +
     "\"NaN\",\"displayName\":\"Display\",\"dataType\":\"Data\",\"lastReceived" +
     "\":1328140801,\"unknownkey\":null}";
    TSMeta tsmeta = JSON.parseToObject(json, TSMeta.class);
    assertNotNull(tsmeta);
    assertEquals("ABCD", tsmeta.getTSUID());
    assertEquals("Notes", tsmeta.getNotes());
    assertEquals(42, tsmeta.getRetention());
  }
  
  @Test
  public void getTSMeta() throws Exception {
    meta = TSMeta.getTSMeta(tsdb, "000001000001000001").joinUninterruptibly();
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
    meta = TSMeta.getTSMeta(tsdb, "000002000001000001").joinUninterruptibly();
    assertNull(meta);
  }
  
  @Test (expected = NoSuchUniqueId.class)
  public void getTSMetaNSUMetric() throws Throwable {
    storage.addColumn(new byte[] { 0, 0, 2, 0, 0, 1, 0, 0, 1 },
        "ts_meta".getBytes(MockBase.ASCII()),
        ("{\"tsuid\":\"000002000001000001\",\"" +
        "description\":\"Description\",\"notes\":\"Notes\",\"created\":1328140800," +
        "\"custom\":null,\"units\":\"\",\"retention\":42,\"max\":1.0,\"min\":" +
        "\"NaN\",\"displayName\":\"Display\",\"dataType\":\"Data\"}")
        .getBytes(MockBase.ASCII()));
    try {
      TSMeta.getTSMeta(tsdb, "000002000001000001").joinUninterruptibly();
    } catch (DeferredGroupException e) {
      throw e.getCause();
    }
  }
  
  @Test (expected = NoSuchUniqueId.class)
  public void getTSMetaNSUTagk() throws Throwable {
    storage.addColumn(new byte[] { 0, 0, 1, 0, 0, 2, 0, 0, 1 },
        "ts_meta".getBytes(MockBase.ASCII()),
        ("{\"tsuid\":\"000001000002000001\",\"" +
        "description\":\"Description\",\"notes\":\"Notes\",\"created\":1328140800," +
        "\"custom\":null,\"units\":\"\",\"retention\":42,\"max\":1.0,\"min\":" +
        "\"NaN\",\"displayName\":\"Display\",\"dataType\":\"Data\"}")
        .getBytes(MockBase.ASCII()));
    try {
      TSMeta.getTSMeta(tsdb, "000001000002000001").joinUninterruptibly();
    } catch (DeferredGroupException e) {
      throw e.getCause();
    }
  }
  
  @Test (expected = NoSuchUniqueId.class)
  public void getTSMetaNSUTagv() throws Throwable {
    storage.addColumn(new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 2 },
        "ts_meta".getBytes(MockBase.ASCII()),
        ("{\"tsuid\":\"000001000001000002\",\"" +
        "description\":\"Description\",\"notes\":\"Notes\",\"created\":1328140800," +
        "\"custom\":null,\"units\":\"\",\"retention\":42,\"max\":1.0,\"min\":" +
        "\"NaN\",\"displayName\":\"Display\",\"dataType\":\"Data\"}")
        .getBytes(MockBase.ASCII()));
    try {
      TSMeta.getTSMeta(tsdb, "000001000001000002").joinUninterruptibly();
    } catch (DeferredGroupException e) {
      throw e.getCause();
    }
  }
  
  @Test
  public void delete() throws Exception {
    meta = TSMeta.getTSMeta(tsdb, "000001000001000001").joinUninterruptibly();
    meta.delete(tsdb);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void deleteNull() throws Exception {
    meta = new TSMeta();
    meta.delete(tsdb);
  }
  
  @Test
  public void syncToStorage() throws Exception {
    meta = new TSMeta(new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 1 }, 1357300800000L);
    meta.setDisplayName("New DN");
    meta.syncToStorage(tsdb, false).joinUninterruptibly();
    assertEquals("New DN", meta.getDisplayName());
    assertEquals(42, meta.getRetention());
  }
  
  @Test
  public void syncToStorageOverwrite() throws Exception {
    meta = new TSMeta(new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 1 }, 1357300800000L);
    meta.setDisplayName("New DN");
    meta.syncToStorage(tsdb, true).joinUninterruptibly();
    assertEquals("New DN", meta.getDisplayName());
    assertEquals(0, meta.getRetention());
  }
  
  @Test (expected = IllegalStateException.class)
  public void syncToStorageNoChanges() throws Exception {
    meta = new TSMeta("ABCD");
    meta.syncToStorage(tsdb, true).joinUninterruptibly();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void syncToStorageNullTSUID() throws Exception {
    meta = new TSMeta();
    meta.syncToStorage(tsdb, true).joinUninterruptibly();
  }

  @Test (expected = IllegalArgumentException.class)
  public void syncToStorageDoesNotExist() throws Exception {
    storage.flushRow(new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 1 });
    meta = new TSMeta(new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 1 }, 1357300800000L);
    meta.syncToStorage(tsdb, false).joinUninterruptibly();
  }
  
  @Test
  public void storeNew() throws Exception {
    meta = new TSMeta(new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 1 }, 1357300800000L);
    meta.setDisplayName("New DN");
    meta.storeNew(tsdb);
    assertEquals("New DN", meta.getDisplayName());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void storeNewNull() throws Exception {
    meta = new TSMeta(null);
    meta.storeNew(tsdb);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void storeNewEmpty() throws Exception {
    meta = new TSMeta("");
    meta.storeNew(tsdb);
  }
  
  @Test
  public void metaExistsInStorage() throws Exception {
    assertTrue(TSMeta.metaExistsInStorage(tsdb, "000001000001000001")
        .joinUninterruptibly());
  }
  
  @Test
  public void metaExistsInStorageNot() throws Exception {
    storage.flushRow(new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 1 });
    assertFalse(TSMeta.metaExistsInStorage(tsdb, "000001000001000001")
        .joinUninterruptibly());
  }
  
  @Test
  public void counterExistsInStorage() throws Exception {
    assertTrue(TSMeta.counterExistsInStorage(tsdb, 
        new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 1 }).joinUninterruptibly());
  }
  
  @Test
  public void counterExistsInStorageNot() throws Exception {
    storage.flushRow(new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 1 });
    assertFalse(TSMeta.counterExistsInStorage(tsdb, 
        new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 1 }).joinUninterruptibly());
  }

  @Test
  public void incrementAndGetCounter() throws Exception {
    final byte[] tsuid = { 0, 0, 1, 0, 0, 1, 0, 0, 1 };
    TSMeta.incrementAndGetCounter(tsdb, tsuid).joinUninterruptibly();
    verify(client).bufferAtomicIncrement((AtomicIncrementRequest)any());
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
    
    TSMeta.incrementAndGetCounter(tsdb, tsuid).addErrback(new ErrBack())
    .joinUninterruptibly();
  }

  @Test
  public void META_QUALIFIER() throws Exception {
    assertArrayEquals("ts_meta".getBytes(MockBase.ASCII()), 
        TSMeta.META_QUALIFIER());
  }
  
  @Test
  public void COUNTER_QUALIFIER() throws Exception {
    assertArrayEquals("ts_ctr".getBytes(MockBase.ASCII()), 
        TSMeta.COUNTER_QUALIFIER());
  }

  @Test
  public void parseFromColumn() throws Exception {
    final KeyValue column = mock(KeyValue.class);
    when(column.key()).thenReturn(new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 1 });
    when(column.value()).thenReturn(storage.getColumn(
        new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 1 }, 
            "ts_meta".getBytes(MockBase.ASCII())));
    final TSMeta meta = TSMeta.parseFromColumn(tsdb, column, false)
      .joinUninterruptibly();
    assertNotNull(meta);
    assertEquals("000001000001000001", meta.getTSUID());
    assertNull(meta.getMetric());
  }
  
  @Test
  public void parseFromColumnWithUIDMeta() throws Exception {
    final KeyValue column = mock(KeyValue.class);
    when(column.key()).thenReturn(new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 1 });
    when(column.value()).thenReturn(storage.getColumn(
        new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 1 }, 
            "ts_meta".getBytes(MockBase.ASCII())));
    final TSMeta meta = TSMeta.parseFromColumn(tsdb, column, true)
      .joinUninterruptibly();
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
        .getBytes(MockBase.ASCII()));
    TSMeta.parseFromColumn(tsdb, column, true).addErrback(new ErrBack())
      .joinUninterruptibly();
  }
}
