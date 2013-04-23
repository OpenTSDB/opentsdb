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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyShort;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.nio.charset.Charset;
import java.util.ArrayList;

import net.opentsdb.core.TSDB;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.uid.UniqueId.UniqueIdType;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.JSON;

import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.Bytes;
import org.hbase.async.DeleteRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.RowLock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stumbleupon.async.Deferred;

@PowerMockIgnore({"javax.management.*", "javax.xml.*",
  "ch.qos.*", "org.slf4j.*",
  "com.sum.*", "org.xml.*"})
@RunWith(PowerMockRunner.class)
@PrepareForTest({TSDB.class, Config.class, UniqueId.class, HBaseClient.class, 
  GetRequest.class, PutRequest.class, DeleteRequest.class, KeyValue.class, 
  RowLock.class, UIDMeta.class, TSMeta.class, AtomicIncrementRequest.class})
public final class TestTSMeta {
  private static final Charset CHARSET = Charset.forName("ISO-8859-1");
  private TSDB tsdb = mock(TSDB.class);
  private HBaseClient client = mock(HBaseClient.class);
  private TSMeta meta = new TSMeta();
  
  @Before
  public void before() throws Exception {
    PowerMockito.mockStatic(UIDMeta.class);
    
    UIDMeta metric = new UIDMeta(UniqueIdType.METRIC, new byte[] { 0, 0, 1 },
        "sys.cpu.0");
    metric.setDisplayName("System CPU");
    UIDMeta tagk = new UIDMeta(UniqueIdType.TAGK, new byte[] { 0, 0, 1 },
        "host");
    tagk.setDisplayName("Host server name");
    UIDMeta tagv = new UIDMeta(UniqueIdType.TAGV, new byte[] { 0, 0, 1 },
        "web01");
    tagv.setDisplayName("Web server 1");
    
    when(UIDMeta.getUIDMeta(tsdb, UniqueIdType.METRIC, "000001"))
      .thenReturn(metric);
    when(UIDMeta.getUIDMeta(tsdb, UniqueIdType.METRIC, "000002"))
      .thenThrow(new NoSuchUniqueName("metric", "sys.cpu.1"));
    
    when(UIDMeta.getUIDMeta(tsdb, UniqueIdType.TAGK, new byte[] { 0, 0, 1 }))
      .thenReturn(tagk);
    when(UIDMeta.getUIDMeta(tsdb, UniqueIdType.TAGK, new byte[] { 0, 0, 2 }))
      .thenThrow(new NoSuchUniqueName("tagk", "dc"));
    
    when(UIDMeta.getUIDMeta(tsdb, UniqueIdType.TAGV, new byte[] { 0, 0, 1 }))
      .thenReturn(tagv);
    when(UIDMeta.getUIDMeta(tsdb, UniqueIdType.TAGV, new byte[] { 0, 0, 2 }))
      .thenThrow(new NoSuchUniqueName("tagv", "web02"));
    
    when(tsdb.getClient()).thenReturn(client);
    when(tsdb.uidTable()).thenReturn("tsdb-uid".getBytes());
    when(tsdb.hbaseAcquireLock((byte[])any(), (byte[])any(), anyShort()))
      .thenReturn(mock(RowLock.class));
    
    KeyValue kv = mock(KeyValue.class);
    String json = 
      "{\"tsuid\":\"ABCD\",\"" +
      "description\":\"Description\",\"notes\":\"Notes\",\"created\":1328140800," +
      "\"custom\":null,\"units\":\"\",\"retention\":42,\"max\":1.0,\"min\":" +
      "\"NaN\",\"displayName\":\"Display\",\"dataType\":\"Data\"}";
    KeyValue ctr = mock(KeyValue.class);
    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>();
    kvs.add(kv);
    kvs.add(ctr);
    when(kv.value()).thenReturn(json.getBytes());
    when(kv.qualifier()).thenReturn("ts_meta".getBytes(CHARSET));
    when(ctr.value()).thenReturn(Bytes.fromLong(1));
    when(ctr.timestamp()).thenReturn(1328140801000L);
    when(ctr.qualifier()).thenReturn("ts_ctr".getBytes(CHARSET));
    
    when(client.get((GetRequest) any())).thenReturn(
        Deferred.fromResult(kvs));
    when(client.delete((DeleteRequest) any())).thenReturn(
        new Deferred<Object>());
    when(client.put((PutRequest) any())).thenReturn(
        new Deferred<Object>());
    when(client.bufferAtomicIncrement((AtomicIncrementRequest)any()))
    .thenReturn(Deferred.fromResult(1L));
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
    meta = TSMeta.getTSMeta(tsdb, "000001000001000001");
    assertNotNull(meta);
    assertEquals("ABCD", meta.getTSUID());
    assertEquals("sys.cpu.0", meta.getMetric().getName());
    assertEquals(2, meta.getTags().size());
    assertEquals("host", meta.getTags().get(0).getName());
    assertEquals("web01", meta.getTags().get(1).getName());
    assertEquals(1, meta.getTotalDatapoints());
    assertEquals(1328140801L, meta.getLastReceived());
  }
  
  @Test
  public void getTSMetaDoesNotExist() throws Exception {
    when(client.get((GetRequest) any())).thenReturn(
        Deferred.fromResult((ArrayList<KeyValue>)null));
    meta = TSMeta.getTSMeta(tsdb, "000001000001000001");
    assertNull(meta);
  }
  
  @Test (expected = NoSuchUniqueName.class)
  public void getTSMetaNSUMetric() throws Exception {
    TSMeta.getTSMeta(tsdb, "000002000001000001");
  }
  
  @Test (expected = NoSuchUniqueName.class)
  public void getTSMetaNSUTagk() throws Exception {
    TSMeta.getTSMeta(tsdb, "000001000002000001");
  }
  
  @Test (expected = NoSuchUniqueName.class)
  public void getTSMetaNSUTagv() throws Exception {
    TSMeta.getTSMeta(tsdb, "000001000001000002");
  }
  
  @Test
  public void delete() throws Exception {
    meta = TSMeta.getTSMeta(tsdb, "000001000001000001");
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
    meta.syncToStorage(tsdb, false);
    assertEquals("New DN", meta.getDisplayName());
    assertEquals(42, meta.getRetention());
  }
  
  @Test
  public void syncToStorageOverwrite() throws Exception {
    meta = new TSMeta(new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 1 }, 1357300800000L);
    meta.setDisplayName("New DN");
    meta.syncToStorage(tsdb, true);
    assertEquals("New DN", meta.getDisplayName());
    assertEquals(0, meta.getRetention());
  }
  
  @Test (expected = IllegalStateException.class)
  public void syncToStorageNoChanges() throws Exception {
    meta = new TSMeta("ABCD");
    meta.syncToStorage(tsdb, true);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void syncToStorageNullTSUID() throws Exception {
    meta = new TSMeta();
    meta.syncToStorage(tsdb, true);
  }

  @Test (expected = IllegalArgumentException.class)
  public void syncToStorageDoesNotExist() throws Exception {
    when(client.get((GetRequest) any())).thenReturn(
        Deferred.fromResult((ArrayList<KeyValue>)null));
    meta = new TSMeta(new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 1 }, 1357300800000L);
    meta.syncToStorage(tsdb, false);
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
    assertTrue(TSMeta.metaExistsInStorage(tsdb, "000001000001000001"));
  }
  
  @Test
  public void metaExistsInStorageNot() throws Exception {
    when(client.get((GetRequest) any())).thenReturn(
        Deferred.fromResult((ArrayList<KeyValue>)null));
    assertFalse(TSMeta.metaExistsInStorage(tsdb, "000001000001000001"));
  }
  
  @Test
  public void counterExistsInStorage() throws Exception {
    assertTrue(TSMeta.counterExistsInStorage(tsdb, 
        new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 1 }));
  }
  
  @Test
  public void counterExistsInStorageNot() throws Exception {
    when(client.get((GetRequest) any())).thenReturn(
        Deferred.fromResult((ArrayList<KeyValue>)null));
    assertFalse(TSMeta.counterExistsInStorage(tsdb, 
        new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 1 }));
  }

  @Test
  public void incrementAndGetCounter() throws Exception {
    final byte[] tsuid = { 0, 0, 1, 0, 0, 1, 0, 0, 1 };
    TSMeta.incrementAndGetCounter(tsdb, tsuid);
    verify(client).bufferAtomicIncrement((AtomicIncrementRequest)any());
  }
}
