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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyShort;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.util.ArrayList;

import net.opentsdb.core.TSDB;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.uid.UniqueId.UniqueIdType;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.JSON;

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
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stumbleupon.async.Deferred;

@RunWith(PowerMockRunner.class)
@PrepareForTest({TSDB.class, Config.class, UniqueId.class, HBaseClient.class, 
  GetRequest.class, PutRequest.class, DeleteRequest.class, KeyValue.class, 
  RowLock.class, UIDMeta.class})
public final class TestUIDMeta {
  private TSDB tsdb = mock(TSDB.class);
  private HBaseClient client = mock(HBaseClient.class);
  private UIDMeta meta = new UIDMeta();
  
  @Before
  public void before() throws Exception {   
    when(tsdb.getUidName(UniqueIdType.METRIC,
        new byte[] { 0, 0, 1 })).thenReturn("sys.cpu.0");
    when(tsdb.getUidName(UniqueIdType.METRIC, 
        new byte[] { 0, 0, 2 })).thenThrow(
        new NoSuchUniqueId("metric", new byte[] { 0, 0, 2 }));

    when(tsdb.getClient()).thenReturn(client);
    when(tsdb.uidTable()).thenReturn("tsdb-uid".getBytes());
    when(tsdb.hbaseAcquireLock((byte[])any(), (byte[])any(), anyShort()))
      .thenReturn(mock(RowLock.class));
    
    KeyValue kv = mock(KeyValue.class);
    String json = 
      "{\"uid\":\"000001\",\"type\":\"METRIC\",\"name\":\"sys.cpu.0\"," +
      "\"description\":\"Description\",\"notes\":\"MyNotes\",\"created\":" + 
      "1328140801,\"displayName\":\"System CPU\"}";
    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>();
    kvs.add(kv);
    when(kv.value()).thenReturn(json.getBytes());
    when(client.get((GetRequest) any())).thenReturn(
        Deferred.fromResult(kvs));
    when(client.delete((DeleteRequest) any())).thenReturn(
        new Deferred<Object>());
    when(client.put((PutRequest) any())).thenReturn(
        new Deferred<Object>());
  }
  
  @Test
  public void constructorEmpty() {
    assertNotNull(new UIDMeta());
  }
  
  @Test
  public void constructor2() {
    meta = new UIDMeta(UniqueIdType.METRIC, "000005");
    assertNotNull(meta);
    assertEquals(UniqueIdType.METRIC, meta.getType());
    assertEquals("000005", meta.getUID());
  }
  
  @Test
  public void constructor3() {
    meta = new UIDMeta(UniqueIdType.METRIC, new byte[] {0, 0, 5}, "sys.cpu.5");
    assertNotNull(meta);
    assertEquals(UniqueIdType.METRIC, meta.getType());
    assertEquals("000005", meta.getUID());
    assertEquals("sys.cpu.5", meta.getName());
    assertEquals(System.currentTimeMillis() / 1000, meta.getCreated());
  }

  @Test
  public void createConstructor() {
    PowerMockito.mockStatic(System.class);
    when(System.currentTimeMillis()).thenReturn(1357300800000L); 
    meta = new UIDMeta(UniqueIdType.TAGK, new byte[] { 1, 0, 0 }, "host");
    assertEquals(1357300800000L / 1000, meta.getCreated());
    assertEquals(UniqueId.uidToString(new byte[] { 1, 0, 0 }), meta.getUID());
    assertEquals("host", meta.getName());
  }
 
  @Test
  public void serialize() throws Exception {
    final String json = JSON.serializeToString(meta);
    assertNotNull(json);
    assertEquals("{\"uid\":\"\",\"type\":null,\"name\":\"\",\"description\":"
        + "\"\",\"notes\":\"\",\"created\":0,\"custom\":null,\"displayName\":"
         + "\"\"}", 
        json);
  }
  
  @Test
  public void deserialize() throws Exception {
    String json = "{\"uid\":\"ABCD\",\"type\":\"MeTriC\",\"name\":\"MyName\"," +
    "\"description\":\"Description\",\"notes\":\"MyNotes\",\"created\":" + 
    "1328140801,\"displayName\":\"Empty\",\"unknownkey\":null}";
    meta = JSON.parseToObject(json, UIDMeta.class);
    assertNotNull(meta);
    assertEquals(meta.getUID(), "ABCD");
    assertEquals(UniqueIdType.METRIC, meta.getType());
    assertEquals("MyNotes", meta.getNotes());
    assertEquals("Empty", meta.getDisplayName());
  }

  @Test
  public void getUIDMetaDefault() throws Exception {
    when(client.get((GetRequest) any())).thenReturn(
        Deferred.fromResult((ArrayList<KeyValue>)null));
    meta = UIDMeta.getUIDMeta(tsdb, UniqueIdType.METRIC, "000001");
    assertEquals(UniqueIdType.METRIC, meta.getType());
    assertEquals("sys.cpu.0", meta.getName());
    assertEquals("000001", meta.getUID());
  }
  
  @Test
  public void getUIDMetaExists() throws Exception {
    meta = UIDMeta.getUIDMeta(tsdb, UniqueIdType.METRIC, "000001");
    assertEquals(UniqueIdType.METRIC, meta.getType());
    assertEquals("sys.cpu.0", meta.getName());
    assertEquals("000001", meta.getUID());
    assertEquals("MyNotes", meta.getNotes());
  }

  @Test (expected = NoSuchUniqueId.class)
  public void getUIDMetaNoSuch() throws Exception {
    UIDMeta.getUIDMeta(tsdb, UniqueIdType.METRIC, "000002");
  }
  
  @Test
  public void delete() throws Exception {
    meta = UIDMeta.getUIDMeta(tsdb, UniqueIdType.METRIC, "000001");
    meta.delete(tsdb);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void deleteNullType() throws Exception {
    meta = new UIDMeta(null, "000001");
    meta.delete(tsdb);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void deleteNullUID() throws Exception {
    meta = new UIDMeta(UniqueIdType.METRIC, null);
    meta.delete(tsdb);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void deleteEmptyUID() throws Exception {
    meta = new UIDMeta(UniqueIdType.METRIC, "");
    meta.delete(tsdb);
  }
  
  @Test
  public void syncToStorage() throws Exception {
    meta = new UIDMeta(UniqueIdType.METRIC, "000001");
    meta.setDisplayName("New Display Name");
    meta.syncToStorage(tsdb, false);
    assertEquals("New Display Name", meta.getDisplayName());
    assertEquals("MyNotes", meta.getNotes());
  }
  
  @Test
  public void syncToStorageOverwrite() throws Exception {
    meta = new UIDMeta(UniqueIdType.METRIC, "000001");
    meta.setDisplayName("New Display Name");
    meta.syncToStorage(tsdb, true);
    assertEquals("New Display Name", meta.getDisplayName());
    assertTrue(meta.getNotes().isEmpty());
  }
  
  @Test (expected = IllegalStateException.class)
  public void syncToStorageNoChanges() throws Exception {
    meta = UIDMeta.getUIDMeta(tsdb, UniqueIdType.METRIC, "000001");
    meta.syncToStorage(tsdb, false);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void syncToStorageNullType() throws Exception {
    meta = new UIDMeta(null, "000001");
    meta.syncToStorage(tsdb, true);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void syncToStorageNullUID() throws Exception {
    meta = new UIDMeta(UniqueIdType.METRIC, null);
    meta.syncToStorage(tsdb, true);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void syncToStorageEmptyUID() throws Exception {
    meta = new UIDMeta(UniqueIdType.METRIC, "");
    meta.syncToStorage(tsdb, true);
  }
  
  @Test (expected = NoSuchUniqueId.class)
  public void syncToStorageNoSuch() throws Exception {
    meta = new UIDMeta(UniqueIdType.METRIC, "000002");
    meta.syncToStorage(tsdb, true);
  }
}
