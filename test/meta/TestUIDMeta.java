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
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import net.opentsdb.core.TSDB;
import net.opentsdb.storage.MockBase;
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
import org.hbase.async.Scanner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@PowerMockIgnore({"javax.management.*", "javax.xml.*",
  "ch.qos.*", "org.slf4j.*",
  "com.sum.*", "org.xml.*"})
@RunWith(PowerMockRunner.class)
@PrepareForTest({TSDB.class, Config.class, UniqueId.class, HBaseClient.class, 
  GetRequest.class, PutRequest.class, DeleteRequest.class, KeyValue.class, 
  Scanner.class, UIDMeta.class})
public final class TestUIDMeta {
  private static byte[] NAME_FAMILY = "name".getBytes(MockBase.ASCII());
  private TSDB tsdb;
  private HBaseClient client = mock(HBaseClient.class);
  private MockBase storage;
  private UIDMeta meta = new UIDMeta();
  
  @Before
  public void before() throws Exception {
    final Config config = new Config(false);
    tsdb = new TSDB(client, config);
    
    storage = new MockBase(tsdb, client, true, true, true, true);

    storage.addColumn(new byte[] { 0, 0, 1 }, 
        NAME_FAMILY,
        "metrics".getBytes(MockBase.ASCII()), 
        "sys.cpu.0".getBytes(MockBase.ASCII()));

    storage.addColumn(new byte[] { 0, 0, 3 }, 
        NAME_FAMILY,
        "metrics".getBytes(MockBase.ASCII()), 
        "sys.cpu.2".getBytes(MockBase.ASCII()));

    storage.addColumn(new byte[] { 0, 0, 1 }, 
        NAME_FAMILY,
        "metric_meta".getBytes(MockBase.ASCII()), 
        ("{\"uid\":\"000001\",\"type\":\"METRIC\",\"name\":\"sys.cpu.0\"," +
        "\"description\":\"Description\",\"notes\":\"MyNotes\",\"created\":" + 
        "1328140801,\"displayName\":\"System CPU\"}").getBytes(MockBase.ASCII()));
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
  public void getUIDMeta() throws Exception {
    meta = UIDMeta.getUIDMeta(tsdb, UniqueIdType.METRIC, "000003")
      .joinUninterruptibly();
    assertEquals(UniqueIdType.METRIC, meta.getType());
    assertEquals("sys.cpu.2", meta.getName());
    assertEquals("000003", meta.getUID());
  }
  
  @Test
  public void getUIDMetaByte() throws Exception {
    meta = UIDMeta.getUIDMeta(tsdb, UniqueIdType.METRIC, new byte[] { 0, 0, 3 })
      .joinUninterruptibly();
    assertEquals(UniqueIdType.METRIC, meta.getType());
    assertEquals("sys.cpu.2", meta.getName());
    assertEquals("000003", meta.getUID());
  }
  
  @Test
  public void getUIDMetaExists() throws Exception {
    meta = UIDMeta.getUIDMeta(tsdb, UniqueIdType.METRIC, "000001")
      .joinUninterruptibly();
    assertEquals(UniqueIdType.METRIC, meta.getType());
    assertEquals("sys.cpu.0", meta.getName());
    assertEquals("000001", meta.getUID());
    assertEquals("MyNotes", meta.getNotes());
  }

  @Test (expected = NoSuchUniqueId.class)
  public void getUIDMetaNoSuch() throws Exception {
    UIDMeta.getUIDMeta(tsdb, UniqueIdType.METRIC, "000002")
      .joinUninterruptibly();
  }
  
  @Test
  public void delete() throws Exception {
    meta = UIDMeta.getUIDMeta(tsdb, UniqueIdType.METRIC, "000001")
      .joinUninterruptibly();
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
    meta.syncToStorage(tsdb, false).joinUninterruptibly();
    assertEquals("New Display Name", meta.getDisplayName());
    assertEquals("MyNotes", meta.getNotes());
    assertEquals(1328140801, meta.getCreated());
  }
  
  @Test
  public void syncToStorageOverwrite() throws Exception {
    meta = new UIDMeta(UniqueIdType.METRIC, "000001");
    meta.setDisplayName("New Display Name");
    meta.syncToStorage(tsdb, true).joinUninterruptibly();
    assertEquals("New Display Name", meta.getDisplayName());
    assertTrue(meta.getNotes().isEmpty());
  }
  
  @Test (expected = IllegalStateException.class)
  public void syncToStorageNoChanges() throws Exception {
    meta = UIDMeta.getUIDMeta(tsdb, UniqueIdType.METRIC, "000001")
      .joinUninterruptibly();
    meta.syncToStorage(tsdb, false).joinUninterruptibly();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void syncToStorageNullType() throws Exception {
    meta = new UIDMeta(null, "000001");
    meta.syncToStorage(tsdb, true).joinUninterruptibly();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void syncToStorageNullUID() throws Exception {
    meta = new UIDMeta(UniqueIdType.METRIC, null);
    meta.syncToStorage(tsdb, true).joinUninterruptibly();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void syncToStorageEmptyUID() throws Exception {
    meta = new UIDMeta(UniqueIdType.METRIC, "");
    meta.syncToStorage(tsdb, true).joinUninterruptibly();
  }
  
  @Test (expected = NoSuchUniqueId.class)
  public void syncToStorageNoSuch() throws Exception {
    meta = new UIDMeta(UniqueIdType.METRIC, "000002");
    meta.setDisplayName("Testing");
    meta.syncToStorage(tsdb, true).joinUninterruptibly();
  }

  @Test
  public void storeNew() throws Exception {
    meta = new UIDMeta(UniqueIdType.METRIC, new byte[] { 0, 0, 1 }, "sys.cpu.1");
    meta.setDisplayName("System CPU");
    meta.storeNew(tsdb).joinUninterruptibly();
    meta = JSON.parseToObject(storage.getColumn(new byte[] { 0, 0, 1 }, 
        NAME_FAMILY,
        "metric_meta".getBytes(MockBase.ASCII())), UIDMeta.class);
    assertEquals("System CPU", meta.getDisplayName());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void storeNewNoName() throws Exception {
    meta = new UIDMeta(UniqueIdType.METRIC, new byte[] { 0, 0, 1 }, "");
    meta.storeNew(tsdb).joinUninterruptibly();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void storeNewNullType() throws Exception {
    meta = new UIDMeta(null, new byte[] { 0, 0, 1 }, "sys.cpu.1");
    meta.storeNew(tsdb).joinUninterruptibly();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void storeNewEmptyUID() throws Exception {
    meta = new UIDMeta(UniqueIdType.METRIC, "");
    meta.storeNew(tsdb).joinUninterruptibly();
  }
}
