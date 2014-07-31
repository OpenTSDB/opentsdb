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

import net.opentsdb.core.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.storage.MemoryStore;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.uid.UniqueId.UniqueIdType;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.JSON;

import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

@PowerMockIgnore({"javax.management.*", "javax.xml.*",
  "ch.qos.*", "org.slf4j.*",
  "com.sum.*", "org.xml.*"})
@RunWith(PowerMockRunner.class)
@PrepareForTest({
  KeyValue.class,
  Scanner.class, UIDMeta.class})
public final class TestUIDMeta {
  private static byte[] NAME_FAMILY = "name".getBytes(Const.CHARSET_ASCII);
  private TSDB tsdb;
  private MemoryStore tsdb_store;
  private UIDMeta meta = new UIDMeta();
  
  @Before
  public void before() throws Exception {
    tsdb_store = new MemoryStore();
    tsdb = new TSDB(tsdb_store, new Config(false));

    tsdb_store.allocateUID(
            "sys.cpu.0".getBytes(Const.CHARSET_ASCII),
            new byte[]{0, 0, 1},
            UniqueIdType.METRIC);

    tsdb_store.allocateUID(
            "sys.cpu.2".getBytes(Const.CHARSET_ASCII),
            new byte[]{0, 0, 3},
            UniqueIdType.METRIC);

    UIDMeta uidMeta = new UIDMeta(
            UniqueIdType.METRIC,
            new byte[] {0, 0, 1},
            "sys.cpu.0");

    uidMeta.setDescription("Description");
    uidMeta.setNotes("MyNotes");
    uidMeta.setCreated(1328140801);;
    uidMeta.setDisplayName("System CPU");

    tsdb_store.add(uidMeta);
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
    meta = tsdb.getUIDMeta(UniqueIdType.METRIC, "000003")
      .joinUninterruptibly();
    assertEquals(UniqueIdType.METRIC, meta.getType());
    assertEquals("sys.cpu.2", meta.getName());
    assertEquals("000003", meta.getUID());
  }
  
  @Test
  public void getUIDMetaByte() throws Exception {
    meta = tsdb.getUIDMeta(UniqueIdType.METRIC, new byte[] { 0, 0, 3 })
      .joinUninterruptibly();
    assertEquals(UniqueIdType.METRIC, meta.getType());
    assertEquals("sys.cpu.2", meta.getName());
    assertEquals("000003", meta.getUID());
  }
  
  @Test
  public void getUIDMetaExists() throws Exception {
    meta = tsdb.getUIDMeta(UniqueIdType.METRIC, "000001")
      .joinUninterruptibly();
    assertEquals(UniqueIdType.METRIC, meta.getType());
    assertEquals("sys.cpu.0", meta.getName());
    assertEquals("000001", meta.getUID());
    assertEquals("MyNotes", meta.getNotes());
  }

  @Test (expected = NoSuchUniqueId.class)
  public void getUIDMetaNoSuch() throws Exception {
    tsdb.getUIDMeta(UniqueIdType.METRIC, "000002")
      .joinUninterruptibly();
  }
  
  @Test
  public void delete() throws Exception {
    meta = tsdb.getUIDMeta(UniqueIdType.METRIC, "000001")
      .joinUninterruptibly();
    tsdb.delete(meta);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void deleteNullType() throws Exception {
    meta = new UIDMeta(null, "000001");
    tsdb.delete(meta);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void deleteNullUID() throws Exception {
    meta = new UIDMeta(UniqueIdType.METRIC, null);
    tsdb.delete(meta);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void deleteEmptyUID() throws Exception {
    meta = new UIDMeta(UniqueIdType.METRIC, "");
    tsdb.delete(meta);
  }
  
  @Test
  public void syncToStorage() throws Exception {
    meta = new UIDMeta(UniqueIdType.METRIC, "000001");
    meta.setDisplayName("New Display Name");
    tsdb.syncUIDMetaToStorage(meta, false).joinUninterruptibly();
    assertEquals("New Display Name", meta.getDisplayName());
    assertEquals("MyNotes", meta.getNotes());
    assertEquals(1328140801, meta.getCreated());
  }
  
  @Test
  public void syncToStorageOverwrite() throws Exception {
    meta = new UIDMeta(UniqueIdType.METRIC, "000001");
    meta.setDisplayName("New Display Name");
    tsdb.syncUIDMetaToStorage(meta, true).joinUninterruptibly();
    assertEquals("New Display Name", meta.getDisplayName());
    assertTrue(meta.getNotes().isEmpty());
  }
  
  @Test (expected = IllegalStateException.class)
  public void syncToStorageNoChanges() throws Exception {
    meta = tsdb.getUIDMeta(UniqueIdType.METRIC, "000001")
      .joinUninterruptibly();
    tsdb.syncUIDMetaToStorage(meta, false).joinUninterruptibly();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void syncToStorageNullType() throws Exception {
    meta = new UIDMeta(null, "000001");
    tsdb.syncUIDMetaToStorage(meta, true).joinUninterruptibly();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void syncToStorageNullUID() throws Exception {
    meta = new UIDMeta(UniqueIdType.METRIC, null);
    tsdb.syncUIDMetaToStorage(meta, true).joinUninterruptibly();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void syncToStorageEmptyUID() throws Exception {
    meta = new UIDMeta(UniqueIdType.METRIC, "");
    tsdb.syncUIDMetaToStorage(meta, true).joinUninterruptibly();
  }
  
  @Test (expected = NoSuchUniqueId.class)
  public void syncToStorageNoSuch() throws Exception {
    meta = new UIDMeta(UniqueIdType.METRIC, "000002");
    meta.setDisplayName("Testing");
    tsdb.syncUIDMetaToStorage(meta, true).joinUninterruptibly();
  }

  @Test
  public void storeNew() throws Exception {
    meta = new UIDMeta(UniqueIdType.METRIC, new byte[] { 0, 0, 1 }, "sys.cpu.1");
    meta.setDisplayName("System CPU");
    tsdb_store.add(meta).joinUninterruptibly();
    meta = JSON.parseToObject(tsdb_store.getColumn(new byte[] { 0, 0, 1 },
        NAME_FAMILY,
        "metric_meta".getBytes(Const.CHARSET_ASCII)), UIDMeta.class);
    assertEquals("System CPU", meta.getDisplayName());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void storeNewNoName() throws Exception {
    meta = new UIDMeta(UniqueIdType.METRIC, new byte[] { 0, 0, 1 }, "");
    tsdb_store.add(meta).joinUninterruptibly();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void storeNewNullType() throws Exception {
    meta = new UIDMeta(null, new byte[] { 0, 0, 1 }, "sys.cpu.1");
    tsdb_store.add(meta).joinUninterruptibly();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void storeNewEmptyUID() throws Exception {
    meta = new UIDMeta(UniqueIdType.METRIC, "");
    tsdb_store.add(meta).joinUninterruptibly();
  }
}
