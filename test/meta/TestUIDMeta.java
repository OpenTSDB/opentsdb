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

import net.opentsdb.core.TSDB;
import net.opentsdb.storage.MemoryStore;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.JSON;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Iterators;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static net.opentsdb.uid.UniqueIdType.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

@PowerMockIgnore({"javax.management.*", "javax.xml.*",
  "ch.qos.*", "org.slf4j.*",
  "com.sum.*", "org.xml.*"})
@RunWith(PowerMockRunner.class)
@PrepareForTest(UIDMeta.class)
public final class TestUIDMeta {
  private TSDB tsdb;
  private MemoryStore tsdb_store;
  private UIDMeta meta = new UIDMeta(METRIC, new byte[] {0, 0, 1});
  
  @Before
  public void before() throws Exception {
    tsdb_store = new MemoryStore();
    tsdb = new TSDB(tsdb_store, new Config(false));

    tsdb_store.allocateUID("sys.cpu.0", new byte[]{0, 0, 1}, METRIC);
    tsdb_store.allocateUID("sys.cpu.2", new byte[]{0, 0, 3}, METRIC);

    UIDMeta uidMeta = new UIDMeta(METRIC, new byte[] {0, 0, 1}, "sys.cpu.0");
    uidMeta.setDescription("Description");
    uidMeta.setNotes("MyNotes");
    uidMeta.setCreated(1328140801);
    uidMeta.setDisplayName("System CPU");

    tsdb_store.add(uidMeta);
  }
  
  @Test
  public void constructor2() {
    meta = new UIDMeta(METRIC, new byte[] {0, 0, 5});
    assertNotNull(meta);
    assertEquals(METRIC, meta.getType());
    assertArrayEquals(new byte[]{0, 0, 5}, meta.getUID());
  }
  
  @Test
  public void constructor3() {
    meta = new UIDMeta(METRIC, new byte[] {0, 0, 5}, "sys.cpu.5");
    assertNotNull(meta);
    assertEquals(METRIC, meta.getType());
    assertArrayEquals(new byte[]{0, 0, 5}, meta.getUID());
    assertEquals("sys.cpu.5", meta.getName());
    assertEquals(System.currentTimeMillis() / 1000, meta.getCreated());
  }

  @Test
  public void createConstructor() {
    PowerMockito.mockStatic(System.class);
    when(System.currentTimeMillis()).thenReturn(1357300800000L);
    meta = new UIDMeta(TAGK, new byte[] { 1, 0, 0 }, "host");
    assertEquals(1357300800000L / 1000, meta.getCreated());
    assertArrayEquals(new byte[]{1, 0, 0}, meta.getUID());
    assertEquals("host", meta.getName());
  }

  @Test (expected = NullPointerException.class)
  public void ctorNullType() throws Exception {
    meta = new UIDMeta(null, new byte[] {0}, null, false);
  }

  @Test (expected = NullPointerException.class)
  public void ctorNullUID() throws Exception {
    new UIDMeta(METRIC, null, null, false);
  }

  @Test (expected = IllegalArgumentException.class)
  public void ctorEmptyUID() throws Exception {
    new UIDMeta(METRIC, new byte[] {}, null, false);
  }

  @Test (expected = IllegalArgumentException.class)
  public void ctorWrongUIDLength() throws Exception {
    new UIDMeta(METRIC, new byte[] {0}, null, false);
    new UIDMeta(METRIC, new byte[] {0, 1, 2, 3}, null, false);
  }
 
  @Test
  public void serialize() throws Exception {
    final byte[] json = meta.getStorageJSON();

    ObjectMapper mapper = new ObjectMapper();
    ObjectNode rootNode = mapper.readValue(json, ObjectNode.class);

    assertEquals(6, Iterators.size(rootNode.fields()));
    assertEquals("METRIC", rootNode.get("type").asText());
    assertEquals(JsonNodeType.NULL, rootNode.get("displayName").getNodeType());
    assertEquals(JsonNodeType.NULL, rootNode.get("description").getNodeType());
    assertEquals(JsonNodeType.NULL, rootNode.get("notes").getNodeType());
    assertEquals(JsonNodeType.NULL, rootNode.get("custom").getNodeType());
    assertEquals(0, rootNode.get("created").longValue());
  }
  
  @Test
  public void deserialize() throws Exception {
    String json = "{\"uid\":\"ABCD\",\"type\":\"MeTriC\",\"name\":\"MyName\"," +
    "\"description\":\"Description\",\"notes\":\"MyNotes\",\"created\":" + 
    "1328140801,\"displayName\":\"Empty\",\"unknownkey\":null}";
    meta = UIDMeta.buildFromJSON(json.getBytes(), METRIC, new byte[] {0, (byte) 16,
            (byte) -125}, "MyName");
    assertNotNull(meta);
    assertArrayEquals(new byte[]{0, (byte) 16, (byte) -125}, meta.getUID());
    assertEquals(METRIC, meta.getType());
    assertEquals("MyNotes", meta.getNotes());
    assertEquals("Empty", meta.getDisplayName());
  }

  @Test
  public void getUIDMeta() throws Exception {
    meta = tsdb.getUIDMeta(METRIC, "000003")
      .joinUninterruptibly();
    assertEquals(METRIC, meta.getType());
    assertEquals("sys.cpu.2", meta.getName());
    assertArrayEquals(new byte[]{0, 0, 3}, meta.getUID());
  }
  
  @Test
  public void getUIDMetaByte() throws Exception {
    meta = tsdb.getUIDMeta(METRIC, new byte[] { 0, 0, 3 })
      .joinUninterruptibly();
    assertEquals(METRIC, meta.getType());
    assertEquals("sys.cpu.2", meta.getName());
    assertArrayEquals(new byte[]{0, 0, 3}, meta.getUID());
  }
  
  @Test
  public void getUIDMetaExists() throws Exception {
    meta = tsdb.getUIDMeta(METRIC, "000001")
      .joinUninterruptibly();
    assertEquals(METRIC, meta.getType());
    assertEquals("sys.cpu.0", meta.getName());
    assertArrayEquals(new byte[]{0, 0, 1}, meta.getUID());
    assertEquals("MyNotes", meta.getNotes());
  }

  @Test (expected = NoSuchUniqueId.class)
  public void getUIDMetaNoSuch() throws Exception {
    tsdb.getUIDMeta(METRIC, "000002")
      .joinUninterruptibly();
  }
  
  @Test
  public void delete() throws Exception {
    meta = tsdb.getUIDMeta(METRIC, "000001")
      .joinUninterruptibly();
    tsdb.delete(meta);
  }
  
  @Test
  public void syncToStorage() throws Exception {
    meta = new UIDMeta(METRIC, new byte[] {0, 0, 1});
    meta.setDisplayName("New Display Name");
    tsdb.syncUIDMetaToStorage(meta, false).joinUninterruptibly();
    assertEquals("New Display Name", meta.getDisplayName());
    assertEquals("MyNotes", meta.getNotes());
    assertEquals(1328140801, meta.getCreated());
  }
  
  @Test
  public void syncToStorageOverwrite() throws Exception {
    meta = new UIDMeta(METRIC, new byte[] {0, 0, 1});
    meta.setDisplayName("New Display Name");
    tsdb.syncUIDMetaToStorage(meta, true).joinUninterruptibly();
    assertEquals("New Display Name", meta.getDisplayName());
    assertNull(meta.getNotes());
  }
  
  @Test (expected = IllegalStateException.class)
  public void syncToStorageNoChanges() throws Exception {
    meta = tsdb.getUIDMeta(METRIC, "000001")
      .joinUninterruptibly();
    tsdb.syncUIDMetaToStorage(meta, false).joinUninterruptibly();
  }
  
  @Test (expected = NoSuchUniqueId.class)
  public void syncToStorageNoSuch() throws Exception {
    meta = new UIDMeta(METRIC, new byte[] {0, 0, 2});
    meta.setDisplayName("Testing");
    tsdb.syncUIDMetaToStorage(meta, true).joinUninterruptibly();
  }

  @Test
  public void storeNew() throws Exception {
    meta = new UIDMeta(METRIC, new byte[] { 0, 0, 1 }, "sys.cpu.1");
    meta.setDisplayName("System CPU");
    tsdb_store.add(meta).joinUninterruptibly();
    meta = tsdb_store.getMeta(new byte[] { 0, 0, 1 },meta.getName() ,METRIC)
            .joinUninterruptibly();

    assertEquals("System CPU", meta.getDisplayName());
  }
}
