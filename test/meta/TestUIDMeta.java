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
import static org.junit.Assert.assertNull;

import java.util.HashMap;

import net.opentsdb.utils.JSON;

import org.junit.Test;

public final class TestUIDMeta {
  UIDMeta meta = new UIDMeta();
  
  @Test
  public void constructor() {
    assertNotNull(new UIDMeta());
  }
  
  @Test
  public void uid() {
    meta.setUID("AB");
    assertEquals(meta.getUID(), "AB");
  }
  
  @Test
  public void type() {
    meta.setType(2);
    assertEquals(meta.getType(), 2);
  }
  
  @Test
  public void name() {
    meta.setName("Metric");
    assertEquals(meta.getName(), "Metric");
  }
  
  @Test
  public void displayName() {
    meta.setDisplayName("Display");
    assertEquals(meta.getDisplayName(), "Display");
  }
  
  @Test
  public void description() {
    meta.setDescription("Description");
    assertEquals(meta.getDescription(), "Description");
  }
  
  @Test
  public void notes() {
    meta.setNotes("Notes");
    assertEquals(meta.getNotes(), "Notes");
  }
  
  @Test
  public void created() {
    meta.setCreated(1328140800L);
    assertEquals(meta.getCreated(), 1328140800L);
  }
  
  @Test
  public void customNull() {
    assertNull(meta.getCustom());
  }
  
  @Test
  public void custom() {
    HashMap<String, String> custom_tags = new HashMap<String, String>();
    custom_tags.put("key", "MyVal");
    meta.setCustom(custom_tags);
    assertNotNull(meta.getCustom());
    assertEquals(meta.getCustom().get("key"), "MyVal");
  }
  
  @Test
  public void serialize() throws Exception {
    assertNotNull(JSON.serializeToString(meta));
  }
  
  @Test
  public void deserialize() throws Exception {
    String json = "{\"uid\":\"ABCD\",\"type\":2,\"name\":\"MyName\"," +
    "\"description\":\"Description\",\"notes\":\"MyNotes\",\"created\":" + 
    "1328140801,\"custom\":null,\"displayName\":\"Empty\"}";
    UIDMeta uidmeta = JSON.parseToObject(json, UIDMeta.class);
    assertNotNull(uidmeta);
    assertEquals(uidmeta.getUID(), "ABCD");
  }
}
