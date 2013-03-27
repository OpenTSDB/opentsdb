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

public final class TestAnnotation {
  private final Annotation note = new Annotation();
  
  @Test
  public void constructor() {
    assertNotNull(new Annotation());
  }
  
  @Test
  public void tsuid() {
    note.setTSUID("ABCD");
    assertEquals(note.getTSUID(), "ABCD");
  }
  
  @Test
  public void starttime() {
    note.setStartTime(1328140800L);
    assertEquals(note.getStartTime(), 1328140800L);
  }
  
  @Test
  public void endtime() {
    note.setEndTime(1328140801L);
    assertEquals(note.getEndTime(), 1328140801L);
  }
  
  @Test
  public void description() {
    note.setDescription("MyDescription");
    assertEquals(note.getDescription(), "MyDescription");
  }
  
  @Test
  public void notes() {
    note.setNotes("Notes");
    assertEquals(note.getNotes(), "Notes");
  }
  
  @Test
  public void customNull() {
    assertNull(note.getCustom());
  }
  
  @Test
  public void custom() {
    HashMap<String, String> custom_tags = new HashMap<String, String>();
    custom_tags.put("key", "MyVal");
    note.setCustom(custom_tags);
    assertNotNull(note.getCustom());
    assertEquals(note.getCustom().get("key"), "MyVal");
  }

  @Test
  public void serialize() throws Exception {
    assertNotNull(JSON.serializeToString(note));
  }
  
  @Test
  public void deserialize() throws Exception {
    String json = "{\"tsuid\":\"ABCD\",\"description\":\"Description\"," + 
    "\"notes\":\"Notes\",\"custom\":null,\"endTime\":1328140801,\"startTime" + 
    "\":1328140800}";
    Annotation note = JSON.parseToObject(json, Annotation.class);
    assertNotNull(note);
    assertEquals(note.getTSUID(), "ABCD");
  }
}
