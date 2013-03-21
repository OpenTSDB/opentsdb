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

import java.util.ArrayList;
import java.util.HashMap;

import net.opentsdb.utils.JSON;

import org.junit.Test;

public final class TestTSMeta {
  TSMeta meta = new TSMeta();
  
  @Test
  public void constructor() { 
    assertNotNull(new TSMeta());
  }
  
  @Test
  public void tsuid() {
    meta.setTSUID("ABCD");
    assertEquals(meta.getTSUID(), "ABCD");
  }
  
  @Test
  public void metricNull() {
    assertNull(meta.getMetric());
  }
  
  @Test
  public void metric() {
    UIDMeta metric = new UIDMeta();
    metric.setUID("AB");
    meta.setMetric(metric);
    assertNotNull(meta.getMetric());
  }
  
  @Test
  public void tagsNull() {
    assertNull(meta.getTags());
  }
  
  @Test
  public void tags() {
    meta.setTags(new ArrayList<UIDMeta>());
    assertNotNull(meta.getTags());
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
  public void units() {
    meta.setUnits("%");
    assertEquals(meta.getUnits(), "%");
  }
  
  @Test
  public void dataType() {
    meta.setDataType("counter");
    assertEquals(meta.getDataType(), "counter");
  }
  
  @Test
  public void retention() {
    meta.setRetention(42);
    assertEquals(meta.getRetention(), 42);
  }
  
  @Test
  public void max() {
    meta.setMax(42.5);
    assertEquals(meta.getMax(), 42.5, 0.000001);
  }
  
  @Test
  public void min() {
    meta.setMin(142.5);
    assertEquals(meta.getMin(), 142.5, 0.000001);
  }
  
  @Test
  public void lastReceived() {
    meta.setLastReceived(1328140801L);
    assertEquals(meta.getLastReceived(), 1328140801L);
  }

  @Test
  public void serialize() throws Exception {
    assertNotNull(JSON.serializeToString(meta));
  }
  
  @Test
  public void deserialize() throws Exception {
    String json = "{\"tsuid\":\"ABCD\",\"metric\":null,\"tags\":null,\"" +
     "description\":\"Description\",\"notes\":\"Notes\",\"created\":1328140800," +
     "\"custom\":null,\"units\":\"\",\"retention\":42,\"max\":1.0,\"min\":" +
     "\"NaN\",\"displayName\":\"Display\",\"dataType\":\"Data\",\"lastReceived" +
     "\":1328140801}";
    TSMeta tsmeta = JSON.parseToObject(json, TSMeta.class);
    assertNotNull(tsmeta);
    assertEquals(tsmeta.getTSUID(), "ABCD");
  }
}
