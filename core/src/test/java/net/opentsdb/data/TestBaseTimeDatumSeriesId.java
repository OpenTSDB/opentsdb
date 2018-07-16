// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.data;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertNull;

import java.util.Map;

import org.junit.Test;

import com.google.common.collect.Maps;

import net.opentsdb.common.Const;

public class TestBaseTimeDatumSeriesId {
  
  @Test
  public void namespace() throws Exception {
    TimeSeriesDatumStringId id = BaseTimeSeriesDatumStringId.newBuilder()
        .setNamespace("Tyrell")
        .setMetric("sys.cpu.user")
        .build();
    assertEquals("Tyrell", id.namespace());
    
    id = BaseTimeSeriesDatumStringId.newBuilder()
        .setNamespace("")
        .setMetric("sys.cpu.user")
        .build();
    assertEquals("", id.namespace());
    
    id = BaseTimeSeriesDatumStringId.newBuilder()
        .setNamespace(null)
        .setMetric("sys.cpu.user")
        .build();
    assertNull(id.namespace());
    
    id = BaseTimeSeriesDatumStringId.newBuilder()
        .setMetric("sys.cpu.user")
        .build();
    assertNull(id.namespace());
  }
  
  @Test
  public void metric() throws Exception {
    TimeSeriesDatumStringId id = BaseTimeSeriesDatumStringId.newBuilder()
        .setMetric("sys.cpu.user")
        .build();
    assertEquals("sys.cpu.user", id.metric());
    
    try {
      id = BaseTimeSeriesDatumStringId.newBuilder()
          .setMetric("")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      id = BaseTimeSeriesDatumStringId.newBuilder()
          .setMetric(null)
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      id = BaseTimeSeriesDatumStringId.newBuilder()
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void tags() throws Exception {
    Map<String, String> tags = Maps.newHashMap();
    tags.put("host", "web01");
    tags.put("colo", "lax");
    TimeSeriesDatumStringId id = BaseTimeSeriesDatumStringId.newBuilder()
        .setTags(tags)
        .setMetric("sys.cpu.user")
        .build();
    assertEquals(2, id.tags().size());
    assertEquals("web01", 
        id.tags().get("host"));
    assertEquals("lax", 
        id.tags().get("colo"));
    
    tags = Maps.newHashMap();
    tags.put("colo", "");
    id = BaseTimeSeriesDatumStringId.newBuilder()
        .setTags(tags)
        .setMetric("sys.cpu.user")
        .build();
    assertEquals(1, id.tags().size());
    assertEquals("", 
        id.tags().get("colo"));
    
    tags = Maps.newHashMap();
    tags.put("colo", null);
    try {
      id = BaseTimeSeriesDatumStringId.newBuilder()
          .setTags(tags)
          .setMetric("sys.cpu.user")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    tags = Maps.newHashMap();
    tags.put("", "lax");
    id = BaseTimeSeriesDatumStringId.newBuilder()
        .setTags(tags)
        .setMetric("sys.cpu.user")
        .build();
    assertEquals(1, id.tags().size());
    assertEquals("lax", 
        id.tags().get(""));
    
    tags = Maps.newHashMap();
    tags.put(null, "lax");
    try {
      id = BaseTimeSeriesDatumStringId.newBuilder()
          .setTags(tags)
          .setMetric("sys.cpu.user")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    tags = Maps.newHashMap();
    id = BaseTimeSeriesDatumStringId.newBuilder()
        .setTags(tags)
        .setMetric("sys.cpu.user")
        .build();
    assertEquals(0, id.tags().size());
    
    id = BaseTimeSeriesDatumStringId.newBuilder()
        .setTags(null)
        .setMetric("sys.cpu.user")
        .build();
    assertEquals(0, id.tags().size());
    
    id = BaseTimeSeriesDatumStringId.newBuilder()
        .setMetric("sys.cpu.user")
        .build();
    assertEquals(0, id.tags().size());
    
    id = BaseTimeSeriesDatumStringId.newBuilder()
        .addTags("host", "web01")
        .addTags("colo", "lax")
        .setMetric("sys.cpu.user")
        .build();
    assertEquals(2, id.tags().size());
    assertEquals("web01", 
        id.tags().get("host"));
    assertEquals("lax", 
        id.tags().get("colo"));
    
    id = BaseTimeSeriesDatumStringId.newBuilder()
        .addTags("colo", "")
        .setMetric("sys.cpu.user")
        .build();
    assertEquals(1, id.tags().size());
    assertEquals("", 
        id.tags().get("colo"));
    
    try {
      id = BaseTimeSeriesDatumStringId.newBuilder()
          .addTags("colo", null)
          .setMetric("sys.cpu.user")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    id = BaseTimeSeriesDatumStringId.newBuilder()
        .addTags("", "lax")
        .setMetric("sys.cpu.user")
        .build();
    assertEquals(1, id.tags().size());
    assertEquals("lax", 
        id.tags().get(""));
    
    try {
      id = BaseTimeSeriesDatumStringId.newBuilder()
          .addTags(null, "lax")
          .setMetric("sys.cpu.user")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      id = BaseTimeSeriesDatumStringId.newBuilder()
          .addTags(null, null)
          .setMetric("sys.cpu.user")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void hashCodeEqualsCompareTo() throws Exception {
    Map<String, String> tags = Maps.newHashMap();
    tags.put("host", "web01");
    tags.put("colo", "lax");
    
    final BaseTimeSeriesDatumStringId id1 = BaseTimeSeriesDatumStringId.newBuilder()
        .setNamespace("OpenTSDB")
        .setMetric("sys.cpu.user")
        .setTags(tags)
        .build();
    
    BaseTimeSeriesDatumStringId id2 = BaseTimeSeriesDatumStringId.newBuilder()
        .setNamespace("OpenTSDB")
        .setMetric("sys.cpu.user")
        .setTags(tags)
        .build();
    assertEquals(id1.hashCode(), id2.hashCode());
    assertEquals(id1, id2);
    assertEquals(0, id1.compareTo(id2));
    
    id2 = BaseTimeSeriesDatumStringId.newBuilder()
        .setNamespace("Yahoo") // <-- Diff
        .setMetric("sys.cpu.user")
        .setTags(tags)
        .build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(-1, id1.compareTo(id2));
    
    id2 = BaseTimeSeriesDatumStringId.newBuilder()
        //.setNamespace("OpenTSDB") // <-- Diff
        .setMetric("sys.cpu.user")
        .setTags(tags)
        .build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(1, id1.compareTo(id2));
    
    id2 = BaseTimeSeriesDatumStringId.newBuilder()
        .setNamespace("OpenTSDB")
        .setMetric("sys.cpu.idle") // <-- Diff
        .setTags(tags)
        .build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(1, id1.compareTo(id2));
    
    Map<String, String> tags2 = Maps.newHashMap();
    tags.put("host", "web02"); // <-- Diff
    tags.put("colo", "lax");
    
    id2 = BaseTimeSeriesDatumStringId.newBuilder()
        .setNamespace("OpenTSDB")
        .setMetric("sys.cpu.user")
        .setTags(tags2) // <-- Diff
        .build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(-1, id1.compareTo(id2));
    
    tags2 = Maps.newHashMap();
    tags.put("host", "web01");
    //tags.put("colo", "lax"); // <-- Diff
    
    id2 = BaseTimeSeriesDatumStringId.newBuilder()
        .setNamespace("OpenTSDB")
        .setMetric("sys.cpu.user")
        .setTags(tags2) // <-- Diff
        .build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(-1, id1.compareTo(id2));
    
    id2 = BaseTimeSeriesDatumStringId.newBuilder()
        .setNamespace("OpenTSDB")
        .setMetric("sys.cpu.user")
        //.setTags(tags) // <-- Diff
        .build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(-1, id1.compareTo(id2));
  }

  @Test
  public void type() throws Exception {
    TimeSeriesDatumStringId id = BaseTimeSeriesDatumStringId.newBuilder()
        .setMetric("sys.cpu.user")
        .build();
    assertEquals(Const.TS_STRING_ID, id.type());
  }
}
