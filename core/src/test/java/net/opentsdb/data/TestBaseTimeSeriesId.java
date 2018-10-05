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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;

public class TestBaseTimeSeriesId {

  @Test
  public void alias() throws Exception {
    TimeSeriesStringId id = BaseTimeSeriesStringId.newBuilder()
        .setAlias("MyID!")
        .setMetric("sys.cpu.user")
        .build();
    assertEquals("MyID!", id.alias());
    
    id = BaseTimeSeriesStringId.newBuilder()
        .setAlias("")
        .setMetric("sys.cpu.user")
        .build();
    assertEquals("", id.alias());
    
    id = BaseTimeSeriesStringId.newBuilder()
        .setAlias(null)
        .setMetric("sys.cpu.user")
        .build();
    assertNull(id.alias());
    
    id = BaseTimeSeriesStringId.newBuilder()
        .setMetric("sys.cpu.user")
        .build();
    assertNull(id.alias());
  }

  @Test
  public void namespace() throws Exception {
    TimeSeriesStringId id = BaseTimeSeriesStringId.newBuilder()
        .setNamespace("Tyrell")
        .setMetric("sys.cpu.user")
        .build();
    assertEquals("Tyrell", id.namespace());
    
    id = BaseTimeSeriesStringId.newBuilder()
        .setNamespace("")
        .setMetric("sys.cpu.user")
        .build();
    assertEquals("", id.namespace());
    
    id = BaseTimeSeriesStringId.newBuilder()
        .setNamespace(null)
        .setMetric("sys.cpu.user")
        .build();
    assertNull(id.namespace());
    
    id = BaseTimeSeriesStringId.newBuilder()
        .setMetric("sys.cpu.user")
        .build();
    assertNull(id.namespace());
  }
  
  @Test
  public void metric() throws Exception {
    TimeSeriesStringId id = BaseTimeSeriesStringId.newBuilder()
        .setMetric("sys.cpu.user")
        .build();
    assertEquals("sys.cpu.user", id.metric());
    
    try {
      id = BaseTimeSeriesStringId.newBuilder()
          .setMetric("")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      id = BaseTimeSeriesStringId.newBuilder()
          .setMetric(null)
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      id = BaseTimeSeriesStringId.newBuilder()
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void tags() throws Exception {
    Map<String, String> tags = Maps.newHashMap();
    tags.put("host", "web01");
    tags.put("colo", "lax");
    TimeSeriesStringId id = BaseTimeSeriesStringId.newBuilder()
        .setTags(tags)
        .setMetric("sys.cpu.user")
        .build();
    assertEquals(2, id.tags().size());
    assertEquals("web01", 
        id.tags().get("host"));
    assertEquals("lax", 
        id.tags().get("colo"));
    
    tags = Maps.newHashMap();
    id = BaseTimeSeriesStringId.newBuilder()
        .setTags(tags)
        .setMetric("sys.cpu.user")
        .build();
    assertEquals(0, id.tags().size());
    
    id = BaseTimeSeriesStringId.newBuilder()
        .setTags(null)
        .setMetric("sys.cpu.user")
        .build();
    assertEquals(0, id.tags().size());
    
    id = BaseTimeSeriesStringId.newBuilder()
        .setMetric("sys.cpu.user")
        .build();
    assertEquals(0, id.tags().size());
    
    id = BaseTimeSeriesStringId.newBuilder()
        .addTags("host", "web01")
        .addTags("colo", "lax")
        .setMetric("sys.cpu.user")
        .build();
    assertEquals(2, id.tags().size());
    assertEquals("web01", 
        id.tags().get("host"));
    assertEquals("lax", 
        id.tags().get("colo"));
    
    id = BaseTimeSeriesStringId.newBuilder()
        .addTags("colo", "")
        .setMetric("sys.cpu.user")
        .build();
    assertEquals(1, id.tags().size());
    assertEquals("", 
        id.tags().get("colo"));
  }
  
  @Test
  public void aggregatedTags() throws Exception {
    TimeSeriesStringId id = BaseTimeSeriesStringId.newBuilder()
        .setAggregatedTags(Lists.newArrayList("Tyrell", "Frey", "Casterly"))
        .setMetric("sys.cpu.user")
        .build();
    assertEquals(3, id.aggregatedTags().size());
    assertEquals("Casterly", id.aggregatedTags().get(0));
    assertEquals("Frey", id.aggregatedTags().get(1));
    assertEquals("Tyrell", id.aggregatedTags().get(2));
    
    id = BaseTimeSeriesStringId.newBuilder()
        .setAggregatedTags(Lists.newArrayList("Tyrell", "", "Casterly"))
        .setMetric("sys.cpu.user")
        .build();
    assertEquals(3, id.aggregatedTags().size());
    assertEquals("", id.aggregatedTags().get(0));
    assertEquals("Casterly", id.aggregatedTags().get(1));
    assertEquals("Tyrell", id.aggregatedTags().get(2));
    
    try {
      id = BaseTimeSeriesStringId.newBuilder()
          .setAggregatedTags(Lists.newArrayList("Tyrell", null, "Casterly"))
          .setMetric("sys.cpu.user")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    id = BaseTimeSeriesStringId.newBuilder()
        .setAggregatedTags(new ArrayList<String>())
        .setMetric("sys.cpu.user")
        .build();
    assertTrue(id.aggregatedTags().isEmpty());
    
    id = BaseTimeSeriesStringId.newBuilder()
        .setAggregatedTags(null)
        .setMetric("sys.cpu.user")
        .build();
    assertTrue(id.aggregatedTags().isEmpty());
    
    id = BaseTimeSeriesStringId.newBuilder()
        .setMetric("sys.cpu.user")
        .build();
    assertTrue(id.aggregatedTags().isEmpty());
    
    id = BaseTimeSeriesStringId.newBuilder()
        .addAggregatedTag("Tyrell")
        .addAggregatedTag("Frey")
        .addAggregatedTag("Casterly")
        .setMetric("sys.cpu.user")
        .build();
    assertEquals(3, id.aggregatedTags().size());
    assertEquals("Casterly", id.aggregatedTags().get(0));
    assertEquals("Frey", id.aggregatedTags().get(1));
    assertEquals("Tyrell", id.aggregatedTags().get(2));
    
    id = BaseTimeSeriesStringId.newBuilder()
        .addAggregatedTag("Tyrell")
        .addAggregatedTag("")
        .addAggregatedTag("Casterly")
        .setMetric("sys.cpu.user")
        .build();
    assertEquals(3, id.aggregatedTags().size());
    assertEquals("", id.aggregatedTags().get(0));
    assertEquals("Casterly", id.aggregatedTags().get(1));
    assertEquals("Tyrell", id.aggregatedTags().get(2));
    
    try {
      id = BaseTimeSeriesStringId.newBuilder()
          .addAggregatedTag("Tyrell")
          .addAggregatedTag(null)
          .addAggregatedTag("Casterly")
          .setMetric("sys.cpu.user")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void disjointTags() throws Exception {
    TimeSeriesStringId id = BaseTimeSeriesStringId.newBuilder()
        .setDisjointTags(Lists.newArrayList("Tyrell", "Frey", "Casterly"))
        .setMetric("sys.cpu.user")
        .build();
    assertEquals(3, id.disjointTags().size());
    assertEquals("Casterly", id.disjointTags().get(0));
    assertEquals("Frey", id.disjointTags().get(1));
    assertEquals("Tyrell", id.disjointTags().get(2));
    
    id = BaseTimeSeriesStringId.newBuilder()
        .setDisjointTags(Lists.newArrayList("Tyrell", "", "Casterly"))
        .setMetric("sys.cpu.user")
        .build();
    assertEquals(3, id.disjointTags().size());
    assertEquals("", id.disjointTags().get(0));
    assertEquals("Casterly", id.disjointTags().get(1));
    assertEquals("Tyrell", id.disjointTags().get(2));
    
    try {
      id = BaseTimeSeriesStringId.newBuilder()
          .setDisjointTags(Lists.newArrayList("Tyrell", null, "Casterly"))
          .setMetric("sys.cpu.user")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    id = BaseTimeSeriesStringId.newBuilder()
        .setDisjointTags(new ArrayList<String>())
        .setMetric("sys.cpu.user")
        .build();
    assertTrue(id.disjointTags().isEmpty());
    
    id = BaseTimeSeriesStringId.newBuilder()
        .setDisjointTags(null)
        .setMetric("sys.cpu.user")
        .build();
    assertTrue(id.disjointTags().isEmpty());
    
    id = BaseTimeSeriesStringId.newBuilder()
        .setMetric("sys.cpu.user")
        .build();
    assertTrue(id.disjointTags().isEmpty());
    
    id = BaseTimeSeriesStringId.newBuilder()
        .addDisjointTag("Tyrell")
        .addDisjointTag("Frey")
        .addDisjointTag("Casterly")
        .setMetric("sys.cpu.user")
        .build();
    assertEquals(3, id.disjointTags().size());
    assertEquals("Casterly", id.disjointTags().get(0));
    assertEquals("Frey", id.disjointTags().get(1));
    assertEquals("Tyrell", id.disjointTags().get(2));
    
    id = BaseTimeSeriesStringId.newBuilder()
        .addDisjointTag("Tyrell")
        .addDisjointTag("")
        .addDisjointTag("Casterly")
        .setMetric("sys.cpu.user")
        .build();
    assertEquals(3, id.disjointTags().size());
    assertEquals("", id.disjointTags().get(0));
    assertEquals("Casterly", id.disjointTags().get(1));
    assertEquals("Tyrell", id.disjointTags().get(2));
    
    try {
      id = BaseTimeSeriesStringId.newBuilder()
          .addDisjointTag("Tyrell")
          .addDisjointTag(null)
          .addDisjointTag("Casterly")
          .setMetric("sys.cpu.user")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void uniqueIds() throws Exception {
    Set<String> ids = Sets.newHashSet("000001", "000002");
    TimeSeriesStringId id = BaseTimeSeriesStringId.newBuilder()
        .setUniqueId(ids)
        .setMetric("sys.cpu.user")
        .build();
    assertEquals(2, id.uniqueIds().size());
    assertTrue(id.uniqueIds().contains("000001"));
    assertTrue(id.uniqueIds().contains("000002"));
    
    id = BaseTimeSeriesStringId.newBuilder()
        .setUniqueId(Sets.newHashSet())
        .setMetric("sys.cpu.user")
        .build();
    assertTrue(id.uniqueIds().isEmpty());
    
    id = BaseTimeSeriesStringId.newBuilder()
        .setUniqueId(null)
        .setMetric("sys.cpu.user")
        .build();
    assertTrue(id.uniqueIds().isEmpty());
    
    id = BaseTimeSeriesStringId.newBuilder()
        .setMetric("sys.cpu.user")
        .build();
    assertTrue(id.uniqueIds().isEmpty());
    
    id = BaseTimeSeriesStringId.newBuilder()
        .addUniqueId("000002")
        .addUniqueId("000001")
        .setMetric("sys.cpu.user")
        .build();
    assertEquals(2, id.uniqueIds().size());
    assertTrue(id.uniqueIds().contains("000001"));
    assertTrue(id.uniqueIds().contains("000002"));
    
    try {
      id = BaseTimeSeriesStringId.newBuilder()
          .addUniqueId(null)
          .build();
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void hashCodeEqualsCompareTo() throws Exception {
    Map<String, String> tags = Maps.newHashMap();
    tags.put("host", "web01");
    tags.put("colo", "lax");
    
    final BaseTimeSeriesStringId id1 = BaseTimeSeriesStringId.newBuilder()
        .setAlias("FakeID")
        .setNamespace("OpenTSDB")
        .setMetric("sys.cpu.user")
        .setTags(tags)
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .addUniqueId("000001")
        .addUniqueId("000002")
        .build();
    
    BaseTimeSeriesStringId id2 = BaseTimeSeriesStringId.newBuilder()
        .setAlias("FakeID")
        .setNamespace("OpenTSDB")
        .setMetric("sys.cpu.user")
        .setTags(tags)
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .addUniqueId("000001")
        .addUniqueId("000002")
        .build();
    assertEquals(id1.hashCode(), id2.hashCode());
    assertEquals(id1, id2);
    assertEquals(0, id1.compareTo(id2));
    
    id2 = BaseTimeSeriesStringId.newBuilder()
        .setAlias("FakeID2") // <-- Diff
        .setNamespace("OpenTSDB")
        .setMetric("sys.cpu.user")
        .setTags(tags)
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .addUniqueId("000001")
        .addUniqueId("000002")
        .build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(-1, id1.compareTo(id2));
    
    id2 = BaseTimeSeriesStringId.newBuilder()
        .setAlias("FakeID")
        .setNamespace("Yahoo") // <-- Diff
        .setMetric("sys.cpu.user")
        .setTags(tags)
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .addUniqueId("000001")
        .addUniqueId("000002")
        .build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(-1, id1.compareTo(id2));
    
    id2 = BaseTimeSeriesStringId.newBuilder()
        .setAlias("FakeID")
        //.setNamespace("OpenTSDB") // <-- Diff
        .setMetric("sys.cpu.user")
        .setTags(tags)
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .addUniqueId("000001")
        .addUniqueId("000002")
        .build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(1, id1.compareTo(id2));
    
    id2 = BaseTimeSeriesStringId.newBuilder()
        .setAlias("FakeID")
        .setNamespace("OpenTSDB")
        .setMetric("sys.cpu.idle") // <-- Diff
        .setTags(tags)
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .addUniqueId("000001")
        .addUniqueId("000002")
        .build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(1, id1.compareTo(id2));
    
    Map<String, String> tags2 = Maps.newHashMap();
    tags.put("host", "web02"); // <-- Diff
    tags.put("colo", "lax");
    
    id2 = BaseTimeSeriesStringId.newBuilder()
        .setAlias("FakeID")
        .setNamespace("OpenTSDB")
        .setMetric("sys.cpu.user")
        .setTags(tags2) // <-- Diff
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .addUniqueId("000001")
        .addUniqueId("000002")
        .build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(-1, id1.compareTo(id2));
    
    tags2 = Maps.newHashMap();
    tags.put("host", "web01");
    //tags.put("colo", "lax"); // <-- Diff
    
    id2 = BaseTimeSeriesStringId.newBuilder()
        .setAlias("FakeID")
        .setNamespace("OpenTSDB")
        .setMetric("sys.cpu.user")
        .setTags(tags2) // <-- Diff
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .addUniqueId("000001")
        .addUniqueId("000002")
        .build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(-1, id1.compareTo(id2));
    
    id2 = BaseTimeSeriesStringId.newBuilder()
        .setAlias("FakeID")
        .setNamespace("OpenTSDB")
        .setMetric("sys.cpu.user")
        //.setTags(tags) // <-- Diff
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .addUniqueId("000001")
        .addUniqueId("000002")
        .build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(-1, id1.compareTo(id2));
    
    id2 = BaseTimeSeriesStringId.newBuilder()
        .setAlias("FakeID")
        .setNamespace("OpenTSDB")
        .setMetric("sys.cpu.user")
        .setTags(tags)
        .setAggregatedTags(Lists.newArrayList("owner")) // <-- Diff
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .addUniqueId("000001")
        .addUniqueId("000002")
        .build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(1, id1.compareTo(id2));
    
    id2 = BaseTimeSeriesStringId.newBuilder()
        .setAlias("FakeID")
        .setNamespace("OpenTSDB")
        .setMetric("sys.cpu.user")
        .setTags(tags)
        //.setAggregatedTags(Lists.newArrayList("owner", "role")) // <-- Diff
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .addUniqueId("000001")
        .addUniqueId("000002")
        .build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(1, id1.compareTo(id2));
    
    id2 = BaseTimeSeriesStringId.newBuilder()
        .setAlias("FakeID")
        .setNamespace("OpenTSDB")
        .setMetric("sys.cpu.user")
        .setTags(tags)
        .setAggregatedTags(Lists.newArrayList("role", "owner")) // <-- Diff order OK!
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .addUniqueId("000001")
        .addUniqueId("000002")
        .build();
    assertEquals(id1.hashCode(), id2.hashCode());
    assertEquals(id1, id2);
    assertEquals(0, id1.compareTo(id2));
    
    id2 = BaseTimeSeriesStringId.newBuilder()
        .setAlias("FakeID")
        .setNamespace("OpenTSDB")
        .setMetric("sys.cpu.user")
        .setTags(tags)
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("propery"))  // <-- Diff
        .addUniqueId("000001")
        .addUniqueId("000002")
        .build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(1, id1.compareTo(id2));
    
    id2 = BaseTimeSeriesStringId.newBuilder()
        .setAlias("FakeID")
        .setNamespace("OpenTSDB")
        .setMetric("sys.cpu.user")
        .setTags(tags)
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        //.setDisjointTags(Lists.newArrayList("propery", "type"))  // <-- Diff
        .addUniqueId("000001")
        .addUniqueId("000002")
        .build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(1, id1.compareTo(id2));
    
    id2 = BaseTimeSeriesStringId.newBuilder()
        .setAlias("FakeID")
        .setNamespace("OpenTSDB")
        .setMetric("sys.cpu.user")
        .setTags(tags) 
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("type", "propery")) // <-- Diff order OK!
        .addUniqueId("000001")
        .addUniqueId("000002")
        .build();
    assertEquals(id1.hashCode(), id2.hashCode());
    assertEquals(id1, id2);
    assertEquals(0, id1.compareTo(id2));
    
    id2 = BaseTimeSeriesStringId.newBuilder()
        .setAlias("FakeID")
        .setNamespace("OpenTSDB")
        .setMetric("sys.cpu.user")
        .setTags(tags) 
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .addUniqueId("000001")
        //.addUniqueId("000002")  // <-- Diff
        .build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(1, id1.compareTo(id2));
    
    id2 = BaseTimeSeriesStringId.newBuilder()
        .setAlias("FakeID")
        .setNamespace("OpenTSDB")
        .setMetric("sys.cpu.user")
        .setTags(tags) 
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        //.addUniqueId("000001")
        //.addUniqueId("000002")  // <-- Diff
        .build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(1, id1.compareTo(id2));
    
    id2 = BaseTimeSeriesStringId.newBuilder()
        .setAlias("FakeID")
        .setNamespace("OpenTSDB")
        .setMetric("sys.cpu.user")
        .setTags(tags) 
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .addUniqueId("000002") // <-- Diff order OK!
        .addUniqueId("000001")
        .build();
    assertEquals(id1.hashCode(), id2.hashCode());
    assertEquals(id1, id2);
    assertEquals(0, id1.compareTo(id2));
  }

  @Test
  public void type() throws Exception {
    TimeSeriesStringId id = BaseTimeSeriesStringId.newBuilder()
        .setAlias("MyID!")
        .setMetric("sys.cpu.user")
        .build();
    assertEquals(TypeToken.of(TimeSeriesStringId.class), id.type());
  }
}
