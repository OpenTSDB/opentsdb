// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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

public class TestBaseTimeSeriesId {

  @Test
  public void alias() throws Exception {
    TimeSeriesId id = BaseTimeSeriesId.newBuilder()
        .setAlias("MyID!")
        .setMetric("sys.cpu.user")
        .build();
    assertEquals("MyID!", id.alias());
    
    id = BaseTimeSeriesId.newBuilder()
        .setAlias("")
        .setMetric("sys.cpu.user")
        .build();
    assertEquals("", id.alias());
    
    id = BaseTimeSeriesId.newBuilder()
        .setAlias(null)
        .setMetric("sys.cpu.user")
        .build();
    assertNull(id.alias());
    
    id = BaseTimeSeriesId.newBuilder()
        .setMetric("sys.cpu.user")
        .build();
    assertNull(id.alias());
  }

  @Test
  public void namespace() throws Exception {
    TimeSeriesId id = BaseTimeSeriesId.newBuilder()
        .setNamespace("Tyrell")
        .setMetric("sys.cpu.user")
        .build();
    assertEquals("Tyrell", id.namespace());
    
    id = BaseTimeSeriesId.newBuilder()
        .setNamespace("")
        .setMetric("sys.cpu.user")
        .build();
    assertEquals("", id.namespace());
    
    id = BaseTimeSeriesId.newBuilder()
        .setNamespace(null)
        .setMetric("sys.cpu.user")
        .build();
    assertNull(id.namespace());
    
    id = BaseTimeSeriesId.newBuilder()
        .setMetric("sys.cpu.user")
        .build();
    assertNull(id.namespace());
  }
  
  @Test
  public void metric() throws Exception {
    TimeSeriesId id = BaseTimeSeriesId.newBuilder()
        .setMetric("sys.cpu.user")
        .build();
    assertEquals("sys.cpu.user", id.metric());
    
    try {
      id = BaseTimeSeriesId.newBuilder()
          .setMetric("")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      id = BaseTimeSeriesId.newBuilder()
          .setMetric(null)
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      id = BaseTimeSeriesId.newBuilder()
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void tags() throws Exception {
    Map<String, String> tags = Maps.newHashMap();
    tags.put("host", "web01");
    tags.put("colo", "lax");
    TimeSeriesId id = BaseTimeSeriesId.newBuilder()
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
    id = BaseTimeSeriesId.newBuilder()
        .setTags(tags)
        .setMetric("sys.cpu.user")
        .build();
    assertEquals(1, id.tags().size());
    assertEquals("", 
        id.tags().get("colo"));
    
    tags = Maps.newHashMap();
    tags.put("colo", null);
    try {
      id = BaseTimeSeriesId.newBuilder()
          .setTags(tags)
          .setMetric("sys.cpu.user")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    tags = Maps.newHashMap();
    tags.put("", "lax");
    id = BaseTimeSeriesId.newBuilder()
        .setTags(tags)
        .setMetric("sys.cpu.user")
        .build();
    assertEquals(1, id.tags().size());
    assertEquals("lax", 
        id.tags().get(""));
    
    tags = Maps.newHashMap();
    tags.put(null, "lax");
    try {
      id = BaseTimeSeriesId.newBuilder()
          .setTags(tags)
          .setMetric("sys.cpu.user")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    tags = Maps.newHashMap();
    id = BaseTimeSeriesId.newBuilder()
        .setTags(tags)
        .setMetric("sys.cpu.user")
        .build();
    assertEquals(0, id.tags().size());
    
    id = BaseTimeSeriesId.newBuilder()
        .setTags(null)
        .setMetric("sys.cpu.user")
        .build();
    assertEquals(0, id.tags().size());
    
    id = BaseTimeSeriesId.newBuilder()
        .setMetric("sys.cpu.user")
        .build();
    assertEquals(0, id.tags().size());
    
    id = BaseTimeSeriesId.newBuilder()
        .addTags("host", "web01")
        .addTags("colo", "lax")
        .setMetric("sys.cpu.user")
        .build();
    assertEquals(2, id.tags().size());
    assertEquals("web01", 
        id.tags().get("host"));
    assertEquals("lax", 
        id.tags().get("colo"));
    
    id = BaseTimeSeriesId.newBuilder()
        .addTags("colo", "")
        .setMetric("sys.cpu.user")
        .build();
    assertEquals(1, id.tags().size());
    assertEquals("", 
        id.tags().get("colo"));
    
    try {
      id = BaseTimeSeriesId.newBuilder()
          .addTags("colo", null)
          .setMetric("sys.cpu.user")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    id = BaseTimeSeriesId.newBuilder()
        .addTags("", "lax")
        .setMetric("sys.cpu.user")
        .build();
    assertEquals(1, id.tags().size());
    assertEquals("lax", 
        id.tags().get(""));
    
    try {
      id = BaseTimeSeriesId.newBuilder()
          .addTags(null, "lax")
          .setMetric("sys.cpu.user")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      id = BaseTimeSeriesId.newBuilder()
          .addTags(null, null)
          .setMetric("sys.cpu.user")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void aggregatedTags() throws Exception {
    TimeSeriesId id = BaseTimeSeriesId.newBuilder()
        .setAggregatedTags(Lists.newArrayList("Tyrell", "Frey", "Casterly"))
        .setMetric("sys.cpu.user")
        .build();
    assertEquals(3, id.aggregatedTags().size());
    assertEquals("Casterly", id.aggregatedTags().get(0));
    assertEquals("Frey", id.aggregatedTags().get(1));
    assertEquals("Tyrell", id.aggregatedTags().get(2));
    
    id = BaseTimeSeriesId.newBuilder()
        .setAggregatedTags(Lists.newArrayList("Tyrell", "", "Casterly"))
        .setMetric("sys.cpu.user")
        .build();
    assertEquals(3, id.aggregatedTags().size());
    assertEquals("", id.aggregatedTags().get(0));
    assertEquals("Casterly", id.aggregatedTags().get(1));
    assertEquals("Tyrell", id.aggregatedTags().get(2));
    
    try {
      id = BaseTimeSeriesId.newBuilder()
          .setAggregatedTags(Lists.newArrayList("Tyrell", null, "Casterly"))
          .setMetric("sys.cpu.user")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    id = BaseTimeSeriesId.newBuilder()
        .setAggregatedTags(new ArrayList<String>())
        .setMetric("sys.cpu.user")
        .build();
    assertTrue(id.aggregatedTags().isEmpty());
    
    id = BaseTimeSeriesId.newBuilder()
        .setAggregatedTags(null)
        .setMetric("sys.cpu.user")
        .build();
    assertTrue(id.aggregatedTags().isEmpty());
    
    id = BaseTimeSeriesId.newBuilder()
        .setMetric("sys.cpu.user")
        .build();
    assertTrue(id.aggregatedTags().isEmpty());
    
    id = BaseTimeSeriesId.newBuilder()
        .addAggregatedTag("Tyrell")
        .addAggregatedTag("Frey")
        .addAggregatedTag("Casterly")
        .setMetric("sys.cpu.user")
        .build();
    assertEquals(3, id.aggregatedTags().size());
    assertEquals("Casterly", id.aggregatedTags().get(0));
    assertEquals("Frey", id.aggregatedTags().get(1));
    assertEquals("Tyrell", id.aggregatedTags().get(2));
    
    id = BaseTimeSeriesId.newBuilder()
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
      id = BaseTimeSeriesId.newBuilder()
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
    TimeSeriesId id = BaseTimeSeriesId.newBuilder()
        .setDisjointTags(Lists.newArrayList("Tyrell", "Frey", "Casterly"))
        .setMetric("sys.cpu.user")
        .build();
    assertEquals(3, id.disjointTags().size());
    assertEquals("Casterly", id.disjointTags().get(0));
    assertEquals("Frey", id.disjointTags().get(1));
    assertEquals("Tyrell", id.disjointTags().get(2));
    
    id = BaseTimeSeriesId.newBuilder()
        .setDisjointTags(Lists.newArrayList("Tyrell", "", "Casterly"))
        .setMetric("sys.cpu.user")
        .build();
    assertEquals(3, id.disjointTags().size());
    assertEquals("", id.disjointTags().get(0));
    assertEquals("Casterly", id.disjointTags().get(1));
    assertEquals("Tyrell", id.disjointTags().get(2));
    
    try {
      id = BaseTimeSeriesId.newBuilder()
          .setDisjointTags(Lists.newArrayList("Tyrell", null, "Casterly"))
          .setMetric("sys.cpu.user")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    id = BaseTimeSeriesId.newBuilder()
        .setDisjointTags(new ArrayList<String>())
        .setMetric("sys.cpu.user")
        .build();
    assertTrue(id.disjointTags().isEmpty());
    
    id = BaseTimeSeriesId.newBuilder()
        .setDisjointTags(null)
        .setMetric("sys.cpu.user")
        .build();
    assertTrue(id.disjointTags().isEmpty());
    
    id = BaseTimeSeriesId.newBuilder()
        .setMetric("sys.cpu.user")
        .build();
    assertTrue(id.disjointTags().isEmpty());
    
    id = BaseTimeSeriesId.newBuilder()
        .addDisjointTag("Tyrell")
        .addDisjointTag("Frey")
        .addDisjointTag("Casterly")
        .setMetric("sys.cpu.user")
        .build();
    assertEquals(3, id.disjointTags().size());
    assertEquals("Casterly", id.disjointTags().get(0));
    assertEquals("Frey", id.disjointTags().get(1));
    assertEquals("Tyrell", id.disjointTags().get(2));
    
    id = BaseTimeSeriesId.newBuilder()
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
      id = BaseTimeSeriesId.newBuilder()
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
    TimeSeriesId id = BaseTimeSeriesId.newBuilder()
        .setUniqueId(ids)
        .setMetric("sys.cpu.user")
        .build();
    assertEquals(2, id.uniqueIds().size());
    assertTrue(id.uniqueIds().contains("000001"));
    assertTrue(id.uniqueIds().contains("000002"));
    
    id = BaseTimeSeriesId.newBuilder()
        .setUniqueId(Sets.newHashSet())
        .setMetric("sys.cpu.user")
        .build();
    assertTrue(id.uniqueIds().isEmpty());
    
    id = BaseTimeSeriesId.newBuilder()
        .setUniqueId(null)
        .setMetric("sys.cpu.user")
        .build();
    assertTrue(id.uniqueIds().isEmpty());
    
    id = BaseTimeSeriesId.newBuilder()
        .setMetric("sys.cpu.user")
        .build();
    assertTrue(id.uniqueIds().isEmpty());
    
    id = BaseTimeSeriesId.newBuilder()
        .addUniqueId("000002")
        .addUniqueId("000001")
        .setMetric("sys.cpu.user")
        .build();
    assertEquals(2, id.uniqueIds().size());
    assertTrue(id.uniqueIds().contains("000001"));
    assertTrue(id.uniqueIds().contains("000002"));
    
    try {
      id = BaseTimeSeriesId.newBuilder()
          .addUniqueId(null)
          .build();
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void hashCodeEqualsCompareTo() throws Exception {
    Map<String, String> tags = Maps.newHashMap();
    tags.put("host", "web01");
    tags.put("colo", "lax");
    
    final BaseTimeSeriesId id1 = BaseTimeSeriesId.newBuilder()
        .setAlias("FakeID")
        .setNamespace("OpenTSDB")
        .setMetric("sys.cpu.user")
        .setTags(tags)
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .addUniqueId("000001")
        .addUniqueId("000002")
        .build();
    
    BaseTimeSeriesId id2 = BaseTimeSeriesId.newBuilder()
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
    
    id2 = BaseTimeSeriesId.newBuilder()
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
    
    id2 = BaseTimeSeriesId.newBuilder()
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
    
    id2 = BaseTimeSeriesId.newBuilder()
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
    
    id2 = BaseTimeSeriesId.newBuilder()
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
    
    id2 = BaseTimeSeriesId.newBuilder()
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
    
    id2 = BaseTimeSeriesId.newBuilder()
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
    
    id2 = BaseTimeSeriesId.newBuilder()
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
    
    id2 = BaseTimeSeriesId.newBuilder()
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
    
    id2 = BaseTimeSeriesId.newBuilder()
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
    
    id2 = BaseTimeSeriesId.newBuilder()
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
    
    id2 = BaseTimeSeriesId.newBuilder()
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
    
    id2 = BaseTimeSeriesId.newBuilder()
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
    
    id2 = BaseTimeSeriesId.newBuilder()
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
    
    id2 = BaseTimeSeriesId.newBuilder()
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
    
    id2 = BaseTimeSeriesId.newBuilder()
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
    
    id2 = BaseTimeSeriesId.newBuilder()
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
}
