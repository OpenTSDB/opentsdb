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
        .build();
    assertEquals("MyID!", id.alias());
    
    id = BaseTimeSeriesId.newBuilder()
        .setAlias("")
        .build();
    assertEquals("", id.alias());
    
    id = BaseTimeSeriesId.newBuilder()
        .setAlias(null)
        .build();
    assertNull(id.alias());
    
    id = BaseTimeSeriesId.newBuilder()
        .build();
    assertNull(id.alias());
  }

  @Test
  public void namespaces() throws Exception {
    TimeSeriesId id = BaseTimeSeriesId.newBuilder()
        .setNamespaces(Lists.newArrayList("Tyrell", "Frey", "Casterly"))
        .build();
    assertEquals(3, id.namespaces().size());
    assertEquals("Casterly", id.namespaces().get(0));
    assertEquals("Frey", id.namespaces().get(1));
    assertEquals("Tyrell", id.namespaces().get(2));
    
    id = BaseTimeSeriesId.newBuilder()
        .setNamespaces(Lists.newArrayList("Tyrell", "", "Casterly"))
        .build();
    assertEquals(3, id.namespaces().size());
    assertEquals("", id.namespaces().get(0));
    assertEquals("Casterly", id.namespaces().get(1));
    assertEquals("Tyrell", id.namespaces().get(2));
    
    try {
      id = BaseTimeSeriesId.newBuilder()
          .setNamespaces(Lists.newArrayList("Tyrell", null, "Casterly"))
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    id = BaseTimeSeriesId.newBuilder()
        .setNamespaces(new ArrayList<String>())
        .build();
    assertTrue(id.namespaces().isEmpty());
    
    id = BaseTimeSeriesId.newBuilder()
        .setNamespaces(null)
        .build();
    assertTrue(id.namespaces().isEmpty());
    
    id = BaseTimeSeriesId.newBuilder()
        .build();
    assertTrue(id.namespaces().isEmpty());
    
    id = BaseTimeSeriesId.newBuilder()
        .addNamespace("Tyrell")
        .addNamespace("Frey")
        .addNamespace("Casterly")
        .build();
    assertEquals(3, id.namespaces().size());
    assertEquals("Casterly", id.namespaces().get(0));
    assertEquals("Frey", id.namespaces().get(1));
    assertEquals("Tyrell", id.namespaces().get(2));
    
    id = BaseTimeSeriesId.newBuilder()
        .addNamespace("Tyrell")
        .addNamespace("")
        .addNamespace("Casterly")
        .build();
    assertEquals(3, id.namespaces().size());
    assertEquals("", id.namespaces().get(0));
    assertEquals("Casterly", id.namespaces().get(1));
    assertEquals("Tyrell", id.namespaces().get(2));
    
    try {
      id = BaseTimeSeriesId.newBuilder()
          .addNamespace("Tyrell")
          .addNamespace(null)
          .addNamespace("Casterly")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void metrics() throws Exception {
    TimeSeriesId id = BaseTimeSeriesId.newBuilder()
        .setMetrics(Lists.newArrayList("Tyrell", "Frey", "Casterly"))
        .build();
    assertEquals(3, id.metrics().size());
    assertEquals("Casterly", id.metrics().get(0));
    assertEquals("Frey", id.metrics().get(1));
    assertEquals("Tyrell", id.metrics().get(2));
    
    id = BaseTimeSeriesId.newBuilder()
        .setMetrics(Lists.newArrayList("Tyrell", "", "Casterly"))
        .build();
    assertEquals(3, id.metrics().size());
    assertEquals("", id.metrics().get(0));
    assertEquals("Casterly", id.metrics().get(1));
    assertEquals("Tyrell", id.metrics().get(2));
    
    try {
      id = BaseTimeSeriesId.newBuilder()
          .setMetrics(Lists.newArrayList("Tyrell", null, "Casterly"))
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    id = BaseTimeSeriesId.newBuilder()
        .setMetrics(new ArrayList<String>())
        .build();
    assertTrue(id.metrics().isEmpty());
    
    id = BaseTimeSeriesId.newBuilder()
        .setMetrics(null)
        .build();
    assertTrue(id.metrics().isEmpty());
    
    id = BaseTimeSeriesId.newBuilder()
        .build();
    assertTrue(id.metrics().isEmpty());
    
    id = BaseTimeSeriesId.newBuilder()
        .addMetric("Tyrell")
        .addMetric("Frey")
        .addMetric("Casterly")
        .build();
    assertEquals(3, id.metrics().size());
    assertEquals("Casterly", id.metrics().get(0));
    assertEquals("Frey", id.metrics().get(1));
    assertEquals("Tyrell", id.metrics().get(2));
    
    id = BaseTimeSeriesId.newBuilder()
        .addMetric("Tyrell")
        .addMetric("")
        .addMetric("Casterly")
        .build();
    assertEquals(3, id.metrics().size());
    assertEquals("", id.metrics().get(0));
    assertEquals("Casterly", id.metrics().get(1));
    assertEquals("Tyrell", id.metrics().get(2));
    
    try {
      id = BaseTimeSeriesId.newBuilder()
          .addMetric("Tyrell")
          .addMetric(null)
          .addMetric("Casterly")
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
        .build();
    assertEquals(1, id.tags().size());
    assertEquals("", 
        id.tags().get("colo"));
    
    tags = Maps.newHashMap();
    tags.put("colo", null);
    try {
      id = BaseTimeSeriesId.newBuilder()
          .setTags(tags)
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    tags = Maps.newHashMap();
    tags.put("", "lax");
    id = BaseTimeSeriesId.newBuilder()
        .setTags(tags)
        .build();
    assertEquals(1, id.tags().size());
    assertEquals("lax", 
        id.tags().get(""));
    
    tags = Maps.newHashMap();
    tags.put(null, "lax");
    try {
      id = BaseTimeSeriesId.newBuilder()
          .setTags(tags)
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    tags = Maps.newHashMap();
    id = BaseTimeSeriesId.newBuilder()
        .setTags(tags)
        .build();
    assertEquals(0, id.tags().size());
    
    id = BaseTimeSeriesId.newBuilder()
        .setTags(null)
        .build();
    assertEquals(0, id.tags().size());
    
    id = BaseTimeSeriesId.newBuilder()
        .build();
    assertEquals(0, id.tags().size());
    
    id = BaseTimeSeriesId.newBuilder()
        .addTags("host", "web01")
        .addTags("colo", "lax")
        .build();
    assertEquals(2, id.tags().size());
    assertEquals("web01", 
        id.tags().get("host"));
    assertEquals("lax", 
        id.tags().get("colo"));
    
    id = BaseTimeSeriesId.newBuilder()
        .addTags("colo", "")
        .build();
    assertEquals(1, id.tags().size());
    assertEquals("", 
        id.tags().get("colo"));
    
    try {
      id = BaseTimeSeriesId.newBuilder()
          .addTags("colo", null)
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    id = BaseTimeSeriesId.newBuilder()
        .addTags("", "lax")
        .build();
    assertEquals(1, id.tags().size());
    assertEquals("lax", 
        id.tags().get(""));
    
    try {
      id = BaseTimeSeriesId.newBuilder()
          .addTags(null, "lax")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      id = BaseTimeSeriesId.newBuilder()
          .addTags(null, null)
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void aggregatedTags() throws Exception {
    TimeSeriesId id = BaseTimeSeriesId.newBuilder()
        .setAggregatedTags(Lists.newArrayList("Tyrell", "Frey", "Casterly"))
        .build();
    assertEquals(3, id.aggregatedTags().size());
    assertEquals("Casterly", id.aggregatedTags().get(0));
    assertEquals("Frey", id.aggregatedTags().get(1));
    assertEquals("Tyrell", id.aggregatedTags().get(2));
    
    id = BaseTimeSeriesId.newBuilder()
        .setAggregatedTags(Lists.newArrayList("Tyrell", "", "Casterly"))
        .build();
    assertEquals(3, id.aggregatedTags().size());
    assertEquals("", id.aggregatedTags().get(0));
    assertEquals("Casterly", id.aggregatedTags().get(1));
    assertEquals("Tyrell", id.aggregatedTags().get(2));
    
    try {
      id = BaseTimeSeriesId.newBuilder()
          .setAggregatedTags(Lists.newArrayList("Tyrell", null, "Casterly"))
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    id = BaseTimeSeriesId.newBuilder()
        .setAggregatedTags(new ArrayList<String>())
        .build();
    assertTrue(id.aggregatedTags().isEmpty());
    
    id = BaseTimeSeriesId.newBuilder()
        .setAggregatedTags(null)
        .build();
    assertTrue(id.aggregatedTags().isEmpty());
    
    id = BaseTimeSeriesId.newBuilder()
        .build();
    assertTrue(id.aggregatedTags().isEmpty());
    
    id = BaseTimeSeriesId.newBuilder()
        .addAggregatedTag("Tyrell")
        .addAggregatedTag("Frey")
        .addAggregatedTag("Casterly")
        .build();
    assertEquals(3, id.aggregatedTags().size());
    assertEquals("Casterly", id.aggregatedTags().get(0));
    assertEquals("Frey", id.aggregatedTags().get(1));
    assertEquals("Tyrell", id.aggregatedTags().get(2));
    
    id = BaseTimeSeriesId.newBuilder()
        .addAggregatedTag("Tyrell")
        .addAggregatedTag("")
        .addAggregatedTag("Casterly")
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
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void disjointTags() throws Exception {
    TimeSeriesId id = BaseTimeSeriesId.newBuilder()
        .setDisjointTags(Lists.newArrayList("Tyrell", "Frey", "Casterly"))
        .build();
    assertEquals(3, id.disjointTags().size());
    assertEquals("Casterly", id.disjointTags().get(0));
    assertEquals("Frey", id.disjointTags().get(1));
    assertEquals("Tyrell", id.disjointTags().get(2));
    
    id = BaseTimeSeriesId.newBuilder()
        .setDisjointTags(Lists.newArrayList("Tyrell", "", "Casterly"))
        .build();
    assertEquals(3, id.disjointTags().size());
    assertEquals("", id.disjointTags().get(0));
    assertEquals("Casterly", id.disjointTags().get(1));
    assertEquals("Tyrell", id.disjointTags().get(2));
    
    try {
      id = BaseTimeSeriesId.newBuilder()
          .setDisjointTags(Lists.newArrayList("Tyrell", null, "Casterly"))
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    id = BaseTimeSeriesId.newBuilder()
        .setDisjointTags(new ArrayList<String>())
        .build();
    assertTrue(id.disjointTags().isEmpty());
    
    id = BaseTimeSeriesId.newBuilder()
        .setDisjointTags(null)
        .build();
    assertTrue(id.disjointTags().isEmpty());
    
    id = BaseTimeSeriesId.newBuilder()
        .build();
    assertTrue(id.disjointTags().isEmpty());
    
    id = BaseTimeSeriesId.newBuilder()
        .addDisjointTag("Tyrell")
        .addDisjointTag("Frey")
        .addDisjointTag("Casterly")
        .build();
    assertEquals(3, id.disjointTags().size());
    assertEquals("Casterly", id.disjointTags().get(0));
    assertEquals("Frey", id.disjointTags().get(1));
    assertEquals("Tyrell", id.disjointTags().get(2));
    
    id = BaseTimeSeriesId.newBuilder()
        .addDisjointTag("Tyrell")
        .addDisjointTag("")
        .addDisjointTag("Casterly")
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
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void uniqueIds() throws Exception {
    Set<String> ids = Sets.newHashSet("000001", "000002");
    TimeSeriesId id = BaseTimeSeriesId.newBuilder()
        .setUniqueId(ids)
        .build();
    assertEquals(2, id.uniqueIds().size());
    assertTrue(id.uniqueIds().contains("000001"));
    assertTrue(id.uniqueIds().contains("000002"));
    
    id = BaseTimeSeriesId.newBuilder()
        .setUniqueId(Sets.newHashSet())
        .build();
    assertTrue(id.uniqueIds().isEmpty());
    
    id = BaseTimeSeriesId.newBuilder()
        .setUniqueId(null)
        .build();
    assertTrue(id.uniqueIds().isEmpty());
    
    id = BaseTimeSeriesId.newBuilder()
        .build();
    assertTrue(id.uniqueIds().isEmpty());
    
    id = BaseTimeSeriesId.newBuilder()
        .addUniqueId("000002")
        .addUniqueId("000001")
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
        .setNamespaces(Lists.newArrayList("Yahoo", "OpenTSDB"))
        .setMetrics(Lists.newArrayList("sys.cpu.user", "sys.cpu.idle"))
        .setTags(tags)
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .addUniqueId("000001")
        .addUniqueId("000002")
        .build();
    
    BaseTimeSeriesId id2 = BaseTimeSeriesId.newBuilder()
        .setAlias("FakeID")
        .setNamespaces(Lists.newArrayList("Yahoo", "OpenTSDB"))
        .setMetrics(Lists.newArrayList("sys.cpu.user", "sys.cpu.idle"))
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
        .setNamespaces(Lists.newArrayList("Yahoo", "OpenTSDB"))
        .setMetrics(Lists.newArrayList("sys.cpu.user", "sys.cpu.idle"))
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
        .setNamespaces(Lists.newArrayList("Yahoo", "OpenTSDB", "EGADS"))  // <-- Diff
        .setMetrics(Lists.newArrayList("sys.cpu.user", "sys.cpu.idle"))
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
        //.setNamespaces(Lists.newArrayList("Yahoo", "OpenTSDB"))  // <-- Diff
        .setMetrics(Lists.newArrayList("sys.cpu.user", "sys.cpu.idle"))
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
        .setNamespaces(Lists.newArrayList("OpenTSDB", "Yahoo"))  // <-- Diff order OK!
        .setMetrics(Lists.newArrayList("sys.cpu.user", "sys.cpu.idle"))
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
        .setAlias("FakeID")
        .setNamespaces(Lists.newArrayList("Yahoo", "OpenTSDB"))
        .setMetrics(Lists.newArrayList("sys.cpu.user")) 
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
        .setNamespaces(Lists.newArrayList("Yahoo", "OpenTSDB"))
        //.setMetrics(Lists.newArrayList("sys.cpu.user", "sys.cpu.idle")) // <-- Diff
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
        .setNamespaces(Lists.newArrayList("Yahoo", "OpenTSDB"))
        .setMetrics(Lists.newArrayList("sys.cpu.idle", "sys.cpu.user")) // <-- Diff order OK!
        .setTags(tags)
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .addUniqueId("000001")
        .addUniqueId("000002")
        .build();
    assertEquals(id1.hashCode(), id2.hashCode());
    assertEquals(id1, id2);
    assertEquals(0, id1.compareTo(id2));
    
    Map<String, String> tags2 = Maps.newHashMap();
    tags.put("host", "web02"); // <-- Diff
    tags.put("colo", "lax");
    
    id2 = BaseTimeSeriesId.newBuilder()
        .setAlias("FakeID")
        .setNamespaces(Lists.newArrayList("Yahoo", "OpenTSDB"))
        .setMetrics(Lists.newArrayList("sys.cpu.user", "sys.cpu.idle"))
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
        .setNamespaces(Lists.newArrayList("Yahoo", "OpenTSDB"))
        .setMetrics(Lists.newArrayList("sys.cpu.user", "sys.cpu.idle"))
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
        .setNamespaces(Lists.newArrayList("Yahoo", "OpenTSDB"))
        .setMetrics(Lists.newArrayList("sys.cpu.user", "sys.cpu.idle"))
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
        .setNamespaces(Lists.newArrayList("Yahoo", "OpenTSDB"))
        .setMetrics(Lists.newArrayList("sys.cpu.user", "sys.cpu.idle"))
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
        .setNamespaces(Lists.newArrayList("Yahoo", "OpenTSDB"))
        .setMetrics(Lists.newArrayList("sys.cpu.user", "sys.cpu.idle"))
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
        .setNamespaces(Lists.newArrayList("Yahoo", "OpenTSDB"))
        .setMetrics(Lists.newArrayList("sys.cpu.user", "sys.cpu.idle"))
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
        .setNamespaces(Lists.newArrayList("Yahoo", "OpenTSDB"))
        .setMetrics(Lists.newArrayList("sys.cpu.user", "sys.cpu.idle"))
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
        .setNamespaces(Lists.newArrayList("Yahoo", "OpenTSDB"))
        .setMetrics(Lists.newArrayList("sys.cpu.user", "sys.cpu.idle"))
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
        .setNamespaces(Lists.newArrayList("Yahoo", "OpenTSDB"))
        .setMetrics(Lists.newArrayList("sys.cpu.user", "sys.cpu.idle"))
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
        .setNamespaces(Lists.newArrayList("Yahoo", "OpenTSDB"))
        .setMetrics(Lists.newArrayList("sys.cpu.user", "sys.cpu.idle"))
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
        .setNamespaces(Lists.newArrayList("Yahoo", "OpenTSDB"))
        .setMetrics(Lists.newArrayList("sys.cpu.user", "sys.cpu.idle"))
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
        .setNamespaces(Lists.newArrayList("Yahoo", "OpenTSDB"))
        .setMetrics(Lists.newArrayList("sys.cpu.user", "sys.cpu.idle"))
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
