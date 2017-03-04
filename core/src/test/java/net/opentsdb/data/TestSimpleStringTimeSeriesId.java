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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.Map;

import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import net.opentsdb.common.Const;
import net.opentsdb.utils.ByteSet;

public class TestSimpleStringTimeSeriesId {

  @Test
  public void alias() throws Exception {
    TimeSeriesId id = SimpleStringTimeSeriesId.newBuilder()
        .setAlias("MyID!")
        .build();
    assertArrayEquals("MyID!".getBytes(Const.UTF8_CHARSET), id.alias());
    
    id = SimpleStringTimeSeriesId.newBuilder()
        .setAlias("")
        .build();
    assertNull(id.alias());
    
    id = SimpleStringTimeSeriesId.newBuilder()
        .setAlias(null)
        .build();
    assertNull(id.alias());
    
    id = SimpleStringTimeSeriesId.newBuilder()
        .build();
    assertNull(id.alias());
  }

  @Test
  public void namespaces() throws Exception {
    TimeSeriesId id = SimpleStringTimeSeriesId.newBuilder()
        .setNamespaces(Lists.newArrayList("Tyrell", "Frey", "Casterly"))
        .build();
    assertEquals(3, id.namespaces().size());
    assertArrayEquals("Casterly".getBytes(Const.UTF8_CHARSET), id.namespaces().get(0));
    assertArrayEquals("Frey".getBytes(Const.UTF8_CHARSET), id.namespaces().get(1));
    assertArrayEquals("Tyrell".getBytes(Const.UTF8_CHARSET), id.namespaces().get(2));
    
    id = SimpleStringTimeSeriesId.newBuilder()
        .setNamespaces(Lists.newArrayList("Tyrell", "", "Casterly"))
        .build();
    assertEquals(3, id.namespaces().size());
    assertArrayEquals("".getBytes(Const.UTF8_CHARSET), id.namespaces().get(0));
    assertArrayEquals("Casterly".getBytes(Const.UTF8_CHARSET), id.namespaces().get(1));
    assertArrayEquals("Tyrell".getBytes(Const.UTF8_CHARSET), id.namespaces().get(2));
    
    try {
      id = SimpleStringTimeSeriesId.newBuilder()
          .setNamespaces(Lists.newArrayList("Tyrell", null, "Casterly"))
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    id = SimpleStringTimeSeriesId.newBuilder()
        .setNamespaces(new ArrayList<String>())
        .build();
    assertTrue(id.namespaces().isEmpty());
    
    id = SimpleStringTimeSeriesId.newBuilder()
        .setNamespaces(null)
        .build();
    assertTrue(id.namespaces().isEmpty());
    
    id = SimpleStringTimeSeriesId.newBuilder()
        .build();
    assertTrue(id.namespaces().isEmpty());
    
    id = SimpleStringTimeSeriesId.newBuilder()
        .addNamespace("Tyrell")
        .addNamespace("Frey")
        .addNamespace("Casterly")
        .build();
    assertEquals(3, id.namespaces().size());
    assertArrayEquals("Casterly".getBytes(Const.UTF8_CHARSET), id.namespaces().get(0));
    assertArrayEquals("Frey".getBytes(Const.UTF8_CHARSET), id.namespaces().get(1));
    assertArrayEquals("Tyrell".getBytes(Const.UTF8_CHARSET), id.namespaces().get(2));
    
    id = SimpleStringTimeSeriesId.newBuilder()
        .addNamespace("Tyrell")
        .addNamespace("")
        .addNamespace("Casterly")
        .build();
    assertEquals(3, id.namespaces().size());
    assertArrayEquals("".getBytes(Const.UTF8_CHARSET), id.namespaces().get(0));
    assertArrayEquals("Casterly".getBytes(Const.UTF8_CHARSET), id.namespaces().get(1));
    assertArrayEquals("Tyrell".getBytes(Const.UTF8_CHARSET), id.namespaces().get(2));
    
    try {
      id = SimpleStringTimeSeriesId.newBuilder()
          .addNamespace("Tyrell")
          .addNamespace(null)
          .addNamespace("Casterly")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void metrics() throws Exception {
    TimeSeriesId id = SimpleStringTimeSeriesId.newBuilder()
        .setMetrics(Lists.newArrayList("Tyrell", "Frey", "Casterly"))
        .build();
    assertEquals(3, id.metrics().size());
    assertArrayEquals("Casterly".getBytes(Const.UTF8_CHARSET), id.metrics().get(0));
    assertArrayEquals("Frey".getBytes(Const.UTF8_CHARSET), id.metrics().get(1));
    assertArrayEquals("Tyrell".getBytes(Const.UTF8_CHARSET), id.metrics().get(2));
    
    id = SimpleStringTimeSeriesId.newBuilder()
        .setMetrics(Lists.newArrayList("Tyrell", "", "Casterly"))
        .build();
    assertEquals(3, id.metrics().size());
    assertArrayEquals("".getBytes(Const.UTF8_CHARSET), id.metrics().get(0));
    assertArrayEquals("Casterly".getBytes(Const.UTF8_CHARSET), id.metrics().get(1));
    assertArrayEquals("Tyrell".getBytes(Const.UTF8_CHARSET), id.metrics().get(2));
    
    try {
      id = SimpleStringTimeSeriesId.newBuilder()
          .setMetrics(Lists.newArrayList("Tyrell", null, "Casterly"))
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    id = SimpleStringTimeSeriesId.newBuilder()
        .setMetrics(new ArrayList<String>())
        .build();
    assertTrue(id.metrics().isEmpty());
    
    id = SimpleStringTimeSeriesId.newBuilder()
        .setMetrics(null)
        .build();
    assertTrue(id.metrics().isEmpty());
    
    id = SimpleStringTimeSeriesId.newBuilder()
        .build();
    assertTrue(id.metrics().isEmpty());
    
    id = SimpleStringTimeSeriesId.newBuilder()
        .addMetric("Tyrell")
        .addMetric("Frey")
        .addMetric("Casterly")
        .build();
    assertEquals(3, id.metrics().size());
    assertArrayEquals("Casterly".getBytes(Const.UTF8_CHARSET), id.metrics().get(0));
    assertArrayEquals("Frey".getBytes(Const.UTF8_CHARSET), id.metrics().get(1));
    assertArrayEquals("Tyrell".getBytes(Const.UTF8_CHARSET), id.metrics().get(2));
    
    id = SimpleStringTimeSeriesId.newBuilder()
        .addMetric("Tyrell")
        .addMetric("")
        .addMetric("Casterly")
        .build();
    assertEquals(3, id.metrics().size());
    assertArrayEquals("".getBytes(Const.UTF8_CHARSET), id.metrics().get(0));
    assertArrayEquals("Casterly".getBytes(Const.UTF8_CHARSET), id.metrics().get(1));
    assertArrayEquals("Tyrell".getBytes(Const.UTF8_CHARSET), id.metrics().get(2));
    
    try {
      id = SimpleStringTimeSeriesId.newBuilder()
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
    TimeSeriesId id = SimpleStringTimeSeriesId.newBuilder()
        .setTags(tags)
        .build();
    assertEquals(2, id.tags().size());
    assertArrayEquals("web01".getBytes(Const.UTF8_CHARSET), 
        id.tags().get("host".getBytes(Const.UTF8_CHARSET)));
    assertArrayEquals("lax".getBytes(Const.UTF8_CHARSET), 
        id.tags().get("colo".getBytes(Const.UTF8_CHARSET)));
    
    tags = Maps.newHashMap();
    tags.put("colo", "");
    id = SimpleStringTimeSeriesId.newBuilder()
        .setTags(tags)
        .build();
    assertEquals(1, id.tags().size());
    assertArrayEquals("".getBytes(Const.UTF8_CHARSET), 
        id.tags().get("colo".getBytes(Const.UTF8_CHARSET)));
    
    tags = Maps.newHashMap();
    tags.put("colo", null);
    try {
      id = SimpleStringTimeSeriesId.newBuilder()
          .setTags(tags)
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    tags = Maps.newHashMap();
    tags.put("", "lax");
    id = SimpleStringTimeSeriesId.newBuilder()
        .setTags(tags)
        .build();
    assertEquals(1, id.tags().size());
    assertArrayEquals("lax".getBytes(Const.UTF8_CHARSET), 
        id.tags().get("".getBytes(Const.UTF8_CHARSET)));
    
    tags = Maps.newHashMap();
    tags.put(null, "lax");
    try {
      id = SimpleStringTimeSeriesId.newBuilder()
          .setTags(tags)
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    tags = Maps.newHashMap();
    id = SimpleStringTimeSeriesId.newBuilder()
        .setTags(tags)
        .build();
    assertEquals(0, id.tags().size());
    
    id = SimpleStringTimeSeriesId.newBuilder()
        .setTags(null)
        .build();
    assertEquals(0, id.tags().size());
    
    id = SimpleStringTimeSeriesId.newBuilder()
        .build();
    assertEquals(0, id.tags().size());
    
    id = SimpleStringTimeSeriesId.newBuilder()
        .addTags("host", "web01")
        .addTags("colo", "lax")
        .build();
    assertEquals(2, id.tags().size());
    assertArrayEquals("web01".getBytes(Const.UTF8_CHARSET), 
        id.tags().get("host".getBytes(Const.UTF8_CHARSET)));
    assertArrayEquals("lax".getBytes(Const.UTF8_CHARSET), 
        id.tags().get("colo".getBytes(Const.UTF8_CHARSET)));
    
    id = SimpleStringTimeSeriesId.newBuilder()
        .addTags("colo", "")
        .build();
    assertEquals(1, id.tags().size());
    assertArrayEquals("".getBytes(Const.UTF8_CHARSET), 
        id.tags().get("colo".getBytes(Const.UTF8_CHARSET)));
    
    try {
      id = SimpleStringTimeSeriesId.newBuilder()
          .addTags("colo", null)
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    id = SimpleStringTimeSeriesId.newBuilder()
        .addTags("", "lax")
        .build();
    assertEquals(1, id.tags().size());
    assertArrayEquals("lax".getBytes(Const.UTF8_CHARSET), 
        id.tags().get("".getBytes(Const.UTF8_CHARSET)));
    
    try {
      id = SimpleStringTimeSeriesId.newBuilder()
          .addTags(null, "lax")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      id = SimpleStringTimeSeriesId.newBuilder()
          .addTags(null, null)
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void aggregatedTags() throws Exception {
    TimeSeriesId id = SimpleStringTimeSeriesId.newBuilder()
        .setAggregatedTags(Lists.newArrayList("Tyrell", "Frey", "Casterly"))
        .build();
    assertEquals(3, id.aggregatedTags().size());
    assertArrayEquals("Casterly".getBytes(Const.UTF8_CHARSET), id.aggregatedTags().get(0));
    assertArrayEquals("Frey".getBytes(Const.UTF8_CHARSET), id.aggregatedTags().get(1));
    assertArrayEquals("Tyrell".getBytes(Const.UTF8_CHARSET), id.aggregatedTags().get(2));
    
    id = SimpleStringTimeSeriesId.newBuilder()
        .setAggregatedTags(Lists.newArrayList("Tyrell", "", "Casterly"))
        .build();
    assertEquals(3, id.aggregatedTags().size());
    assertArrayEquals("".getBytes(Const.UTF8_CHARSET), id.aggregatedTags().get(0));
    assertArrayEquals("Casterly".getBytes(Const.UTF8_CHARSET), id.aggregatedTags().get(1));
    assertArrayEquals("Tyrell".getBytes(Const.UTF8_CHARSET), id.aggregatedTags().get(2));
    
    try {
      id = SimpleStringTimeSeriesId.newBuilder()
          .setAggregatedTags(Lists.newArrayList("Tyrell", null, "Casterly"))
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    id = SimpleStringTimeSeriesId.newBuilder()
        .setAggregatedTags(new ArrayList<String>())
        .build();
    assertTrue(id.aggregatedTags().isEmpty());
    
    id = SimpleStringTimeSeriesId.newBuilder()
        .setAggregatedTags(null)
        .build();
    assertTrue(id.aggregatedTags().isEmpty());
    
    id = SimpleStringTimeSeriesId.newBuilder()
        .build();
    assertTrue(id.aggregatedTags().isEmpty());
    
    id = SimpleStringTimeSeriesId.newBuilder()
        .addAggregatedTag("Tyrell")
        .addAggregatedTag("Frey")
        .addAggregatedTag("Casterly")
        .build();
    assertEquals(3, id.aggregatedTags().size());
    assertArrayEquals("Casterly".getBytes(Const.UTF8_CHARSET), id.aggregatedTags().get(0));
    assertArrayEquals("Frey".getBytes(Const.UTF8_CHARSET), id.aggregatedTags().get(1));
    assertArrayEquals("Tyrell".getBytes(Const.UTF8_CHARSET), id.aggregatedTags().get(2));
    
    id = SimpleStringTimeSeriesId.newBuilder()
        .addAggregatedTag("Tyrell")
        .addAggregatedTag("")
        .addAggregatedTag("Casterly")
        .build();
    assertEquals(3, id.aggregatedTags().size());
    assertArrayEquals("".getBytes(Const.UTF8_CHARSET), id.aggregatedTags().get(0));
    assertArrayEquals("Casterly".getBytes(Const.UTF8_CHARSET), id.aggregatedTags().get(1));
    assertArrayEquals("Tyrell".getBytes(Const.UTF8_CHARSET), id.aggregatedTags().get(2));
    
    try {
      id = SimpleStringTimeSeriesId.newBuilder()
          .addAggregatedTag("Tyrell")
          .addAggregatedTag(null)
          .addAggregatedTag("Casterly")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void disjointTags() throws Exception {
    TimeSeriesId id = SimpleStringTimeSeriesId.newBuilder()
        .setDisjointTags(Lists.newArrayList("Tyrell", "Frey", "Casterly"))
        .build();
    assertEquals(3, id.disjointTags().size());
    assertArrayEquals("Casterly".getBytes(Const.UTF8_CHARSET), id.disjointTags().get(0));
    assertArrayEquals("Frey".getBytes(Const.UTF8_CHARSET), id.disjointTags().get(1));
    assertArrayEquals("Tyrell".getBytes(Const.UTF8_CHARSET), id.disjointTags().get(2));
    
    id = SimpleStringTimeSeriesId.newBuilder()
        .setDisjointTags(Lists.newArrayList("Tyrell", "", "Casterly"))
        .build();
    assertEquals(3, id.disjointTags().size());
    assertArrayEquals("".getBytes(Const.UTF8_CHARSET), id.disjointTags().get(0));
    assertArrayEquals("Casterly".getBytes(Const.UTF8_CHARSET), id.disjointTags().get(1));
    assertArrayEquals("Tyrell".getBytes(Const.UTF8_CHARSET), id.disjointTags().get(2));
    
    try {
      id = SimpleStringTimeSeriesId.newBuilder()
          .setDisjointTags(Lists.newArrayList("Tyrell", null, "Casterly"))
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    id = SimpleStringTimeSeriesId.newBuilder()
        .setDisjointTags(new ArrayList<String>())
        .build();
    assertTrue(id.disjointTags().isEmpty());
    
    id = SimpleStringTimeSeriesId.newBuilder()
        .setDisjointTags(null)
        .build();
    assertTrue(id.disjointTags().isEmpty());
    
    id = SimpleStringTimeSeriesId.newBuilder()
        .build();
    assertTrue(id.disjointTags().isEmpty());
    
    id = SimpleStringTimeSeriesId.newBuilder()
        .addDisjointTag("Tyrell")
        .addDisjointTag("Frey")
        .addDisjointTag("Casterly")
        .build();
    assertEquals(3, id.disjointTags().size());
    assertArrayEquals("Casterly".getBytes(Const.UTF8_CHARSET), id.disjointTags().get(0));
    assertArrayEquals("Frey".getBytes(Const.UTF8_CHARSET), id.disjointTags().get(1));
    assertArrayEquals("Tyrell".getBytes(Const.UTF8_CHARSET), id.disjointTags().get(2));
    
    id = SimpleStringTimeSeriesId.newBuilder()
        .addDisjointTag("Tyrell")
        .addDisjointTag("")
        .addDisjointTag("Casterly")
        .build();
    assertEquals(3, id.disjointTags().size());
    assertArrayEquals("".getBytes(Const.UTF8_CHARSET), id.disjointTags().get(0));
    assertArrayEquals("Casterly".getBytes(Const.UTF8_CHARSET), id.disjointTags().get(1));
    assertArrayEquals("Tyrell".getBytes(Const.UTF8_CHARSET), id.disjointTags().get(2));
    
    try {
      id = SimpleStringTimeSeriesId.newBuilder()
          .addDisjointTag("Tyrell")
          .addDisjointTag(null)
          .addDisjointTag("Casterly")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void uniqueIds() throws Exception {
    ByteSet ids = new ByteSet();
    ids.add(new byte[] { 0, 0, 2 });
    ids.add(new byte[] { 0, 0, 1 });
    TimeSeriesId id = SimpleStringTimeSeriesId.newBuilder()
        .setUniqueId(ids)
        .build();
    assertEquals(2, id.uniqueIds().size());
    assertTrue(id.uniqueIds().contains(new byte[] { 0, 0, 1 }));
    assertTrue(id.uniqueIds().contains(new byte[] { 0, 0, 2 }));
    
    id = SimpleStringTimeSeriesId.newBuilder()
        .setUniqueId(new ByteSet())
        .build();
    assertTrue(id.uniqueIds().isEmpty());
    
    id = SimpleStringTimeSeriesId.newBuilder()
        .setUniqueId(null)
        .build();
    assertTrue(id.uniqueIds().isEmpty());
    
    id = SimpleStringTimeSeriesId.newBuilder()
        .build();
    assertTrue(id.uniqueIds().isEmpty());
    
    id = SimpleStringTimeSeriesId.newBuilder()
        .addUniqueId(new byte[] { 0, 0, 2 })
        .addUniqueId(new byte[] { 0, 0, 1 })
        .build();
    assertEquals(2, id.uniqueIds().size());
    assertTrue(id.uniqueIds().contains(new byte[] { 0, 0, 1 }));
    assertTrue(id.uniqueIds().contains(new byte[] { 0, 0, 2 }));
    
    try {
      id = SimpleStringTimeSeriesId.newBuilder()
          .addUniqueId(null)
          .build();
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void hashCodeEqualsCompareTo() throws Exception {
    Map<String, String> tags = Maps.newHashMap();
    tags.put("host", "web01");
    tags.put("colo", "lax");
    
    final SimpleStringTimeSeriesId id1 = SimpleStringTimeSeriesId.newBuilder()
        .setAlias("FakeID")
        .setNamespaces(Lists.newArrayList("Yahoo", "OpenTSDB"))
        .setMetrics(Lists.newArrayList("sys.cpu.user", "sys.cpu.idle"))
        .setTags(tags)
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .addUniqueId(new byte[] { 0, 0, 1 })
        .addUniqueId(new byte[] { 0, 0, 2 })
        .build();
    
    SimpleStringTimeSeriesId id2 = SimpleStringTimeSeriesId.newBuilder()
        .setAlias("FakeID")
        .setNamespaces(Lists.newArrayList("Yahoo", "OpenTSDB"))
        .setMetrics(Lists.newArrayList("sys.cpu.user", "sys.cpu.idle"))
        .setTags(tags)
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .addUniqueId(new byte[] { 0, 0, 1 })
        .addUniqueId(new byte[] { 0, 0, 2 })
        .build();
    assertEquals(id1.hashCode(), id2.hashCode());
    assertEquals(id1, id2);
    assertEquals(0, id1.compareTo(id2));
    
    id2 = SimpleStringTimeSeriesId.newBuilder()
        .setAlias("FakeID2") // <-- Diff
        .setNamespaces(Lists.newArrayList("Yahoo", "OpenTSDB"))
        .setMetrics(Lists.newArrayList("sys.cpu.user", "sys.cpu.idle"))
        .setTags(tags)
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .addUniqueId(new byte[] { 0, 0, 1 })
        .addUniqueId(new byte[] { 0, 0, 2 })
        .build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(-1, id1.compareTo(id2));
    
    id2 = SimpleStringTimeSeriesId.newBuilder()
        .setAlias("FakeID")
        .setNamespaces(Lists.newArrayList("Yahoo", "OpenTSDB", "EGADS"))  // <-- Diff
        .setMetrics(Lists.newArrayList("sys.cpu.user", "sys.cpu.idle"))
        .setTags(tags)
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .addUniqueId(new byte[] { 0, 0, 1 })
        .addUniqueId(new byte[] { 0, 0, 2 })
        .build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(1, id1.compareTo(id2));
    
    id2 = SimpleStringTimeSeriesId.newBuilder()
        .setAlias("FakeID")
        //.setNamespaces(Lists.newArrayList("Yahoo", "OpenTSDB"))  // <-- Diff
        .setMetrics(Lists.newArrayList("sys.cpu.user", "sys.cpu.idle"))
        .setTags(tags)
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .addUniqueId(new byte[] { 0, 0, 1 })
        .addUniqueId(new byte[] { 0, 0, 2 })
        .build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(1, id1.compareTo(id2));
    
    id2 = SimpleStringTimeSeriesId.newBuilder()
        .setAlias("FakeID")
        .setNamespaces(Lists.newArrayList("OpenTSDB", "Yahoo"))  // <-- Diff order OK!
        .setMetrics(Lists.newArrayList("sys.cpu.user", "sys.cpu.idle"))
        .setTags(tags)
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .addUniqueId(new byte[] { 0, 0, 1 })
        .addUniqueId(new byte[] { 0, 0, 2 })
        .build();
    assertEquals(id1.hashCode(), id2.hashCode());
    assertEquals(id1, id2);
    assertEquals(0, id1.compareTo(id2));
    
    id2 = SimpleStringTimeSeriesId.newBuilder()
        .setAlias("FakeID")
        .setNamespaces(Lists.newArrayList("Yahoo", "OpenTSDB"))
        .setMetrics(Lists.newArrayList("sys.cpu.user")) 
        .setTags(tags)
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .addUniqueId(new byte[] { 0, 0, 1 })
        .addUniqueId(new byte[] { 0, 0, 2 })
        .build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(-1, id1.compareTo(id2));
    
    id2 = SimpleStringTimeSeriesId.newBuilder()
        .setAlias("FakeID")
        .setNamespaces(Lists.newArrayList("Yahoo", "OpenTSDB"))
        //.setMetrics(Lists.newArrayList("sys.cpu.user", "sys.cpu.idle")) // <-- Diff
        .setTags(tags)
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .addUniqueId(new byte[] { 0, 0, 1 })
        .addUniqueId(new byte[] { 0, 0, 2 })
        .build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(1, id1.compareTo(id2));
    
    id2 = SimpleStringTimeSeriesId.newBuilder()
        .setAlias("FakeID")
        .setNamespaces(Lists.newArrayList("Yahoo", "OpenTSDB"))
        .setMetrics(Lists.newArrayList("sys.cpu.idle", "sys.cpu.user")) // <-- Diff order OK!
        .setTags(tags)
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .addUniqueId(new byte[] { 0, 0, 1 })
        .addUniqueId(new byte[] { 0, 0, 2 })
        .build();
    assertEquals(id1.hashCode(), id2.hashCode());
    assertEquals(id1, id2);
    assertEquals(0, id1.compareTo(id2));
    
    Map<String, String> tags2 = Maps.newHashMap();
    tags.put("host", "web02"); // <-- Diff
    tags.put("colo", "lax");
    
    id2 = SimpleStringTimeSeriesId.newBuilder()
        .setAlias("FakeID")
        .setNamespaces(Lists.newArrayList("Yahoo", "OpenTSDB"))
        .setMetrics(Lists.newArrayList("sys.cpu.user", "sys.cpu.idle"))
        .setTags(tags2) // <-- Diff
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .addUniqueId(new byte[] { 0, 0, 1 })
        .addUniqueId(new byte[] { 0, 0, 2 })
        .build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(-1, id1.compareTo(id2));
    
    tags2 = Maps.newHashMap();
    tags.put("host", "web01");
    //tags.put("colo", "lax"); // <-- Diff
    
    id2 = SimpleStringTimeSeriesId.newBuilder()
        .setAlias("FakeID")
        .setNamespaces(Lists.newArrayList("Yahoo", "OpenTSDB"))
        .setMetrics(Lists.newArrayList("sys.cpu.user", "sys.cpu.idle"))
        .setTags(tags2) // <-- Diff
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .addUniqueId(new byte[] { 0, 0, 1 })
        .addUniqueId(new byte[] { 0, 0, 2 })
        .build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(-1, id1.compareTo(id2));
    
    id2 = SimpleStringTimeSeriesId.newBuilder()
        .setAlias("FakeID")
        .setNamespaces(Lists.newArrayList("Yahoo", "OpenTSDB"))
        .setMetrics(Lists.newArrayList("sys.cpu.user", "sys.cpu.idle"))
        //.setTags(tags) // <-- Diff
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .addUniqueId(new byte[] { 0, 0, 1 })
        .addUniqueId(new byte[] { 0, 0, 2 })
        .build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(-1, id1.compareTo(id2));
    
    id2 = SimpleStringTimeSeriesId.newBuilder()
        .setAlias("FakeID")
        .setNamespaces(Lists.newArrayList("Yahoo", "OpenTSDB"))
        .setMetrics(Lists.newArrayList("sys.cpu.user", "sys.cpu.idle"))
        .setTags(tags)
        .setAggregatedTags(Lists.newArrayList("owner")) // <-- Diff
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .addUniqueId(new byte[] { 0, 0, 1 })
        .addUniqueId(new byte[] { 0, 0, 2 })
        .build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(1, id1.compareTo(id2));
    
    id2 = SimpleStringTimeSeriesId.newBuilder()
        .setAlias("FakeID")
        .setNamespaces(Lists.newArrayList("Yahoo", "OpenTSDB"))
        .setMetrics(Lists.newArrayList("sys.cpu.user", "sys.cpu.idle"))
        .setTags(tags)
        //.setAggregatedTags(Lists.newArrayList("owner", "role")) // <-- Diff
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .addUniqueId(new byte[] { 0, 0, 1 })
        .addUniqueId(new byte[] { 0, 0, 2 })
        .build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(1, id1.compareTo(id2));
    
    id2 = SimpleStringTimeSeriesId.newBuilder()
        .setAlias("FakeID")
        .setNamespaces(Lists.newArrayList("Yahoo", "OpenTSDB"))
        .setMetrics(Lists.newArrayList("sys.cpu.user", "sys.cpu.idle"))
        .setTags(tags)
        .setAggregatedTags(Lists.newArrayList("role", "owner")) // <-- Diff order OK!
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .addUniqueId(new byte[] { 0, 0, 1 })
        .addUniqueId(new byte[] { 0, 0, 2 })
        .build();
    assertEquals(id1.hashCode(), id2.hashCode());
    assertEquals(id1, id2);
    assertEquals(0, id1.compareTo(id2));
    
    id2 = SimpleStringTimeSeriesId.newBuilder()
        .setAlias("FakeID")
        .setNamespaces(Lists.newArrayList("Yahoo", "OpenTSDB"))
        .setMetrics(Lists.newArrayList("sys.cpu.user", "sys.cpu.idle"))
        .setTags(tags)
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("propery"))  // <-- Diff
        .addUniqueId(new byte[] { 0, 0, 1 })
        .addUniqueId(new byte[] { 0, 0, 2 })
        .build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(1, id1.compareTo(id2));
    
    id2 = SimpleStringTimeSeriesId.newBuilder()
        .setAlias("FakeID")
        .setNamespaces(Lists.newArrayList("Yahoo", "OpenTSDB"))
        .setMetrics(Lists.newArrayList("sys.cpu.user", "sys.cpu.idle"))
        .setTags(tags)
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        //.setDisjointTags(Lists.newArrayList("propery", "type"))  // <-- Diff
        .addUniqueId(new byte[] { 0, 0, 1 })
        .addUniqueId(new byte[] { 0, 0, 2 })
        .build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(1, id1.compareTo(id2));
    
    id2 = SimpleStringTimeSeriesId.newBuilder()
        .setAlias("FakeID")
        .setNamespaces(Lists.newArrayList("Yahoo", "OpenTSDB"))
        .setMetrics(Lists.newArrayList("sys.cpu.user", "sys.cpu.idle"))
        .setTags(tags) 
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("type", "propery")) // <-- Diff order OK!
        .addUniqueId(new byte[] { 0, 0, 1 })
        .addUniqueId(new byte[] { 0, 0, 2 })
        .build();
    assertEquals(id1.hashCode(), id2.hashCode());
    assertEquals(id1, id2);
    assertEquals(0, id1.compareTo(id2));
    
    id2 = SimpleStringTimeSeriesId.newBuilder()
        .setAlias("FakeID")
        .setNamespaces(Lists.newArrayList("Yahoo", "OpenTSDB"))
        .setMetrics(Lists.newArrayList("sys.cpu.user", "sys.cpu.idle"))
        .setTags(tags) 
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .addUniqueId(new byte[] { 0, 0, 1 })
        //.addUniqueId(new byte[] { 0, 0, 2 })  // <-- Diff
        .build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(-1, id1.compareTo(id2));
    
    id2 = SimpleStringTimeSeriesId.newBuilder()
        .setAlias("FakeID")
        .setNamespaces(Lists.newArrayList("Yahoo", "OpenTSDB"))
        .setMetrics(Lists.newArrayList("sys.cpu.user", "sys.cpu.idle"))
        .setTags(tags) 
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        //.addUniqueId(new byte[] { 0, 0, 1 })
        //.addUniqueId(new byte[] { 0, 0, 2 })  // <-- Diff
        .build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(-1, id1.compareTo(id2));
    
    id2 = SimpleStringTimeSeriesId.newBuilder()
        .setAlias("FakeID")
        .setNamespaces(Lists.newArrayList("Yahoo", "OpenTSDB"))
        .setMetrics(Lists.newArrayList("sys.cpu.user", "sys.cpu.idle"))
        .setTags(tags) 
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .addUniqueId(new byte[] { 0, 0, 2 }) // <-- Diff order OK!
        .addUniqueId(new byte[] { 0, 0, 1 })
        .build();
    assertEquals(id1.hashCode(), id2.hashCode());
    assertEquals(id1, id2);
    assertEquals(0, id1.compareTo(id2));
  }
}
