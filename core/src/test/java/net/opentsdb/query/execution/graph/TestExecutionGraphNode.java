// This file is part of OpenTSDB.
// Copyright (C) 2017-2018  The OpenTSDB Authors.
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
package net.opentsdb.query.execution.graph;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.google.common.collect.Lists;

import net.opentsdb.query.pojo.RateOptions;
import net.opentsdb.utils.JSON;

public class TestExecutionGraphNode {

  @Test
  public void builder() throws Exception {
    ExecutionGraphNode node = ExecutionGraphNode.newBuilder()
        .setId("TestNode")
        .setType("Rate")
        .setSources(Lists.newArrayList("node1", "node2"))
        .setConfig(RateOptions.newBuilder()
            .setResetValue(1)
            .build())
        .build();
    
    assertEquals("TestNode", node.getId());
    assertEquals("Rate", node.getType());
    assertEquals(2, node.getSources().size());
    assertEquals("node1", node.getSources().get(0));
    assertEquals("node2", node.getSources().get(1));
    assertEquals(1, ((RateOptions) node.getConfig()).getResetValue());
    assertNotNull(node.toString());
    
    String json = "{\"sources\":[\"node1\",\"node2\"],\"id\":\"TestNode\","
        + "\"type\":\"Rate\"}";
    node = JSON.parseToObject(json, ExecutionGraphNode.class);
    
    assertEquals("TestNode", node.getId());
    assertEquals("Rate", node.getType());
    assertEquals(2, node.getSources().size());
    assertEquals("node1", node.getSources().get(0));
    assertEquals("node2", node.getSources().get(1));
    
    json = JSON.serializeToString(node);
    assertTrue(json.contains("\"id\":\"TestNode\""));
    assertTrue(json.contains("\"sources\":[\"node1\",\"node2\"]"));
    assertTrue(json.contains("\"type\":\"Rate\""));
    
    // minimum reqs
    node = ExecutionGraphNode.newBuilder()
        .setId("Rate")
        .build();
    assertEquals("Rate", node.getId());
    assertEquals("Rate", node.getType());
    assertNull(node.getSources());
    assertNotNull(node.toString());
    
    json = JSON.serializeToString(node);
    assertTrue(json.contains("\"id\":\"Rate\""));
    assertFalse(json.contains("\"sources\""));
    assertTrue(json.contains("\"type\":\"Rate\""));
    
    json = "{\"id\":\"Rate\"}";
    node = JSON.parseToObject(json, ExecutionGraphNode.class);
    assertEquals("Rate", node.getId());
    assertEquals("Rate", node.getType());
    assertNull(node.getSources());
    assertNotNull(node.toString());
    
    // missing args
    json = "{\"type\":\"Rate\"}";
    try {
      JSON.parseToObject(json, ExecutionGraphNode.class);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    json = "{\"id\":\"\"}";
    try {
      JSON.parseToObject(json, ExecutionGraphNode.class);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    json = "{}";
    try {
      JSON.parseToObject(json, ExecutionGraphNode.class);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // uknowns are ok
    json = "{\"id\":\"Rate\",\"someJunk\":\"field\"}";
    node = JSON.parseToObject(json, ExecutionGraphNode.class);
    assertEquals("Rate", node.getId());
    assertEquals("Rate", node.getType());
    assertNull(node.getSources());
    assertNotNull(node.toString());
  }
  
  @Test
  public void builderClone() throws Exception {
    ExecutionGraphNode node = ExecutionGraphNode.newBuilder()
        .setId("TestNode")
        .setType("Rate")
        .setSources(Lists.newArrayList("node1", "node2"))
        .build();
    
    ExecutionGraphNode clone = ExecutionGraphNode.newBuilder(node).build();
    assertEquals("TestNode", clone.getId());
    assertEquals("Rate", clone.getType());
    assertEquals(2, clone.getSources().size());
    assertEquals("node1", clone.getSources().get(0));
    assertEquals("node2", clone.getSources().get(1));
    assertNotNull(clone.toString());
    
    assertNotSame(node.getSources(), clone.getSources());
    
    node = ExecutionGraphNode.newBuilder()
        .setId("TestNode")
        .setType("Rate")
        .build();
    
    clone = ExecutionGraphNode.newBuilder(node).build();
    assertEquals("TestNode", clone.getId());
    assertEquals("Rate", clone.getType());
    assertNull(clone.getSources());
    assertNotNull(clone.toString());
  }

  @Test
  public void hashCodeEqualsCompareTo() throws Exception {
    final ExecutionGraphNode n1 = ExecutionGraphNode.newBuilder()
        .setId("TestNode")
        .setType("Rate")
        .setSources(Lists.newArrayList("node1", "node2"))
        .build();
    
    ExecutionGraphNode n2 = ExecutionGraphNode.newBuilder()
        .setId("TestNode")
        .setType("Rate")
        .setSources(Lists.newArrayList("node1", "node2"))
        .build();
    assertEquals(n1.hashCode(), n2.hashCode());
    assertEquals(n1, n2);
    assertEquals(0, n1.compareTo(n2));
    
    n2 = ExecutionGraphNode.newBuilder()
        .setId("Foo")  // <-- Diff
        .setType("Rate")
        .setSources(Lists.newArrayList("node1", "node2"))
        .build();
    assertNotEquals(n1.hashCode(), n2.hashCode());
    assertNotEquals(n1, n2);
    assertEquals(1, n1.compareTo(n2));
    
    n2 = ExecutionGraphNode.newBuilder()
        .setId("TestNode")
        .setType("SomeOtherType")  // <-- DIFF
        .setSources(Lists.newArrayList("node1", "node2"))
        .build();
    assertNotEquals(n1.hashCode(), n2.hashCode());
    assertNotEquals(n1, n2);
    assertEquals(-1, n1.compareTo(n2));
    
    n2 = ExecutionGraphNode.newBuilder()
        .setId("TestNode")
        .setType("Rate")
        .setSources(Lists.newArrayList("node2", "node1")) // <-- DIFF order is OK!
        .build();
    assertEquals(n1.hashCode(), n2.hashCode());
    assertEquals(n1, n2);
    assertEquals(0, n1.compareTo(n2));
    
    n2 = ExecutionGraphNode.newBuilder()
        .setId("TestNode")
        .setType("Rate")
        .setSources(Lists.newArrayList("node1", "node3")) // <-- DIFF
        .build();
    assertNotEquals(n1.hashCode(), n2.hashCode());
    assertNotEquals(n1, n2);
    assertEquals(-1, n1.compareTo(n2));
    
    n2 = ExecutionGraphNode.newBuilder()
        .setId("TestNode")
        .setType("Rate")
        .setSources(Lists.newArrayList("node1")) // <-- DIFF
        .build();
    assertNotEquals(n1.hashCode(), n2.hashCode());
    assertNotEquals(n1, n2);
    assertEquals(1, n1.compareTo(n2));
    
    n2 = ExecutionGraphNode.newBuilder()
        .setId("TestNode")
        .setType("Rate")
        //.setSources(Lists.newArrayList("node1", "node2")) // <-- DIFF
        .build();
    assertNotEquals(n1.hashCode(), n2.hashCode());
    assertNotEquals(n1, n2);
    assertEquals(1, n1.compareTo(n2));
  }
}
