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

import net.opentsdb.query.execution.TimedQueryExecutor;
import net.opentsdb.utils.JSON;

public class TestExecutionGraphNode {

  @Test
  public void builder() throws Exception {
    ExecutionGraphNode node = ExecutionGraphNode.newBuilder()
        .setExecutorId("TestNode")
        .setExecutorType("TimedQueryExecutor")
        .setUpstream("UpstreamNode")
        .setDataType("Long")
        .setDefaultConfig(TimedQueryExecutor.Config.newBuilder()
            .setTimeout(60000)
            .setExecutorType("TimedQueryExecutor")
            .setExecutorId("TestNode")
            .build())
        .build();
    
    assertEquals("TestNode", node.getExecutorId());
    assertEquals("TimedQueryExecutor", node.getExecutorType());
    assertEquals("UpstreamNode", node.getUpstream());
    assertEquals("Long", node.getDataType());
    assertEquals(TimedQueryExecutor.Config.class, 
        node.getDefaultConfig().getClass());
    assertNotNull(node.toString());
    
    String json = "{\"upstream\":\"UpstreamNode\",\"executorId\":\"TestNode\","
        + "\"executorType\":\"TimedQueryExecutor\",\"dataType\":\"Long\","
        + "\"defaultConfig\":{\"executorType\":\"TimedQueryExecutor\","
        + "\"timeout\":60000,\"executorId\":\"TestNode\"}}";
    node = JSON.parseToObject(json, ExecutionGraphNode.class);
    
    assertEquals("TestNode", node.getExecutorId());
    assertEquals("TimedQueryExecutor", node.getExecutorType());
    assertEquals("UpstreamNode", node.getUpstream());
    assertEquals("Long", node.getDataType());
    assertEquals(TimedQueryExecutor.Config.class, 
        node.getDefaultConfig().getClass());
    
    json = JSON.serializeToString(node);
    assertTrue(json.contains("\"executorId\":\"TestNode\""));
    assertTrue(json.contains("\"upstream\":\"UpstreamNode\""));
    assertTrue(json.contains("\"executorType\":\"TimedQueryExecutor\""));
    assertTrue(json.contains("\"dataType\":\"Long\""));
    assertTrue(json.contains("\"defaultConfig\":{"));
    
    // minimum reqs
    node = ExecutionGraphNode.newBuilder()
        .setExecutorId("TestNode")
        .setExecutorType("TimedQueryExecutor")
        .build();
    assertEquals("TestNode", node.getExecutorId());
    assertEquals("TimedQueryExecutor", node.getExecutorType());
    assertNull(node.getUpstream());
    assertNull(node.getDataType());
    assertNull(node.getDefaultConfig());
    assertNotNull(node.toString());
    
    json = JSON.serializeToString(node);
    assertTrue(json.contains("\"executorId\":\"TestNode\""));
    assertFalse(json.contains("\"upstream\":\"UpstreamNode\""));
    assertTrue(json.contains("\"executorType\":\"TimedQueryExecutor\""));
    assertFalse(json.contains("\"dataType\":\"Long\""));
    assertFalse(json.contains("\"defaultConfig\":{"));
    
    json = "{\"executorId\":\"TestNode\",\"executorType\":"
        + "\"TimedQueryExecutor\"}";
    node = JSON.parseToObject(json, ExecutionGraphNode.class);
    assertEquals("TestNode", node.getExecutorId());
    assertEquals("TimedQueryExecutor", node.getExecutorType());
    assertNull(node.getUpstream());
    assertNull(node.getDataType());
    assertNull(node.getDefaultConfig());
    assertNotNull(node.toString());
    
    // clone
    final ExecutionGraphNode clone = ExecutionGraphNode.newBuilder(node).build();
    assertNotSame(clone, node);
    assertEquals("TestNode", clone.getExecutorId());
    assertEquals("TimedQueryExecutor", clone.getExecutorType());
    assertNull(clone.getUpstream());
    assertNull(clone.getDataType());
    assertNull(clone.getDefaultConfig());
    assertNotNull(clone.toString());
    
    // missing args
    json = "{\"id\":\"TestNode\"}";
    try {
      JSON.parseToObject(json, ExecutionGraphNode.class);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    json = "{\"executorType\":\"TimedQueryExecutor\"}";
    try {
      JSON.parseToObject(json, ExecutionGraphNode.class);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    json = "{}";
    try {
      JSON.parseToObject(json, ExecutionGraphNode.class);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void resetExecutorId() throws Exception {
    final ExecutionGraphNode node = ExecutionGraphNode.newBuilder()
        .setExecutorId("TestNode")
        .setExecutorType("TimedQueryExecutor")
        .build();
    assertEquals("TestNode", node.getExecutorId());
    
    node.resetId("DiffNode");
    assertEquals("DiffNode", node.getExecutorId());
    
    try {
      node.resetId(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      node.resetId("");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void resetUpstream() throws Exception {
    final ExecutionGraphNode node = ExecutionGraphNode.newBuilder()
        .setExecutorId("TestNode")
        .setUpstream("Node1")
        .setExecutorType("TimedQueryExecutor")
        .build();
    assertEquals("Node1", node.getUpstream());
    
    node.resetUpstream("Cluster_Node1");
    assertEquals("Cluster_Node1", node.getUpstream());
    
    try {
      node.resetUpstream(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      node.resetUpstream("");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void hashCodeEqualsCompareTo() throws Exception {
    final ExecutionGraphNode n1 = ExecutionGraphNode.newBuilder()
        .setExecutorId("TestNode")
        .setExecutorType("TimedQueryExecutor")
        .setUpstream("UpstreamNode")
        .setDataType("Long")
        .setDefaultConfig(TimedQueryExecutor.Config.newBuilder()
            .setTimeout(60000)
            .setExecutorId("TestNode")
            .setExecutorType("TimedQueryExecutor")
            .build())
        .build();
    
    ExecutionGraphNode n2 = ExecutionGraphNode.newBuilder()
        .setExecutorId("TestNode")
        .setExecutorType("TimedQueryExecutor")
        .setUpstream("UpstreamNode")
        .setDataType("Long")
        .setDefaultConfig(TimedQueryExecutor.Config.newBuilder()
            .setTimeout(60000)
            .setExecutorId("TestNode")
            .setExecutorType("TimedQueryExecutor")
            .build())
        .build();
    assertEquals(n1.hashCode(), n2.hashCode());
    assertEquals(n1, n2);
    assertEquals(0, n1.compareTo(n2));
    
    n2 = ExecutionGraphNode.newBuilder()
        .setExecutorId("Foo")  // <-- Diff
        .setExecutorType("TimedQueryExecutor")
        .setUpstream("UpstreamNode")
        .setDataType("Long")
        .setDefaultConfig(TimedQueryExecutor.Config.newBuilder()
            .setTimeout(60000)
            .setExecutorId("TestNode")
            .setExecutorType("TimedQueryExecutor")
            .build())
        .build();
    assertNotEquals(n1.hashCode(), n2.hashCode());
    assertNotEquals(n1, n2);
    assertEquals(1, n1.compareTo(n2));
    
    n2 = ExecutionGraphNode.newBuilder()
        .setExecutorId("TestNode")
        .setExecutorType("Another Type")  // <-- Diff
        .setUpstream("UpstreamNode")
        .setDataType("Long")
        .setDefaultConfig(TimedQueryExecutor.Config.newBuilder()
            .setTimeout(60000)
            .setExecutorId("TestNode")
            .setExecutorType("TimedQueryExecutor")
            .build())
        .build();
    assertNotEquals(n1.hashCode(), n2.hashCode());
    assertNotEquals(n1, n2);
    assertEquals(1, n1.compareTo(n2));
    
    n2 = ExecutionGraphNode.newBuilder()
        .setExecutorId("TestNode")
        .setExecutorType("TimedQueryExecutor")
        .setUpstream("DiffNode")  // <-- Diff
        .setDataType("Long")
        .setDefaultConfig(TimedQueryExecutor.Config.newBuilder()
            .setTimeout(60000)
            .setExecutorId("TestNode")
            .setExecutorType("TimedQueryExecutor")
            .build())
        .build();
    assertNotEquals(n1.hashCode(), n2.hashCode());
    assertNotEquals(n1, n2);
    assertEquals(1, n1.compareTo(n2));
    
    n2 = ExecutionGraphNode.newBuilder()
        .setExecutorId("TestNode")
        .setExecutorType("TimedQueryExecutor")
        //.setUpstream("UpstreamNode")  // <-- Diff
        .setDataType("Long")
        .setDefaultConfig(TimedQueryExecutor.Config.newBuilder()
            .setTimeout(60000)
            .setExecutorId("TestNode")
            .setExecutorType("TimedQueryExecutor")
            .build())
        .build();
    assertNotEquals(n1.hashCode(), n2.hashCode());
    assertNotEquals(n1, n2);
    assertEquals(1, n1.compareTo(n2));
    
    n2 = ExecutionGraphNode.newBuilder()
        .setExecutorId("TestNode")
        .setExecutorType("TimedQueryExecutor")
        .setUpstream("UpstreamNode")
        .setDataType("String")  // <-- Diff
        .setDefaultConfig(TimedQueryExecutor.Config.newBuilder()
            .setTimeout(60000)
            .setExecutorId("TestNode")
            .setExecutorType("TimedQueryExecutor")
            .build())
        .build();
    assertNotEquals(n1.hashCode(), n2.hashCode());
    assertNotEquals(n1, n2);
    assertEquals(-1, n1.compareTo(n2));
    
    n2 = ExecutionGraphNode.newBuilder()
        .setExecutorId("TestNode")
        .setExecutorType("TimedQueryExecutor")
        .setUpstream("UpstreamNode")
        //.setDataType("Long")  // <-- Diff
        .setDefaultConfig(TimedQueryExecutor.Config.newBuilder()
            .setTimeout(60000)
            .setExecutorId("TestNode")
            .setExecutorType("TimedQueryExecutor")
            .build())
        .build();
    assertNotEquals(n1.hashCode(), n2.hashCode());
    assertNotEquals(n1, n2);
    assertEquals(1, n1.compareTo(n2));
    
    n2 = ExecutionGraphNode.newBuilder()
        .setExecutorId("TestNode")
        .setExecutorType("TimedQueryExecutor")
        .setUpstream("UpstreamNode")
        .setDataType("Long")
        .setDefaultConfig(TimedQueryExecutor.Config.newBuilder()
            .setTimeout(30000)  // <-- Diff
            .setExecutorId("TestNode")
            .setExecutorType("TimedQueryExecutor")
            .build())
        .build();
    assertNotEquals(n1.hashCode(), n2.hashCode());
    assertNotEquals(n1, n2);
    assertEquals(1, n1.compareTo(n2));
    
    n2 = ExecutionGraphNode.newBuilder()
        .setExecutorId("TestNode")
        .setExecutorType("TimedQueryExecutor")
        .setUpstream("UpstreamNode")
        .setDataType("Long")
        //.setDefaultConfig(TimedQueryExecutor.Config.newBuilder()  // <-- Diff
        //    .setTimeout(60000)
        //.setExecutorId("TestNode")
        //.setExecutorType("TimedQueryExecutor")
        //    .build())
        .build();
    assertNotEquals(n1.hashCode(), n2.hashCode());
    assertNotEquals(n1, n2);
    assertEquals(1, n1.compareTo(n2));
  }
}
