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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.junit.Test;

import com.google.common.collect.Lists;

import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.utils.JSON;

public class TestExecutionGraph {

  @Test
  public void builder() throws Exception {
    ExecutionGraph graph = ExecutionGraph.newBuilder()
        .setId("Graph1")
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("N1")
          .setType("MockFactoryA")
          .addSource("S1")
          .addSource("S2"))
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("S1")
          .setType("MockSourceFactory"))
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("S2")
          .setType("MockSourceFactory"))
        .build();
    
    assertEquals("Graph1", graph.getId());
    assertEquals(3, graph.getNodes().size());
    assertEquals("N1", graph.getNodes().get(0).getId());
    assertEquals("MockFactoryA", graph.getNodes().get(0).getType());
    assertEquals(2, graph.getNodes().get(0).getSources().size());
    assertEquals("S1", graph.getNodes().get(0).getSources().get(0));
    assertEquals("S2", graph.getNodes().get(0).getSources().get(1));
    assertEquals("S1", graph.getNodes().get(1).getId());
    assertEquals("MockSourceFactory", graph.getNodes().get(1).getType());
    assertEquals("S2", graph.getNodes().get(2).getId());
    assertEquals("MockSourceFactory", graph.getNodes().get(2).getType());
    
    String json = "{\"id\":\"Graph1\",\"nodes\":[{\"id\":\"N1\","
        + "\"type\":\"MockFactoryA\",\"sources\":[\"S1\",\"S2\"]},"
        + "{\"id\":\"S1\",\"type\":\"MockSourceFactory\"},"
        + "{\"id\":\"S2\",\"type\":\"MockSourceFactory\"}]}";
    graph = JSON.parseToObject(json, ExecutionGraph.class);
    
    assertEquals("Graph1", graph.getId());
    assertEquals(3, graph.getNodes().size());
    assertEquals("N1", graph.getNodes().get(0).getId());
    assertEquals("MockFactoryA", graph.getNodes().get(0).getType());
    assertEquals(2, graph.getNodes().get(0).getSources().size());
    assertEquals("S1", graph.getNodes().get(0).getSources().get(0));
    assertEquals("S2", graph.getNodes().get(0).getSources().get(1));
    assertEquals("S1", graph.getNodes().get(1).getId());
    assertEquals("MockSourceFactory", graph.getNodes().get(1).getType());
    assertEquals("S2", graph.getNodes().get(2).getId());
    assertEquals("MockSourceFactory", graph.getNodes().get(2).getType());
    
    json = JSON.serializeToString(graph);
    assertTrue(json.contains("\"id\":\"Graph1\""));
    assertTrue(json.contains("\"nodes\":["));
    assertTrue(json.contains("\"id\":\"N1\""));
    assertTrue(json.contains("\"id\":\"S1\""));
    assertTrue(json.contains("\"id\":\"S2\""));
    assertTrue(json.contains("\"sources\":[\"S1\",\"S2\"]"));
    
    ExecutionGraph clone = ExecutionGraph.newBuilder(graph).build();
    assertNotSame(clone, graph);
    assertEquals("Graph1", clone.getId());
    assertEquals(3, clone.getNodes().size());
    assertNotSame(clone.nodes, graph.nodes);
    assertNotSame(clone.nodes.get(0), graph.nodes.get(0));
    assertNotSame(clone.nodes.get(1), graph.nodes.get(1));
    
    // null or empty ID is ok.
    graph = ExecutionGraph.newBuilder()
    //.setId("Graph1")
    .addNode(ExecutionGraphNode.newBuilder()
      .setId("Node1")
      .setType("TimedQueryExecutor"))
    .addNode(ExecutionGraphNode.newBuilder()
      .setId("Node2")
      .setType("TimedQueryExecutor"))
    .build();
    
    graph = ExecutionGraph.newBuilder()
    .setId("")
    .addNode(ExecutionGraphNode.newBuilder()
      .setId("Node1")
      .setType("TimedQueryExecutor"))
    .addNode(ExecutionGraphNode.newBuilder()
        .setId("Node2")
        .setType("TimedQueryExecutor"))
    .build();
    
    try {
      ExecutionGraph.newBuilder()
        .setId("Graph1")
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      ExecutionGraph.newBuilder()
        .setId("Graph1")
        .setNodes(Collections.<ExecutionGraphNode>emptyList())
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // Unknowns are ok.
    json = "{\"id\":\"Graph1\",\"unkownfield\":42,\"nodes\":"
        + "[{\"id\":\"Node1\",\"type\":\"TimedQueryExecutor\"},"
        + "{\"id\":\"Node2\",\"type\":\"TimedQueryExecutor\"}]}";
    graph = JSON.parseToObject(json, ExecutionGraph.class);
    assertEquals(2, graph.getNodes().size());
  }
  
  @Test
  public void hashCodeEqualsCompareTo() throws Exception {
    final ExecutionGraph g1 = ExecutionGraph.newBuilder()
        .setId("Graph1")
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("N1")
          .setType("MockFactoryA")
          .addSource("S1")
          .addSource("S2"))
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("S1")
          .setType("MockSourceFactory"))
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("S2")
          .setType("MockSourceFactory"))
        .build();
    
    ExecutionGraph g2 = ExecutionGraph.newBuilder()
        .setId("Graph1")
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("N1")
          .setType("MockFactoryA")
          .addSource("S1")
          .addSource("S2"))
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("S1")
          .setType("MockSourceFactory"))
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("S2")
          .setType("MockSourceFactory"))
        .build();
    assertEquals(g1.hashCode(), g2.hashCode());
    assertEquals(g1, g2);
    assertEquals(0, g1.compareTo(g2));
    
    g2 = ExecutionGraph.newBuilder()
        .setId("Graph2")  // <-- DIFF
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("N1")
          .setType("MockFactoryA")
          .addSource("S1")
          .addSource("S2"))
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("S1")
          .setType("MockSourceFactory"))
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("S2")
          .setType("MockSourceFactory"))
        .build();
    assertNotEquals(g1.hashCode(), g2.hashCode());
    assertNotEquals(g1, g2);
    assertEquals(-1, g1.compareTo(g2));
    
    g2 = ExecutionGraph.newBuilder()
        .setId("Graph1")
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("N1")
          .setType("MockFactoryA")
          .addSource("S1")
          .addSource("S3")) // <-- DIFF
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("S3") // <-- DIFF
          .setType("MockSourceFactory"))
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("S2")
          .setType("MockSourceFactory"))
        .build();
    assertNotEquals(g1.hashCode(), g2.hashCode());
    assertNotEquals(g1, g2);
    assertEquals(-1, g1.compareTo(g2));
    
    g2 = ExecutionGraph.newBuilder()
        .setId("Graph1")
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("N1")
          .setType("MockFactoryA")
          .addSource("S1")
          .addSource("S2"))
        .addNode(ExecutionGraphNode.newBuilder() // ORDER change is ok
            .setId("S2")
            .setType("MockSourceFactory"))
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("S1")
          .setType("MockSourceFactory"))
        .build();
    assertEquals(g1.hashCode(), g2.hashCode());
    assertEquals(g1, g2);
    assertEquals(0, g1.compareTo(g2));
  }
  
  @Test
  public void addNodeConfig() throws Exception {
    ExecutionGraph graph = ExecutionGraph.newBuilder()
        .setId("Graph1")
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("N1")
          .setType("MockFactoryA")
          .addSource("S1")
          .addSource("S2"))
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("S1")
          .setType("MockSourceFactory"))
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("S2")
          .setType("MockSourceFactory"))
        .build();
    
    QueryNodeConfig config1 = mock(QueryNodeConfig.class);
    when(config1.getId()).thenReturn("config1");
    QueryNodeConfig config2 = mock(QueryNodeConfig.class);
    when(config2.getId()).thenReturn("config2");
    
    graph.addNodeConfig(config1);
    assertEquals(1, graph.nodeConfigs().size());
    assertSame(config1, graph.nodeConfigs().get("config1"));
    
    graph.addNodeConfig(config2);
    assertEquals(2, graph.nodeConfigs().size());
    assertSame(config1, graph.nodeConfigs().get("config1"));
    assertSame(config2, graph.nodeConfigs().get("config2"));
    
    graph.setNodeConfigs(Lists.newArrayList(config2));
    assertEquals(1, graph.nodeConfigs().size());
    assertSame(config2, graph.nodeConfigs().get("config2"));
    
    try {
      graph.addNodeConfig(config2);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    graph.node_configs.clear();
    when(config2.getId()).thenReturn(null);
    try {
      graph.addNodeConfig(config2);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    assertTrue(graph.nodeConfigs().isEmpty());
    
    when(config2.getId()).thenReturn("");
    try {
      graph.addNodeConfig(config2);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    assertTrue(graph.nodeConfigs().isEmpty());
    
    try {
      graph.addNodeConfig(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    assertTrue(graph.nodeConfigs().isEmpty());
    
    try {
      graph.setNodeConfigs(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    assertTrue(graph.nodeConfigs().isEmpty());
  }
  
}