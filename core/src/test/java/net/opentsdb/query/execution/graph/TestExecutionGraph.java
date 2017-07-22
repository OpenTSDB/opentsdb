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
package net.opentsdb.query.execution.graph;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.stumbleupon.async.Deferred;

import net.opentsdb.core.Registry;
import net.opentsdb.core.TSDB;
import net.opentsdb.query.execution.QueryExecutor;
import net.opentsdb.query.execution.QueryExecutorFactory;
import net.opentsdb.utils.JSON;

public class TestExecutionGraph {

  private TSDB tsdb;
  private Registry registry;
  private QueryExecutorFactory<?> factory;
  private QueryExecutor<?> executor_a;
  private QueryExecutor<?> executor_b;
  private QueryExecutor<?> executor_c;
  private ExecutionGraph.Builder builder;
  
  @Before
  public void before() throws Exception {
    tsdb = mock(TSDB.class);
    registry = mock(Registry.class);
    factory = mock(QueryExecutorFactory.class);
    executor_a = mock(QueryExecutor.class);
    executor_b = mock(QueryExecutor.class);
    executor_c = mock(QueryExecutor.class);
    
    when(tsdb.getRegistry()).thenReturn(registry);
    when(registry.getFactory("TimedQueryExecutor"))
      .thenAnswer(new Answer<QueryExecutorFactory<?>>() {
      @Override
      public QueryExecutorFactory<?> answer(InvocationOnMock invocation)
          throws Throwable {
        return factory;
      }
    });
    when(factory.newExecutor(any(ExecutionGraphNode.class)))
      .thenAnswer(new Answer<QueryExecutor<?>>() {
      @Override
      public QueryExecutor<?> answer(InvocationOnMock invocation)
          throws Throwable {
        final ExecutionGraphNode node = 
            (ExecutionGraphNode) invocation.getArguments()[0];
        if (node.getExecutorId().equals("Node1")) {
          return executor_a;
        } else if (node.getExecutorId().equals("Node2")) {
          return executor_b;
        } else if (node.getExecutorId().equals("Node3")) {
          return executor_c;
        }
        return null;
      }
    });
    when(executor_a.id()).thenReturn("Node1");
    when(executor_b.id()).thenReturn("Node2");
    when(executor_c.id()).thenReturn("Node3");
    
    builder = ExecutionGraph.newBuilder()
        .setId("Graph1")
        .addNode(ExecutionGraphNode.newBuilder()
          .setExecutorId("Node1")
          .setExecutorType("TimedQueryExecutor"))
        .addNode(ExecutionGraphNode.newBuilder()
            .setExecutorId("Node2")
            .setExecutorType("TimedQueryExecutor")
            .setUpstream("Node1"));
  }
  
  @Test
  public void builder() throws Exception {
    ExecutionGraph graph = builder.build();
    
    assertEquals("Graph1", graph.getId());
    assertEquals(2, graph.getNodes().size());
    assertEquals("Node1", graph.getNodes().get(0).getExecutorId());
    assertEquals("TimedQueryExecutor", graph.getNodes().get(0).getExecutorType());
    assertEquals("Node2", graph.getNodes().get(1).getExecutorId());
    assertEquals("TimedQueryExecutor", graph.getNodes().get(1).getExecutorType());
    assertTrue(graph.executors.isEmpty());
    assertTrue(graph.graph.vertexSet().isEmpty());
    assertNull(graph.tsdb());
    assertNull(graph.sinkExecutor());
    
    String json = "{\"id\":\"Graph1\",\"nodes\":[{\"executorId\":\"Node1\","
        + "\"executorType\":\"TimedQueryExecutor\"},{\"executorId\":\"Node2\","
        + "\"upstream\":\"Node1\",\"executorType\":\"TimedQueryExecutor\"}]}";
    graph = JSON.parseToObject(json, ExecutionGraph.class);
    
    assertEquals("Graph1", graph.getId());
    assertEquals(2, graph.getNodes().size());
    assertEquals("Node1", graph.getNodes().get(0).getExecutorId());
    assertEquals("TimedQueryExecutor", graph.getNodes().get(0).getExecutorType());
    assertEquals("Node2", graph.getNodes().get(1).getExecutorId());
    assertEquals("TimedQueryExecutor", graph.getNodes().get(1).getExecutorType());
    assertTrue(graph.executors.isEmpty());
    assertTrue(graph.graph.vertexSet().isEmpty());
    assertNull(graph.tsdb());
    assertNull(graph.sinkExecutor());
    
    json = JSON.serializeToString(graph);
    assertTrue(json.contains("\"id\":\"Graph1\""));
    assertTrue(json.contains("\"nodes\":["));
    assertTrue(json.contains("\"executorId\":\"Node1\""));
    assertTrue(json.contains("\"executorId\":\"Node2\""));
    
    ExecutionGraph clone = ExecutionGraph.newBuilder(graph).build();
    assertNotSame(clone, graph);
    assertEquals("Graph1", clone.getId());
    assertEquals(2, clone.getNodes().size());
    assertNotSame(clone.nodes, graph.nodes);
    assertNotSame(clone.nodes.get(0), graph.nodes.get(0));
    assertNotSame(clone.nodes.get(1), graph.nodes.get(1));
    
    // null or empty ID is ok.
    graph = ExecutionGraph.newBuilder()
    //.setId("Graph1")
    .addNode(ExecutionGraphNode.newBuilder()
      .setExecutorId("Node1")
      .setExecutorType("TimedQueryExecutor"))
    .addNode(ExecutionGraphNode.newBuilder()
        .setExecutorId("Node2")
        .setExecutorType("TimedQueryExecutor")
        .setUpstream("Node1"))
    .build();
    
    graph = ExecutionGraph.newBuilder()
    .setId("")
    .addNode(ExecutionGraphNode.newBuilder()
      .setExecutorId("Node1")
      .setExecutorType("TimedQueryExecutor"))
    .addNode(ExecutionGraphNode.newBuilder()
        .setExecutorId("Node2")
        .setExecutorType("TimedQueryExecutor")
        .setUpstream("Node1"))
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
    
    json = "{\"id\":\"Graph1\",\"unkownfield\":42,\"nodes\":"
        + "[{\"executorId\":\"Node1\",\"executorType\":\"TimedQueryExecutor\"},"
        + "{\"executorId\":\"Node2\",\"upstream\":\"Node1\",\"executorType\":"
        + "\"TimedQueryExecutor\"}]}";
    try {
      JSON.parseToObject(json, ExecutionGraph.class);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void hashCodeEqualsCompareTo() throws Exception {
    final ExecutionGraph g1 = builder.build();
    
    ExecutionGraph g2 = ExecutionGraph.newBuilder()
        .setId("Graph1")
        .addNode(ExecutionGraphNode.newBuilder()
          .setExecutorId("Node1")
          .setExecutorType("TimedQueryExecutor"))
        .addNode(ExecutionGraphNode.newBuilder()
            .setExecutorId("Node2")
            .setExecutorType("TimedQueryExecutor")
            .setUpstream("Node1"))
        .build();
    assertEquals(g1.hashCode(), g2.hashCode());
    assertEquals(g1, g2);
    assertEquals(0, g1.compareTo(g2));
    
    g2 = ExecutionGraph.newBuilder()
        .setId("Graph2")  // <-- Diff
        .addNode(ExecutionGraphNode.newBuilder()
          .setExecutorId("Node1")
          .setExecutorType("TimedQueryExecutor"))
        .addNode(ExecutionGraphNode.newBuilder()
            .setExecutorId("Node2")
            .setExecutorType("TimedQueryExecutor")
            .setUpstream("Node1"))
        .build();
    assertNotEquals(g1.hashCode(), g2.hashCode());
    assertNotEquals(g1, g2);
    assertEquals(-1, g1.compareTo(g2));
    
    g2 = ExecutionGraph.newBuilder()
        .setId("Graph1")
        .addNode(ExecutionGraphNode.newBuilder()
          .setExecutorId("Node3")  // <-- Diff
          .setExecutorType("TimedQueryExecutor"))
        .addNode(ExecutionGraphNode.newBuilder()
            .setExecutorId("Node2")
            .setExecutorType("TimedQueryExecutor")
            .setUpstream("Node1"))
        .build();
    assertNotEquals(g1.hashCode(), g2.hashCode());
    assertNotEquals(g1, g2);
    assertEquals(-1, g1.compareTo(g2));
    
    g2 = ExecutionGraph.newBuilder()
        .setId("Graph1")
        .addNode(ExecutionGraphNode.newBuilder()  // <-- change order OK!
            .setExecutorId("Node2")
            .setExecutorType("TimedQueryExecutor")
            .setUpstream("Node1"))
        .addNode(ExecutionGraphNode.newBuilder()
            .setExecutorId("Node1")
            .setExecutorType("TimedQueryExecutor"))
        .build();
    assertEquals(g1.hashCode(), g2.hashCode());
    assertEquals(g1, g2);
    assertEquals(0, g1.compareTo(g2));
  }
  
  @Test
  public void initialize1Node() throws Exception {
    builder = ExecutionGraph.newBuilder()
      .setId(null)
      .addNode(ExecutionGraphNode.newBuilder()
          .setExecutorId("Node1")
          .setExecutorType("TimedQueryExecutor"));
    
    final ExecutionGraph graph = builder.build();
    assertNull(graph.initialize(tsdb).join());
    assertSame(tsdb, graph.tsdb());
    
    assertEquals(1, graph.executors.size());
    assertSame(executor_a, graph.executors.get("Node1"));
    assertTrue(graph.graph.containsVertex("Node1"));
    assertSame(executor_a, graph.sinkExecutor());
    assertSame(graph, graph.nodes.get(0).graph());
    try {
      graph.getDownstreamExecutor("Node1");
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
  }
  
  @Test
  public void initialize2Nodes() throws Exception {
    final ExecutionGraph graph = builder.build();
    assertNull(graph.initialize(tsdb).join());
    
    assertSame(tsdb, graph.tsdb());
    
    assertEquals(2, graph.executors.size());
    assertSame(executor_a, graph.executors.get("Node1"));
    assertSame(executor_b, graph.executors.get("Node2"));
    
    assertTrue(graph.graph.containsVertex("Node1"));
    assertTrue(graph.graph.containsVertex("Node2"));
    assertTrue(graph.graph.containsEdge("Node1", "Node2"));
    
    assertSame(executor_a, graph.sinkExecutor());
    assertSame(graph, graph.nodes.get(0).graph());
    assertSame(graph, graph.nodes.get(1).graph());
    assertSame(executor_b, graph.getDownstreamExecutor("Node1"));
  }
  
  @Test
  public void initialize3Nodes() throws Exception {
    builder = ExecutionGraph.newBuilder()
        .setId("Graph1")
        .addNode(ExecutionGraphNode.newBuilder()
            .setExecutorId("Node1")
            .setExecutorType("TimedQueryExecutor"))
        .addNode(ExecutionGraphNode.newBuilder()
            .setExecutorId("Node2")
            .setExecutorType("TimedQueryExecutor")
            .setUpstream("Node1"))
        .addNode(ExecutionGraphNode.newBuilder()
            .setExecutorId("Node3")
            .setExecutorType("TimedQueryExecutor")
            .setUpstream("Node1"));
    final ExecutionGraph graph = builder.build();
    assertNull(graph.initialize(tsdb).join());
    
    assertSame(tsdb, graph.tsdb());
    
    assertEquals(3, graph.executors.size());
    assertSame(executor_a, graph.executors.get("Node1"));
    assertSame(executor_b, graph.executors.get("Node2"));
    assertSame(executor_c, graph.executors.get("Node3"));
    
    assertTrue(graph.graph.containsVertex("Node1"));
    assertTrue(graph.graph.containsVertex("Node2"));
    assertTrue(graph.graph.containsVertex("Node3"));
    assertTrue(graph.graph.containsEdge("Node1", "Node2"));
    assertTrue(graph.graph.containsEdge("Node1", "Node3"));
    
    assertSame(executor_a, graph.sinkExecutor());
    assertSame(graph, graph.nodes.get(0).graph());
    assertSame(graph, graph.nodes.get(1).graph());
    assertSame(graph, graph.nodes.get(2).graph());
  }
  
  @Test
  public void initialize3NodesDiffOrder() throws Exception {
    builder = ExecutionGraph.newBuilder()
        .setId("Graph1")
        .addNode(ExecutionGraphNode.newBuilder()
            .setExecutorId("Node2")
            .setExecutorType("TimedQueryExecutor")
            .setUpstream("Node1"))
        .addNode(ExecutionGraphNode.newBuilder()
            .setExecutorId("Node3")
            .setExecutorType("TimedQueryExecutor")
            .setUpstream("Node1"))
        .addNode(ExecutionGraphNode.newBuilder() // sink is now last
            .setExecutorId("Node1")
            .setExecutorType("TimedQueryExecutor"));
    final ExecutionGraph graph = builder.build();
    assertNull(graph.initialize(tsdb).join());
    
    assertSame(tsdb, graph.tsdb());
    
    assertEquals(3, graph.executors.size());
    assertSame(executor_a, graph.executors.get("Node1"));
    assertSame(executor_b, graph.executors.get("Node2"));
    assertSame(executor_c, graph.executors.get("Node3"));
    
    assertTrue(graph.graph.containsVertex("Node1"));
    assertTrue(graph.graph.containsVertex("Node2"));
    assertTrue(graph.graph.containsVertex("Node3"));
    assertTrue(graph.graph.containsEdge("Node1", "Node2"));
    assertTrue(graph.graph.containsEdge("Node1", "Node3"));
    
    assertSame(executor_a, graph.sinkExecutor());
    assertSame(graph, graph.nodes.get(0).graph());
    assertSame(graph, graph.nodes.get(1).graph());
    assertSame(graph, graph.nodes.get(2).graph());
  }
  
  @Test
  public void initializeDupeId() throws Exception {
    builder = ExecutionGraph.newBuilder()
        .setId("Graph1")
        .addNode(ExecutionGraphNode.newBuilder()
            .setExecutorId("Node1")
            .setExecutorType("TimedQueryExecutor"))
        .addNode(ExecutionGraphNode.newBuilder()
            .setExecutorId("Node2")
            .setExecutorType("TimedQueryExecutor")
            .setUpstream("Node1"))
        .addNode(ExecutionGraphNode.newBuilder()
            .setExecutorId("Node2")
            .setExecutorType("TimedQueryExecutor")
            .setUpstream("Node1"));
    final ExecutionGraph graph = builder.build();
    final Deferred<Object> deferred = graph.initialize(tsdb);
    try {
      deferred.join();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // partial init
    assertSame(tsdb, graph.tsdb());
    
    assertEquals(0, graph.executors.size());
    
    assertTrue(graph.graph.containsVertex("Node1"));
    assertTrue(graph.graph.containsVertex("Node2"));
    assertFalse(graph.graph.containsVertex("Node3"));
    assertTrue(graph.graph.containsEdge("Node1", "Node2"));
    assertFalse(graph.graph.containsEdge("Node1", "Node3"));
    
    assertNull(graph.sinkExecutor());
    assertSame(graph, graph.nodes.get(0).graph());
    assertSame(graph, graph.nodes.get(1).graph());
    assertSame(graph, graph.nodes.get(2).graph());
  }
  
  @Test
  public void initializeCycle() throws Exception {
    builder = ExecutionGraph.newBuilder()
        .setId("Graph1")
        .addNode(ExecutionGraphNode.newBuilder()
            .setExecutorId("Node1")
            .setExecutorType("TimedQueryExecutor")
            .setUpstream("Node2"))
        .addNode(ExecutionGraphNode.newBuilder()
            .setExecutorId("Node2")
            .setExecutorType("TimedQueryExecutor")
            .setUpstream("Node1"));
    final ExecutionGraph graph = builder.build();
    final Deferred<Object> deferred = graph.initialize(tsdb);
    try {
      deferred.join();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // partial init
    assertSame(tsdb, graph.tsdb());
    
    assertEquals(0, graph.executors.size());
    
    assertTrue(graph.graph.containsVertex("Node1"));
    assertTrue(graph.graph.containsVertex("Node2"));
    assertFalse(graph.graph.containsEdge("Node1", "Node2"));
    
    assertNull(graph.sinkExecutor());
    assertSame(graph, graph.nodes.get(0).graph());
    assertSame(graph, graph.nodes.get(1).graph());
  }
  
  @Test
  public void initializeNoFactory() throws Exception {
    builder = ExecutionGraph.newBuilder()
        .setId("Graph1")
        .addNode(ExecutionGraphNode.newBuilder()
            .setExecutorId("Node1")
            .setExecutorType("TimedQueryExecutor"))
        .addNode(ExecutionGraphNode.newBuilder()
            .setExecutorId("Node2")
            .setExecutorType("MultiClusterQueryExecutor")
            .setUpstream("Node1"));
    final ExecutionGraph graph = builder.build();
    final Deferred<Object> deferred = graph.initialize(tsdb);
    try {
      deferred.join();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // partial init
    assertSame(tsdb, graph.tsdb());
    
    assertEquals(0, graph.executors.size());
    
    assertTrue(graph.graph.containsVertex("Node1"));
    assertTrue(graph.graph.containsVertex("Node2"));
    assertTrue(graph.graph.containsEdge("Node1", "Node2"));
    
    assertNull(graph.sinkExecutor());
    assertSame(graph, graph.nodes.get(0).graph());
    assertSame(graph, graph.nodes.get(1).graph());
  }
  
  @Test
  public void initializeFactoryReturnedNullExecutor() throws Exception {
    builder = ExecutionGraph.newBuilder()
        .setId("Graph1")
        .addNode(ExecutionGraphNode.newBuilder()
            .setExecutorId("Node1")
            .setExecutorType("TimedQueryExecutor"))
        .addNode(ExecutionGraphNode.newBuilder()
            .setExecutorId("Node4")
            .setExecutorType("TimedQueryExecutor")
            .setUpstream("Node1"));
    final ExecutionGraph graph = builder.build();
    final Deferred<Object> deferred = graph.initialize(tsdb);
    try {
      deferred.join();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    
    // partial init
    assertSame(tsdb, graph.tsdb());
    assertEquals("Node4", graph.nodes.get(1).getExecutorId());
    
    assertEquals(0, graph.executors.size());
    
    assertTrue(graph.graph.containsVertex("Node1"));
    assertTrue(graph.graph.containsVertex("Node4"));
    assertTrue(graph.graph.containsEdge("Node1", "Node4"));
    
    assertNull(graph.sinkExecutor());
    assertSame(graph, graph.nodes.get(0).graph());
    assertSame(graph, graph.nodes.get(1).graph());
  }
  
  @Test
  public void initializeTwoSinks() throws Exception {
    builder = ExecutionGraph.newBuilder()
        .setId("Graph1")
        .addNode(ExecutionGraphNode.newBuilder()
            .setExecutorId("Node1")
            .setExecutorType("TimedQueryExecutor"))
        .addNode(ExecutionGraphNode.newBuilder()
            .setExecutorId("Node2")
            .setExecutorType("TimedQueryExecutor"));
    final ExecutionGraph graph = builder.build();
    final Deferred<Object> deferred = graph.initialize(tsdb);
    try {
      deferred.join();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // partial init
    assertSame(tsdb, graph.tsdb());
    
    assertEquals(2, graph.executors.size());
    assertSame(executor_a, graph.executors.get("Node1"));
    assertSame(executor_b, graph.executors.get("Node2"));
    
    assertTrue(graph.graph.containsVertex("Node1"));
    assertTrue(graph.graph.containsVertex("Node2"));
    assertFalse(graph.graph.containsEdge("Node1", "Node2"));
    
    assertSame(executor_a, graph.sinkExecutor());
    assertSame(graph, graph.nodes.get(0).graph());
    assertSame(graph, graph.nodes.get(1).graph());
  }
  
  @Test
  public void initializeUnexpectedException() throws Exception {
    final IllegalStateException ex = new IllegalStateException("Boo!");
    when(tsdb.getRegistry()).thenThrow(ex);
    
    final ExecutionGraph graph = builder.build();
    final Deferred<Object> deferred = graph.initialize(tsdb);
    try {
      deferred.join();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    
    // partial init
    assertSame(tsdb, graph.tsdb());
    
    assertEquals(0, graph.executors.size());
    
    assertTrue(graph.graph.containsVertex("Node1"));
    assertTrue(graph.graph.containsVertex("Node2"));
    assertTrue(graph.graph.containsEdge("Node1", "Node2"));
    
    assertNull(graph.sinkExecutor());
    assertSame(graph, graph.nodes.get(0).graph());
    assertSame(graph, graph.nodes.get(1).graph());
  }
  
  @Test
  public void initializeNoSuchUpstreamNode() throws Exception {
    builder = ExecutionGraph.newBuilder()
      .setId(null)
      .addNode(ExecutionGraphNode.newBuilder()
          .setExecutorId("Node1")
          .setExecutorType("TimedQueryExecutor")
          .setUpstream("NoSuchNode"));
    
    final ExecutionGraph graph = builder.build();
    try {
      graph.initialize(tsdb).join();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void initializeWithPrefix() throws Exception {
    factory = mock(QueryExecutorFactory.class);
    when(registry.getFactory("TimedQueryExecutor"))
      .thenAnswer(new Answer<QueryExecutorFactory<?>>() {
      @Override
      public QueryExecutorFactory<?> answer(InvocationOnMock invocation)
          throws Throwable {
        return factory;
      }
    });
    when(factory.newExecutor(any(ExecutionGraphNode.class)))
      .thenAnswer(new Answer<QueryExecutor<?>>() {
      @Override
      public QueryExecutor<?> answer(InvocationOnMock invocation)
          throws Throwable {
        final ExecutionGraphNode node = 
            (ExecutionGraphNode) invocation.getArguments()[0];
        if (node.getExecutorId().equals("Cluster_Node1")) {
          return executor_a;
        } else if (node.getExecutorId().equals("Cluster_Node2")) {
          return executor_b;
        }
        return null;
      }
    });
    when(executor_a.id()).thenReturn("Cluster_Node1");
    when(executor_b.id()).thenReturn("Cluster_Node2");
    
    final ExecutionGraph graph = builder.build();
    assertNull(graph.initialize(tsdb, "Cluster").join());
    
    assertSame(tsdb, graph.tsdb());
    
    assertEquals(2, graph.executors.size());
    assertSame(executor_a, graph.executors.get("Cluster_Node1"));
    assertSame(executor_b, graph.executors.get("Cluster_Node2"));
    
    assertTrue(graph.graph.containsVertex("Cluster_Node1"));
    assertTrue(graph.graph.containsVertex("Cluster_Node2"));
    assertTrue(graph.graph.containsEdge("Cluster_Node1", "Cluster_Node2"));
    assertSame(executor_b, graph.getDownstreamExecutor("Cluster_Node1"));
    
    assertSame(executor_a, graph.sinkExecutor());
    assertSame(graph, graph.nodes.get(0).graph());
    assertSame(graph, graph.nodes.get(1).graph());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void initializeNullTSDB() throws Exception {
    final ExecutionGraph graph = builder.build();
    graph.initialize(null);
  }

  @Test
  public void getDownstreamExecutor() throws Exception {
    final ExecutionGraph graph = builder.build();
    assertNull(graph.initialize(tsdb).join());
    
    assertSame(executor_b, graph.getDownstreamExecutor("Node1"));
    
    try {
      graph.getDownstreamExecutor(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      graph.getDownstreamExecutor("");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      graph.getDownstreamExecutor("Node3");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      graph.getDownstreamExecutor("Node2");
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
  }
}
