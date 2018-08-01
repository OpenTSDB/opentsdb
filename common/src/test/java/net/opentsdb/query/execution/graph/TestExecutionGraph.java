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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;

import net.opentsdb.configuration.Configuration;
import net.opentsdb.core.MockTSDB;
import net.opentsdb.core.TSDB;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.SingleQueryNodeFactory;

public class TestExecutionGraph {

  protected static final ObjectMapper MAPPER = new ObjectMapper();
  
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
    graph = MAPPER.readValue(json, ExecutionGraph.class);
    
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
    
    json = MAPPER.writeValueAsString(graph);
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
    graph = MAPPER.readValue(json, ExecutionGraph.class);
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
  
  @Test
  public void parse() throws Exception {
    TSDB tsdb = new MockTSDB();
    when(tsdb.getRegistry().getQueryNodeFactory("mockfactorya"))
      .thenReturn(new MockFactoryA());
    when(tsdb.getRegistry().getQueryNodeFactory("mockfactoryb"))
      .thenReturn(new MockFactoryB());
    
    String json = "{\"id\":\"Graph1\",\"nodes\":[{\"id\":\"N1\","
        + "\"type\":\"MockFactoryA\",\"sources\":[\"S1\",\"S2\"],\"config\":"
        + "{\"id\":\"N1\",\"foo\":\"myfoo\",\"bars\":[\"a\",\"b\",\"c\"]}},"
        + "{\"id\":\"S1\",\"type\":\"MockFactoryB\"},"
        + "{\"id\":\"S2\",\"type\":\"MockFactoryB\",\"config\":"
        + "{\"id\":\"S2\",\"mynum\":42,\"settings\":{\"a\":-2.76,\"b\":42.5}}}]}";
    JsonNode root = MAPPER.readTree(json);
    ExecutionGraph graph = ExecutionGraph.parse(MAPPER, tsdb, root).build();
    assertEquals("Graph1", graph.getId());
    assertEquals(3, graph.getNodes().size());
   
    ExecutionGraphNode node = graph.getNodes().get(0);
    assertEquals("N1", node.getId());
    assertEquals("MockFactoryA", node.getType());
    assertEquals(2, node.getSources().size());
    assertEquals("S1", node.getSources().get(0));
    assertEquals("S2", node.getSources().get(1));
    assertTrue(node.getConfig() instanceof MockConfigA);
    assertEquals("myfoo", ((MockConfigA) node.getConfig()).foo);
    assertEquals(3, ((MockConfigA) node.getConfig()).bars.size());
    assertEquals("a", ((MockConfigA) node.getConfig()).bars.get(0));
    assertEquals("b", ((MockConfigA) node.getConfig()).bars.get(1));
    assertEquals("c", ((MockConfigA) node.getConfig()).bars.get(2));
    
    node = graph.getNodes().get(1);
    assertEquals("S1", node.getId());
    assertEquals("MockFactoryB", node.getType());
    assertNull(node.getSources());
    
    node = graph.getNodes().get(2);
    assertEquals("S2", node.getId());
    assertEquals("MockFactoryB", node.getType());
    assertNull(node.getSources());
    assertTrue(node.getConfig() instanceof MockConfigB);
    assertEquals(42, ((MockConfigB) node.getConfig()).mynum);
    assertEquals(2, ((MockConfigB) node.getConfig()).settings.size());
    assertEquals(-2.76, ((MockConfigB) node.getConfig()).settings.get("a"), 0.001);
    assertEquals(42.5, ((MockConfigB) node.getConfig()).settings.get("b"), 0.001);
    
    ObjectMapper yaml_mapper = new ObjectMapper(new YAMLFactory());
    // YAML!
    String yaml = "---\n" +
        "id: Graph1\n" +
        "nodes:\n" +
        "  - \n" +
        "    id: N1\n" +
        "    type: MockFactoryA\n" +
        "    sources:\n" +
        "      - S1\n" +
        "      - S2\n" +
        "    config:\n" +
        "      foo: myfoo\n" +
        "      bars:\n" +
        "        - a\n" +
        "        - b\n" +
        "        - c\n" +
        "  -\n" +
        "    id: S1\n" +
        "    type: MockFactoryB\n" +
        "  -\n" +
        "    id: S2\n" +
        "    type: MockFactoryB\n" +
        "    config:\n" +
        "      mynum: 42\n" +
        "      settings:\n" +
        "        a: -2.76\n" +
        "        b: 42.5";
    root = yaml_mapper.readTree(yaml);
    graph = ExecutionGraph.parse(yaml_mapper, tsdb, root).build();
    assertEquals("Graph1", graph.getId());
    assertEquals(3, graph.getNodes().size());
   
    node = graph.getNodes().get(0);
    assertEquals("N1", node.getId());
    assertEquals("MockFactoryA", node.getType());
    assertEquals(2, node.getSources().size());
    assertEquals("S1", node.getSources().get(0));
    assertEquals("S2", node.getSources().get(1));
    assertTrue(node.getConfig() instanceof MockConfigA);
    assertEquals("myfoo", ((MockConfigA) node.getConfig()).foo);
    assertEquals(3, ((MockConfigA) node.getConfig()).bars.size());
    assertEquals("a", ((MockConfigA) node.getConfig()).bars.get(0));
    assertEquals("b", ((MockConfigA) node.getConfig()).bars.get(1));
    assertEquals("c", ((MockConfigA) node.getConfig()).bars.get(2));
    
    node = graph.getNodes().get(1);
    assertEquals("S1", node.getId());
    assertEquals("MockFactoryB", node.getType());
    assertNull(node.getSources());
    
    node = graph.getNodes().get(2);
    assertEquals("S2", node.getId());
    assertEquals("MockFactoryB", node.getType());
    assertNull(node.getSources());
    assertTrue(node.getConfig() instanceof MockConfigB);
    assertEquals(42, ((MockConfigB) node.getConfig()).mynum);
    assertEquals(2, ((MockConfigB) node.getConfig()).settings.size());
    assertEquals(-2.76, ((MockConfigB) node.getConfig()).settings.get("a"), 0.001);
    assertEquals(42.5, ((MockConfigB) node.getConfig()).settings.get("b"), 0.001);
    
    // No such node with config
    json = "{\"id\":\"Graph1\",\"nodes\":[{\"id\":\"N1\","
        + "\"type\":\"MockFactoryA\",\"sources\":[\"S1\",\"S2\"],\"config\":"
        + "{\"id\":\"N1\",\"foo\":\"myfoo\",\"bars\":[\"a\",\"b\",\"c\"]}},"
        + "{\"id\":\"S1\",\"type\":\"MockFactoryB\"},"
        + "{\"id\":\"S2\",\"type\":\"MockFactoryC\",\"config\":"
        + "{\"id\":\"S2\",\"mynum\":42,\"settings\":{\"a\":-2.76,\"b\":42.5}}}]}";
    root = MAPPER.readTree(json);
    
    try {
      ExecutionGraph.parse(MAPPER, tsdb, root).build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // No such node, but it's OK because there isn't a config to parse.
    json = "{\"id\":\"Graph1\",\"nodes\":[{\"id\":\"N1\","
        + "\"type\":\"MockFactoryA\",\"sources\":[\"S1\",\"S2\"],\"config\":"
        + "{\"id\":\"N1\",\"foo\":\"myfoo\",\"bars\":[\"a\",\"b\",\"c\"]}},"
        + "{\"id\":\"S1\",\"type\":\"MockFactoryC\"},"
        + "{\"id\":\"S2\",\"type\":\"MockFactoryB\",\"config\":"
        + "{\"id\":\"S2\",\"mynum\":42,\"settings\":{\"a\":-2.76,\"b\":42.5}}}]}";
    root = MAPPER.readTree(json);
    graph = ExecutionGraph.parse(MAPPER, tsdb, root).build();
    assertEquals(3, graph.getNodes().size());
  }
  
  @JsonInclude(Include.NON_NULL)
  @JsonDeserialize(builder = MockConfigA.Builder.class)
  static class MockConfigA implements QueryNodeConfig {
    public String foo;
    public List<String> bars;
    public String id;
    
    protected MockConfigA(final Builder builder) {
      foo = builder.foo;
      bars = builder.bars;
      id = builder.id;
    }

    @Override
    public HashCode buildHashCode() { return null; }

    @Override
    public int compareTo(QueryNodeConfig o) { return 0; }

    @Override
    public boolean equals(Object o) { return false; }

    @Override
    public int hashCode() { return 0; }
    
    @Override
    public String getId() {
      return id;
    }

    @Override
    public Map<String, String> getOverrides() { return null; }

    @Override
    public String getString(Configuration config, String key) { return null; }

    @Override
    public int getInt(Configuration config, String key) { return 0; }

    @Override
    public long getLong(Configuration config, String key) { return 0; }

    @Override
    public boolean getBoolean(Configuration config, String key) { return false; }

    @Override
    public double getDouble(Configuration config, String key) { return 0; }

    @Override
    public boolean hasKey(String key) { return false; }
  
    public static Builder newBuilder() {
      return new Builder();
    }
    
    @JsonIgnoreProperties(ignoreUnknown = true)
    static class Builder {
      @JsonProperty
      private String id;
      @JsonProperty
      private String foo;
      @JsonProperty
      private List<String> bars;
      
      public Builder setId(final String id) {
        this.id = id;
        return this;
      }
      
      public Builder setFoo(final String foo) {
        this.foo = foo;
        return this;
      }
      
      public Builder setBars(final List<String> bars) {
        this.bars = bars;
        return this;
      }
      
      public QueryNodeConfig build() {
        return new MockConfigA(this);
      }
      
    }
  }
  
  static class MockFactoryA implements SingleQueryNodeFactory {

    @Override
    public QueryNode newNode(QueryPipelineContext context, String id) {
      return newNode(context, id, null);
    }

    @Override
    public QueryNode newNode(QueryPipelineContext context, String id,
        QueryNodeConfig config) {
      return null;
    }

    @Override
    public String id() {
      return "MockFactoryA";
    }

    @Override
    public QueryNodeConfig parseConfig(ObjectMapper mapper, TSDB tsdb,
        JsonNode node) {
      try {
        return (QueryNodeConfig) mapper.treeToValue(node, MockConfigA.class);
      } catch (JsonProcessingException e) {
        throw new IllegalArgumentException("Failed to parse TagValueLiteralOr", e);
      }
    }
  }
  
  @JsonInclude(Include.NON_NULL)
  @JsonDeserialize(builder = MockConfigB.Builder.class)
  static class MockConfigB implements QueryNodeConfig {
    public long mynum;
    public Map<String, Double> settings;
    public String id;
    
    protected MockConfigB(final Builder builder) {
      mynum = builder.mynum;
      settings = builder.settings;
      id = builder.id;
    }

    @Override
    public HashCode buildHashCode() { return null; }

    @Override
    public int compareTo(QueryNodeConfig o) { return 0; }

    @Override
    public boolean equals(Object o) { return false; }

    @Override
    public int hashCode() { return 0; }
    
    @Override
    public String getId() {
      return id;
    }

    @Override
    public Map<String, String> getOverrides() { return null; }

    @Override
    public String getString(Configuration config, String key) { return null; }

    @Override
    public int getInt(Configuration config, String key) { return 0; }

    @Override
    public long getLong(Configuration config, String key) { return 0; }

    @Override
    public boolean getBoolean(Configuration config, String key) { return false; }

    @Override
    public double getDouble(Configuration config, String key) { return 0; }

    @Override
    public boolean hasKey(String key) { return false; }
    
    @JsonIgnoreProperties(ignoreUnknown = true)
    static class Builder {
      @JsonProperty
      private String id;
      @JsonProperty
      private long mynum;
      @JsonProperty
      private Map<String, Double> settings;
      
      public Builder setId(final String id) {
        this.id = id;
        return this;
      }
      
      public Builder setMynum(final long mynum) {
        this.mynum = mynum;
        return this;
      }
      
      public Builder setSEttings(final Map<String, Double> settings) {
        this.settings = settings;
        return this;
      }
      
      public QueryNodeConfig build() {
        return new MockConfigB(this);
      }
      
    }

  }
  
  static class MockFactoryB implements SingleQueryNodeFactory {

    @Override
    public QueryNode newNode(QueryPipelineContext context, String id) {
      return newNode(context, id, null);
    }

    @Override
    public QueryNode newNode(QueryPipelineContext context, String id,
        QueryNodeConfig config) {
      return null;
    }

    @Override
    public String id() {
      return "MockFactoryB";
    }
    
    @Override
    public QueryNodeConfig parseConfig(ObjectMapper mapper, TSDB tsdb,
        JsonNode node) {
      try {
        return (QueryNodeConfig) mapper.treeToValue(node, MockConfigB.class);
      } catch (JsonProcessingException e) {
        throw new IllegalArgumentException("Failed to parse TagValueLiteralOr", e);
      }
    }
  }
  
}