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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;

import net.opentsdb.core.Const;
import net.opentsdb.core.DefaultTSDB;
import net.opentsdb.core.TSDB;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.utils.JSON;
import net.opentsdb.utils.JSONException;

/**
 * An execution graph that defines a set of executors, default configs and the
 * path of execution. The class must be serialiazble so that one or more 
 * graphs can be setup per TSD and consumed or overridden at query time.
 * <p>
 * Note that the builder is used to instantiate the object (via code or JSON
 * deserialization) and construction/validation of the graph must be done
 * elsewhere.
 * 
 * @since 3.0
 */
@JsonInclude(Include.NON_NULL)
@JsonDeserialize(builder = ExecutionGraph.Builder.class)
public class ExecutionGraph implements Comparable<ExecutionGraph> {
  /** The TSDB to which this graph belongs. */
  protected TSDB tsdb;
  
  /** The ID of this execution graph. */
  protected String id;
  
  /** The list of nodes given by the user or config. */
  protected List<ExecutionGraphNode> nodes;

  /** A set of optional node configs manually de-serialized. Not counted
   * in the equals, hash code or comparable. */
  protected Map<String, QueryNodeConfig> node_configs;
  
  /**
   * Protected ctor that sets up maps but doesn't generate the graph.
   * @param builder A non-null builder.
   */
  protected ExecutionGraph(final Builder builder) {
    if (builder.nodes == null || builder.nodes.isEmpty()) {
      throw new IllegalArgumentException("Executors cannot be null or empty.");
    }
    id = builder.id;
    nodes = builder.nodes;
  }

  /** @return The unique ID of this graph. */
  public String getId() {
    return id;
  }
  
  /** @return The list of nodes configured in this graph. */
  public List<ExecutionGraphNode> getNodes() {
    return Collections.unmodifiableList(nodes);
  }
  
  /** @return An optional map of node configs. */
  public Map<String, QueryNodeConfig> nodeConfigs() {
    return node_configs != null ? node_configs : Collections.emptyMap();
  }
  
  /**
   * Overrides the list of node configs with those given in the collection.
   * Uses the {@link QueryNodeConfig#getId()} as the ID of a specific
   * node or the name of node types. 
   * @param configs A non-null list of node configs. May be empty.
   * @throws IllegalArgumentException if the collection was null.
   */
  public void setNodeConfigs(final Collection<QueryNodeConfig> configs) {
    if (configs == null) {
      throw new IllegalArgumentException("Configs cannot be null.");
    }
    if (node_configs != null) {
      node_configs.clear();
    }
    for (final QueryNodeConfig config : configs) {
      addNodeConfig(config);
    }
  }
  
  /**
   * Adds the given node config to the map using the 
   * {@link QueryNodeConfig#getId()} as the unique key of config. This 
   * may map to a node type to be used as the default for all nodes of 
   * that type or it may be a specific node ID. It may not be null.
   * @param config A non-null config.
   * @throws IllegalArgumentException if the config was null or the 
   * id of the config was null or empty or if a config with the same
   * ID was already present.
   */
  public void addNodeConfig(final QueryNodeConfig config) {
    if (config == null) {
      throw new IllegalArgumentException("Config cannot be null.");
    }
    if (Strings.isNullOrEmpty(config.getId())) {
      throw new IllegalArgumentException("Config ID cannot be null or "
          + "empty.");
    }
    if (node_configs == null) {
      node_configs = Maps.newHashMap();
    }
    if (node_configs.containsKey(config.getId())) {
      throw new IllegalArgumentException("Duplicate config ID found "
          + "for:  " + config.getId());
    }
    node_configs.put(config.getId(), config);
  }
  
  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    final ExecutionGraph graph = (ExecutionGraph) o;
    return Objects.equal(id, graph.id)
        && Objects.equal(nodes, graph.nodes);
  }
  
  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }
  
  /** @return A HashCode object for deterministic, non-secure hashing */
  public HashCode buildHashCode() {
    final HashCode hc = Const.HASH_FUNCTION().newHasher()
        .putString(Strings.nullToEmpty(id), Const.UTF8_CHARSET)
        .hash();
    final List<HashCode> hashes = 
        Lists.newArrayListWithCapacity(nodes.size() + 1);
    Collections.sort(nodes);
    hashes.add(hc);
    for (final ExecutionGraphNode node : nodes) {
      hashes.add(node.buildHashCode());
    }
    return Hashing.combineOrdered(hashes);
  }
  
  @Override
  public int compareTo(final ExecutionGraph o) {
    return ComparisonChain.start()
        .compare(id, o.id, Ordering.natural().nullsFirst())
        .compare(nodes, o.nodes, 
            Ordering.<ExecutionGraphNode>natural().lexicographical().nullsFirst())
        .result();
  }
  
  @Override
  public String toString() {
    return new StringBuilder()
        .append("id=")
        .append(id)
        .append(", nodes=")
        .append(nodes)
        .append(", nodeConfigs=")
        .append(node_configs)
        .toString();
  }
  
  /** @return A new builder for constructing graphs. */
  public static Builder newBuilder() {
    return new Builder();
  }
  
  /**
   * Clones a graph, returning a builder. <b>NOTE:</b> The cloned graph must be
   * initialized via {@link #initialize(DefaultTSDB)}.
   * @param graph A non-null graph to clone from.
   * @return A cloned builder using configs from the source graph.
   */
  public static Builder newBuilder(final ExecutionGraph graph) {
    final Builder builder = new Builder()
        .setId(graph.id);
    final List<ExecutionGraphNode> nodes = 
        Lists.newArrayListWithExpectedSize(graph.nodes.size());
    for (final ExecutionGraphNode node : graph.nodes) {
      nodes.add(ExecutionGraphNode.newBuilder(node).build());
    }
    builder.setNodes(nodes);
    return builder;
  }
  
  /** A builder for ExecutionGraphs. */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Builder {
    @JsonProperty
    private String id;
    @JsonProperty
    private List<ExecutionGraphNode> nodes;
    
    /**
     * @param id The non-null and non-empty ID of the graph.
     * @return The builder.
     */
    public Builder setId(final String id) {
      this.id = id;
      return this;
    }
   
    /**
     * @param nodes A non-null and non-empty set of graph nodes.
     * @return The builder.
     */
    public Builder setNodes(final List<ExecutionGraphNode> nodes) {
      this.nodes = nodes;
      if (this.nodes != null) {
        Collections.sort(this.nodes);
      }
      return this;
    }
    
    /**
     * @param node A non-null node to add to the configuration.
     * @return The builder.
     */
    public Builder addNode(final ExecutionGraphNode node) {
      if (nodes == null) {
        nodes = Lists.newArrayList(node);
      } else {
        nodes.add(node);
        Collections.sort(nodes);
      }
      return this;
    }
    
    /**
     * @param node A non-null node builder to add to the configuration.
     * @return The builder.
     */
    @JsonIgnore
    public Builder addNode(final ExecutionGraphNode.Builder node) {
      if (nodes == null) {
        nodes = Lists.newArrayList(node.build());
      } else {
        nodes.add(node.build());
        Collections.sort(nodes);
      }
      return this;
    }
    
    /** @return An ExecutionGraph instance that needs to be initialized. */
    public ExecutionGraph build() {
      return new ExecutionGraph(this);
    }
  }

  public static ExecutionGraph.Builder parse(final TSDB tsdb, 
                                             final JsonNode graph_root) {
    if (graph_root == null) {
      throw new IllegalArgumentException("Graph root cannot be null.");
    }
    final Builder builder = newBuilder();
    builder.setId(graph_root.get("id").asText());
    
    final JsonNode nodes = graph_root.get("nodes");
    for (final JsonNode node : nodes) {
      final ExecutionGraphNode.Builder node_builder = 
          ExecutionGraphNode.newBuilder();
      final String id = node.get("id").asText();
      final String type = node.get("type").asText();
      node_builder.setId(id);
      if (!Strings.isNullOrEmpty(type)) {
        node_builder.setType(type);
      }
      
      final JsonNode sources = node.get("sources");
      if (sources != null) {
        try {
          node_builder.setSources(JSON.getMapper().treeToValue(
              node.get("sources"), List.class));
        } catch (JsonProcessingException e) {
          throw new JSONException("Failed to parse sources: " + node, e);
        }
      }
      
      final JsonNode config = node.get("config");
      if (config != null) {
        final QueryNodeFactory factory;
        if (!Strings.isNullOrEmpty(type)) {
          factory = tsdb.getRegistry().getQueryNodeFactory(type.toLowerCase());
          if (factory == null) {
            throw new IllegalArgumentException("No node factory found "
                + "for node type: " + type);
          }
        } else {
          factory = tsdb.getRegistry().getQueryNodeFactory(id.toLowerCase());
          if (factory == null) {
            throw new IllegalArgumentException("No node factory found "
                + "for node type: " + id);
          }
        }
        try {
          final QueryNodeConfig node_config = 
              JSON.getMapper().treeToValue(config, factory.nodeConfigClass());
          node_builder.setConfig(node_config);
          
        } catch (JsonProcessingException e) {
          throw new JSONException("Failed to parse config: " + node, e);
        }
      }
      
      builder.addNode(node_builder.build());
    }
    
    return builder;
  }
  
}
