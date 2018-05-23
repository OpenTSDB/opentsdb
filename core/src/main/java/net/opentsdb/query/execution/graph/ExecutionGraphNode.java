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

import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Strings;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;

import net.opentsdb.core.Const;
import net.opentsdb.query.execution.QueryExecutor;
import net.opentsdb.query.execution.QueryExecutorConfig;

/**
 * A serializable executor node configuration for use in building DAG of 
 * executors.
 * <p>
 * The only required value is {@link Builder#setExecutorId(String)}. This
 * must be a unique ID within the query graph. Additionally, if 
 * {@link Builder#setExecutorType(String)} was not set to a value, then
 * the {@link #getExecutorId()} must be the name of a {@link QueryNode} 
 * factory.
 * <p>
 * An optional config can be supplied for the specific executor, 
 * otherwise the defaults are used.
 * <p>
 * If the node is a child of another executor, make sure to 
 * set {@link #getUpstream()} to the Id of the upstream executor.
 * 
 * @since 3.0
 */
@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonDeserialize(builder = ExecutionGraphNode.Builder.class)
public class ExecutionGraphNode implements Comparable<ExecutionGraphNode> {
  
  /** The unique ID of this executor node within the graph. */
  private String executor_id;
  
  /** The class of an {@link QueryNode} implementation. */
  private String executor_type;
  
  /** An optional upstream executor ID. */
  private String upstream;
  
  /** An optional configuration for the executor. */
  private QueryExecutorConfig config;
  
  /** The graph this node belongs to (set when the graph is initialized) */
  private ExecutionGraph graph;
  
  /**
   * Protected ctor to build from the builder.
   * @param builder A non-null builder.
   * @throws IllegalArgumentException if the id or executor types were null or
   * empty.
   */
  protected ExecutionGraphNode(final Builder builder) {
    if (Strings.isNullOrEmpty(builder.executorId)) {
      throw new IllegalArgumentException("ID cannot be null or empty");
    }
    if (Strings.isNullOrEmpty(builder.executorType)) {
      executor_type = builder.executorId;
    } else {
      executor_type = builder.executorType;
    }
    executor_id = builder.executorId;
    upstream = builder.upstream;
    config = builder.config;
  }
  
  /** @return The id of this executor node. */
  public String getExecutorId() {
    return executor_id;
  }
  
  /** @return The class of the executor implementation. */
  public String getExecutorType() {
    return executor_type;
  }
    
  /** @return An optional node ID of an upstream executor node. */
  public String getUpstream() {
    return upstream;
  }
  
  /** @return An optional configuration for the node. May be null. */
  public QueryExecutorConfig getConfig() {
    return config;
  }
  
  /**
   * <b>WARNING:</b> Public only for unit testing. Don't play with it.
   * @param graph The graph this node belongs to.
   */
  public void setExecutionGraph(final ExecutionGraph graph) {
    this.graph = graph;
  }
  
  /**
   * Package private method used by the graph to override the node ID.
   * @param id A non-null and non-empty node ID.
   */
  void resetId(final String id) {
    if (Strings.isNullOrEmpty(id)) {
      throw new IllegalArgumentException("ID cannot be null.");
    }
    this.executor_id = id;
  }
  
  /**
   * Package private method used by the graph to override the upstream ID.
   * @param upstream A non-null and non-empty node ID.
   */
  void resetUpstream(final String upstream) {
    if (Strings.isNullOrEmpty(upstream)) {
      throw new IllegalArgumentException("Upstream cannot be null.");
    }
    this.upstream = upstream;
  }
  
  /** @return The graph this node belongs to. Null if the graph has not been
   * initialized yet. */
  public ExecutionGraph graph() {
    return graph;
  }
  
  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ExecutionGraphNode node = (ExecutionGraphNode) o;
    return Objects.equals(executor_id, node.executor_id)
        && Objects.equals(upstream, node.upstream)
        && Objects.equals(executor_type, node.executor_type)
        && Objects.equals(config, node.config);
  }
  
  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }
  
  /** @return A HashCode object for deterministic, non-secure hashing */
  public HashCode buildHashCode() {
    final HashCode hc = Const.HASH_FUNCTION().newHasher()
        .putString(Strings.nullToEmpty(executor_id), Const.UTF8_CHARSET)
        .putString(Strings.nullToEmpty(upstream), Const.UTF8_CHARSET)
        .putString(Strings.nullToEmpty(executor_type), Const.UTF8_CHARSET)
        .hash();
    if (config != null) {
      final List<HashCode> hashes = Lists.newArrayListWithCapacity(2);
      hashes.add(hc);
      hashes.add(config.buildHashCode());
      return Hashing.combineOrdered(hashes);
    }
    return hc;
  }
  
  @Override
  public int compareTo(final ExecutionGraphNode o) {
    return ComparisonChain.start()
        .compare(executor_id, o.executor_id, Ordering.natural().nullsFirst())
        .compare(upstream, o.upstream, Ordering.natural().nullsFirst())
        .compare(executor_type, o.executor_type, Ordering.natural().nullsFirst())
        .compare(config, o.config, 
            Ordering.natural().nullsFirst())
        .result();
  }
  
  @Override
  public String toString() {
    return new StringBuilder()
        .append("id=")
        .append(executor_id)
        .append(", upstream=")
        .append(upstream)
        .append(", executor=")
        .append(executor_type)
        .append(", config=[")
        .append(config)
        .append("], graph=[")
        .append(graph == null ? null : graph.getId()) // don't print .toString() or it'll recurse.
        .append("]")
        .toString();
  }
  
  /** @return A new node builder. */
  public static Builder newBuilder() {
    return new Builder();
  }
  
  /** 
   * A node builder set with parameters cloned from the given node
   * @param node A non-null node to pull from.
   * @return A non-null builder.
   */
  public static Builder newBuilder(final ExecutionGraphNode node) {
    return new Builder()
        .setExecutorId(node.executor_id)
        .setUpstream(node.upstream)
        .setExecutorType(node.executor_type)
        .setConfig(node.config); // TODO ! may need copy
  }
  
  /** Builder class for the graph node. */
  public static class Builder {
    @JsonProperty
    private String executorId;
    @JsonProperty
    private String executorType;
    @JsonProperty
    private String upstream;
    @JsonProperty
    private QueryExecutorConfig config;
    
    /**
     * @param executorId A non-null and non-empty ID for the node.
     * @return The builder.
     */
    public Builder setExecutorId(final String executorId) {
      this.executorId = executorId;
      return this;
    }
    
    /**
     * @param executorType The class of the executor implementation.
     * @return The builder.
     */
    public Builder setExecutorType(final String executorType) {
      this.executorType = executorType;
      return this;
    }
    
    /**
     * @param upstream An optional upstream ID of an executor node.
     * @return The builder.
     */
    public Builder setUpstream(final String upstream) {
      this.upstream = upstream;
      return this;
    }
    
    /**
     * @param config An optional config for the executor.
     * @return The builder.
     */
    public Builder setConfig(final QueryExecutorConfig config) {
      this.config = config;
      return this;
    }
    
    /**
     * @param defaultConfig An optional default config for the executor.
     * @return The builder.
     */
    @JsonIgnore
    public Builder setDefaultConfig(
        final QueryExecutorConfig.Builder defaultConfig) {
      this.config = defaultConfig.build();
      return this;
    }
    
    /** @return A graph node on success or an exception if required parameters 
     * were missing. */
    public ExecutionGraphNode build() {
      return new ExecutionGraphNode(this);
    }
  }
}
