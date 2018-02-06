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
 * The required components are the {@link #getExecutorType()} that must map
 * to a {@link QueryExecutor} class and the {@link #getExecutorId()} that must 
 * be a unique ID within the graph.
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
  private String executorid;
  
  /** The class of an {@link QueryExecutor} implementation. */
  private String executor_type;
  
  /** TODO - may not need this but in the future may be used to define the 
   * response type. */
  private String data_type;
  
  /** An optional upstream executor ID. */
  private String upstream;
  
  /** An optional default configuration for the executor. */
  private QueryExecutorConfig default_config;
  
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
      throw new IllegalArgumentException("Executor type cannot be null or empty.");
    }
    executorid = builder.executorId;
    executor_type = builder.executorType;
    data_type = builder.dataType;
    upstream = builder.upstream;
    default_config = builder.defaultConfig;
  }
  
  /** @return The id of this executor node. */
  public String getExecutorId() {
    return executorid;
  }
  
  /** @return The class of the executor implementation. */
  public String getExecutorType() {
    return executor_type;
  }
  
  /** @return The data type the executor will return. */
  public String getDataType() {
    return data_type;
  }
  
  /** @return An optional node ID of an upstream executor node. */
  public String getUpstream() {
    return upstream;
  }
  
  /** @return An optional default configuration. May be null. */
  public QueryExecutorConfig getDefaultConfig() {
    return default_config;
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
    this.executorid = id;
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
    return Objects.equals(executorid, node.executorid)
        && Objects.equals(data_type, node.data_type)
        && Objects.equals(upstream, node.upstream)
        && Objects.equals(executor_type, node.executor_type)
        && Objects.equals(default_config, node.default_config);
  }
  
  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }
  
  /** @return A HashCode object for deterministic, non-secure hashing */
  public HashCode buildHashCode() {
    final HashCode hc = Const.HASH_FUNCTION().newHasher()
        .putString(Strings.nullToEmpty(executorid), Const.UTF8_CHARSET)
        .putString(Strings.nullToEmpty(data_type), Const.UTF8_CHARSET)
        .putString(Strings.nullToEmpty(upstream), Const.UTF8_CHARSET)
        .putString(Strings.nullToEmpty(executor_type), Const.UTF8_CHARSET)
        .hash();
    if (default_config != null) {
      final List<HashCode> hashes = Lists.newArrayListWithCapacity(2);
      hashes.add(hc);
      hashes.add(default_config.buildHashCode());
      return Hashing.combineOrdered(hashes);
    }
    return hc;
  }
  
  @Override
  public int compareTo(final ExecutionGraphNode o) {
    return ComparisonChain.start()
        .compare(executorid, o.executorid, Ordering.natural().nullsFirst())
        .compare(data_type, o.data_type, Ordering.natural().nullsFirst())
        .compare(upstream, o.upstream, Ordering.natural().nullsFirst())
        .compare(executor_type, o.executor_type, Ordering.natural().nullsFirst())
        .compare(default_config, o.default_config, 
            Ordering.natural().nullsFirst())
        .result();
  }
  
  @Override
  public String toString() {
    return new StringBuilder()
        .append("id=")
        .append(executorid)
        .append(", dataType=")
        .append(data_type)
        .append(", upstream=")
        .append(upstream)
        .append(", executor=")
        .append(executor_type)
        .append(", defaultConfig=[")
        .append(default_config)
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
        .setExecutorId(node.executorid)
        .setDataType(node.data_type)
        .setUpstream(node.upstream)
        .setExecutorType(node.executor_type)
        .setDefaultConfig(node.default_config); // TODO ! may need copy
  }
  
  /** Builder class for the graph node. */
  public static class Builder {
    @JsonProperty
    private String executorId;
    @JsonProperty
    private String executorType;
    @JsonProperty
    private String dataType;
    @JsonProperty
    private String upstream;
    @JsonProperty
    private QueryExecutorConfig defaultConfig;
    
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
     * @param dataType The type of data returned by the executor.
     * @return The builder.
     */
    public Builder setDataType(final String dataType) {
      this.dataType = dataType;
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
     * @param defaultConfig An optional default config for the executor.
     * @return The builder.
     */
    public Builder setDefaultConfig(final QueryExecutorConfig defaultConfig) {
      this.defaultConfig = defaultConfig;
      return this;
    }
    
    /**
     * @param defaultConfig An optional default config for the executor.
     * @return The builder.
     */
    @JsonIgnore
    public Builder setDefaultConfig(
        final QueryExecutorConfig.Builder defaultConfig) {
      this.defaultConfig = defaultConfig.build();
      return this;
    }
    
    /** @return A graph node on success or an exception if required parameters 
     * were missing. */
    public ExecutionGraphNode build() {
      return new ExecutionGraphNode(this);
    }
  }
}
