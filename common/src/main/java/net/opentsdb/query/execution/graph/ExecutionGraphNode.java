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

import java.util.Collections;
import java.util.List;
import java.util.Objects;

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

import net.opentsdb.common.Const;
import net.opentsdb.query.QueryNodeConfig;

/**
 * A serializable node configuration for use in building DAG of a query.
 * <p>
 * The only required value is {@link Builder#setId(String)}. This
 * must be a unique ID within the query graph. Additionally, if 
 * {@link Builder#setType(String)} was not set to a value, then
 * the {@link #getId()} must be the name of a {@link QueryNode} 
 * factory. Each {@link #getType()} must be unique in the list.
 * <p>
 * To construct the graph, link nodes by setting the 
 * {@link Builder#setSources(List)} to one or more nodes. Make sure not
 * to have any cycles in the graph.
 * 
 * @since 3.0
 */
@JsonInclude(Include.NON_NULL)
@JsonDeserialize(builder = ExecutionGraphNode.Builder.class)
public class ExecutionGraphNode implements Comparable<ExecutionGraphNode> {
  
  /** The unique ID of this node within the graph. */
  private String id;
  
  /** The class of an {@link QueryNode} implementation. */
  private String type;
  
  /** An optional list of downstream sources. */
  private List<String> sources;
  
  /** An optional configuration override. */
  private QueryNodeConfig config;
  
  /**
   * Protected ctor to build from the builder.
   * @param builder A non-null builder.
   * @throws IllegalArgumentException if the id or executor types were null or
   * empty.
   */
  protected ExecutionGraphNode(final Builder builder) {
    if (Strings.isNullOrEmpty(builder.id)) {
      throw new IllegalArgumentException("ID cannot be null or empty");
    }
    if (Strings.isNullOrEmpty(builder.type)) {
      type = builder.id;
    } else {
      type = builder.type;
    }
    id = builder.id;
    sources = builder.sources;
    config = builder.config;
  }
  
  /** @return The id of this node. */
  public String getId() {
    return id;
  }
  
  /** @return The class of the implementation. */
  public String getType() {
    return type;
  }
    
  /** @return An optional lost of sources mapping to node IDs. */
  public List<String> getSources() {
    return sources;
  }
  
  /** @return An optional node config override. */
  public QueryNodeConfig getConfig() {
    return config;
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
    return Objects.equals(id, node.id)
        && Objects.equals(sources, node.sources)
        && Objects.equals(type, node.type)
        && Objects.equals(config, node.config);
  }
  
  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }
  
  /** @return A HashCode object for deterministic, non-secure hashing */
  public HashCode buildHashCode() {
    final HashCode hc = Const.HASH_FUNCTION().newHasher()
        .putString(Strings.nullToEmpty(id), Const.UTF8_CHARSET)
        .putString(Strings.nullToEmpty(type), Const.UTF8_CHARSET)
        .hash();
    if (sources != null && !sources.isEmpty()) {
      final List<HashCode> hashes = 
          Lists.newArrayListWithCapacity(sources.size() + 
              (config != null ? 2 : 1));
      hashes.add(hc);
      Collections.sort(sources);
      for (final String source : sources) {
        hashes.add(Const.HASH_FUNCTION().newHasher().putString(
            source, Const.UTF8_CHARSET).hash());
      }
      if (config != null) {
        hashes.add(config.buildHashCode());
      }
      return Hashing.combineOrdered(hashes);
    }
    return hc;
  }
  
  @Override
  public int compareTo(final ExecutionGraphNode o) {
    return ComparisonChain.start()
        .compare(id, o.id, Ordering.natural().nullsFirst())
        .compare(sources, o.sources, Ordering.<String>natural()
            .lexicographical().nullsFirst())
        .compare(type, o.type, Ordering.natural().nullsFirst())
        .compare(config, o.config, Ordering.natural().nullsFirst())
        .result();
  }
  
  @Override
  public String toString() {
    return new StringBuilder()
        .append("id=")
        .append(id)
        .append(", sources=")
        .append(sources)
        .append(", type=")
        .append(type)
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
        .setId(node.id)
        .setSources(node.sources != null ? 
            Lists.newArrayList(node.sources) : null)
        .setType(node.type);
  }
  
  /** Builder class for the graph node. */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Builder {
    @JsonProperty
    private String id;
    @JsonProperty
    private String type;
    @JsonProperty
    private List<String> sources;
    @JsonProperty
    private QueryNodeConfig config;
    
    /**
     * @param id A non-null and non-empty ID for the node.
     * @return The builder.
     */
    public Builder setId(final String id) {
      this.id = id;
      return this;
    }
    
    /**
     * @param type The class of the implementation.
     * @return The builder.
     */
    public Builder setType(final String type) {
      this.type = type;
      return this;
    }
    
    /**
     * @param sources An optional list of sources consisting of the IDs 
     * of a nodes in the graph.
     * @return The builder.
     */
    public Builder setSources(final List<String> sources) {
      this.sources = sources;
      return this;
    }
    
    /**
     * @param source A source to pull from for this node.
     * @return The builder.
     */
    public Builder addSource(final String source) {
      if (sources == null) {
        sources = Lists.newArrayListWithExpectedSize(1);
      }
      sources.add(source);
      return this;
    }
    
    /**
     * @param config An optional config for the node.
     * @return The builder.
     */
    public Builder setConfig(final QueryNodeConfig config) {
      this.config = config;
      return this;
    }
    
    /** @return A graph node on success or an exception if required parameters 
     * were missing. */
    public ExecutionGraphNode build() {
      return new ExecutionGraphNode(this);
    }
  }
}
