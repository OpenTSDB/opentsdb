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
package net.opentsdb.query.execution.cluster;

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

import net.opentsdb.core.Const;
import net.opentsdb.query.execution.QueryExecutorConfig;

/**
 * A cluster descriptor consisting of an ID for the cluster, an optional user
 * readable description and one or more executor configs that override executor
 * behavior so that the cluster will be queried when it's used in the execution
 * path.
 * <p>
 * Note that executor configs may be empty if the the descriptor is used as an
 * override.
 * 
 * @since 3.0
 */
@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonDeserialize(builder = ClusterDescriptor.Builder.class)
public class ClusterDescriptor implements Comparable<ClusterDescriptor> {
  /** The non-null and unique cluster ID. */
  protected String cluster;
  
  /** An optiona user readable description of the cluster. */
  protected String description;
  
  /** A list of one or more executor configs that will cause the graph to query
   * the cluster. */
  protected List<QueryExecutorConfig> executor_configs;
  
  /**
   * Protected ctor.
   * @param builder A non-null builder.
   * @throws IllegalArgumentException if the cluster was null or empty.
   */
  protected ClusterDescriptor(final Builder builder){
    if (Strings.isNullOrEmpty(builder.cluster)) {
      throw new IllegalArgumentException("Cluster cannot be null or empty.");
    }
    cluster = builder.cluster;
    description = builder.description;
    executor_configs = builder.executorConfigs;
  }
  
  /** @return The cluster ID */
  public String getCluster() {
    return cluster;
  }
  
  /** @return The description of this cluster. */
  public String getDescription() {
    return description;
  }
  
  /** @return The list of executor configurations. */
  public List<QueryExecutorConfig> getExecutorConfigs() {
    if (executor_configs == null) {
      return Collections.emptyList();
    }
    return Collections.unmodifiableList(executor_configs);
  }
  
  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    final ClusterDescriptor descriptor = (ClusterDescriptor) o;
    return Objects.equals(cluster, descriptor.cluster)
        && Objects.equals(description, descriptor.description)
        && Objects.equals(executor_configs, descriptor.executor_configs);
  }
  
  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }
  
  /** @return A HashCode object for deterministic, non-secure hashing */
  public HashCode buildHashCode() {
    final HashCode hc = Const.HASH_FUNCTION().newHasher()
        .putString(Strings.nullToEmpty(cluster), Const.UTF8_CHARSET)
        .putString(Strings.nullToEmpty(description), Const.UTF8_CHARSET)
        .hash();
    if (executor_configs != null) {
      final List<HashCode> hashes = Lists.newArrayListWithCapacity(
          executor_configs.size() + 1);
      hashes.add(hc);
      for (final QueryExecutorConfig config : executor_configs) {
        hashes.add(config.buildHashCode());
      }
      return Hashing.combineOrdered(hashes);
    }
    return hc;
  }
  
  @Override
  public int compareTo(final ClusterDescriptor o) {
    return ComparisonChain.start()
        .compare(cluster, o.cluster, Ordering.natural().nullsFirst())
        .compare(description, o.description, Ordering.natural().nullsFirst())
        .compare(executor_configs, o.executor_configs, 
            Ordering.<QueryExecutorConfig>natural().lexicographical().nullsFirst())
        .result();
  }
  
  @Override
  public String toString() {
    return new StringBuilder()
        .append("cluster=")
        .append(cluster)
        .append(", description=")
        .append(description)
        .append(", executorConfigs=")
        .append(executor_configs)
        .toString();
  }
  
  /** @return A builder to construct a descriptor. */
  public static Builder newBuilder() {
    return new Builder();
  }
  
  /**
   * Creates a clone of the descriptor.
   * @param descriptor A non-null descriptor to clone.
   * @return A clone of the descriptor.
   */
  public static Builder newBuilder(final ClusterDescriptor descriptor) {
    final Builder builder = new Builder()
        .setCluster(descriptor.cluster)
        .setDescription(descriptor.description);
    if (descriptor.executor_configs != null) {
      for (final QueryExecutorConfig config : descriptor.executor_configs) {
        // TODO - may need to clone
        builder.addExecutorConfig(config);
      }
    }
    return builder;
  }
  
  /** Builder used to instantiate a ClusterDescriptor. */
  public static class Builder {
    @JsonProperty
    private String cluster;
    @JsonProperty
    private String description;
    @JsonProperty
    private List<QueryExecutorConfig> executorConfigs;
    
    /**
     * @param cluster A non-null and non-empty cluster ID.
     * @return The builder.
     */
    public Builder setCluster(final String cluster) {
      this.cluster = cluster;
      return this;
    }
    
    /**
     * @param description A user readable description of the cluster.
     * @return The builder.
     */
    public Builder setDescription(final String description) {
      this.description = description;
      return this;
    }
    
    /**
     * @param executorConfigs A non-null and non-empty set of executor configs.
     * @return The builder.
     */
    public Builder setExecutorConfigs(
        final List<QueryExecutorConfig> executorConfigs) {
      this.executorConfigs = executorConfigs;
      if (this.executorConfigs != null) {
        Collections.sort(this.executorConfigs);
      }
      return this;
    }
    
    /**
     * @param config A non-null query executor config.
     * @return The builder.
     */
    public Builder addExecutorConfig(final QueryExecutorConfig config) {
      if (executorConfigs == null) {
        executorConfigs = Lists.newArrayList(config);
      } else {
        executorConfigs.add(config);
        Collections.sort(executorConfigs);
      }
      return this;
    }
    
    /**
     * @param config A non-null query executor config builder.
     * @return The builder.
     */
    public Builder addExecutorConfig(final QueryExecutorConfig.Builder config) {
      if (executorConfigs == null) {
        executorConfigs = Lists.newArrayList(config.build());
      } else {
        executorConfigs.add(config.build());
        Collections.sort(executorConfigs);
      }
      return this;
    }
    
    /** @return An instantiated ClusterDescriptor or exceptions if required 
     * params were missing. */
    public ClusterDescriptor build() {
      return new ClusterDescriptor(this);
    }
  }
}
