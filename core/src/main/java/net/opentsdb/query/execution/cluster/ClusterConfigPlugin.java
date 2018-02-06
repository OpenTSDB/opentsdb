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
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver;
import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;

import net.opentsdb.core.Const;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.query.context.QueryContext;

/**
 * The base class that's used for implementing various cluster configuration
 * and management methods. For example, this implementation could periodically
 * poll an asset system or load balancer to determine what configs should
 * be set on a query via {@link #setupQuery(QueryContext)} at any given time.
 *
 * TODO - more docs.
 *
 * @since 3.0
 */
public abstract class ClusterConfigPlugin extends BaseTSDBPlugin {
  /** The config for this plugin. */
  protected Config config;
  
  /**
   * After the config has been built or parsed, it's passed to the plugin.
   * It is guaranteed that this will be called *before* 
   * {@link #initialize(net.opentsdb.core.TSDB)} so the config should setup
   * access to external systems if needed but actual calls to those systems 
   * should be performed in an asynchronous manner via overriding 
   * {@link #initialize(net.opentsdb.core.TSDB)}.
   * 
   * @param config A non-null config for the implementation.
   */
  public void setConfig(final Config config) {
    this.config = config;
  }
  
  /**
   * The complete set of available clusters to choose from. Generally this should
   * be all clusters that will possibly be available during the TSD's run and is
   * used by executors to determine how many downstream graphs they need to 
   * setup on instantiation. At query time, the plugin can choose not use some
   * of the clusters.
   * <b>Warning:</b> Executors should *always* call this method before executing
   * a query to make certain no new clusters have been added.
   * 
   * @return A non-null map of cluster descriptors.
   */
  public abstract Map<String, ClusterDescriptor> clusters();

  /**
   * Called on every query to populate the {@link QueryContext} with executor
   * configs that will be used downstream to execute queries against the 
   * clusters that are currently in rotation from the plugin's standpoint.
   * 
   * @param context A non-null query context.
   * @return A non-null list of the cluster IDs participating in this query. If
   * the list is returned as null, the executor should return an exception 
   * upstream.
   * TODO Doc exception
   * @throws IllegalArgumentException if the context was null. 
   */
  public List<String> setupQuery(final QueryContext context) {
    return setupQuery(context, null);
  }
  
  /**
   * Called on every query to populate the {@link QueryContext} with executor
   * configs that will be used downstream to execute queries against the 
   * clusters that are currently in rotation from the plugin's standpoint.
   * 
   * @param context A non-null query context.
   * @param override An optional named configuration set override to use in
   * place of the default or current clusters.
   * @return A non-null list of the cluster IDs participating in this query. If
   * the list is returned as null, the executor should return an exception 
   * upstream.
   * TODO Doc exception
   * @throws IllegalArgumentException if the context was null. 
   */
  public abstract List<String> setupQuery(final QueryContext context, 
                                          final String override);
  
  /**
   * The base configuration class for a cluster plugin. All plugins must
   * include a class named "Config" for easy of use at deserialization time.
   * E.g. for ease of access, the following implementation:
   * {@code
   * package net.opentsdb.query.execution.cluster;
   * 
   * public class StaticClusterConfig extends ClusterConfigPlugin {
   *   
   *   public static class Config extends ClusterConfigPlugin.Config {
   *   
   *     public static class Builder extends ClusterConfigPlugin.Config.Builder {
   *     
   *     }
   *   }
   * }
   * }
   * allows admins and users to create a JSON config like:
   * {@code
   * {
   *   "implementation":"StaticClusterConfig",
   *   "clusters":[
   *     ...
   *   ]
   * }
   * }
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonTypeInfo(use = Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "implementation",
    visible = true)
  @JsonTypeIdResolver(ClusterConfigResolver.class)
  public static abstract class Config implements Comparable<Config> {
    /** A unique Id for the config. */
    protected final String id;
    
    /** The {@link ClusterConfigPlugin} implementation name used to deserialize
     * to the proper class. May be short or canonical. */
    protected final String implementation;
    
    /** The list of cluster descriptors that are available by default. */
    protected List<ClusterDescriptor> clusters;
    
    /** An optional list of named override cluster sets. */
    protected List<ClusterOverride> overrides;
    
    /**
     * Protected ctor.
     * @param builder A non-null builder to pull from.
     * @throws IllegalArgumentException if the id, implementation or clusters
     * were null or empty.
     */
    protected Config(final Builder builder) {
      if (Strings.isNullOrEmpty(builder.id)) {
        throw new IllegalArgumentException("ID cannot be null.");
      }
      if (Strings.isNullOrEmpty(builder.implementation)) {
        throw new IllegalArgumentException("Implementation cannot be null.");
      }
      if (builder.clusters == null || builder.clusters.isEmpty()) {
        throw new IllegalArgumentException("Clusters cannot be null or empty.");
      }
      id = builder.id;
      implementation = builder.implementation;
      clusters = builder.clusters;
      overrides = builder.overrides;
    }
    
    /** @return The id of this config. */
    public String getId() {
      return id;
    }
    
    /** @return The name of the cluster config plugin. */
    public String implementation() {
      return implementation;
    }
    
    /** @return The list of clusters. */
    public List<ClusterDescriptor> getClusters() {
      return clusters;
    }
    
    /** @return The list of overrides, may be null. */
    public List<ClusterOverride> getOverrides() {
      return overrides;
    }
    
    @Override
    public boolean equals(final Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;
      
      final Config config = (Config) o;
      
      return Objects.equal(id, config.id)
          && Objects.equal(implementation, config.implementation)
          && Objects.equal(clusters, config.clusters)
          && Objects.equal(overrides, config.overrides);
    }

    @Override
    public int hashCode() {
      return buildHashCode().asInt();
    }

    /** @return A HashCode object for deterministic, non-secure hashing */
    public HashCode buildHashCode() {
      final HashCode hc = Const.HASH_FUNCTION().newHasher()
          .putString(Strings.nullToEmpty(id), Const.UTF8_CHARSET)
          .putString(Strings.nullToEmpty(implementation), Const.UTF8_CHARSET)
          .hash();
      int size = clusters != null ? clusters.size() : 0;
      size += overrides != null ? overrides.size() : 0;
      ++size;
      if (size > 0) {
        final List<HashCode> hashes = Lists.newArrayListWithCapacity(size);
        hashes.add(hc);
        if (clusters != null) {
          for (final ClusterDescriptor cluster : clusters) {
            hashes.add(cluster.buildHashCode());
          }
        }
        if (overrides != null) {
          for (final ClusterOverride override : overrides) {
            hashes.add(override.buildHashCode());
          }
        }
        return Hashing.combineOrdered(hashes);
      }
      return hc;
    }

    @Override
    public int compareTo(final Config config) {
      if (!(config instanceof Config)) {
        return -1;
      }
      final Config local = (Config) config;
      return ComparisonChain.start()
          .compare(id, local.id, Ordering.natural().nullsFirst())
          .compare(implementation, local.implementation, 
              Ordering.natural().nullsFirst())
          .compare(clusters, local.clusters, 
              Ordering.<ClusterDescriptor>natural().lexicographical().nullsFirst())
          .compare(overrides, local.overrides, 
              Ordering.<ClusterOverride>natural().lexicographical().nullsFirst())
          .result();
    }
    
    @Override
    public String toString() {
      return new StringBuilder()
          .append("id=")
          .append(id)
          .append(", implementation=")
          .append(implementation)
          .append(", clusters=")
          .append(clusters)
          .append(", overrides=")
          .append(overrides)
          .toString();
    }
    
    /**
     * The base builder class for configuration plugins. Used for deserialization.
     */
    public static abstract class Builder {
      @JsonProperty
      protected String id;
      @JsonProperty
      protected String implementation;
      @JsonProperty
      protected List<ClusterDescriptor> clusters;
      @JsonProperty
      protected List<ClusterOverride> overrides;
      
      /**
       * @param id A non-null and non-empty ID for the config.
       * @return The builder.
       */
      public Builder setId(final String id) {
        this.id = id;
        return this;
      }
      
      /** 
       * @param implementation The {@link ClusterConfigPlugin} implementation 
       * name used to deserialize to the proper class. May be short or 
       * canonical. 
       * @return The builder.
       */
      public Builder setImplementation(final String implementation) {
        this.implementation = implementation;
        return this;
      }
     
      /**
       * @param clusters A non-null and non-empty list of cluster descriptors.
       * @return The builder.
       */
      public Builder setClusters(final List<ClusterDescriptor> clusters) {
        this.clusters = clusters;
        if (this.clusters != null) {
          Collections.sort(this.clusters);
        }
        return this;
      }
      
      /**
       * @param cluster A non-null cluster to add to the list.
       * @return The builder.
       */
      @JsonIgnore
      public Builder addCluster(final ClusterDescriptor cluster) {
        if (clusters == null) {
          clusters = Lists.newArrayList(cluster);
        } else {
          clusters.add(cluster);
          Collections.sort(clusters);
        }
        return this;
      }
      
      /**
       * @param cluster A non-null cluster builder to add to the list.
       * @return The builder.
       */
      @JsonIgnore
      public Builder addCluster(final ClusterDescriptor.Builder cluster) {
        if (clusters == null) {
          clusters = Lists.newArrayList(cluster.build());
        } else {
          clusters.add(cluster.build());
          Collections.sort(clusters);
        }
        return this;
      }
      
      /**
       * @param overrides An optional list of cluster overrides.
       * @return The builder.
       */
      public Builder setOverrides(final List<ClusterOverride> overrides) {
        this.overrides = overrides;
        if (this.overrides != null) {
          Collections.sort(this.overrides);
        }
        return this;
      }
      
      /**
       * @param override A non-null override to add to the list.
       * @return The builder.
       */
      @JsonIgnore
      public Builder addOverride(final ClusterOverride override) {
        if (overrides == null) {
          overrides = Lists.newArrayList(override);
        } else {
          overrides.add(override);
          Collections.sort(overrides);
        }
        return this;
      }
      
      /**
       * @param override A non-null override builder to add to the list.
       * @return The builder.
       */
      @JsonIgnore
      public Builder addOverride(final ClusterOverride.Builder override) {
        if (overrides == null) {
          overrides = Lists.newArrayList(override.build());
        } else {
          overrides.add(override.build());
          Collections.sort(overrides);
        }
        return this;
      }
      
      /** @return A Config object or exceptions if the config failed. */
      public abstract Config build();
    }
  }
}
