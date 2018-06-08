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

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.util.ClassUtil;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.Const;
import net.opentsdb.core.DefaultTSDB;
import net.opentsdb.core.TSDB;
import net.opentsdb.query.context.QueryContext;
import net.opentsdb.query.execution.MultiClusterQueryExecutor;
import net.opentsdb.query.execution.QueryExecutor;
import net.opentsdb.query.execution.cluster.ClusterConfigPlugin.Config;
import net.opentsdb.query.execution.graph.ExecutionGraph;
import net.opentsdb.utils.Deferreds;
import net.opentsdb.utils.PluginLoader;

/**
 * A class defining a multi-cluster executor configuration. It consists of:
 * <ul>
 * <li>A {@link ClusterConfigPlugin} Config describing one or more clusters
 * to execute queries against and a plugin implementation used to parse and
 * process that config, loaded during the  {@link #initialize(DefaultTSDB)} phase.</li>
 * <li>A {@link ExecutionGraph} configuration that will instantiate a graph
 * of one or more {@link QueryExecutor}s for use in queries.</li>
 * </ul>
 * <p>
 * The config must contain a unique ID and can be registered with the TSD to be
 * used with a {@link MultiClusterQueryExecutor}.
 * 
 * @since 3.0
 */
@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonDeserialize(builder = ClusterConfig.Builder.class)
public class ClusterConfig implements Comparable<ClusterConfig> {
  /** The unique ID for this config. */
  protected String id;
  
  /** The graph used by executors calling this config. */
  protected ExecutionGraph graph;
  
  /** The source config for the cluster. */
  protected Config config;
  
  /** The plugin implementation. */
  protected ClusterConfigPlugin implementation;
  
  /** A map of executors by name, maintained for propre shutdown.*/
  protected Map<String, ExecutionGraph> executors;
  
  /**
   * Default ctor.
   * @param builder A non-null builder.
   * @throws IllegalArgumentException if the ID was null or empty, if the
   * cluster config was null or the execution graph config was null.
   */
  protected ClusterConfig(final Builder builder) {
    if (Strings.isNullOrEmpty(builder.id)) {
      throw new IllegalArgumentException("ID cannot be null.");
    }
    if (builder.config == null) {
      throw new IllegalArgumentException("Cluster config cannot be null.");
    }
    if (builder.executionGraph == null) {
      throw new IllegalArgumentException("Execution graph cannot be null.");
    }
    id = builder.id;
    graph = builder.executionGraph;
    config = builder.config;
  }
  
  /**
   * Initializes the cluster config, loading the implementation and initializing
   * the executor graph. Make sure to check the deferred for exceptions in case
   * the implementation couldn't be found or something went wrong with the
   * graphs.
   * @param tsdb A non-null TSDB to use for loading.
   * @return A deferred to wait on for initialization to complete. Resolves to
   * null on success or an exception on failure.
   */
  public Deferred<Object> initialize(final TSDB tsdb) {
    final String implementation_name;
    if (config.implementation().contains(".") || 
        config.implementation().endsWith("$Config")) {
      if (config.implementation().endsWith("$Config")) {
        String temp = config.implementation().substring(0, 
            config.implementation().length() - 7);
        if (temp.contains(".")) {
          implementation_name = temp;
        } else {
          implementation_name = ClusterConfigResolver.PACKAGE + "." + temp;
        }
      } else {
        implementation_name = config.implementation();
      }
    } else {
      implementation_name = ClusterConfigResolver.PACKAGE + "." 
          + config.implementation();
    }
    
    Exception exception = null;
    try {
      //final Class<?> clazz = ClassUtil.findClass(implementation_name);
      // ^^ disappeared in later Jackson versions :(
      final Class<?> clazz = Class.forName(implementation_name);
      implementation = (ClusterConfigPlugin) clazz.newInstance();
    } catch (Exception e) {
      exception = e;
    }
    
    // nothing on the class path, try finding a plugin.
    if (exception != null) {
      try {
        implementation = PluginLoader.loadSpecificPlugin(implementation_name, 
            ClusterConfigPlugin.class);
      } catch (Exception e) {
        return Deferred.fromError(
            new ClassNotFoundException("Unable to locate implementation: " 
            + implementation_name, e));
      }
    }
    if (implementation == null) {
      return Deferred.fromError(
          new ClassNotFoundException("Unable to locate implementation: " 
          + implementation_name, exception));
    }
    
    // the response.
    final Deferred<Object> deferred = new Deferred<Object>();
    
    /** Exception handler. */
    class ErrCB implements Callback<Object, Exception> {
      @Override
      public Object call(final Exception ex) throws Exception {
        deferred.callback(ex);
        return null;
      }
    }
    
    /** Final callback on success. */
    class GraphInitCB implements Callback<Object, Object> {
      @Override
      public Object call(final Object arg) throws Exception {
        deferred.callback(null);
        return null;
      }
    }
    
    /** Initial callback that clones the execution graphs for each cluster
     * and calls their init methods. */
    class ConfigInitCB implements Callback<Object, Object> {
      @Override
      public Object call(final Object arg) throws Exception {
        try {
          executors = Maps.newHashMapWithExpectedSize(
              implementation.clusters().size());
  
          final List<Deferred<Object>> deferreds = 
              Lists.newArrayListWithExpectedSize(implementation.clusters().size());
          for (final ClusterDescriptor cluster : 
            implementation.clusters().values()) {
            final ExecutionGraph graph_copy = 
                ExecutionGraph.newBuilder(graph).build();
            //deferreds.add(graph_copy.initialize(tsdb, cluster.getCluster()));
            executors.put(cluster.getCluster(), graph_copy);
          }
          
          Deferred.group(deferreds)
            .addCallback(Deferreds.NULL_GROUP_CB)
            .addCallback(new GraphInitCB())
            .addErrback(new ErrCB());
        } catch (Exception e) {
          deferred.callback(e);
        }
        return null;
      }
    }
    
    try {
      implementation.setConfig(config);
      implementation.initialize(tsdb)
        .addCallback(new ConfigInitCB())
        .addErrback(new ErrCB());
      return deferred;
    } catch (Exception e) {
      return Deferred.fromError(e);
    }
  }

  /** @return The ID of this cluster config. */
  public String getId() {
    return id;
  }
  
  /** @return The cluster config. */
  public Config getConfig() {
    return config;
  }
  
  /** @return The base execution graph configuration. (not the cloned and 
   * instantiated graphs) */
  public ExecutionGraph getExecutionGraph() {
    return graph;
  }
  
  /** @return The base list of clusters available via this config. */
  public Map<String, ClusterDescriptor> clusters() {
    if (implementation == null) {
      throw new IllegalStateException("Cluster config has not been "
          + "initialized: " + this);
    }
    return implementation.clusters();
  }
  
  /**
   * Returns the sink executor for the given cluster ID.
   * @param cluster_id A non-null and non-empty cluster ID.
   * @return An instantiated executor to send queries to.
   * @throws IllegalArgumentException if the cluster ID was null, empty or did
   * not exist in the cluster configuration.
   * @throws IllegalStateException if this method was called before 
   * {@link #initialize(DefaultTSDB)}.
   */
  public QueryExecutor<?> getSinkExecutor(final String cluster_id) {
    if (Strings.isNullOrEmpty(cluster_id)) {
      throw new IllegalArgumentException("Cluster ID cannot be null or empty.");
    }
    if (implementation == null) {
      throw new IllegalStateException("Cluster config has not been "
          + "initialized: " + this);
    }
    final ExecutionGraph cluster_graph = executors.get(cluster_id);
    if (cluster_graph == null) {
      throw new IllegalArgumentException("No such cluster: " + cluster_id);
    }
    
    final QueryExecutor<?> executor = null;//cluster_graph.sinkExecutor();
    if (executor == null) {
      throw new IllegalStateException("Graph: " + cluster_graph 
          + " returned a null executor.");
    }
    return executor;
  }
  
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
   * @throws IllegalStateException if this method was called before 
   * {@link #initialize(DefaultTSDB)}.
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
   * @throws IllegalStateException if this method was called before 
   * {@link #initialize(DefaultTSDB)}.
   */
  public List<String> setupQuery(final QueryContext context, 
                                 final String override) {
    if (implementation == null) {
      throw new IllegalStateException("Cluster config has not been "
          + "initialized: " + this);
    }
    return implementation.setupQuery(context, override);
  }
  
  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    
    final ClusterConfig config = (ClusterConfig) o;
    
    return Objects.equal(id, config.id)
        && Objects.equal(this.config, config.config)
        && Objects.equal(graph, config.graph);
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
    final List<HashCode> hashes = Lists.newArrayListWithCapacity(3);
    hashes.add(hc);
    hashes.add(config.buildHashCode());
    hashes.add(graph.buildHashCode());
    return Hashing.combineOrdered(hashes);
  }
  
  @Override
  public int compareTo(final ClusterConfig o) {
    return ComparisonChain.start()
        .compare(id, o.id, Ordering.natural().nullsFirst())
        .compare(config, o.config, 
            Ordering.natural().nullsFirst())
        .compare(graph, o.graph, Ordering.natural().nullsFirst())
        .result();
  }
  
  @Override
  public String toString() {
    return new StringBuilder()
        .append("id=")
        .append(id)
        .append(", clusterConfig=")
        .append(config)
        .append(", graph=")
        .append(graph)
        .toString();
  }
  
  @VisibleForTesting
  ClusterConfigPlugin implementation() {
    return implementation;
  }
  
  /** @return A new builder to construct a Cluster Config */
  public static Builder newBuilder() {
    return new Builder();
  }
  
  /** The builder class for cluster configs. */
  public static class Builder {
    @JsonProperty
    private String id;
    @JsonProperty
    private Config config;
    @JsonProperty
    private ExecutionGraph executionGraph;
    
    /**
     * @param id A non-null and non-empty ID for the cluster config.
     * @return The builder.
     */
    public Builder setId(final String id) {
      this.id = id;
      return this;
    }
    
    /**
     * @param config A non-null cluster config to parse.
     * @return The builder.
     */
    public Builder setConfig(
        final ClusterConfigPlugin.Config config) {
      this.config = config;
      return this;
    }
    
    /**
     * @param config A non-null cluster config builder to parse.
     * @return The builder.
     */
    @JsonIgnore
    public Builder setConfig(
        final ClusterConfigPlugin.Config.Builder config) {
      this.config = config.build();
      return this;
    }
    
    /**
     * @param executionGraph A non-null execution graph to use for the clusters.
     * @return The builder.
     */
    public Builder setExecutionGraph(final ExecutionGraph executionGraph) {
      this.executionGraph = executionGraph;
      return this;
    }

    /**
     * @param executionGraph A non-null execution graph builder to use for 
     * the clusters.
     * @return The builder.
     */
    public Builder setExecutionGraph(
        final ExecutionGraph.Builder executionGraph) {
      this.executionGraph = executionGraph.build();
      return this;
    }
    
    /** @return The instantiated ClusterConfig on success or exceptions on 
     * failure. */
    public ClusterConfig build() {
      return new ClusterConfig(this);
    }
  }
}
