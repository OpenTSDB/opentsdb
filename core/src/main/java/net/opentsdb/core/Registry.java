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
package net.opentsdb.core;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.stumbleupon.async.Deferred;

import net.opentsdb.data.DataMerger;
import net.opentsdb.data.DataShardMerger;
import net.opentsdb.data.types.numeric.NumericMergeLargest;
import net.opentsdb.query.execution.QueryExecutorFactory;
import net.opentsdb.query.execution.cluster.ClusterConfig;
import net.opentsdb.query.execution.graph.ExecutionGraph;
import net.opentsdb.stats.TsdbTracer;

/**
 * A shared location for registering context, mergers, plugins, etc.
 *
 * @since 3.0
 */
public class Registry {
  private static final Logger LOG = LoggerFactory.getLogger(Registry.class);
  
  /** The TSDB to which this registry belongs. Used for reading the config. */
  private final TSDB tsdb;
  
  /** The map of data mergers. */
  private final Map<String, DataMerger<?>> data_mergers;
  
  /** The map of available executor graphs for query execution. */
  private final Map<String, ExecutionGraph> executor_graphs;
  
  /** The map of executor factories for use in constructing graphs. */
  private final Map<String, QueryExecutorFactory<?>> factories;
  
  /** The map of cluster configurations for multi-cluster queries. */
  private final Map<String, ClusterConfig> clusters;
  
  /** The thread pool used for cleanup post query or other operations. */
  private final ExecutorService cleanup_pool;
  
  /** The loaded tracer plugin or null if disabled. */
  private TsdbTracer tracer_plugin;
  
  /**
   * Default Ctor. Sets up containers and initializes a cleanup pool but that's
   * all for now.
   * @param tsdb A non-null TSDB to load and pass to plugins.
   */
  public Registry(final TSDB tsdb) {
    if (tsdb == null) {
      throw new IllegalArgumentException("TSDB cannot be null.");
    }
    this.tsdb = tsdb;
    data_mergers = 
        Maps.<String, DataMerger<?>>newHashMapWithExpectedSize(1);
    executor_graphs = 
        Maps.<String, ExecutionGraph>newHashMapWithExpectedSize(1);
    factories = Maps.newHashMapWithExpectedSize(1);
    clusters = Maps.newHashMapWithExpectedSize(1);
    cleanup_pool = Executors.newFixedThreadPool(1);
  }
  
  /**
   * Initializes plugins and registry types.
   * @return A non-null deferred to wait on for initialization to complete.
   */
  public Deferred<Object> initialize() {
    initDataMergers();
    if (tracer_plugin != null) {
      return tracer_plugin.initialize(tsdb);
    }
    return Deferred.fromResult(null);
  }
  
  /** @return The cleanup thread pool for post-query or other tasks. */
  public ExecutorService cleanupPool() {
    return cleanup_pool;
  }
  
  /**
   * Adds the executor graph to the registry.
   * <b>WARNING:</b> Not thread safe.
   * @param graph A non-null execution graph that has been initialized.
   * @param is_default Whether or not the graph should be used as the default
   * for queries.
   * @throws IllegalArgumentException if the graph was null, it's ID was null or
   * empty, or the graph was already present.
   */
  public void registerExecutionGraph(final ExecutionGraph graph,
                                     final boolean is_default) {
    if (graph == null) {
      throw new IllegalArgumentException("Execution graph cannot be null.");
    }
    if (Strings.isNullOrEmpty(graph.getId())) {
      throw new IllegalArgumentException("Execution graph returned a "
          + "null or empty ID");
    }
    if (is_default) {
      if (executor_graphs.get(null) != null) {
        throw new IllegalArgumentException("Graph already exists for default: " 
            + executor_graphs.get(null).getId());
      }
      executor_graphs.put(null, graph);
      LOG.info("Registered default execution graph: " + graph.getId());
    }
    if (executor_graphs.get(graph.getId()) != null) {
      throw new IllegalArgumentException("Graph already exists for ID: " 
          + graph.getId());
    }
    executor_graphs.put(graph.getId(), graph);
    LOG.info("Registered execution graph: " + graph.getId());
  }
  
  /**
   * Fetches the default graph. May be null if no graphs have been set.
   * @return An execution graph or null if no graph was set.
   */
  public ExecutionGraph getDefaultExecutionGraph() {
    return getExecutionGraph(null);
  }
  
  /**
   * Fetches the execution graph if it exists.
   * @param id A non-null and non-empty ID.
   * @return The graph if present.
   */
  public ExecutionGraph getExecutionGraph(final String id) {
    if (Strings.isNullOrEmpty(id)) {
      return executor_graphs.get(null);
    }
    return executor_graphs.get(id);
  }
  
  /**
   * Adds the given factory to the registry.
   * <b>WARNING:</b> Not thread safe.
   * @param factory A non-null factory to add.
   * @throws IllegalArgumentException if the factory was null, it's ID was null
   * or empty, or the factory already exists.
   */
  public void registerFactory(final QueryExecutorFactory<?> factory) {
    if (factory == null) {
      throw new IllegalArgumentException("Factory cannot be null.");
    }
    if (Strings.isNullOrEmpty(factory.id())) {
      throw new IllegalArgumentException("Factory ID was null or empty.");
    }
    if (factories.containsKey(factory.id())) {
      throw new IllegalArgumentException("Factory already registered: " 
          + factory.id());
    }
    factories.put(factory.id(), factory);
    LOG.info("Registered factory: " + factory.id());
  }
  
  /**
   * Returns the factory for the given ID if present.
   * @param id A non-null and non-empty ID.
   * @return The factory if present.
   */
  public QueryExecutorFactory<?> getFactory(final String id) {
    if (Strings.isNullOrEmpty(id)) {
      throw new IllegalArgumentException("ID was null or empty.");
    }
    return factories.get(id);
  }
  
  /**
   * Adds the given cluster config to the registry.
   * <b>WARNING:</b> Not thread safe.
   * @param cluster A non-null cluster config to add.
   * @throws IllegalArgumentException if the cluster was null, it's ID was null
   * or empty, or the cluster already exists.
   */
  public void registerClusterConfig(final ClusterConfig cluster) {
    if (cluster == null){
      throw new IllegalArgumentException("Cluster cannot be null.");
    }
    if (Strings.isNullOrEmpty(cluster.getId())){
      throw new IllegalArgumentException("Cluster ID cannot be null or empty.");
    }
    if (clusters.containsKey(cluster.getId())) {
      throw new IllegalArgumentException("Cluster already registered.");
    }
    clusters.put(cluster.getId(), cluster);
    LOG.info("Registered cluster: " + cluster.getId());
  }
  
  /**
   * Returns the cluster config for the given ID if present.
   * @param cluster A non-null and non-empty cluster ID.
   * @return The cluster config if present.
   */
  public ClusterConfig getClusterConfig(final String cluster) {
    if (Strings.isNullOrEmpty(cluster)) {
      throw new IllegalArgumentException("ID was null or empty.");
    }
    return clusters.get(cluster);
  }
  
  /**
   * Add the tracer implementation. Note that it must already be initialized.
   * @param tracer The tracer to pass to operations. May be null.
   */
  public void registerTracer(final TsdbTracer tracer) {
    this.tracer_plugin = tracer;
  }
  
  /** @return The tracer for use with operaitons. May be null. */
  public TsdbTracer tracer() {
    return tracer_plugin;
  }
  
  public DataMerger<?> getDataMerger(final String merger) {
    return data_mergers.get(merger);
  }
  
  /** @return Package private shutdown returning the deferred to wait on. */
  Deferred<Object> shutdown() {
    cleanup_pool.shutdown();
    return Deferred.fromResult(null);
  }
  
  private void initDataMergers() {
    final DataShardMerger shards_merger = new DataShardMerger();
    shards_merger.registerStrategy(new NumericMergeLargest());
    data_mergers.put(null, shards_merger);
    data_mergers.put("default", shards_merger);
    data_mergers.put("largest", shards_merger);
  }
  
}
