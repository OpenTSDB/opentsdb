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
package net.opentsdb.core;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.data.DataMerger;
import net.opentsdb.data.DataShardMerger;
import net.opentsdb.data.iterators.IteratorGroups;
import net.opentsdb.data.types.numeric.NumericMergeLargest;
import net.opentsdb.query.QueryIteratorFactory;
import net.opentsdb.query.QueryIteratorInterpolatorFactory;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.execution.CachingQueryExecutor;
import net.opentsdb.query.execution.DefaultQueryExecutorFactory;
import net.opentsdb.query.execution.MetricShardingExecutor;
import net.opentsdb.query.execution.MultiClusterQueryExecutor;
import net.opentsdb.query.execution.QueryExecutor;
import net.opentsdb.query.execution.QueryExecutorFactory;
import net.opentsdb.query.execution.TimeSlicedCachingExecutor;
import net.opentsdb.query.execution.cache.QueryCachePlugin;
import net.opentsdb.query.execution.cache.TimeSeriesCacheKeyGenerator;
import net.opentsdb.query.execution.cache.DefaultTimeSeriesCacheKeyGenerator;
import net.opentsdb.query.execution.cache.GuavaLRUCache;
import net.opentsdb.query.execution.cluster.ClusterConfig;
import net.opentsdb.query.execution.graph.ExecutionGraph;
import net.opentsdb.query.execution.graph.ExecutionGraphNode;
import net.opentsdb.query.execution.serdes.TimeSeriesSerdes;
import net.opentsdb.query.execution.serdes.UglyByteIteratorGroupsSerdes;
import net.opentsdb.query.plan.DefaultQueryPlannerFactory;
import net.opentsdb.query.plan.IteratorGroupsSlicePlanner;
import net.opentsdb.query.plan.QueryPlannnerFactory;
import net.opentsdb.query.plan.QueryPlanner;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.utils.Deferreds;
import net.opentsdb.utils.JSON;

/**
 * A shared location for registering context, mergers, plugins, etc.
 *
 * @since 3.0
 */
public class DefaultRegistry implements Registry {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultRegistry.class);
  
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
    
  /** The map of serdes classes. */
  private final Map<String, TimeSeriesSerdes> serdes;
  
  /** The map of query plans. */
  private final Map<String, QueryPlannnerFactory<?>> query_plans;
  
  private final Map<String, QueryNodeFactory> node_factories;
  
  /** A concurrent map of shared objects used by various plugins such as 
   * connection pools, etc. */
  private final Map<String, Object> shared_objects;
  
  /** The thread pool used for cleanup post query or other operations. */
  private final ExecutorService cleanup_pool;
  
  /** The plugins loaded by this TSD. */
  private PluginsConfig plugins;
  
  /**
   * Default Ctor. Sets up containers and initializes a cleanup pool but that's
   * all for now.
   * @param tsdb A non-null TSDB to load and pass to plugins.
   */
  public DefaultRegistry(final TSDB tsdb) {
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
    serdes = Maps.newHashMapWithExpectedSize(1);
    query_plans = Maps.newHashMapWithExpectedSize(1);
    node_factories = Maps.newHashMap();
    shared_objects = Maps.newConcurrentMap();
    cleanup_pool = Executors.newFixedThreadPool(1);
  }
  
  /**
   * Initializes the registry including loading the plugins if specified.
   * @param load_plugins Whether or not to load plugins.
   * @return A non-null deferred to wait on for initialization to complete.
   */
  public Deferred<Object> initialize(final boolean load_plugins) {
    if (!load_plugins) {
      return Deferred.fromResult(null);
    }
    
    class LoadedCB implements Callback<Deferred<Object>, Object> {
      @Override
      public Deferred<Object> call(final Object ignored) throws Exception {
        return initDefaults();
      }
    }
    
    return loadPlugins().addCallbackDeferring(new LoadedCB());
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
    if (is_default) {
      if (executor_graphs.get(null) != null) {
        throw new IllegalArgumentException("Graph already exists for default: " 
            + executor_graphs.get(null).getId());
      }
      if (executor_graphs.get(null) != null) {
        throw new IllegalArgumentException("Default execution graph "
            + "already exists.");
      }
      executor_graphs.put(null, graph);
      LOG.info("Registered default execution graph: " + graph);
    } else {
      if (Strings.isNullOrEmpty(graph.getId())) {
        throw new IllegalArgumentException("Execution graph returned a "
            + "null or empty ID");
      }
    }
    if (!Strings.isNullOrEmpty(graph.getId())) {
      if (executor_graphs.get(graph.getId()) != null) {
        throw new IllegalArgumentException("Graph already exists for ID: " 
            + graph.getId());
      }
      executor_graphs.put(graph.getId(), graph);
    }
    LOG.info("Registered execution graph: " + 
        (is_default ? "(default)" : graph.getId()));
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
  
  @Override
  public void registerFactory(final QueryNodeFactory factory) {
    node_factories.put(factory.id(), factory);
  }
  
  @Override
  public QueryNodeFactory getQueryNodeFactory(final String id) {
    QueryNodeFactory factory = node_factories.get(id);
    if (factory != null) {
      return factory;
    }
    final TSDBPlugin plugin = plugins.getPlugin(QueryNodeFactory.class, id);
    if (plugin == null) {
      return null;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Caching QueryNodeFactory " + plugin + " with ID: " + id);
    }
    factory = (QueryNodeFactory) plugin;
    node_factories.put(id, factory);
    return factory;
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
   * Registers a query plan factory.
   * @param factory A non-null factory to register.
   * @throws IllegalArgumentException if the factory was null, it's ID was null
   * or empty, or the factory already exists.
   */
  public void registerFactory(final QueryPlannnerFactory<?> factory) {
    if (factory == null) {
      throw new IllegalArgumentException("Factory cannot be null.");
    }
    if (Strings.isNullOrEmpty(factory.id())) {
      throw new IllegalArgumentException("Factory ID was null or empty.");
    }
    if (query_plans.containsKey(factory.id())) {
      throw new IllegalArgumentException("Factory already registered: " 
          + factory.id());
    }
    query_plans.put(factory.id(), factory);
    LOG.info("Registered query plan factory: " + factory.id());
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
   * Returns the factory for a given ID if present.
   * @param id A non-null and non-empty ID.
   * @return The factory if present.
   */
  public QueryPlannnerFactory<?> getQueryPlanner(final String id) {
    if (Strings.isNullOrEmpty(id)) {
      throw new IllegalArgumentException("ID was null or empty.");
    }
    return query_plans.get(id);
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
   * Registers the given plugin in the map. If a plugin with the ID is already
   * present, an exception is thrown.
   * @param clazz The type of plugin to be stored.
   * @param id An ID for the plugin (may be null if it's a default).
   * @param plugin A non-null and initialized plugin to register.
   * @throws IllegalArgumentException if the class or plugin was null or if
   * a plugin was already registered with the given ID. Also thrown if the
   * plugin given is not an instance of the class.
   */
  public void registerPlugin(final Class<?> clazz, 
                             final String id, 
                             final TSDBPlugin plugin) {
    if (plugins == null) {
      throw new IllegalStateException("Plugins have not been loaded. "
          + "Call loadPlugins();");
    }
    plugins.registerPlugin(clazz, id, plugin);
  }
  
  /**
   * Retrieves the default plugin of the given type (i.e. the ID was null when
   * registered).
   * @param clazz The type of plugin to be fetched.
   * @return An instantiated plugin if found, null if not.
   * @throws IllegalArgumentException if the clazz was null.
   */
  public TSDBPlugin getDefaultPlugin(final Class<?> clazz) {
    return getPlugin(clazz, null);
  }
  
  /**
   * Retrieves the plugin with the given class type and ID.
   * @param clazz The type of plugin to be fetched.
   * @param id An optional ID, may be null if the default is fetched.
   * @return An instantiated plugin if found, null if not.
   * @throws IllegalArgumentException if the clazz was null.
   */
  public TSDBPlugin getPlugin(final Class<?> clazz, final String id) {
    if (plugins == null) {
      throw new IllegalStateException("Plugins have not been loaded. "
          + "Call loadPlugins();");
    }
    return plugins.getPlugin(clazz, id);
  }
  
  /**
   * Registers a shared object in the concurrent map if the object was not
   * present. If an object was already present, the existing object is returned.
   * @param id A non-null and non-empty ID for the shared object.
   * @param obj A non-null object.
   * @return Null if the object was inserted successfully, a non-null object
   * if something with the given ID was already present.
   * @throws IllegalArgumentException if the ID was null or empty or the
   * object was null.
   */
  public Object registerSharedObject(final String id, final Object obj) {
    if (Strings.isNullOrEmpty(id)) {
      throw new IllegalArgumentException("ID cannot be null or empty.");
    }
    if (obj == null) {
      throw new IllegalArgumentException("Shared object may not be null.");
    }
    return shared_objects.putIfAbsent(id, obj);
  }
  
  /**
   * Returns the shared object for this Id if it exists.
   * @param id A non-null and non-empty ID.
   * @return The object if present, null if not.
   */
  public Object getSharedObject(final String id) {
    if (Strings.isNullOrEmpty(id)) {
      throw new IllegalArgumentException("ID cannot be null or empty.");
    }
    return shared_objects.get(id);
  }
  
  public DataMerger<?> getDataMerger(final String merger) {
    return data_mergers.get(merger);
  }
  
  public TimeSeriesSerdes getSerdes(final String id) {
    return serdes.get(id);
  }
  
  /**
   * Loads plugins using the 'tsd.plugin.config' property.
   * @return A deferred to wait on for results.
   */
  public Deferred<Object> loadPlugins() {
    final String config = tsdb.getConfig().getString("tsd.plugin.config");
    if (Strings.isNullOrEmpty(config)) {
      if (plugins == null) {
        LOG.info("No plugin config provided. Instantiating empty plugin config.");
        plugins = new PluginsConfig();
      }
    } else {
      if (config.toLowerCase().endsWith(".json")) {
        LOG.info("Loading plugin config from file: " + config);
        final String json;
        try {
          json = Files.toString(new File(config), Const.UTF8_CHARSET);
        } catch (IOException e) {
          throw new RuntimeException("Unable to open plugin configfile: " 
              + config, e);
        }
        plugins = JSON.parseToObject(json, PluginsConfig.class);
      } else {
        LOG.info("Loading plugin config from JSON");
        plugins = JSON.parseToObject(config, PluginsConfig.class);
      }
    }
    
    return plugins.initialize(tsdb);
  }
  
  /** @return Package private shutdown returning the deferred to wait on. */
  public Deferred<Object> shutdown() {
    cleanup_pool.shutdown();
    return Deferred.fromResult(null);
  }
  
  /** Sets up default objects in the registry. */
  @SuppressWarnings("unchecked")
  private Deferred<Object> initDefaults() {
    final DataShardMerger shards_merger = new DataShardMerger();
    shards_merger.registerStrategy(new NumericMergeLargest());
    data_mergers.put(null, shards_merger);
    data_mergers.put("default", shards_merger);
    data_mergers.put("largest", shards_merger);
    
    List<Deferred<Object>> deferreds = Lists.newArrayList();
    
    final GuavaLRUCache query_cache = new GuavaLRUCache();
    deferreds.add(query_cache.initialize(tsdb));
    
    registerPlugin(QueryCachePlugin.class, null, query_cache);
    registerPlugin(QueryCachePlugin.class, "GuavaLRUCache", query_cache);
    
    final UglyByteIteratorGroupsSerdes ugly = new UglyByteIteratorGroupsSerdes();
    serdes.put(null, ugly);
    serdes.put("UglyByteSerdes", ugly);
    
    final TimeSeriesCacheKeyGenerator key_gen = 
        new DefaultTimeSeriesCacheKeyGenerator();
    deferreds.add(key_gen.initialize(tsdb));
    registerPlugin(TimeSeriesCacheKeyGenerator.class, null, key_gen);
    
    try {
      Constructor<?> ctor = IteratorGroupsSlicePlanner.class
          .getDeclaredConstructor(TimeSeriesQuery.class);
      final QueryPlannnerFactory<?> planner_factory = 
          new DefaultQueryPlannerFactory<IteratorGroups>(
              (Constructor<QueryPlanner<?>>) ctor,
              IteratorGroups.class,
              "IteratorGroupsSlicePlanner");
      registerFactory(planner_factory);
      
      // TODO - this can be done with reflection programmatically.
      ctor = CachingQueryExecutor.class.getConstructor(ExecutionGraphNode.class);
      QueryExecutorFactory<IteratorGroups> executor_factory =
          new DefaultQueryExecutorFactory<IteratorGroups>(
              (Constructor<QueryExecutor<?>>) ctor, IteratorGroups.class, 
              "CachingQueryExecutor");
      registerFactory(executor_factory);
      
      ctor = TimeSlicedCachingExecutor.class.getConstructor(ExecutionGraphNode.class);
      executor_factory = 
          new DefaultQueryExecutorFactory<IteratorGroups>(
              (Constructor<QueryExecutor<?>>) ctor, IteratorGroups.class, 
              "TimeSlicedCachingExecutor");
      registerFactory(executor_factory);
      
      ctor = MetricShardingExecutor.class.getConstructor(ExecutionGraphNode.class);
      executor_factory = 
          new DefaultQueryExecutorFactory<IteratorGroups>(
              (Constructor<QueryExecutor<?>>) ctor, IteratorGroups.class, 
              "MetricShardingExecutor");
      registerFactory(executor_factory);

//      ctor = StorageQueryExecutor.class.getConstructor(
//              ExecutionGraphNode.class);
//      executor_factory = 
//          new DefaultQueryExecutorFactory<IteratorGroups>(
//              (Constructor<QueryExecutor<?>>) ctor, IteratorGroups.class,
//                "StorageQueryExecutor");
//      tsdb.getRegistry().registerFactory(executor_factory);
      
      ctor = MultiClusterQueryExecutor.class.getConstructor(
          ExecutionGraphNode.class);
  executor_factory = 
      new DefaultQueryExecutorFactory<IteratorGroups>(
          (Constructor<QueryExecutor<?>>) ctor, IteratorGroups.class,
            "MultiClusterQueryExecutor");
  registerFactory(executor_factory);
      
    } catch (Exception e) {
      LOG.error("Failed setting up one or more default executors or planners", e);
      return Deferred.fromError(new RuntimeException(
          "Unexpected exception initializing defaults", e));
    }
    
    try {
      // load default cluster BEFORE execution graphs as they may depend on
      // cluster configs.
      String clusters = tsdb.getConfig().getString("tsd.query.default_clusters");
      if (!Strings.isNullOrEmpty(clusters)) {
        if (clusters.toLowerCase().endsWith(".json") ||
            clusters.toLowerCase().endsWith(".conf")) {
          clusters = Files.toString(new File(clusters), Const.UTF8_CHARSET);
        }
        // double check in case the file was empty.
        if (!Strings.isNullOrEmpty(clusters)) {
          final TypeReference<List<ClusterConfig>> typeref = 
              new TypeReference<List<ClusterConfig>>() {};
          final List<ClusterConfig> configs = 
              (List<ClusterConfig>) JSON.parseToObject(clusters, typeref);
          
          // validation before adding them to the registry
          final Set<String> ids = Sets.newHashSet();
          for (final ClusterConfig cluster : configs) {
            if (ids.contains(cluster.getId())) {
              return Deferred.fromError(new IllegalArgumentException(
                  "More than one cluster had the same ID: " + cluster));
            }
          }
          
          for (final ClusterConfig cluster : configs) {
            deferreds.add(cluster.initialize(tsdb));
            registerClusterConfig(cluster);
          }
        }
      }
      
      // load default execution graphs
      String exec_graphs = tsdb.getConfig().getString(
          "tsd.query.default_execution_graphs");
      if (!Strings.isNullOrEmpty(exec_graphs)) {
        if (exec_graphs.toLowerCase().endsWith(".json") || 
            exec_graphs.toLowerCase().endsWith(".conf")) {
          exec_graphs = Files.toString(new File(exec_graphs), Const.UTF8_CHARSET);
        }
        // double check in case the file was empty.
        if (!Strings.isNullOrEmpty(exec_graphs)){
          final TypeReference<List<ExecutionGraph>> typeref = 
              new TypeReference<List<ExecutionGraph>>() {};
          final List<ExecutionGraph> graphs = 
              (List<ExecutionGraph>) JSON.parseToObject(exec_graphs, typeref);
          
          // validation before adding them to the registry
          final Set<String> ids = Sets.newHashSet();
          boolean had_null = false;
          for (final ExecutionGraph graph : graphs) {
            if (Strings.isNullOrEmpty(graph.getId())) {
              if (had_null) {
                return Deferred.fromError(new IllegalArgumentException(
                    "More than one graph was configured as the default. "
                    + "There can be only one: " + graph));
              }
              had_null = true;
            } else {
              if (ids.contains(graph.getId())) {
                return Deferred.fromError(new IllegalArgumentException(
                    "More than one graph had the same ID: " + graph));
              }
              ids.add(graph.getId());
            }
          }
          
//          for (final ExecutionGraph graph : graphs) {
//            deferreds.add(graph.initialize(tsdb));
//            registerExecutionGraph(graph, 
//                Strings.isNullOrEmpty(graph.getId()) ? true : false);
//          }
        }
      }
      
    } catch (Exception e) {
      LOG.error("Failed loading default execution graphs and cluster configs", e);
      return Deferred.fromError(new RuntimeException("Unexpected exception "
          + "initializing defaults", e));
    }
    
    LOG.info("Completed initializing registry defaults.");
    return Deferred.group(deferreds).addCallback(Deferreds.NULL_GROUP_CB);
  }

  @Override
  public QueryIteratorInterpolatorFactory getQueryIteratorInterpolatorFactory(
      String id) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public QueryIteratorFactory getQueryIteratorFactory(String id) {
    // TODO Auto-generated method stub
    return null;
  }
  
}
