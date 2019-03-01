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
package net.opentsdb.core;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.configuration.ConfigurationEntrySchema;
import net.opentsdb.data.TimeSeriesDataSourceFactory;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.pools.ObjectPool;
import net.opentsdb.query.QueryIteratorFactory;
import net.opentsdb.query.interpolation.QueryInterpolatorFactory;
import net.opentsdb.query.processor.ProcessorFactory;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.execution.QueryExecutorFactory;
import net.opentsdb.query.hacluster.HAClusterConfig;
import net.opentsdb.query.serdes.TimeSeriesSerdes;
import net.opentsdb.storage.WritableTimeSeriesDataStore;
import net.opentsdb.utils.JSON;

/**
 * A shared location for registering context, mergers, plugins, etc.
 *
 * @since 3.0
 */
public class DefaultRegistry implements Registry {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultRegistry.class);
  
  /** Configuration keys. */
  public static final String PLUGIN_CONFIG_KEY = "tsd.plugin.config";
  public static final String DEFAULT_CLUSTERS_KEY = "tsd.query.default_clusters";
  public static final String DEFAULT_GRAPHS_KEY = "tsd.query.default_execution_graphs";
  
  /** The TSDB to which this registry belongs. Used for reading the config. */
  private final TSDB tsdb;
  
  private final Map<String, TypeToken<? extends TimeSeriesDataType>> type_map;
  
  private final Map<TypeToken<? extends TimeSeriesDataType>, String> default_type_name_map;
  
  /** The map of executor factories for use in constructing graphs. */
  private final Map<String, QueryExecutorFactory<?>> factories;
  
  /** The map of cluster configurations for multi-cluster queries. */
  private final Map<String, HAClusterConfig> clusters;
    
  /** The map of serdes classes. */
  private final Map<String, TimeSeriesSerdes> serdes;
  
  private final Map<String, QueryNodeFactory> node_factories;
  
  private final Map<String, WritableTimeSeriesDataStore> write_stores;
  
  /** A concurrent map of shared objects used by various plugins such as 
   * connection pools, etc. */
  private final Map<String, Object> shared_objects;
  
  /** The map of pools. */
  private final Map<String, ObjectPool> pools;
  
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
    
    if (!tsdb.getConfig().hasProperty(PLUGIN_CONFIG_KEY)) {
      tsdb.getConfig().register(ConfigurationEntrySchema.newBuilder()
          .setKey(PLUGIN_CONFIG_KEY)
          .setType(PluginsConfig.class)
          .setDescription("The path to or direct plugin configuration.")
          .isNullable()
          .setSource(getClass().getName())
          .setValidator(new PluginConfigValidator())
          .build());
    }
    tsdb.getConfig().register(DEFAULT_CLUSTERS_KEY, null, false, 
        "TODO");
    tsdb.getConfig().register(DEFAULT_GRAPHS_KEY, null, false, "TODO");
    
    this.tsdb = tsdb;
    type_map = Maps.newHashMap();
    default_type_name_map = Maps.newHashMap();
    factories = Maps.newHashMapWithExpectedSize(1);
    clusters = Maps.newHashMapWithExpectedSize(1);
    serdes = Maps.newHashMapWithExpectedSize(1);
    node_factories = Maps.newHashMap();
    write_stores = Maps.newHashMap();
    shared_objects = Maps.newConcurrentMap();
    pools = Maps.newHashMap();
    cleanup_pool = Executors.newFixedThreadPool(1);
    
    // TODO - better registration
    type_map.put("numeric", NumericType.TYPE);
    type_map.put("numerictype", NumericType.TYPE);
    type_map.put(NumericType.TYPE.toString().toLowerCase(), 
        NumericType.TYPE);
    type_map.put("numericsummary", NumericSummaryType.TYPE);
    type_map.put("numericsummarytype", NumericSummaryType.TYPE);
    type_map.put(NumericSummaryType.TYPE.toString().toLowerCase(), 
        NumericSummaryType.TYPE);
    
    default_type_name_map.put(NumericType.TYPE, "Numeric");
    default_type_name_map.put(NumericSummaryType.TYPE, "NumericSummary");
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
  
  @Override
  public Map<String, TypeToken<? extends TimeSeriesDataType>> typeMap() {
    return Collections.unmodifiableMap(type_map);
  }
  
  @Override
  public Map<TypeToken<? extends TimeSeriesDataType>, String> defaultTypeNameMap() {
    return Collections.unmodifiableMap(default_type_name_map);
  }
  
  /** @return The cleanup thread pool for post-query or other tasks. */
  public ExecutorService cleanupPool() {
    return cleanup_pool;
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
    QueryNodeFactory plugin = plugins.getPlugin(ProcessorFactory.class, id);
    if (plugin == null) {
      plugin = plugins.getPlugin(TimeSeriesDataSourceFactory.class, id);
      if (plugin == null) {
        return null;
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Caching QueryNodeFactory " + plugin + " with ID: " + id);
    }
    factory = (QueryNodeFactory) plugin;
    node_factories.put(id, factory);
    return factory;
  }
  
  public void registerWriteStore(final WritableTimeSeriesDataStore store, 
      final String id) {
    if (write_stores.putIfAbsent(id, store) != null) {
      throw new IllegalArgumentException("A store with the ID, " + id 
          + ", already exists for: " + store.getClass());
    }
  }
  
  public WritableTimeSeriesDataStore getDefaultWriteStore() {
    return write_stores.get(null);
  }
  
  public WritableTimeSeriesDataStore getWriteStore(final String id) {
    return write_stores.get(id);
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
  public void registerClusterConfig(final HAClusterConfig cluster) {
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
  public HAClusterConfig getClusterConfig(final String cluster) {
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
  public <T> T getDefaultPlugin(final Class<T> clazz) {
    return getPlugin(clazz, null);
  }
  
  /**
   * Retrieves the plugin with the given class type and ID.
   * @param clazz The type of plugin to be fetched.
   * @param id An optional ID, may be null if the default is fetched.
   * @return An instantiated plugin if found, null if not.
   * @throws IllegalArgumentException if the clazz was null.
   */
  public <T> T getPlugin(final Class<T> clazz, final String id) {
    if (plugins == null) {
      throw new IllegalStateException("Plugins have not been loaded. "
          + "Call loadPlugins();");
    }
    return plugins.getPlugin(clazz, id);
  }
  
  @Override
  public Map<Class<?>, Map<String, TSDBPlugin>> plugins() {
    return plugins.plugins();
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
  
  @Override
  public void registerObjectPool(final ObjectPool pool) {
    final ObjectPool extant = pools.putIfAbsent(pool.id(), pool);
    if (extant != null) {
      throw new IllegalArgumentException("Pool with ID " + pool.id() 
        + " is already present.");
    }
  }
  
  @Override
  public ObjectPool getObjectPool(final String id) {
    return pools.get(id);
  }
  
  @Override
  public Map<String, Object> sharedObjects() {
    return Collections.unmodifiableMap(shared_objects);
  }
  
  public TimeSeriesSerdes getSerdes(final String id) {
    return serdes.get(id);
  }
  
  @Override
  public void registerType(final TypeToken<? extends TimeSeriesDataType> type, 
                           final String name,
                           final boolean is_default_name) {
    if (type == null) {
      throw new IllegalArgumentException("The type cannot be null.");
    }
    if (Strings.isNullOrEmpty(name)) {
      throw new IllegalArgumentException("The name cannot be null or "
          + "empty.");
    }
    
    if (is_default_name) {
      final String default_name = default_type_name_map.putIfAbsent(type, name);
      if (default_name != null && !default_name.equals(name)) {
        throw new IllegalArgumentException("Type is already registered "
            + "with a default name: " + default_name);
      }
    }
    
    final TypeToken<?> extant = type_map.putIfAbsent(
        name.trim().toLowerCase(), type);
    if (extant != null && extant != type) {
      throw new IllegalArgumentException("The following type: " + extant 
          + " is already registered under the name " + name);
    }
    
    type_map.putIfAbsent(type.toString().toLowerCase(), type);
  }
  
  @Override
  public TypeToken<? extends TimeSeriesDataType> getType(final String name) {
    return type_map.get(name.trim().toLowerCase());
  }
  
  @Override
  public String getDefaultTypeName(final TypeToken<? extends TimeSeriesDataType> type) {
    return default_type_name_map.get(type);
  }
  
  /**
   * Loads plugins using the 'tsd.plugin.config' property.
   * @return A deferred to wait on for results.
   */
  public Deferred<Object> loadPlugins() {
    try {
      plugins = tsdb.getConfig().getTyped(PLUGIN_CONFIG_KEY, PluginsConfig.class);
      if (plugins != null) {
        return plugins.initialize(tsdb);
      }
    } catch (Exception e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Looks like the plugin config is not an object. Trying a file", e);
      }
    }
    
    // try it as a string.
    final String config = tsdb.getConfig().getString(PLUGIN_CONFIG_KEY);
    if (Strings.isNullOrEmpty(config)) {
      if (plugins == null) {
        LOG.info("No plugin config provided. Using the default config.");
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
    return Deferred.fromResult(null);
  }

  @Override
  public QueryInterpolatorFactory getQueryIteratorInterpolatorFactory(
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
