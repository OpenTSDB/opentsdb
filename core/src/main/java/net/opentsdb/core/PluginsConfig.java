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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.exceptions.PluginLoadException;
import net.opentsdb.query.pojo.Validatable;
import net.opentsdb.utils.Deferreds;
import net.opentsdb.utils.JSON;
import net.opentsdb.utils.PluginLoader;

/**
 * The configuration class that handles loading, initializing and shutting down
 * of TSDB plugins. It should ONLY be instantiated and dealt with via the 
 * {@link DefaultRegistry}.
 * <p>
 * The class allows for plugin initialization ordering in that initialization
 * occurs by iterating over the {@link #configs} in order. Each initialization
 * will block (asynchronously) and only trigger the next plugin's init method
 * once it has completed. Shutdown happens in reverse initialization order by
 * default and can be changed via {@link #setShutdownReverse(boolean)}.
 * <p>
 * By default, if initialization fails for any plugin, an exception will be
 * returned. If you want to continue loading plugins and don't mind if one or
 * more fail to load, then set {@link #setContinueOnError(boolean)}.
 * <p>
 * There are two types of {@link PluginConfig} that can be handled by this class.
 * <ol>
 * <li>The first is an un-named <i>multi-instance</i> plugin wherein the plugin
 * directory and class path are scanned for all implementations of a plugin type.
 * Each type is loaded and identified by it's full class name. These are useful
 * for plugins like Filters and Factories where there may be many implementations
 * and the user may not reference them by name directly. Only one instance of
 * each concrete class can be loaded by the TSD. For these types, only the
 * {@link PluginConfig#setType(String)} field is populated and all other fields
 * are empty or false.</li>
 * <li>The second time is a specific concrete plugin where multiple instances
 * of the same plugin may be loaded with different configurations or uses. E.g.
 * Multiple executors of the same type may be loaded with different configs and
 * the user may reference them at query time by ID. For these plugins, the 
 * {@link PluginConfig#setPlugin(String)} must be populated (along with the
 * type) as well as either {@link PluginConfig#setId(String)} or 
 * {@link PluginConfig#setDefault(boolean)} must be true (not both at the same
 * time though). If default is true, then the ID in the plugin map will be
 * null and when {@link #getDefaultPlugin(Class)} is called, the plugin for
 * that type will be returned.</li>
 * </ol> 
 * <p>
 * <b>Validation:</b> The following rules pertain to validation:
 * <ul>
 * <li>{@link PluginConfig#setType(String)} cannot be empty or null.</li>
 * <li>If {@link PluginConfig#setPlugin(String)} is not empty or null then either
 * {@link PluginConfig#setId(String)} must be set to a non-empty value OR
 * {@link PluginConfig#setDefault(boolean)} must be set to true.</li>
 * <li>For each plugin type, each ID must be unique.</li>
 * <li>Only one default can exist for each plugin type.</li>
 * </ul>
 * 
 * @since 3.0
 */
@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class PluginsConfig extends Validatable {
  private static final Logger LOG = LoggerFactory.getLogger(PluginsConfig.class);
  
  /** The key used for finding the plugin directory. */
  public static final String PLUGIN_PATH_KEY = "tsd.core.plugin_path";
  
  public static final List<String> DEFAULT_TYPES = Lists.newArrayList();
  static {
    DEFAULT_TYPES.add("net.opentsdb.query.QueryNodeFactory");
    DEFAULT_TYPES.add("net.opentsdb.stats.StatsCollector");
    DEFAULT_TYPES.add("net.opentsdb.query.QueryInterpolatorFactory");
    DEFAULT_TYPES.add("net.opentsdb.uid.UniqueIdFactory");
    DEFAULT_TYPES.add("net.opentsdb.query.serdes.TimeSeriesSerdes");
    DEFAULT_TYPES.add("net.opentsdb.query.execution.QueryExecutorFactory");
    DEFAULT_TYPES.add("net.opentsdb.storage.schemas.tsdb1x.Tsdb1xDataStoreFactory");
    DEFAULT_TYPES.add("net.opentsdb.storage.TimeSeriesDataStoreFactory");
  }
  
  public static final Map<String, String> DEFAULT_IMPLEMENTATIONS = 
      Maps.newHashMap();
  static {
    DEFAULT_IMPLEMENTATIONS.put("net.opentsdb.storage.TimeSeriesDataStoreFactory", 
        "net.opentsdb.storage.schemas.tsdb1x.SchemaFactory");
    DEFAULT_IMPLEMENTATIONS.put(
        "net.opentsdb.storage.schemas.tsdb1x.Tsdb1xDataStoreFactory", 
        "net.opentsdb.storage.Tsdb1xHBaseFactory");
    DEFAULT_IMPLEMENTATIONS.put("net.opentsdb.query.serdes.TimeSeriesSerdes", 
        "net.opentsdb.query.serdes.PBufSerdes");
    DEFAULT_IMPLEMENTATIONS.put("net.opentsdb.query.QueryInterpolatorFactory", 
        "net.opentsdb.query.interpolation.DefaultInterpolatorFactory");    
    DEFAULT_IMPLEMENTATIONS.put("net.opentsdb.stats.Tracer", 
        "net.opentsdb.stats.BraveTracer");
  }
  
  /** The list of plugin configs. */
  protected List<PluginConfig> configs;
  
  /** A list of plugin locations. */
  protected List<String> plugin_locations;
  
  /** Whether or not to continue initializing plugins on error. */
  private boolean continue_on_error;
  
  /** Whether or not to shutdown plugins in reverse initialization order. */
  private boolean shutdown_reverse = true;
  
  /** Whether or not to load the default types like TimeSeriesSerdes, 
   * StatsCollector, etc. When true then regardless of the config, these 
   * default plugins are scanned for and when found, instantiated and 
   * loaded with their names. None are set as defaults though. Defined in
   * {@link #getLoadDefaultTypes()} */
  private boolean load_default_types = true;
  
  /** Whether or not to load the default instances defined in 
   * {@link #getLoadDefaultInstances()}. These would be settings like 
   * the AsyncHBase data store, Brave tracing, etc. */
  private boolean load_default_instances = true;
  
  /** The list of configured and instantiated plugins. */
  private List<TSDBPlugin> instantiated_plugins;
  
  /** The map of plugins loaded by the TSD. This includes the 
   * {@link #instantiated_plugins} as well as those registered. */
  private final Map<Class<?>, Map<String, TSDBPlugin>> plugins;
  
  /** Default ctor */
  public PluginsConfig() {
    instantiated_plugins = Lists.newArrayList();
    plugins = Maps.newHashMapWithExpectedSize(1);
  }
  
  /** @param configs A list of plugin configurations. */
  public void setConfigs(final List<PluginConfig> configs) {
    this.configs = configs;
  }
  
  /** @param plugin_locations A list of plugins and/or directories. */
  public void setPluginLocations(final List<String> plugin_locations) {
    this.plugin_locations = plugin_locations;
  }
  
  /** @param continue_on_error Whether or not to continue initialization when
   * one or more plugins fail. */
  public void setContinueOnError(final boolean continue_on_error) {
    this.continue_on_error = continue_on_error;
  }
  
  /** @param shutdown_reverse Whether or not to shutdown plugins in reverse
   * initialization order. */
  public void setShutdownReverse(final boolean shutdown_reverse) {
    this.shutdown_reverse = shutdown_reverse;
  }
  
  /** @param load_default_types Whether or not to load the default types 
   * like TimeSeriesSerdes, StatsCollector, etc. When true then regardless 
   * of the config, these default plugins are scanned for and when found, 
   * instantiated and loaded with their names. None are set as defaults 
   * though. Defined in {@link #getLoadDefaultTypes()}*/
  public void setLoadDefaultTypes(final boolean load_default_types) {
    this.load_default_types = load_default_types;
  }
  
  /** @param load_default_instances Whether or not to load the default 
   * instances defined in {@link #getLoadDefaultInstances()}. These would 
   * be settings like the AsyncHBase data store, Brave tracing, etc. */
  public void setLoadDefaultInstances(final boolean load_default_instances) {
    this.load_default_instances = load_default_instances;
  }
  
  /** @return An unmodifiable list of the configs. */
  public List<PluginConfig> getConfigs() {
    return configs == null ? Collections.emptyList() : 
      Collections.unmodifiableList(configs);
  }
  
  /** @return Whether or not to continue initialization when one or more 
   * plugins fail. */
  public boolean getContinueOnError() {
    return continue_on_error;
  }
  
  /** @return Whether or not to shutdown plugins in reverse initialization order. */
  public boolean getShutdownReverse() {
    return shutdown_reverse;
  }
  
  /** @return Whether or not to load the default types like 
   * TimeSeriesSerdes, StatsCollector, etc. When true then regardless 
   * of the config, these default plugins are scanned for and when found, 
   * instantiated and loaded with their names. None are set as defaults 
   * though. Defined in {@link #getLoadDefaultTypes()}*/
  public boolean getLoadDefaultTypes() {
    return load_default_types;
  }
  
  /** @return Whether or not to load the default 
   * instances defined in {@link #getLoadDefaultInstances()}. These would 
   * be settings like the AsyncHBase data store, Brave tracing, etc. */
  public boolean getLoadDefaultInstances() {
    return load_default_instances;
  }
  
  /** @return A list of plugins and/or locations. */
  public List<String> getPluginLocations() {
    return plugin_locations == null ? Collections.emptyList() : 
      Collections.unmodifiableList(plugin_locations);
  }
  
  /**
   * Registers the given plugin in the map. If a plugin with the ID is already
   * present, an exception is thrown.
   * <b>Warning:</b> The plugin MUST have been initialized prior to adding it
   * to the registry.
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
    if (clazz == null) {
      throw new IllegalArgumentException("Class cannot be null.");
    }
    if (plugin == null) {
      throw new IllegalArgumentException("Plugin cannot be null.");
    }
    if (!(clazz.isAssignableFrom(plugin.getClass()))) {
      throw new IllegalArgumentException("Plugin [" + plugin 
          + "] is not an instance of class: " + clazz);
    }
    Map<String, TSDBPlugin> class_map = plugins.get(clazz);
    if (class_map == null) {
      class_map = Maps.newHashMapWithExpectedSize(1);
      plugins.put(clazz, class_map);
    } else {
      final TSDBPlugin extant = class_map.get(id);
      if (extant != null) {
        throw new IllegalArgumentException("Plugin with ID [" + id 
            + "] and class [" + clazz + "] already exists: " + extant);
      }
    }
    class_map.put(id, plugin);
  }
  
  /**
   * Retrieves the default plugin of the given type (i.e. the ID was null when
   * registered).
   * @param clazz The type of plugin to be fetched.
   * @return An instantiated plugin if found, null if not.
   * @throws IllegalArgumentException if the clazz was null.
   */
  public <T> T getDefaultPlugin(final Class<T> clazz) {
    return (T) getPlugin(clazz, null);
  }
  
  /**
   * Retrieves the plugin with the given class type and ID.
   * @param clazz The type of plugin to be fetched.
   * @param id An optional ID, may be null if the default is fetched.
   * @return An instantiated plugin if found, null if not.
   * @throws IllegalArgumentException if the clazz was null.
   */
  @SuppressWarnings("unchecked")
  public <T> T getPlugin(final Class<T> clazz, final String id) {
    if (clazz == null) {
      throw new IllegalArgumentException("Class cannot be null.");
    }
    final Map<String, TSDBPlugin> class_map = plugins.get(clazz);
    if (class_map == null) {
      return null;
    }
    return (T) class_map.get(id);
  }
  
  /**
   * Initializes the plugins in the config in order of their appearance in the
   * list.
   * @param tsdb The TSDB used during initialization.
   * @return A deferred to wait on resolving to a null on success or an exception
   * if something went wrong.
   */
  public Deferred<Object> initialize(final TSDB tsdb) {
    // backwards compatibility.
    if (!tsdb.getConfig().hasProperty(PLUGIN_PATH_KEY)) {
      tsdb.getConfig().register(PLUGIN_PATH_KEY, null, false, 
          "An optional directory that is checked for .JAR files "
          + "containing plugin implementations. When found, the files "
          + "are loaded so that plugins can be instantiated.");
    }
    final String plugin_path = tsdb.getConfig().getString(PLUGIN_PATH_KEY);
    if (plugin_locations == null && !Strings.isNullOrEmpty(plugin_path)) {
      plugin_locations = Lists.newArrayListWithCapacity(1);
      plugin_locations.add(plugin_path);
    } else if (!Strings.isNullOrEmpty(plugin_path)) {
      if (!plugin_locations.contains(plugin_path)) {
        plugin_locations.add(plugin_path);
      }
    }
    
    if (plugin_locations != null) {
      for (final String location : plugin_locations) {
        try {
          if (location.endsWith(".jar")) {
            PluginLoader.loadJAR(location);
            LOG.info("Loaded Plugin JAR: " + location);
          } else {
            PluginLoader.loadJARs(location);
            LOG.info("Loaded Plugin directory: " + location);
          }
        } catch (Exception e) {
          if (continue_on_error) {
            LOG.error("Unable to read from the plugin location: " + location 
                + " but configured to continue.", e);
          } else {
            return Deferred.fromError(new PluginLoadException(
                "Unable to read from plugin location: " + location, null, e));
          }
        }
      }
    }
    
    if (load_default_types) {
      loadDefaultTypes();
    }
    
    if (load_default_instances) {
      loadDefaultInstances();
    }
    
    if (configs == null || configs.isEmpty()) {
      return Deferred.fromResult(null);
    }
    
    final List<PluginConfig> waiting_on_init = Lists.newArrayListWithCapacity(1);
    final Deferred<Object> deferred = new Deferred<Object>();
    
    /** The error handler for use when things go pear shaped. */
    class ErrorCB implements Callback<Object, Exception> {
      final int index;
      final Callback<Object, Object> downstream;
      
      ErrorCB(final int index, final Callback<Object, Object> downstream) {
        this.index = index;
        this.downstream = downstream;
      }
      
      @Override
      public Object call(final Exception ex) throws Exception {
        if (continue_on_error) {
          LOG.error("Unable to load plugin(s): " + waiting_on_init, ex);
          waiting_on_init.clear();
          downstream.call(null);
        } else {
          try {
            final PluginLoadException e = new PluginLoadException(
                "Initialization failed for plugin " 
                + configs.get(index).getPlugin(), 
                  configs.get(index).getPlugin(),
                  ex);
            deferred.callback(e);
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
        return null;
      }
    }
    
    /** Loads and initializes the next plugin. */
    class SuccessCB implements Callback<Object, Object> {
      final int index;
      final SuccessCB downstream;
      
      SuccessCB(final int index, final SuccessCB downstream) {
        this.index = index;
        this.downstream = downstream;
      }
      
      @SuppressWarnings("unchecked")
      @Override
      public Object call(final Object ignored) throws Exception {
        for (final PluginConfig waiting : waiting_on_init) {
          registerPlugin(waiting);
        }
        waiting_on_init.clear();
        
        if (index >= configs.size() || index < 0) {
          if (LOG.isDebugEnabled() && index > 0) {
            LOG.debug("Completed loading of plugins.");
          }
          deferred.callback(null);
        } else {
          final PluginConfig plugin_config = configs.get(index);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Loading plugin config: " + plugin_config);
          }
          try {
            if (!(Strings.isNullOrEmpty(plugin_config.getId())) || 
                plugin_config.getIsDefault()) {
              
              // load specific
              final Class<?> type = Class.forName(plugin_config.getType());
              final TSDBPlugin plugin = (TSDBPlugin) PluginLoader
                  .loadSpecificPlugin(plugin_config.getPlugin(), type);
              if (plugin == null) {
                throw new RuntimeException("No plugin found for type: " 
                    + plugin_config.getType());
              }
              
              // stash the plugin while we're waiting for it to init.
              plugin_config.clazz = type;
              plugin_config.instantiated_plugin = plugin;
              waiting_on_init.add(plugin_config);
              
              LOG.info("Loaded plugin " + plugin.id() + " version: " 
                  + plugin.version());
              
              if (downstream != null) {
                plugin.initialize(tsdb)
                  .addCallback(downstream)
                  .addErrback(new ErrorCB(index, downstream));
              } else {
                plugin.initialize(tsdb)
                  .addErrback(new ErrorCB(-1, null));
              }
            } else {
              // load all plugins of a type.
              final Class<?> type = Class.forName(plugin_config.getType());
              final List<TSDBPlugin> plugins = 
                  (List<TSDBPlugin>) PluginLoader.loadPlugins(type);
              
              if (plugins == null || plugins.isEmpty()) {
                LOG.info("No plugins found for type: " + type);
                if (downstream == null) {
                  deferred.callback(null);
                } else {
                  downstream.call(null);
                }
              } else {
                final List<Deferred<Object>> deferreds = 
                    Lists.newArrayListWithCapacity(plugins.size());
                for (final TSDBPlugin plugin : plugins) {
                  deferreds.add(plugin.initialize(tsdb));
                  final PluginConfig.Builder builder = PluginConfig.newBuilder()
                       .setPlugin(plugin.getClass().getCanonicalName())
                       .setType(type.getCanonicalName());
                  if (Strings.isNullOrEmpty(plugin.id())) {
                    builder.setId(plugin.getClass().getCanonicalName());
                  } else {
                    builder.setId(plugin.id());
                  }
                  final PluginConfig waiting = builder.build();
                  waiting.clazz = type;
                  waiting.instantiated_plugin = plugin;
                  waiting_on_init.add(waiting);
                }
                
                if (downstream != null) {
                  Deferred.group(deferreds)
                    .addCallback(Deferreds.NULL_GROUP_CB)
                    .addCallback(downstream)
                    .addErrback(new ErrorCB(index, downstream));
                } else {
                  Deferred.group(deferreds)
                    .addCallback(Deferreds.NULL_GROUP_CB)
                    .addErrback(new ErrorCB(-1, null));
                }
              }
            }
          } catch (Exception e) {
            final PluginLoadException ex = new PluginLoadException(
                "Unable to find instances of plugin " 
                + plugin_config.getPlugin() + " for type " + plugin_config.getType(), 
                  plugin_config.getPlugin(),
                  e);
            if (continue_on_error) {
              LOG.error("Unable to load plugin(s): " + configs.get(index), ex);
              downstream.call(null);
            } else {
              deferred.callback(ex);
            }
          }
        }
        return null;
      }
    }
    
    // build callback chain
    SuccessCB last_cb = new SuccessCB(configs.size(), null);
    for (int i = configs.size() - 1; i >= 0; i--) {
      if (last_cb == null) {
        last_cb = new SuccessCB(i, null);
      } else {
        final SuccessCB cb = new SuccessCB(i, last_cb);
        last_cb = cb;
      }
    }
    
    try {
      last_cb.call(null);
    } catch (Exception e) {
      LOG.error("Failed initial loading of plugins", e);
      deferred.callback(e);
    }
    
    return deferred;
  }
  
  /**
   * Shuts down all of the configured plugins as well as those that were 
   * registered after.
   * @return A deferred to wait on resolving to a null. Exceptions returned by
   * shutdown methods will be logged and not returned.
   */
  public Deferred<Object> shutdown() {
    if ((instantiated_plugins == null || instantiated_plugins.isEmpty()) && 
        plugins.isEmpty()) {
      return Deferred.fromResult(null);
    }
    
    final Deferred<Object> deferred = new Deferred<Object>();
    
    /** Error handler that continues shutdown with the next plugin. */
    class ErrorCB implements Callback<Object, Exception> {
      final TSDBPlugin plugin;
      final Callback<Object, Object> downstream;
      
      ErrorCB(final TSDBPlugin plugin, final Callback<Object, Object> downstream) {
        this.plugin = plugin;
        this.downstream = downstream;
      }
      
      @Override
      public Object call(final Exception ex) throws Exception {
        LOG.error("Failed shutting down plugin: " + plugin, ex);
        try {
          if (downstream != null) {
            downstream.call(null);
          } else {
            LOG.info("Completed shutdown of plugins.");
            deferred.callback(null);
          }
        } catch (Exception e) {
          LOG.error("Unexpected exception calling downstream", e);
        }
        return null;
      }
      
    }
    
    /** Shuts down the given plugin and continues downstream. */
    class SuccessCB implements Callback<Object, Object> {
      final TSDBPlugin plugin;
      final SuccessCB downstream;
      
      SuccessCB(final TSDBPlugin plugin, final SuccessCB downstream) {
        this.plugin = plugin;
        this.downstream = downstream;
      }

      @Override
      public Object call(Object arg) throws Exception {
        if (downstream == null) {
          LOG.info("Completed shutdown of plugins.");
          deferred.callback(null);
        } else {
          try {
            if (downstream != null) {
              plugin.shutdown()
                .addCallback(downstream)
                .addErrback(new ErrorCB(plugin, downstream));
            } else {
              plugin.shutdown()
                .addErrback(new ErrorCB(plugin, null));
            }
          } catch (Exception e) {
            LOG.error("Failed to shutdown plugin: " + plugin.id());
          }
        }
        return null;
      }
    }
    
    // build callback chain
    SuccessCB last_cb = new SuccessCB(null, null);
    if (shutdown_reverse) {
      LOG.info("Shutting down plugins in reverse order of initialization");
      for (int i = instantiated_plugins.size() - 1; i >= 0; i--) {
        if (last_cb == null) {
          last_cb = new SuccessCB(instantiated_plugins.get(i), null);
        } else {
          final SuccessCB cb = 
              new SuccessCB(instantiated_plugins.get(i), last_cb);
          last_cb = cb;
        }
      }
    } else {
      LOG.info("Shutting down plugins in same order as initialization");
      for (int i = 0; i < instantiated_plugins.size(); i++) {
        if (last_cb == null) {
          last_cb = new SuccessCB(instantiated_plugins.get(i), null);
        } else {
          final SuccessCB cb = 
              new SuccessCB(instantiated_plugins.get(i), last_cb);
          last_cb = cb;
        }
      }
    }
    
    // now add any other plugin that snuck in another way.
    for (final Map<String, TSDBPlugin> named_plugins : plugins.values()) {
      for (final TSDBPlugin plugin : named_plugins.values()) {
        if (instantiated_plugins.contains(plugin)) {
          continue;
        }
        if (last_cb == null) {
          last_cb = new SuccessCB(plugin, null);
        } else {
          final SuccessCB cb = new SuccessCB(plugin, last_cb);
          last_cb = cb;
        }
      }
    }
    
    try {
      last_cb.call(null);
    } catch (Exception e) {
      LOG.error("Failed shutdown of plugins", e);
      deferred.callback(e);
    }
    
    return deferred;
  }
  
  @Override
  public void validate() {
    if (configs == null || configs.isEmpty()) {
      // no worries mate.
      return;
    }
    
    final Map<String, Set<String>> type_id_map = Maps.newHashMap();
    final Set<String> defaults = Sets.newHashSet();
    final Set<String> multi_types = Sets.newHashSet();
    
    for (final PluginConfig config : configs) {
      // everyone MUST have a type.
      if (Strings.isNullOrEmpty(config.getType())) {
        throw new IllegalArgumentException("Type cannot be null or empty:" 
            + JSON.serializeToString(config));
      }
      
      // make sure it's either a multi-type or single type with the proper info.
      if (Strings.isNullOrEmpty(config.getId()) && !config.getIsDefault()) {
        if (multi_types.contains(config.getType())) {
          throw new IllegalArgumentException("Duplicate multi-type found. "
              + "Remove one of them:" + JSON.serializeToString(config));
        }
        multi_types.add(config.getType());
      } else {
        if (config.getIsDefault() && !Strings.isNullOrEmpty(config.getId())) {
          throw new IllegalArgumentException("Default configs cannot have "
              + "an ID: " + JSON.serializeToString(config));
        }
        if (!config.getIsDefault()) {
          if (Strings.isNullOrEmpty(config.getId())) {
            throw new IllegalArgumentException("Specific plugin instance must "
                + "have an ID if it is not the default: " 
                + JSON.serializeToString(config));
          }
          Set<String> ids = type_id_map.get(config.getType());
          if (ids != null && ids.contains(config.getId())) {
            throw new IllegalArgumentException("Duplicate ID found. "
                + "Remove or rename one: " + JSON.serializeToString(config));
          }
          if (ids == null) {
            ids = Sets.newHashSetWithExpectedSize(1);
            type_id_map.put(config.getType(), ids);
          }
          ids.add(config.getId());
        } else {
          if (defaults.contains(config.getType())) {
            throw new IllegalArgumentException("Cannot have more than one "
                + "default for a plugin type: " + JSON.serializeToString(config));
          }
          defaults.add(config.getType());
        }
      }
    }
  }
  
  /**
   * Helper method that determines how to register the plugin during initialization.
   * @param config A non-null plugin config.
   */
  void registerPlugin(final PluginConfig config) {
    if (Strings.isNullOrEmpty(config.getId()) && !config.getIsDefault()) {
      // see if the plugin has already been registered.
      final Map<String, TSDBPlugin> extant = plugins.get(config.clazz);
      if (extant != null && extant.containsKey(
          config.instantiated_plugin.getClass().getCanonicalName())) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Plugin already exists: " + config);
        }
        config.instantiated_plugin.shutdown();
        return;
      }
      
      registerPlugin(config.clazz, 
          config.instantiated_plugin.getClass().getCanonicalName(), 
          config.instantiated_plugin);
    } else {
      if (!config.getIsDefault()) {
        // see if the plugin has already been registered.
        final Map<String, TSDBPlugin> extant = plugins.get(config.clazz);
        if (extant != null && extant.containsKey(config.getId())) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Plugin already exists: " + config);
          }
          config.instantiated_plugin.shutdown();
          return;
        }
      }
      
      registerPlugin(config.clazz, 
          config.getIsDefault() ? null : config.getId(), 
          config.instantiated_plugin);
    }
    instantiated_plugins.add(config.instantiated_plugin);
    LOG.info("Registered plugin " + config);
  }
  
  @VisibleForTesting
  List<TSDBPlugin> instantiatedPlugins() {
    return instantiated_plugins;
  }
  
  /** Loads the the {@link #DEFAULT_TYPES} */
  void loadDefaultTypes() {
    if (configs == null) {
      configs = Lists.newArrayList();
    }
    for (final String type : DEFAULT_TYPES) {
      final PluginConfig config = PluginConfig.newBuilder()
          .setType(type)
          .build();
      if (!configs.contains(config)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Will try to load default plugin type: " + type);
        }
        configs.add(config);
      }
    }
  }
  
  /** Loads the {@link #DEFAULT_IMPLEMENTATIONS} */
  void loadDefaultInstances() {
    if (configs == null) {
      configs = Lists.newArrayList();
    }
    for (final Entry<String, String> type : DEFAULT_IMPLEMENTATIONS.entrySet()) {
      final PluginConfig config = PluginConfig.newBuilder()
          .setType(type.getKey())
          .setPlugin(type.getValue())
          .setIsDefault(true)
          .build();
      if (!configs.contains(config)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Will try to load default plugin implementation: " 
              + type.getValue());
        }
        configs.add(config);
      }
    }
  }
  
  /**
   * A single plugin configuration.
   */
  @JsonInclude(Include.NON_DEFAULT)
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonDeserialize(builder = PluginConfig.Builder.class)
  public static class PluginConfig {
    /** The canonical class name of the plugin implementation. */
    private String plugin;
    
    /** A descriptive ID to associate with the plugin. */
    private String id;
    
    /** The canonical class name of the type of plugin implemented by the plugin. */
    private String type;

    /** Whether or not the plugin should be identified as the default for a type. */
    private boolean is_default;
    
    /** Used by the initialization routine for storing the class. */
    protected Class<?> clazz;
    
    /** Used by the initialization routine for storing the instantiated plugin. */
    protected TSDBPlugin instantiated_plugin;
    
    protected PluginConfig(final Builder builder) {
      if (Strings.isNullOrEmpty(builder.type)) {
        throw new IllegalArgumentException("Type cannot be null or empty.");
      }
      plugin = builder.plugin;
      id = builder.id;
      type = builder.type;
      is_default = builder.isDefault;
    }
    
    /** @return The canonical class name of the plugin implementation. */
    public String getPlugin() {
      return plugin;
    }
    
    /** @return A descriptive ID to associate with the plugin. */
    public String getId() {
      return id;
    }

    /** @return The canonical class name of the type of plugin implemented 
     * by the plugin. */
    public String getType() {
      return type;
    }
    
    /** @return Whether or not the plugin should be identified as the default 
     * for a type. */
    public boolean getIsDefault() {
      return is_default;
    }
    
    /** @param plugin The canonical class name of the plugin implementation. */
    public void setPlugin(final String plugin) {
      this.plugin = plugin;
    }
    
    /** @param id A descriptive ID to associate with the plugin. */
    public void setId(final String id) {
      this.id = id;
    }
  
    /** @param type The canonical class name of the type of plugin implemented 
     * by the plugin.*/
    public void setType(final String type) {
      this.type = type;
    }
    
    /** @param is_default Whether or not the plugin should be identified as 
     * the default for a type. */
    public void setDefault(final boolean is_default) {
      this.is_default = is_default;
    }
    
    @Override
    public boolean equals(final Object other) {
      if (other == null) {
        return false;
      }
      if (other == this) {
        return true;
      }
      final PluginConfig c = (PluginConfig) other;
      return Objects.equals(plugin, c.plugin) &&
             Objects.equals(id, c.id) &&
             Objects.equals(type, c.type) && 
             Objects.equals(is_default, c.is_default);
    }
    
    @Override
    public String toString() {
      return new StringBuilder()
          .append("id=")
          .append(id)
          .append(", type=")
          .append(type)
          .append(", plugin=")
          .append(plugin)
          .append(", isDefault=")
          .append(is_default)
          .toString();
    }
    
    public static Builder newBuilder() {
      return new Builder();
    }
    
    @JsonIgnoreProperties(ignoreUnknown = true)
    static class Builder {
      @JsonProperty
      private String plugin;
      @JsonProperty
      private String id;
      @JsonProperty
      private String type;
      @JsonProperty
      private boolean isDefault;
      
      public Builder setPlugin(final String plugin) {
        this.plugin = plugin;
        return this;
      }
      
      public Builder setId(final String id) {
        this.id = id;
        return this;
      }
      
      public Builder setType(final String type) {
        this.type = type;
        return this;
      }
      
      public Builder setIsDefault(final boolean is_default) {
        isDefault = is_default;
        return this;
      }
      
      public PluginConfig build() {
        return new PluginConfig(this);
      }
    }
  }
}
