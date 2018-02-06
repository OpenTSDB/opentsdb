// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
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

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.DeferredGroupException;

import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//import org.hbase.async.AppendRequest;
//import org.hbase.async.Bytes;
//import org.hbase.async.Bytes.ByteMap;
//import org.hbase.async.ClientStats;
//import org.hbase.async.DeleteRequest;
//import org.hbase.async.GetRequest;
//import org.hbase.async.HBaseClient;
//import org.hbase.async.HBaseException;
//import org.hbase.async.KeyValue;
//import org.hbase.async.PutRequest;
//import org.jboss.netty.util.HashedWheelTimer;
//import org.jboss.netty.util.Timeout;
//import org.jboss.netty.util.Timer;

//import net.opentsdb.auth.AuthenticationPlugin;
//import net.opentsdb.tree.TreeBuilder;
//import net.opentsdb.tsd.RTPublisher;
//import net.opentsdb.tsd.StorageExceptionHandler;
//import net.opentsdb.uid.NoSuchUniqueId;
//import net.opentsdb.uid.NoSuchUniqueName;
//import net.opentsdb.uid.UniqueId;
//import net.opentsdb.uid.UniqueIdFilterPlugin;
//import net.opentsdb.uid.UniqueId.UniqueIdType;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.PluginLoader;
import net.opentsdb.utils.Threads;
//import net.opentsdb.meta.Annotation;
//import net.opentsdb.meta.MetaDataCache;
//import net.opentsdb.meta.TSMeta;
//import net.opentsdb.meta.UIDMeta;
//import net.opentsdb.query.expression.ExpressionFactory;
import net.opentsdb.query.filter.TagVFilter;
//import net.opentsdb.rollup.RollupConfig;
//import net.opentsdb.rollup.RollupInterval;
//import net.opentsdb.rollup.RollupUtils;
//import net.opentsdb.search.SearchPlugin;
//import net.opentsdb.search.SearchQuery;
//import net.opentsdb.tools.StartupPlugin;
//import net.opentsdb.stats.Histogram;
//import net.opentsdb.stats.QueryStats;
//import net.opentsdb.stats.StatsCollector;

/**
 * Thread-safe implementation of the TSDB client.
 * <p>
 * This class is the central class of OpenTSDB.  You use it to add new data
 * points or query the database.
 */
public class DefaultTSDB implements TSDB {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultTSDB.class);
  
//  static final byte[] FAMILY = { 't' };
//
//  /** Charset used to convert Strings to byte arrays and back. */
//  private static final Charset CHARSET = Charset.forName("ISO-8859-1");
//  private static final String METRICS_QUAL = "metrics";
//  private static short METRICS_WIDTH = 3;
//  private static final String TAG_NAME_QUAL = "tagk";
//  private static short TAG_NAME_WIDTH = 3;
//  private static final String TAG_VALUE_QUAL = "tagv";
//  private static short TAG_VALUE_WIDTH = 3;
//
//  /** Client for the HBase cluster to use.  */
//  final HBaseClient client;
//
//  /** Name of the table in which timeseries are stored.  */
//  final byte[] table;
//  /** Name of the table in which UID information is stored. */
//  final byte[] uidtable;
//  /** Name of the table where tree data is stored. */
//  final byte[] treetable;
//  /** Name of the table where meta data is stored. */
//  final byte[] meta_table;
//
//  /** Unique IDs for the metric names. */
//  final UniqueId metrics;
//  /** Unique IDs for the tag names. */
//  final UniqueId tag_names;
//  /** Unique IDs for the tag values. */
//  final UniqueId tag_values;

  /** Configuration object for all TSDB components */
  final Config config;
  
  /** The plugin and object regsitry used by OpenTSDB. */
  final Registry registry;

  /** Timer used for various tasks such as idle timeouts or query timeouts */
  private final HashedWheelTimer timer;
  
//  /**
//   * Row keys that need to be compacted.
//   * Whenever we write a new data point to a row, we add the row key to this
//   * set.  Every once in a while, the compaction thread will go through old
//   * row keys and will read re-compact them.
//   */
//  private final CompactionQueue compactionq;
//
//  /** Authentication Plugin to use if configured */
//  private AuthenticationPlugin authentication = null;
//
//  /** Search indexer to use if configured */
//  private SearchPlugin search = null;
//
//  /** Optional Startup Plugin to use if configured */
//  private StartupPlugin startup = null;
//
//  /** Optional real time pulblisher plugin to use if configured */
//  private RTPublisher rt_publisher = null;
//  
//  /** Optional plugin for handling meta data caching and updating */
//  private MetaDataCache meta_cache = null;
//  
//  /** Plugin for dealing with data points that can't be stored */
//  private StorageExceptionHandler storage_exception_handler = null;
//
//  /** A filter plugin for allowing or blocking time series */
//  private WriteableDataPointFilterPlugin ts_filter;
//  
//  /** A filter plugin for allowing or blocking UIDs */
//  private UniqueIdFilterPlugin uid_filter;
//  
//  /** The rollup config object for storing and querying rollups */
//  private final RollupConfig rollup_config;
//  
//  /** The default rollup interval. */
//  private final RollupInterval default_interval;
//  
//  /** Name of the tag we use to determine aggregates */
//  private final String agg_tag_key;
//  
//  /** Name of the tag we use use for raw data. */
//  private final String raw_agg_tag_value;
//  
//  /** Whether or not to tag raw data with the raw value tag */
//  private final boolean tag_raw_data;
//  
//  /** Whether or not to block writing of derived rollups/pre-ags */
//  private final boolean rollups_block_derived;
//  
//  /** Writes rejected by the filter */ 
//  private final AtomicLong rejected_dps = new AtomicLong();
//  private final AtomicLong rejected_aggregate_dps = new AtomicLong();
//  
//  /** Datapoints Added */
//  private static final AtomicLong datapoints_added = new AtomicLong();

//  /**
//   * Constructor
//   * @param client An initialized HBase client object
//   * @param config An initialized configuration object
//   * @since 2.1
//   */
//  public TSDB(final HBaseClient client, final Config config) {
//    this.config = config;
//    if (client == null) {
//      final org.hbase.async.Config async_config;
//      if (config.configLocation() != null && !config.configLocation().isEmpty()) {
//        try {
//          async_config = new org.hbase.async.Config(config.configLocation());
//        } catch (final IOException e) {
//          throw new RuntimeException("Failed to read the config file: " + 
//              config.configLocation(), e);
//        }
//      } else {
//        async_config = new org.hbase.async.Config();
//      }
//      async_config.overrideConfig("hbase.zookeeper.znode.parent", 
//          config.getString("tsd.storage.hbase.zk_basedir"));
//      async_config.overrideConfig("hbase.zookeeper.quorum", 
//          config.getString("tsd.storage.hbase.zk_quorum"));
//      this.client = new HBaseClient(async_config);
//    } else {
//      this.client = client;
//    }
//    
//    // SALT AND UID WIDTHS
//    // Users really wanted this to be set via config instead of having to 
//    // compile. Hopefully they know NOT to change these after writing data.
//    if (config.hasProperty("tsd.storage.uid.width.metric")) {
//      METRICS_WIDTH = config.getShort("tsd.storage.uid.width.metric");
//    }
//    if (config.hasProperty("tsd.storage.uid.width.tagk")) {
//      TAG_NAME_WIDTH = config.getShort("tsd.storage.uid.width.tagk");
//    }
//    if (config.hasProperty("tsd.storage.uid.width.tagv")) {
//      TAG_VALUE_WIDTH = config.getShort("tsd.storage.uid.width.tagv");
//    }
//    if (config.hasProperty("tsd.storage.max_tags")) {
//      Const.setMaxNumTags(config.getShort("tsd.storage.max_tags"));
//    }
//    if (config.hasProperty("tsd.storage.salt.buckets")) {
//      Const.setSaltBuckets(config.getInt("tsd.storage.salt.buckets"));
//    }
//    if (config.hasProperty("tsd.storage.salt.width")) {
//      Const.setSaltWidth(config.getInt("tsd.storage.salt.width"));
//    }
//    
//    table = config.getString("tsd.storage.hbase.data_table").getBytes(CHARSET);
//    uidtable = config.getString("tsd.storage.hbase.uid_table").getBytes(CHARSET);
//    treetable = config.getString("tsd.storage.hbase.tree_table").getBytes(CHARSET);
//    meta_table = config.getString("tsd.storage.hbase.meta_table").getBytes(CHARSET);
//    
//    if (config.getBoolean("tsd.core.uid.random_metrics")) {
//      metrics = new UniqueId(this, uidtable, METRICS_QUAL, METRICS_WIDTH, true);
//    } else {
//      metrics = new UniqueId(this, uidtable, METRICS_QUAL, METRICS_WIDTH, false);
//    }
//    tag_names = new UniqueId(this, uidtable, TAG_NAME_QUAL, TAG_NAME_WIDTH, false);
//    tag_values = new UniqueId(this, uidtable, TAG_VALUE_QUAL, TAG_VALUE_WIDTH, false);
//    compactionq = new CompactionQueue(this);
//    
//    if (config.hasProperty("tsd.core.timezone")) {
//      DateTime.setDefaultTimezone(config.getString("tsd.core.timezone"));
//    }
//    
//    timer = Threads.newTimer("TSDB Timer");
//    
//    if (config.getBoolean("tsd.rollups.enable")) {
//      rollup_config = new RollupConfig();
//      RollupInterval config_default = null;
//      for (final RollupInterval interval: rollup_config.getRollups().values()) {
//        if (interval.isDefaultRollupInterval()) {
//          config_default = interval;
//          System.out.println("Found default: " + interval);
//          break;
//        }
//      }
//      
//      if (config_default == null) {
//        throw new IllegalArgumentException("None of the rollup intervals were "
//            + "marked as the \"default\".");
//      }
//      default_interval = config_default;
//      tag_raw_data = config.getBoolean("tsd.rollups.tag_raw");
//      agg_tag_key = config.getString("tsd.rollups.agg_tag_key");
//      raw_agg_tag_value = config.getString("tsd.rollups.raw_agg_tag_value");
//      rollups_block_derived = config.getBoolean("tsd.rollups.block_derived");
//    } else {
//      rollup_config = null;
//      default_interval = null;
//      tag_raw_data = false;
//      agg_tag_key = null;
//      raw_agg_tag_value = null;
//      rollups_block_derived = false;
//    }
//    
//    QueryStats.setEnableDuplicates(
//        config.getBoolean("tsd.query.allow_simultaneous_duplicates"));
//    
//    if (config.getBoolean("tsd.core.preload_uid_cache")) {
//      final ByteMap<UniqueId> uid_cache_map = new ByteMap<UniqueId>();
//      uid_cache_map.put(METRICS_QUAL.getBytes(CHARSET), metrics);
//      uid_cache_map.put(TAG_NAME_QUAL.getBytes(CHARSET), tag_names);
//      uid_cache_map.put(TAG_VALUE_QUAL.getBytes(CHARSET), tag_values);
//      UniqueId.preloadUidCache(this, uid_cache_map);
//    }
//    
//    if (config.getString("tsd.core.tag.allow_specialchars") != null) {
//      Tags.setAllowSpecialChars(config.getString("tsd.core.tag.allow_specialchars"));
//    }
//    
//    // load up the functions that require the TSDB object
//    ExpressionFactory.addTSDBFunctions(this);
//    
//    // set any extra tags from the config for stats
//    StatsCollector.setGlobalTags(config);
//    
//    LOG.debug(config.dumpConfiguration());
//  }
//  
  /**
   * Constructor
   * @param config An initialized configuration object
   * @since 2.0
   */
  public DefaultTSDB(final Config config) {
    this.config = config;
    registry = new DefaultRegistry(this);
    timer = Threads.newTimer("MainTSDBTimer");
  }
  
  /**
   * Initializes the registry for use.
   * @param load_plugins Whether or not to load plugins.
   * @return A deferred to wait on resolving to a null on success or an
   * exception on failure.
   */
  public Deferred<Object> initializeRegistry(final boolean load_plugins) {
    return registry.initialize(load_plugins);
  }
  
//  /** @return The data point column family name */
//  public static byte[] FAMILY() {
//    return FAMILY;
//  }
//
//  /**
//   * Called by initializePlugins, also used to load startup plugins.
//   * @since 2.3
//   */
//  public static void loadPluginPath(final String plugin_path) {
//    if (plugin_path != null && !plugin_path.isEmpty()) {
//      try {
//        PluginLoader.loadJARs(plugin_path);
//      } catch (Exception e) {
//        LOG.error("Error loading plugins from plugin path: " + plugin_path, e);
//        throw new RuntimeException("Error loading plugins from plugin path: " +
//                plugin_path, e);
//      }
//    }
//  }
//
//  /**
//   * Should be called immediately after construction to initialize plugins and
//   * objects that rely on such. It also moves most of the potential exception
//   * throwing code out of the constructor so TSDMain can shutdown clients and
//   * such properly.
//   * @param init_rpcs Whether or not to initialize RPC plugins as well
//   * @throws RuntimeException if the plugin path could not be processed
//   * @throws IllegalArgumentException if a plugin could not be initialized
//   * @since 2.0
//   */
//  public void initializePlugins(final boolean init_rpcs) {
//    final String plugin_path = config.getString("tsd.core.plugin_path");
//    loadPluginPath(plugin_path);
//
//    try {
//      TagVFilter.initializeFilterMap(this);
//      // @#$@%$%#$ing typed exceptions
//    } catch (SecurityException e) {
//      throw new RuntimeException("Failed to instantiate filters", e);
//    } catch (IllegalArgumentException e) {
//      throw new RuntimeException("Failed to instantiate filters", e);
//    } catch (ClassNotFoundException e) {
//      throw new RuntimeException("Failed to instantiate filters", e);
//    } catch (NoSuchMethodException e) {
//      throw new RuntimeException("Failed to instantiate filters", e);
//    } catch (NoSuchFieldException e) {
//      throw new RuntimeException("Failed to instantiate filters", e);
//    } catch (IllegalAccessException e) {
//      throw new RuntimeException("Failed to instantiate filters", e);
//    } catch (InvocationTargetException e) {
//      throw new RuntimeException("Failed to instantiate filters", e);
//    }
//
//    // load the authentication plugin if enabled
//    if (config.getBoolean("tsd.core.authentication.enable")) {
//      authentication = PluginLoader.loadSpecificPlugin(config.getString("tsd.core.authentication.plugin"), AuthenticationPlugin.class);
//      if (authentication == null) {
//        throw new IllegalArgumentException("Unable to locate authentication plugin: "+ config.getString("tsd.core.authentication.plugin"));
//      }
//      try {
//        authentication.initialize(this);
//      } catch (Exception e) {
//        throw new RuntimeException("Failed to initialize authentication plugin", e);
//      }
//    }
//
//    // load the search plugin if enabled
//    if (config.getBoolean("tsd.search.enable")) {
//      search = PluginLoader.loadSpecificPlugin(
//          config.getString("tsd.search.plugin"), SearchPlugin.class);
//      if (search == null) {
//        throw new IllegalArgumentException("Unable to locate search plugin: " + 
//            config.getString("tsd.search.plugin"));
//      }
//      try {
//        search.initialize(this);
//      } catch (Exception e) {
//        throw new RuntimeException("Failed to initialize search plugin", e);
//      }
//      LOG.info("Successfully initialized search plugin [" + 
//          search.getClass().getCanonicalName() + "] version: " 
//          + search.version());
//    } else {
//      search = null;
//    }
//    
//    // load the real time publisher plugin if enabled
//    if (config.getBoolean("tsd.rtpublisher.enable")) {
//      rt_publisher = PluginLoader.loadSpecificPlugin(
//          config.getString("tsd.rtpublisher.plugin"), RTPublisher.class);
//      if (rt_publisher == null) {
//        throw new IllegalArgumentException(
//            "Unable to locate real time publisher plugin: " + 
//            config.getString("tsd.rtpublisher.plugin"));
//      }
//      try {
//        rt_publisher.initialize(this);
//      } catch (Exception e) {
//        throw new RuntimeException(
//            "Failed to initialize real time publisher plugin", e);
//      }
//      LOG.info("Successfully initialized real time publisher plugin [" + 
//          rt_publisher.getClass().getCanonicalName() + "] version: " 
//          + rt_publisher.version());
//    } else {
//      rt_publisher = null;
//    }
//    
//    // load the meta cache plugin if enabled
//    if (config.getBoolean("tsd.core.meta.cache.enable")) {
//      meta_cache = PluginLoader.loadSpecificPlugin(
//          config.getString("tsd.core.meta.cache.plugin"), MetaDataCache.class);
//      if (meta_cache == null) {
//        throw new IllegalArgumentException(
//            "Unable to locate meta cache plugin: " + 
//            config.getString("tsd.core.meta.cache.plugin"));
//      }
//      try {
//        meta_cache.initialize(this);
//      } catch (Exception e) {
//        throw new RuntimeException(
//            "Failed to initialize meta cache plugin", e);
//      }
//      LOG.info("Successfully initialized meta cache plugin [" + 
//          meta_cache.getClass().getCanonicalName() + "] version: " 
//          + meta_cache.version());
//    }
//    
//    // load the storage exception plugin if enabled
//    if (config.getBoolean("tsd.core.storage_exception_handler.enable")) {
//      storage_exception_handler = PluginLoader.loadSpecificPlugin(
//          config.getString("tsd.core.storage_exception_handler.plugin"), 
//          StorageExceptionHandler.class);
//      if (storage_exception_handler == null) {
//        throw new IllegalArgumentException(
//            "Unable to locate storage exception handler plugin: " + 
//            config.getString("tsd.core.storage_exception_handler.plugin"));
//      }
//      try {
//        storage_exception_handler.initialize(this);
//      } catch (Exception e) {
//        throw new RuntimeException(
//            "Failed to initialize storage exception handler plugin", e);
//      }
//      LOG.info("Successfully initialized storage exception handler plugin [" + 
//          storage_exception_handler.getClass().getCanonicalName() + "] version: " 
//          + storage_exception_handler.version());
//    }
//    
//    // Writeable Data Point Filter
//    if (config.getBoolean("tsd.timeseriesfilter.enable")) {
//      ts_filter = PluginLoader.loadSpecificPlugin(
//          config.getString("tsd.timeseriesfilter.plugin"), 
//          WriteableDataPointFilterPlugin.class);
//      if (ts_filter == null) {
//        throw new IllegalArgumentException(
//            "Unable to locate time series filter plugin plugin: " + 
//            config.getString("tsd.timeseriesfilter.plugin"));
//      }
//      try {
//        ts_filter.initialize(this);
//      } catch (Exception e) {
//        throw new RuntimeException(
//            "Failed to initialize time series filter plugin", e);
//      }
//      LOG.info("Successfully initialized time series filter plugin [" + 
//          ts_filter.getClass().getCanonicalName() + "] version: " 
//          + ts_filter.version());
//    }
//    
//    // UID Filter
//    if (config.getBoolean("tsd.uidfilter.enable")) {
//      uid_filter = PluginLoader.loadSpecificPlugin(
//          config.getString("tsd.uidfilter.plugin"), 
//          UniqueIdFilterPlugin.class);
//      if (uid_filter == null) {
//        throw new IllegalArgumentException(
//            "Unable to locate UID filter plugin plugin: " + 
//            config.getString("tsd.uidfilter.plugin"));
//      }
//      try {
//        uid_filter.initialize(this);
//      } catch (Exception e) {
//        throw new RuntimeException(
//            "Failed to initialize UID filter plugin", e);
//      }
//      LOG.info("Successfully initialized UID filter plugin [" + 
//          uid_filter.getClass().getCanonicalName() + "] version: " 
//          + uid_filter.version());
//    }
//  }
//
//  /**
//   * Returns the configured Authentication Plugin
//   * @return The Authentication Plugin
//   * @since 2.4
//   */
//  public final AuthenticationPlugin getAuth() {
//    return this.authentication;
//  }
//
//  /** 
//   * Returns the configured HBase client 
//   * @return The HBase client
//   * @since 2.0 
//   */
//  public final HBaseClient getClient() {
//    return this.client;
//  }
//
//  /**
//   * Sets the startup plugin so that it can be shutdown properly. 
//   * Note that this method will not initialize or call any other methods 
//   * belonging to the plugin's implementation.
//   * @param plugin The startup plugin that was used. 
//   * @since 2.3
//   */
//  public final void setStartupPlugin(final StartupPlugin plugin) { 
//    startup = plugin; 
//  }
//  
//  /**
//   * Getter that returns the startup plugin object.
//   * @return The StartupPlugin object or null if the plugin was not set.
//   * @since 2.3
//   */
//  public final StartupPlugin getStartupPlugin() { 
//    return startup; 
//  }
//
  /**
   * Getter that returns the configuration object
   * @return The configuration object
   * @since 2.0 
   */
  public Config getConfig() {
    return config;
  }
  
  /**
   * @return The registry for working with plugins, processors and types.
   * @since 3.0
   */
  public Registry getRegistry() {
    return registry;
  }
  
//  /**
//   * Returns the storage exception handler. May be null if not enabled
//   * @return The storage exception handler
//   * @since 2.2
//   */
//  public final StorageExceptionHandler getStorageExceptionHandler() {
//    return storage_exception_handler;
//  }
//
//  /**
//   * @return the TS filter object, may be null
//   * @since 2.3
//   */
//  public WriteableDataPointFilterPlugin getTSfilter() {
//    return ts_filter;
//  }
//  
//  /** 
//   * @return The UID filter object, may be null. 
//   * @since 2.3 
//   */
//  public UniqueIdFilterPlugin getUidFilter() {
//    return uid_filter;
//  }
//  
//  /**
//   * Attempts to find the name for a unique identifier given a type
//   * @param type The type of UID
//   * @param uid The UID to search for
//   * @return The name of the UID object if found
//   * @throws IllegalArgumentException if the type is not valid
//   * @throws NoSuchUniqueId if the UID was not found
//   * @since 2.0
//   */
//  public Deferred<String> getUidName(final UniqueIdType type, final byte[] uid) {
//    if (uid == null) {
//      throw new IllegalArgumentException("Missing UID");
//    }
//
//    switch (type) {
//      case METRIC:
//        return this.metrics.getNameAsync(uid);
//      case TAGK:
//        return this.tag_names.getNameAsync(uid);
//      case TAGV:
//        return this.tag_values.getNameAsync(uid);
//      default:
//        throw new IllegalArgumentException("Unrecognized UID type");
//    }
//  }
//  
//  /**
//   * Attempts to find the UID matching a given name
//   * @param type The type of UID
//   * @param name The name to search for
//   * @throws IllegalArgumentException if the type is not valid
//   * @throws NoSuchUniqueName if the name was not found
//   * @since 2.0
//   */
//  public byte[] getUID(final UniqueIdType type, final String name) {
//    try {
//      return getUIDAsync(type, name).join();
//    } catch (NoSuchUniqueName e) {
//      throw e;
//    } catch (IllegalArgumentException e) {
//      throw e;
//    } catch (Exception e) {
//      LOG.error("Unexpected exception", e);
//      throw new RuntimeException(e);
//    }
//  }
//  
//  /**
//   * Attempts to find the UID matching a given name
//   * @param type The type of UID
//   * @param name The name to search for
//   * @throws IllegalArgumentException if the type is not valid
//   * @throws NoSuchUniqueName if the name was not found
//   * @since 2.1
//   */
//  public Deferred<byte[]> getUIDAsync(final UniqueIdType type, final String name) {
//    if (name == null || name.isEmpty()) {
//      throw new IllegalArgumentException("Missing UID name");
//    }
//    switch (type) {
//      case METRIC:
//        return metrics.getIdAsync(name);
//      case TAGK:
//        return tag_names.getIdAsync(name);
//      case TAGV:
//        return tag_values.getIdAsync(name);
//      default:
//        throw new IllegalArgumentException("Unrecognized UID type");
//    }
//  }
//  
//  /**
//   * Verifies that the data and UID tables exist in HBase and optionally the
//   * tree and meta data tables if the user has enabled meta tracking or tree
//   * building
//   * @return An ArrayList of objects to wait for
//   * @throws TableNotFoundException
//   * @since 2.0
//   */
//  public Deferred<ArrayList<Object>> checkNecessaryTablesExist() {
//    final ArrayList<Deferred<Object>> checks = 
//      new ArrayList<Deferred<Object>>(2);
//    checks.add(client.ensureTableExists(
//        config.getString("tsd.storage.hbase.data_table")));
//    checks.add(client.ensureTableExists(
//        config.getString("tsd.storage.hbase.uid_table")));
//    if (config.enable_tree_processing()) {
//      checks.add(client.ensureTableExists(
//          config.getString("tsd.storage.hbase.tree_table")));
//    }
//    if (config.enable_realtime_ts() || config.enable_realtime_uid() || 
//        config.enable_tsuid_incrementing()) {
//      checks.add(client.ensureTableExists(
//          config.getString("tsd.storage.hbase.meta_table")));
//    }
//    return Deferred.group(checks);
//  }
//  
//  /** Number of cache hits during lookups involving UIDs. */
//  public int uidCacheHits() {
//    return (metrics.cacheHits() + tag_names.cacheHits()
//            + tag_values.cacheHits());
//  }
//
//  /** Number of cache misses during lookups involving UIDs. */
//  public int uidCacheMisses() {
//    return (metrics.cacheMisses() + tag_names.cacheMisses()
//            + tag_values.cacheMisses());
//  }
//
//  /** Number of cache entries currently in RAM for lookups involving UIDs. */
//  public int uidCacheSize() {
//    return (metrics.cacheSize() + tag_names.cacheSize()
//            + tag_values.cacheSize());
//  }
//
//  /**
//   * Collects the stats and metrics tracked by this instance.
//   * @param collector The collector to use.
//   */
//  public void collectStats(final StatsCollector collector) {
//    final byte[][] kinds = { 
//        METRICS_QUAL.getBytes(CHARSET), 
//        TAG_NAME_QUAL.getBytes(CHARSET), 
//        TAG_VALUE_QUAL.getBytes(CHARSET) 
//      };
//    try {
//      final Map<String, Long> used_uids = UniqueId.getUsedUIDs(this, kinds)
//        .joinUninterruptibly();
//      
//      collectUidStats(metrics, collector);
//      if (config.getBoolean("tsd.core.uid.random_metrics")) {
//        collector.record("uid.ids-used", 0, "kind=" + METRICS_QUAL);
//        collector.record("uid.ids-available", 0, "kind=" + METRICS_QUAL);
//      } else {
//        collector.record("uid.ids-used", used_uids.get(METRICS_QUAL), 
//            "kind=" + METRICS_QUAL);
//        collector.record("uid.ids-available", 
//            (Internal.getMaxUnsignedValueOnBytes(metrics.width()) - 
//                used_uids.get(METRICS_QUAL)), "kind=" + METRICS_QUAL);
//      }
//      
//      collectUidStats(tag_names, collector);
//      collector.record("uid.ids-used", used_uids.get(TAG_NAME_QUAL), 
//          "kind=" + TAG_NAME_QUAL);
//      collector.record("uid.ids-available", 
//          (Internal.getMaxUnsignedValueOnBytes(tag_names.width()) - 
//              used_uids.get(TAG_NAME_QUAL)), 
//          "kind=" + TAG_NAME_QUAL);
//      
//      collectUidStats(tag_values, collector);
//      collector.record("uid.ids-used", used_uids.get(TAG_VALUE_QUAL), 
//          "kind=" + TAG_VALUE_QUAL);
//      collector.record("uid.ids-available", 
//          (Internal.getMaxUnsignedValueOnBytes(tag_values.width()) - 
//              used_uids.get(TAG_VALUE_QUAL)), "kind=" + TAG_VALUE_QUAL);
//      
//    } catch (Exception e) {
//      throw new RuntimeException("Shouldn't be here", e);
//    }
//    
//    collector.record("uid.filter.rejected", rejected_dps.get(), "kind=raw");
//    collector.record("uid.filter.rejected", rejected_aggregate_dps.get(), 
//        "kind=aggregate");
//
//    {
//      final Runtime runtime = Runtime.getRuntime();
//      collector.record("jvm.ramfree", runtime.freeMemory());
//      collector.record("jvm.ramused", runtime.totalMemory());
//    }
//
//    collector.addExtraTag("class", "IncomingDataPoints");
//    try {
//      collector.record("hbase.latency", IncomingDataPoints.putlatency, "method=put");
//    } finally {
//      collector.clearExtraTag("class");
//    }
//
//    collector.addExtraTag("class", "TSDB");
//    try {
//      collector.record("datapoints.added", datapoints_added, "type=all");
//    } finally {
//      collector.clearExtraTag("class");
//    }
//
//    collector.addExtraTag("class", "TsdbQuery");
//    try {
//      collector.record("hbase.latency", TsdbQuery.scanlatency, "method=scan");
//    } finally {
//      collector.clearExtraTag("class");
//    }
//    final ClientStats stats = client.stats();
//    collector.record("hbase.root_lookups", stats.rootLookups());
//    collector.record("hbase.meta_lookups",
//                     stats.uncontendedMetaLookups(), "type=uncontended");
//    collector.record("hbase.meta_lookups",
//                     stats.contendedMetaLookups(), "type=contended");
//    collector.record("hbase.rpcs",
//                     stats.atomicIncrements(), "type=increment");
//    collector.record("hbase.rpcs", stats.deletes(), "type=delete");
//    collector.record("hbase.rpcs", stats.gets(), "type=get");
//    collector.record("hbase.rpcs", stats.puts(), "type=put");
//    collector.record("hbase.rpcs", stats.appends(), "type=append");
//    collector.record("hbase.rpcs", stats.rowLocks(), "type=rowLock");
//    collector.record("hbase.rpcs", stats.scannersOpened(), "type=openScanner");
//    collector.record("hbase.rpcs", stats.scans(), "type=scan");
//    collector.record("hbase.rpcs.batched", stats.numBatchedRpcSent());
//    collector.record("hbase.flushes", stats.flushes());
//    collector.record("hbase.connections.created", stats.connectionsCreated());
//    collector.record("hbase.connections.idle_closed", stats.idleConnectionsClosed());
//    collector.record("hbase.nsre", stats.noSuchRegionExceptions());
//    collector.record("hbase.nsre.rpcs_delayed",
//                     stats.numRpcDelayedDueToNSRE());
//    collector.record("hbase.region_clients.open",
//        stats.regionClients());
//    collector.record("hbase.region_clients.idle_closed",
//        stats.idleConnectionsClosed());
//
//    compactionq.collectStats(collector);
//    // Collect Stats from Plugins
//    if (startup != null) {
//      try {
//        collector.addExtraTag("plugin", "startup");
//        startup.collectStats(collector);
//      } finally {
//        collector.clearExtraTag("plugin");
//      }
//    }
//    if (rt_publisher != null) {
//      try {
//        collector.addExtraTag("plugin", "publish");
//        rt_publisher.collectStats(collector);
//      } finally {
//        collector.clearExtraTag("plugin");
//      }                        
//    }
//    if (search != null) {
//      try {
//        collector.addExtraTag("plugin", "search");
//        search.collectStats(collector);
//      } finally {
//        collector.clearExtraTag("plugin");
//      }                        
//    }
//    if (storage_exception_handler != null) {
//      try {
//        collector.addExtraTag("plugin", "storageExceptionHandler");
//        storage_exception_handler.collectStats(collector);
//      } finally {
//        collector.clearExtraTag("plugin");
//      }
//    }
//    if (ts_filter != null) {
//      try {
//        collector.addExtraTag("plugin", "timeseriesFilter");
//        ts_filter.collectStats(collector);
//      } finally {
//        collector.clearExtraTag("plugin");
//      }
//    }
//    if (uid_filter != null) {
//      try {
//        collector.addExtraTag("plugin", "uidFilter");
//        uid_filter.collectStats(collector);
//      } finally {
//        collector.clearExtraTag("plugin");
//      }
//    }
//  }
//
//  /** Returns a latency histogram for Put RPCs used to store data points. */
//  public Histogram getPutLatencyHistogram() {
//    return IncomingDataPoints.putlatency;
//  }
//
//  /** Returns a latency histogram for Scan RPCs used to fetch data points.  */
//  public Histogram getScanLatencyHistogram() {
//    return TsdbQuery.scanlatency;
//  }
//
//  /**
//   * Collects the stats for a {@link UniqueId}.
//   * @param uid The instance from which to collect stats.
//   * @param collector The collector to use.
//   */
//  private static void collectUidStats(final UniqueId uid,
//                                      final StatsCollector collector) {
//    collector.record("uid.cache-hit", uid.cacheHits(), "kind=" + uid.kind());
//    collector.record("uid.cache-miss", uid.cacheMisses(), "kind=" + uid.kind());
//    collector.record("uid.cache-size", uid.cacheSize(), "kind=" + uid.kind());
//    collector.record("uid.random-collisions", uid.randomIdCollisions(), 
//        "kind=" + uid.kind());
//    collector.record("uid.rejected-assignments", uid.rejectedAssignments(), 
//        "kind=" + uid.kind());
//  }
//
//  /** @return the width, in bytes, of metric UIDs */
//  public static short metrics_width() {
//    return METRICS_WIDTH;
//  }
//  
//  /** @return the width, in bytes, of tagk UIDs */
//  public static short tagk_width() {
//    return TAG_NAME_WIDTH;
//  }
//  
//  /** @return the width, in bytes, of tagv UIDs */
//  public static short tagv_width() {
//    return TAG_VALUE_WIDTH;
//  }
//  
//  /**
//   * Returns a new {@link Query} instance suitable for this TSDB.
//   */
//  public Query newQuery() {
//    return new TsdbQuery(this);
//  }
//
//  /**
//   * Returns a new {@link WritableDataPoints} instance suitable for this TSDB.
//   * <p>
//   * If you want to add a single data-point, consider using {@link #addPoint}
//   * instead.
//   */
//  public WritableDataPoints newDataPoints() {
//    return new IncomingDataPoints(this);
//  }
//
//  /**
//   * Returns a new {@link BatchedDataPoints} instance suitable for this TSDB.
//   * 
//   * @param metric Every data point that gets appended must be associated to this metric.
//   * @param tags The associated tags for all data points being added.
//   * @return data structure which can have data points appended.
//   */
//  public WritableDataPoints newBatch(String metric, Map<String, String> tags) {
//    return new BatchedDataPoints(this, metric, tags);
//  }
//
//  /**
//   * Adds a single integer value data point in the TSDB.
//   * <p>
//   * WARNING: The tags map may be modified by this method without a lock. Give 
//   * the method a copy if you plan to use it elsewhere.
//   * <p>
//   * @param metric A non-empty string.
//   * @param timestamp The timestamp associated with the value.
//   * @param value The value of the data point.
//   * @param tags The tags on this series.  This map must be non-empty.
//   * @return A deferred object that indicates the completion of the request.
//   * The {@link Object} has not special meaning and can be {@code null} (think
//   * of it as {@code Deferred<Void>}). But you probably want to attach at
//   * least an errback to this {@code Deferred} to handle failures.
//   * @throws IllegalArgumentException if the timestamp is less than or equal
//   * to the previous timestamp added or 0 for the first timestamp, or if the
//   * difference with the previous timestamp is too large.
//   * @throws IllegalArgumentException if the metric name is empty or contains
//   * illegal characters.
//   * @throws IllegalArgumentException if the tags list is empty or one of the
//   * elements contains illegal characters.
//   * @throws HBaseException (deferred) if there was a problem while persisting
//   * data.
//   */
//  public Deferred<Object> addPoint(final String metric,
//                                   final long timestamp,
//                                   final long value,
//                                   final Map<String, String> tags) {
//    final byte[] v;
//    if (Byte.MIN_VALUE <= value && value <= Byte.MAX_VALUE) {
//      v = new byte[] { (byte) value };
//    } else if (Short.MIN_VALUE <= value && value <= Short.MAX_VALUE) {
//      v = Bytes.fromShort((short) value);
//    } else if (Integer.MIN_VALUE <= value && value <= Integer.MAX_VALUE) {
//      v = Bytes.fromInt((int) value);
//    } else {
//      v = Bytes.fromLong(value);
//    }
//
//    final short flags = (short) (v.length - 1);  // Just the length.
//    return addPointInternal(metric, timestamp, v, tags, flags);
//  }
//
//  /**
//   * Adds a double precision floating-point value data point in the TSDB.
//   * <p>
//   * WARNING: The tags map may be modified by this method without a lock. Give 
//   * the method a copy if you plan to use it elsewhere.
//   * <p>
//   * @param metric A non-empty string.
//   * @param timestamp The timestamp associated with the value.
//   * @param value The value of the data point.
//   * @param tags The tags on this series.  This map must be non-empty.
//   * @return A deferred object that indicates the completion of the request.
//   * The {@link Object} has not special meaning and can be {@code null} (think
//   * of it as {@code Deferred<Void>}). But you probably want to attach at
//   * least an errback to this {@code Deferred} to handle failures.
//   * @throws IllegalArgumentException if the timestamp is less than or equal
//   * to the previous timestamp added or 0 for the first timestamp, or if the
//   * difference with the previous timestamp is too large.
//   * @throws IllegalArgumentException if the metric name is empty or contains
//   * illegal characters.
//   * @throws IllegalArgumentException if the value is NaN or infinite.
//   * @throws IllegalArgumentException if the tags list is empty or one of the
//   * elements contains illegal characters.
//   * @throws HBaseException (deferred) if there was a problem while persisting
//   * data.
//   * @since 1.2
//   */
//  public Deferred<Object> addPoint(final String metric,
//                                   final long timestamp,
//                                   final double value,
//                                   final Map<String, String> tags) {
//    if (Double.isNaN(value) || Double.isInfinite(value)) {
//      throw new IllegalArgumentException("value is NaN or Infinite: " + value
//                                         + " for metric=" + metric
//                                         + " timestamp=" + timestamp);
//    }
//    final short flags = Const.FLAG_FLOAT | 0x7;  // A float stored on 8 bytes.
//    return addPointInternal(metric, timestamp,
//                            Bytes.fromLong(Double.doubleToRawLongBits(value)),
//                            tags, flags);
//  }
//
//  /**
//   * Adds a single floating-point value data point in the TSDB.
//   * <p>
//   * WARNING: The tags map may be modified by this method without a lock. Give 
//   * the method a copy if you plan to use it elsewhere.
//   * <p>
//   * @param metric A non-empty string.
//   * @param timestamp The timestamp associated with the value.
//   * @param value The value of the data point.
//   * @param tags The tags on this series.  This map must be non-empty.
//   * @return A deferred object that indicates the completion of the request.
//   * The {@link Object} has not special meaning and can be {@code null} (think
//   * of it as {@code Deferred<Void>}). But you probably want to attach at
//   * least an errback to this {@code Deferred} to handle failures.
//   * @throws IllegalArgumentException if the timestamp is less than or equal
//   * to the previous timestamp added or 0 for the first timestamp, or if the
//   * difference with the previous timestamp is too large.
//   * @throws IllegalArgumentException if the metric name is empty or contains
//   * illegal characters.
//   * @throws IllegalArgumentException if the value is NaN or infinite.
//   * @throws IllegalArgumentException if the tags list is empty or one of the
//   * elements contains illegal characters.
//   * @throws HBaseException (deferred) if there was a problem while persisting
//   * data.
//   */
//  public Deferred<Object> addPoint(final String metric,
//                                   final long timestamp,
//                                   final float value,
//                                   final Map<String, String> tags) {
//    if (Float.isNaN(value) || Float.isInfinite(value)) {
//      throw new IllegalArgumentException("value is NaN or Infinite: " + value
//                                         + " for metric=" + metric
//                                         + " timestamp=" + timestamp);
//    }
//    final short flags = Const.FLAG_FLOAT | 0x3;  // A float stored on 4 bytes.
//    return addPointInternal(metric, timestamp,
//                            Bytes.fromInt(Float.floatToRawIntBits(value)),
//                            tags, flags);
//  }
//
//  Deferred<Object> addPointInternal(final String metric,
//                                            final long timestamp,
//                                            final byte[] value,
//                                            final Map<String, String> tags,
//                                            final short flags) {
//    // we only accept positive unix epoch timestamps in seconds or milliseconds
//    if (timestamp < 0 || ((timestamp & Const.SECOND_MASK) != 0 && 
//        timestamp > 9999999999999L)) {
//      throw new IllegalArgumentException((timestamp < 0 ? "negative " : "bad")
//          + " timestamp=" + timestamp
//          + " when trying to add value=" + Arrays.toString(value) + '/' + flags
//          + " to metric=" + metric + ", tags=" + tags);
//    }
//    IncomingDataPoints.checkMetricAndTags(metric, tags);
//    final byte[] row = IncomingDataPoints.rowKeyTemplate(this, metric, tags);
//    final long base_time;
//    final byte[] qualifier = Internal.buildQualifier(timestamp, flags);
//    
//    if ((timestamp & Const.SECOND_MASK) != 0) {
//      // drop the ms timestamp to seconds to calculate the base timestamp
//      base_time = ((timestamp / 1000) - 
//          ((timestamp / 1000) % Const.MAX_TIMESPAN));
//    } else {
//      base_time = (timestamp - (timestamp % Const.MAX_TIMESPAN));
//    }
//    
//    /** Callback executed for chaining filter calls to see if the value
//     * should be written or not. */
//    final class WriteCB implements Callback<Deferred<Object>, Boolean> {
//      @Override
//      public Deferred<Object> call(final Boolean allowed) throws Exception {
//        if (!allowed) {
//          rejected_dps.incrementAndGet();
//          return Deferred.fromResult(null);
//        }
//        
//        Bytes.setInt(row, (int) base_time, metrics.width() + Const.SALT_WIDTH());
//        RowKey.prefixKeyWithSalt(row);
//
//        Deferred<Object> result = null;
//        if (config.enable_appends()) {
//          final AppendDataPoints kv = new AppendDataPoints(qualifier, value);
//          final AppendRequest point = new AppendRequest(table, row, FAMILY, 
//              AppendDataPoints.APPEND_COLUMN_QUALIFIER, kv.getBytes());
//          result = client.append(point);
//        } else {
//          scheduleForCompaction(row, (int) base_time);
//          final PutRequest point = new PutRequest(table, row, FAMILY, qualifier, value);
//          result = client.put(point);
//        }
//
//        // Count all added datapoints, not just those that came in through PUT rpc
//        // Will there be others? Well, something could call addPoint programatically right?
//        datapoints_added.incrementAndGet();
//
//        // TODO(tsuna): Add a callback to time the latency of HBase and store the
//        // timing in a moving Histogram (once we have a class for this).
//        
//        if (!config.enable_realtime_ts() && !config.enable_tsuid_incrementing() && 
//            !config.enable_tsuid_tracking() && rt_publisher == null) {
//          return result;
//        }
//        
//        final byte[] tsuid = UniqueId.getTSUIDFromKey(row, METRICS_WIDTH, 
//            Const.TIMESTAMP_BYTES);
//        
//        // if the meta cache plugin is instantiated then tracking goes through it
//        if (meta_cache != null) {
//          meta_cache.increment(tsuid);
//        } else {
//          if (config.enable_tsuid_tracking()) {
//            if (config.enable_realtime_ts()) {
//              if (config.enable_tsuid_incrementing()) {
//                TSMeta.incrementAndGetCounter(TSDB.this, tsuid);
//              } else {
//                TSMeta.storeIfNecessary(TSDB.this, tsuid);
//              }
//            } else {
//              final PutRequest tracking = new PutRequest(meta_table, tsuid, 
//                  TSMeta.FAMILY(), TSMeta.COUNTER_QUALIFIER(), Bytes.fromLong(1));
//              client.put(tracking);
//            }
//          }
//        }
//
//        if (rt_publisher != null) {
//          rt_publisher.sinkDataPoint(metric, timestamp, value, tags, tsuid, flags);
//        }
//        return result;
//      }
//      @Override
//      public String toString() {
//        return "addPointInternal Write Callback";
//      }
//    }
//    
//    if (ts_filter != null && ts_filter.filterDataPoints()) {
//      return ts_filter.allowDataPoint(metric, timestamp, value, tags, flags)
//          .addCallbackDeferring(new WriteCB());
//    }
//    return Deferred.fromResult(true).addCallbackDeferring(new WriteCB());
//  }
//
//  /**
//   * Adds a rolled up and/or groupby/pre-agged data point to the proper table.
//   * <p>
//   * WARNING: The tags map may be modified by this method without a lock. Give 
//   * the method a copy if you plan to use it elsewhere.
//   * <p>
//   * If {@code interval} is null then the value will be directed to the 
//   * pre-agg table.
//   * If the {@code is_groupby} flag is set, then the aggregate tag, defined in
//   * "tsd.rollups.agg_tag", will be added or overwritten with the {@code aggregator}
//   * value in uppercase as the value. 
//   * @param metric A non-empty string.
//   * @param timestamp The timestamp associated with the value.
//   * @param value The value of the data point.
//   * @param tags The tags on this series.  This map must be non-empty.
//   * @param is_groupby Whether or not the value is a pre-aggregate
//   * @param interval The interval the data reflects (may be null)
//   * @param rollup_aggregator The aggregator used to generate the data
//   * @param groupby_aggregator = The aggregator used for pre-aggregated data.
//   * @return A deferred to optionally wait on to be sure the value was stored 
//   * @throws IllegalArgumentException if the timestamp is less than or equal
//   * to the previous timestamp added or 0 for the first timestamp, or if the
//   * difference with the previous timestamp is too large.
//   * @throws IllegalArgumentException if the metric name is empty or contains
//   * illegal characters.
//   * @throws IllegalArgumentException if the tags list is empty or one of the
//   * elements contains illegal characters.
//   * @throws HBaseException (deferred) if there was a problem while persisting
//   * data.
//   * @since 2.4
//   */
//  public Deferred<Object> addAggregatePoint(final String metric,
//                                   final long timestamp,
//                                   final long value,
//                                   final Map<String, String> tags,
//                                   final boolean is_groupby,
//                                   final String interval,
//                                   final String rollup_aggregator,
//                                   final String groupby_aggregator) {
//    final byte[] val = Internal.vleEncodeLong(value);
//
//    final short flags = (short) (val.length - 1);  // Just the length.
//    
//    return addAggregatePointInternal(metric, timestamp,
//            val, tags, flags, is_groupby, interval, rollup_aggregator,
//            groupby_aggregator);
//  }
//  
//  /**
//   * Adds a rolled up and/or groupby/pre-agged data point to the proper table.
//   * <p>
//   * WARNING: The tags map may be modified by this method without a lock. Give 
//   * the method a copy if you plan to use it elsewhere.
//   * <p>
//   * If {@code interval} is null then the value will be directed to the 
//   * pre-agg table.
//   * If the {@code is_groupby} flag is set, then the aggregate tag, defined in
//   * "tsd.rollups.agg_tag", will be added or overwritten with the {@code aggregator}
//   * value in uppercase as the value. 
//   * @param metric A non-empty string.
//   * @param timestamp The timestamp associated with the value.
//   * @param value The value of the data point.
//   * @param tags The tags on this series.  This map must be non-empty.
//   * @param is_groupby Whether or not the value is a pre-aggregate
//   * @param interval The interval the data reflects (may be null)
//   * @param rollup_aggregator The aggregator used to generate the data
//   * @param groupby_aggregator = The aggregator used for pre-aggregated data.
//   * @return A deferred to optionally wait on to be sure the value was stored 
//   * @throws IllegalArgumentException if the timestamp is less than or equal
//   * to the previous timestamp added or 0 for the first timestamp, or if the
//   * difference with the previous timestamp is too large.
//   * @throws IllegalArgumentException if the metric name is empty or contains
//   * illegal characters.
//   * @throws IllegalArgumentException if the tags list is empty or one of the
//   * elements contains illegal characters.
//   * @throws HBaseException (deferred) if there was a problem while persisting
//   * data.
//   * @since 2.4
//   */
//  public Deferred<Object> addAggregatePoint(final String metric,
//                                   final long timestamp,
//                                   final float value,
//                                   final Map<String, String> tags,
//                                   final boolean is_groupby,
//                                   final String interval,
//                                   final String rollup_aggregator,
//                                   final String groupby_aggregator) {
//    if (Float.isNaN(value) || Float.isInfinite(value)) {
//      throw new IllegalArgumentException("value is NaN or Infinite: " + value
//              + " for metric=" + metric
//              + " timestamp=" + timestamp);
//    }
//
//    final short flags = Const.FLAG_FLOAT | 0x3;  // A float stored on 4 bytes.
//
//    final byte[] val = Bytes.fromInt(Float.floatToRawIntBits(value));
//
//    return addAggregatePointInternal(metric, timestamp,
//            val, tags, flags, is_groupby, interval, rollup_aggregator,
//            groupby_aggregator);
//  }
//  
//  /**
//   * Adds a rolled up and/or groupby/pre-agged data point to the proper table.
//   * If {@code interval} is null then the value will be directed to the 
//   * pre-agg table.
//   * If the {@code is_groupby} flag is set, then the aggregate tag, defined in
//   * "tsd.rollups.agg_tag", will be added or overwritten with the {@code aggregator}
//   * value in uppercase as the value. 
//   * @param metric A non-empty string.
//   * @param timestamp The timestamp associated with the value.
//   * @param value The value of the data point.
//   * @param tags The tags on this series.  This map must be non-empty.
//   * @param is_groupby Whether or not the value is a pre-aggregate
//   * @param interval The interval the data reflects (may be null)
//   * @param rollup_aggregator The aggregator used to generate the data
//   * @param groupby_aggregator = The aggregator used for pre-aggregated data.
//   * @return A deferred to optionally wait on to be sure the value was stored 
//   * @throws IllegalArgumentException if the timestamp is less than or equal
//   * to the previous timestamp added or 0 for the first timestamp, or if the
//   * difference with the previous timestamp is too large.
//   * @throws IllegalArgumentException if the metric name is empty or contains
//   * illegal characters.
//   * @throws IllegalArgumentException if the tags list is empty or one of the
//   * elements contains illegal characters.
//   * @throws HBaseException (deferred) if there was a problem while persisting
//   * data.
//   * @since 2.4
//   */
//  public Deferred<Object> addAggregatePoint(final String metric,
//                                   final long timestamp,
//                                   final double value,
//                                   final Map<String, String> tags,
//                                   final boolean is_groupby,
//                                   final String interval,
//                                   final String rollup_aggregator,
//                                   final String groupby_aggregator) {
//    if (Double.isNaN(value) || Double.isInfinite(value)) {
//      throw new IllegalArgumentException("value is NaN or Infinite: " + value
//              + " for metric=" + metric
//              + " timestamp=" + timestamp);
//    }
//
//    final short flags = Const.FLAG_FLOAT | 0x7;  // A float stored on 4 bytes.
//
//    final byte[] val = Bytes.fromLong(Double.doubleToRawLongBits(value));
//
//    return addAggregatePointInternal(metric, timestamp,
//            val, tags, flags, is_groupby, interval, rollup_aggregator,
//            groupby_aggregator);
//  }
//  
//  Deferred<Object> addAggregatePointInternal(final String metric,
//      final long timestamp,
//      final byte[] value,
//      final Map<String, String> tags,
//      final short flags,
//      final boolean is_groupby,
//      final String interval,
//      final String rollup_aggregator,
//      final String groupby_aggregator) {
//    
//    if (interval != null && !interval.isEmpty() && rollup_config == null) {
//      throw new IllegalArgumentException(
//          "No rollup or aggregations were configured");
//    }
//    if (is_groupby && 
//        (groupby_aggregator == null || groupby_aggregator.isEmpty())) {
//      throw new IllegalArgumentException("Cannot write a group by data point "
//          + "without specifying the aggregation function. Metric=" + metric 
//          + " tags=" + tags);
//    }
//    
//    // we only accept positive unix epoch timestamps in seconds for rollups
//    // and allow milliseconds for pre-aggregates
//    if (timestamp < 0 || ((timestamp & Const.SECOND_MASK) != 0)) {
//      throw new IllegalArgumentException((timestamp < 0 ? "negative " : "bad")
//        + " timestamp=" + timestamp
//        + " when trying to add value=" + Arrays.toString(value) + '/' + flags
//        + " to metric=" + metric + ", tags=" + tags);
//    }
//    
//    String agg_tag_value = tags.get(agg_tag_key);
//    if (agg_tag_value == null) {
//      if (!is_groupby) {
//        // it's a rollup on "raw" data.
//        if (tag_raw_data) {
//          tags.put(agg_tag_key, raw_agg_tag_value);
//        }
//        agg_tag_value = raw_agg_tag_value;
//      } else {
//        // pre-agged so use the aggregator as the tag
//        agg_tag_value = groupby_aggregator.toUpperCase();
//        tags.put(agg_tag_key, agg_tag_value);
//      }
//    } else {
//      // sanity check
//      if (!agg_tag_value.equalsIgnoreCase(groupby_aggregator)) {
//        throw new IllegalArgumentException("Given tag value for " + agg_tag_key 
//            + " of " + agg_tag_value + " did not match the group by "
//                + "aggregator of " + groupby_aggregator + " for " + metric 
//                + " " + tags);
//      }
//      // force upper case
//      agg_tag_value = groupby_aggregator.toUpperCase();
//      tags.put(agg_tag_key, agg_tag_value);
//    }
//    
//    if (is_groupby) {
//      try {
//        Aggregators.get(groupby_aggregator.toLowerCase());
//      } catch (NoSuchElementException e) {
//        throw new IllegalArgumentException("Invalid group by aggregator " 
//            + groupby_aggregator + " with metric " + metric + " " + tags);
//      }
//      if (rollups_block_derived &&
//          // TODO - create a better list of aggs to block
//          (agg_tag_value.equals("AVG") || 
//              agg_tag_value.equals("DEV"))) {
//        throw new IllegalArgumentException("Derived group by aggregations "
//            + "are not allowed " + groupby_aggregator + " with metric " 
//            + metric + " " + tags);
//      }
//    }
//    
//    IncomingDataPoints.checkMetricAndTags(metric, tags);
//    
//    final RollupInterval rollup_interval = interval == null || interval.isEmpty()
//        ? null : rollup_config.getRollupInterval(interval);
//    
//    final byte[] row = IncomingDataPoints.rowKeyTemplate(this, metric, tags);
//    final String rollup_agg = rollup_aggregator != null ? 
//        rollup_aggregator.toUpperCase() : null;
//    if (rollup_agg!= null && rollups_block_derived && 
//        // TODO - create a better list of aggs to block
//        (rollup_agg.equals("AVG") || 
//            rollup_agg.equals("DEV"))) {
//      throw new IllegalArgumentException("Derived rollup aggregations "
//          + "are not allowed " + rollup_agg + " with metric " 
//          + metric + " " + tags);
//    }
//    final int base_time = interval == null || interval.isEmpty() ? 
//        (int)(timestamp - (timestamp % Const.MAX_TIMESPAN))
//          : RollupUtils.getRollupBasetime(timestamp, rollup_interval);
//    final byte[] qualifier = interval == null || interval.isEmpty() ? 
//        Internal.buildQualifier(timestamp, flags)
//        : RollupUtils.buildRollupQualifier(
//            timestamp, base_time, flags, rollup_agg, rollup_interval);
//    
//    /** Callback executed for chaining filter calls to see if the value
//    * should be written or not. */
//    final class WriteCB implements Callback<Deferred<Object>, Boolean> {
//      @Override
//      public Deferred<Object> call(final Boolean allowed) throws Exception {
//        if (!allowed) {
//          rejected_aggregate_dps.incrementAndGet();
//          return Deferred.fromResult(null);
//        }
//        Internal.setBaseTime(row, base_time);
//        // NOTE: Do not modify the row key after calculating and applying the salt
//        RowKey.prefixKeyWithSalt(row);
//        
//        Deferred<Object> result;
//        
//        final PutRequest point;
//        if (interval == null || interval.isEmpty()) {
//          if (!is_groupby) {
//            throw new IllegalArgumentException("Interval cannot be null "
//                + "for a non-group by point");
//          }
//          point = new PutRequest(default_interval.getGroupbyTable(), row, 
//              FAMILY, qualifier, value);
//        } else {
//          point = new PutRequest(
//          is_groupby ? rollup_interval.getGroupbyTable() : 
//            rollup_interval.getTemporalTable(), 
//              row, FAMILY, qualifier, value);
//        }
//        
//        // TODO: Add a callback to time the latency of HBase and store the
//        // timing in a moving Histogram (once we have a class for this).
//        result = client.put(point);
//        
//        // TODO - figure out what we want to do with the real time publisher and
//        // the meta tracking.
//        return result;
//      }
//    }
//    
//    if (ts_filter != null) {
//      return ts_filter.allowDataPoint(metric, timestamp, value, tags, flags)
//          .addCallbackDeferring(new WriteCB());
//    }
//    try {
//      return new WriteCB().call(true);
//    } catch (Exception e) {
//      return Deferred.fromError(e);
//    }
//  }
//  
//  /**
//   * Forces a flush of any un-committed in memory data including left over 
//   * compactions.
//   * <p>
//   * For instance, any data point not persisted will be sent to HBase.
//   * @return A {@link Deferred} that will be called once all the un-committed
//   * data has been successfully and durably stored.  The value of the deferred
//   * object return is meaningless and unspecified, and can be {@code null}.
//   * @throws HBaseException (deferred) if there was a problem sending
//   * un-committed data to HBase.  Please refer to the {@link HBaseException}
//   * hierarchy to handle the possible failures.  Some of them are easily
//   * recoverable by retrying, some are not.
//   */
//  public Deferred<Object> flush() throws HBaseException {
//    final class HClientFlush implements Callback<Object, ArrayList<Object>> {
//      public Object call(final ArrayList<Object> args) {
//        return client.flush();
//      }
//      public String toString() {
//        return "flush HBase client";
//      }
//    }
//
//    return config.enable_compactions() && compactionq != null
//      ? compactionq.flush().addCallback(new HClientFlush())
//      : client.flush();
//  }

  /**
   * Gracefully shuts down this TSD instance.
   * <p>
   * The method must call {@code shutdown()} on all plugins, thread pools, etc.
   * @return A {@link Deferred} that will be called once all the un-committed
   * data has been successfully and durably stored, and all resources used by
   * this instance have been released.  The value of the deferred object
   * return is meaningless and unspecified, and can be {@code null} or may
   * return an exception.
   * 
   * @since 1.0
   */
  public Deferred<Object> shutdown() {
    return registry.shutdown();
//    final ArrayList<Deferred<Object>> deferreds = 
//      new ArrayList<Deferred<Object>>();
//    
//    final class FinalShutdown implements Callback<Object, Object> {
//      @Override
//      public Object call(Object result) throws Exception {
//        if (result instanceof Exception) {
//          LOG.error("A previous shutdown failed", (Exception)result);
//        }
//        final Set<Timeout> timeouts = timer.stop();
//        // TODO - at some point we should clean these up.
//        if (timeouts.size() > 0) {
//          LOG.warn("There were " + timeouts.size() + " timer tasks queued");
//        }
//        LOG.info("Completed shutting down the TSDB");
//        return Deferred.fromResult(null);
//      }
//    }
//    
//    final class SEHShutdown implements Callback<Object, Object> {
//      @Override
//      public Object call(Object result) throws Exception {
//        if (result instanceof Exception) {
//          LOG.error("Shutdown of the HBase client failed", (Exception)result);
//        }
//        LOG.info("Shutting down storage exception handler plugin: " + 
//            storage_exception_handler.getClass().getCanonicalName());
//        return storage_exception_handler.shutdown().addBoth(new FinalShutdown());
//      }
//      @Override
//      public String toString() {
//        return "SEHShutdown";
//      }
//    }
//    
//    final class HClientShutdown implements Callback<Deferred<Object>, ArrayList<Object>> {
//      public Deferred<Object> call(final ArrayList<Object> args) {
//        if (storage_exception_handler != null) {
//          return client.shutdown().addBoth(new SEHShutdown());
//        }
//        return client.shutdown().addBoth(new FinalShutdown());
//      }
//      public String toString() {
//        return "shutdown HBase client";
//      }
//    }
//    
//    final class ShutdownErrback implements Callback<Object, Exception> {
//      public Object call(final Exception e) {
//        final Logger LOG = LoggerFactory.getLogger(ShutdownErrback.class);
//        if (e instanceof DeferredGroupException) {
//          final DeferredGroupException ge = (DeferredGroupException) e;
//          for (final Object r : ge.results()) {
//            if (r instanceof Exception) {
//              LOG.error("Failed to shutdown the TSD", (Exception) r);
//            }
//          }
//        } else {
//          LOG.error("Failed to shutdown the TSD", e);
//        }
//        return new HClientShutdown().call(null);
//      }
//      public String toString() {
//        return "shutdown HBase client after error";
//      }
//    }
//    
//    final class CompactCB implements Callback<Object, ArrayList<Object>> {
//      public Object call(ArrayList<Object> compactions) throws Exception {
//        return null;
//      }
//    }
//    
//    if (config.enable_compactions()) {
//      LOG.info("Flushing compaction queue");
//      deferreds.add(compactionq.flush().addCallback(new CompactCB()));
//    }
//    if (startup != null) {
//      LOG.info("Shutting down startup plugin: " +
//              startup.getClass().getCanonicalName());
//      deferreds.add(startup.shutdown());
//    }
//    if (authentication != null) {
//      LOG.info("Shutting down authentication plugin: " +
//              authentication.getClass().getCanonicalName());
//      deferreds.add(authentication.shutdown());
//    }
//    if (search != null) {
//      LOG.info("Shutting down search plugin: " + 
//          search.getClass().getCanonicalName());
//      deferreds.add(search.shutdown());
//    }
//    if (rt_publisher != null) {
//      LOG.info("Shutting down RT plugin: " + 
//          rt_publisher.getClass().getCanonicalName());
//      deferreds.add(rt_publisher.shutdown());
//    }
//    if (meta_cache != null) {
//      LOG.info("Shutting down meta cache plugin: " + 
//          meta_cache.getClass().getCanonicalName());
//      deferreds.add(meta_cache.shutdown());
//    }
//    if (storage_exception_handler != null) {
//      LOG.info("Shutting down storage exception handler plugin: " + 
//          storage_exception_handler.getClass().getCanonicalName());
//      deferreds.add(storage_exception_handler.shutdown());
//    }
//    if (ts_filter != null) {
//      LOG.info("Shutting down time series filter plugin: " + 
//          ts_filter.getClass().getCanonicalName());
//      deferreds.add(ts_filter.shutdown());
//    }
//    if (uid_filter != null) {
//      LOG.info("Shutting down UID filter plugin: " + 
//          uid_filter.getClass().getCanonicalName());
//      deferreds.add(uid_filter.shutdown());
//    }
//    
//    // wait for plugins to shutdown before we close the client
//    return deferreds.size() > 0
//      ? Deferred.group(deferreds).addCallbackDeferring(new HClientShutdown())
//          .addErrback(new ShutdownErrback())
//      : new HClientShutdown().call(null);
  }
//
//  /**
//   * Given a prefix search, returns a few matching metric names.
//   * @param search A prefix to search.
//   */
//  public List<String> suggestMetrics(final String search) {
//    return metrics.suggest(search);
//  }
//  
//  /**
//   * Given a prefix search, returns matching metric names.
//   * @param search A prefix to search.
//   * @param max_results Maximum number of results to return.
//   * @since 2.0
//   */
//  public List<String> suggestMetrics(final String search, 
//      final int max_results) {
//    return metrics.suggest(search, max_results);
//  }
//
//  /**
//   * Given a prefix search, returns a few matching tag names.
//   * @param search A prefix to search.
//   */
//  public List<String> suggestTagNames(final String search) {
//    return tag_names.suggest(search);
//  }
//  
//  /**
//   * Given a prefix search, returns matching tagk names.
//   * @param search A prefix to search.
//   * @param max_results Maximum number of results to return.
//   * @since 2.0
//   */
//  public List<String> suggestTagNames(final String search, 
//      final int max_results) {
//    return tag_names.suggest(search, max_results);
//  }
//
//  /**
//   * Given a prefix search, returns a few matching tag values.
//   * @param search A prefix to search.
//   */
//  public List<String> suggestTagValues(final String search) {
//    return tag_values.suggest(search);
//  }
//  
//  /**
//   * Given a prefix search, returns matching tag values.
//   * @param search A prefix to search.
//   * @param max_results Maximum number of results to return.
//   * @since 2.0
//   */
//  public List<String> suggestTagValues(final String search, 
//      final int max_results) {
//    return tag_values.suggest(search, max_results);
//  }
//
//  /**
//   * Discards all in-memory caches.
//   * @since 1.1
//   */
//  public void dropCaches() {
//    metrics.dropCaches();
//    tag_names.dropCaches();
//    tag_values.dropCaches();
//  }
//
//  /**
//   * Attempts to assign a UID to a name for the given type
//   * Used by the UniqueIdRpc call to generate IDs for new metrics, tagks or 
//   * tagvs. The name must pass validation and if it's already assigned a UID,
//   * this method will throw an error with the proper UID. Otherwise if it can
//   * create the UID, it will be returned
//   * @param type The type of uid to assign, metric, tagk or tagv
//   * @param name The name of the uid object
//   * @return A byte array with the UID if the assignment was successful
//   * @throws IllegalArgumentException if the name is invalid or it already 
//   * exists
//   * @since 2.0
//   */
//  public byte[] assignUid(final String type, final String name) {
//    Tags.validateString(type, name);
//    if (type.toLowerCase().equals("metric")) {
//      try {
//        final byte[] uid = this.metrics.getId(name);
//        throw new IllegalArgumentException("Name already exists with UID: " +
//            UniqueId.uidToString(uid));
//      } catch (NoSuchUniqueName nsue) {
//        return this.metrics.getOrCreateId(name);
//      }
//    } else if (type.toLowerCase().equals("tagk")) {
//      try {
//        final byte[] uid = this.tag_names.getId(name);
//        throw new IllegalArgumentException("Name already exists with UID: " +
//            UniqueId.uidToString(uid));
//      } catch (NoSuchUniqueName nsue) {
//        return this.tag_names.getOrCreateId(name);
//      }
//    } else if (type.toLowerCase().equals("tagv")) {
//      try {
//        final byte[] uid = this.tag_values.getId(name);
//        throw new IllegalArgumentException("Name already exists with UID: " +
//            UniqueId.uidToString(uid));
//      } catch (NoSuchUniqueName nsue) {
//        return this.tag_values.getOrCreateId(name);
//      }
//    } else {
//      LOG.warn("Unknown type name: " + type);
//      throw new IllegalArgumentException("Unknown type name");
//    }
//  }
//  
//  /**
//   * Attempts to delete the given UID name mapping from the storage table as
//   * well as the local cache.
//   * @param type The type of UID to delete. Must be "metrics", "tagk" or "tagv"
//   * @param name The name of the UID to delete
//   * @return A deferred to wait on for completion, or an exception if thrown
//   * @throws IllegalArgumentException if the type is invalid
//   * @since 2.2
//   */
//  public Deferred<Object> deleteUidAsync(final String type, final String name) {
//    final UniqueIdType uid_type = UniqueId.stringToUniqueIdType(type);
//    switch (uid_type) {
//    case METRIC:
//      return metrics.deleteAsync(name);
//    case TAGK:
//      return tag_names.deleteAsync(name);
//    case TAGV:
//      return tag_values.deleteAsync(name);
//    default:
//      throw new IllegalArgumentException("Unrecognized UID type: " + uid_type); 
//    }
//  }
//  
//  /**
//   * Attempts to rename a UID from existing name to the given name
//   * Used by the UniqueIdRpc call to rename name of existing metrics, tagks or
//   * tagvs. The name must pass validation. If the UID doesn't exist, the method
//   * will throw an error. Chained IllegalArgumentException is directly exposed
//   * to caller. If the rename was successful, this method returns.
//   * @param type The type of uid to rename, one of metric, tagk and tagv
//   * @param oldname The existing name of the uid object
//   * @param newname The new name to be used on the uid object
//   * @throws IllegalArgumentException if error happened
//   * @since 2.2
//   */
//  public void renameUid(final String type, final String oldname,
//      final String newname) {
//    Tags.validateString(type, oldname);
//    Tags.validateString(type, newname);
//    if (type.toLowerCase().equals("metric")) {
//      try {
//        this.metrics.getId(oldname);
//        this.metrics.rename(oldname, newname);
//      } catch (NoSuchUniqueName nsue) {
//        throw new IllegalArgumentException("Name(\"" + oldname +
//            "\") does not exist");
//      }
//    } else if (type.toLowerCase().equals("tagk")) {
//      try {
//        this.tag_names.getId(oldname);
//        this.tag_names.rename(oldname, newname);
//      } catch (NoSuchUniqueName nsue) {
//        throw new IllegalArgumentException("Name(\"" + oldname +
//            "\") does not exist");
//      }
//    } else if (type.toLowerCase().equals("tagv")) {
//      try {
//        this.tag_values.getId(oldname);
//        this.tag_values.rename(oldname, newname);
//      } catch (NoSuchUniqueName nsue) {
//        throw new IllegalArgumentException("Name(\"" + oldname +
//            "\") does not exist");
//      }
//    } else {
//      LOG.warn("Unknown type name: " + type);
//      throw new IllegalArgumentException("Unknown type name");
//    }
//  }
//
//  /** @return the name of the UID table as a byte array for client requests */
//  public byte[] uidTable() {
//    return this.uidtable;
//  }
//  
//  /** @return the name of the data table as a byte array for client requests */
//  public byte[] dataTable() {
//    return this.table;
//  }
//  
//  /** @return the name of the tree table as a byte array for client requests */
//  public byte[] treeTable() {
//    return this.treetable;
//  }
//  
//  /** @return the name of the meta table as a byte array for client requests */
//  public byte[] metaTable() {
//    return this.meta_table;
//  }
//
//  /**
//   * Index the given timeseries meta object via the configured search plugin
//   * @param meta The meta data object to index
//   * @since 2.0
//   */
//  public void indexTSMeta(final TSMeta meta) {
//    if (search != null) {
//      search.indexTSMeta(meta).addErrback(new PluginError());
//    }
//  }
//  
//  /**
//   * Delete the timeseries meta object from the search index
//   * @param tsuid The TSUID to delete
//   * @since 2.0
//   */
//  public void deleteTSMeta(final String tsuid) {
//    if (search != null) {
//      search.deleteTSMeta(tsuid).addErrback(new PluginError());
//    }
//  }
//  
//  /**
//   * Index the given UID meta object via the configured search plugin
//   * @param meta The meta data object to index
//   * @since 2.0
//   */
//  public void indexUIDMeta(final UIDMeta meta) {
//    if (search != null) {
//      search.indexUIDMeta(meta).addErrback(new PluginError());
//    }
//  }
//  
//  /**
//   * Delete the UID meta object from the search index
//   * @param meta The UID meta object to delete
//   * @since 2.0
//   */
//  public void deleteUIDMeta(final UIDMeta meta) {
//    if (search != null) {
//      search.deleteUIDMeta(meta).addErrback(new PluginError());
//    }
//  }
//  
//  /**
//   * Index the given Annotation object via the configured search plugin
//   * @param note The annotation object to index
//   * @since 2.0
//   */
//  public void indexAnnotation(final Annotation note) {
//    if (search != null) {
//      search.indexAnnotation(note).addErrback(new PluginError());
//    }
//    if( rt_publisher != null ) {
//    	rt_publisher.publishAnnotation(note);
//    }
//  }
//  
//  /**
//   * Delete the annotation object from the search index
//   * @param note The annotation object to delete
//   * @since 2.0
//   */
//  public void deleteAnnotation(final Annotation note) {
//    if (search != null) {
//      search.deleteAnnotation(note).addErrback(new PluginError());
//    }
//  }
//  
//  /**
//   * Processes the TSMeta through all of the trees if configured to do so
//   * @param meta The meta data to process
//   * @since 2.0
//   */
//  public Deferred<Boolean> processTSMetaThroughTrees(final TSMeta meta) {
//    if (config.enable_tree_processing()) {
//      return TreeBuilder.processAllTrees(this, meta);
//    }
//    return Deferred.fromResult(false);
//  }
//  
//  /**
//   * Executes a search query using the search plugin
//   * @param query The query to execute
//   * @return A deferred object to wait on for the results to be fetched
//   * @throws IllegalStateException if the search plugin has not been enabled or
//   * configured
//   * @since 2.0
//   */
//  public Deferred<SearchQuery> executeSearch(final SearchQuery query) {
//    if (search == null) {
//      throw new IllegalStateException(
//          "Searching has not been enabled on this TSD");
//    }
//    
//    return search.executeQuery(query);
//  }
//  
//  /**
//   * Simply logs plugin errors when they're thrown by attaching as an errorback. 
//   * Without this, exceptions will just disappear (unless logged by the plugin) 
//   * since we don't wait for a result.
//   */
//  final class PluginError implements Callback<Object, Exception> {
//    @Override
//    public Object call(final Exception e) throws Exception {
//      LOG.error("Exception from Search plugin indexer", e);
//      return null;
//    }
//  }
//  
//  /** @return the rollup config object. May be null 
//   * @since 2.4 */
//  public RollupConfig getRollupConfig() {
//    return rollup_config;
//  }
//  
//  /** @return The default rollup interval config. May be null.
//   * @since 2.4 */
//  public RollupInterval getDefaultInterval() {
//    return default_interval;
//  }
//  
  /** @return the timer used for various house keeping functions */
  public Timer getTimer() {
    return timer;
  }
  
//  // ------------------ //
//  // Compaction helpers //
//  // ------------------ //
//
//  final KeyValue compact(final ArrayList<KeyValue> row, 
//      List<Annotation> annotations) {
//    return compactionq.compact(row, annotations);
//  }
//
//  /**
//   * Schedules the given row key for later re-compaction.
//   * Once this row key has become "old enough", we'll read back all the data
//   * points in that row, write them back to HBase in a more compact fashion,
//   * and delete the individual data points.
//   * @param row The row key to re-compact later.  Will not be modified.
//   * @param base_time The 32-bit unsigned UNIX timestamp.
//   */
//  final void scheduleForCompaction(final byte[] row, final int base_time) {
//    if (config.enable_compactions()) {
//      compactionq.add(row);
//    }
//  }

}
