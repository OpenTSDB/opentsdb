// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
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
package net.opentsdb.utils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

/**
 * OpenTSDB Configuration Class
 * 
 * This handles all of the user configurable variables for a TSD. On
 * initialization default values are configured for all variables. Then
 * implementations should call the {@link #loadConfig()} methods to search for a
 * default configuration or try to load one provided by the user.
 * 
 * To add a configuration, simply set a default value in {@link #setDefaults()}.
 * Wherever you need to access the config value, use the proper helper to fetch
 * the value, accounting for exceptions that may be thrown if necessary.
 * 
 * The get<type> number helpers will return NumberFormatExceptions if the
 * requested property is null or unparseable. The {@link #getString(String)} 
 * helper will return a NullPointerException if the property isn't found.
 * <p>
 * Plugins can extend this class and copy the properties from the main
 * TSDB.config instance. Plugins should never change the main TSD's config
 * properties, rather a plugin should use the Config(final Config parent)
 * constructor to get a copy of the parent's properties and then work with the
 * values locally.
 * @since 2.0
 */
public class Config {
  private static final Logger LOG = LoggerFactory.getLogger(Config.class);

  /** Flag to determine if we're running under Windows or not */
  public static final boolean IS_WINDOWS = 
      System.getProperty("os.name", "").contains("Windows");
  
  // These are accessed often so need a set address for fast access (faster
  // than accessing the map. Their value will be changed when the config is 
  // loaded
  // NOTE: edit the setDefaults() method if you add a public field

  /** tsd.core.auto_create_metrics */
  private boolean auto_metric = false;

  /** tsd.core.auto_create_tagk */
  private boolean auto_tagk = true;
  
  /** tsd.core.auto_create_tagv */
  private boolean auto_tagv = true;
  
  /** tsd.storage.enable_compaction */
  private boolean enable_compactions = true;
  
  /** tsd.storage.enable_appends */
  private boolean enable_appends = false;
  
  /** tsd.storage.repair_appends */
  private boolean repair_appends = false;
  
  /** tsd.core.meta.enable_realtime_ts */
  private boolean enable_realtime_ts = false;
  
  /** tsd.core.meta.enable_realtime_uid */
  private boolean enable_realtime_uid = false;
  
  /** tsd.core.meta.enable_tsuid_incrementing */
  private boolean enable_tsuid_incrementing = false;
  
  /** tsd.core.meta.enable_tsuid_tracking */
  private boolean enable_tsuid_tracking = false;
  
  /** tsd.http.request.enable_chunked */
  private boolean enable_chunked_requests = false;
  
  /** tsd.storage.fix_duplicates */
  private boolean fix_duplicates = false;

  /** tsd.http.request.max_chunk */
  private int max_chunked_requests = 4096; 
  
  /** tsd.core.tree.enable_processing */
  private boolean enable_tree_processing = false;

  /** tsd.storage.hbase.scanner.maxNumRows */
  private int scanner_max_num_rows = 128;
  
  /**
   * The list of properties configured to their defaults or modified by users
   */
  protected final HashMap<String, String> properties = 
    new HashMap<String, String>();

  /** Holds default values for the config */
  protected static final HashMap<String, String> default_map = 
    new HashMap<String, String>();
  
  /** Tracks the location of the file that was actually loaded */
  protected String config_location;

  /**
   * Constructor that initializes default configuration values. May attempt to
   * search for a config file if configured.
   * @param auto_load_config When set to true, attempts to search for a config
   *          file in the default locations
   * @throws IOException Thrown if unable to read or parse one of the default
   *           config files
   */
  public Config(final boolean auto_load_config) throws IOException {
    if (auto_load_config) {
      loadConfig();
    }
    setDefaults();
  }

  /**
   * Constructor that initializes default values and attempts to load the given
   * properties file
   * @param file Path to the file to load
   * @throws IOException Thrown if unable to read or parse the file
   */
  public Config(final String file) throws IOException {
    loadConfig(file);
    setDefaults();
  }

  /**
   * Constructor for plugins or overloaders who want a copy of the parent
   * properties but without the ability to modify them
   * 
   * This constructor will not re-read the file, but it will copy the location
   * so if a child wants to reload the properties periodically, they may do so
   * @param parent Parent configuration object to load from
   */
  public Config(final Config parent) {
    // copy so changes to the local props by the plugin don't affect the master
    properties.putAll(parent.properties);
    config_location = parent.config_location;
    setDefaults();
  }

  /**
   * Creates a new empty Config
   */
  public Config() {
    
  }
  
  /** @return The file that generated this config. May be null */
  public String configLocation() {
    return config_location;
  }
  
  /** @return the auto_metric value */
  public boolean auto_metric() {
    return auto_metric;
  }
  
  /** @return the auto_tagk value */
  public boolean auto_tagk() {
    return auto_tagk;
  }
  
  /** @return the auto_tagv value */
  public boolean auto_tagv() {
    return auto_tagv;
  }
  
  /** @param auto_metric whether or not to auto create metrics */
  public void setAutoMetric(boolean auto_metric) {
    this.auto_metric = auto_metric;
    properties.put("tsd.core.auto_create_metrics", 
        Boolean.toString(auto_metric));
  }
  
  /** @return the enable_compaction value */
  public boolean enable_compactions() {
    return enable_compactions;
  }
  
  /** @return whether or not to write data in the append format */
  public boolean enable_appends() {
    return enable_appends;
  }
  
  /** @return whether or not to re-write appends with duplicates or out of order
   * data when queried. */
  public boolean repair_appends() {
    return repair_appends;
  }
  
  /** @return whether or not to record new TSMeta objects in real time */
  public boolean enable_realtime_ts() { 
    return enable_realtime_ts;
  }
  
  /** @return whether or not record new UIDMeta objects in real time */
  public boolean enable_realtime_uid() { 
    return enable_realtime_uid;
  }
  
  /** @return whether or not to increment TSUID counters */
  public boolean enable_tsuid_incrementing() { 
    return enable_tsuid_incrementing;
  }
  
  /** @return whether or not to record a 1 for every TSUID */
  public boolean enable_tsuid_tracking() {
    return enable_tsuid_tracking;
  }

  /** @return maximum number of rows to be fetched per round trip while scanning HBase */
  public int scanner_maxNumRows() {
    return scanner_max_num_rows;
  }
  
  /** @return whether or not chunked requests are supported */
  public boolean enable_chunked_requests() {
    return enable_chunked_requests;
  }
  
  /** @return max incoming chunk size in bytes */
  public int max_chunked_requests() {
    return max_chunked_requests;
  }
  
  /** @return true if duplicate values should be fixed */
  public boolean fix_duplicates() {
    return fix_duplicates;
  }

  /** @param fix_duplicates true if duplicate values should be fixed */
  public void setFixDuplicates(final boolean fix_duplicates) {
    this.fix_duplicates = fix_duplicates;
  }

  /** @return whether or not to process new or updated TSMetas through trees */
  public boolean enable_tree_processing() {
    return enable_tree_processing;
  }
  
  /**
   * Allows for modifying properties after creation or loading.
   * 
   * WARNING: This should only be used on initialization and is meant for 
   * command line overrides. Also note that it will reset all static config 
   * variables when called.
   * 
   * @param property The name of the property to override
   * @param value The value to store
   */
  public void overrideConfig(final String property, final String value) {
    properties.put(property, value);
    loadStaticVariables();
  }

  /**
   * Returns the given property as a String
   * @param property The property to load
   * @return The property value as a string
   * @throws NullPointerException if the property did not exist
   */
  public final String getString(final String property) {
    return properties.get(property);
  }

  /**
   * Returns the given property as an integer
   * @param property The property to load
   * @return A parsed integer or an exception if the value could not be parsed
   * @throws NumberFormatException if the property could not be parsed
   * @throws NullPointerException if the property did not exist
   */
  public final int getInt(final String property) {
    return Integer.parseInt(sanitize(properties.get(property)));
  }

  /**
   * Returns the given string trimed or null if is null 
   * @param string The string be trimmed of 
   * @return The string trimed or null
  */  
  private final String sanitize(final String string) {
    if (string == null) {
      return null;
    }

    return string.trim();
  }

  /**
   * Returns the given property as a short
   * @param property The property to load
   * @return A parsed short or an exception if the value could not be parsed
   * @throws NumberFormatException if the property could not be parsed
   * @throws NullPointerException if the property did not exist
   */
  public final short getShort(final String property) {
    return Short.parseShort(sanitize(properties.get(property)));
  }

  /**
   * Returns the given property as a long
   * @param property The property to load
   * @return A parsed long or an exception if the value could not be parsed
   * @throws NumberFormatException if the property could not be parsed
   * @throws NullPointerException if the property did not exist
   */
  public final long getLong(final String property) {
    return Long.parseLong(sanitize(properties.get(property)));
  }

  /**
   * Returns the given property as a float
   * @param property The property to load
   * @return A parsed float or an exception if the value could not be parsed
   * @throws NumberFormatException if the property could not be parsed
   * @throws NullPointerException if the property did not exist
   */
  public final float getFloat(final String property) {
    return Float.parseFloat(sanitize(properties.get(property)));
  }

  /**
   * Returns the given property as a double
   * @param property The property to load
   * @return A parsed double or an exception if the value could not be parsed
   * @throws NumberFormatException if the property could not be parsed
   * @throws NullPointerException if the property did not exist
   */
  public final double getDouble(final String property) {
    return Double.parseDouble(sanitize(properties.get(property)));
  }

  /**
   * Returns the given property as a boolean
   * 
   * Property values are case insensitive and the following values will result
   * in a True return value: - 1 - True - Yes
   * 
   * Any other values, including an empty string, will result in a False
   * 
   * @param property The property to load
   * @return A parsed boolean
   * @throws NullPointerException if the property was not found
   */
  public final boolean getBoolean(final String property) {
    final String val = properties.get(property).trim().toUpperCase();
    if (val.equals("1"))
      return true;
    if (val.equals("TRUE"))
      return true;
    if (val.equals("YES"))
      return true;
    return false;
  }

  /**
   * Returns the directory name, making sure the end is an OS dependent slash
   * @param property The property to load
   * @return The property value with a forward or back slash appended or null
   * if the property wasn't found or the directory was empty.
   */
  public final String getDirectoryName(final String property) {
    String directory = properties.get(property);
    if (directory == null || directory.isEmpty()){
      return null;
    }
    if (IS_WINDOWS) {
      // Windows swings both ways. If a forward slash was already used, we'll
      // add one at the end if missing. Otherwise use the windows default of \
      if (directory.charAt(directory.length() - 1) == '\\' || 
          directory.charAt(directory.length() - 1) == '/') {
        return directory;
      }
      if (directory.contains("/")) {
        return directory + "/";
      }
      return directory + "\\";
    }
    if (directory.contains("\\")) {
      throw new IllegalArgumentException(
          "Unix path names cannot contain a back slash");
    }
    
    if (directory == null || directory.isEmpty()){
    	return null;
    }
    
    if (directory.charAt(directory.length() - 1) == '/') {
      return directory;
    }
    return directory + "/";
  }
  
  /**
   * Determines if the given propery is in the map
   * @param property The property to search for
   * @return True if the property exists and has a value, not an empty string
   */
  public final boolean hasProperty(final String property) {
    final String val = properties.get(property);
    if (val == null)
      return false;
    if (val.isEmpty())
      return false;
    return true;
  }

  /**
   * Returns a simple string with the configured properties for debugging
   * @return A string with information about the config
   */
  public final String dumpConfiguration() {
    if (properties.isEmpty())
      return "No configuration settings stored";

    StringBuilder response = new StringBuilder("TSD Configuration:\n");
    response.append("File [" + config_location + "]\n");
    int line = 0;
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      if (line > 0) {
        response.append("\n");
      }
      response.append("Key [" + entry.getKey() + "]  Value [");
      if (entry.getKey().toUpperCase().contains("PASS")) {
         response.append("********");
      } else {
        response.append(entry.getValue());
      }
      response.append("]");
      line++;
    }
    return response.toString();
  }

  /** @return An immutable copy of the configuration map */
  public final Map<String, String> getMap() {
    return ImmutableMap.copyOf(properties);
  }

  /**
   * set enable_compactions to true
   */
  public final void enableCompactions() {
    this.enable_compactions = true;
  }

  /**
   * set enable_compactions to false
   */
  public final void disableCompactions() {
    this.enable_compactions = false;
  }
  
  /**
   * Loads default entries that were not provided by a file or command line
   * 
   * This should be called in the constructor
   */
  protected void setDefaults() {
    // map.put("tsd.network.port", ""); // does not have a default, required
    // map.put("tsd.http.cachedir", ""); // does not have a default, required
    // map.put("tsd.http.staticroot", ""); // does not have a default, required
    default_map.put("tsd.mode", "rw");
    default_map.put("tsd.no_diediedie", "false");
    default_map.put("tsd.network.bind", "0.0.0.0");
    default_map.put("tsd.network.worker_threads", "");
    default_map.put("tsd.network.async_io", "true");
    default_map.put("tsd.network.tcp_no_delay", "true");
    default_map.put("tsd.network.keep_alive", "true");
    default_map.put("tsd.network.reuse_address", "true");
    default_map.put("tsd.core.agg_tag", "_aggregate");
    default_map.put("tsd.core.auto_create_metrics", "false");
    default_map.put("tsd.core.auto_create_tagks", "true");
    default_map.put("tsd.core.auto_create_tagvs", "true");
    default_map.put("tsd.core.connections.limit", "0");
    default_map.put("tsd.core.enable_api", "true");
    default_map.put("tsd.core.enable_ui", "true");
    default_map.put("tsd.core.meta.enable_realtime_ts", "false");
    default_map.put("tsd.core.meta.enable_realtime_uid", "false");
    default_map.put("tsd.core.meta.enable_tsuid_incrementing", "false");
    default_map.put("tsd.core.meta.enable_tsuid_tracking", "false");
    default_map.put("tsd.core.meta.cache.enable", "false");
    default_map.put("tsd.core.plugin_path", "");
    default_map.put("tsd.core.socket.timeout", "0");
    default_map.put("tsd.core.tree.enable_processing", "false");
    default_map.put("tsd.core.preload_uid_cache", "false");
    default_map.put("tsd.core.preload_uid_cache.max_entries", "300000");
    default_map.put("tsd.core.storage_exception_handler.enable", "false");
    default_map.put("tsd.core.uid.random_metrics", "false");
    default_map.put("tsd.core.bulk.allow_out_of_order_timestamps", "false");
    default_map.put("tsd.query.filter.expansion_limit", "4096");
    default_map.put("tsd.query.skip_unresolved_tagvs", "false");
    default_map.put("tsd.query.allow_simultaneous_duplicates", "true");
    default_map.put("tsd.query.enable_fuzzy_filter", "true");
    default_map.put("tsd.rpc.telnet.return_errors", "true");
    // Rollup related settings
    default_map.put("tsd.rollups.enable", "false");
    default_map.put("tsd.rollups.tag_raw", "false");
    default_map.put("tsd.rollups.agg_tag_key", "_aggregate");
    default_map.put("tsd.rollups.raw_agg_tag_value", "RAW");
    default_map.put("tsd.rollups.block_derived", "true");
    default_map.put("tsd.rtpublisher.enable", "false");
    default_map.put("tsd.rtpublisher.plugin", "");
    default_map.put("tsd.search.enable", "false");
    default_map.put("tsd.search.plugin", "");
    default_map.put("tsd.stats.canonical", "false");
    default_map.put("tsd.startup.enable", "false");
    default_map.put("tsd.startup.plugin", "");
    default_map.put("tsd.storage.hbase.scanner.maxNumRows", "128");
    default_map.put("tsd.storage.fix_duplicates", "false");
    default_map.put("tsd.storage.flush_interval", "1000");
    default_map.put("tsd.storage.hbase.data_table", "tsdb");
    default_map.put("tsd.storage.hbase.uid_table", "tsdb-uid");
    default_map.put("tsd.storage.hbase.tree_table", "tsdb-tree");
    default_map.put("tsd.storage.hbase.meta_table", "tsdb-meta");
    default_map.put("tsd.storage.hbase.zk_quorum", "localhost");
    default_map.put("tsd.storage.hbase.zk_basedir", "/hbase");
    default_map.put("tsd.storage.hbase.prefetch_meta", "false");
    default_map.put("tsd.storage.enable_appends", "false");
    default_map.put("tsd.storage.repair_appends", "false");
    default_map.put("tsd.storage.enable_compaction", "true");
    default_map.put("tsd.storage.compaction.flush_interval", "10");
    default_map.put("tsd.storage.compaction.min_flush_threshold", "100");
    default_map.put("tsd.storage.compaction.max_concurrent_flushes", "10000");
    default_map.put("tsd.storage.compaction.flush_speed", "2");
    default_map.put("tsd.timeseriesfilter.enable", "false");
    default_map.put("tsd.uidfilter.enable", "false");
    default_map.put("tsd.core.stats_with_port", "false");    
    default_map.put("tsd.http.show_stack_trace", "true");
    default_map.put("tsd.http.query.allow_delete", "false");
    default_map.put("tsd.http.request.enable_chunked", "false");
    default_map.put("tsd.http.request.max_chunk", "4096");
    default_map.put("tsd.http.request.cors_domains", "");
    default_map.put("tsd.http.request.cors_headers", "Authorization, "
      + "Content-Type, Accept, Origin, User-Agent, DNT, Cache-Control, "
      + "X-Mx-ReqToken, Keep-Alive, X-Requested-With, If-Modified-Since");
    default_map.put("tsd.query.timeout", "0");

    for (Map.Entry<String, String> entry : default_map.entrySet()) {
      if (!properties.containsKey(entry.getKey()))
        properties.put(entry.getKey(), entry.getValue());
    }

    loadStaticVariables();
  }

  /**
   * Searches a list of locations for a valid opentsdb.conf file
   * 
   * The config file must be a standard JAVA properties formatted file. If none
   * of the locations have a config file, then the defaults or command line
   * arguments will be used for the configuration
   * 
   * Defaults for Linux based systems are: ./opentsdb.conf /etc/opentsdb.conf
   * /etc/opentsdb/opentdsb.conf /opt/opentsdb/opentsdb.conf
   * 
   * @throws IOException Thrown if there was an issue reading a file
   */
  protected void loadConfig() throws IOException {
    if (config_location != null && !config_location.isEmpty()) {
      loadConfig(config_location);
      return;
    }

    final ArrayList<String> file_locations = new ArrayList<String>();

    // search locally first
    file_locations.add("opentsdb.conf");

    // add default locations based on OS
    if (System.getProperty("os.name").toUpperCase().contains("WINDOWS")) {
      file_locations.add("C:\\Program Files\\opentsdb\\opentsdb.conf");
      file_locations.add("C:\\Program Files (x86)\\opentsdb\\opentsdb.conf");
    } else {
      file_locations.add("/etc/opentsdb.conf");
      file_locations.add("/etc/opentsdb/opentsdb.conf");
      file_locations.add("/opt/opentsdb/opentsdb.conf");
    }

    for (String file : file_locations) {
      try {
        FileInputStream file_stream = new FileInputStream(file);
        Properties props = new Properties();
        props.load(file_stream);
        
        // load the hash map
        loadHashMap(props);        
      } catch (Exception e) {
        // don't do anything, the file may be missing and that's fine
        LOG.debug("Unable to find or load " + file, e);
        continue;
      }

      // no exceptions thrown, so save the valid path and exit
      LOG.info("Successfully loaded configuration file: " + file);
      config_location = file;
      return;
    }

    LOG.info("No configuration found, will use defaults");
  }

  /**
   * Attempts to load the configuration from the given location
   * @param file Path to the file to load
   * @throws IOException Thrown if there was an issue reading the file
   * @throws FileNotFoundException Thrown if the config file was not found
   */
  protected void loadConfig(final String file) throws FileNotFoundException,
      IOException {
    final FileInputStream file_stream = new FileInputStream(file);
    try {
      final Properties props = new Properties();
      props.load(file_stream);
  
      // load the hash map
      loadHashMap(props);
  
      // no exceptions thrown, so save the valid path and exit
      LOG.info("Successfully loaded configuration file: " + file);
      config_location = file;
    } finally {
      file_stream.close();
    }
  }

  /**
   * Loads the static class variables for values that are called often. This
   * should be called any time the configuration changes.
   */
  public void loadStaticVariables() {
    auto_metric = this.getBoolean("tsd.core.auto_create_metrics");
    auto_tagk = this.getBoolean("tsd.core.auto_create_tagks");
    auto_tagv = this.getBoolean("tsd.core.auto_create_tagvs");
    enable_compactions = this.getBoolean("tsd.storage.enable_compaction");
    enable_appends = this.getBoolean("tsd.storage.enable_appends");
    repair_appends = this.getBoolean("tsd.storage.repair_appends");
    enable_chunked_requests = this.getBoolean("tsd.http.request.enable_chunked");
    enable_realtime_ts = this.getBoolean("tsd.core.meta.enable_realtime_ts");
    enable_realtime_uid = this.getBoolean("tsd.core.meta.enable_realtime_uid");
    enable_tsuid_incrementing = 
      this.getBoolean("tsd.core.meta.enable_tsuid_incrementing");
    enable_tsuid_tracking = 
      this.getBoolean("tsd.core.meta.enable_tsuid_tracking");
    if (this.hasProperty("tsd.http.request.max_chunk")) {
      max_chunked_requests = this.getInt("tsd.http.request.max_chunk");
    }
    enable_tree_processing = this.getBoolean("tsd.core.tree.enable_processing");
    fix_duplicates = this.getBoolean("tsd.storage.fix_duplicates");
    scanner_max_num_rows = this.getInt("tsd.storage.hbase.scanner.maxNumRows");
  }
  
  /**
   * Called from {@link #loadConfig} to copy the properties into the hash map
   * Tsuna points out that the Properties class is much slower than a hash
   * map so if we'll be looking up config values more than once, a hash map
   * is the way to go 
   * @param props The loaded Properties object to copy
   */
  private void loadHashMap(final Properties props) {
    properties.clear();
    
    @SuppressWarnings("rawtypes")
    Enumeration e = props.propertyNames();
    while (e.hasMoreElements()) {
      String key = (String) e.nextElement();
      properties.put(key, props.getProperty(key));
    }
  }
}
