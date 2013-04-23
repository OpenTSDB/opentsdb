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

/**
 * OpenTSDB Configuration Class
 * 
 * This handles all of the user configurable variables for a TSD. On
 * initialization default values are configured for all variables. Then
 * implementations should call the {@link loadConfig()} methods to search for a
 * default configuration or try to load one provided by the user.
 * 
 * To add a configuration, simply set a default value in {@link setDefaults).
 * Wherever you need to access the config value, use the proper helper to fetch
 * the value, accounting for exceptions that may be thrown if necessary.
 * 
 * The get<type> number helpers will return NumberFormatExceptions if the
 * requested property is null or unparseable. The {@link getString()} helper
 * will return a NullPointerException if the property isn't found.
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

  // These are accessed often so need a set address for fast access (faster
  // than accessing the map. Their value will be changed when the config is 
  // loaded
  // NOTE: edit the setDefaults() method if you add a public field

  /** tsd.core.auto_create_metrics */
  private boolean auto_metric = false;

  /** tsd.storage.enable_compaction */
  private boolean enable_compactions = true;
  
  /** tsd.core.meta.enable_tracking */
  private boolean enable_meta_tracking = false;
  
  /** tsd.http.request.enable_chunked */
  private boolean enable_chunked_requests = false;
  
  /** tsd.http.request.max_chunk */
  private int max_chunked_requests = 4096; 
  
  /**
   * The list of properties configured to their defaults or modified by users
   */
  protected final HashMap<String, String> properties = 
    new HashMap<String, String>();

  /** Holds default values for the config */
  protected static final HashMap<String, String> default_map = 
    new HashMap<String, String>();
  
  /** Tracks the location of the file that was actually loaded */
  private String config_location;

  /**
   * Constructor that initializes default configuration values. May attempt to
   * search for a config file if configured.
   * @param auto_load_config When set to true, attempts to search for a config
   *          file in the default locations
   * @throws IOException Thrown if unable to read or parse one of the default
   *           config files
   */
  public Config(final boolean auto_load_config) throws IOException {
    if (auto_load_config)
      this.loadConfig();
    this.setDefaults();
  }

  /**
   * Constructor that initializes default values and attempts to load the given
   * properties file
   * @param file Path to the file to load
   * @throws IOException Thrown if unable to read or parse the file
   */
  public Config(final String file) throws IOException {
    this.loadConfig(file);
    this.setDefaults();
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
    this.properties.putAll(parent.properties);
    this.config_location = parent.config_location;
    this.setDefaults();
  }

  /** @return the auto_metric value */
  public boolean auto_metric() {
    return this.auto_metric;
  }
  
  /** @return the enable_compaction value */
  public boolean enable_compactions() {
    return this.enable_compactions;
  }
  
  /** @return whether or not to track meta data as new UID/TS are created */
  public boolean enable_meta_tracking() { 
    return enable_meta_tracking;
  }
  
  /** @return whether or not chunked requests are supported */
  public boolean enable_chunked_requests() {
    return this.enable_chunked_requests;
  }
  
  /** @return max incoming chunk size in bytes */
  public int max_chunked_requests() {
    return this.max_chunked_requests;
  }
  
  /**
   * Allows for modifying properties after loading
   * 
   * @warn This should only be used on initialization and is meant for command
   *       line overrides
   * 
   * @param property The name of the property to override
   * @param value The value to store
   */
  public void overrideConfig(final String property, final String value) {
    this.properties.put(property, value);
  }

  /**
   * Returns the given property as a String
   * @param property The property to load
   * @return The property value as a string
   * @throws NullPointerException if the property did not exist
   */
  public final String getString(final String property) {
    return this.properties.get(property);
  }

  /**
   * Returns the given property as an integer
   * @param property The property to load
   * @return A parsed integer or an exception if the value could not be parsed
   * @throws NumberFormatException if the property could not be parsed
   * @throws NullPointerException if the property did not exist
   */
  public final int getInt(final String property) {
    return Integer.parseInt(this.properties.get(property));
  }

  /**
   * Returns the given property as a short
   * @param property The property to load
   * @return A parsed short or an exception if the value could not be parsed
   * @throws NumberFormatException if the property could not be parsed
   * @throws NullPointerException if the property did not exist
   */
  public final short getShort(final String property) {
    return Short.parseShort(this.properties.get(property));
  }

  /**
   * Returns the given property as a long
   * @param property The property to load
   * @return A parsed long or an exception if the value could not be parsed
   * @throws NumberFormatException if the property could not be parsed
   * @throws NullPointerException if the property did not exist
   */
  public final long getLong(final String property) {
    return Long.parseLong(this.properties.get(property));
  }

  /**
   * Returns the given property as a float
   * @param property The property to load
   * @return A parsed float or an exception if the value could not be parsed
   * @throws NumberFormatException if the property could not be parsed
   * @throws NullPointerException if the property did not exist
   */
  public final float getFloat(final String property) {
    return Float.parseFloat(this.properties.get(property));
  }

  /**
   * Returns the given property as a double
   * @param property The property to load
   * @return A parsed double or an exception if the value could not be parsed
   * @throws NumberFormatException if the property could not be parsed
   * @throws NullPointerException if the property did not exist
   */
  public final double getDouble(final String property) {
    return Double.parseDouble(this.properties.get(property));
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
    final String val = this.properties.get(property).toUpperCase();
    if (val.equals("1"))
      return true;
    if (val.equals("TRUE"))
      return true;
    if (val.equals("YES"))
      return true;
    return false;
  }

  /**
   * Determines if the given propery is in the map
   * @param property The property to search for
   * @return True if the property exists and has a value, not an empty string
   */
  public final boolean hasProperty(final String property) {
    final String val = this.properties.get(property);
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
    if (this.properties.isEmpty())
      return "No configuration settings stored";

    StringBuilder response = new StringBuilder("TSD Configuration:\n");
    response.append("File [" + this.config_location + "]\n");
    for (Map.Entry<String, String> entry : this.properties.entrySet()) {
      response.append("Key [" + entry.getKey() + "]  Value [").
          append(entry.getValue() + "]\n");
    }
    return response.toString();
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
    default_map.put("tsd.network.bind", "0.0.0.0");
    default_map.put("tsd.network.worker_threads", "");
    default_map.put("tsd.network.async_io", "true");
    default_map.put("tsd.network.tcp_no_delay", "true");
    default_map.put("tsd.network.keep_alive", "true");
    default_map.put("tsd.network.reuse_address", "true");
    default_map.put("tsd.core.auto_create_metrics", "false");
    default_map.put("tsd.core.meta.enable_tracking", "false");
    default_map.put("tsd.core.plugin_path", "");
    default_map.put("tsd.search.enable", "false");
    default_map.put("tsd.search.plugin", "");
    default_map.put("tsd.storage.flush_interval", "1000");
    default_map.put("tsd.storage.hbase.data_table", "tsdb");
    default_map.put("tsd.storage.hbase.uid_table", "tsdb-uid");
    default_map.put("tsd.storage.hbase.zk_quorum", "localhost");
    default_map.put("tsd.storage.hbase.zk_basedir", "/hbase");
    default_map.put("tsd.storage.enable_compaction", "true");
    default_map.put("tsd.http.show_stack_trace", "true");
    default_map.put("tsd.http.request.enable_chunked", "false");
    default_map.put("tsd.http.request.max_chunk", "4096");

    for (Map.Entry<String, String> entry : default_map.entrySet()) {
      if (!properties.containsKey(entry.getKey()))
        properties.put(entry.getKey(), entry.getValue());
    }

    // set statics
    auto_metric = this.getBoolean("tsd.core.auto_create_metrics");
    enable_compactions = this.getBoolean("tsd.storage.enable_compaction");
    enable_chunked_requests = this.getBoolean("tsd.http.request.enable_chunked");
    enable_meta_tracking = this.getBoolean("tsd.core.meta.enable_tracking");
    if (this.hasProperty("tsd.http.request.max_chunk")) {
      max_chunked_requests = this.getInt("tsd.http.request.max_chunk");
    }
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
    if (this.config_location != null && !this.config_location.isEmpty()) {
      this.loadConfig(this.config_location);
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
        this.loadHashMap(props);        
      } catch (Exception e) {
        // don't do anything, the file may be missing and that's fine
        LOG.debug("Unable to find or load " + file, e);
        continue;
      }

      // no exceptions thrown, so save the valid path and exit
      LOG.info("Successfully loaded configuration file: " + file);
      this.config_location = file;
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
    FileInputStream file_stream;
    file_stream = new FileInputStream(file);
    Properties props = new Properties();
    props.load(file_stream);
    
    // load the hash map
    this.loadHashMap(props);

    // no exceptions thrown, so save the valid path and exit
    LOG.info("Successfully loaded configuration file: " + file);
    this.config_location = file;
  }

  /**
   * Calld from {@link #loadConfig} to copy the properties into the hash map
   * Tsuna points out that the Properties class is much slower than a hash
   * map so if we'll be looking up config values more than once, a hash map
   * is the way to go 
   * @param props The loaded Properties object to copy
   */
  private void loadHashMap(final Properties props) {
    this.properties.clear();
    
    @SuppressWarnings("rawtypes")
    Enumeration e = props.propertyNames();
    while (e.hasMoreElements()) {
      String key = (String) e.nextElement();
      this.properties.put(key, props.getProperty(key));
    }
  }
}
