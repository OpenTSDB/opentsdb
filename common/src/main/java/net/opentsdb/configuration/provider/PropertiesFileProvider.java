// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
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
package net.opentsdb.configuration.provider;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.netty.util.HashedWheelTimer;
import net.opentsdb.configuration.Configuration;
import net.opentsdb.configuration.ConfigurationException;
import net.opentsdb.configuration.ConfigurationOverride;

/**
 * Parses a Java style properties file, i.e. key = value. If a specific
 * file is not given (meaning the default ctor is called) then we will
 * search for some files as older versions of OpenTSDB did.
 * <p>
 * The order for Linux systems is:
 * <ol><li>opentsdb.conf</li>
 * <li>/etc/opentsdb.conf</li>
 * <li>/etc/opentsdb/opentsdb.conf</li>
 * <li>/opt/opentsdb/opentsdb.conf<li>
 * </ol>
 * The order for Windows is:
 * <ol><li>opentsdb.conf</li>
 * <li>C:\Program Files\opentsdb\opentsdb.conf</li>
 * <li>C:\Program Files (x86)\opentsdb\opentsdb.conf</li>
 * </ol>
 * 
 * @since 3.0
 */
public class PropertiesFileProvider extends Provider {
  private static final Logger LOG = LoggerFactory.getLogger(PropertiesFileProvider.class);
  
  /** The file name. */
  private final String file_name;
  
  /** The cache of entries last loaded. */
  private final Map<String, String> cache;
  
  /**
   * Default ctor that attempts to load the old configs from TSDB 2x.
   * @param factory A non-null provider factory.
   * @param config A non-null config object we belong to.
   * @param timer A non-null timer object.
   * @param reload_keys A non-null (possibly empty) set of keys to reload.
   * @throws IllegalArgumentException if a required parameter is missing.
   */
  public PropertiesFileProvider(final ProviderFactory factory, 
                        final Configuration config, 
                        final HashedWheelTimer timer,
                        final Set<String> reload_keys) {
    this(factory, config, timer, reload_keys, null);
  }
  
  /**
   * Ctor used by the factory to load a specific file. If a user provided
   * file name is given, the file will attempt reloads at each interval.
   * @param file_name A file name. If null or empty it will look for defaults.
   * @throws ConfigurationException if the default file name couldn't be found.
   */
  public PropertiesFileProvider(final ProviderFactory factory, 
                        final Configuration config, 
                        final HashedWheelTimer timer,
                        final Set<String> reload_keys,
                        final String file_name) {
    super(factory, config, timer, reload_keys);
    if (Strings.isNullOrEmpty(file_name)) {
      this.file_name = findDefault();
      if (Strings.isNullOrEmpty(this.file_name)) {
        throw new ConfigurationException("No default file name was found.");
      }
    } else {
      int idx = file_name.toLowerCase().indexOf("file://");
      if (idx < 0) {
        throw new IllegalArgumentException("File name did not start "
            + "with `file://`: " + file_name);
      }
      this.file_name = file_name.substring(7);
    }
    
    cache = Maps.newConcurrentMap();
    
    try {
      reload();
    } catch (Exception e) {
      LOG.error("Failed to load config file: " + this.file_name, e);
    }
  }
  
  @Override
  public ConfigurationOverride getSetting(final String key) {
    final String value = cache.get(key);
    if (value == null) {
      return null;
    }
    return ConfigurationOverride.newBuilder()
        .setSource(file_name)
        .setValue(value)
        .build();
  }

  @Override
  public String source() {
    return file_name;
  }

  @Override
  public void close() throws IOException {
    // no-op
  }

  @Override
  public void reload() {
    try (final FileInputStream file_stream = new FileInputStream(file_name)) {
      final Properties properties = new Properties();
      properties.load(file_stream);
      
      final Set<Entry<Object, Object>> entries = properties.entrySet();
      final Set<String> new_keys = Sets.newHashSetWithExpectedSize(entries.size());
      
      if (!entries.isEmpty()) {
        for (final Entry<Object, Object> entry : entries) {
          cache.put((String) entry.getKey(), (String) entry.getValue());
          new_keys.add((String) entry.getKey());
          
          if (reload_keys != null && 
              reload_keys.contains((String) entry.getKey())) {
            try {
              config.addOverride((String) entry.getKey(),
                  ConfigurationOverride.newBuilder()
                    .setSource(file_name)
                    .setValue(entry.getValue())
                    .build());
            } catch (Exception e) {
              LOG.warn("Failed to store key [" + entry.getKey() 
                + "] from file: " + file_name, e);
            }
          }
        }
      }
      
      for (final String key : cache.keySet()) {
        if (!new_keys.contains(key)) {
          cache.remove(key);
        }
      }
      
      if (LOG.isDebugEnabled()) {
        LOG.debug("Successfully parsed file: " + file_name 
            + " with " + properties.size() + " entries");
      }
    } catch (FileNotFoundException e) {
      LOG.warn("Configuration file [" + file_name + "] did not exist. "
          + "Will retry.");
    } catch (IOException e) {
      LOG.error("Failed to read file: " + file_name, e);
    }
  }
  
  /**
   * Helper to find the default config file.
   * @return Null if not found, a string if exists.
   */
  private String findDefault() {
    final List<String> file_locations = Lists.newArrayListWithCapacity(4);
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
    
    for (final String file : file_locations) {
      if (new File(file).exists()) {
        return file;
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("No configuration file found at: " + file);
      }
    }
    return null;
  }
  
  @VisibleForTesting
  Map<String, String> cache() {
    return cache;
  }
  
}
