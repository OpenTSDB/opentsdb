// This file is part of OpenTSDB.
// Copyright (C) 2016  The OpenTSDB Authors.
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
package net.opentsdb.query.execution;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import net.opentsdb.core.Const;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.JSON;

/**
 * Handles loading Http endpoints from a config file and returns the proper
 * endpoints when a query is instantiated.
 * Each endpoint is a string name with one or more Http addresses with optional
 * ports. E.g. {@code http://somehost:4242}.
 * 
 * @since 3.0
 */
public class HttpEndpoints implements TimerTask {
  private static final Logger LOG = LoggerFactory.getLogger(HttpEndpoints.class);
  
  /** How often, in milliseconds, to check the file for updates. */
  public static final int DEFAULT_LOAD_INTERVAL = 6000;
  
  /** The default endpoints collection name. */
  public static final String DEFAULT_KEY = "DEFAULT";
  
  /** A typeref used for deserializing the JSON. */
  public static TypeReference<Map<String, List<String>>> TR_ENDPOINTS = 
      new TypeReference<Map<String, List<String>>>() { };
  
  /** The Timer to use. */
  private final Timer timer;
  
  /** The map of endpoings */
  private final ConcurrentMap<String, List<String>> endpoints;
  
  /** The configured reload interval. */
  private final int load_interval;
  
  /** The configured file location. */
  private final String file_location;
  
  /** The timeout to return for each endpoint. TODO - this is a temp hack. */
  private final long cluster_timeout;
  
  /** The last load hash so we know if we have changes. */
  private int last_hash = 0;
  
  /**
   * Default ctor that initializes loading and adds a reload task to the timer.
   * @param config A non-null config to pull info from.
   * @param timer A non-null timer to use for reloading the config.
   * @throws IllegalArgumentException if the file location is empty, i.e. the
   * tsd.query.http.endpoints.config property.
   */
  public HttpEndpoints(final Config config, final Timer timer) {
    this.timer = timer;
    endpoints = new ConcurrentHashMap<String, List<String>>();
    
    if (!config.hasProperty("tsd.query.http.endpoints.load_interval")) {
      load_interval = DEFAULT_LOAD_INTERVAL;
    } else {
      load_interval = config.getInt("tsd.query.http.endpoints.load_interval");
    }
    file_location = config.getString("tsd.query.http.endpoints.config");
    if (file_location == null) {
      throw new IllegalArgumentException("The config tsd.query.http."
          + "endpoints.config cannot be empty.");
    }
    if (config.hasProperty("tsd.query.http.endpoints.timeout")) {
      cluster_timeout = config.getLong("tsd.query.http.endpoints.timeout");
    } else {
      cluster_timeout = 120000;
    }
    try {
      run(null);
    } catch (Exception e) {
      LOG.error("Failed to execute initial run of the HttpEndpoints loader");
    }
  }

  @Override
  public void run(final Timeout ignored) throws Exception {
    try {
      final File file = new File(file_location);
      if (!file.exists()) {
        LOG.warn("Http Endpoints config file " + file_location 
            + " does not exist");
        return;
      }
      final String raw_json = Files.toString(file, Const.UTF8_CHARSET);
      if (raw_json.hashCode() != last_hash) {
        final Map<String, List<String>> map = 
            JSON.parseToObject(raw_json, TR_ENDPOINTS);
        
        // add/replace existing. Simpler that way.
        endpoints.putAll(map);
        final Iterator<Entry<String, List<String>>> it = endpoints.entrySet().iterator();
        while (it.hasNext()) {
          Entry<String, List<String>> entry = it.next();
          if (!map.containsKey(entry.getKey())) {
            it.remove();
          }
        }
        LOG.info("Loaded Http Endpoints map: " + map);
        last_hash = raw_json.hashCode();
      }
    } catch (Exception e) {
      LOG.error("Error while trying to load endpoints config", e);
    } finally {
      timer.newTimeout(this, load_interval, TimeUnit.MILLISECONDS);
    }
  }

  /**
   * Returns a non-null list of endpoints given the ID.
   * If the ID is null, then the default key is looked up.
   * If the ID starts with "http://" or "https://" then it will be split on
   * semicolons and the given list is returned. This is useful for custom host
   * or VIPs. 
   * Otherwise the given ID is looked up in the map and if a list of endpoints
   * is present then the endpoints are returned. Otherwise and exception is thrown
   * if the ID isn't found. 
   * @param id Null for the default list, a configured ID to lookup in the map
   * or a literal semicolon delimited list of endpoints.
   * @return A non-null list of endpoints if found.
   * @throws IllegalArgumentException if the endpoint was not in the map.
   */
  public List<String> getEndpoints(final String id) {
    final List<String> results;
    if (id == null || id.isEmpty()) {
      results = endpoints.get(DEFAULT_KEY);
    } else if (id.toLowerCase().startsWith("http://") || 
               id.toLowerCase().startsWith("https://")) {
      // TODO - maybe some validation on the split urls?
      results = Lists.newArrayList(id.split(";"));
    } else {
      results = endpoints.get(id);
    }
    if (results == null) {
      throw new IllegalArgumentException("No such endpoint collection found");
    }
        
    return results;
  }

  /**
   * Returns a copy of the current map for debugging or printing.
   * @return A copy of the current map.
   */
  public Map<String, List<String>> getEndpoints() {
    final Map<String, List<String>> map = 
        new HashMap<String, List<String>>(endpoints.size());
    for (final Entry<String, List<String>> entry : endpoints.entrySet()) {
      map.put(entry.getKey(), new ArrayList<String>(entry.getValue()));
    }
    return map;
  }
  
  /** @return The temporary cluster timeout. */
  public long clusterTimeout() {
    return cluster_timeout;
  }
  
  @VisibleForTesting
  int getLastHash() {
    return last_hash;
  }
  
}
