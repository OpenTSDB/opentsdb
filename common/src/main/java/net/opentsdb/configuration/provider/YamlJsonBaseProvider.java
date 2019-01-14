// This file is part of OpenTSDB.
// Copyright (C) 2019  The OpenTSDB Authors.
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

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import net.opentsdb.configuration.Configuration;
import net.opentsdb.configuration.ConfigurationOverride;
import net.opentsdb.utils.DateTime;

/**
 * The base class for a YAML or JSON formatted config object.
 * 
 * @since 3.0
 */
public abstract class YamlJsonBaseProvider implements Provider {
  private static final Logger LOG = LoggerFactory.getLogger(
      YamlJsonBaseProvider.class);

  /** The factory that instantiated this provider. */
  protected final ProviderFactory factory;
  
  /** The configuration object this provider is associated with. */
  protected final Configuration config;
  
  /** A timer from the config used to reload the config. */
  protected final HashedWheelTimer timer;
  
  /** The last reload time in ms. May be 0 for non-reloadable providers. */
  protected long last_reload;
  
  /** The cache of entries last loaded. */
  protected final Map<String, Object> cache;
  
  /** The URI. */
  protected final String uri;
  
  /** A hash to compare against. */
  protected long last_hash;
  
  /**
   * Default ctor.
   * @param factory A non-null provider factory.
   * @param config A non-null config object we belong to.
   * @param timer A non-null timer object.
   * s@param uri The URI to parse out. A file in this case.
   * @throws IllegalArgumentException if a required parameter is missing.
   */
  public YamlJsonBaseProvider(final ProviderFactory factory, 
                              final Configuration config, 
                              final HashedWheelTimer timer,
                              final String uri) {
    if (factory == null) {
      throw new IllegalArgumentException("Factory cannot be null.");
    }
    if (config == null) {
      throw new IllegalArgumentException("Config cannot be null.");
    }
    if (timer == null) {
      throw new IllegalArgumentException("Timer cannot be null.");
    }
    this.factory = factory;
    this.config = config;
    this.timer = timer;
    this.uri = uri;
    
    cache = Maps.newConcurrentMap();
  }
  
  /**
   * Called by the {@link Configuration} class to load the current value for the
   * given key when a schema is registered via 
   * {@link Configuration#register(net.opentsdb.configuration.ConfigurationEntrySchema)}.
   * @param key A non-null and non-empty key.
   * @return A configuration override if the provider had data for the 
   * key (even if it was null) or null if the provider did not have any
   * data.
   */
  public ConfigurationOverride getSetting(final String key) {
    Object value = cache.get(key);
    if (value == null) {
      final String[] path = key.split("\\.");
      if (path.length < 2) {
        return null;
      }
      
      // recursive search
      value = cache.get(path[0]);
      if (value == null || !(value instanceof JsonNode)) {
        return null;
      }
      recursiveSet(key, path, 1, (JsonNode) value);
      value = cache.get(key);
      if (value == null) {
        return null;
      }
    }
    
    return ConfigurationOverride.newBuilder()
        .setSource(uri)
        .setValue(value)
        .build();
  }
  
  /**
   * The name of this provider.
   * @return A non-null string.
   */
  public String source() {
    return uri;
  }

  /**
   * Called by the {@link Configuration} timer to refresh it's values.
   * Only called if {@link ProviderFactory#isReloadable()} returned true.
   */
  public abstract void reload();
  
  /**
   * A millisecond Unix epoch timestamp when the config was last reloaded.
   * Starts at 0 and is 0 always for non-reloadable providers.
   * @return An integer from 0 to max int.
   */
  public long lastReload() {
    return last_reload;
  }
  
  /**
   * The factory this provider was instantiated from.
   * @return A non-null factory.
   */
  public ProviderFactory factory() {
    return factory;
  }
  
  /**
   * The code that runs in the timer to execute {@link #reload()}.
   */
  @Override
  public void run(final Timeout ignored) throws Exception {
    last_reload = DateTime.currentTimeMillis();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Starting config reload for provider: " + this);
    }
    try {
      reload();
    } catch (Exception e) {
      LOG.error("Failed to run the reload task.", e);
    }
    
    final long interval = config.getTyped(
        Configuration.CONFIG_RELOAD_INTERVAL_KEY, long.class);
    final long next_run = interval - 
        ((DateTime.currentTimeMillis() - last_reload) / 1000);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Reload completed in " 
          + (((double) DateTime.currentTimeMillis() - 
              (double)last_reload) / (double) 1000) 
          + "ms. Scheduling reload for provider [" + this + "] in " 
          + next_run + " seconds");
    }
    timer.newTimeout(this, next_run, TimeUnit.SECONDS);
  }
  
  /**
   * Helper to call binders.
   * @param key The non-null key to pass to the bind function.
   * @param value The value to return. May be null.
   */
  void notifyBinds(final String key, final Object value) {
    if (config.reloadableKeys().contains(key)) {
      try {
        config.addOverride(key,
            ConfigurationOverride.newBuilder()
              .setSource(uri)
              .setValue(value)
              .build());
      } catch (Exception e) {
        LOG.warn("Failed to store key [" + key  + "] from: " 
            + uri, e);
      }
    }
  }
  
  /**
   * Recursively attempts to find a path through objects to match the key.
   * Just stores the result in the map, so try fetching after calling.
   * @param key The non-null original key with dots.
   * @param path The parsed path, split on dots.
   * @param idx The current depth.
   * @param node The current node to process.
   */
  void recursiveSet(final String key, 
                    final String[] path, 
                    final int idx, 
                    final JsonNode node) {
    if (idx >= path.length) {
      // successful match!
      switch (node.getNodeType()) {
      case STRING:
        cache.put(key, node.asText());
        notifyBinds(key, node.asText());
        break;
      case BOOLEAN:
        cache.put(key, node.asBoolean());
        notifyBinds(key, node.asBoolean());
        break;
      case NULL:
        cache.put(key, null);
        notifyBinds(key, null);
        break;
      case NUMBER:
        if (node.asText().contains(".")) {
          cache.put(key, node.asDouble());
          notifyBinds(key, node.asDouble());
        } else {
          cache.put(key, node.asLong());
          notifyBinds(key, node.asLong());
        }
        break;
      default:
        cache.put(key, node);
        notifyBinds(key, node);
      }
      return;
    }
    
    if (node.getNodeType() == JsonNodeType.OBJECT) {
      final JsonNode child = node.get(path[idx]);
      if (child == null || child.isNull()) {
        return;
      }
      
      recursiveSet(key, path, idx + 1, child);
    }
  }

  /**
   * Recursively attempts to find a path through objects to match the key.
   * If the path failed to match, the cached entry is purged and callbacks
   * notified. If it does match, the map is updated and callbacks are
   * notified.
   * @param key The non-null original key with dots.
   * @param path The parsed path, split on dots.
   * @param idx The current depth.
   * @param node The current node to process.
   */
  void recursiveUpdate(final String key, 
                       final String[] path, 
                       final int idx, 
                       final JsonNode node) {
    if (idx >= path.length) {
      // successful match!
      switch (node.getNodeType()) {
      case STRING:
        cache.put(key, node.asText());
        notifyBinds(key, node.asText());
        break;
      case BOOLEAN:
        cache.put(key, node.asBoolean());
        notifyBinds(key, node.asBoolean());
        break;
      case NULL:
        cache.put(key, null);
        notifyBinds(key, null);
        break;
      case NUMBER:
        if (node.asText().contains(".")) {
          cache.put(key, node.asDouble());
          notifyBinds(key, node.asDouble());
        } else {
          cache.put(key, node.asLong());
          notifyBinds(key, node.asLong());
        }
        break;
      default:
        cache.put(key, node);
        notifyBinds(key, node);
      }
      return;
    } else if (node.getNodeType() == JsonNodeType.OBJECT) {
      final JsonNode child = node.get(path[idx]);
      if (child == null || child.isNull()) {
        cache.remove(key);
        notifyBinds(key, null);
        return;
      }
      
      recursiveSet(key, path, idx + 1, child);
    } else {
      cache.remove(key);
      notifyBinds(key, null);
    }
  }

  /**
   * Parses the YAML or JSON.
   * @param stream A non-null stream to work on.
   */
  void parse(final InputStream stream) {
    try {
      JsonNode node = Configuration.OBJECT_MAPPER.readTree(stream);
      if (node == null) {
        LOG.error("The uri was empty: " + uri + ". Skipping "
            + "loading.");
        return;
      }
      
      if (!node.isObject()) {
        LOG.error("The file must contain an object/map of key values: " 
            + uri + ". Skipping loading. Type: " + node.getNodeType());
        return;
      }
      
      final Iterator<Entry<String, JsonNode>> iterator = node.fields();
      final Set<String> keys = Sets.newHashSet();
      while (iterator.hasNext()) {
        final Entry<String, JsonNode> entry = iterator.next();
  
        switch (entry.getValue().getNodeType()) {
        case STRING:
          cache.put(entry.getKey(), entry.getValue().asText());
          notifyBinds(entry.getKey(), entry.getValue().asText());
          break;
        case BOOLEAN:
          cache.put(entry.getKey(), entry.getValue().asBoolean());
          notifyBinds(entry.getKey(), entry.getValue().asBoolean());
          break;
        case NULL:
          // can't store nulls in the map
          continue;
        case NUMBER:
          if (entry.getValue().asText().contains(".")) {
            cache.put(entry.getKey(), entry.getValue().asDouble());
            notifyBinds(entry.getKey(), entry.getValue().asDouble());
          } else {
            cache.put(entry.getKey(), entry.getValue().asLong());
            notifyBinds(entry.getKey(), entry.getValue().asLong());
          }
          break;
        default:
          cache.put(entry.getKey(), entry.getValue());
          notifyBinds(entry.getKey(), entry.getValue());
        }
        
        keys.add(entry.getKey());
      }
      
      for (final String key : cache.keySet()) {
        if (!keys.contains(key)) {
          // double check it's not a nested object that we followed down. 
          // If it IS then we need to update or delete the entry.
          final String[] path = key.split("\\.");
          if (path.length >= 1) {
            if (keys.contains(path[0])) {
              recursiveUpdate(key, path, 1, node.get(path[0]));
              continue;
            }
          }
          
          cache.remove(key);
          notifyBinds(key, null);
        }
      }
    } catch (IOException e) {
      LOG.error("Failed to open or parse uri at: " + uri, e);
    }
  }
}
