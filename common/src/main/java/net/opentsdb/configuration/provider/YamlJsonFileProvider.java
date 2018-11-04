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
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

import io.netty.util.HashedWheelTimer;
import net.opentsdb.common.Const;
import net.opentsdb.configuration.Configuration;
import net.opentsdb.configuration.ConfigurationOverride;

/**
 * Handles parsing of JSON or YAML formatted files. Supports dotted paths
 * as long as each sub-path is an object. The root of the path can be
 * any object.
 * Use this class for complex configs that require maps, lists and possibly
 * POJOs in the source.
 * 
 * @since 3.0
 */
public class YamlJsonFileProvider extends BaseProvider {
  private static final Logger LOG = LoggerFactory.getLogger(
      YamlJsonFileProvider.class);
  
  /** The file name. */
  protected final String file_name;
  
  /** The cache of entries last loaded. */
  protected final Map<String, Object> cache;
  
  /** A hash to compare against. */
  protected long last_hash;
  
  /**
   * Default ctor.
   * @param factory A non-null provider factory.
   * @param config A non-null config object we belong to.
   * @param timer A non-null timer object.
   * @param uri The URI to parse out. A file in this case.
   * @throws IllegalArgumentException if a required parameter is missing.
   */
  public YamlJsonFileProvider(final ProviderFactory factory, 
                              final Configuration config,
                              final HashedWheelTimer timer, 
                              final String uri) {
    super(factory, config, timer);
    
    cache = Maps.newConcurrentMap();
    
    int idx = uri.toLowerCase().indexOf(FileFactory.PROTOCOL);
    if (idx < 0) {
      throw new IllegalArgumentException("File name did not start "
          + "with `" + FileFactory.PROTOCOL + "`: " + uri);
    }
    this.file_name = uri.substring(FileFactory.PROTOCOL.length());
    
    try {
      reload();
    } catch (Throwable t) {
      LOG.error("Failed to load config file: " + this.file_name, t);
    }
  }

  @Override
  public void close() throws IOException {
    // no-op
  }

  @Override
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
        .setSource(file_name)
        .setValue(value)
        .build();
  }

  @Override
  public String source() {
    return file_name;
  }

  @Override
  public void reload() {
    final File file = new File(file_name);
    if (!file.exists()) {
      LOG.warn("No file found at: " + file_name);
      return;
    }
    
    try {
      long hash = Files.asByteSource(file).hash(
          Const.HASH_FUNCTION()).asLong();
      // NOTE: Tiny possibility the initial file hash could hash to 0...
      // if that happens lemme know.
      if (hash == last_hash) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("No changes to file: " + file_name);
        }
        return;
      }
      last_hash = hash;
    } catch (IOException e) {
      LOG.error("Failed to read the file at: " + file_name, e);
      return;
    }
    
    try {
      final InputStream stream = Files.asByteSource(file).openStream();
      JsonNode node = Configuration.OBJECT_MAPPER.readTree(stream);
      if (node == null) {
        LOG.error("The file was empty: " + file_name + ". Skipping "
            + "loading.");
        return;
      }
      
      if (!node.isObject()) {
        LOG.error("The file must contain an object/map of key values: " 
            + file_name + ". Skipping loading. Type: " + node.getNodeType());
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
      LOG.error("Failed to open or parse file at: " + file_name, e);
    }
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
              .setSource(file_name)
              .setValue(value)
              .build());
      } catch (Exception e) {
        LOG.warn("Failed to store key [" + key  + "] from file: " 
            + file_name, e);
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
}
