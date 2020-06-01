// This file is part of OpenTSDB.
// Copyright (C) 2018-2020  The OpenTSDB Authors.
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
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.io.Files;

import io.netty.util.HashedWheelTimer;
import net.opentsdb.common.Const;
import net.opentsdb.configuration.Configuration;

/**
 * Handles parsing of JSON or YAML formatted files. Supports dotted paths
 * as long as each sub-path is an object. The root of the path can be
 * any object.
 * Use this class for complex configs that require maps, lists and possibly
 * POJOs in the source.
 * 
 * @since 3.0
 */
public class YamlJsonFileProvider extends YamlJsonBaseProvider {
  private static final Logger LOG = LoggerFactory.getLogger(
      YamlJsonFileProvider.class);
  
  /** The file name. */
  protected final String file_name;
  
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
    super(factory, config, timer, uri);
    
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
    
    InputStream stream = null;
    try {
      stream = Files.asByteSource(file).openStream();
      parse(stream);
    } catch (IOException e) {
      LOG.error("Failed to open file: " + uri);
    } finally {
      if (stream != null) {
        try {
          stream.close();
        } catch (IOException e) {
          LOG.warn("Failed to close stream: " + uri);
        }
      }
    }
  }
 
  @Override
  public void populateRawMap(final Map<String, String> map) {
    final File file = new File(file_name);
    if (!file.exists()) {
      LOG.warn("No file found at: " + file_name);
      return;
    }
    
    InputStream stream = null;
    try {
      stream = Files.asByteSource(file).openStream();
      
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
      
      recursiveToMap(node, null, map);
    } catch (IOException e) {
      LOG.error("Failed to open file: " + uri);
    } finally {
      if (stream != null) {
        try {
          stream.close();
        } catch (IOException e) {
          LOG.warn("Failed to close stream: " + uri);
        }
      }
    }
  }
  
  private void recursiveToMap(final JsonNode node, 
                              final String key, 
                              final Map<String, String> map) {
    switch (node.getNodeType()) {
    case STRING:
    case BOOLEAN:
    case NUMBER:
      map.put(key, node.asText());
      break;
    case NULL:
      // no-op
      break;
    case ARRAY:
      // can't do deep nesting here but if we have simple numbers or strings we
      // can comma delimit.
      StringBuilder buffer = null;
      for (final JsonNode child : node) {
        switch (child.getNodeType()) {
        case STRING:
        case BOOLEAN:
        case NUMBER:
          if (buffer == null) {
            buffer = new StringBuilder()
                .append(child.asText());
          } else {
            buffer.append(",")
                  .append(child.asText());
          }
        default:
          if (LOG.isTraceEnabled()) {
            LOG.trace("Ignoring node of type: " + child.getNodeType() 
              + " nested in an array.");
          }
        }
        break;
      }
      if (buffer != null) {
        map.put(key, buffer.toString());
      }
      break;
      
    case OBJECT:
      final Iterator<Entry<String, JsonNode>> iterator = node.fields();
      while (iterator.hasNext()) {
        final Entry<String, JsonNode> entry = iterator.next();
        recursiveToMap(entry.getValue(), 
                       key != null ? key + "." + entry.getKey() : entry.getKey(), 
                       map);
      }
      break;
    default:
      if (LOG.isTraceEnabled()) {
        LOG.trace("Ignoring node of type: " + node.getNodeType());
      }
    }
  }
}
