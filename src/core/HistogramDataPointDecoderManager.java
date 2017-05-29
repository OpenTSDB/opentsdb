// This file is part of OpenTSDB.
// Copyright (C) 2016-2017  The OpenTSDB Authors.
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
package net.opentsdb.core;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.io.Files;

import net.opentsdb.utils.JSON;
import net.opentsdb.utils.PluginLoader;

/**
 * <p>
 * Manages the histogram decoder singletons.
 * </p>
 * <p>
 * This manage accepts the full class name of the decoder, use reflection to 
 * create the decoder, and it ensures each type of the decoder will be created 
 * only once, after that, the cached decoder instance will be returned.
 * </p>
 * <p>
 * This behavior actually makes each decoder a singleton.
 * </p>
 *
 * <p>This class is thread safe</p>
 * 
 * @since 2.4
 */
public class HistogramDataPointDecoderManager {
  private static final Logger LOG = LoggerFactory.getLogger(
      HistogramDataPointDecoderManager.class);
  
  /** The map of IDs to decoders. */
  private final Map<Byte, HistogramDataPointDecoder> decoders;
  
  /** The map of classes to decoder IDs. */
  private final Map<Class<?>, Byte> decoder_ids;

  /**
   * Default ctor that loads the decoder map. It will parse the 
   * 'tsd.core.histograms.config' parameter. If it ends with .json then we'll 
   * try to load a file of that name, otherwise we'll just parse it as raw JSON.
   * For each map in the JSON we search the classpath then loaded plugins.
   * 
   * @param tsdb A non-null TSDB to load the config from.
   * @throws IllegalArgumentException if the config was null or empty or if the 
   * JSON was malformed.
   * @throws RuntimeException if the file couldn't be opened.
   * @throws IllegalStateException if no classes/plugins of the type could be 
   * found OR if one was found but couldn't be instantiated. 
   */
  public HistogramDataPointDecoderManager(final TSDB tsdb) {
    final String config = tsdb.getConfig().getString("tsd.core.histograms.config");
    if (Strings.isNullOrEmpty(config)) {
      throw new IllegalArgumentException("Missing configuration "
          + "'tsd.core.histograms.config'");
    }
    
    final TypeReference<Map<String, Byte>> type_ref = 
        new TypeReference<Map<String, Byte>>() {};
    final Map<String, Byte> mappings;
    if (config.endsWith(".json")) {
      final String json;
      try {
        json = Files.toString(new File(config), Const.UTF8_CHARSET);
      } catch (IOException e) {
        throw new RuntimeException("Unable to open plugin config file: " 
            + config, e);
      }
      mappings = JSON.parseToObject(json, type_ref);
    } else {
      mappings = JSON.parseToObject(config, type_ref);
    }
    
    decoders = Maps.newHashMap();
    decoder_ids = Maps.newHashMap();
    
    if (mappings.isEmpty()) {
      LOG.warn("No histograms configured. Histogram writes and reads will "
          + "throw exceptions.");
      return;
    }
    
    for (final Entry<String, Byte> mapping : mappings.entrySet()) {
      HistogramDataPointDecoder decoder = null;
      try {
        final Class<?> clazz = Class.forName(mapping.getKey());
        decoder = (HistogramDataPointDecoder) clazz.newInstance();
      } catch (ClassNotFoundException e) {
        decoder = PluginLoader
            .loadSpecificPlugin(mapping.getKey(), HistogramDataPointDecoder.class);
      } catch (InstantiationException e) {
        throw new IllegalStateException("Found decoder '" + mapping.getKey() 
          + "' on the class path but failed to instantiate it.", e);
      } catch (IllegalAccessException e) {
        throw new IllegalStateException("Found decoder '" + mapping.getKey() 
          + "' on the class path but we did not have permission to access it.", e);
      }
      
      if (decoder == null) {
        throw new IllegalStateException("Unable to find a decoder named '" 
            + mapping.getKey() + "'");
      } else {
        decoders.put(mapping.getValue(), decoder);
        decoder_ids.put(decoder.getClass(), mapping.getValue());
        LOG.info("Successfully loaded decoder '" + mapping.getKey() 
          + "' with ID " + mapping.getValue());
      }
    }
  }
  
  /**
   * Return the instance of the given decoder.
   * @param id The numeric ID of the decoder (the first byte in storage).
   * @return The instance of the given decoder
   * @throws IllegalArgumentException if no decoder was found for the given ID.
   */
  public HistogramDataPointDecoder getDecoder(final byte id) {
    final HistogramDataPointDecoder decoder = decoders.get(id);
    if (decoder == null) {
      throw new IllegalArgumentException("No decoder found mapped to ID " + id);
    }
    return decoder;
  }
  
  public byte getDecoder(final Class<?> clazz) {
    if (clazz == null) {
      throw new IllegalArgumentException("Clazz cannot be null.");
    }
    final Byte id = decoder_ids.get(clazz);
    if (id == null) {
      throw new IllegalArgumentException("No decoder ID assigned to class " 
          + clazz);
    }
    return id;
  }
}
