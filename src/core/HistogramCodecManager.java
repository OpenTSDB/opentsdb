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
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

import net.opentsdb.utils.JSON;
import net.opentsdb.utils.PluginLoader;

/**
 * <p>
 * Manages the histogram codecs loaded by default or as plugins. Codecs are 
 * defined in the 'tsd.core.histograms.config' property as either direct, 
 * escaped JSON or a file with the ".json" extension. Each codec config entry
 * must include the full class name and a unique ID for the type of histogram
 * or digest stored. 
 * <b>WARNING:</b> After writing data with a given ID, you cannot change the
 * ID and read that data any more.
 * <p>
 * This class is thread safe
 * 
 * @since 2.4
 */
public class HistogramCodecManager {
  private static final Logger LOG = LoggerFactory.getLogger(
      HistogramCodecManager.class);
  
  /** The map of IDs to decoders. */
  private final Map<Integer, HistogramDataPointCodec> codecs;
  
  /** The map of classes to decoder IDs. */
  private final Map<Class<?>, Integer> codecs_ids;

  /**
   * Default ctor that loads the codec map. It will parse the 
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
  public HistogramCodecManager(final TSDB tsdb) {
    final String config = tsdb.getConfig().getString("tsd.core.histograms.config");
    if (Strings.isNullOrEmpty(config)) {
      throw new IllegalArgumentException("Missing configuration "
          + "'tsd.core.histograms.config'");
    }
    
    final TypeReference<Map<String, Integer>> type_ref = 
        new TypeReference<Map<String, Integer>>() {};
    final Map<String, Integer> mappings;
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
    
    codecs = Maps.newHashMap();
    codecs_ids = Maps.newHashMap();
    
    if (mappings.isEmpty()) {
      LOG.warn("No histograms configured. Histogram writes and reads will "
          + "throw exceptions.");
      return;
    }
    
    final Set<Integer> ids = Sets.newHashSet();
    for (final Entry<String, Integer> mapping : mappings.entrySet()) {
      if (mapping.getValue() < 0 || mapping.getValue() > 255) {
        throw new IllegalArgumentException("ID for codec '" + mapping.getKey() 
          + "' must be from 0 to 255.");
      }
      if (ids.contains(mapping.getValue())) {
        throw new IllegalArgumentException("Duplicate ID found for codec '" 
            + mapping.getKey() + "': " + mapping.getValue());
      }
      ids.add(mapping.getValue());
      
      HistogramDataPointCodec codec = null;
      try {
        final Class<?> clazz = Class.forName(mapping.getKey());
        codec = (HistogramDataPointCodec) clazz.newInstance();
      } catch (ClassNotFoundException e) {
        codec = PluginLoader
            .loadSpecificPlugin(mapping.getKey(), HistogramDataPointCodec.class);
      } catch (InstantiationException e) {
        throw new IllegalStateException("Found decoder '" + mapping.getKey() 
          + "' on the class path but failed to instantiate it.", e);
      } catch (IllegalAccessException e) {
        throw new IllegalStateException("Found decoder '" + mapping.getKey() 
          + "' on the class path but we did not have permission to access it.", e);
      }
      
      if (codec == null) {
        throw new IllegalStateException("Unable to find a decoder named '" 
            + mapping.getKey() + "'");
      } else {
        codec.setId(mapping.getValue());
        codecs.put(mapping.getValue(), codec);
        codecs_ids.put(codec.getClass(), mapping.getValue());
        LOG.info("Successfully loaded decoder '" + mapping.getKey() 
          + "' with ID " + mapping.getValue());
      }
    }
  }
  
  /**
   * Return the instance of the given codec.
   * @param id The numeric ID of the codec (the first byte in storage).
   * @return The instance of the given codec
   * @throws IllegalArgumentException if no codec was found for the given ID.
   */
  public HistogramDataPointCodec getCodec(final int id) {
    final HistogramDataPointCodec codec = codecs.get(id);
    if (codec == null) {
      throw new IllegalArgumentException("No codec found mapped to ID " + id);
    }
    return codec;
  }
  
  /**
   * Return the ID of the given codec.
   * @param clazz The non-null class to search for.
   * @return The ID of the codec.
   * @throws IllegalArgumentException if the class was null or no ID was assigned
   * to the class.
   */
  public int getCodec(final Class<?> clazz) {
    if (clazz == null) {
      throw new IllegalArgumentException("Clazz cannot be null.");
    }
    final Integer id = codecs_ids.get(clazz);
    if (id == null) {
      throw new IllegalArgumentException("No codec ID assigned to class " 
          + clazz);
    }
    return id;
  }
  
  /**
   * Finds the proper codec and calls it's encode method to create a byte array
   * with the given ID as the first byte.
   * @param id The ID of the histogram type to search for.
   * @param data_point The non-null data point to encode.
   * @param include_id Whether or not to include the ID prefix when encoding.
   * @return A non-null and non-empty byte array if the codec was found.
   * @throws IllegalArgumentException if no codec was found for the given ID or 
   * the histogram may have been of the wrong type or failed encoding.
   */
  public byte[] encode(final int id, 
                       final Histogram data_point, 
                       final boolean include_id) {
    final HistogramDataPointCodec codec = getCodec(id);
    return codec.encode(data_point, include_id);
  }
  
  /**
   * Finds the proper codec and calls it's decode method to return the histogram
   * data point for queries or validation.
   * @param id The ID of the histogram type to search for.
   * @param raw_data The non-null and non-empty byte array to parse. Should NOT
   * include the first byte of the ID in the data.
   * @param timestamp The timestamp associated with the data point.
   * @param includes_id Whether or not the data includes the ID prefix.
   * @return A non-null data point if decoding was successful.
   */
  public Histogram decode(final int id, 
                                   final byte[] raw_data,
                                   final boolean includes_id) {
    final HistogramDataPointCodec codec = getCodec(id);
    return codec.decode(raw_data, includes_id);
  }
}
