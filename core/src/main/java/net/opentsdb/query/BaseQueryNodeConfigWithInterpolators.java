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
package net.opentsdb.query;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import net.opentsdb.configuration.Configuration;
import net.opentsdb.query.interpolation.QueryInterpolatorConfig;
import net.opentsdb.utils.Comparators.MapComparator;

/**
 * Base node config class that handles interpolation configs.
 * 
 * @since 3.0
 */
public abstract class BaseQueryNodeConfigWithInterpolators 
    implements QueryNodeConfig {

  /** A comparator for the interpolator map. */
  protected static MapComparator<TypeToken<?>, QueryInterpolatorConfig> INTERPOLATOR_CMP
     = new MapComparator<TypeToken<?>, QueryInterpolatorConfig>();
  
  /** The ID of this config. */
  protected final String id;
  
  /** The optional map of overrides. */
  protected final Map<String, String> overrides;
  
  /** The map of types to configs. */
  protected final Map<TypeToken<?>, QueryInterpolatorConfig> interpolator_configs;
  
  /**
   * Protected ctor.
   * @param builder A non-null builder.
   */
  protected BaseQueryNodeConfigWithInterpolators(final Builder builder) {
    this.id = builder.id;
    this.overrides = builder.overrides;
    if (builder.interpolatorConfigs != null && 
        !builder.interpolatorConfigs.isEmpty()) {
      interpolator_configs = Maps.newHashMapWithExpectedSize(
          builder.interpolatorConfigs.size());
      for (final QueryInterpolatorConfig config : builder.interpolatorConfigs) {
        // TODO - may need to put this in the registry AND we want to 
        // figure out if the name is a full string or not.
        final Class<?> clazz;
        try {
          clazz = Class.forName(config.dataType());
        } catch (ClassNotFoundException e) {
          throw new IllegalArgumentException("No data type found for: " 
              + config.dataType());
        }
        final TypeToken<?> type = TypeToken.of(clazz);
        if (interpolator_configs.containsKey(type)) {
          throw new IllegalArgumentException("Already have an "
              + "interpolator configuration for: " + type);
        }
        interpolator_configs.put(type, config);
      }
    } else {
      interpolator_configs = null;
    }
  }
  
  @Override
  public String getId() {
    return id;
  }
  
  /** @return The interpolator configs. May be null. */
  public Map<TypeToken<?>, QueryInterpolatorConfig> interpolatorConfigs() {
    return interpolator_configs;
  }
  
  @Override
  public Map<String, String> getOverrides() {
    return overrides;
  }
  
  @Override
  public String getString(final Configuration config, final String key) {
    if (config == null) {
      throw new IllegalArgumentException("Config cannot be null");
    }
    if (Strings.isNullOrEmpty(key)) {
      throw new IllegalArgumentException("Key cannot be null or empty.");
    }
    String value = overrides == null ? null : overrides.get(key);
    if (Strings.isNullOrEmpty(value)) {
      if (config.hasProperty(key)) {
        return config.getString(key);
      }
    }
    return value;
  }
  
  @Override
  public int getInt(final Configuration config, final String key) {
    if (config == null) {
      throw new IllegalArgumentException("Config cannot be null");
    }
    if (Strings.isNullOrEmpty(key)) {
      throw new IllegalArgumentException("Key cannot be null or empty.");
    }
    String value = overrides == null ? null : overrides.get(key);
    if (Strings.isNullOrEmpty(value)) {
      if (config.hasProperty(key)) {
        return config.getInt(key);
      }
      throw new IllegalArgumentException("No value for key '" + key + "'");
    }
    return Integer.parseInt(value);
  }
  
  @Override
  public long getLong(final Configuration config, final String key) {
    if (config == null) {
      throw new IllegalArgumentException("Config cannot be null");
    }
    if (Strings.isNullOrEmpty(key)) {
      throw new IllegalArgumentException("Key cannot be null or empty.");
    }
    String value = overrides == null ? null : overrides.get(key);
    if (Strings.isNullOrEmpty(value)) {
      if (config.hasProperty(key)) {
        return config.getInt(key);
      }
      throw new IllegalArgumentException("No value for key '" + key + "'");
    }
    return Long.parseLong(value);
  }
  
  @Override
  public boolean getBoolean(final Configuration config, final String key) {
    if (config == null) {
      throw new IllegalArgumentException("Config cannot be null");
    }
    if (Strings.isNullOrEmpty(key)) {
      throw new IllegalArgumentException("Key cannot be null or empty.");
    }
    String value = overrides == null ? null : overrides.get(key);
    if (Strings.isNullOrEmpty(value)) {
      if (config.hasProperty(key)) {
        return config.getBoolean(key);
      }
      throw new IllegalArgumentException("No value for key '" + key + "'");
    }
    value = value.trim().toLowerCase();
    return value.equals("true") || value.equals("1") || value.equals("yes");
  }
  
  @Override
  public double getDouble(final Configuration config, final String key) {
    if (config == null) {
      throw new IllegalArgumentException("Config cannot be null");
    }
    if (Strings.isNullOrEmpty(key)) {
      throw new IllegalArgumentException("Key cannot be null or empty.");
    }
    String value = overrides == null ? null : overrides.get(key);
    if (Strings.isNullOrEmpty(value)) {
      if (config.hasProperty(key)) {
        return config.getInt(key);
      }
      throw new IllegalArgumentException("No value for key '" + key + "'");
    }
    return Double.parseDouble(value);
  }
  
  @Override
  public boolean hasKey(final String key) {
    if (Strings.isNullOrEmpty(key)) {
      throw new IllegalArgumentException("Key cannot be null or empty.");
    }
    return overrides == null ? false : overrides.containsKey(key);
  }
  
  /**
   * Fetches the interpolator config for a type if present.
   * @param type A non-null type.
   * @return The config if present, null if not.
   */
  public QueryInterpolatorConfig interpolatorConfig(final TypeToken<?> type) {
    return interpolator_configs == null ? null :
      interpolator_configs.get(type);
  }
  
  public static abstract class Builder {
    @JsonProperty
    protected String id;
    @JsonProperty
    protected Map<String, String> overrides;
    @JsonProperty
    protected List<QueryInterpolatorConfig> interpolatorConfigs;
    
    /**
     * @param id An ID for this builder.
     * @return The builder.
     */
    public Builder setId(final String id) {
      this.id = id;
      return this;
    }
    
    /**
     * @param overrides An override map to replace the existing map.
     * @return The builder.
     */
    public Builder setOverrides(final Map<String, String> overrides) {
      this.overrides = overrides;
      return this;
    }
    
    /**
     * @param key An override key to store in the override map.
     * @param value A value to store, overwriting existing values.
     * @return The builder.
     */
    public Builder addOverride(final String key, final String value) {
      if (overrides == null) {
        overrides = Maps.newHashMap();
      }
      overrides.put(key, value);
      return this;
    }
    
    /**
     * @param interpolator_configs A list of interpolator configs 
     * replacing any existing list.
     * @return The builder.
     */
    public Builder setInterpolatorConfigs(
          final List<QueryInterpolatorConfig> interpolator_configs) {
      this.interpolatorConfigs = interpolator_configs;
      return this;
    }
    
    /**
     * @param interpolator_config A non-null interpolator config to 
     * add to the list (does not replace).
     * @return The builder.
     */
    public Builder addInterpolatorConfig(
          final QueryInterpolatorConfig interpolator_config) {
      if (interpolatorConfigs == null) {
        interpolatorConfigs = Lists.newArrayList();
      }
      interpolatorConfigs.add(interpolator_config);
      return this;
    }
    
    /** @return A config object or an exception if the config failed. */
    public abstract QueryNodeConfig build();
  }
  
}
