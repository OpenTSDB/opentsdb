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

import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import net.opentsdb.query.interpolation.QueryInterpolatorConfig;
import net.opentsdb.utils.Comparators.MapComparator;

/**
 * Base node config class that handles interpolation configs.
 * 
 * @since 3.0
 */
public abstract class BaseQueryNodeConfigWithInterpolators extends 
  BaseQueryNodeConfig {

  /** A comparator for the interpolator map. */
  protected static MapComparator<TypeToken<?>, QueryInterpolatorConfig> INTERPOLATOR_CMP
     = new MapComparator<TypeToken<?>, QueryInterpolatorConfig>();
  
  /** The map of types to configs. */
  protected final Map<TypeToken<?>, QueryInterpolatorConfig> interpolator_configs;
  
  /**
   * Protected ctor.
   * @param builder A non-null builder.
   * @throws IllegalArgumentException if the ID was null or empty.
   */
  protected BaseQueryNodeConfigWithInterpolators(final Builder builder) {
    super(builder);
    if (builder.interpolatorConfigs != null && 
        !builder.interpolatorConfigs.isEmpty()) {
      interpolator_configs = Maps.newHashMapWithExpectedSize(
          builder.interpolatorConfigs.size());
      for (final QueryInterpolatorConfig config : builder.interpolatorConfigs) {
        if (interpolator_configs.containsKey(config.type())) {
          throw new IllegalArgumentException("Already have an "
              + "interpolator configuration for: " + config.type());
        }
        interpolator_configs.put(config.type(), config);
      }
    } else {
      interpolator_configs = null;
    }
  }
  
  /** @return The interpolator configs as a typed map. May be null. */
  public Map<TypeToken<?>, QueryInterpolatorConfig> interpolatorConfigs() {
    return interpolator_configs;
  }
  
  /** @return The array of interpolator configs. */
  public Collection<QueryInterpolatorConfig> getInterpolatorConfigs() {
    return interpolator_configs.values();
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
  
  public static abstract class Builder extends BaseQueryNodeConfig.Builder {
    @JsonProperty
    protected List<QueryInterpolatorConfig> interpolatorConfigs;
    
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
