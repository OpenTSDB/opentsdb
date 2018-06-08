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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

/**
 * Base node config class that handles interpolation configs.
 * 
 * @since 3.0
 */
public abstract class BaseQueryNodeConfigWithInterpolators 
    implements QueryNodeConfig {

  /** The ID of this config. */
  protected final String id;
  
  /** The map of types to configs. */
  protected final Map<TypeToken<?>, QueryInterpolatorConfig> interpolator_configs;
  
  /**
   * Protected ctor.
   * @param builder A non-null builder.
   */
  protected BaseQueryNodeConfigWithInterpolators(final Builder builder) {
    this.id = builder.id;
    if (builder.interpolator_configs != null && 
        !builder.interpolator_configs.isEmpty()) {
      interpolator_configs = Maps.newHashMapWithExpectedSize(
          builder.interpolator_configs.size());
      for (final QueryInterpolatorConfig config : builder.interpolator_configs) {
        // TODO - may need to put this in the registry AND we want to 
        // figure out if the name is a full string or not.
        final Class<?> clazz;
        try {
          clazz = Class.forName(config.type());
        } catch (ClassNotFoundException e) {
          throw new IllegalArgumentException("No data type found for: " 
              + config.type());
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
    protected String id;
    protected List<QueryInterpolatorConfig> interpolator_configs;
    
    public Builder setId(final String id) {
      this.id = id;
      return this;
    }
    
    public Builder setInterpolatorConfigs(final List<QueryInterpolatorConfig> interpolator_configs) {
      this.interpolator_configs = interpolator_configs;
      return this;
    }
    
    public Builder addInterpolatorConfig(final QueryInterpolatorConfig interpolator_config) {
      if (interpolator_configs == null) {
        interpolator_configs = Lists.newArrayList();
      }
      interpolator_configs.add(interpolator_config);
      return this;
    }
    
    public abstract QueryNodeConfig build();
  }
}
