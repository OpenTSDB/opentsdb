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
package net.opentsdb.query.interpolation;

import java.util.Iterator;
import java.util.Map;

import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.query.QueryInterpolationConfig;
import net.opentsdb.query.QueryInterpolator;
import net.opentsdb.query.QueryInterpolatorConfig;
import net.opentsdb.query.QueryInterpolatorFactory;

/**
 * A simple default implementation for storing interpolator configs and
 * factories for a query. An empty config is allowed, just returns null
 * for every request.
 * 
 * @since 3.0
 */
public class DefaultInterpolationConfig implements QueryInterpolationConfig {

  /** The map of data types to config and factories. */
  protected Map<TypeToken<?>, Container> configs;
  
  /**
   * Package private CTor to build the config.
   * @param builder A non-null builder.
   */
  DefaultInterpolationConfig(final Builder builder) {
    configs = builder.configs;
  }
  
  @Override
  public QueryInterpolatorConfig config(
      final TypeToken<? extends TimeSeriesDataType> type) {
    final Container container = configs.get(type);
    if (container == null) {
      return null;
    }
    return container.config;
  }

  @Override
  public QueryInterpolator<? extends TimeSeriesDataType> newInterpolator(
      final TypeToken<? extends TimeSeriesDataType> type, 
      final TimeSeries source, 
      final QueryInterpolatorConfig config) {
    final Container container = configs.get(type);
    if (container == null) {
      return null;
    }
    return container.factory.newInterpolator(type, source, config);
  }

  @Override
  public QueryInterpolator<? extends TimeSeriesDataType> newInterpolator(
      final TypeToken<? extends TimeSeriesDataType> type, 
      final TimeSeries source) {
    final Container container = configs.get(type);
    if (container == null) {
      return null;
    }
    return container.factory.newInterpolator(type, source, container.config);
  }

  @Override
  public QueryInterpolator<? extends TimeSeriesDataType> newInterpolator(
      final TypeToken<? extends TimeSeriesDataType> type,
      final Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> iterator,
      final QueryInterpolatorConfig config) {
    final Container container = configs.get(type);
    if (container == null) {
      return null;
    }
    return container.factory.newInterpolator(type, iterator, config);
  }

  @Override
  public QueryInterpolator<? extends TimeSeriesDataType> newInterpolator(
      final TypeToken<? extends TimeSeriesDataType> type,
      final Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> iterator) {
    final Container container = configs.get(type);
    if (container == null) {
      return null;
    }
    return container.factory.newInterpolator(type, iterator, container.config);
  }

  /**
   * Package private class that just stores the config and factory refs.
   */
  static class Container {
    public QueryInterpolatorConfig config;
    public QueryInterpolatorFactory factory;
  }
  
  /** @return A new builder instance. */
  public static Builder newBuilder() {
    return new Builder();
  }
  
  public static class Builder {
    private Map<TypeToken<?>, Container> configs = Maps.newHashMap();
    
    /**
     * Adds a config and factory for a type.
     * @param type A non-null type.
     * @param config A non-null config.
     * @param factory A non-null factory.
     * @return The builder.
     */
    public Builder add(final TypeToken<?> type, 
                       final QueryInterpolatorConfig config, 
                       final QueryInterpolatorFactory factory) {
      if (type == null) {
        throw new IllegalArgumentException("Type cannot be null.");
      }
      if (config == null) {
        throw new IllegalArgumentException("Config cannot be null.");
      }
      if (factory == null) {
        throw new IllegalArgumentException("Factory cannot be null.");
      }
      Container container = configs.get(type);
      if (container == null) {
        container = new Container();
        configs.put(type, container);
      }
      container.config = config;
      container.factory = factory;
      return this;
    }
    
    /** @return The config instance. */
    public DefaultInterpolationConfig build() {
      return new DefaultInterpolationConfig(this);
    }
  }
  
}