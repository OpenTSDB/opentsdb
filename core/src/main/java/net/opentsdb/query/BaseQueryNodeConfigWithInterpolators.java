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
package net.opentsdb.query;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.reflect.TypeToken;

import net.opentsdb.core.TSDB;
import net.opentsdb.query.interpolation.QueryInterpolatorConfig;
import net.opentsdb.query.interpolation.QueryInterpolatorFactory;
import net.opentsdb.utils.Comparators.MapComparator;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Base node config class that handles interpolation configs.
 * 
 * @since 3.0
 */
public abstract class BaseQueryNodeConfigWithInterpolators
    <B extends BaseQueryNodeConfigWithInterpolators.Builder<B, C>, 
       C extends BaseQueryNodeConfigWithInterpolators> 
          extends BaseQueryNodeConfig<B, C> {

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
      List<QueryInterpolatorConfig> interpolatorConfigs = builder.interpolatorConfigs;
      for (final QueryInterpolatorConfig config : interpolatorConfigs) {
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

  protected void toBuilder(final Builder builder) {
    builder.setInterpolatorConfigs(
        Lists.newArrayList(interpolator_configs.values()));
    super.toBuilder(builder);
  }
  
  public static abstract class Builder<B extends Builder<B, C>, C extends BaseQueryNodeConfigWithInterpolators> extends BaseQueryNodeConfig.Builder<B, C> {
    @JsonProperty
    protected List<QueryInterpolatorConfig> interpolatorConfigs;
    
    /**
     * @param interpolator_configs A list of interpolator configs 
     * replacing any existing list.
     * @return The builder.
     */
    public B setInterpolatorConfigs(
          final List<QueryInterpolatorConfig> interpolator_configs) {
      this.interpolatorConfigs = interpolator_configs;
      return self();
    }
    
    /**
     * @param interpolator_config A non-null interpolator config to 
     * add to the list (does not replace).
     * @return The builder.
     */
    public B addInterpolatorConfig(
          final QueryInterpolatorConfig interpolator_config) {
      if (interpolator_config == null) {
        throw new IllegalArgumentException("Config cannot be null.");
      }
      if (interpolatorConfigs == null) {
        interpolatorConfigs = Lists.newArrayList();
      }
      interpolatorConfigs.add(interpolator_config);
      return self();
    }
    
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    if (!super.equals(o)) {
      return false;
    }

    final BaseQueryNodeConfigWithInterpolators other = (BaseQueryNodeConfigWithInterpolators) o;

    return Objects.equal(interpolator_configs, other.interpolatorConfigs());

  }

  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }

  /** @return A HashCode object for deterministic, non-secure hashing */
  public HashCode buildHashCode() {
    final List<HashCode> hashes =
            Lists.newArrayListWithCapacity(1 +
                    (interpolator_configs != null ? interpolator_configs.size() : 0));

    hashes.add(super.buildHashCode());

    if (interpolator_configs != null) {
      List<QueryInterpolatorConfig> values = Lists.newArrayList(interpolator_configs.values());
      Collections.sort(values);
      for (final QueryInterpolatorConfig type : values) {
        if (type != null) {
          hashes.add(type.buildHashCode());
        }
      }
    }

    return Hashing.combineOrdered(hashes);
  }
  
  /**
   * Parses out the "interpolatorConfigs" config.
   * @param builder The non-null builder to populate.
   * @param mapper The non-null mapper.
   * @param tsdb The non-null TSDB.
   * @param node The non-null node.
   */
  public static void parse(final Builder builder,
                           final ObjectMapper mapper,
                           final TSDB tsdb,
                           final JsonNode node) {
    JsonNode n = node.get("interpolatorConfigs");
    if (n != null && !n.isNull()) {
      for (final JsonNode config : n) {
        JsonNode type_json = config.get("type");
        final QueryInterpolatorFactory factory = tsdb.getRegistry().getPlugin(
            QueryInterpolatorFactory.class, 
            type_json == null ? null : type_json.asText());
        if (factory == null) {
          throw new IllegalArgumentException("Unable to find an "
              + "interpolator factory for: " + 
              type_json == null ? "default" :
                type_json.asText());
        }
        
        final QueryInterpolatorConfig interpolator_config = 
            factory.parseConfig(mapper, tsdb, config);
        builder.addInterpolatorConfig(interpolator_config);
      }
    }
    
    BaseQueryNodeConfig.parse(builder, mapper, tsdb, node);
  }
}
