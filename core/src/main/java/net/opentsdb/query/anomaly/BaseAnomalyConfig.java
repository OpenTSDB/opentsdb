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
package net.opentsdb.query.anomaly;

import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;

import net.opentsdb.core.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.query.BaseQueryNodeConfigWithInterpolators;
import net.opentsdb.query.interpolation.QueryInterpolatorConfig;
import net.opentsdb.query.interpolation.QueryInterpolatorFactory;

/**
 * Base class for an anomaly config object.
 * 
 * @since 3.0
 */
public abstract class BaseAnomalyConfig 
    extends BaseQueryNodeConfigWithInterpolators 
    implements AnomalyConfig {

  protected final ExecutionMode mode;
  protected final boolean serialize_observed;
  protected final boolean serialize_thresholds;
  
  protected BaseAnomalyConfig(final Builder builder) {
    super(builder);
    mode = builder.mode;
    serialize_observed = builder.serializeObserved;
    serialize_thresholds = builder.serializeThresholds;
  }
  
  @Override
  public ExecutionMode getMode() {
    return mode;
  }
  
  @Override
  public boolean getSerializeObserved() {
    return serialize_observed;
  }
  
  @Override
  public boolean getSerializeThresholds() {
    return serialize_thresholds;
  }
  
  @Override
  public boolean equals(final Object o) {
    if (o == null) {
      return false;
    }
    
    if (o == this) {
      return true;
    }
    
    if (!(o instanceof BaseAnomalyConfig)) {
      return false;
    }
    
    BaseAnomalyConfig other = (BaseAnomalyConfig) o;
    return Objects.equals(mode, other.mode) &&
        super.equals(other);
  }
  
  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }
  
  public HashCode buildHashCode() {
    HashCode hash = Const.HASH_FUNCTION().newHasher()
        .putString(mode.toString(), Const.UTF8_CHARSET)
        .hash();
    final List<HashCode> hashes = Lists.newArrayListWithCapacity(2);
    hashes.add(hash);
    hashes.add(super.buildHashCode());
    return Hashing.combineOrdered(hashes);
  }
  
  public static abstract class Builder<B extends Builder<B, C>, 
                                       C extends BaseQueryNodeConfigWithInterpolators> 
      extends BaseQueryNodeConfigWithInterpolators.Builder<B, C> {
    protected ExecutionMode mode;
    protected boolean serializeObserved;
    protected boolean serializeThresholds;
    
    public B setMode(final ExecutionMode mode) {
      this.mode = mode;
      return self();
    }
    
    public B setSerializeObserved(final boolean serialize_observed) {
      serializeObserved = serialize_observed;
      return self();
    }
    
    public B setSerializeThresholds(final boolean serialize_thresholds) {
      serializeThresholds = serialize_thresholds;
      return self();
    }
    
    public static void parseConfig(final ObjectMapper mapper, 
        final TSDB tsdb,
        final JsonNode node,
        final Builder builder) {
      JsonNode n = node.get("id");
      if (n != null) {
        builder.setId(n.asText());
      }
      
      n = node.get("mode");
      if (n != null && !n.isNull()) {
        builder.setMode(ExecutionMode.valueOf(n.asText()));
      }
      
      n = node.get("sources");
      if (n != null && !n.isNull()) {
        try {
          builder.setSources(mapper.treeToValue(n, List.class));
        } catch (JsonProcessingException e) {
          throw new IllegalArgumentException("Failed to parse json", e);
        }
      }
      
      n = node.get("interpolatorConfigs");
      for (final JsonNode config : n) {
        JsonNode type_json = config.get("type");
        final QueryInterpolatorFactory factory = tsdb.getRegistry().getPlugin(
            QueryInterpolatorFactory.class, 
            type_json == null || type_json.isNull() ? 
               null : type_json.asText());
        if (factory == null) {
          throw new IllegalArgumentException("Unable to find an "
              + "interpolator factory for: " + 
              (type_json == null || type_json.isNull() ? "Default" :
               type_json.asText()));
        }
        
        final QueryInterpolatorConfig interpolator_config = 
            factory.parseConfig(mapper, tsdb, config);
        builder.addInterpolatorConfig(interpolator_config);
      }
      
      n = node.get("serializeObserved");
      if (n != null && !n.isNull()) {
        builder.setSerializeObserved(n.asBoolean());
      }
      
      n = node.get("serializeThresholds");
      if (n != null && !n.isNull()) {
        builder.setSerializeThresholds(n.asBoolean());
      }
    }
  }
}