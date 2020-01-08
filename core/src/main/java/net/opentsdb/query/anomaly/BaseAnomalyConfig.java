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

import com.fasterxml.jackson.annotation.JsonProperty;
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
  protected final boolean serialize_deltas;
  protected final boolean serialize_alerts;
  protected final double upper_threshold_bad;
  protected final double upper_threshold_warn;
  protected final boolean upper_is_scalar;
  protected final double lower_threshold_bad;
  protected final double lower_threshold_warn;
  protected final boolean lower_is_scalar;
  
  protected BaseAnomalyConfig(final Builder builder) {
    super(builder);
    mode = builder.mode;
    serialize_observed = builder.serializeObserved;
    serialize_thresholds = builder.serializeThresholds;
    serialize_deltas = builder.serializeDeltas;
    serialize_alerts = builder.serializeAlerts;
    upper_threshold_bad = builder.upperThresholdBad;
    upper_threshold_warn = builder.upperThresholdWarn;
    upper_is_scalar = builder.upperIsScalar;
    lower_threshold_bad = builder.lowerThresholdBad;
    lower_threshold_warn = builder.lowerThresholdWarn;
    lower_is_scalar = builder.lowerIsScalar;
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
  
  public boolean getSerializeDeltas() {
    return serialize_deltas;
  }
  
  public boolean getSerializeAlerts() {
    return serialize_alerts;
  }
  
  public double getUpperThresholdBad() {
    return upper_threshold_bad;
  }
  
  public double getUpperThresholdWarn() {
    return upper_threshold_warn;
  }

  public boolean isUpperIsScalar() {
    return upper_is_scalar;
  }

  public double getLowerThresholdBad() {
    return lower_threshold_bad;
  }
  
  public double getLowerThresholdWarn() {
    return lower_threshold_warn;
  }

  public boolean isLowerIsScalar() {
    return lower_is_scalar;
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
        Objects.equals(upper_threshold_bad, other.upper_threshold_bad) &&
        Objects.equals(upper_threshold_warn, other.upper_threshold_warn) &&
        Objects.equals(upper_is_scalar, other.upper_is_scalar) &&
        Objects.equals(lower_threshold_bad, other.lower_threshold_bad) &&
        Objects.equals(lower_threshold_warn, other.lower_threshold_warn) &&
        Objects.equals(lower_is_scalar, other.lower_is_scalar) &&
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
    // NOTE: Purposely leaving out the thresholds.
    return Hashing.combineOrdered(hashes);
  }
  
  public static abstract class Builder<B extends Builder<B, C>, 
                                       C extends BaseQueryNodeConfigWithInterpolators> 
      extends BaseQueryNodeConfigWithInterpolators.Builder<B, C> {
    @JsonProperty
    protected ExecutionMode mode;
    @JsonProperty
    protected boolean serializeObserved;
    @JsonProperty
    protected boolean serializeThresholds;
    @JsonProperty
    protected boolean serializeDeltas;
    @JsonProperty
    protected boolean serializeAlerts = true;
    @JsonProperty
    private double upperThresholdBad;
    @JsonProperty
    private double upperThresholdWarn;
    @JsonProperty
    private boolean upperIsScalar;
    @JsonProperty
    private double lowerThresholdBad;
    @JsonProperty
    private double lowerThresholdWarn;
    @JsonProperty
    private boolean lowerIsScalar;
    
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
    
    public B setSerializeDeltas(final boolean serialize_deltas) {
      this.serializeDeltas = serialize_deltas;
      return self();
    }
    
    public B setSerializeAlerts(final boolean serialize_alerts) {
      this.serializeAlerts = serialize_alerts;
      return self();
    }

    public B setUpperThresholdBad(final double upper_threshold) {
      upperThresholdBad = upper_threshold;
      return self();
    }
    
    public B setUpperThresholdWarn(final double upper_threshold) {
      upperThresholdWarn = upper_threshold;
      return self();
    }
    
    public B setUpperIsScalar(final boolean upper_is_scalar) {
      upperIsScalar = upper_is_scalar;
      return self();
    }
    
    public B setLowerThresholdBad(final double lower_treshold) {
      lowerThresholdBad = lower_treshold;
      return self();
    }
    
    public B setLowerThresholdWarn(final double lower_treshold) {
      lowerThresholdWarn = lower_treshold;
      return self();
    }
    
    public B setLowerIsScalar(final boolean lower_is_scalar) {
      lowerIsScalar = lower_is_scalar;
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
      
      n = node.get("serializeDeltas");
      if (n != null && !n.isNull()) {
        builder.setSerializeDeltas(n.asBoolean());
      }
      
      n = node.get("serializeAlerts");
      if (n != null && !n.isNull()) {
        builder.setSerializeAlerts(n.asBoolean());
      }
      
      n = node.get("upperThresholdBad");
      if (n != null && !n.isNull()) {
        builder.setUpperThresholdBad(n.asDouble());
      }
      
      n = node.get("upperThresholdWarn");
      if (n != null && !n.isNull()) {
        builder.setUpperThresholdWarn(n.asDouble());
      }
      
      n = node.get("upperIsScalar");
      if (n != null && !n.isNull()) {
        builder.setUpperIsScalar(n.asBoolean());
      }
      
      n = node.get("lowerThresholdBad");
      if (n != null && !n.isNull()) {
        builder.setLowerThresholdBad(n.asDouble());
      }
      
      n = node.get("lowerThresholdWarn");
      if (n != null && !n.isNull()) {
        builder.setLowerThresholdWarn(n.asDouble());
      }
      
      n = node.get("lowerIsScalar");
      if (n != null && !n.isNull()) {
        builder.setLowerIsScalar(n.asBoolean());
      }
      
    }
  }
}