// This file is part of OpenTSDB.
// Copyright (C) 2017-2018  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.expressions;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.TreeMap;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Strings;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.reflect.TypeToken;

import net.opentsdb.core.Const;
import net.opentsdb.query.BaseQueryNodeConfigWithInterpolators;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.interpolation.QueryInterpolatorConfig;
import net.opentsdb.query.joins.JoinConfig;
import net.opentsdb.utils.Comparators.MapComparator;

/**
 * Represents a single arithmetic and/or logical expression involving 
 * (for now) numeric time series.
 * 
 * TODO - overrides in hashes/equals/compareto
 * 
 * @since 3.0
 */
@JsonInclude(Include.NON_NULL)
@JsonDeserialize(builder = ExpressionConfig.class)
public class ExpressionConfig extends BaseQueryNodeConfigWithInterpolators {
  
  /** A comparator for the variable map. */
  private static MapComparator<String, QueryInterpolatorConfig> VARIABLE_INTERP_CMP
    = new MapComparator<String, QueryInterpolatorConfig>();
  
  /** The original expression string. */
  private final String expression;
  
  /** The non-null join config. */
  private final JoinConfig join_config;
  
  /** An optional map of variable to interpolators to override the defaults. */
  private final Map<String, QueryInterpolatorConfig> variable_interpolators;
  
  /**
   * Protected ctor.
   * @param builder The non-null builder.
   */
  protected ExpressionConfig(final Builder builder) {
    super(builder);
    if (Strings.isNullOrEmpty(builder.expression)) {
      throw new IllegalArgumentException("Expression cannot be null.");
    }
    if (builder.joinConfig == null) {
      throw new IllegalArgumentException("Join config cannot be null.");
    }
    if (interpolator_configs == null || interpolator_configs.isEmpty()) {
      throw new IllegalArgumentException("Must have at least default interpolator.");
    }
    expression = builder.expression;
    join_config = builder.joinConfig;
    variable_interpolators = builder.variable_interpolators;
  }
  
  /** @return The raw expression string to be parsed. */
  public String getExpression() {
    return expression;
  }
  
  /** @return The join config. */
  public JoinConfig getJoinConfig() {
    return join_config;
  }
  
  /** @return A possibly null map of variable names to interpolators. */
  public Map<String, QueryInterpolatorConfig> getVariableInterpolators() {
    return variable_interpolators;
  }
  
  @Override
  public HashCode buildHashCode() {
    final List<HashCode> hashes = Lists.newArrayListWithExpectedSize(2);
    hashes.add(join_config.buildHashCode());
    hashes.add(Const.HASH_FUNCTION().newHasher()
        .putString(id, Const.UTF8_CHARSET)
        .putString(expression, Const.UTF8_CHARSET).hash());
    if (variable_interpolators != null && !variable_interpolators.isEmpty()) {
      final Map<String, QueryInterpolatorConfig> sorted = 
          new TreeMap<String, QueryInterpolatorConfig>(variable_interpolators);
      for (final Entry<String, QueryInterpolatorConfig> entry : sorted.entrySet()) {
        hashes.add(entry.getValue().buildHashCode());
      }
    }
    if (interpolator_configs != null && 
        !interpolator_configs.isEmpty()) {
      final Map<String, QueryInterpolatorConfig> sorted = 
          new TreeMap<String, QueryInterpolatorConfig>();
      for (final Entry<TypeToken<?>, QueryInterpolatorConfig> entry : 
          interpolator_configs.entrySet()) {
        sorted.put(entry.getKey().toString(), entry.getValue());
      }
      for (final Entry<String, QueryInterpolatorConfig> entry : sorted.entrySet()) {
        hashes.add(entry.getValue().buildHashCode());
      }
    }
    return Hashing.combineOrdered(hashes);
  }

  @Override
  public int compareTo(final QueryNodeConfig o) {
    if (o == null) {
      return 1;
    }
    if (o == this) {
      return 0;
    }
    if (!(o instanceof ExpressionConfig)) {
      return 1;
    }
    
    return ComparisonChain.start()
        .compare(id, ((ExpressionConfig) o).id)
        .compare(expression, ((ExpressionConfig) o).expression)
        .compare(join_config, ((ExpressionConfig) o).join_config)
        .compare(variable_interpolators, ((ExpressionConfig) o).variable_interpolators, VARIABLE_INTERP_CMP)
        .compare(interpolator_configs, ((ExpressionConfig) o).interpolator_configs, INTERPOLATOR_CMP)
        .result();
  }

  @Override
  public boolean equals(final Object o) {
    if (o == null) {
      return false;
    }
    if (o == this) {
      return true;
    }
    if (!(o instanceof ExpressionConfig)) {
      return false;
    }
    
    final ExpressionConfig other = (ExpressionConfig) o;
    return Objects.equals(id, other.id) && 
           Objects.equals(expression, other.expression) &&
           Objects.equals(join_config, other.join_config) &&
           Objects.equals(variable_interpolators, other.variable_interpolators) &&
           Objects.equals(interpolator_configs, other.interpolator_configs);
  }

  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }

  /** @return A new builder. */
  public static Builder newBuilder() {
    return new Builder();
  }
  
  public static class Builder extends BaseQueryNodeConfigWithInterpolators.Builder {
    @JsonProperty
    private String expression;
    @JsonProperty
    private JoinConfig joinConfig;
    @JsonProperty
    private Map<String, QueryInterpolatorConfig> variable_interpolators;
    
    public Builder setExpression(final String expression) {
      this.expression = expression;
      return this;
    }
    
    public Builder setJoinConfig(final JoinConfig join) {
      this.joinConfig = join;
      return this;
    }
    
    public Builder setVariableInterpolators(
        final Map<String, QueryInterpolatorConfig> variable_interpolators) {
      this.variable_interpolators = variable_interpolators;
      return this;
    }
    
    public Builder addVariableInterpolator(final String variable, 
                                           final QueryInterpolatorConfig interpolator) {
      if (variable_interpolators == null) {
        variable_interpolators = Maps.newHashMap();
      }
      variable_interpolators.put(variable, interpolator);
      return this;
    }
    
    @Override
    public QueryNodeConfig build() {
      return new ExpressionConfig(this);
    }
    
  }
}
