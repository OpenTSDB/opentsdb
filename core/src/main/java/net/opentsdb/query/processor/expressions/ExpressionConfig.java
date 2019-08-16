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

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.TreeMap;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Strings;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.reflect.TypeToken;

import net.opentsdb.core.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.query.BaseQueryNodeConfigWithInterpolators;
import net.opentsdb.query.interpolation.QueryInterpolatorConfig;
import net.opentsdb.query.interpolation.QueryInterpolatorFactory;
import net.opentsdb.query.joins.JoinConfig;

/**
 * Represents a single arithmetic and/or logical expression involving 
 * (for now) numeric time series.
 * 
 * TODO - overrides in hashes/equals/compareto
 * 
 * @since 3.0
 */
@JsonInclude(Include.NON_NULL)
@JsonDeserialize(builder = ExpressionConfig.Builder.class)
public class ExpressionConfig extends BaseQueryNodeConfigWithInterpolators<ExpressionConfig.Builder, ExpressionConfig> {
  
  /** The original expression string. */
  private final String expression;
  
  /** The non-null join config. */
  private final JoinConfig join_config;
  
  /** An optional map of variable to interpolators to override the defaults. */
  private final Map<String, List<QueryInterpolatorConfig>> variable_interpolators;
  
  /** Whether or not NaN is infectious. */
  private final boolean infectious_nan;
  
  /** The resulting metric name. */
  private final String as;
  
  /** Whether or not to substitute missing metrics with somewhat useful values. */
  private final boolean substitute_missing;
  
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
    infectious_nan = builder.infectiousNan;
    if (Strings.isNullOrEmpty(builder.as)) {
      as = getId();
    } else {
      as = builder.as;
    }
    substitute_missing = builder.substituteMissing;
  }
  
  /** @return The raw expression string to be parsed. */
  public String getExpression() {
    return expression;
  }
  
  /** @return The join config. */
  public JoinConfig getJoin() {
    return join_config;
  }
  
  /** @return A possibly null map of variable names to interpolators. */
  public Map<String, List<QueryInterpolatorConfig>> getVariableInterpolators() {
    return variable_interpolators;
  }
  
  /** @return Whether or not nans are infectious. */
  public boolean getInfectiousNan() {
    return infectious_nan;
  }
  
  /** @return The new name for the metric. */
  public String getAs() {
    return as;
  }
  
  /** @return Whether or not to substitute values for missing time series. */
  public boolean getSubstituteMissing() {
    return substitute_missing;
  }
  
  /**
   * Helper to pull out the proper config based on the optional variable 
   * name.
   * @param type The non-null data type.
   * @param variable An optional variable name.
   * @return An interpolator or null if none is configured for the given type.
   */
  public QueryInterpolatorConfig interpolatorConfig(final TypeToken<?> type, 
                                                    final String variable) {
    QueryInterpolatorConfig config = null;
    if (!Strings.isNullOrEmpty(variable) && variable_interpolators != null) {
      final List<QueryInterpolatorConfig> configs = variable_interpolators.get(variable);
      if (configs != null) {
        for (final QueryInterpolatorConfig cfg : configs) {
          if (cfg.type() == type) {
            config = cfg;
            break;
          }
        }
      }
    }
    
    if (config != null) {
      return config;
    }
    
    return interpolatorConfig(type);
  }
  
  @Override
  public boolean pushDown() {
    // TODO Auto-generated method stub
    return false;
  }
  
  @Override
  public boolean joins() {
    return true;
  }

  @Override
  public Builder toBuilder() {
    Builder cloneBuilder = new Builder();
    cloneBuilder(this,cloneBuilder);

    return cloneBuilder;
  }

  @Override
  public HashCode buildHashCode() {
    final List<HashCode> hashes = Lists.newArrayListWithExpectedSize(2);
    hashes.add(join_config.buildHashCode());
    hashes.add(Const.HASH_FUNCTION().newHasher()
        .putBoolean(infectious_nan)
        .putString(id == null ? "null" : id, Const.UTF8_CHARSET)
        .putString(expression, Const.UTF8_CHARSET)
        .putString(as == null ? "null" : as, Const.UTF8_CHARSET)
        .putBoolean(substitute_missing)
        .hash());
    if (variable_interpolators != null && !variable_interpolators.isEmpty()) {
      final Map<String, List<QueryInterpolatorConfig>> sorted = 
          new TreeMap<String, List<QueryInterpolatorConfig>>(variable_interpolators);
      for (final Entry<String, List<QueryInterpolatorConfig>> entry : sorted.entrySet()) {
        Collections.sort(entry.getValue());
        for (final QueryInterpolatorConfig cfg : entry.getValue()) {
          hashes.add(cfg.buildHashCode());
        }
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
  public int compareTo(final ExpressionConfig o) {
    if (o == null) {
      return 1;
    }
    if (o == this) {
      return 0;
    }

    return ComparisonChain.start()
        .compare(id, o.id)
        .compare(expression, o.expression)
        .compare(join_config, o.join_config)
        .compare(variable_interpolators, o.variable_interpolators, VARIABLE_INTERP_CMP)
        .compare(interpolator_configs, o.interpolator_configs, INTERPOLATOR_CMP)
        .compare(infectious_nan, o.infectious_nan)
        .compare(as, o.as)
        .compare(substitute_missing, o.substitute_missing)
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

    if (!super.equals(o)) {
      return false;
    }
    
    final ExpressionConfig other = (ExpressionConfig) o;
    return Objects.equals(id, other.id) && 
           Objects.equals(expression, other.expression) &&
           Objects.equals(join_config, other.join_config) &&
           Objects.equals(variable_interpolators, other.variable_interpolators) &&
           Objects.equals(interpolator_configs, other.interpolator_configs) &&
           Objects.equals(infectious_nan, other.infectious_nan) &&
           Objects.equals(as, other.as) &&
           Objects.equals(substitute_missing, other.substitute_missing);
  }

  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }

  public static ExpressionConfig parse(final ObjectMapper mapper,
                                       final TSDB tsdb,
                                       final JsonNode node) {
    Builder builder = new Builder();
    JsonNode n = node.get("expression");
    if (n != null) {
      builder.setExpression(n.asText());
    }
    
    n = node.get("join");
    if (n != null) {
      try {
        builder.setJoinConfig(mapper.treeToValue(n, JoinConfig.class));
      } catch (JsonProcessingException e) {
        throw new IllegalArgumentException("Unable to parse JoinConfig", e);
      }
    }
    
    n = node.get("infectiousNan");
    if (n != null) {
      builder.setInfectiousNan(n.asBoolean());
    }
    
    n = node.get("as");
    if (n != null) {
      builder.setAs(n.asText());
    }
    
    n = node.get("id");
    if (n != null) {
      builder.setId(n.asText());
    }
    
    n = node.get("substituteMissing");
    if (n != null && !n.isNull()) {
      builder.setSubstituteMissing(n.asBoolean());
    }
    
    n = node.get("variableInterpolators");
    if (n != null && !n.isNull()) {
      final Iterator<Entry<String, JsonNode>> iterator = n.fields();
      while (iterator.hasNext()) {
        final Entry<String, JsonNode> entry = iterator.next();
        for (final JsonNode config : entry.getValue()) {
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
          builder.addVariableInterpolator(entry.getKey(), interpolator_config);
        }
      }
    }
    
    n = node.get("interpolatorConfigs");
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
    
    n = node.get("sources");
    if (n != null && !n.isNull()) {
      try {
        builder.setSources(mapper.treeToValue(n, List.class));
      } catch (JsonProcessingException e) {
        throw new IllegalArgumentException("Failed to parse json", e);
      }
    }
    
    return builder.build();
  }

  /**
   * Following the pattern of DefaultTimeSeriesDataSourceConfig
   */
  public static void cloneBuilder(
          final ExpressionConfig expressionConfig, final Builder builder) {

    final JoinConfig join = expressionConfig.getJoin();

    builder.setId(expressionConfig.getId())
           .setAs(expressionConfig.getAs())
           .setType(expressionConfig.getType())
           .setSources(expressionConfig.getSources() == null ? null : Lists.newArrayList(expressionConfig.getSources()))
           .setInterpolatorConfigs(expressionConfig.getInterpolatorConfigs() == null ? null : Lists.newArrayList(expressionConfig.getInterpolatorConfigs()) )
           .setVariableInterpolators(expressionConfig.getVariableInterpolators() == null ? null : Maps.newHashMap(expressionConfig.getVariableInterpolators()))
           .setOverrides(expressionConfig.getOverrides() == null ? null : Maps.newHashMap(expressionConfig.getOverrides()))
           .setExpression(expressionConfig.getExpression())
           .setJoinConfig(join.toBuilder().build())
           .setInfectiousNan(expressionConfig.getInfectiousNan());

  }

  public static Builder newBuilder(){
    return new Builder();
  }
  
  public static class Builder extends BaseQueryNodeConfigWithInterpolators.Builder<Builder, ExpressionConfig> {
    @JsonProperty
    private String expression;
    @JsonProperty
    private JoinConfig joinConfig;
    @JsonProperty
    private Map<String, List<QueryInterpolatorConfig>> variable_interpolators;
    @JsonProperty
    private boolean infectiousNan;
    @JsonProperty
    private String as;
    @JsonProperty
    private boolean substituteMissing;
    
    Builder() {
      setType(ExpressionFactory.TYPE);
    }
    
    public Builder setExpression(final String expression) {
      this.expression = expression;
      return this;
    }
    
    public Builder setJoinConfig(final JoinConfig join) {
      this.joinConfig = join;
      return this;
    }
    
    public Builder setVariableInterpolators(
        final Map<String, List<QueryInterpolatorConfig>> variable_interpolators) {
      this.variable_interpolators = variable_interpolators;
      return this;
    }
    
    public Builder addVariableInterpolator(final String variable, 
                                           final QueryInterpolatorConfig interpolator) {
      if (variable_interpolators == null) {
        variable_interpolators = Maps.newHashMap();
      }
      List<QueryInterpolatorConfig> configs = variable_interpolators.get(variable);
      if (configs == null) {
        configs = Lists.newArrayList();
        variable_interpolators.put(variable, configs);
      }
      configs.add(interpolator);
      return this;
    }
    
    public Builder setInfectiousNan(final boolean infectious_nan) {
      this.infectiousNan = infectious_nan;
      return this;
    }
    
    public Builder setAs(final String as) {
      this.as = as;
      return this;
    }
    
    public Builder setSubstituteMissing(final boolean substitute_missing) {
      this.substituteMissing = substitute_missing;
      return this;
    }
    
    @Override
    public ExpressionConfig build() {
      return new ExpressionConfig(this);
    }

    @Override
    public Builder self() {
      return this;
    }
  }

  public static class InterpCmp 
    implements Comparator<Map<String, List<QueryInterpolatorConfig>>> {
  
    @Override
    public int compare(final Map<String, List<QueryInterpolatorConfig>> a, 
        Map<String, List<QueryInterpolatorConfig>> b) {
      if (a == b || a == null && b == null) {
        return 0;
      }
      if (a == null && b != null) {
        return -1;
      }
      if (b == null && a != null) {
        return 1;
      }
      if (a.size() > b.size()) {
        return -1;
      }
      if (b.size() > a.size()) {
        return 1;
      }
      for (final Entry<String, List<QueryInterpolatorConfig>> entry : a.entrySet()) {
        final List<QueryInterpolatorConfig> b_value = b.get(entry.getKey());
        if (b_value == null && entry.getValue() != null) {
          return 1;
        }
        if (entry.getValue().size() != b_value.size()) {
          return entry.getValue().size() - b_value.size();
        }
        for (final QueryInterpolatorConfig cfg : entry.getValue()) {
          if (!b_value.contains(cfg)) {
            return -1;
          }
        }
      }
      return 0;
    }
  
  }
  
  private static final InterpCmp VARIABLE_INTERP_CMP = new InterpCmp();
}
