// This file is part of OpenTSDB.
// Copyright (C) 2020  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.ratio;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.reflect.TypeToken;

import net.opentsdb.common.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.query.BaseQueryNodeConfigWithInterpolators;
import net.opentsdb.query.interpolation.QueryInterpolatorConfig;

/**
 * Config for the ratio node. 
 * 
 * @since 3.0
 */
@JsonInclude(Include.NON_DEFAULT)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonDeserialize(builder = RatioConfig.Builder.class)
public class RatioConfig extends BaseQueryNodeConfigWithInterpolators<
    RatioConfig.Builder, RatioConfig> {

  private final List<String> data_sources;
  private final boolean infectious_nan;
  private final String as;
  private final boolean as_percent;
  
  /**
   * Ctor.
   * @param builder The non-null builder.
   */
  private RatioConfig(final Builder builder) {
    super(builder);
    data_sources = builder.data_sources;
    infectious_nan = builder.infectiousNan;
    as = builder.as;
    as_percent = builder.as_percent;
    if (data_sources == null || data_sources.isEmpty()) {
      throw new IllegalArgumentException("Must have at least one data source.");
    }
    if (Strings.isNullOrEmpty(as)) {
      throw new IllegalArgumentException("The 'as' field must be set with a "
          + "non-empty string.");
    }
    Collections.sort(data_sources);
  }
  
  /** @return The list of data sources to work on. */
  public List<String> getDataSources() {
    return data_sources;
  }
  
  /** @return The metric name to use for the ratios. */
  public String getAs() {
    return as;
  }
  
  /** @return Whether or not NaNs should be treated as sentinels or considered 
   * in arithmetic. */
  public boolean getInfectiousNan() {
    return infectious_nan;
  }

  /** @return Whether or not to express the ratio as a percent. */
  public boolean getAsPercent() {
    return as_percent;
  }
  
  @Override
  public boolean pushDown() {
    return false;
  }

  @Override
  public boolean joins() {
    return false;
  }

  @Override
  public Builder toBuilder() {
    final Builder builder = new Builder()
        .setAs(as)
        .setDataSources(data_sources)
        .setAsPercent(as_percent)
        .setInfectiousNan(infectious_nan);
    super.toBuilder(builder);
    return builder;
  }
  
  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final RatioConfig config = (RatioConfig) o;
    return Objects.equal(as, config.getAs()) &&
        Objects.equal(data_sources, config.getDataSources()) &&
        Objects.equal(as_percent, config.getAsPercent()) &&
        Objects.equal(infectious_nan, config.getInfectiousNan()) &&
        Objects.equal(interpolator_configs, config.interpolator_configs) &&
        Objects.equal(id, config.getId());
  }
  
  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }

  /** @return A HashCode object for deterministic, non-secure hashing */
  public HashCode buildHashCode() {
    final List<HashCode> hashes =
        Lists.newArrayListWithCapacity(3);
    hashes.add(super.buildHashCode());
    
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
    
    final Hasher hasher = Const.HASH_FUNCTION().newHasher();
    for (int i = 0; i < data_sources.size(); i++) {
      hasher.putString(data_sources.get(i), Const.UTF8_CHARSET);
    }
    hasher.putString(as, Const.UTF8_CHARSET)
          .putBoolean(as_percent)
          .putBoolean(infectious_nan);
    hashes.add(hasher.hash());
    return Hashing.combineOrdered(hashes);
  }

  @Override
  public int compareTo(final RatioConfig o) {
    // TODO Auto-generated method stub
    return 0;
  }

  /**
   * Parses a JSON config.
   * @param mapper The non-null mapper.
   * @param tsdb The non-null TSDB for factories.
   * @param node The non-null node.
   * @return The parsed config.
   */
  public static RatioConfig parse(final ObjectMapper mapper,
                                  final TSDB tsdb,
                                  final JsonNode node) {
    Builder builder = new Builder();
    JsonNode n = node.get("dataSource");
    if (n != null && !n.isNull()) {
      builder.setDataSources(Lists.newArrayList(n.asText()));
    } else {
      n = node.get("dataSources");
      if (n != null && !n.isNull()) {
        List<String> sources = Lists.newArrayList();
        for (final JsonNode source : n) {
          sources.add(source.asText());
        }
        builder.setDataSources(sources);
      }
    }
    
    n = node.get("as");
    if (n != null && !n.isNull()) {
      builder.setAs(n.asText());
    }
    
    n = node.get("asPercent");
    if (n != null && !n.isNull()) {
      builder.setAsPercent(n.asBoolean());
    }
    
    BaseQueryNodeConfigWithInterpolators.parse(builder, mapper, tsdb, node);
    
    return builder.build();
  }
  
  /** @return A new builder to construct a RateOptions from. */
  public static Builder newBuilder() {
    return new Builder();
  }
  
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static final class Builder extends BaseQueryNodeConfigWithInterpolators.Builder<Builder, RatioConfig> {
    private List<String> data_sources;
    private boolean infectiousNan;
    private String as;
    private boolean as_percent;
    
    Builder() {
      setType(RatioFactory.TYPE);
    }
    
    public Builder setDataSources(final List<String> data_sources) {
      this.data_sources = data_sources;
      return this;
    }
    
    public Builder addDataSource(final String data_source) {
      if (data_sources == null) {
        data_sources = Lists.newArrayList();
      }
      data_sources.add(data_source);
      return this;
    }
    
    public Builder setAs(final String as) {
      this.as = as;
      return this;
    }
    
    /**
     * @param infectious_nan Whether or not NaNs should be sentinels or included
     * in arithmetic.
     * @return The builder.
     */
    public Builder setInfectiousNan(final boolean infectious_nan) {
      this.infectiousNan = infectious_nan;
      return this;
    }
    
    public Builder setAsPercent(final boolean as_percent) {
      this.as_percent = as_percent;
      return this;
    }
    
    public RatioConfig build() {
      return new RatioConfig(this);
    }

    @Override
    public Builder self() {
      return this;
    }
    
  }

}
