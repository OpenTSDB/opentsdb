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
package net.opentsdb.query.anomaly.egads.olympicscoring;

import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;

import net.opentsdb.core.Const;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.anomaly.BaseAnomalyConfig;

/**
 * TODO ------- SUPER IMPORTANT!!! EQUALS AND HASH
 *
 */
public class OlympicScoringConfig extends BaseAnomalyConfig {
  
  private final SemanticQuery baseline_query;
  private final String baseline_period;
  private final int baseline_num_periods;
  private final String baseline_aggregator;
  private final int exclude_max;
  private final int exclude_min;
  
  protected OlympicScoringConfig(final Builder builder) {
    super(builder);
    baseline_query = builder.baselineQuery;
    baseline_period = builder.baselinePeriod;
    baseline_num_periods = builder.baselineNumPeriods;
    baseline_aggregator = builder.baselineAggregator;
    exclude_max = builder.excludeMax;
    exclude_min = builder.excludeMin;
  }
  
  public SemanticQuery getBaselineQuery() {
    return baseline_query;
  }

  public String getBaselinePeriod() {
    return baseline_period;
  }
  
  public int getBaselineNumPeriods() {
    return baseline_num_periods;
  }

  public String getBaselineAggregator() {
    return baseline_aggregator;
  }
  
  public int getExcludeMax() {
    return exclude_max;
  }

  public int getExcludeMin() {
    return exclude_min;
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
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int compareTo(Object o) {
    // TODO Auto-generated method stub
    return 0;
  }
  
  @Override
  public boolean equals(final Object o) {
    if (o == null) {
      return false;
    }
    if (o == this) {
      return true;
    }
    
    if (!(o instanceof OlympicScoringConfig)) {
      return false;
    }
    
    final OlympicScoringConfig config = (OlympicScoringConfig) o;
    return Objects.equals(baseline_period, config.baseline_period) &&
        Objects.equals(baseline_num_periods, config.baseline_num_periods) &&
        Objects.equals(baseline_aggregator, config.baseline_aggregator) &&
        Objects.equals(exclude_max, config.exclude_max) &&
        Objects.equals(exclude_min, config.exclude_min) &&
        baseline_query.equals(config.baseline_query) &&
        super.equals(config);
  }
  
  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }
  
  @Override
  public HashCode buildHashCode() {
    HashCode hash = Const.HASH_FUNCTION().newHasher()
        .putString(baseline_period, Const.UTF8_CHARSET)
        .putInt(baseline_num_periods)
        .putString(baseline_aggregator, Const.UTF8_CHARSET)
        .putInt(exclude_max)
        .putInt(exclude_min)
        .hash();
    final List<HashCode> hashes = Lists.newArrayListWithCapacity(3);
    hashes.add(hash);
    hashes.add(baseline_query.buildHashCode());
    hashes.add(super.buildHashCode());
    return Hashing.combineOrdered(hashes);
  }
  
  public static Builder newBuilder() {
    return new Builder();
  }
  
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Builder extends BaseAnomalyConfig.Builder<
      Builder, OlympicScoringConfig> {
    @JsonProperty
    private SemanticQuery baselineQuery;
    @JsonProperty
    private String baselinePeriod;
    @JsonProperty
    private int baselineNumPeriods;
    @JsonProperty
    private String baselineAggregator;
    @JsonProperty
    private int excludeMax;
    @JsonProperty
    private int excludeMin;
    
    Builder() {
      setType(OlympicScoringFactory.TYPE);
    }
    
    public Builder setBaselineQuery(final SemanticQuery baseline_query) {
      baselineQuery = baseline_query;
      return this;
    }
    
    public Builder setBaselinePeriod(final String baseline_period) {
      baselinePeriod = baseline_period;
      return this;
    }
    
    public Builder setBaselineNumPeriods(final int baseline_num_periods) {
      baselineNumPeriods = baseline_num_periods;
      return this;
    }
    
    public Builder setBaselineAggregator(final String baseline_aggregator) {
      baselineAggregator = baseline_aggregator;
      return this;
    }
    
    public Builder setExcludeMax(final int exclude_max) {
      excludeMax = exclude_max;
      return this;
    }
    
    public Builder setExcludeMin(final int exclude_min) {
      excludeMin = exclude_min;
      return this;
    }
    
    @Override
    public Builder self() {
      return this;
    }

    @Override
    public OlympicScoringConfig build() {
      return new OlympicScoringConfig(this);
    }
    
  }
}
