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
package net.opentsdb.query.processor.topn;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;

import com.google.common.hash.Hashing;
import net.opentsdb.core.Const;
import net.opentsdb.query.BaseQueryNodeConfig;

import java.util.List;

/**
 * A config for TopN processor nodes.
 * 
 * @since 3.0
 */
@JsonInclude(Include.NON_NULL)
@JsonDeserialize(builder = TopNConfig.Builder.class)
public class TopNConfig extends BaseQueryNodeConfig<TopNConfig.Builder, TopNConfig> {

  /** The max number of time series to return. */
  private int count;
  
  /** Whether or not to return the top (highest values) or bottom (lowest
   * values) time series post aggregation. */
  private boolean top;
  
  /** The aggregator to use to sort the series. */
  private String aggregator;
  
  /** Whether or not NaNs are infectious. */
  private boolean infectious_nan;
  
  /**
   * Protected ctor.
   * @param builder Non-null builder.
   */
  protected TopNConfig(final Builder builder) {
    super(builder);
    count = builder.count;
    top = builder.top;
    aggregator = builder.aggregator;
    infectious_nan = builder.infectiousNan;
  }
  
  /** @return The maximum number of time series to return. */
  public int getCount() {
    return count;
  }
  
  /** @return Whether or not to return the top or bottom series. */
  public boolean getTop() {
    return top;
  }
  
  /** @return The aggregation function to use. */
  public String getAggregator() {
    return aggregator;
  }
  
  /** @return Whether or not NaNs should be treated as sentinels or considered 
   * in arithmetic. */
  public boolean getInfectiousNan() {
    return infectious_nan;
  }

  @Override
  public boolean pushDown() {
    // TODO Auto-generated method stub
    return false;
  }
  
  @Override
  public boolean joins() {
    return false;
  }

  @Override
  public Builder toBuilder() {
    final Builder builder = newBuilder()
        .setAggregator(aggregator)
        .setCount(count)
        .setTop(top)
        .setInfectiousNan(infectious_nan);
    super.toBuilder(builder);
    return builder;
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

    final TopNConfig tnconfig = (TopNConfig) o;

    return Objects.equal(count, tnconfig.getCount())
            && Objects.equal(top, tnconfig.getTop())
            && Objects.equal(aggregator, tnconfig.getAggregator())
            && Objects.equal(infectious_nan, tnconfig.getInfectiousNan());

  }

  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }

  @Override
  public HashCode buildHashCode() {
    if (cached_hash != null) {
      return cached_hash;
    }
    final HashCode hc = Const.HASH_FUNCTION().newHasher()
            .putInt(count)
            .putBoolean(top)
            .putString(Strings.nullToEmpty(aggregator), Const.UTF8_CHARSET)
            .putBoolean(infectious_nan)
            .hash();

    final List<HashCode> hashes =
            Lists.newArrayListWithCapacity(2);

    hashes.add(super.buildHashCode());

    hashes.add(hc);

    cached_hash = Hashing.combineOrdered(hashes);
    return cached_hash;
  }

  @Override
  public int compareTo(TopNConfig o) {
    // TODO Auto-generated method stub
    return 0;
  }
  
  /** @return A new builder to work from. */
  public static Builder newBuilder() {
    return new Builder();
  }
  
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Builder extends BaseQueryNodeConfig.Builder<Builder, TopNConfig> {
    @JsonProperty
    private int count;
    @JsonProperty
    private boolean top;
    @JsonProperty
    private String aggregator;
    @JsonProperty
    private boolean infectiousNan;
    
    Builder() {
      setType(TopNFactory.TYPE);
    }
    
    public Builder setCount(final int count) {
      this.count = count;
      return this;
    }
    
    public Builder setTop(final boolean top) {
      this.top = top;
      return this;
    }
    
    public Builder setAggregator(final String aggregator) {
      this.aggregator = aggregator;
      return this;
    }
    
    public Builder setInfectiousNan(final boolean infectious_nan) {
      this.infectiousNan = infectious_nan;
      return this;
    }
    
    @Override
    public TopNConfig build() {
      return new TopNConfig(this);
    }

    @Override
    public Builder self() {
      return this;
    }
  }

}
