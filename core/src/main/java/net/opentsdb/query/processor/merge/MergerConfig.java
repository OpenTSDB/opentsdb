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
package net.opentsdb.query.processor.merge;

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
import net.opentsdb.query.BaseQueryNodeConfigWithInterpolators;

import java.util.List;

@JsonInclude(Include.NON_NULL)
@JsonDeserialize(builder = MergerConfig.Builder.class)
public class MergerConfig extends BaseQueryNodeConfigWithInterpolators<MergerConfig.Builder, MergerConfig> {

  /** The data source we'll send up. */
  private final String data_source;
  
  /** The raw aggregator. */
  private final String aggregator;
  
  /** Whether or not NaNs are infectious. */
  private final boolean infectious_nan;
  
  protected MergerConfig(Builder builder) {
    super(builder);
    if (Strings.isNullOrEmpty(builder.aggregator)) {
      throw new IllegalArgumentException("Aggregator cannot be null or empty.");
    }
    if (Strings.isNullOrEmpty(builder.dataSource)) {
      throw new IllegalArgumentException("Data source cannot be null or empty.");
    }
    data_source = builder.dataSource;
    aggregator = builder.aggregator;
    infectious_nan = builder.infectious_nan;
  }
  
  public String getDataSource() {
    return data_source;
  }

  /** @return The non-null and non-empty aggregation function name. */
  public String getAggregator() {
    return aggregator;
  }
  
  /** @return Whether or not NaNs should be treated as sentinels or considered 
   * in arithmetic. */
  public boolean getInfectiousNan() {
    return infectious_nan;
  }

  @Override
  public Builder toBuilder() {
    return new Builder()
        .setDataSource(data_source)
        .setAggregator(aggregator)
        .setInfectiousNan(infectious_nan)
        .setInterpolatorConfigs(Lists.newArrayList(interpolator_configs.values()))
        .setId(id);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    // is this necessary?
    if (!super.equals(o)) {
      return false;
    }

    final MergerConfig merger = (MergerConfig) o;

    return Objects.equal(id, merger.getId()) &&
            Objects.equal(aggregator, merger.getAggregator())
            && Objects.equal(infectious_nan, merger.getInfectiousNan());

  }

  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }

  @Override
  /** @return A HashCode object for deterministic, non-secure hashing */
  public HashCode buildHashCode() {
    final HashCode hc = Const.HASH_FUNCTION().newHasher()
            .putString(Strings.nullToEmpty(aggregator), Const.UTF8_CHARSET)
            .putBoolean(infectious_nan)
            .hash();
    final List<HashCode> hashes =
            Lists.newArrayListWithCapacity(2);

    hashes.add(super.buildHashCode());
    hashes.add(hc);

    return Hashing.combineOrdered(hashes);
  }
  
  @Override
  public boolean pushDown() {
    return false;
  }

  @Override
  public boolean joins() {
    return true;
  }

  @Override
  public int compareTo(MergerConfig o) {
    // TODO Auto-generated method stub
    return 0;
  }
  
  /** @return A new builder to work from. */
  public static Builder newBuilder() {
    return new Builder();
  }
  
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Builder extends BaseQueryNodeConfigWithInterpolators.Builder<Builder, MergerConfig> {
    @JsonProperty
    private String dataSource;
    @JsonProperty
    private String aggregator;
    @JsonProperty
    private boolean infectious_nan;
    
    Builder() {
      setType(MergerFactory.TYPE);
    }
    
    public Builder setDataSource(final String data_source) {
      dataSource = data_source;
      return this;
    }
    
    /**
     * @param aggregator A non-null and non-empty aggregation function.
     * @return The builder.
     */
    public Builder setAggregator(final String aggregator) {
      this.aggregator = aggregator;
      return this;
    }
    
    /**
     * @param infectious_nan Whether or not NaNs should be sentinels or included
     * in arithmetic.
     * @return The builder.
     */
    public Builder setInfectiousNan(final boolean infectious_nan) {
      this.infectious_nan = infectious_nan;
      return this;
    }
    
    @Override
    public MergerConfig build() {
      return new MergerConfig(this);
    }

    @Override
    public Builder self() {
      return this;
    }
  }
}
