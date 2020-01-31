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
package net.opentsdb.query.processor.slidingwindow;

import java.time.temporal.TemporalAmount;
import java.util.List;

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
import net.opentsdb.utils.DateTime;

/**
 * The configuration class for a sliding window node.
 * <p>
 * The {@link #getWindowSize()} is the width of the window as a 
 * resolution.
 * <p>
 * The {@link #getAggregator()} is the computation to perform.
 * 
 * TODO - calendaring?
 * 
 * @since 3.0
 */
@JsonInclude(Include.NON_NULL)
@JsonDeserialize(builder = SlidingWindowConfig.Builder.class)
public class SlidingWindowConfig extends BaseQueryNodeConfig<SlidingWindowConfig.Builder, SlidingWindowConfig> {
  private final String window_size;
  private final String aggregator;
  private final boolean infectious_nan;
  private final TemporalAmount window;
  
  /**
   * Protected ctor.
   * @param builder The non-null builder.
   */
  protected SlidingWindowConfig(final Builder builder) {
    super(builder);
    if (Strings.isNullOrEmpty(builder.windowSize)) {
      throw new IllegalArgumentException("Window size cannot be null.");
    }
    if (Strings.isNullOrEmpty(builder.aggregator)) {
      throw new IllegalArgumentException("Aggregator cannot be null.");
    }
    
    window_size = builder.windowSize;
    aggregator = builder.aggregator;
    infectious_nan = builder.infectiousNan;
    window = DateTime.parseDuration2(window_size);
  }

  /** @return The non-null and non-empty window size as a duration, 
   * e.g. "1m" for 1 minute. */
  public String getWindowSize() {
    return window_size;
  }
  
  /** @return The non-null and non-empty aggregation function. */
  public String getAggregator() {
    return aggregator;
  }

  /** @return Whether or not NaNs should be treated as sentinels or considered 
   * in arithmetic. */
  public boolean getInfectiousNan() {
    return infectious_nan;
  }
  
  /** @return The parsed window. */
  public TemporalAmount window() {
    return window;
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
    return null;
  }

  @Override
  public HashCode buildHashCode() {
    if (cached_hash != null) {
      return cached_hash;
    }
    
    final HashCode hc = Const.HASH_FUNCTION().newHasher()
            .putString(Strings.nullToEmpty(window_size), net.opentsdb.core.Const.UTF8_CHARSET)
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
  public int compareTo(SlidingWindowConfig o) {
    // TODO Auto-generated method stub
    return 0;
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

    final SlidingWindowConfig swconfig = (SlidingWindowConfig) o;


    return Objects.equal(window_size, swconfig.getWindowSize())
            && Objects.equal(aggregator, swconfig.getAggregator())
            && Objects.equal(infectious_nan, swconfig.getInfectiousNan());

  }

  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }
  
  /** @return A new builder to work from. */
  public static Builder newBuilder() {
    return new Builder();
  }
  
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Builder extends BaseQueryNodeConfig.Builder<Builder, SlidingWindowConfig> {
    @JsonProperty
    private String windowSize;
    @JsonProperty
    private String aggregator;
    @JsonProperty
    private boolean infectiousNan;
    
    Builder() {
      setType(SlidingWindowFactory.TYPE);
    }
    
    public Builder setWindowSize(final String window_size) {
      this.windowSize = window_size;
      return this;
    }
    
    public Builder setAggregator(final String aggregator) {
      this.aggregator = aggregator;
      return this;
    }
    
    @Override
    public SlidingWindowConfig build() {
      return new SlidingWindowConfig(this);
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

    @Override
    public Builder self() {
      return this;
    }
  }

}
