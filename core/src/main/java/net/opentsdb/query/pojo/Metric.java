// This file is part of OpenTSDB.
// Copyright (C) 2015-2017  The OpenTSDB Authors.
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
package net.opentsdb.query.pojo;

import java.util.List;
import java.util.NoSuchElementException;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;

import net.opentsdb.data.types.numeric.Aggregators;
import net.opentsdb.core.Const;
import net.opentsdb.utils.DateTime;

/**
 * Pojo builder class used for serdes of a metric component of a query
 * @since 2.3
 */
@JsonInclude(Include.NON_NULL)
@JsonDeserialize(builder = Metric.Builder.class)
public class Metric extends Validatable implements Comparable<Metric> {
  /** The name of the metric */
  private String metric;
  
  /** An ID for the metric */
  private String id;
  
  /** The ID of a filter set */
  private String filter;
  
  /** An optional time offset for time over time expressions */
  private String time_offset;
  
  /** An optional aggregation override for the metric */
  private String aggregator;
  
  /** A fill policy for dealing with missing values in the metric */
  private NumericFillPolicy fill_policy;

  /** An optional downsampler for this metric. */
  private Downsampler downsampler;
  
  /** Whether or not to convert to a rate. */
  private boolean is_rate;
  
  /** Optional rate options. */
  private RateOptions rate_options;
  
  /**
   * Default ctor
   * @param builder The builder to pull values from
   */
  protected Metric(final Builder builder) {
    metric = builder.metric;
    id = builder.id;
    filter = builder.filter;
    time_offset = builder.timeOffset;
    aggregator = builder.aggregator;
    fill_policy = builder.fillPolicy;
    downsampler = builder.downsampler;
    is_rate = builder.isRate;
    rate_options = builder.rateOptions;
  }

  /** @return the name of the metric */
  public String getMetric() {
    return metric;
  }

  /** @return an ID for the metric */
  public String getId() {
    return id;
  }

  /** @return the ID of a filter set */
  public String getFilter() {
    return filter;
  }

  /** @return an optional time offset for time over time expressions */
  public String getTimeOffset() {
    return time_offset;
  }

  /** @return an optional aggregation override for the metric */
  public String getAggregator() {
    return aggregator;
  }
  
  /** @return a fill policy for dealing with missing values in the metric */
  public NumericFillPolicy getFillPolicy() {
    return fill_policy;
  }
  
  /** @return An optional downsampler for this metric. */
  public Downsampler getDownsampler() {
    return downsampler;
  }
  
  /** @return Whether or not to convert to a rate. */
  public boolean isRate() {
    return is_rate;
  }
  
  /** @return An optional rate options config. */
  public RateOptions getRateOptions() {
    return rate_options;
  }
  
  /** @return A new builder for the metric */
  public static Builder newBuilder() {
    return new Builder();
  }
  
  /**
   * Clones an metric into a new builder.
   * @param metric A non-null metric to pull values from
   * @return A new builder populated with values from the given metric.
   * @throws IllegalArgumentException if the metric was null.
   * @since 3.0
   */
  public static Builder newBuilder(final Metric metric) {
    if (metric == null) {
      throw new IllegalArgumentException("Metric cannot be null.");
    }
    return new Builder()
        .setAggregator(metric.aggregator)
        .setDownsampler(metric.downsampler)
        .setFillPolicy(metric.fill_policy)
        .setFilter(metric.filter)
        .setId(metric.id)
        .setMetric(metric.metric)
        .setTimeOffset(metric.time_offset);
  }

  /** Validates the metric
   * @throws IllegalArgumentException if one or more parameters were invalid
   */
  public void validate() {
    if (metric == null || metric.isEmpty()) {
      throw new IllegalArgumentException("missing or empty metric");
    }

    if (id == null || id.isEmpty()) {
      throw new IllegalArgumentException("missing or empty id");
    }
    TimeSeriesQuery.validateId(id);

    if (time_offset != null) {
      DateTime.parseDateTimeString(time_offset, null);
    }
    
    if (aggregator != null && !aggregator.isEmpty()) {
      try {
        Aggregators.get(aggregator.toLowerCase());
      } catch (final NoSuchElementException e) {
        throw new IllegalArgumentException("Invalid aggregator");
      }
    }

    if (fill_policy != null) {
      fill_policy.validate();
    }
    
    if (downsampler != null) {
      downsampler.validate();
    }
    
    if (rate_options != null) {
      rate_options.validate();
    }
  }
  
  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    final Metric that = (Metric) o;

    return Objects.equal(that.filter, filter)
        && Objects.equal(that.id, id)
        && Objects.equal(that.metric, metric)
        && Objects.equal(that.time_offset, time_offset)
        && Objects.equal(that.aggregator, aggregator)
        && Objects.equal(that.fill_policy, fill_policy)
        && Objects.equal(that.downsampler, downsampler)
        && Objects.equal(that.is_rate, is_rate)
        && Objects.equal(that.rate_options, rate_options);
  }

  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }

  /** @return A HashCode object for deterministic, non-secure hashing */
  public HashCode buildHashCode() {
    final HashCode hc = Const.HASH_FUNCTION().newHasher()
        .putString(Strings.nullToEmpty(metric), Const.UTF8_CHARSET)
        .putString(Strings.nullToEmpty(id), Const.UTF8_CHARSET)
        .putString(Strings.nullToEmpty(filter), Const.UTF8_CHARSET)
        .putString(Strings.nullToEmpty(time_offset), Const.UTF8_CHARSET)
        .putString(Strings.nullToEmpty(aggregator), Const.UTF8_CHARSET)
        .putBoolean(is_rate)
        .hash();
    final List<HashCode> hashes = Lists.newArrayListWithCapacity(2);
    hashes.add(hc);
    if (fill_policy != null) {
      hashes.add(fill_policy.buildHashCode());
    }
    if (downsampler != null) {
      hashes.add(downsampler.buildHashCode());
    }
    if (rate_options != null) {
      hashes.add(rate_options.buildHashCode());
    }
    return Hashing.combineOrdered(hashes);
  }

  @Override
  public int compareTo(final Metric o) {
    return ComparisonChain.start()
        .compare(id, o.id, Ordering.natural().nullsFirst())
        .compare(metric, o.metric, Ordering.natural().nullsFirst())
        .compare(filter, o.filter, Ordering.natural().nullsFirst())
        .compare(time_offset, o.time_offset, Ordering.natural().nullsFirst())
        .compare(aggregator, o.aggregator, Ordering.natural().nullsFirst())
        .compare(fill_policy, o.fill_policy, Ordering.natural().nullsFirst())
        .compare(downsampler, o.downsampler, Ordering.natural().nullsFirst())
        .compareTrueFirst(is_rate, o.is_rate)
        .compare(rate_options, o.rate_options, Ordering.natural().nullsFirst())
        .result();
  }

  /**
   * A builder for a metric component of a query
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "")
  public static final class Builder {
    @JsonProperty
    private String metric;
    @JsonProperty
    private String id;
    @JsonProperty
    private String filter;
    @JsonProperty
    private String timeOffset;
    @JsonProperty
    private String aggregator;
    @JsonProperty
    private NumericFillPolicy fillPolicy;
    @JsonProperty
    private Downsampler downsampler;
    @JsonProperty
    private boolean isRate;
    @JsonProperty
    private RateOptions rateOptions;
    
    public Builder setMetric(String metric) {
      this.metric = metric;
      return this;
    }

    public Builder setId(String id) {
      TimeSeriesQuery.validateId(id);
      this.id = id;
      return this;
    }

    public Builder setFilter(String filter) {
      this.filter = filter;
      return this;
    }

    public Builder setTimeOffset(String time_offset) {
      this.timeOffset = time_offset;
      return this;
    }

    public Builder setAggregator(String aggregator) {
      this.aggregator = aggregator;
      return this;
    }

    public Builder setFillPolicy(NumericFillPolicy fill_policy) {
      this.fillPolicy = fill_policy;
      return this;
    }
    
    @JsonIgnore
    public Builder setFillPolicy(NumericFillPolicy.Builder fill_policy) {
      this.fillPolicy = fill_policy.build();
      return this;
    }
    
    public Builder setDownsampler(final Downsampler downsampler) {
      this.downsampler = downsampler;
      return this;
    }
    
    @JsonIgnore
    public Builder setDownsampler(final Downsampler.Builder downsampler) {
      this.downsampler = downsampler.build();
      return this;
    }
    
    public Builder setIsRate(final boolean is_rate) {
      isRate = is_rate;
      return this;
    }
    
    public Builder setRateOptions(final RateOptions rate_options) {
      rateOptions = rate_options;
      return this;
    }
    
    @JsonIgnore
    public Builder setRateOptions(final RateOptions.Builder rate_options) {
      rateOptions = rate_options.build();
      return this;
    }
    
    public Metric build() {
      return new Metric(this);
    }
  }
}
