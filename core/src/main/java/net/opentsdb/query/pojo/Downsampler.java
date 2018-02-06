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
 * Pojo builder class used for serdes of the downsampler component of a query
 * 
 * @since 2.3
 */
@JsonInclude(Include.NON_NULL)
@JsonDeserialize(builder = Downsampler.Builder.class)
public class Downsampler extends Validatable implements Comparable<Downsampler> {
  /** The relative interval with value and unit, e.g. 60s */
  private String interval;
  
  /** The aggregator to use for downsampling */
  private String aggregator;
  
  /** A fill policy for downsampling and working with missing values */
  private NumericFillPolicy fill_policy;
  
  /** Whether or not to use the calendar for downsampling. */
  private boolean use_calendar;
  
  /** An optional timezone for downsampling if different from the primary. */
  private String timezone;
  
  /**
   * Default ctor
   * @param builder The builder to pull values from
   */
  protected Downsampler(final Builder builder) {
    interval = builder.interval;
    aggregator = builder.aggregator;
    fill_policy = builder.fillPolicy;
    use_calendar = builder.useCalendar;
    timezone = builder.timezone;
  }
  
  /** @return A new builder for the downsampler */
  public static Builder newBuilder() {
    return new Builder();
  }
  
  /** 
   * Clones a downsampler into a new builder.
   * @param downsampler A non-null downsampler to pull values from.
   * @return A new builder populated with values from the given downsampler. 
   * @throws IllegalArgumentException if the downsampler was null.
   * @since 3.0
   */
  public static Builder newBuilder(final Downsampler downsampler) {
    if (downsampler == null) {
      throw new IllegalArgumentException("Downsampler cannot be null.");
    }
    final Builder builder =  new Builder()
        .setAggregator(downsampler.aggregator)
        .setInterval(downsampler.interval)
        .setUseCalendar(downsampler.use_calendar)
        .setTimezone(downsampler.timezone);
    if (downsampler.fill_policy != null) {
      builder.setFillPolicy(downsampler.fill_policy);
    }
    return builder;
  }
  
  /** Validates the downsampler
   * @throws IllegalArgumentException if one or more parameters were invalid
   */
  public void validate() {
    if (interval == null || interval.isEmpty()) {
      throw new IllegalArgumentException("Missing or empty interval");
    }
    DateTime.parseDuration(interval);
    
    if (aggregator == null || aggregator.isEmpty()) {
      throw new IllegalArgumentException("Missing or empty aggregator");
    }
    try {
      Aggregators.get(aggregator.toLowerCase());
    } catch (final NoSuchElementException e) {
      throw new IllegalArgumentException("Invalid aggregator");
    }
    
    if (fill_policy != null) {
      fill_policy.validate();
    }
  }
  
  /** @return the interval for the downsampler */
  public String getInterval() {
    return interval;
  }
  
  /** @return the name of the aggregator to use */
  public String getAggregator() {
    return aggregator;
  }
  
  /** @return the fill policy to use */
  public NumericFillPolicy getFillPolicy() {
    return fill_policy;
  }
  
  /** @return Whether or not the fill policy has been set. */
  public boolean hasFillPolicy() {
    return fill_policy != null;
  }
  
  /** @return Whether or not to use the calendar for downsampling buckets. */
  public boolean useCalendar() {
    return use_calendar;
  }
  
  /** @return An optional timezone for downsampling. May be null. */
  public String getTimezone() {
    return timezone;
  }
  
  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    final Downsampler downsampler = (Downsampler) o;

    return Objects.equal(interval, downsampler.interval)
        && Objects.equal(aggregator, downsampler.aggregator)
        && Objects.equal(fill_policy, downsampler.fill_policy)
        && Objects.equal(use_calendar, downsampler.use_calendar)
        && Objects.equal(timezone, downsampler.timezone);
  }
  
  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }

  /** @return A HashCode object for deterministic, non-secure hashing */
  public HashCode buildHashCode() {
    final HashCode hc = Const.HASH_FUNCTION().newHasher()
        .putString(Strings.nullToEmpty(interval), Const.ASCII_CHARSET)
        .putString(Strings.nullToEmpty(aggregator), Const.ASCII_CHARSET)
        .putBoolean(use_calendar)
        .putString(Strings.nullToEmpty(timezone), Const.ASCII_CHARSET)
        .hash();
    final List<HashCode> hashes = Lists.newArrayListWithCapacity(2);
    hashes.add(hc);
    if (fill_policy != null) {
      hashes.add(fill_policy.buildHashCode());
    }
    return Hashing.combineOrdered(hashes);
  }
  
  @Override
  public int compareTo(final Downsampler o) {
    return ComparisonChain.start()
        .compare(interval, o.interval, Ordering.natural().nullsFirst())
        .compare(aggregator, o.aggregator, Ordering.natural().nullsFirst())
        .compare(fill_policy, o.fill_policy, Ordering.natural().nullsFirst())
        .compareTrueFirst(use_calendar, o.use_calendar)
        .compare(timezone, o.timezone, Ordering.natural().nullsFirst())
        .result();
  }
  
  /**
   * A builder for the downsampler component of a query
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "")
  public static final class Builder {
    @JsonProperty
    private String interval;
    @JsonProperty
    private String aggregator;
    @JsonProperty
    private NumericFillPolicy fillPolicy;
    @JsonProperty
    private boolean useCalendar;
    @JsonProperty
    private String timezone;
    
    public Builder setInterval(final String interval) {
      this.interval = interval;
      return this;
    }
    
    public Builder setAggregator(final String aggregator) {
      this.aggregator = aggregator;
      return this;
    }
    
    public Builder setFillPolicy(final NumericFillPolicy fill_policy) {
      this.fillPolicy = fill_policy;
      return this;
    }
    
    @JsonIgnore
    public Builder setFillPolicy(final NumericFillPolicy.Builder fill_policy) {
      this.fillPolicy = fill_policy.build();
      return this;
    }
    
    public Builder setUseCalendar(final boolean use_calendar) {
      this.useCalendar = use_calendar;
      return this;
    }
    
    public Builder setTimezone(final String timezone) {
      this.timezone = timezone;
      return this;
    }
    
    public Downsampler build() {
      return new Downsampler(this);
    }
  }
}
