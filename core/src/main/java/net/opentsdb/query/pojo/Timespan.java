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
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.query.SliceConfig;
import net.opentsdb.utils.DateTime;

/**
 * Pojo builder class used for serdes of the timespan component of a query
 * @since 2.3
 */
@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonDeserialize(builder = Timespan.Builder.class)
public class Timespan extends Validatable implements Comparable<Timespan> {
  /** User given start date/time, could be relative or absolute */
  private String start;
  private TimeStamp start_ts;
  
  /** User given end date/time, could be relative, absolute or empty */
  private String end;
  private TimeStamp end_ts;
  
  /** User's timezone used for converting absolute human readable dates */
  private String timezone;

  /** An optional downsampler for all queries */
  private Downsampler downsampler;
  
  /** The global aggregator to use */
  private String aggregator; 
  
  /** Whether or not to compute a rate */
  private boolean rate;
  
  /** Optional rate options. */
  private RateOptions rate_options;

  /** A query slice config. */
  private String slice_config;
  
  /** A parsed slice config. */
  private SliceConfig slice;
  
  /**
   * Default ctor
   * @param builder The builder to pull values from
   */
  public Timespan(final Builder builder) {
    start = builder.start;
    end = builder.end;
    timezone = builder.timezone;
    downsampler = builder.downsampler;
    aggregator = builder.aggregator;
    rate = builder.rate;
    rate_options = builder.rateOptions;
    slice_config = builder.sliceConfig;
    
    if (builder.start_ts != null) {
      start_ts = builder.start_ts;
    } else {
      start_ts = new MillisecondTimeStamp(
          DateTime.parseDateTimeString(start, timezone));
    }
    
    if (builder.end_ts != null) {
      end_ts = builder.end_ts;
    } else {
      if (Strings.isNullOrEmpty(end)) {
        end_ts = new MillisecondTimeStamp(DateTime.currentTimeMillis());
      } else {
        end_ts = new MillisecondTimeStamp(
            DateTime.parseDateTimeString(end, timezone));
      }
    }
  }
  
  /** @return user given start date/time, could be relative or absolute */
  public String getStart() {
    return start;
  }

  /** @return user given end date/time, could be relative, absolute or empty */
  public String getEnd() {
    return end;
  }

  /** @return user's timezone used for converting absolute human readable dates */
  public String getTimezone() {
    return timezone;
  }

  /** @return an optional downsampler for all queries */
  public Downsampler getDownsampler() {
    return downsampler;
  }
  
  /** @return the global aggregator to use */
  public String getAggregator() {
    return aggregator;
  }
  
  /** @return whether or not to compute a rate */
  public boolean isRate() {
    return rate;
  }
  
  /** @return An optional config for rate calculations. */
  public RateOptions getRateOptions() {
    return rate_options;
  }
  
  /** @return The slice config if provided. May be null. */
  public SliceConfig sliceConfig() {
    return slice;
  }
  
  /** @return The slice config as a string (for serialization). */
  public String getSliceConfig() {
    return slice_config;
  }
  
  /** @return Returns the parsed start time. 
   * @see DateTime#parseDateTimeString(String, String) */
  public TimeStamp startTime() {
    return start_ts;
  }
  
  /** @return Returns the parsed end time. 
   * @see DateTime#parseDateTimeString(String, String) */
  public TimeStamp endTime() {
    return end_ts;
  }
  
  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    Timespan timespan = (Timespan) o;

    return Objects.equal(timespan.downsampler, downsampler)
        && Objects.equal(timespan.end, end)
        && Objects.equal(timespan.start, start)
        && Objects.equal(timespan.timezone, timezone)
        && Objects.equal(timespan.aggregator, aggregator)
        && Objects.equal(timespan.rate, rate)
        && Objects.equal(timespan.rate_options, rate_options)
        && Objects.equal(timespan.slice_config, slice_config);
  }

  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }

  /** @return A HashCode object for deterministic, non-secure hashing */
  public HashCode buildHashCode() {
    final HashCode hc = Const.HASH_FUNCTION().newHasher()
        .putString(Strings.nullToEmpty(start), Const.ASCII_CHARSET)
        .putString(Strings.nullToEmpty(end), Const.ASCII_CHARSET)
        .putString(Strings.nullToEmpty(timezone), Const.ASCII_CHARSET)
        .putString(Strings.nullToEmpty(aggregator), Const.ASCII_CHARSET)
        .putBoolean(rate)
        .putString(Strings.nullToEmpty(slice_config), Const.ASCII_CHARSET)
        .hash();
    final List<HashCode> hashes = Lists.newArrayListWithCapacity(2);
    hashes.add(hc);
    if (downsampler != null) {
      hashes.add(downsampler.buildHashCode());
    }
    if (rate_options != null) {
      hashes.add(rate_options.buildHashCode());
    }
    return Hashing.combineOrdered(hashes);
  }
  
  /** @return A HashCode object for deterministic, non-secure hashing without
   * the timestamps. */
  public HashCode buildTimelessHashCode() {
    final HashCode hc = Const.HASH_FUNCTION().newHasher()
        .putString(Strings.nullToEmpty(timezone), Const.ASCII_CHARSET)
        .putString(Strings.nullToEmpty(aggregator), Const.ASCII_CHARSET)
        .putBoolean(rate)
        .putString(Strings.nullToEmpty(slice_config), Const.ASCII_CHARSET)
        .hash();
    final List<HashCode> hashes = Lists.newArrayListWithCapacity(2);
    hashes.add(hc);
    if (downsampler != null) {
      hashes.add(downsampler.buildHashCode());
    }
    if (rate_options != null) {
      hashes.add(rate_options.buildHashCode());
    }
    return Hashing.combineOrdered(hashes);
  }
  
  @Override
  public int compareTo(final Timespan o) {
    return ComparisonChain.start()
        .compare(start, o.start, Ordering.natural().nullsFirst())
        .compare(end, o.end, Ordering.natural().nullsFirst())
        .compare(timezone, o.timezone, Ordering.natural().nullsFirst())
        .compare(downsampler, o.downsampler, Ordering.natural().nullsFirst())
        .compare(aggregator, o.aggregator, Ordering.natural().nullsFirst())
        .compareTrueFirst(rate, o.rate)
        .compare(rate_options, o.rate_options, Ordering.natural().nullsFirst())
        .compare(slice_config, o.slice_config, Ordering.natural().nullsFirst())
        .result();
  }
  
  /** @return A new builder for the downsampler */
  public static Builder newBuilder() {
    return new Builder();
  }
  
  /**
   * Clones an timespan into a new builder.
   * @param timespan A non-null timespan to pull values from
   * @return A new builder populated with values from the given timespan.
   * @throws IllegalArgumentException if the timespan was null.
   * @since 3.0
   */
  public static Builder newBuilder(final Timespan timespan) {
    if (timespan == null) {
      throw new IllegalArgumentException("Timespan cannot be null.");
    }
    final Builder builder = new Builder()
        .setAggregator(timespan.aggregator)
        .setDownsampler(timespan.downsampler)
        .setEnd(timespan.end)
        .setStart(timespan.start)
        .setRate(timespan.rate)
        .setRateOptions(timespan.rate_options)
        .setSliceConfig(timespan.slice_config)
        .setTimezone(timespan.timezone);
    builder.start_ts = timespan.start_ts;
    builder.end_ts = timespan.end_ts;
    return builder;
  }

  /** Validates the timespan
   * @throws IllegalArgumentException if one or more parameters were invalid
   */
  public void validate() {
    if (start == null || start.isEmpty()) {
      throw new IllegalArgumentException("missing or empty start");
    }
    DateTime.parseDateTimeString(start, timezone);
    
    if (end != null && !end.isEmpty()) {
      DateTime.parseDateTimeString(end, timezone);
    }
    
    if (downsampler != null) {
      downsampler.validate();
    }
    
    if (aggregator == null || aggregator.isEmpty()) {
      throw new IllegalArgumentException("Missing or empty aggregator");
    }
    
    try {
      Aggregators.get(aggregator.toLowerCase());
    } catch (final NoSuchElementException e) {
      throw new IllegalArgumentException("Invalid aggregator");
    }
    
    if (rate_options != null) {
      rate_options.validate();
    }
    
    if (!Strings.isNullOrEmpty(slice_config)) {
      slice = new SliceConfig(slice_config);
    }
  }
  
  /**
   * A builder for the downsampler component of a query
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "")
  public static final class Builder {
    @JsonProperty
    private String start;
    @JsonProperty
    private String end;
    @JsonProperty
    private String timezone;
    @JsonProperty
    private Downsampler downsampler;
    @JsonProperty
    private String aggregator;
    @JsonProperty
    private boolean rate;
    @JsonProperty
    private RateOptions rateOptions;
    @JsonProperty
    private String sliceConfig;
    // not publicly serdes'able. Only for copy builders.
    private TimeStamp start_ts;
    private TimeStamp end_ts;
    
    public Builder setStart(final String start) {
      this.start = start;
      start_ts = null;
      return this;
    }

    public Builder setEnd(final String end) {
      this.end = end;
      end_ts = null;
      return this;
    }

    public Builder setTimezone(final String timezone) {
      this.timezone = timezone;
      return this;
    }

    public Builder setDownsampler(final Downsampler downsample) {
      this.downsampler = downsample;
      return this;
    }

    @JsonIgnore
    public Builder setDownsampler(final Downsampler.Builder downsample) {
      this.downsampler = downsample.build();
      return this;
    }
    
    public Builder setAggregator(final String aggregator) {
      this.aggregator = aggregator;
      return this;
    }
    
    public Builder setRate(final boolean rate) {
      this.rate = rate;
      return this;
    }
    
    public Builder setRateOptions(final RateOptions rate_options) {
      this.rateOptions = rate_options;
      return this;
    }
    
    @JsonIgnore
    public Builder setRateOptions(final RateOptions.Builder rate_options) {
      this.rateOptions = rate_options.build();
      return this;
    }
    
    public Builder setSliceConfig(final String slice_config) {
      this.sliceConfig = slice_config;
      return this;
    }
    
    /**
     * Compiles and validates the timespan.
     * @return An instantiated timespan if successful.
     * @throws IllegalArgumentException if one of the parameters was 
     * missconfigured.
     */
    public Timespan build() {
      return new Timespan(this);
    }
  }
  
}
