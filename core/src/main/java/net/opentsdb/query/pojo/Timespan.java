// This file is part of OpenTSDB.
// Copyright (C) 2015-2017  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
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

import net.opentsdb.core.Aggregators;
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
  
  /** User given end date/time, could be relative, absolute or empty */
  private String end;
  
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
  
  /** @return Parses the start time and converts it to a {@link TimeStamp}. 
   * @see DateTime#parseDateTimeString(String, String) */
  public TimeStamp startTime() {
    return new MillisecondTimeStamp(
        DateTime.parseDateTimeString(start, timezone));
  }
  
  /** @return Parses the end time and converts it to a {@link TimeStamp} if set.
   * If the time was not set, uses {@link DateTime#currentTimeMillis()}. 
   * @see DateTime#parseDateTimeString(String, String) */
  public TimeStamp endTime() {
    if (Strings.isNullOrEmpty(end)) {
      return new MillisecondTimeStamp(DateTime.currentTimeMillis());
    }
    return new MillisecondTimeStamp(
        DateTime.parseDateTimeString(end, timezone));
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
    return new Builder()
        .setAggregator(timespan.aggregator)
        .setDownsampler(timespan.downsampler)
        .setEnd(timespan.end)
        .setStart(timespan.start)
        .setRate(timespan.rate)
        .setRateOptions(timespan.rate_options)
        .setSliceConfig(timespan.slice_config)
        .setTimezone(timespan.timezone);
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
    
    public Builder setStart(final String start) {
      this.start = start;
      return this;
    }

    public Builder setEnd(final String end) {
      this.end = end;
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
    
    public Timespan build() {
      return new Timespan(this);
    }
  }
  
}
