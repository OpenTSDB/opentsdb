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

import java.time.Duration;
import java.time.temporal.ChronoUnit;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.ComparisonChain;
import com.google.common.hash.HashCode;

import net.opentsdb.core.Const;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.utils.DateTime;

/**
 * Provides additional options that will be used when calculating rates. These
 * options are useful when working with metrics that are raw counter values, 
 * where a counter is defined by a value that always increases until it hits
 * a maximum value and then it "rolls over" to start back at 0.
 * <p>
 * These options will only be utilized if the query is for a rate calculation
 * and if the "counter" options is set to true.
 * @since 2.0
 */
@JsonInclude(Include.NON_DEFAULT)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonDeserialize(builder = RateOptions.Builder.class)
public class RateOptions extends Validatable implements Comparable<RateOptions>,
  QueryNodeConfig {
  public static final long DEFAULT_RESET_VALUE = 0;
  public static final String DEFAULT_INTERVAL = "1s";
  public static final long DEFAULT_COUNTER_MAX = Long.MAX_VALUE;

  /** The ID of this config. */
  private String id;
  
  /**
   * If true, then when calculating a rate of change assume that the metric
   * values are counters and thus non-zero, always increasing and wrap around at
   * some maximum. */
  private boolean counter;
  
  /** Whether or not to simply drop rolled-over or reset data points */
  private boolean drop_resets;

  /**
   * If calculating a rate of change over a metric that is a counter, then this
   * value specifies the maximum value the counter will obtain before it rolls
   * over. This value will default to Long.MAX_VALUE.
   */
  private long counter_max = DEFAULT_COUNTER_MAX;

  /**
   * Specifies the the rate change value which, if exceeded, will be considered
   * a data anomaly, such as a system reset of the counter, and the rate will be
   * returned as a zero value for a given data point.
   */
  private long reset_value;
  
  /** The rate interval in duration format. Default is 1 seconds as per TSDB 1/2 */
  private String interval = DEFAULT_INTERVAL;

  private Duration duration;
  private ChronoUnit units;
  
  /** Used for Jackson non-default serdes. */
  protected RateOptions() {
    
  }
  
  /**
   * Ctor
   */
  protected RateOptions(final Builder builder) {
    id = builder.id;
    counter = builder.counter;
    drop_resets = builder.dropResets;
    counter_max = builder.counterMax;
    reset_value = builder.resetValue;
    interval = builder.interval;
    
    final long interval_part = DateTime.getDurationInterval(interval);
    units = DateTime.unitsToChronoUnit(DateTime.getDurationUnits(interval));
    duration = Duration.of(interval_part, units);
  }
  
  /** @return Whether or not the counter flag is set */
  public boolean isCounter() {
    return counter;
  }

  /** @return The counter max value */
  public long getCounterMax() {
    return counter_max;
  }

  /** @return The optional reset value for anomaly suppression */
  public long getResetValue() {
    return reset_value;
  }

  /** @return Whether or not to drop rolled-over or reset counters */
  public boolean getDropResets() {
    return drop_resets;
  }
  
  /** @return The rate interval in duration format. Default is 1 seconds as 
   * per TSDB 1/2. */
  public String getInterval() {
    return interval;
  }

  @Override
  public String getId() {
    return id;
  }
  
  /** @return The duration of the rate to convert to. E.g. per second or per
   * 8 seconds, etc. */
  public Duration duration() {
    return duration;
  }
  
  /** @return The parsed units of the interval. */
  public ChronoUnit units() {
    return units;
  }
  
  /**
   * Generates a String version of the rate option instance in a format that 
   * can be utilized in a query.
   * @return string version of the rate option instance.
   */
  public String toString() {
    StringBuilder buf = new StringBuilder();
    buf.append('{');
    buf.append(counter);
    buf.append(',').append(counter_max);
    buf.append(',').append(reset_value);
    buf.append('}');
    return buf.toString();
  }

  /** Validates the config
   * @throws IllegalArgumentException if one or more parameters were invalid
   */
  public void validate() {
    if (Strings.isNullOrEmpty(interval)) {
      throw new IllegalArgumentException("Interval cannot be null or empty.");
    }
    DateTime.parseDuration2(interval);
  }
  
  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
   final RateOptions options = (RateOptions) o;
   return Objects.equal(counter, options.counter)
       && Objects.equal(drop_resets, options.drop_resets)
       && Objects.equal(counter_max, options.counter_max)
       && Objects.equal(reset_value, options.reset_value)
       && Objects.equal(interval, options.interval);
  }
  
  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }
  
  /** @return A HashCode object for deterministic, non-secure hashing */
  public HashCode buildHashCode() {
    return Const.HASH_FUNCTION().newHasher()
        .putBoolean(counter)
        .putBoolean(drop_resets)
        .putLong(counter_max)
        .putLong(reset_value)
        .putString(interval, Const.UTF8_CHARSET)
        .hash();
  }
  
  @Override
  public int compareTo(final RateOptions o) {
    return ComparisonChain.start()
        .compareTrueFirst(counter, o.counter)
        .compareTrueFirst(drop_resets, o.drop_resets)
        .compare(counter_max, o.counter_max)
        .compare(reset_value, o.reset_value)
        .compare(interval, o.interval)
        .result();
  }
  
  /** @return A new builder to construct a RateOptions from. */
  public static Builder newBuilder() {
    return new Builder();
  }
  
  /**
   * Clones an options into a new builder.
   * @param options A non-null options to pull values from
   * @return A new builder populated with values from the given options.
   * @throws IllegalArgumentException if the options was null.
   * @since 3.0
   */
  public static Builder newBuilder(final RateOptions options) {
    if (options == null) {
      throw new IllegalArgumentException("RateOptions cannot be null.");
    }
    return new Builder()
        .setCounter(options.counter)
        .setCounterMax(options.counter_max)
        .setResetValue(options.reset_value)
        .setDropResets(options.drop_resets)
        .setInterval(options.interval);
  }
  
  /**
   * A builder for the rate options config for a query.
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "")
  public static final class Builder {
    private String id;
    @JsonProperty
    private boolean counter;
    @JsonProperty
    private boolean dropResets;
    @JsonProperty
    private long counterMax = DEFAULT_COUNTER_MAX;
    @JsonProperty
    private long resetValue = DEFAULT_RESET_VALUE;
    @JsonProperty
    private String interval = DEFAULT_INTERVAL;
    
    public Builder setId(final String id) {
      this.id = id;
      return this;
    }
    
    public Builder setCounter(final boolean counter) {
      this.counter = counter;
      return this;
    }
    
    public Builder setDropResets(final boolean drop_resets) {
      this.dropResets = drop_resets;
      return this;
    }
    
    public Builder setCounterMax(final long counter_max) {
      this.counterMax = counter_max;
      return this;
    }
    
    public Builder setResetValue(final long counter_reset) {
      this.resetValue = counter_reset;
      return this;
    }
    
    public Builder setInterval(final String interval) {
      this.interval = interval;
      return this;
    }
    
    public RateOptions build() {
      return new RateOptions(this);
    }
  }
  
}
