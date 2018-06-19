// This file is part of OpenTSDB.
// Copyright (C) 2017-2018  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.downsample;

import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Strings;
import com.google.common.hash.HashCode;

import net.opentsdb.common.Const;
import net.opentsdb.query.BaseQueryNodeConfigWithInterpolators;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.TimeSeriesQuery;
import net.opentsdb.utils.DateTime;

/**
 * A configuration implementation for Downsampling processors.
 * <p>
 * Given the {@link TimeSeriesQuery}, the builder will parse out the start and 
 * end timestamps and snap to an interval greater than or equal to the query
 * start time and less than or equal to the query end time using the interval.
 * <p>
 * If the end time would be the same as the start, after snapping, the builder
 * will throw an {@link IllegalArgumentException}.
 * <p>
 * By default, snaps are to the UTC calendar. If you want to snap to a different
 * calendar for hourly and greater intervals, please supply the timezone.
 * @since 3.0
 */
@JsonInclude(Include.NON_NULL)
@JsonDeserialize(builder = DownsampleConfig.Builder.class)
public class DownsampleConfig extends BaseQueryNodeConfigWithInterpolators {
  
  /** The raw interval string. */
  private final String interval;
  
  /** The timezone for the downsample snapping. Defaults to UTC. */
  private final ZoneId timezone;
  
  /** The raw aggregator. */
  private final String aggregator;
  
  /** Whether or not NaNs are infectious. */
  private final boolean infectious_nan;
  
  /** Whether or not to reduce to a single value. */
  private final boolean run_all;
  
  /** Whether or not to fill empty timestamps. */
  private final boolean fill;
  
  /** The numeric part of the parsed interval. */
  private final int interval_part;
  
  /** The units of the parsed interval. */
  private final ChronoUnit units;
  
  /** The interval converted to a duration. */
  private final TemporalAmount duration;
  
  /**
   * Default ctor.
   * @param builder A non-null builder to pull settings from.
   */
  DownsampleConfig(final Builder builder) {
    super(builder);
    if (Strings.isNullOrEmpty(builder.interval)) {
      throw new IllegalArgumentException("Interval cannot be null or empty.");
    }
    if (Strings.isNullOrEmpty(builder.aggregator)) {
      throw new IllegalArgumentException("Aggregator cannot be null or empty.");
    }
    if (interpolator_configs == null || 
        interpolator_configs.isEmpty()) {
      throw new IllegalArgumentException("At least one interpolator "
          + "config must be present.");
    }
    interval = builder.interval;
    timezone = builder.timezone != null ? ZoneId.of(builder.timezone) : Const.UTC;
    aggregator = builder.aggregator;
    infectious_nan = builder.infectious_nan;
    run_all = builder.run_all;
    fill = builder.fill;
    
    if (!run_all) {
      interval_part = DateTime.getDurationInterval(interval);
      units = DateTime.unitsToChronoUnit(DateTime.getDurationUnits(interval));
      duration = DateTime.parseDuration2(interval);
    } else {
      interval_part = 0;
      units = null;
      duration = null;
    }
  }
  
  /** @return The non-null and non-empty interval. */
  public TemporalAmount interval() {
    return duration;
  }
  
  /** @return The non-null timezone. */
  public ZoneId timezone() {
    return timezone;
  }
  
  /** @return The non-null and non-empty aggregation function name. */
  public String aggregator() {
    return aggregator;
  }
  
  /** @return Whether or not NaNs should be treated as sentinels or considered 
   * in arithmetic. */
  public boolean infectiousNan() {
    return infectious_nan;
  }
  
  /** @return Whether or not to downsample to a single value. */
  public boolean runAll() {
    return run_all;
  }
  
  /** @return Whether or not to fill missing values. */
  public boolean fill() {
    return fill;
  }
  
  /** @return The numeric part of the raw interval. */
  public int intervalPart() {
    return interval_part;
  }
  
  /** @return The units of the interval. */
  public ChronoUnit units() {
    return units;
  }

  public TemporalAmount duration() {
    return duration;
  }
  
  /**
   * Converts the units to a 2x style parseable string.
   * @return
   */
  public String intervalAsString() {
    switch (units) {
    case NANOS:
      return interval_part + "ns";
    case MICROS:
      return interval_part + "mu";
    case MILLIS:
      return interval_part + "ms";
    case SECONDS:
      return interval_part + "s";
    case MINUTES:
      return interval_part + "m";
    case HOURS:
      return interval_part + "h";
    case DAYS:
      return interval_part + "d";
    case WEEKS:
      return interval_part + "w";
    case MONTHS:
      return interval_part + "n";
    case YEARS:
      return interval_part + "y";
    default:
      throw new IllegalStateException("Unsupported units: " + units);
    }
  }
  
  @Override
  public int compareTo(QueryNodeConfig o) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public HashCode buildHashCode() {
    // TODO Auto-generated method stub
    return null;
  }
  
  /** @return A new builder to work from. */
  public static Builder newBuilder() {
    return new Builder();
  }
  
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Builder extends BaseQueryNodeConfigWithInterpolators.Builder {
    @JsonProperty
    private String interval;
    @JsonProperty
    private String timezone;
    @JsonProperty
    private String aggregator;
    @JsonProperty
    private boolean infectious_nan;
    @JsonProperty
    private boolean run_all;
    @JsonProperty
    private boolean fill;
    
    /**
     * @param id A non-null and on-empty Id for the group by function.
     * @return The builder.
     */
    public Builder setId(final String id) {
      this.id = id;
      return this;
    }
    
    /**
     * @param interval The non-null and non-empty interval, e.g. "1h" or "60s"
     * @return The builder.
     */
    public Builder setInterval(final String interval) {
      this.interval = interval;
      return this;
    }
    
    /**
     * @param timezone An optional timezone. If null, UTC is assumed.
     * @return The builder.
     */
    public Builder setTimeZone(final String timezone) {
      this.timezone = timezone;
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
    
    /**
     * @param run_all Whether or not to downsample to a single value.
     * @return The builder.
     */
    public Builder setRunAll(final boolean run_all) {
      this.run_all = run_all;
      return this;
    }
    
    /**
     * @param fill Whether or not to fill empty values.
     * @return The builder.
     */
    public Builder setFill(final boolean fill) {
      this.fill = fill;
      return this;
    }
    
    /** @return The constructed config.
     * @throws IllegalArgumentException if a required parameter is missing or
     * invalid. */
    public QueryNodeConfig build() {
      return (QueryNodeConfig) new DownsampleConfig(this);
    }
  }
  
}
