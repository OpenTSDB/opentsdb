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

import com.google.common.base.Strings;
import com.google.common.hash.HashCode;

import net.opentsdb.common.Const;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.ZonedNanoTimeStamp;
import net.opentsdb.data.TimeStamp.Op;
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
public class DownsampleConfig extends BaseQueryNodeConfigWithInterpolators 
                              implements TimeSpecification {
  
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
  
  /** The query. */
  private final TimeSeriesQuery query;
  
  /** The numeric part of the parsed interval. */
  private final int interval_part;
  
  /** The units of the parsed interval. */
  private final ChronoUnit units;
  
  /** The interval converted to a duration. */
  private final TemporalAmount duration;
  
  /** The snapped starting timestamp of the downsample span. */
  private final TimeStamp start;
  
  /** The snapped final timestamp of the downsample span. */
  private final TimeStamp end;
  
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
    if (builder.query == null) {
      throw new IllegalArgumentException("Query cannot be null.");
    }
    if (interpolator_configs == null || 
        interpolator_configs.isEmpty()) {
      throw new IllegalArgumentException("At least one interpolator "
          + "config must be present.");
    }
    interval = builder.interval;
    timezone = builder.timezone != null ? builder.timezone : Const.UTC;
    aggregator = builder.aggregator;
    infectious_nan = builder.infectious_nan;
    run_all = builder.run_all;
    fill = builder.fill;
    query = builder.query;
    
    if (!run_all) {
      interval_part = DateTime.getDurationInterval(interval);
      units = DateTime.unitsToChronoUnit(DateTime.getDurationUnits(interval));
      duration = DateTime.parseDuration2(interval);
    } else {
      interval_part = 0;
      units = null;
      duration = null;
    }
    
    net.opentsdb.query.pojo.TimeSeriesQuery q = 
        (net.opentsdb.query.pojo.TimeSeriesQuery) builder.query;
    if (run_all) {
      start = q.getTime().startTime();
      end = q.getTime().endTime();
    } else if (timezone != Const.UTC) {
      start = new ZonedNanoTimeStamp(q.getTime().startTime().epoch(), 
          q.getTime().startTime().nanos(), timezone);
      start.snapToPreviousInterval(interval_part, units);
      if (start.compare(Op.LT, q.getTime().startTime())) {
        nextTimestamp(start);
      }
      end = new ZonedNanoTimeStamp(q.getTime().endTime().epoch(), 
          q.getTime().endTime().nanos(), timezone);
      end.snapToPreviousInterval(interval_part, units);
      if (end.compare(Op.LTE, start)) {
        throw new IllegalArgumentException("Snapped end time: " + end 
            + " must be greater than the start time: " + start);
      }
    } else {
      start = q.getTime().startTime().getCopy();
      start.snapToPreviousInterval(interval_part, units);
      if (start.compare(Op.LT, q.getTime().startTime())) {
        nextTimestamp(start);
      }
      end = q.getTime().endTime().getCopy();
      end.snapToPreviousInterval(interval_part, units);
      if (end.compare(Op.LTE, start)) {
        throw new IllegalArgumentException("Snapped end time: " + end 
            + " must be greater than the start time: " + start);
      }
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
  
  /** @return The time series query. */
  public TimeSeriesQuery query() {
    return query;
  }
  
  /** @return The numeric part of the raw interval. */
  public int intervalPart() {
    return interval_part;
  }
  
  /** @return The units of the interval. */
  public ChronoUnit units() {
    return units;
  }
    
  @Override
  public TimeStamp start() {
    return start;
  }

  @Override
  public TimeStamp end() {
    return end;
  }

  @Override
  public void updateTimestamp(final int offset, final TimeStamp timestamp) {
    if (offset < 0) {
      throw new IllegalArgumentException("Negative offsets are not allowed.");
    }
    if (timestamp == null) {
      throw new IllegalArgumentException("Timestamp cannot be null.");
    }
    if (run_all) {
      timestamp.update(start);
    } else {
      final TimeStamp increment = new ZonedNanoTimeStamp(
          start.epoch(), start.msEpoch(), start.timezone());
      for (int i = 0; i < offset; i++) {
        increment.add(duration);
      }
      timestamp.update(increment);
    }
  }
  
  @Override
  public void nextTimestamp(final TimeStamp timestamp) {
    if (run_all) {
      timestamp.update(start);
    } else {
      timestamp.add(duration);
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
  
  public static class Builder extends BaseQueryNodeConfigWithInterpolators.Builder {
    private String interval;
    private ZoneId timezone;
    private String aggregator;
    private boolean infectious_nan;
    private boolean run_all;
    private boolean fill;
    private TimeSeriesQuery query;
    
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
    public Builder setTimeZone(final ZoneId timezone) {
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
    
    /**
     * @param query The non-null time series query.
     * @return The builder.
     */
    public Builder setQuery(final TimeSeriesQuery query) {
      this.query = query;
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
