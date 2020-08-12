// This file is part of OpenTSDB.
// Copyright (C) 2017-2020  The OpenTSDB Authors.
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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;

import com.google.common.hash.Hashing;
import net.opentsdb.common.Const;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.Op;
import net.opentsdb.query.BaseQueryNodeConfigWithInterpolators;
import net.opentsdb.query.TimeSeriesQuery;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.Pair;

import java.time.Duration;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.util.List;

/**
 * A configuration implementation for Downsampling processors.
 * <p>
 * Given the {@link TimeSeriesQuery}, the builder will parse out the start and
 * end timestamps and snap to an interval greater than or equal to the query
 * start time and less than or equal to the query end time using the interval.
 * <p>
 * Note that there is an implicit runall conversion when the delta of the time
 * stamps from the query matches the interval. In that case the runall flag will
 * be set to true.
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
public class DownsampleConfig extends BaseQueryNodeConfigWithInterpolators<
  DownsampleConfig.Builder, DownsampleConfig> {
  /** The given start timestamp. */
  private final String start;

  /** The given end timestamp. */
  private final String end;

  /** The raw, original and optional min interval string. */
  private String interval;
  private final String original_interval;
  private final String min_interval;
  
  /** An optional reporting interval for regular metrics. */
  private final String reporting_interval;
  private final long reporting_interval_ms;
  
  /** Compute the number of values we expect in this interval if reporting is
   * set. */
  private final int dps_in_interval;

  /** The timezone for the downsample snapping. Defaults to UTC. */
  private final ZoneId timezone;

  /** The raw aggregator. */
  private final String aggregator;

  /** Whether or not NaNs are infectious. */
  private final boolean infectious_nan;

  /** Whether or not to reduce to a single value. */
  private boolean run_all;

  /** Whether or not to fill empty timestamps. */
  private final boolean fill;
  
  /** Whether or not to process the data as arrays. */
  private boolean process_as_arrays;

  /** The numeric part of the parsed interval. */
  private final int interval_part;

  /** The units of the parsed interval. */
  private final ChronoUnit units;

  /** The interval converted to a duration. */
  private final TemporalAmount duration;

  /** The computed start time for downsampling (First value) */
  private final TimeStamp start_time;

  /** The computed end time for downsampling (Last value) */
  private final TimeStamp end_time;

  /** A reference to the auto downsample interval config. */
  private final List<Pair<Long, String>> intervals;

  /** The cached intervals. */
  private int cached_intervals = -1;

  /**
   * Default ctor.
   * @param builder A non-null builder to pull settings from.
   */
  protected DownsampleConfig(final Builder builder) {
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

    intervals = builder.intervals;
    min_interval = builder.minInterval;
    reporting_interval = builder.reportingInterval;
    if (!Strings.isNullOrEmpty(reporting_interval)) {
      reporting_interval_ms = DateTime.parseDuration(reporting_interval);
    } else {
      reporting_interval_ms = 0;
    }
    timezone = builder.timezone != null ? ZoneId.of(builder.timezone) : Const.UTC;
    aggregator = builder.aggregator;
    infectious_nan = builder.infectious_nan;
    run_all = builder.run_all || builder.interval.toLowerCase().contains("all");
    fill = builder.fill;
    process_as_arrays = builder.process_as_arrays;
    if (Strings.isNullOrEmpty(builder.original_interval)) {
      original_interval = builder.interval;
    } else {
      original_interval = builder.original_interval;
    }

    if (!Strings.isNullOrEmpty(builder.start)) {
      start_time = new MillisecondTimeStamp(
              DateTime.parseDateTimeString(builder.start, builder.timezone));
    } else {
      start_time = null;
    }
    if (!Strings.isNullOrEmpty(builder.end)) {
      end_time = new MillisecondTimeStamp(
              DateTime.parseDateTimeString(builder.end, builder.timezone));
    } else {
      end_time = new MillisecondTimeStamp(DateTime.currentTimeMillis());
    }

    if (!run_all) {
      // try for auto.
      if (builder.interval.equalsIgnoreCase("auto")) {
        if (start_time != null && end_time != null) {
          if (intervals == null) {
            throw new IllegalArgumentException("Auto downsampling is not "
                    + "configured or enabled.");
          }
          // TODO - handle smaller scales
          final long delta = end_time.msEpoch() - start_time.msEpoch();
          interval = DownsampleFactory.getAutoInterval(delta, intervals, min_interval);
        } else {
          // we've just been parsed, not setup, so set back to auto.
          interval = "auto";
        }
      } else {
        interval = builder.interval;
      }

      if (interval.equalsIgnoreCase("auto")) {
        interval_part = DateTime.getDurationInterval("1m");
        units = DateTime.unitsToChronoUnit(DateTime.getDurationUnits("1m"));
        duration = DateTime.parseDuration2("1m");
      } else {
        final TemporalAmount duration = DateTime.parseDuration2(interval);
        if (start_time != null && end_time != null && 
           duration.equals(Duration.of(
               end_time.epoch() - start_time.epoch(), ChronoUnit.SECONDS))) {
          // implicit run-all
          run_all = true;
          interval = "0all";
          interval_part = 0;
          units = null;
          this.duration = null;
        } else {
          interval_part = DateTime.getDurationInterval(interval);
          units = DateTime.unitsToChronoUnit(DateTime.getDurationUnits(interval));
          this.duration = duration;
        }
      }
    } else {
      interval = "0all";
      interval_part = 0;
      units = null;
      duration = null;
    }
    
    start = builder.start;
    if (!Strings.isNullOrEmpty(builder.start)) {
      if (!run_all) {
        final TimeStamp original = start_time.getCopy();
        start_time.snapToPreviousInterval(interval_part, units);
        if (start_time.compare(Op.LT, original)) {
          start_time.add(duration);
        }
      }
    }

    end = builder.end;
    if (!Strings.isNullOrEmpty(builder.end) && !run_all) {
      end_time.snapToPreviousInterval(interval_part, units);
      // TODO - fall to next interval?
    } else if (!run_all) {
      end_time.snapToPreviousInterval(interval_part, units);
    }
        
    if (reporting_interval_ms > 0) {
      if (interval.equalsIgnoreCase("0all")) {
        dps_in_interval = (int) ((end_time.msEpoch() - start_time.msEpoch()) / reporting_interval_ms);
      } else if (interval.equalsIgnoreCase("auto")) {
        dps_in_interval = 0;
      } else {
        final long ds = DateTime.parseDuration(interval);
        if ((ds % reporting_interval_ms) != 0) {
          throw new IllegalArgumentException("The interval must be an multiple of "
              + "the reporting interval.");
        }
        dps_in_interval = (int) (ds / reporting_interval_ms);
      }
    } else {
      dps_in_interval = 0;
    }
    
    // make sure we have at least one interval in our range
    // TODO - propper difference function
    if (!run_all && start_time != null && end_time.msEpoch() - start_time.msEpoch() <
            DateTime.parseDuration(interval)) {
      throw new IllegalArgumentException("The start and stop times of "
              + "the query must be greater than the interval provided: " + interval 
              + ". Difference after alignment was " 
              + (end_time.msEpoch() - start_time.msEpoch()) + " with start=" 
              + start_time.msEpoch() + " and end=" + end_time.msEpoch());
    }
  }

  /** @return The non-null and non-empty interval. */
  public TemporalAmount interval() {
    return duration;
  }

  public String getMinInterval() {
    return min_interval;
  }
  
  public String getReportingInterval() {
    return reporting_interval;
  }
  
  public int dpsInInterval() {
    return dps_in_interval;
  }
  
  /** @return The non-null timezone. */
  public ZoneId timezone() {
    return timezone;
  }

  /** @return The non-null timezone string name. */
  public String getTimezone() {
    return timezone.toString();
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

  /** @return Whether or not to downsample to a single value. */
  public boolean getRunAll() {
    return run_all;
  }

  /** @return Whether or not to process the data as arrays. */
  public boolean getProcessAsArrays() {
    return process_as_arrays;
  }
  
  public void setProcessAsArrays(boolean b) {
    process_as_arrays = b;
  }

  /** @return Whether or not to fill missing values. */
  public boolean getFill() {
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

  /**  @return Converts the units to a 2x style parseable string. */
  public String getInterval() {
    if (units == null) {
      return "0all";
    }

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

  /** @return The original interval given to the config, may be "auto". */
  public String getOriginalInterval() {
    return original_interval;
  }
  
  public String getStart() { return start; }

  public String getEnd() { return end; }

  /** @return The non-null computed start time for downsampling. */
  public TimeStamp startTime() {
    return start_time;
  }

  /** @return The non-null computed end time for downsampling. */
  public TimeStamp endTime() {
    return end_time;
  }

  /** @return The number of intervals in the downsampling window bounded
   * by {@link #startTime()} and {@link #endTime()}. */
  public int intervals() {
    if (run_all) {
      return 1;
    }

    if (cached_intervals < 0) {
      TimeStamp ts = start_time.getCopy();
      int intervals = 0;
      while (ts.compare(Op.LT, end_time)) {
        intervals++;
        ts.add(duration);
      }
      cached_intervals = intervals;
    }
    return cached_intervals;
  }

  /**
   * Returns the auto intervals for this factory.
   * <b>WARNING:</b> Do NOT modify the list or entries.
   * @return The non-null intervals list.
   */
  public List<Pair<Long, String>> autoIntervals() {
    return intervals;
  }

  @Override
  public boolean pushDown() {
    return true;
  }
  
  @Override
  public boolean joins() {
    return false;
  }
  
  @Override
  public Builder toBuilder() {
    return new Builder()
        .setInterval(interval)
        .setMinInterval(min_interval)
        .setReportingInterval(reporting_interval)
        .setTimeZone(timezone.toString())
        .setAggregator(aggregator)
        .setInfectiousNan(infectious_nan)
        .setRunAll(run_all)
        .setFill(fill)
        .setStart(start)
        .setProcessAsArrays(process_as_arrays)
        .setEnd(end)
        .setIntervals(intervals)
        .setInterpolatorConfigs(Lists.newArrayList(interpolator_configs.values()))
        .setResultIds(result_ids != null ? Lists.newArrayList(result_ids) : null)
        .setId(id);
  }

  public static void cloneBuilder(DownsampleConfig config, Builder builder) {
    builder
        .setStart(config.start)
        .setEnd(config.end)
        .setAggregator(config.aggregator)
        .setFill(config.fill)
        .setProcessAsArrays(config.process_as_arrays)
        .setRunAll(config.run_all)
        .setInfectiousNan(config.infectious_nan)
        .setInterval(config.interval)
        .setMinInterval(config.min_interval)
        .setReportingInterval(config.reporting_interval)
        .setTimeZone(config.timezone.toString())
        .setIntervals(config.intervals)
        .setOriginalInterval(config.original_interval)
        .setInterpolatorConfigs(Lists.newArrayList(config.interpolator_configs.values()))
        .setSources(Lists.newArrayList(config.getSources()))
        .setResultIds(config.result_ids != null ? Lists.newArrayList(config.result_ids) : null)
        .setId(config.id);
  }

  @Override
  public int compareTo(DownsampleConfig o) {
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

    final DownsampleConfig dsconfig = (DownsampleConfig) o;
    
    return Objects.equal(timezone.toString(), dsconfig.getTimezone())
            && Objects.equal(min_interval, dsconfig.min_interval)
            && Objects.equal(reporting_interval, dsconfig.reporting_interval)
            && Objects.equal(original_interval, dsconfig.original_interval)
            && Objects.equal(aggregator, dsconfig.getAggregator())
            && Objects.equal(infectious_nan, dsconfig.getInfectiousNan())
            && Objects.equal(run_all, dsconfig.getRunAll())
            && Objects.equal(fill, dsconfig.getFill());
  }

  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }

  @Override
  /** @return A HashCode object for deterministic, non-secure hashing */
  public HashCode buildHashCode() {
    final HashCode hc = net.opentsdb.core.Const.HASH_FUNCTION().newHasher()
            .putString(Strings.nullToEmpty(interval), Const.UTF8_CHARSET)
            .putString(Strings.nullToEmpty(min_interval), Const.UTF8_CHARSET)
            .putString(Strings.nullToEmpty(reporting_interval), Const.UTF8_CHARSET)
            .putString(Strings.nullToEmpty(timezone.toString()), Const.UTF8_CHARSET)
            .putString(Strings.nullToEmpty(aggregator), Const.UTF8_CHARSET)
            .putBoolean(infectious_nan)
            .putBoolean(run_all)
            .putBoolean(fill)
            .hash();

    final List<HashCode> hashes =
            Lists.newArrayListWithCapacity(2);

    hashes.add(super.buildHashCode());

    hashes.add(hc);

    return Hashing.combineOrdered(hashes);
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Builder extends 
      BaseQueryNodeConfigWithInterpolators.Builder<Builder, DownsampleConfig> {
    @JsonProperty
    private String interval;
    @JsonProperty
    private String minInterval;
    @JsonProperty
    private String reportingInterval;
    @JsonProperty
    private String timezone;
    @JsonProperty
    private String aggregator;
    @JsonProperty
    private boolean infectious_nan;
    @JsonProperty
    private boolean run_all;
    @JsonProperty
    private boolean process_as_arrays = true;
    @JsonProperty
    private boolean fill;
    @JsonProperty
    private String start;
    @JsonProperty
    private String end;
    private List<Pair<Long, String>> intervals;
    private String original_interval;

    Builder() {
      setType(DownsampleFactory.TYPE);
    }

    @Override
    public Builder self() {
      return this;
    }

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
     * which is dynamic.
     * @return The builder.
     */
    public Builder setInterval(final String interval) {
      this.interval = interval;
      return this;
    }

    public Builder setMinInterval(final String interval) {
      this.minInterval = interval;
      return this;
    }
    
    public Builder setReportingInterval(final String reporting_interval) {
      reportingInterval = reporting_interval;
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
     * @param process_as_arrays Whether or not to downsample into arrays.
     * @return The builder.
     */
    public Builder setProcessAsArrays(final boolean process_as_arrays) {
      this.process_as_arrays = process_as_arrays;
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

    public Builder setStart(final String start) {
      this.start = start;
      return this;
    }

    public Builder setEnd(final String end) {
      this.end = end;
      return this;
    }

    public Builder setIntervals(final List<Pair<Long, String>> intervals) {
      this.intervals = intervals;
      return this;
    }

    public Builder setOriginalInterval(final String original_interval) {
      this.original_interval = original_interval;
      return this;
    }
    
    /** @return The constructed config.
     * @throws IllegalArgumentException if a required parameter is missing or
     * invalid. */
    public DownsampleConfig build() {
      return new DownsampleConfig(this);
    }
  }

}