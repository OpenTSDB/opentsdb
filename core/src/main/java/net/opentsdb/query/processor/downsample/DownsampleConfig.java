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
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;

import net.opentsdb.common.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.Op;
import net.opentsdb.query.BaseQueryNodeConfigWithInterpolators;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.TimeSeriesQuery;
import net.opentsdb.query.interpolation.QueryInterpolatorConfig;
import net.opentsdb.query.interpolation.QueryInterpolatorFactory;
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
  
  /** The given start timestamp. */
  private final String start;
  
  /** The given end timestamp. */
  private final String end;
  
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
  
  /** The computed start time for downsampling (First value) */
  private final TimeStamp start_time;
  
  /** The computed end time for downsampling (Last value) */
  private final TimeStamp end_time;
  
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
    
    start = builder.start;
    if (!Strings.isNullOrEmpty(builder.start)) {
      start_time = new MillisecondTimeStamp(
          DateTime.parseDateTimeString(builder.start, builder.timezone));
      if (!run_all) {
        final TimeStamp original = start_time.getCopy();
        start_time.snapToPreviousInterval(interval_part, units);
        if (start_time.compare(Op.LT, original)) {
          start_time.add(duration);
        }
      }
    } else {
      start_time = null;
    }
    
    end = builder.end;
    if (!Strings.isNullOrEmpty(builder.end)) {
      end_time = new MillisecondTimeStamp(
          DateTime.parseDateTimeString(builder.end, builder.timezone));
      if (!run_all) {
        end_time.snapToPreviousInterval(interval_part, units);
      }
      // TODO - fall to next interval?
    } else {
      end_time = new MillisecondTimeStamp(DateTime.currentTimeMillis());
      if (!run_all) {
        end_time.snapToPreviousInterval(interval_part, units);
      }
    }
    
    // make sure we have at least one interval in our range
    // TODO - propper difference function
    if (start_time != null && end_time.msEpoch() - start_time.msEpoch() < 
        DateTime.parseDuration(interval)) {
      throw new IllegalArgumentException("The start and stop times of "
          + "the query must be greater than the interval provided.");
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
  
  /** @return The non-null timezone string name.. */
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
  
  /**  @returnConverts the units to a 2x style parseable string. */
  public String getInterval() {
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
  
  @Override
  public Builder toBuilder() {
    return (Builder) new Builder()
        .setInterval(interval)
        .setTimeZone(timezone.toString())
        .setAggregator(aggregator)
        .setInfectiousNan(infectious_nan)
        .setRunAll(run_all)
        .setFill(fill)
        .setStart(start)
        .setEnd(end)
        .setInterpolatorConfigs(Lists.newArrayList(interpolator_configs.values()))
        .setId(id);
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
  public int compareTo(QueryNodeConfig o) {
    // TODO Auto-generated method stub
    return 0;
  }
  
  @Override
  public boolean equals(final Object o) {
    // TODO Auto-generated method stub
    if (o == null) {
      return false;
    }
    if (o == this) {
      return true;
    }
    if (!(o instanceof DownsampleConfig)) {
      return false;
    }
    
    return id.equals(((DownsampleConfig) o).id);
  }

  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }
  
  @Override
  public HashCode buildHashCode() {
    // TODO Auto-generated method stub
    return Const.HASH_FUNCTION().newHasher()
        .putString(id, Const.UTF8_CHARSET)
        .hash();
  }
  
  /** @return A new builder to work from. */
  public static Builder newBuilder() {
    return new Builder();
  }
  
  public static Builder newBuilder(final DownsampleConfig config) {
    return (Builder) new Builder()
        .setStart(config.start)
        .setEnd(config.end)
        .setAggregator(config.aggregator)
        .setFill(config.fill)
        .setInfectiousNan(config.infectious_nan)
        .setInterval(config.interval)
        .setTimeZone(config.timezone.toString())
        .setInterpolatorConfigs(Lists.newArrayList(
            config.interpolator_configs.values()))
        .setSources(Lists.newArrayList(config.getSources()))
        .setId(config.id);
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
    @JsonProperty
    private String start;
    @JsonProperty
    private String end;
    
    Builder() {
      setType(DownsampleFactory.TYPE);
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
    
    public Builder setStart(final String start) {
      this.start = start;
      return this;
    }
    
    public Builder setEnd(final String end) {
      this.end = end;
      return this;
    }
    
    /** @return The constructed config.
     * @throws IllegalArgumentException if a required parameter is missing or
     * invalid. */
    public QueryNodeConfig build() {
      return (QueryNodeConfig) new DownsampleConfig(this);
    }
  }
  
  public static DownsampleConfig parse(final ObjectMapper mapper,
                                       final TSDB tsdb, 
                                       final JsonNode node) {
    Builder builder = new Builder();
    JsonNode n = node.get("interval");
    if (n != null) {
      builder.setInterval(n.asText());
    }
    
    n = node.get("id");
    if (n != null) {
      builder.setId(n.asText());
    }
    
    n = node.get("timezone");
    if (n != null) {
      builder.setTimeZone(n.asText());
    }
    
    n = node.get("aggregator");
    if (n != null) {
      builder.setAggregator(n.asText());
    }
    
    n = node.get("infectiousNan");
    if (n != null) {
      builder.setInfectiousNan(n.asBoolean());
    }
    
    n = node.get("runAll");
    if (n != null) {
      builder.setRunAll(n.asBoolean());
    }
    
    n = node.get("fill");
    if (n != null) {
      builder.setFill(n.asBoolean());
    }
    
    n = node.get("start");
    if (n != null) {
      builder.setStart(n.asText());
    }
    
    n = node.get("end");
    if (n != null) {
      builder.setEnd(n.asText());
    }
    
    n = node.get("interpolatorConfigs");
    for (final JsonNode config : n) {
      JsonNode type_json = config.get("type");
      final QueryInterpolatorFactory factory = tsdb.getRegistry().getPlugin(
          QueryInterpolatorFactory.class, 
          type_json == null || type_json.isNull() ? 
             null : type_json.asText());
      if (factory == null) {
        throw new IllegalArgumentException("Unable to find an "
            + "interpolator factory for: " + 
            (type_json == null || type_json.isNull() ? "Default" :
             type_json.asText()));
      }
      
      final QueryInterpolatorConfig interpolator_config = 
          factory.parseConfig(mapper, tsdb, config);
      builder.addInterpolatorConfig(interpolator_config);
    }
    
    n = node.get("sources");
    if (n != null && !n.isNull()) {
      try {
        builder.setSources(mapper.treeToValue(n, List.class));
      } catch (JsonProcessingException e) {
        throw new IllegalArgumentException("Failed to parse json", e);
      }
    }
    
    return (DownsampleConfig) builder.build();
  }

}