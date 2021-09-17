// This file is part of OpenTSDB.
// Copyright (C) 2018-2021  The OpenTSDB Authors.
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
package net.opentsdb.query.router;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Strings;

import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeSeriesDataSourceFactory;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.plan.QueryPlanner.TimeAdjustments;
import net.opentsdb.utils.DateTime;

import java.time.temporal.TemporalAmount;
import java.util.Comparator;
import java.util.List;

/**
 * A config that represents a single data source and optionally when data
 * may or may not be available in that store. The "start" time is similar to the
 * start time of a query meaning it must be less than the end time of a query.
 * E.g. start = 2h-ago, end = 1h-ago is valid.
 * <p>
 * <b>Invariates:</b>
 * <ul>
 *   <li>The start or the end times may be 0 in which case the start or
 *   end time is ignored respectively, meaning the system can query for data
 *   before or beyond those times.</li>
 *   <li>If the start and end times are not 0, then the start time must be
 *   less than the end time, whether this is relative or absolute.</li>
 *   <li>If the start is absolute and the end is relative but the end would
 *   be earlier than the start at instantiation, an error will be thrown until
 *   enough wall-clock time has passed that the end is later than the start.</li>
 * </ul>
 *
 * TODO - adjustable retention like Aura... *sniff*
 * @since 3.0
 */
@JsonInclude(Include.NON_NULL)
@JsonDeserialize(builder = TimeRouterConfigEntry.Builder.class)
public class TimeRouterConfigEntry {

  /** Enum to determine if the results matched or not. */
  public static enum MatchType {
    FULL,
    PARTIAL,
    NONE
  }

  /** The original config string for re-serializing. */
  private final String start_string;

  /** The converted start time in seconds (relative or epoch) */
  private final long start;

  /** Whether or not the start time is relative. */
  private final boolean start_relative;

  /** The original config string for re-serializing. */
  private final String end_string;

  /** The converted end time in seconds (relative or epoch) */
  private final long end;

  /** Whether or not the end time is relative. */
  private final boolean end_relative;

  /** An optional time zone for absolute timestamps. */
  private final String time_zone;

  /** Whether or not this source should return results only when the
   * entire query range is encompassed by the config range. */
  private final boolean full_only;

  /** The type of data this store holds. Could be all or time series or events,
   * etc. */
  private final String data_type;

  /** The ID of the data source. */
  private final String source_id;

  /** Optional timeout for queries to the source. */
  private final String timeout;

  /** The source factory to see if it supports this query. */
  protected TimeSeriesDataSourceFactory factory;

  /**
   * Default protected ctor.
   * @param builder The non-null builder.
   */
  protected TimeRouterConfigEntry(final Builder builder) {
    time_zone = builder.timeZone;
    full_only = builder.fullOnly;
    data_type = builder.dataType;
    source_id = builder.sourceId;
    timeout = builder.timeout;
    factory = builder.factory;

    if (Strings.isNullOrEmpty(builder.start)) {
      start_string = null;
      start = 0;
      start_relative = true;
    } else {
      start_string = builder.start;
      if (DateTime.isRelativeDate(start_string)) {
        start_relative = true;
        start = DateTime.parseDuration(start_string.substring(0,
                start_string.indexOf("-ago"))) / 1000;
      } else {
        start_relative = false;
        start = DateTime.parseDateTimeString(start_string, time_zone) / 1000;
      }
    }

    if (Strings.isNullOrEmpty(builder.end)) {
      end_string = null;
      end = 0;
      end_relative = true;
    } else {
      end_string = builder.end;
      if (DateTime.isRelativeDate(end_string)) {
        end_relative = true;
        end = DateTime.parseDuration(end_string.substring(0,
                end_string.indexOf("-ago"))) / 1000;
      } else {
        end_relative = false;
        end = DateTime.parseDateTimeString(end_string, time_zone) / 1000;
      }
    }

    // start and end validation
    if (start == 0 && end == 0) {
      // no-op
    } else {
      // NOTE: UT hack
      int now = builder.now > 0 ? builder.now : (int) (DateTime.currentTimeMillis() / 1000);
      long normalizedStart = start;
      if (start > 0 ) {
        normalizedStart = start_relative ? now - start : start;
      }
      long normalizedEnd = end;
      if (end > 0) {
        normalizedEnd = end_relative ? now - end : end;
      }
      if (normalizedStart != 0 && normalizedEnd != 0 &&
              normalizedEnd <= normalizedStart) {
        throw new IllegalArgumentException("Start time [" + normalizedStart
                + "] must be less than end time [" + normalizedEnd + "] for "
                + this);
      }
    }
  }

  public String getStartString() {
    return start_string;
  }

  public long getStart(final long now) {
    return start_relative ?
            start == 0 ? 0 : now - start
              : start;
  }

  public boolean isStartRelative() {
    return start_relative;
  }

  public String getEndString() {
    return end_string;
  }

  public long getEnd(final long now) {
    return end_relative ? now - end : end;
  }

  public boolean isEndRelative() {
    return end_relative;
  }

  public String getTimeZone() {
    return time_zone;
  }

  public boolean isFullOnly() {
    return full_only;
  }

  public String getDataType() {
    return data_type;
  }

  public String getSourceId() {
    return source_id;
  }

  public String getTimeout() {
    return timeout;
  }

  @Override
  public String toString() {
    return new StringBuilder()
            .append("{dataType=")
            .append(data_type)
            .append(", fullOnly=")
            .append(full_only)
            .append(", srcID=")
            .append(source_id)
            .append(", startString=")
            .append(start_string)
            .append(", start=")
            .append(start)
            .append(", startIsRelative=")
            .append(start_relative)
            .append(", endString=")
            .append(end)
            .append(", end=")
            .append(end)
            .append(", endIsRelative=")
            .append(end_relative)
            .append(", timeout=")
            .append(timeout)
            .append("}")
            .toString();
  }

  /**
   * Package private to allow the factory to determine if this node
   * matches the query.
   * <b>NOTE:</b> This has to be thread safe as multiple callers will be
   * checking in.
   * @param context The non-null query context to pull the time from.
   * @param config The non-null config to send to the factory for
   * validation.
   * @param now The Unix epoch time in seconds.
   * @return The type of match made.
   */
  protected MatchType match(final QueryPipelineContext context,
                            final TimeSeriesDataSourceConfig.Builder config,
                            final int now) {
    // fudge for UT mocks;
    if (config == null) {
      return MatchType.NONE;
    }

    // time match first
    MatchType match = MatchType.FULL;

    if (start != 0 || end != 0) { // 0,0 == all time
      final long queryStart;
      final long queryEnd;
      if (config.timeShifts() != null) {
        TimeStamp ts = config.startOverrideTimeStamp().getCopy();
        ts.subtract((TemporalAmount) config.timeShifts().getValue());
        queryStart = ts.epoch();

        ts = config.endOverrideTimeStamp().getCopy();
        ts.subtract((TemporalAmount) config.timeShifts().getValue());
        queryEnd = ts.epoch();
      } else {
        queryStart = config.startOverrideTimeStamp().epoch();
        queryEnd = config.endOverrideTimeStamp().epoch();
      }

      final long storeStart = start_relative ? (now - start) : start;
      final long storeEnd = end_relative ? (now - end) : end;

      if (end != 0) {
        if (queryStart >= storeEnd) {
          return MatchType.NONE;
        } else if (queryEnd >= storeEnd) {
          match = MatchType.PARTIAL;
        }
      }

      if (start != 0) {
        if (queryEnd <= storeStart) {
          return MatchType.NONE;
        } else if (queryStart < storeStart) {
          match = MatchType.PARTIAL;
        }
      }
    }

    if (full_only && match != MatchType.FULL) {
      return MatchType.NONE;
    }

    // time matched, check the factory now.
    String type;
    if (config.types() != null && !config.types().isEmpty()) {
      List<String> types = config.types();
      type = types.get(0);
    } else {
      type = null;
    }

    if (!Strings.isNullOrEmpty(type) || !Strings.isNullOrEmpty(data_type)) {
      if (Strings.isNullOrEmpty(type)) {
        type = "metric";
      }
      String dt = data_type;
      if (Strings.isNullOrEmpty(data_type)) {
        dt = "metric";
      }

      if (!dt.equalsIgnoreCase(type)) {
        return MatchType.NONE;
      }
    }

    if (!factory.supportsQuery(context, (TimeSeriesDataSourceConfig) config.build())) {
      return MatchType.NONE;
    }
    return match;
  }

  /**
   * @return The source factory for this entry.
   */
  public TimeSeriesDataSourceFactory factory() {
    return factory;
  }

  /** @return A new builder. */
  public static Builder newBuilder() {
    return new Builder();
  }

  protected static class ConfigSorter implements Comparator<TimeRouterConfigEntry> {
    public final long now = DateTime.currentTimeMillis() / 1000;

    @Override
    public int compare(TimeRouterConfigEntry o1, TimeRouterConfigEntry o2) {
      long end1 = o1.getEnd(now);
      long end2 = o2.getEnd(now);

      if (end1 == 0) {
        end1 = Long.MAX_VALUE;
      }
      if (end2 == 0) {
        end2 = Long.MAX_VALUE;
      }

      if (end1 == end2) {
        // look at starts
        final long start1 = o1.getStart(now);
        final long start2 = o2.getStart(now);
        if (start1 == start2) {
          // TODO really should throw
          return 0;
        } else if (start1 < start2) {
          return 1;
        }
        return -1;
      }

      if (end1 < end2) {
        return 1;
      }
      return -1;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Builder {
    @JsonProperty
    private String start;
    @JsonProperty
    private String end;
    @JsonProperty
    private String timeZone;
    @JsonProperty
    private boolean fullOnly;
    @JsonProperty
    private String dataType;
    @JsonProperty
    private String sourceId;
    @JsonProperty
    private String timeout;
    // For UTs
    private TimeSeriesDataSourceFactory factory;
    private int now;

    public Builder setStart(final String start) {
      this.start = start;
      return this;
    }

    public Builder setEnd(final String end) {
      this.end = end;
      return this;
    }

    public Builder setTimeZone(final String time_zone) {
      timeZone = time_zone;
      return this;
    }

    public Builder setFullOnly(final boolean full_only) {
      fullOnly = full_only;
      return this;
    }

    public Builder setDataType(final String data_type) {
      this.dataType = data_type;
      return this;
    }

    public Builder setSourceId(final String source_id) {
      sourceId = source_id;
      return this;
    }

    public Builder setTimeout(final String timeout) {
      this.timeout = timeout;
      return this;
    }

    // For UTs
    public Builder setFactory(final TimeSeriesDataSourceFactory factory) {
      this.factory = factory;
      return this;
    }

    // For UTs
    public Builder setNow(final int now) {
      this.now = now;
      return this;
    }

    public TimeRouterConfigEntry build() {
      return new TimeRouterConfigEntry(this);
    }
  }

}
