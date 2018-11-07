// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;

import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeSeriesDataSourceFactory;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.TimeSeriesQuery;
import net.opentsdb.utils.DateTime;

/**
 * A config that represents a single data source.
 * 
 * @since 3.0
 */
public class TimeRouterConfigEntry {
  
  /** Enum to determine if the results matched or not. */
  static enum MatchType {
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
  
  /** The ID of the data source. */
  private final String source_id;
  
  /**
   * Default protected ctor.
   * @param builder The non-null builder.
   */
  protected TimeRouterConfigEntry(final Builder builder) {
    time_zone = builder.timeZone;
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
    
    if (end != 0 && start != 0) {
      if (start_relative && end_relative && end >= start) {
        throw new IllegalArgumentException("The end time must be >= start time.");
      }
    } else if (!start_relative && !end_relative && end <= start) {
      throw new IllegalArgumentException("The end time must be >= start time.");
    }
    
    full_only = builder.fullOnly;
    source_id = builder.sourceId;
  }
  
  public String getStartString() {
    return start_string;
  }

  public long getStart() {
    return start;
  }

  public boolean isStartRelative() {
    return start_relative;
  }

  public String getEndString() {
    return end_string;
  }

  public long getEnd() {
    return end;
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

  public String getSourceId() {
    return source_id;
  }

  /**
   * Package private to allow the factory to determine if this node 
   * matches the query.
   * @param query The non-null query to pull the time from.
   * @param config The non-null config to send to the factory for 
   * validation.
   * @param tsdb The non-null TSDB.
   * @return The type of match made.
   */
  MatchType match(final TimeSeriesQuery query, 
                  final TimeSeriesDataSourceConfig config, 
                  final TSDB tsdb) {
    // time match first
    MatchType match = MatchType.FULL;
    if (start != 0 || end != 0) { // 0,0 == all time
      // assume start & end in the query have been set and validated.
      final long now = DateTime.currentTimeMillis() / 1000;
      
      // first check for out of bounds.
      if (start != 0) {
        if (start_relative ? query.endTime().epoch() < now - start : 
            query.endTime().epoch() < start) {
          return MatchType.NONE;
        } else if (start_relative ? query.startTime().epoch() >= now - start : 
              query.startTime().epoch() >= start) {
          // full
        } else {
          match = MatchType.PARTIAL;
        }
      }
    
      if (end != 0) {
        if (end_relative ? query.startTime().epoch() >= now - end : 
            query.startTime().epoch() >= end) {
          return MatchType.NONE;
        } else if (match == MatchType.FULL && 
            (end_relative ? query.endTime().epoch() < now - end :
                query.endTime().epoch() < end)) {
          // full though we leave it at partial if the start was out of bound.
        } else {
          match = MatchType.PARTIAL;
        }
      }
    }
    
    if (full_only && match != MatchType.FULL) {
      return MatchType.NONE;
    }
    
    // time matched, check the factory now.
    String source_id;
    if (!Strings.isNullOrEmpty(config.getSourceId())) {
      source_id = config.getSourceId();
    } else if (!Strings.isNullOrEmpty(config.getType())) {
      source_id = config.getType();
    } else {
      source_id = config.getId();
    }
    
    TimeSeriesDataSourceFactory factory = tsdb.getRegistry().getPlugin(
        TimeSeriesDataSourceFactory.class, source_id);
    if (factory == null) {
      throw new IllegalArgumentException("No factory found for source: " 
          + (Strings.isNullOrEmpty(source_id) ? "Default" : source_id));
    }
    if (!factory.supportsQuery(query, config)) {
      return MatchType.NONE;
    }
    return match;
  }
  
  public static Builder newBuilder() {
    return new Builder();
  }
  
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
    private String sourceId;
    
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
    
    public Builder setSourceId(final String source_id) {
      sourceId = source_id;
      return this;
    }
    
    public TimeRouterConfigEntry builder() {
      return new TimeRouterConfigEntry(this);
    }
  }

}
