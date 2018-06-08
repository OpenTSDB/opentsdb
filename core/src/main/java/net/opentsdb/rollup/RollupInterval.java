// This file is part of OpenTSDB.
// Copyright (C) 2015-2018 The OpenTSDB Authors.
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
package net.opentsdb.rollup;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.common.base.Objects;
import com.google.common.hash.HashCode;

import net.opentsdb.core.Const;
import net.opentsdb.utils.DateTime;

/**
 * Holds information about a rollup interval. During construction the inputs
 * are validated.
 * @since 2.4
 */
@JsonDeserialize(builder = RollupInterval.Builder.class)
public class RollupInterval {
  /** Static intervals */
  private static final int MAX_SECONDS_IN_HOUR = 60 * 60;
  private static final int MAX_SECONDS_IN_DAY = 60 * 60 * 24;
  // account for leap years, etc
  private static final int MAX_SECONDS_IN_MONTH = 60 * 60 * 24 * 32;
  private static final int MAX_SECONDS_IN_YEAR = 60 * 60 * 24 * 366; 

  /** Based on a 2 bytes with 4 bits reserved for data value size and type */
  public static int MAX_INTERVALS = 7774;
  
  /** The minimum # of intervals in a span */
  public static int MIN_INTERVALS = 12;
  
  /** User assigned name for the temporal only table */
  private final String temporal_table_name;
  private byte[] temporal_table; 
  
  /** User assigned name for the group by table for this interval */
  private final String groupby_table_name;
  private byte[] groupby_table;
  
  /** User given interval as a string in the format <#><unit> similar to a 
   * downsampling query
   */
  private final String string_interval;
  
  /** How wide the row will be in values. */
  private final String row_span;
  
  /** Width of the row as a time unit, e.g. 'h' for hour, 'd' for day, 'm' for
   * month and 'y' for year
   */
  private final char units;
  
  /** How many of the units we want in our span */
  private final int unit_multiplier;
  
  /** Parsed interval unit from the user supplied string */
  private char interval_units;
  
  /** The interval in seconds */
  private int interval;
  
  /** The number of intervals in this span */
  private int intervals;

  /** A pointer back to the config this interval belongs to. */
  private DefaultRollupConfig config; 
  
  /** Tells whether it is the default rollup interval
   * Default interval is of 1m interval, and will be stored in normal
   * tsdb table/s. So if true, which means the raw cell column qualifier format 
   * also it might be compacted.
   */
  private final boolean is_default_interval;
  
  /**
   * Protected ctor used by the builder.
   * @param builder The non-null builder to load from.
   */
  protected RollupInterval(final Builder builder) {
    temporal_table_name = builder.table;
    groupby_table_name = builder.preAggregationTable;
    string_interval = builder.interval;
    row_span = builder.rowSpan;
    is_default_interval = builder.defaultInterval;
    
    final String parsed_units = DateTime.getDurationUnits(row_span);
    if (parsed_units.length() > 1) {
      throw new IllegalArgumentException("Milliseconds are not supported");
    }
    units = parsed_units.charAt(0);
    this.unit_multiplier = DateTime.getDurationInterval(row_span);
    
    validateAndCompile();
  }
  
  @Override
  public String toString() {
    final StringBuilder buf = new StringBuilder();
    buf.append("{table=").append(temporal_table_name)
       .append(", preAggTable=").append(groupby_table_name)
       .append(", rowSpan=").append(row_span)
       .append(", isDefaultInterval=").append(is_default_interval)
       .append(", interval=").append(string_interval)
       .append(", units=").append(units)
       .append(", unit_multipier=").append(unit_multiplier)
       .append(", intervals=").append(intervals)
       .append(", interval=").append(interval)
       .append(", interval_units=").append(interval_units)
       .append("}");
    return buf.toString();
  }

  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }
  
  /** @return A HashCode object for deterministic, non-secure hashing */
  public HashCode buildHashCode() {
    return Const.HASH_FUNCTION().newHasher()
        .putString(temporal_table_name, Const.UTF8_CHARSET)
        .putString(groupby_table_name, Const.UTF8_CHARSET)
        .putString(string_interval, Const.UTF8_CHARSET)
        .putString(row_span, Const.UTF8_CHARSET)
        .putBoolean(is_default_interval)
        .hash();
  }
  
  @Override
  public boolean equals(final Object obj) {
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof RollupInterval)) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    final RollupInterval interval = (RollupInterval)obj;
    return Objects.equal(temporal_table_name, interval.temporal_table_name) 
        && Objects.equal(groupby_table_name, interval.groupby_table_name)
        && Objects.equal(row_span, interval.row_span)
        && Objects.equal(string_interval, interval.string_interval)
        && Objects.equal(is_default_interval, interval.is_default_interval);
  }
  
  /**
   * Calculates the number of intervals in a given span for the rollup
   * interval and makes sure we have table names. It also sets the table byte
   * arrays.
   * @return The number of intervals in the span
   * @throws IllegalArgumentException if milliseconds were passed in the interval
   * or the interval couldn't be parsed, the tables are missing, or if the 
   * duration is too large, too large for the span or the interval is too 
   * large or small for the span or if the span is invalid.
   * @throws NullPointerException if the interval is empty or null
   */
  void validateAndCompile() {
    if (temporal_table_name == null || temporal_table_name.isEmpty()) {
      throw new IllegalArgumentException("The rollup table cannot be null or empty");
    }
    temporal_table = temporal_table_name.getBytes(Const.ASCII_CHARSET);
    
    if (groupby_table_name == null || groupby_table_name.isEmpty()) {
      throw new IllegalArgumentException("The pre-aggregate rollup table cannot"
          + " be null or empty");
    }
    groupby_table = groupby_table_name.getBytes(Const.ASCII_CHARSET);
    
    if (units != 'h' && unit_multiplier > 1) {
      throw new IllegalArgumentException("Multipliers are only usable with the 'h' unit");
    } else if (units == 'h' && unit_multiplier > 1 && unit_multiplier % 2 != 0) {
      throw new IllegalArgumentException("The multiplier must be 1 or an even value");
    }
    
    interval = (int) (DateTime.parseDuration(string_interval) / 1000);
    if (interval < 1) {
      throw new IllegalArgumentException("Millisecond intervals are not supported");
    }
    if (interval >= Integer.MAX_VALUE) {
      throw new IllegalArgumentException("Interval is too big: " + interval);
    }
    // The line above will validate for us
    interval_units = string_interval.charAt(string_interval.length() - 1);

    int num_span = 0;
    switch (units) {
    case 'h':
      num_span = MAX_SECONDS_IN_HOUR;
      break;
    case 'd':
      num_span = MAX_SECONDS_IN_DAY;
      break;
    case 'n':
      num_span = MAX_SECONDS_IN_MONTH;
      break;
    case 'y':
      num_span = MAX_SECONDS_IN_YEAR;
      break;
    default:
      throw new IllegalArgumentException("Unrecogznied span '" + units + "'");
    }
    num_span *= unit_multiplier;
    
    if (interval >= num_span) {
      throw new IllegalArgumentException("Interval [" + interval + 
          "] is too large for the span [" + units + "]");
    }

    intervals = num_span / (int)interval;
    if (intervals > MAX_INTERVALS) {
      throw new IllegalArgumentException("Too many intervals [" + intervals + 
          "] in the span. Must be smaller than [" + MAX_INTERVALS + 
          "] to fit in 14 bits");
    }
    
    if (intervals < MIN_INTERVALS) {
      throw new IllegalArgumentException("Not enough intervals [" + intervals + 
          "] for the span. Must be at least [" + MIN_INTERVALS + "]");
    }
  }
  
  /** @return the string name of the temporal rollup table */
  public String getTable() {
    return temporal_table_name;
  }
  
  /** @return the temporal rollup table name as a byte array */
  @JsonIgnore
  public byte[] getTemporalTable() {
    return temporal_table;
  }
  
  /** @return the string name of the group by rollup table */
  public String getPreAggregationTable() {
    return groupby_table_name;
  }
  
  /** @return the group by table name as a byte array */
  @JsonIgnore
  public byte[] getGroupbyTable() {
    return groupby_table;
  }

  /** @return the configured interval as a string */
  public String getInterval() {
    return string_interval;
  }

  /** @return the character describing the span of this interval */
  @JsonIgnore
  public char getUnits() {
    return units;
  }
  
  /** @return the unit multiplier */
  @JsonIgnore
  public int getUnitMultiplier() {
    return unit_multiplier;
  }

  /** @return the interval units character */
  @JsonIgnore
  public char getIntervalUnits() {
    return interval_units;
  }

  /** @return the interval for this span in seconds */
  @JsonIgnore
  public int getIntervalSeconds() {
    return interval;
  }

  /** @return the count of intervals in this span */
  @JsonIgnore
  public int getIntervals() {
    return intervals;
  }

  /**
   * Is it the default roll up interval that need to be written to default 
   * tsdb data table. So if true, which means the raw cell column qualifier 
   * is not encoded with the aggregate function and the cell might have been
   * compacted
   * @return true if it is default rollup interval
   */
  public boolean isDefaultInterval() {
    return is_default_interval;
  }

  /** @return The width of each row as an interval string. */
  public String getRowSpan() {
    return row_span;
  }
  
  /** @param config The rollup config this interval belongs to. */
  public void setConfig(final DefaultRollupConfig config) {
    this.config = config;
  }
  
  /** @return The rollup config this interval belongs to. */
  public DefaultRollupConfig rollupConfig() {
    return config;
  }
  
  public static Builder builder() {
    return new Builder();
  }
  
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "")
  public static class Builder {
    @JsonProperty
    private String table;
    @JsonProperty
    private String preAggregationTable;
    @JsonProperty
    private String interval;
    @JsonProperty
    private String rowSpan;
    @JsonProperty
    private boolean defaultInterval;
    
    public Builder setTable(final String table) {
      this.table = table;
      return this;
    }
    
    public Builder setPreAggregationTable(final String preAggregationTable) {
      this.preAggregationTable = preAggregationTable;
      return this;
    }
    
    public Builder setInterval(final String interval) {
      this.interval = interval;
      return this;
    }
    
    public Builder setRowSpan(final String rowSpan) {
      this.rowSpan = rowSpan;
      return this;
    }
    
    public Builder setDefaultInterval(final boolean defaultInterval) {
      this.defaultInterval = defaultInterval;
      return this;
    }
    
    public RollupInterval build() {
      return new RollupInterval(this);
    }
  }
}
