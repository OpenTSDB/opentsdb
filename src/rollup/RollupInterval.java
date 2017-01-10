// This file is part of OpenTSDB.
// Copyright (C) 2010-2015  The OpenTSDB Authors.
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
package net.opentsdb.rollup;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Objects;

import net.opentsdb.core.Const;
import net.opentsdb.utils.DateTime;

/**
 * Holds information about a rollup interval. During construction the inputs
 * are validated.
 * @since 2.4
 */
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
  
  /** Tells whether it is the default rollup interval
   * Default interval is of 1m interval, and will be stored in normal
   * tsdb table/s. So if true, which means the raw cell column qualifier format 
   * also it might be compacted.
   * TODO. This will be changed when the spatial aggregation logic is in place.
   * Here it is added to handle the pre-aggregated data on raw data
   */
  private final boolean default_interval;
  
  /**
   * Default Ctor used when configuring rollups
   * @param temporal_table_name The rollup only table name
   * @param groupby_table_name The pre-agg rollup table name
   * @param interval The rollup interval, e.g. 10m or 15m or 1h
   * @param span The row span, e.g. 1h, 6h, 1d, 1m, 1y. Values greater than 1
   * are only allowed with the 'h' unit.
   * @throws IllegalArgumentException if milliseconds were passed in the interval
   * or the interval couldn't be parsed, the tables are missing, or if the 
   * duration is too large, too large for the span or the interval is too 
   * large or small for the span or if the span is invalid.
   * @throws NullPointerException if the interval is empty or null
   */
  public RollupInterval(final String temporal_table_name, 
      final String groupby_table_name, final String interval, 
      final String span) {
    this(temporal_table_name, groupby_table_name, interval, span, false);
  }

  /**
   * Default Ctor used when configuring rollups
   * @param temporal_table_name The rollup only table name
   * @param groupby_table_name The pre-agg rollup table name
   * @param interval The rollup interval, e.g. 10m or 15m or 1h
   * @param span The row span, e.g. 1h, 6h, 1d, 1m, 1y. Values greater than 1
   * are only allowed with the 'h' unit.
   * @param default_interval Tells whether it is the default rollup interval
   *     that needs to be written into default tsdb table
   * @throws IllegalArgumentException if milliseconds were passed in the interval
   * or the interval couldn't be parsed, the tables are missing, or if the 
   * duration is too large, too large for the span or the interval is too 
   * large or small for the span or if the span is invalid.
   * @throws NullPointerException if the interval is empty or null
   */
  public RollupInterval(final String temporal_table_name, 
      final String groupby_table_name, final String interval, 
      final String span, boolean default_interval) {
    this.temporal_table_name = temporal_table_name;
    this.groupby_table_name = groupby_table_name;
    this.string_interval = interval;
    this.default_interval = default_interval;
    
    final String parsed_units = DateTime.getDurationUnits(span);
    if (parsed_units.length() > 1) {
      throw new IllegalArgumentException("Milliseconds are not supported");
    }
    units = parsed_units.charAt(0);
    this.unit_multiplier = DateTime.getDurationInterval(span);
    
    validateAndCompile();
  }
  
  @Override
  public String toString() {
    final StringBuilder buf = new StringBuilder();
    buf.append("table=").append(temporal_table_name)
       .append(", agg_table=").append(groupby_table_name)
       .append(", interval=").append(string_interval)
       .append(", units=").append(units)
       .append(", unit_multipier=").append(unit_multiplier)
       .append(", intervals=").append(intervals)
       .append(", interval=").append(interval)
       .append(", interval_units=").append(interval_units);
    return buf.toString();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(temporal_table_name, groupby_table_name, units,
        unit_multiplier, string_interval, default_interval);
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
        && Objects.equal(units, interval.units)
        && Objects.equal(unit_multiplier, interval.unit_multiplier)
        && Objects.equal(string_interval, interval.string_interval)
        && Objects.equal(default_interval, interval.default_interval);
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
    case 'm':
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
  public String getTemporalTableName() {
    return temporal_table_name;
  }
  
  /** @return the temporal rollup table name as a byte array */
  @JsonIgnore
  public byte[] getTemporalTable() {
    return temporal_table;
  }
  
  /** @return the string name of the group by rollup table */
  public String getGroupbyTableName() {
    return groupby_table_name;
  }
  
  /** @return the group by table name as a byte array */
  @JsonIgnore
  public byte[] getGroupbyTable() {
    return groupby_table;
  }

  /** @return the configured interval as a string */
  public String getStringInterval() {
    return string_interval;
  }

  /** @return the character describing the span of this interval */
  public char getUnits() {
    return units;
  }
  
  /** @return the unit multiplier */
  public int getUnitMultiplier() {
    return unit_multiplier;
  }

  /** @return the interval units character */
  public char getIntervalUnits() {
    return interval_units;
  }

  /** @return the interval for this span in seconds */
  public int getInterval() {
    return interval;
  }

  /** @return the count of intervals in this span */
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
  public boolean isDefaultRollupInterval() {
    return default_interval;
  }
}
