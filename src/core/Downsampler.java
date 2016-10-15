// This file is part of OpenTSDB.
// Copyright (C) 2014  The OpenTSDB Authors.
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
package net.opentsdb.core;

import java.util.Calendar;
import java.util.NoSuchElementException;

import net.opentsdb.utils.DateTime;

/**
 * Iterator that downsamples data points using an {@link Aggregator}.
 */
public class Downsampler implements SeekableView, DataPoint {
  
  /** Matches the weekly downsampler as it requires special handling. */
  protected final static int WEEK_UNIT = DateTime.unitsToCalendarType("w");
  protected final static int DAY_UNIT = DateTime.unitsToCalendarType("d");
  protected final static int WEEK_LENGTH = 7;
  
  /** The downsampling specification when provided */
  protected final DownsamplingSpecification specification;
  
  /** The start timestamp of the actual query for use with "all" */
  protected final long query_start;
  
  /** The end timestamp of the actual query for use with "all" */
  protected final long query_end;
  
  /** The data source */
  protected final SeekableView source;
  
  /** Iterator to iterate the values of the current interval. */
  protected final ValuesInInterval values_in_interval;
  
  /** Last normalized timestamp */ 
  protected long timestamp;
  
  /** Last value as a double */
  protected double value;
  
  /** Whether or not to merge all DPs in the source into one vaalue */
  protected final boolean run_all;
  
  /** The interval to use with a calendar */
  protected final int interval;
  
  /** The unit to use with a calendar as a Calendar integer */
  protected final int unit;
  
  /**
   * Ctor.
   * @param source The iterator to access the underlying data.
   * @param interval_ms The interval in milli seconds wanted between each data
   * point.
   * @param downsampler The downsampling function to use.
   * @deprecated as of 2.3
   */
  Downsampler(final SeekableView source,
              final long interval_ms,
              final Aggregator downsampler) {
    this.source = source;
    if (downsampler == Aggregators.NONE) {
      throw new IllegalArgumentException("cannot use the NONE "
          + "aggregator for downsampling");
    }
    specification = new DownsamplingSpecification(interval_ms, downsampler, 
        DownsamplingSpecification.DEFAULT_FILL_POLICY);
    values_in_interval = new ValuesInInterval();
    query_start = 0;
    query_end = 0;
    interval = unit = 0;
    run_all = false;
  }
  
  /**
   * Ctor.
   * @param source The iterator to access the underlying data.
   * @param specification The downsampling spec to use
   * @param query_start The start timestamp of the actual query for use with "all"
   * @param query_end The end timestamp of the actual query for use with "all"
   * @since 2.3
   */
  Downsampler(final SeekableView source,
              final DownsamplingSpecification specification,
              final long query_start,
              final long query_end
      ) {
    this.source = source;
    this.specification = specification;
    values_in_interval = new ValuesInInterval();
    this.query_start = query_start;
    this.query_end = query_end;
    
    final String s = specification.getStringInterval();
    if (s != null && s.toLowerCase().contains("all")) {
      run_all = true;
      interval = unit = 0;
    } else if (s != null && specification.useCalendar()) {
      if (s.toLowerCase().contains("ms")) {
        interval = Integer.parseInt(s.substring(0, s.length() - 2));
        unit = DateTime.unitsToCalendarType(s.substring(s.length() - 2));
      } else {
        interval = Integer.parseInt(s.substring(0, s.length() - 1));
        unit = DateTime.unitsToCalendarType(s.substring(s.length() - 1));
      }
      run_all = false;
    } else {
      run_all = false;
      interval = unit = 0;
    }
  }

  // ------------------ //
  // Iterator interface //
  // ------------------ //

  @Override
  public boolean hasNext() {
    return values_in_interval.hasNextValue();
  }

  /**
   * @throws NoSuchElementException if no data points remain.
   */
  @Override
  public DataPoint next() {
    if (hasNext()) {
      value = specification.getFunction().runDouble(values_in_interval);
      timestamp = values_in_interval.getIntervalTimestamp();
      values_in_interval.moveToNextInterval();
      return this;
    }
    throw new NoSuchElementException("no more data points in " + this);
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  // ---------------------- //
  // SeekableView interface //
  // ---------------------- //

  @Override
  public void seek(final long timestamp) {
    values_in_interval.seekInterval(timestamp);
  }

  // ------------------- //
  // DataPoint interface //
  // ------------------- //

  @Override
  public long timestamp() {
    if (run_all) {
      return query_start;
    }
    return timestamp;
  }

  @Override
  public boolean isInteger() {
    return false;
  }

  @Override
  public long longValue() {
    throw new ClassCastException("Downsampled values are doubles");
  }

  @Override
  public double doubleValue() {
    return value;
  }

  @Override
  public double toDouble() {
    return value;
  }

  @Override
  public String toString() {
    final StringBuilder buf = new StringBuilder();
    buf.append("Downsampler: ")
       .append(", downsampler=").append(specification)
       .append(", queryStart=").append(query_start)
       .append(", queryEnd=").append(query_end)
       .append(", runAll=").append(run_all)
       .append(", current data=(timestamp=").append(timestamp)
       .append(", value=").append(value)
       .append("), values_in_interval=").append(values_in_interval);
   return buf.toString();
  }

  /** Iterates source values for an interval. */
  protected class ValuesInInterval implements Aggregator.Doubles {

    /** An optional calendar set to the current timestamp for the data point */
    private Calendar previous_calendar;
    
    /** An optional calendar set to the end of the interval timestamp */
    private Calendar next_calendar;
    
    /** The end of the current interval. */
    private long timestamp_end_interval = Long.MIN_VALUE;
    
    /** True if the last value was successfully extracted from the source. */
    private boolean has_next_value_from_source = false;
    
    /** The last data point extracted from the source. */
    private DataPoint next_dp = null;

    /** True if it is initialized for iterating intervals. */
    private boolean initialized = false;

    /**
     * Constructor.
     */
    protected ValuesInInterval() {
      if (run_all) {
        timestamp_end_interval = query_end;
      } else if (!specification.useCalendar()) {
        timestamp_end_interval = specification.getInterval();
      }
    }

    /** Initializes to iterate intervals. */
    protected void initializeIfNotDone() {
      // NOTE: Delay initialization is required to not access any data point
      // from the source until a user requests it explicitly to avoid the severe
      // performance penalty by accessing the unnecessary first data of a span.
      if (!initialized) {
        initialized = true;
        if (source.hasNext()) {
          moveToNextValue();
          if (!run_all) {
            if (specification.useCalendar()) {
              previous_calendar = DateTime.previousInterval(next_dp.timestamp(), 
                  interval, unit, specification.getTimezone());
              next_calendar = DateTime.previousInterval(next_dp.timestamp(), 
                  interval, unit, specification.getTimezone());
              if (unit == WEEK_UNIT) {
                next_calendar.add(DAY_UNIT, interval * WEEK_LENGTH);
              } else {
                next_calendar.add(unit, interval);
              }
              timestamp_end_interval = next_calendar.getTimeInMillis();
            } else {
              timestamp_end_interval = alignTimestamp(next_dp.timestamp()) + 
                  specification.getInterval();
            }
          }
        }
      }
    }

    /** Extracts the next value from the source. */
    private void moveToNextValue() {
      if (source.hasNext()) {
        has_next_value_from_source = true;
        // filter out dps that don't match start and end for run_alls
        if (run_all) {
          while (source.hasNext()) {
            next_dp = source.next();
            if (next_dp.timestamp() < query_start) {
              next_dp = null;
              continue;
            }
            if (next_dp.timestamp() >= query_end) {
              has_next_value_from_source = false;
            }
            break;
          }
          if (next_dp == null) {
            has_next_value_from_source = false;
          }
        } else {
          next_dp = source.next();
        }
      } else {
        has_next_value_from_source = false;
      }
    }

    /**
     * Resets the current interval with the interval of the timestamp of
     * the next value read from source. It is the first value of the next
     * interval. */
    private void resetEndOfInterval() {
      if (has_next_value_from_source && !run_all) {
        if (specification.useCalendar()) {
          while (next_dp.timestamp() >= timestamp_end_interval) {
            if (unit == WEEK_UNIT) {
              previous_calendar.add(DAY_UNIT, interval * WEEK_LENGTH);
              next_calendar.add(DAY_UNIT, interval * WEEK_LENGTH);
            } else {
              previous_calendar.add(unit, interval);
              next_calendar.add(unit, interval);
            }
            timestamp_end_interval = next_calendar.getTimeInMillis();
          }
        } else {
          timestamp_end_interval = alignTimestamp(next_dp.timestamp()) + 
              specification.getInterval();
        }
      }
    }

    /** Moves to the next available interval. */
    void moveToNextInterval() {
      initializeIfNotDone();
      resetEndOfInterval();
    }

    /** Advances the interval iterator to the given timestamp. */
    void seekInterval(final long timestamp) {
      // To make sure that the interval of the given timestamp is fully filled,
      // rounds up the seeking timestamp to the smallest timestamp that is
      // a multiple of the interval and is greater than or equal to the given
      // timestamp..
      if (run_all) {
        source.seek(timestamp);
      } else if (specification.useCalendar()) {
        final Calendar seek_calendar = DateTime.previousInterval(
            timestamp, interval, unit, specification.getTimezone());
        if (timestamp > seek_calendar.getTimeInMillis()) {
          if (unit == WEEK_UNIT) {
            seek_calendar.add(DAY_UNIT, interval * WEEK_LENGTH);
          } else {
            seek_calendar.add(unit, interval);
          }
        }
        source.seek(seek_calendar.getTimeInMillis());
      } else {
        source.seek(alignTimestamp(timestamp + specification.getInterval() - 1));
      }
      initialized = false;
    }

    /** Returns the representative timestamp of the current interval. */
    protected long getIntervalTimestamp() {
      // NOTE: It is well-known practice taking the start time of
      // a downsample interval as a representative timestamp of it. It also
      // provides the correct context for seek.
      if (run_all) {
        return timestamp_end_interval;
      } else if (specification.useCalendar()) {
        return previous_calendar.getTimeInMillis();
      } else {
        return alignTimestamp(timestamp_end_interval - 
            specification.getInterval());
      }
    }

    /** Returns timestamp aligned by interval. */
    protected long alignTimestamp(final long timestamp) {
      return timestamp - (timestamp % specification.getInterval());
    }
    
    // ---------------------- //
    // Doubles interface //
    // ---------------------- //

    @Override
    public boolean hasNextValue() {
      initializeIfNotDone();
      if (run_all) {
        return has_next_value_from_source;
      }
      return has_next_value_from_source &&
          next_dp.timestamp() < timestamp_end_interval;
    }

    @Override
    public double nextDoubleValue() {
      if (hasNextValue()) {
        double value = next_dp.toDouble();
        moveToNextValue();
        return value;
      }
      throw new NoSuchElementException("no more values in interval of "
          + timestamp_end_interval);
    }

    @Override
    public String toString() {
      final StringBuilder buf = new StringBuilder();
      buf.append("ValuesInInterval: ")
         .append(", timestamp_end_interval=").append(timestamp_end_interval)
         .append(", has_next_value_from_source=")
         .append(has_next_value_from_source)
         .append(", previousCalendar=")
         .append(previous_calendar == null ? "null" : previous_calendar)
         .append(", nextCalendar=")
         .append(next_calendar == null ? "null" : next_calendar);
      if (has_next_value_from_source) {
        buf.append(", nextValue=(").append(next_dp).append(')');
      }
      buf.append(", source=").append(source);
      return buf.toString();
    }
  }

  
  @Override
  public long valueCount() {
    throw new UnsupportedOperationException();
  }
}
