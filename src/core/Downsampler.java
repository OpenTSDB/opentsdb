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

import static net.opentsdb.utils.DateTime.toEndOfDay;
import static net.opentsdb.utils.DateTime.toEndOfMonth;
import static net.opentsdb.utils.DateTime.toEndOfWeek;
import static net.opentsdb.utils.DateTime.toEndOfYear;
import static net.opentsdb.utils.DateTime.toStartOfDay;
import static net.opentsdb.utils.DateTime.toStartOfMonth;
import static net.opentsdb.utils.DateTime.toStartOfWeek;
import static net.opentsdb.utils.DateTime.toStartOfYear;

import java.util.NoSuchElementException;
import java.util.TimeZone;

/**
 * Iterator that downsamples data points using an {@link Aggregator}.
 */
public class Downsampler implements SeekableView, DataPoint {

  static final long ONE_WEEK_INTERVAL = 604800000L;
  static final long ONE_MONTH_INTERVAL = 2592000000L;
  static final long ONE_YEAR_INTERVAL = 31536000000L;
  static final long ONE_DAY_INTERVAL = 86400000L;
  
  /** Function to use for downsampling. */
  protected final Aggregator downsampler;
  /** Iterator to iterate the values of the current interval. */
  protected final ValuesInInterval values_in_interval;
  /** Last normalized timestamp */ 
  protected long timestamp;
  /** Last value as a double */
  protected double value;
  
  /**
   * Ctor (for backward compatibility).
   * @param source The iterator to access the underlying data.
   * @param interval_ms The interval in milli seconds wanted between each data
   * point.
   * @param downsampler The downsampling function to use.
   */
  Downsampler(final SeekableView source,
              final long interval_ms,
              final Aggregator downsampler) {
    this(source, interval_ms, downsampler, null, false);
  }

  /**
   * Ctor.
   * @param source The iterator to access the underlying data.
   * @param interval_ms The interval in milli seconds wanted between each data
   * point.
   * @param downsampler The downsampling function to use.
   * @param timezone The timezone to use for aligning intervals based on the calendar.
   * @param use_calendar A flag denoting whether or not to align intervals based on the calendar.
   */
  Downsampler(final SeekableView source, 
              final long interval_ms, 
              final Aggregator downsampler,
              final TimeZone timezone,
              final boolean use_calendar) {
    this.values_in_interval = new ValuesInInterval(source, interval_ms, timezone, use_calendar);
    this.downsampler = downsampler;
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
      value = downsampler.runDouble(values_in_interval);
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
       .append("interval_ms=").append(values_in_interval.interval_ms)
       .append(", downsampler=").append(downsampler)
       .append(", current data=(timestamp=").append(timestamp)
       .append(", value=").append(value)
       .append("), values_in_interval=").append(values_in_interval);
   return buf.toString();
  }

  /** Iterates source values for an interval. */
  protected static class ValuesInInterval implements Aggregator.Doubles {

    /** The iterator of original source values. */
    private final SeekableView source;
    /** The sampling interval in milliseconds. */
    protected final long interval_ms;
    /** The end of the current interval. */
    private long timestamp_end_interval = Long.MIN_VALUE;
    /** True if the last value was successfully extracted from the source. */
    private boolean has_next_value_from_source = false;
    /** The last data point extracted from the source. */
    private DataPoint next_dp = null;
    /** The timezone to use for aligning intervals based on the calendar */
    private final TimeZone timezone;
    /** A flag denoting whether or not to align intervals based on the calendar */
    private final boolean use_calendar;

    /** True if it is initialized for iterating intervals. */
    private boolean initialized = false;

    /**
     * Constructor.
     * @param source The iterator to access the underlying data.
     * @param interval_ms Downsampling interval.
     */
    ValuesInInterval(final SeekableView source, final long interval_ms, 
                     final TimeZone timezone, boolean use_calendar) {
      this.source = source;
      this.interval_ms = interval_ms;
      this.timestamp_end_interval = interval_ms;
      this.timezone = timezone != null ? timezone : TimeZone.getDefault();
      this.use_calendar = use_calendar;
    }

    /** Initializes to iterate intervals. */
    protected void initializeIfNotDone() {
      // NOTE: Delay initialization is required to not access any data point
      // from the source until a user requests it explicitly to avoid the severe
      // performance penalty by accessing the unnecessary first data of a span.
      if (!initialized) {
        initialized = true;
        moveToNextValue();
        resetEndOfInterval();
      }
    }

    /** Extracts the next value from the source. */
    private void moveToNextValue() {
      if (source.hasNext()) {
        has_next_value_from_source = true;
        next_dp = source.next();
      } else {
        has_next_value_from_source = false;
      }
    }

    /**
     * Resets the current interval with the interval of the timestamp of
     * the next value read from source. It is the first value of the next
     * interval. */
    private void resetEndOfInterval() {
      if (has_next_value_from_source) {
        // Sets the end of the interval of the timestamp.
        
        if (use_calendar && isCalendarInterval()) {
          timestamp_end_interval =  toEndOfInterval(next_dp.timestamp());
        }  else {
          // default timestamp normalization (tsdb v2.1.0)
           timestamp_end_interval = alignTimestamp(next_dp.timestamp()) + 
            interval_ms;
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
      // timestamp.
      if (use_calendar && isCalendarInterval()) {
        source.seek(alignTimestamp(timestamp + toEndOfInterval(timestamp)
            - toStartOfInterval(timestamp)));
      }  else {
        source.seek(alignTimestamp(timestamp + interval_ms - 1));
      }
      
      initialized = false;
    }

    /** Returns the representative timestamp of the current interval. */
    protected long getIntervalTimestamp() {
      // NOTE: It is well-known practice taking the start time of
      // a downsample interval as a representative timestamp of it. It also
      // provides the correct context for seek.
      if (use_calendar && isCalendarInterval()) {
        return toStartOfInterval(timestamp_end_interval);
      }  else {
        return alignTimestamp(timestamp_end_interval - interval_ms);
      }
    }

    /** Returns timestamp aligned by interval. */
    protected long alignTimestamp(final long timestamp) {
      if (use_calendar && isCalendarInterval()) {
        return toStartOfInterval(timestamp);
      }  else {
        return timestamp - (timestamp % interval_ms);
      }
    }
    
    /** Returns a flag denoting whether the interval can
     *  be aligned to the calendar */
    private boolean isCalendarInterval () {
      if (interval_ms != 0 && 
         (interval_ms % ONE_YEAR_INTERVAL == 0 ||
          interval_ms % ONE_MONTH_INTERVAL == 0 ||
          interval_ms % ONE_WEEK_INTERVAL == 0 ||
          interval_ms % ONE_DAY_INTERVAL == 0)) {
        return true;
      }
      return false;
    }
    
    /** Returns a timestamp corresponding to the start of the interval
     *  in which the specified timestamp occurs, aligned to the calendar
     *  based on the timezone. */
    private long toStartOfInterval(long timestamp) {
      if (interval_ms % ONE_YEAR_INTERVAL == 0) {
        final long multiplier = interval_ms / ONE_YEAR_INTERVAL;
        long result = timestamp;
        for (long i = 0; i < multiplier; i++) {
          result = toStartOfYear(result, timezone) - 1;
        }
        return result + 1;
      } else if (interval_ms % ONE_MONTH_INTERVAL == 0) {
        final long multiplier = interval_ms / ONE_MONTH_INTERVAL;
        long result = timestamp;
        for (long i = 0; i < multiplier; i++) {
          result = toStartOfMonth(result, timezone) - 1;
        }
        return result + 1;
      } else if (interval_ms % ONE_WEEK_INTERVAL == 0) {
        final long multiplier = interval_ms / ONE_WEEK_INTERVAL;
        long result = timestamp;
        for (long i = 0; i < multiplier; i++) {
          result = toStartOfWeek(result, timezone) - 1;
        }
        return result + 1;
      } else if (interval_ms % ONE_DAY_INTERVAL == 0) {
        final long multiplier = interval_ms / ONE_DAY_INTERVAL;
        long result = timestamp;
        for (long i = 0; i < multiplier; i++) {
          result = toStartOfDay(result, timezone) - 1;
        }
        return result + 1;
      } else {
        throw new IllegalArgumentException(interval_ms + " does not correspond to a "
            + "an interval that can be aligned to the calendar.");
      }
    }

    /** Returns a timestamp corresponding to the end of the interval
     *  in which the specified timestamp occurs, aligned to the calendar
     *  based on the timezone. */
    private long toEndOfInterval(long timestamp) {
      if (interval_ms % ONE_YEAR_INTERVAL == 0) {
        final long multiplier = interval_ms / ONE_YEAR_INTERVAL;
        long result = timestamp;
        for (long i = 0; i < multiplier; i++) {
          result = toEndOfYear(result, timezone) + 1;
        }
        return result - 1;
      } else if (interval_ms % ONE_MONTH_INTERVAL == 0) {
        final long multiplier = interval_ms / ONE_MONTH_INTERVAL;
        long result = timestamp;
        for (long i = 0; i < multiplier; i++) {
          result = toEndOfMonth(result, timezone) + 1;
        }
        return result - 1;
      } else if (interval_ms % ONE_WEEK_INTERVAL == 0) {
        final long multiplier = interval_ms / ONE_WEEK_INTERVAL;
        long result = timestamp;
        for (long i = 0; i < multiplier; i++) {
          result = toEndOfWeek(result, timezone) + 1;
        }
        return result - 1;
      } else if (interval_ms % ONE_DAY_INTERVAL == 0) {
        final long multiplier = interval_ms / ONE_DAY_INTERVAL;
        long result = timestamp;
        for (long i = 0; i < multiplier; i++) {
          result = toEndOfDay(result, timezone) + 1;
        }
        return result - 1;
      } else {
        throw new IllegalArgumentException(interval_ms + " does not correspond to a "
            + "an interval that can be aligned to the calendar.");
      }
    }
     

    // ---------------------- //
    // Doubles interface //
    // ---------------------- //

    @Override
    public boolean hasNextValue() {
      initializeIfNotDone();
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
         .append("interval_ms=").append(interval_ms)
         .append(", timestamp_end_interval=").append(timestamp_end_interval)
         .append(", has_next_value_from_source=")
         .append(has_next_value_from_source);
      if (has_next_value_from_source) {
        buf.append(", nextValue=(").append(next_dp).append(')');
      }
      buf.append(", source=").append(source);
      return buf.toString();
    }
  }
}
