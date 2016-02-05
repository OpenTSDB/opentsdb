// This file is part of OpenTSDB.
// Copyright (C) 2015  The OpenTSDB Authors.
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

import java.util.NoSuchElementException;
import java.util.TimeZone;

/**
 * A specialized downsampler that returns special values, based on the fill
 * policy, for intervals for which no data could be found. The default
 * implementation, {@link Downsampler}, simply skips intervals that have no
 * data, which causes the {@link AggregationIterator} up the chain to
 * interpolate.
 * @since 2.2
 */
public class FillingDownsampler extends Downsampler {
  /** Track when the downsampled data should end. */
  protected long end_timestamp;

  /** Downsampling fill policy. */
  protected final FillPolicy fill_policy;

  /** 
   * Ctor (preserved for backward compatibility).
   * @param source The iterator to access the underlying data.
   * @param start_time The time in milliseconds at which the data begins.
   * @param end_time The time in milliseconds at which the data ends.
   * @param interval_ms The interval in milli seconds wanted between each data
   * point.
   * @param downsampler The downsampling function to use.
   * @param fill_policy Policy specifying whether to interpolate or to fill
   * missing intervals with special values.
   * @throws IllegalArgumentException if fill_policy is interpolation.
   */
  FillingDownsampler(final SeekableView source, final long start_time,
      final long end_time, final long interval_ms,
      final Aggregator downsampler, final FillPolicy fill_policy) {
    this(source, start_time, end_time, interval_ms, downsampler, fill_policy, null, false);
  }
  
  
  /** 
   * Create a new nulling downsampler.
   * @param source The iterator to access the underlying data.
   * @param start_time The time in milliseconds at which the data begins.
   * @param end_time The time in milliseconds at which the data ends.
   * @param interval_ms The interval in milli seconds wanted between each data
   * point.
   * @param downsampler The downsampling function to use.
   * @param fill_policy Policy specifying whether to interpolate or to fill
   * missing intervals with special values.
   * @param timezone The timezone to use for aligning intervals based on the calendar.
   * @throws IllegalArgumentException if fill_policy is interpolation.
   */
  FillingDownsampler(final SeekableView source, final long start_time,
      final long end_time, final long interval_ms,
      final Aggregator downsampler, final FillPolicy fill_policy,
      final TimeZone timezone,
      final boolean use_calendar) {
    // Lean on the superclass implementation.
    super(source, interval_ms, downsampler, timezone, use_calendar);

    // Ensure we aren't given a bogus fill policy.
    if (FillPolicy.NONE == fill_policy) {
      throw new IllegalArgumentException("Cannot instantiate this class with" +
        " linear-interpolation fill policy");
    }
    this.fill_policy = fill_policy;

    // Use the values-in-interval object to align the timestamps at which we
    // expect data to arrive for the first and last intervals.
    this.timestamp = values_in_interval.alignTimestamp(start_time);
    this.end_timestamp = values_in_interval.alignTimestamp(end_time);
  }

  /**
   * Please note that when this method returns true, the value yielded by the
   * object returned by {@link #next()} might be NaN, which indicates no data
   * could be found for the current interval.
   * @return true if this iterator has not yet reached the end of the specified
   * range of data; otherwise, false.
   */
  @Override
  public boolean hasNext() {
    // No matter the state of the values-in-interval object, if our current
    // timestamp hasn't reached the end of the requested overall interval, then
    // we still have iterating to do.
    return timestamp < end_timestamp;
  }

  /**
   * Please note that the object returned by this method may return the value
   * NaN, which indicates that no data count be found for the interval. This is
   * intentional. Future intervals, if any, may still hava data and thus yield
   * non-NaN values.
   * @return the next data point, which might yield a NaN value.
   * @throws NoSuchElementException if no more intervals remain.
   */
  @Override
  public DataPoint next() {
    // Don't proceed if we've already completed iteration.
    if (hasNext()) {
      // Ensure that the timestamp we request is valid.
      values_in_interval.initializeIfNotDone();

      // Skip any leading data outside the query bounds.
      long actual = values_in_interval.getIntervalTimestamp();
      while (values_in_interval.hasNextValue() && actual < timestamp) {
        // The actual timestamp precedes our expected, so there's data in the
        // values-in-interval object that we wish to ignore.
        downsampler.runDouble(values_in_interval);
        values_in_interval.moveToNextInterval();
        actual = values_in_interval.getIntervalTimestamp();
      }

      // Check whether the timestamp of the calculation interval matches what
      // we expect.
      if (actual == timestamp) {
        // The calculated interval timestamp matches what we expect, so we can
        // do normal processing.
        value = downsampler.runDouble(values_in_interval);
        values_in_interval.moveToNextInterval();
      } else {
        // Our expected timestamp precedes the actual, so the interval is
        // missing. We will use a special value, based on the fill policy, to
        // represent this case.
        switch (fill_policy) {
        case NOT_A_NUMBER:
        case NULL:
          value = Double.NaN;
          break;

        case ZERO:
          value = 0.0;
          break;

        default:
          throw new RuntimeException("unhandled fill policy");
        }
      }

      // Advance the expected timestamp to the next interval.
      timestamp += values_in_interval.interval_ms;

      // This object also represents the data.
      return this;
    }

    // Ideally, the user will not call this method when no data remains, but
    // we can't enforce that.
    throw new NoSuchElementException("no more data points in " + this);
  }

  @Override
  public long timestamp() {
    return timestamp - values_in_interval.interval_ms;
  }
}

