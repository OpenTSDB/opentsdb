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

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import net.opentsdb.core.Aggregator.Doubles;
import net.opentsdb.rollup.RollupQuery;
import net.opentsdb.utils.DateTime;

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
  
  /** An optional calendar set to the current timestamp for the data point */
  private final Calendar previous_calendar;
  
  /** An optional calendar set to the end of the interval timestamp */
  private final Calendar next_calendar;

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
   * @throws IllegalArgumentException if fill_policy is interpolation.
   * @deprecated as of 2.3
   */
  FillingDownsampler(final SeekableView source, final long start_time,
      final long end_time, final long interval_ms,
      final Aggregator downsampler, final FillPolicy fill_policy) {
    this(source, start_time, end_time, 
        new DownsamplingSpecification(interval_ms, downsampler, fill_policy)
        , 0, 0);
  }
  
  /** 
   * Create a new filling downsampler.
   * @param source The iterator to access the underlying data.
   * @param start_time The time in milliseconds at which the data begins.
   * @param end_time The time in milliseconds at which the data ends.
   * @param specification The downsampling spec to use
   * @param query_start The start timestamp of the actual query for use with "all"
   * @param query_end The end timestamp of the actual query for use with "all"
   * @throws IllegalArgumentException if fill_policy is interpolation.
   * @since 2.3
   */
  FillingDownsampler(final SeekableView source, final long start_time,
      final long end_time, final DownsamplingSpecification specification, 
      final long query_start, final long end_start) {
    this(source, start_time, end_time, specification, query_start, end_start,
        null);
  }
  
  /** 
   * Create a new filling downsampler.
   * @param source The iterator to access the underlying data.
   * @param start_time The time in milliseconds at which the data begins.
   * @param end_time The time in milliseconds at which the data ends.
   * @param specification The downsampling spec to use
   * @param query_start The start timestamp of the actual query for use with "all"
   * @param query_end The end timestamp of the actual query for use with "all"
   * @param rollup_query An optional rollup query.
   * @throws IllegalArgumentException if fill_policy is interpolation.
   * @since 2.4
   */
  FillingDownsampler(final SeekableView source, final long start_time,
      final long end_time, final DownsamplingSpecification specification, 
      final long query_start, final long end_start, 
      final RollupQuery rollup_query) {
    // Lean on the superclass implementation.
    super(source, specification, query_start, end_start, rollup_query);

    // Ensure we aren't given a bogus fill policy.
    if (FillPolicy.NONE == specification.getFillPolicy()) {
      throw new IllegalArgumentException("Cannot instantiate this class with" +
        " linear-interpolation fill policy");
    }
    
    // Use the values-in-interval object to align the timestamps at which we
    // expect data to arrive for the first and last intervals.
    if (run_all) {
      timestamp = start_time;
      end_timestamp = end_time;
      previous_calendar = next_calendar = null;
    } else if (specification.useCalendar()) {
      previous_calendar = DateTime.previousInterval(start_time, interval, unit, 
          specification.getTimezone());
      if (unit == WEEK_UNIT) {
        previous_calendar.add(DAY_UNIT, -(interval * WEEK_LENGTH));
      } else {
        previous_calendar.add(unit, -interval);
      }
      next_calendar = DateTime.previousInterval(start_time, interval, unit, 
          specification.getTimezone());
      
      final Calendar end_calendar = DateTime.previousInterval(
          end_time, interval, unit, specification.getTimezone());
      if (end_calendar.getTimeInMillis() == next_calendar.getTimeInMillis()) {
        // advance once
        if (unit == WEEK_UNIT) {
          end_calendar.add(DAY_UNIT, interval * WEEK_LENGTH);
        } else {
          end_calendar.add(unit, interval);
        }
      }
      timestamp = next_calendar.getTimeInMillis();
      end_timestamp = end_calendar.getTimeInMillis();
    } else {
      // Use the values-in-interval object to align the timestamps at which we
      // expect data to arrive for the first and last intervals.
      timestamp = values_in_interval.alignTimestamp(start_time);
      end_timestamp = values_in_interval.alignTimestamp(end_time);
      previous_calendar = next_calendar = null;
    }
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
    if (run_all) {
      return values_in_interval.hasNextValue();
    }
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
      long actual = values_in_interval.hasNextValue() ? 
          values_in_interval.getIntervalTimestamp() : Long.MAX_VALUE;
      
      while (!run_all && values_in_interval.hasNextValue() 
          && actual < timestamp) {
        // The actual timestamp precedes our expected, so there's data in the
        // values-in-interval object that we wish to ignore.
        specification.getFunction().runDouble(values_in_interval);
        values_in_interval.moveToNextInterval();
        actual = values_in_interval.getIntervalTimestamp();
      }
      
      // Check whether the timestamp of the calculation interval matches what
      // we expect.
      if (run_all || actual == timestamp) {
        // The calculated interval timestamp matches what we expect, so we can
        // do normal processing.
        if (rollup_query != null && 
            (rollup_query.getRollupAgg() == Aggregators.AVG || 
            rollup_query.getRollupAgg() == Aggregators.DEV)) {
          if (rollup_query.getRollupAgg() == Aggregators.AVG) {
            if (specification.getFunction() == Aggregators.AVG) {
              double sum = 0;
              long count = 0;
              while (values_in_interval.hasNextValue()) {
                count += values_in_interval.nextValueCount();
                sum += values_in_interval.nextDoubleValue();
              }
              if (count == 0) { // avoid # / 0
                value = 0;
              } else {
                value = sum / (double)count;
              }
            } else {
              class Accumulator implements Doubles {
                List<Double> values = new ArrayList<Double>();
                Iterator<Double> iterator;
                @Override
                public boolean hasNextValue() {
                  return iterator.hasNext();
                }
                @Override
                public double nextDoubleValue() {
                  return iterator.next();
                }
              }
              
              final Accumulator accumulator = new Accumulator();
              while (values_in_interval.hasNextValue()) {
                long count = values_in_interval.nextValueCount();
                double sum = values_in_interval.nextDoubleValue();
                if (count == 0) {
                  accumulator.values.add(0D);
                } else {
                  accumulator.values.add(sum / (double) count);
                }
              }
              accumulator.iterator = accumulator.values.iterator();
              value = specification.getFunction().runDouble(accumulator);
            }
          } else if (specification.getFunction() == Aggregators.DEV) {
            throw new UnsupportedOperationException("Standard deviation over "
                + "rolled up data is not supported at this time");
          }
        } else if (rollup_query != null && 
            specification.getFunction() == Aggregators.COUNT) {
          double count = 0;
          while (values_in_interval.hasNextValue()) {
            count += values_in_interval.nextDoubleValue();
          }
          value = count;
        } else {
          value = specification.getFunction().runDouble(values_in_interval);
        }
        values_in_interval.moveToNextInterval();
      } else {
        // Our expected timestamp precedes the actual, so the interval is
        // missing. We will use a special value, based on the fill policy, to
        // represent this case.
        switch (specification.getFillPolicy()) {
        case NOT_A_NUMBER:
        case NULL:
          value = Double.NaN;
          break;

        case ZERO:
          value = 0.0;
          break;
          
          // TODO - scalar

        default:
          throw new RuntimeException("unhandled fill policy");
        }
      }

      // Advance the expected timestamp to the next interval.
      if (!run_all) {
        if (specification.useCalendar()) {
          if (unit == WEEK_UNIT) {
            previous_calendar.add(DAY_UNIT, interval * WEEK_LENGTH);
            next_calendar.add(DAY_UNIT, interval * WEEK_LENGTH);
          } else {
            previous_calendar.add(unit, interval);
            next_calendar.add(unit, interval);
          }
          timestamp = next_calendar.getTimeInMillis();
        } else {
          timestamp += specification.getInterval();
        }
      }

      // This object also represents the data.
      return this;
    }

    // Ideally, the user will not call this method when no data remains, but
    // we can't enforce that.
    throw new NoSuchElementException("no more data points in " + this);
  }

  @Override
  public long timestamp() {
    if (run_all) {
      return query_start;
    } else if (specification.useCalendar()) {
      return previous_calendar.getTimeInMillis();
    }
    return timestamp - specification.getInterval();
  }
}

