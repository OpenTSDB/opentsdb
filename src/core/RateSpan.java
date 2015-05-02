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

import java.util.NoSuchElementException;


/**
 * Iterator that generates rates from a sequence of adjacent data points.
 */
public class RateSpan implements SeekableView {

  // The Long.MAX_VALUE works fine as the invalid timestamp with open-ended
  // time ranges.
  /** Timestamp to indicate that the data point is invalid. */
  private static long INVALID_TIMESTAMP = Long.MAX_VALUE;

  /** A sequence of data points to compute rates. */
  private final SeekableView source;
  /** Options for calculating rates. */
  private final RateOptions options;
  // TODO: use primitives for next_data, next_rate, and prev_rate instead
  // in order to reduce memory and CPU overhead.
  /** The latter of two raw data points used to calculate the next rate. */
  private final MutableDataPoint next_data = new MutableDataPoint();
  /** The rate that will be returned at the {@link #next} call. */
  private final MutableDataPoint next_rate = new MutableDataPoint();
  /** Users see this rate after they called next. */
  private final MutableDataPoint prev_rate = new MutableDataPoint();
  /** True if it is initialized for iterating rates of changes. */
  private boolean initialized = false;

  /**
   * Constructs a {@link RateSpan} instance.
   * @param source The iterator to access the underlying data.
   * @param options Options for calculating rates.
   */
  RateSpan(final SeekableView source, final RateOptions options) {
    this.source = source;
    this.options = options;
  }

  // ------------------ //
  // Iterator interface //
  // ------------------ //

  /** @return True if there is a valid next value. */
  @Override
  public boolean hasNext() {
    initializeIfNotDone();
    return next_rate.timestamp() != INVALID_TIMESTAMP;
  }

  /**
   * @return the next rate of changes.
   * @throws NoSuchElementException if there is no more data.
   */
  @Override
  public DataPoint next() {
    initializeIfNotDone();
    if (hasNext()) {
      // NOTE: Just copies currentRate to prevRate, and does not allocate
      // any new DataPoint object to reduce the memory allocation overhead.
      // So, users access data at prev_rate.
      prev_rate.reset(next_rate);
      populateNextRate();
      return prev_rate;
    } else {
      throw new NoSuchElementException("no more values for " + toString());
    }
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  // ---------------------- //
  // SeekableView interface //
  // ---------------------- //

  @Override
  public void seek(long timestamp) {
    source.seek(timestamp);
    initialized = false;
  }

  // ---------------------- //
  // Private methods        //
  // ---------------------- //

  /** Initializes to iterate rate of changes. */
  private void initializeIfNotDone() {
    // NOTE: Delay initialization is needed to not access any data point
    // from source until a user requests it explicitly to avoid the
    // performance penalty by accessing the first data of a span.
    if (!initialized) {
      initialized = true;
      // NOTE: Calculates the first rate between the time zero and the first
      // data point for the backward compatibility.
      // TODO: Don't compute the first rate with the time zero.
      next_data.reset(0, 0);
      // Sets the first rate to be retrieved.
      populateNextRate();
    }
  }

  /**
   * Populate the next rate.
   */
  private void populateNextRate() {
    final MutableDataPoint prev_data = new MutableDataPoint();
    if (source.hasNext()) {
      prev_data.reset(next_data);
      next_data.reset(source.next());
      
      final long t0 = prev_data.timestamp();
      final long t1 = next_data.timestamp();
      if (t1 <= t0) {
        throw new IllegalStateException(
            "Next timestamp (" + t1 + ") is supposed to be "
            + " strictly greater than the previous one (" + t0 + "), but it's"
            + " not.  this=" + this);
      }
      // TODO: for backwards compatibility we'll convert the ms to seconds
      // but in the future we should add a ratems flag that will calculate
      // the rate as is.
      final double time_delta_secs = ((double)(t1 - t0) / 1000.0);
      double difference;
      if (prev_data.isInteger() && next_data.isInteger()) {
        // NOTE: Calculates in the long type to avoid precision loss
        // while converting long values to double values if both values are long.
        // NOTE: Ignores the integer overflow.
        difference = next_data.longValue() - prev_data.longValue();
      } else {
        difference = next_data.toDouble() - prev_data.toDouble();
      }
      
      if (options.isCounter() && difference < 0) {
        if (options.getDropResets()) {
          populateNextRate();
          return;
        }
        
        if (prev_data.isInteger() && next_data.isInteger()) {
          // NOTE: Calculates in the long type to avoid precision loss
          // while converting long values to double values if both values are long.
          difference = options.getCounterMax() - prev_data.longValue() +
              next_data.longValue();
        } else {
          difference = options.getCounterMax() - prev_data.toDouble() +
              next_data.toDouble();
        }
        
        // If the rate is greater than the reset value, return a 0
        final double rate = difference / time_delta_secs;
        if (options.getResetValue() > RateOptions.DEFAULT_RESET_VALUE
            && rate > options.getResetValue()) {
          next_rate.reset(next_data.timestamp(), 0.0D);
        } else {
          next_rate.reset(next_data.timestamp(), rate);
        }
      } else {
        next_rate.reset(next_data.timestamp(), (difference / time_delta_secs));
      }
    } else {
      // Invalidates the next rate with invalid timestamp.
      next_rate.reset(INVALID_TIMESTAMP, 0);
    }
  }

  @Override
  public String toString() {
    final StringBuilder buf = new StringBuilder();
    buf.append("RateSpan: ")
       .append(", options=").append(options)
       .append(", next_data=[").append(next_data)
       .append("], next_rate=[").append(next_rate)
       .append("], prev_rate=[").append(prev_rate)
       .append("], source=[").append(source).append("]");
    return buf.toString();
  }
}
