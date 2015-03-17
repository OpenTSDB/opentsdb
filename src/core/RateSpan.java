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
  private MutableDataPoint prev_data = new MutableDataPoint();
  /** The latter of two raw data points used to calculate the next rate. */
  private MutableDataPoint next_data = new MutableDataPoint();
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
   * Move to the next datapoint to calculate rate.
   */
  private void moveToNextDatapoint() {
    if (source.hasNext()) {
      prev_data.reset(next_data);
      next_data.reset(source.next());
    }
  }

  /**
   * @return time delta in seconds
   */
  private double getTimeDeltaSecs() {
    // Validate the datapoint first.
    final long t0 = prev_data.timestamp();
    final long t1 = next_data.timestamp();
    if (t1 <= t0) {
      throw new IllegalStateException(
          "Next timestamp (" + t1 + ") is supposed to be "
          + " strictly greater than the previous one (" + t0 + "), but it's"
          + " not.  this=" + this);
    }
    return (double)((t1 - t0) / 1000.0);
  }

  /**
   * @return time delta in seconds
   */
  private double getValueDifference() {
    double difference;
    if (prev_data.isInteger() && next_data.isInteger()) {
      // NOTE: Calculates in the long type to avoid precision loss
      // while converting long values to double values if both values are long.
      // NOTE: Ignores the integer overflow.
      difference = next_data.longValue() - prev_data.longValue();
    } else {
      difference = next_data.toDouble() - prev_data.toDouble();
    }
    return difference;
  }

  /*
   * Marks the end of rate time series.
   */
  private void markEndOfSeries() {
    // Invalidates the next rate with invalid timestamp.
    next_rate.reset(INVALID_TIMESTAMP, 0);
  }
  /**
   * Populate the next rate.
   */
  private void populateNextRate() {
    if (source.hasNext()) {
      moveToNextDatapoint();
      double difference = getValueDifference();
      if (options.isCounter() && difference < 0) {
        while(source.hasNext()) {
          moveToNextDatapoint();
          difference = getValueDifference();
          if (difference >= 0) {
            break;
          }
        }
        if (!source.hasNext()) {
          markEndOfSeries();
          return;
        }
      }
      // TODO: for backwards compatibility we'll convert the ms to seconds
      // but in the future we should add a ratems flag that will calculate
      // the rate as is.
      final double time_delta_secs = getTimeDeltaSecs();
      next_rate.reset(next_data.timestamp(), (difference / time_delta_secs));
    } else {
      markEndOfSeries();
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
