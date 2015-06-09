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
 * Iterator that downsamples data points using an {@link Aggregator}.
 */
public class Downsampler implements SeekableView, DataPoint {

  /** Function to use for downsampling. */
  private final Aggregator downsampler;
  /** Iterator to iterate the values of the current interval. */
  private final ValuesInInterval valuesInInterval;
  /** Last normalized timestamp */
  private long timestamp;
  /** Last value as a double */
  private double value;

  /**
   * Ctor.
   *
   * @param source The iterator to access the underlying data.
   * @param intervalMs The interval in milli seconds wanted between each data point.
   * @param downsampler The downsampling function to use.
   */
  Downsampler(final SeekableView source,
              final long intervalMs,
              final Aggregator downsampler) {
    this.valuesInInterval = new ValuesInInterval(source, intervalMs);
    this.downsampler = downsampler;
  }

  // ------------------ //
  // Iterator interface //
  // ------------------ //

  @Override
  public boolean hasNext() {
    return valuesInInterval.hasNextValue();
  }

  @Override
  public DataPoint next() {
    if (hasNext()) {
      value = downsampler.runDouble(valuesInInterval);
      timestamp = valuesInInterval.getIntervalTimestamp();
      valuesInInterval.moveToNextInterval();
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
    valuesInInterval.seekInterval(timestamp);
  }

  @Override
  public String toString() {
    final StringBuilder buf = new StringBuilder();
    buf.append("Downsampler: ")
        .append("intervalMs=").append(valuesInInterval.intervalMs)
        .append(", downsampler=").append(downsampler)
        .append(", current data=(timestamp=").append(timestamp)
        .append(", value=").append(value)
        .append("), valuesInInterval=").append(valuesInInterval);
    return buf.toString();
  }

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

  /** Iterates source values for an interval. */
  private static class ValuesInInterval implements Aggregator.Doubles {

    /** The iterator of original source values. */
    private final SeekableView source;
    /** The sampling interval in milliseconds. */
    private final long intervalMs;
    /** The end of the current interval. */
    private long timestampEndInterval = Long.MIN_VALUE;
    /** True if the last value was successfully extracted from the source. */
    private boolean hasNextValueFromSource = false;
    /** The last data point extracted from the source. */
    private DataPoint nextDp = null;

    /** True if it is initialized for iterating intervals. */
    private boolean initialized = false;

    /**
     * Constructor.
     *
     * @param source The iterator to access the underlying data.
     * @param intervalMs Downsampling interval.
     */
    ValuesInInterval(final SeekableView source, final long intervalMs) {
      this.source = source;
      this.intervalMs = intervalMs;
      this.timestampEndInterval = intervalMs;
    }

    /** Initializes to iterate intervals. */
    private void initializeIfNotDone() {
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
        hasNextValueFromSource = true;
        nextDp = source.next();
      } else {
        hasNextValueFromSource = false;
      }
    }

    /**
     * Resets the current interval with the interval of the timestamp of the next value read from
     * source. It is the first value of the next interval.
     */
    private void resetEndOfInterval() {
      if (hasNextValueFromSource) {
        // Sets the end of the interval of the timestamp.
        timestampEndInterval = alignTimestamp(nextDp.timestamp()) +
                                 intervalMs;
      }
    }

    /** Moves to the next available interval. */
    void moveToNextInterval() {
      initializeIfNotDone();
      resetEndOfInterval();
    }

    /** Advances the interval iterator to the given timestamp. */
    void seekInterval(long timestamp) {
      // To make sure that the interval of the given timestamp is fully filled,
      // rounds up the seeking timestamp to the smallest timestamp that is
      // a multiple of the interval and is greater than or equal to the given
      // timestamp..
      source.seek(alignTimestamp(timestamp + intervalMs - 1));
      initialized = false;
    }

    /** Returns the representative timestamp of the current interval. */
    private long getIntervalTimestamp() {
      // NOTE: It is well-known practice taking the start time of
      // a downsample interval as a representative timestamp of it. It also
      // provides the correct context for seek.
      return alignTimestamp(timestampEndInterval - intervalMs);
    }

    /** Returns timestamp aligned by interval. */
    private long alignTimestamp(long timestamp) {
      return timestamp - (timestamp % intervalMs);
    }

    // ---------------------- //
    // Doubles interface //
    // ---------------------- //

    @Override
    public boolean hasNextValue() {
      initializeIfNotDone();
      return hasNextValueFromSource &&
             nextDp.timestamp() < timestampEndInterval;
    }

    @Override
    public double nextDoubleValue() {
      if (hasNextValue()) {
        double value = nextDp.toDouble();
        moveToNextValue();
        return value;
      }
      throw new NoSuchElementException("no more values in interval of "
                                       + timestampEndInterval);
    }

    @Override
    public String toString() {
      final StringBuilder buf = new StringBuilder();
      buf.append("ValuesInInterval: ")
          .append("intervalMs=").append(intervalMs)
          .append(", timestampEndInterval=").append(timestampEndInterval)
          .append(", hasNextValueFromSource=")
          .append(hasNextValueFromSource);
      if (hasNextValueFromSource) {
        buf.append(", nextValue=(").append(nextDp).append(')');
      }
      buf.append(", source=").append(source);
      return buf.toString();
    }
  }
}
