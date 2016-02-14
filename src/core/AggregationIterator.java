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

import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

import com.google.common.annotations.VisibleForTesting;

import net.opentsdb.core.Aggregators.Interpolation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Iterator that aggregates multiple spans or time series data and does linear
 * interpolation (lerp) for missing data points.
 * <p>
 * This where the real business of {@link SpanGroup} is.  This iterator
 * provides a merged, aggregated view of multiple {@link Span}s.  The data
 * points in all the Spans are returned in chronological order.  Each time
 * we return a data point from a span, we aggregate it with the current
 * value from all the other Spans.  If other Spans don't have a value at
 * that specific timestamp, we do a linear interpolation in order to
 * estimate what the value of that Span should be at that time.
 * <p>
 * All this merging, linear interpolation and aggregation happens in
 * {@code O(1)} space and {@code O(N)} time.  All we need is to keep an
 * iterator on each Span, and {@code 4*k} {@code long}s in memory, where
 * {@code k} is the number of Spans in the group.  When computing a rate,
 * we need an extra {@code 2*k} {@code long}s in memory (see below).
 * <p>
 * In order to do linear interpolation, we need to know two data points:
 * the current one and the next one.  So for each Span in the group, we need
 * 4 longs: the current value, the current timestamp, the next value and the
 * next timestamp.  We maintain two arrays for timestamps and values.  Those
 * arrays have {@code 2 * iterators.length} elements.  The first half
 * contains the current values and second half the next values.  When a Span
 * gets used, its next data point becomes the current one (so its value and
 * timestamp are moved from the 2nd half of their respective array to the
 * first half) and the new-next data point is fetched from the underlying
 * iterator of that Span.
 * <p>
 * Here is an example when the SpanGroup contains 2 Spans:
 * <pre>              current    |     next
 *               +-------+-------+-------+-------+
 *   timestamps: |  T1   |  T2   |  T3   |  T4   |
 *               +-------+-------+-------+-------+
 *                    current    |     next
 *               +-------+-------+-------+-------+
 *       values: |  V1   |  V2   |  V3   |  V4   |
 *               +-------+-------+-------+-------+
 *                               |
 *   current: 0
 *   pos: 0
 *   iterators: [ it0, it1 ]
 * </pre>
 * Since {@code current == 0}, the current data point has the value V1
 * and time T1.  Let's note that (V1, T1).  Now this group has 2 Spans,
 * which means we're trying to aggregate 2 different series (same metric ID
 * but different tags).  So The next value that this iterator returns needs
 * to be a combination of V1 and V2 (assuming that T2 is less than T1).
 * If our aggregation function is "sum", we sort of want to sum up V1 and
 * V2.  But those two data points may not necessarily be at the same time.
 * T2 can be less than or equal to T1.  If T2 is greater than T1, we ignore
 * V2 and return just V1, since we haven't reached the time yet where V2
 * exist, so it's essentially as if it wasn't there.
 * Say T2 is less than T1.  Summing up V1 and V2 doesn't make sense, since
 * they represent two measurements made at different times.  So instead,
 * we need to find what the value V2 would have been, had it been measured
 * at time T1 instead of T2.  We do this using linear interpolation between
 * the data point (V2, T2) and the following one for that series, (V4, T4).
 * The result is thus the sum of V1 and the interpolated value between V2
 * and V4.
 * <p>
 * Now let's move onto the next data point.  Assuming that T3 is less than
 * T4, it means we need to advance to the next point on the 1st series.  To
 * do this we use the iterator it0 to get the next data point for that
 * series and we end up with the following state:
 * <pre>              current    |     next
 *               +-------+-------+-------+-------+
 *   timestamps: |  T3   |  T2   |  T5   |  T4   |
 *               +-------+-------+-------+-------+
 *                    current    |     next
 *               +-------+-------+-------+-------+
 *       values: |  V3   |  V2   |  V5   |  V4   |
 *               +-------+-------+-------+-------+
 *                               |
 *   current: 0
 *   pos: 0
 *   iterators: [ it0, it1 ]
 * </pre>
 * Then all you need is to "rinse and repeat".
 * <p>
 * More details: Since each value above can be either an integer or a
 * floating point, we have to keep track of the type of each value.  Values
 * are always stored in a {@code long}.  When a value is a floating point
 * value, the bits of the longs just need to be interpreted to get back the
 * floating point value.  The way we keep track of the type is by using the
 * most significant bit of the timestamp (to avoid an extra array).  This is
 * fine since our timestamps only really use 32 of the 64 bits of the long
 * in which they're stored.  When there is no "current" value (1st half of
 * the arrays depicted above), the timestamp will be set to 0.  When there
 * is no "next" value (2nd half of the arrays), the timestamp will be set
 * to a special, really large value (too large to be a valid timestamp).
 * <p>
 */
final class AggregationIterator implements SeekableView, DataPoint,
                                           Aggregator.Longs, Aggregator.Doubles {

  private static final Logger LOG =
      LoggerFactory.getLogger(AggregationIterator.class);

  /** Extra bit we set on the timestamp of floating point values. */
  private static final long FLAG_FLOAT = 0x8000000000000000L;

  /** Mask to use in order to get rid of the flag above.
   * This value also conveniently represents the largest timestamp we can
   * possibly store, provided that the most significant bit is reserved by
   * FLAG_FLOAT.
   */
  private static final long TIME_MASK  = 0x7FFFFFFFFFFFFFFFL;

  /** Aggregator to use to aggregate data points from different Spans. */
  private final Aggregator aggregator;

  /** Interpolation method to use when aggregating time series */
  private final Interpolation method;

  /** If true, use rate of change instead of actual values. */
  private final boolean rate;

  /**
   * Where we are in each {@link Span} in the group.
   * The iterators in this array always points to 2 values ahead of the
   * current value, as we pre-load the current and the next values into the
   * {@link #timestamps} and {@link #values} member.
   * Once we reach the end of a Span, we'll null out its iterator from this
   * array.
   */
  private final SeekableView[] iterators;

  /** Start time (UNIX timestamp in seconds or ms) on 32 bits ("unsigned" int). */
  private final long start_time;

  /** End time (UNIX timestamp in seconds or ms) on 32 bits ("unsigned" int). */
  private final long end_time;

  /**
   * The current and previous timestamps for the data points being used.
   * <p>
   * Are we computing a rate?
   * <ul>
   * <li>No: for {@code iterators[i]} the timestamp of the current data
   *     point is {@code timestamps[i]} and the timestamp of the next data
   *     point is {@code timestamps[iterators.length + i]}.</li>
   * </li></ul>
   * <p>
   * Each timestamp can have the {@code FLAG_FLOAT} applied so it's important
   * to use the {@code TIME_MASK} when getting the actual timestamp value
   * out of it.
   * There are two special values for timestamps:
   * <ul>
   * <li>{@code 0} when in the first half of the array: this iterator has
   * run out of data points and must not be used anymore.</li>
   * <li>{@code TIME_MASK} when in the second half of the array: this
   * iterator has reached its last data point and must not be used for
   * linear interpolation anymore.</li>
   * </ul>
   */
  private final long[] timestamps; // 32 bit unsigned + flag

  /**
   * The current and next values for the data points being used.
   * This array works exactly in the same fashion as the 'timestamps' array.
   * This array is also used to store floating point values, in which case
   * their binary representation just happens to be stored in a {@code long}.
   */
  private final long[] values;

  /** The index in {@link #iterators} of the current Span being used. */
  private int current;

  /** The index in {@link #values} of the current value being aggregated. */
  private int pos;

  /**
   * Creates a new iterator for a {@link SpanGroup}.
   * @param spans Spans in a group.
   * @param start_time Any data point strictly before this timestamp will be
   * ignored.
   * @param end_time Any data point strictly after this timestamp will be
   * ignored.
   * @param aggregator The aggregation function to use.
   * @param method Interpolation method to use when aggregating time series
   * @param downsampler Aggregation function to use to group data points
   * within an interval.
   * @param sample_interval_ms Number of milliseconds wanted between each data
   * point.
   * @param rate If {@code true}, the rate of the series will be used instead
   * of the actual values.
   * @param rate_options Specifies the optional additional rate calculation
   * options.
   * @return An {@link AggregationIterator} object.
   */
  public static AggregationIterator create(final List<Span> spans,
                                           final long start_time,
                                           final long end_time,
                                           final Aggregator aggregator,
                                           final Interpolation method,
                                           final Aggregator downsampler,
                                           final long sample_interval_ms,
                                           final boolean rate,
                                           final RateOptions rate_options) {
    return create(spans, start_time, end_time, aggregator, method, downsampler,
        sample_interval_ms, rate, rate_options, null);
  }
  
  /**
   * Creates a new iterator for a {@link SpanGroup}.
   * @param spans Spans in a group.
   * @param start_time Any data point strictly before this timestamp will be
   * ignored.
   * @param end_time Any data point strictly after this timestamp will be
   * ignored.
   * @param aggregator The aggregation function to use.
   * @param method Interpolation method to use when aggregating time series
   * @param downsampler Aggregation function to use to group data points
   * within an interval.
   * @param sample_interval_ms Number of milliseconds wanted between each data
   * point.
   * @param rate If {@code true}, the rate of the series will be used instead
   * of the actual values.
   * @param rate_options Specifies the optional additional rate calculation
   * options.
   * @param fill_policy Policy specifying whether to interpolate or to fill
   * missing intervals with special values.
   * @return An {@link AggregationIterator} object.
   * @since 2.2
   */
  public static AggregationIterator create(final List<Span> spans,
                                           final long start_time,
                                           final long end_time,
                                           final Aggregator aggregator,
                                           final Interpolation method,
                                           final Aggregator downsampler,
                                           final long sample_interval_ms,
                                           final boolean rate,
                                           final RateOptions rate_options,
                                           final FillPolicy fill_policy) {
    final int size = spans.size();
    final SeekableView[] iterators = new SeekableView[size];
    for (int i = 0; i < size; i++) {
      SeekableView it;
      if (downsampler == null) {
        it = spans.get(i).spanIterator();
      } else {
        it = spans.get(i).downsampler(start_time, end_time, sample_interval_ms, 
            downsampler, fill_policy);
      }
      if (rate) {
        it = new RateSpan(it, rate_options);
      }
      iterators[i] = it;
    }
    return new AggregationIterator(iterators, start_time, end_time, aggregator,
                                   method, rate);
  }

  /**
   * Creates an aggregation iterator for a group of data point iterators.
   * @param iterators An array of Seekable views of spans in a group. Ignored
   * if {@code null}. We modify the array while processing data points.
   * @param start_time Any data point strictly before this timestamp will be
   * ignored.
   * @param end_time Any data point strictly after this timestamp will be
   * ignored.
   * @param aggregator The aggregation function to use.
   * @param method Interpolation method to use when aggregating time series
   * @param rate If {@code true}, the rate of the series will be used instead
   * of the actual values.
   */
  private AggregationIterator(final SeekableView[] iterators,
                              final long start_time,
                              final long end_time,
                              final Aggregator aggregator,
                              final Interpolation method,
                              final boolean rate) {
    LOG.debug("Aggregating {} iterators", iterators.length);
    this.iterators = iterators;
    this.start_time = start_time;
    this.end_time = end_time;
    this.aggregator = aggregator;
    this.method = method;
    this.rate = rate;
    final int size = iterators.length;
    timestamps = new long[size * 2];
    values = new long[size * 2];
    // Initialize every Iterator, fetch their first values that fall
    // within our time range.
    int num_empty_spans = 0;
    for (int i = 0; i < size; i++) {
      SeekableView it = iterators[i];
      it.seek(start_time);
      final DataPoint dp;
      if (!it.hasNext()) {
        ++num_empty_spans;
        endReached(i);
        continue;
      }
      dp = it.next();
      //LOG.debug("Creating iterator #" + i);
      if (dp.timestamp() >= start_time) {
        //LOG.debug("First DP in range for #" + i + ": "
        //          + dp.timestamp() + " >= " + start_time);
        putDataPoint(size + i, dp);
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("No DP in range for #%d: %d < %d", i,
                                  dp.timestamp(), start_time));
        }
        endReached(i);
        continue;
      }
      if (rate) {
        // The first rate against the time zero should be populated
        // for the backward compatibility that uses the previous rate
        // instead of interpolating for aggregation when a data point is
        // missing for the current timestamp.
        // TODO: Use the next rate that contains the current timestamp.
        if (it.hasNext()) {
          moveToNext(i);
        } else {
          endReached(i);
        }
      }
    }
    if (num_empty_spans > 0) {
      LOG.debug(String.format("%d out of %d spans are empty!",
                              num_empty_spans, size));
    }
  }

  /**
   * Indicates that an iterator in {@link #iterators} has reached the end.
   * @param i The index in {@link #iterators} of the iterator.
   */
  private void endReached(final int i) {
    //LOG.debug("No more DP for #" + i);
    timestamps[iterators.length + i] = TIME_MASK;
    iterators[i] = null;  // We won't use it anymore, so free() it.
  }

  /**
   * Puts the next data point of an iterator in the internal buffer.
   * @param i The index in {@link #iterators} of the iterator.
   * @param dp The last data point returned by that iterator.
   */
  private void putDataPoint(final int i, final DataPoint dp) {
    timestamps[i] = dp.timestamp();
    if (dp.isInteger()) {
      //LOG.debug("Putting #" + i + " (long) " + dp.longValue()
      //          + " @ time " + dp.timestamp());
      values[i] = dp.longValue();
    } else {
      //LOG.debug("Putting #" + i + " (double) " + dp.doubleValue()
      //          + " @ time " + dp.timestamp());
      values[i] = Double.doubleToRawLongBits(dp.doubleValue());
      timestamps[i] |= FLAG_FLOAT;
    }
  }

  // ------------------ //
  // Iterator interface //
  // ------------------ //

  public boolean hasNext() {
    final int size = iterators.length;
    for (int i = 0; i < size; i++) {
      // As long as any of the iterators has a data point with a timestamp
      // that falls within our interval, we know we have at least one next.
      if ((timestamps[size + i] & TIME_MASK) <= end_time) {
        //LOG.debug("hasNext #" + (size + i));
        return true;
      }
    }
    //LOG.debug("No hasNext (return false)");
    return false;
  }

  public DataPoint next() {
    final int size = iterators.length;
    long min_ts = Long.MAX_VALUE;

    // In case we reached the end of one or more Spans, we need to make sure
    // we mark them as such by zeroing their current timestamp.  There may
    // be multiple Spans that reached their end at once, so check them all.
    for (int i = current; i < size; i++) {
      if (timestamps[i + size] == TIME_MASK) {
        //LOG.debug("Expiring last DP for #" + current);
        timestamps[i] = 0;
      }
    }

    // Now we need to find which Span we'll consume next.  We'll pick the
    // one that has the data point with the smallest timestamp since we want to
    // return them in chronological order.
    current = -1;
    // If there's more than one Span with the same smallest timestamp, we'll
    // set this to true so we can fetch the next data point in all of them at
    // the same time.
    boolean multiple = false;
    for (int i = 0; i < size; i++) {
      final long timestamp = timestamps[size + i] & TIME_MASK;
      if (timestamp <= end_time) {
        if (timestamp < min_ts) {
          min_ts = timestamp;
          current = i;
          // We just found a new minimum so right now we can't possibly have
          // multiple Spans with the same minimum.
          multiple = false;
        } else if (timestamp == min_ts) {
          multiple = true;
        }
      }
    }
    if (current < 0) {
      throw new NoSuchElementException("no more elements");
    }
    moveToNext(current);
    if (multiple) {
      //LOG.debug("Moving multiple DPs at time " + min_ts);
      // We know we saw at least one other data point with the same minimum
      // timestamp after `current', so let's move those ones too.
      for (int i = current + 1; i < size; i++) {
        final long timestamp = timestamps[size + i] & TIME_MASK;
        if (timestamp == min_ts) {
          moveToNext(i);
        }
      }
    }

    return this;
  }

  /**
   * Makes iterator number {@code i} move forward to the next data point.
   * @param i The index in {@link #iterators} of the iterator.
   */
  private void moveToNext(final int i) {
    final int next = iterators.length + i;
    timestamps[i] = timestamps[next];
    values[i] = values[next];
    //LOG.debug("Moving #" + next + " -> #" + i
    //          + ((timestamps[i] & FLAG_FLOAT) == FLAG_FLOAT
    //             ? " float " + Double.longBitsToDouble(values[i])
    //             : " long " + values[i])
    //          + " @ time " + (timestamps[i] & TIME_MASK));
    final SeekableView it = iterators[i];
    if (it.hasNext()) {
      putDataPoint(next, it.next());
    } else {
      endReached(i);
    }
  }

  public void remove() {
    throw new UnsupportedOperationException();
  }

  // ---------------------- //
  // SeekableView interface //
  // ---------------------- //

  public void seek(final long timestamp) {
    for (final SeekableView it : iterators) {
      it.seek(timestamp);
    }
  }

  // ------------------- //
  // DataPoint interface //
  // ------------------- //

  public long timestamp() {
    return timestamps[current] & TIME_MASK;
  }

  public boolean isInteger() {
    if (rate) {
      // An rate can never be precisely represented without floating point.
      return false;
    }
    // If at least one of the values we're going to aggregate or interpolate
    // with is a float, we have to convert everything to a float.
    for (int i = timestamps.length - 1; i >= 0; i--) {
      if ((timestamps[i] & FLAG_FLOAT) == FLAG_FLOAT) {
        return false;
      }
    }
    return true;
  }

  public long longValue() {
    if (isInteger()) {
      pos = -1;
      return aggregator.runLong(this);
    }
    throw new ClassCastException("current value is a double: " + this);
  }

  public double doubleValue() {
    if (!isInteger()) {
      pos = -1;
      final double value = aggregator.runDouble(this);
      //LOG.debug("aggregator returned " + value);
      if (Double.isInfinite(value)) {
        throw new IllegalStateException("Got Infinity: "
           + value + " in this " + this);
      }
      return value;
    }
    throw new ClassCastException("current value is a long: " + this);
  }

  public double toDouble() {
    return isInteger() ? longValue() : doubleValue();
  }

  // -------------------------- //
  // Aggregator.Longs interface //
  // -------------------------- //

  public boolean hasNextValue() {
    return hasNextValue(false);
  }

  /**
   * Returns whether or not there are more values to aggregate.
   * @param update_pos Whether or not to also move the internal pointer
   * {@link #pos} to the index of the next value to aggregate.
   * @return true if there are more values to aggregate, false otherwise.
   */
  private boolean hasNextValue(boolean update_pos) {
    final int size = iterators.length;
    for (int i = pos + 1; i < size; i++) {
      if (timestamps[i] != 0) {
        //LOG.debug("hasNextValue -> true #" + i);
        if (update_pos) {
          pos = i;
        }
        return true;
      }
    }
    //LOG.debug("hasNextValue -> false (ran out)");
    return false;
  }

  public long nextLongValue() {
    if (hasNextValue(true)) {
      final long y0 = values[pos];
      if (rate) {
        throw new AssertionError("Should not be here, impossible! " + this);
      }
      if (current == pos) {
        return y0;
      }
      final long x = timestamps[current] & TIME_MASK;
      final long x0 = timestamps[pos] & TIME_MASK;
      if (x == x0) {
        return y0;
      }
      final long y1 = values[pos + iterators.length];
      final long x1 = timestamps[pos + iterators.length] & TIME_MASK;
      if (x == x1) {
        return y1;
      }
      if ((x1 & Const.MILLISECOND_MASK) != 0) {
        throw new AssertionError("x1=" + x1 + " in " + this);
      }
      final long r;
      switch (method) {
        case LERP:
          r = y0 + (x - x0) * (y1 - y0) / (x1 - x0);
          //LOG.debug("Lerping to time " + x + ": " + y0 + " @ " + x0
          //          + " -> " + y1 + " @ " + x1 + " => " + r);
          break;
        case ZIM:
          r = 0;
          break;
        case MAX:
          r = Long.MAX_VALUE;
          break;
        case MIN:
          r = Long.MIN_VALUE;
          break;
        default:
          throw new IllegalDataException("Invalid interpolation somehow??");
      }
      return r;
    }
    throw new NoSuchElementException("no more longs in " + this);
  }

  // ---------------------------- //
  // Aggregator.Doubles interface //
  // ---------------------------- //

  public double nextDoubleValue() {
    if (hasNextValue(true)) {
      final double y0 = ((timestamps[pos] & FLAG_FLOAT) == FLAG_FLOAT
                         ? Double.longBitsToDouble(values[pos])
                         : values[pos]);
      if (current == pos) {
        //LOG.debug("Exact match, no lerp needed");
        return y0;
      }
      if (rate) {
        // No LERP for the rate. Just uses the rate of any previous timestamp.
        // If x0 is smaller than the current time stamp 'x', we just use
        // y0 as a current rate of the 'pos' span. If x0 is bigger than the
        // current timestamp 'x', we don't go back further and just use y0
        // instead. It happens only at the beginning of iteration.
        // TODO: Use the next rate the time range of which includes the current
        // timestamp 'x'.
        return y0;
      }
      final long x = timestamps[current] & TIME_MASK;
      final long x0 = timestamps[pos] & TIME_MASK;
      if (x == x0) {
        //LOG.debug("No lerp needed x == x0 (" + x + " == "+x0+") => " + y0);
        return y0;
      }
      final int next = pos + iterators.length;
      final double y1 = ((timestamps[next] & FLAG_FLOAT) == FLAG_FLOAT
                         ? Double.longBitsToDouble(values[next])
                         : values[next]);
      final long x1 = timestamps[next] & TIME_MASK;
      if (x == x1) {
        //LOG.debug("No lerp needed x == x1 (" + x + " == "+x1+") => " + y1);
        return y1;
      }
      if ((x1 & Const.MILLISECOND_MASK) != 0) {
        throw new AssertionError("x1=" + x1 + " in " + this);
      }
      final double r;
      switch (method) {
      case LERP:
        r = y0 + (x - x0) * (y1 - y0) / (x1 - x0);
        //LOG.debug("Lerping to time " + x + ": " + y0 + " @ " + x0
        //          + " -> " + y1 + " @ " + x1 + " => " + r);
        break;
      case ZIM:
        r = 0;
        break;
      case MAX:
        r = Double.MAX_VALUE;
        break;
      case MIN:
        r = Double.MIN_VALUE;
        break;
      default:
        throw new IllegalDataException("Invalid interploation somehow??");
    }
      return r;
    }
    throw new NoSuchElementException("no more doubles in " + this);
  }

  public String toString() {
    return "SpanGroup.Iterator(timestamps=" + Arrays.toString(timestamps)
      + ", values=" + Arrays.toString(values)
      + ", current=" + current
      + ", pos=" + pos
      + ", (SpanGroup: " + toStringSharedAttributes()
      + "), iterators=" + Arrays.toString(iterators)
      + ')';
  }

  private String toStringSharedAttributes() {
    return "start_time=" + start_time
      + ", end_time=" + end_time
      + ", rate=" + rate
      + ", aggregator=" + aggregator
      + ')';
  }

  /**
   * Creates an aggregation iterator for unit tests.
   * @param iterators An array of Seekable views of spans in a group. Ignored
   * if {@code null}. We modify the array while processing data points.
   * @param start_time Any data point strictly before this timestamp will be
   * ignored.
   * @param end_time Any data point strictly after this timestamp will be
   * ignored.
   * @param aggregator The aggregation function to use.
   * @param method Interpolation method to use when aggregating time series
   * @param rate If {@code true}, the rate of the series will be used instead
   * of the actual values.
   */
  @VisibleForTesting
  static AggregationIterator createForTesting(final SeekableView[] iterators,
                                              final long start_time,
                                              final long end_time,
                                              final Aggregator aggregator,
                                              final Interpolation method,
                                              final boolean rate) {
    return new AggregationIterator(iterators, start_time, end_time,
                                   aggregator, method, rate);
  }
}
