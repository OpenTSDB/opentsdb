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

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.core.HistogramDataPoint.HistogramBucket;

/**
 * 
 * This is where the real business of @{link HistogramSpanGroup}. It provides a merged and aggregated 
 * view of the data points. It will apply the following processing:
 * <ul>
 *    <li> Down sampling
 *    <li> Aggregation
 * </ul>
 *
 * The following processing is inapplicable:
 * <ul>
 *    <li> Interpolation
 *    <li> Rate Calculation
 * </ul>
 * 
 */
public class HistogramAggregationIterator implements HistogramSeekableView, HistogramDataPoint {
  private static final Logger LOG = LoggerFactory.getLogger(HistogramAggregationIterator.class);

  /** Aggregator to use to aggregate histogram data points from different HistogramSpans. */
  private final HistogramAggregation aggregation;

  /**
   * Where we are in each {@link HistogramSpan} in the group. 
   */
  private final HistogramSeekableView[] iterators;

  /**
   * Start time (UNIX timestamp in seconds or ms) on 32 bits ("unsigned" int).
   */
  private final long start_time;

  /** End time (UNIX timestamp in seconds or ms) on 32 bits ("unsigned" int). */
  private final long end_time;

  /**
   * The next timestamps for the data points being used
   */
  private final long[] timestamps;

  /**
   * The next values for the data points being used.
   */
  private final HistogramDataPoint[] values;

  /**
   * The current value
   */
  private HistogramDataPoint value;
  
  /**
   * Cotr.
   * 
   * @param spans The spans that join the aggregation
   * @param start_time Any data point strictly before this timestamp will be ignored.
   * @param end_time Any data point strictly after this timestamp will be ignored.
   * @param aggregation The aggregation will be applied on the spans 
   * @param downsampler The downsamper will be applied on each span
   * @param query_start The start time of the actual query
   * @param query_end The end time of the actual query
   * @param is_rollup Whether we are handling the rollup data points
   * @return
   */
  public static HistogramAggregationIterator create(final List<HistogramSpan> spans, 
                                                    final long start_time,
                                                    final long end_time, 
                                                    final HistogramAggregation aggregation, 
                                                    final DownsamplingSpecification downsampler,
                                                    final long query_start, 
                                                    final long query_end, 
                                                    final boolean is_rollup) {
    final int size = spans.size();
    final HistogramSeekableView[] iterators = new HistogramSeekableView[size];
    for (int i = 0; i < size; i++) {
      HistogramSeekableView it;
      if (downsampler == DownsamplingSpecification.NO_DOWNSAMPLER) {
        it = spans.get(i).spanIterator();
      } else {
        it = spans.get(i).downsampler(start_time, end_time, downsampler, is_rollup, query_start, query_end);
      }
      iterators[i] = it;
    }
    return new HistogramAggregationIterator(iterators, start_time, end_time, aggregation);
  }

  private HistogramAggregationIterator(final HistogramSeekableView[] iterators, 
                                       final long start_time,
                                       final long end_time,
                                       final HistogramAggregation aggregation) {
    this.iterators = iterators;
    this.start_time = start_time;
    this.end_time = end_time;
    this.aggregation = aggregation;

    timestamps = new long[this.iterators.length];
    values = new HistogramDataPoint[this.iterators.length];

    // Initialize every Iterator, fetch their first values that fall
    // within our time range.
    int num_empty_spans = 0;
    for (int i = 0; i < this.iterators.length; i++) {
      HistogramSeekableView it = iterators[i];
      it.seek(this.start_time);

      final HistogramDataPoint dp;
      if (!it.hasNext()) {
        ++num_empty_spans;
        endReached(i);
        continue;
      }

      dp = it.next();
      if (dp.timestamp() >= this.start_time) {
        putDataPoint(i, dp);
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("No DP in range for #%d: %d < %d", i, dp.timestamp(), start_time));
        }
        endReached(i);
        continue;
      }
    } // end for

    if (num_empty_spans > 0) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(String.format("%d out of %d spans are empty!", num_empty_spans, this.iterators.length));
      }
    }
  }

  /**
   * Indicates that an iterator in {@link #iterators} has reached the end.
   * 
   * @param i The index in {@link #iterators} of the iterator.
   */
  private void endReached(final int i) {
    timestamps[i] = 0;
    iterators[i] = null; // We won't use it anymore, so free() it.
  }

  /**
   * Puts the next data point of an iterator in the internal buffer.
   * 
   * @param i The index in {@link #iterators} of the iterator.
   * @param dp The last data point returned by that iterator.
   */
  private void putDataPoint(final int i, final HistogramDataPoint dp) {
    timestamps[i] = dp.timestamp();
    values[i] = dp.clone();
  }

  @Override
  public long timestamp() {
    return value.timestamp();
  }

  @Override
  public byte[] getRawData() {
    return value.getRawData();
  }

  @Override
  public void resetFromRawData(byte[] raw_data) {
    value.resetFromRawData(raw_data);
  }

  @Override
  public double percentile(double p) {
    return value.percentile(p);
  }

  @Override
  public List<Double> percentile(List<Double> p) {
    return value.percentile(p);
  }

  @Override
  public void aggregate(HistogramDataPoint histo, HistogramAggregation func) {
    value.aggregate(histo, func);
  }

  @Override
  public HistogramDataPoint clone() {
    return value.clone();
  }

  @Override
  public HistogramDataPoint cloneAndSetTimestamp(long timestamp) {
    return value.cloneAndSetTimestamp(timestamp);
  }

  @Override
  public boolean hasNext() {
    for (int i = 0; i < iterators.length; ++i) {
      if (0 != this.timestamps[i] && this.timestamps[i] <= this.end_time) {
        return true;
      }
    }

    return false;
  }

  @Override
  public HistogramDataPoint next() {
    if (!hasNext()) {
      throw new NoSuchElementException("no more elements");
    }
    
    long min_ts = Long.MAX_VALUE;
    int fist_min_ts_index = -1;
    boolean is_multiple = false;

    // find out the data points with the smallest timestamp
    for (int i = 0; i < this.iterators.length; ++i) {
      if (0 == this.timestamps[i]) {
        continue;
      }
      
      if (this.timestamps[i] > this.end_time) {
        continue;
      }

      if (this.timestamps[i] < min_ts) {
        min_ts = this.timestamps[i];
        fist_min_ts_index = i;
        is_multiple = false;
      } else if (this.timestamps[i] == min_ts) {
        is_multiple = true;
      }
    } // end for

    if (fist_min_ts_index < 0) {
      throw new NoSuchElementException("no more elements");
    }

    // do the aggregation on the data points with the smallest timestamp
    this.value = this.values[fist_min_ts_index];
    if (is_multiple) {
      for (int i = fist_min_ts_index + 1; i < this.iterators.length; ++i) {
        if (this.timestamps[i] == min_ts) {
          this.value.aggregate(this.values[i], this.aggregation);

          // move to next data point on this span
          moveToNext(i);
        }
      } // end for
    } 
    
    moveToNext(fist_min_ts_index);
    return this;
  }

  /**
   * Makes iterator number {@code i} move forward to the next data point.
   * 
   * @param i The index in {@link #iterators} of the iterator.
   */
  private void moveToNext(final int i) {
    final HistogramSeekableView it = iterators[i];
    if (it.hasNext()) {
      putDataPoint(i, it.next());
    } else {
      endReached(i);
    }
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void seek(long timestamp) {
    for (final HistogramSeekableView it : iterators) {
      it.seek(timestamp);
    }
  }
  
  @Override
  public Map<HistogramBucket, Long> getHistogramBucketsIfHas() {
    return this.value.getHistogramBucketsIfHas();
  }
}
