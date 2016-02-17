// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
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
package net.opentsdb.query.expression;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import net.opentsdb.core.AggregationIterator;
import net.opentsdb.core.Aggregator;
import net.opentsdb.core.Aggregators;
import net.opentsdb.core.DataPoint;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.MutableDataPoint;
import net.opentsdb.core.SeekableView;
import net.opentsdb.core.TSQuery;
import net.opentsdb.core.Aggregators.Interpolation;

/**
 * Implements top-n functionality by iterating over each of the time series,
 * finding the max value for each time series within the query time range,
 * and up to "n" time series with the highest values, sorted in descending
 * order.
 * @since 2.3
 */
public class HighestMax implements Expression {

  @Override
  public DataPoints[] evaluate(final TSQuery data_query, 
      final List<DataPoints[]> query_results, final List<String> params) {
    if (data_query == null) {
      throw new IllegalArgumentException("Missing time series query");
    }
    if (query_results == null || query_results.isEmpty()) {
      return new DataPoints[]{};
    }
    // TODO(cl) - allow for empty top-n maybe? Just sort the results by max?
    if (params == null || params.isEmpty()) {
      throw new IllegalArgumentException("Need aggregation window for moving average");
    }

    String param = params.get(0);
    if (param == null || param.length() == 0) {
      throw new IllegalArgumentException("Missing top n value "
          + "(number of series to return)");
    }

    int topn = 0;
    if (param.matches("^[0-9]+$")) {
      try {
        topn = Integer.parseInt(param);
      } catch (NumberFormatException nfe) {
        throw new IllegalArgumentException(
            "Invalid parameter, must be an integer", nfe);
      }
    } else {
      throw new IllegalArgumentException("Unparseable top n value: " + param);
    }
    if (topn < 1) {
      throw new IllegalArgumentException("Top n value must be greater "
          + "than zero: " + topn);
    }

    int num_results = 0;
    for (DataPoints[] results: query_results) {
      num_results += results.length;
    }

    final PostAggregatedDataPoints[] post_agg_results = 
        new PostAggregatedDataPoints[num_results];
    int ix = 0;
    // one or more sub queries (m=...&m=...&m=...)
    for (final DataPoints[] sub_query_result : query_results) {
      // group bys (m=sum:foo{host=*})
      for (final DataPoints dps : sub_query_result) {
        // TODO(cl) - Avoid iterating and copying if we can help it. We should
        // be able to pass the original DataPoints object to the seekable view
        // and then iterate through it.
        final List<DataPoint> mutable_points = new ArrayList<DataPoint>();
        for (final DataPoint point : dps) {
          mutable_points.add(point.isInteger() ?
              MutableDataPoint.ofLongValue(point.timestamp(), point.longValue())
            : MutableDataPoint.ofDoubleValue(point.timestamp(), point.doubleValue()));
        }
        post_agg_results[ix++] = new PostAggregatedDataPoints(dps,
                mutable_points.toArray(new DataPoint[mutable_points.size()]));
      }
    }
    
    final SeekableView[] views = new SeekableView[num_results];
    for (int i = 0; i < num_results; i++) {
      views[i] = post_agg_results[i].iterator();
    }

    final MaxCacheAggregator aggregator = new MaxCacheAggregator(
            Aggregators.Interpolation.LERP, "maxCache", num_results, 
            data_query.startTime(), data_query.endTime());

    final SeekableView view = (new AggregationIterator(views,
            data_query.startTime(), data_query.endTime(),
            aggregator, Aggregators.Interpolation.LERP, false));

    // slurp all the points even though we aren't using them at this stage
    while (view.hasNext()) {
      final DataPoint mdp = view.next();
      @SuppressWarnings("unused")
      final Object o = mdp.isInteger() ? mdp.longValue() : mdp.doubleValue();
    }

    final long[] max_longs = aggregator.getLongMaxes();
    final double[] max_doubles = aggregator.getDoubleMaxes();
    final TopNSortingEntry[] max_by_ts = new TopNSortingEntry[num_results];
    if (aggregator.hasDoubles() && aggregator.hasLongs()) {
      for (int i = 0; i < num_results; i++) {
        max_by_ts[i] = new TopNSortingEntry(
            Math.max((double)max_longs[i], max_doubles[i]), i);
      }
    } else if (aggregator.hasLongs() && !aggregator.hasDoubles()) {
      for (int i = 0; i < num_results; i++) {
        max_by_ts[i] = new TopNSortingEntry((double) max_longs[i], i);
      }
    } else if (aggregator.hasDoubles() && !aggregator.hasLongs()) {
      for (int i = 0; i < num_results; i++) {
        max_by_ts[i] = new TopNSortingEntry(max_doubles[i], i);
      }
    }
    
    Arrays.sort(max_by_ts);
    
    final int result_count = Math.min(topn, num_results);
    final DataPoints[] results = new DataPoints[result_count];
    for (int i = 0; i < result_count; i++) {
      results[i] = post_agg_results[max_by_ts[i].pos];
    }

    return results;
  }

  /**
   * Helper class for sorting the series. It will sort from highest to lowest.
   */
  static class TopNSortingEntry implements Comparable<TopNSortingEntry> {
    final double val;
    final int pos;
    
    public TopNSortingEntry(final double val, final int pos) {
      this.val = val;
      this.pos = pos;
    }
    
    @Override
    public String toString() {
      return "{" + val + "," + pos + "}";
    }
    
    @Override
    public int compareTo(final TopNSortingEntry o) {
      return -1 * Double.compare(val, o.val);
    }
  }

  @Override
  public String writeStringField(final List<String> query_params, 
      final String inner_expression) {
    return "highestMax(" + inner_expression + ")";
  }
  
  /**
   * Aggregator that stores the overall maximum value for the entire series
   */
  public static class MaxCacheAggregator extends Aggregator {
    /** The total number of series in the result set, including sub queries and
     * group bys */
    private final int total_series;
    /** An array of maximum integers by time series */
    private final long[] max_longs;
    /** An array of maximum doubles by time series */
    private final double[] max_doubles;
    /** Whether or not any of the series contain integers */
    private boolean has_longs = false;
    /** Whether or not any of the series contain doubles */
    private boolean has_doubles = false;
    /** Query start time in milliseconds for filtering */
    private long start;
    /** Query end time in milliseconds for filtering */
    private long end;

    /**
     * An aggregator that keeps track of the maximum values for each time series
     * in the result set.
     * @param method The interpolation method (not used)
     * @param name The name of the aggregator 
     * @param total_series The total number of series in the result set, 
     * including sub queries and group bys
     * @param start Query start time in milliseconds for filtering
     * @param end Query end time in milliseconds for filtering
     */
    public MaxCacheAggregator(final Interpolation method, final String name, 
        final int total_series, final long start, final long end) {
      super(method, name);
      this.total_series = total_series;
      this.start = start;
      this.end = end;
      this.max_longs = new long[total_series];
      this.max_doubles = new double[total_series];

      for (int i = 0; i < total_series; i++) {
        max_doubles[i] = Double.MIN_VALUE;
        max_longs[i] = Long.MIN_VALUE;
      }
    }

    @Override
    public long runLong(final Longs values) {
      // TODO(cl) - Can we get anything other than a DataPoint?
      if (values instanceof DataPoint) {
        long ts = ((DataPoint) values).timestamp();
        //data point falls outside required range
        if (ts < start || ts > end) {
          return 0;
        }
      }

      final long[] longs = new long[total_series];
      int ix = 0;
      longs[ix++] = values.nextLongValue();
      while (values.hasNextValue()) {
        longs[ix++] = values.nextLongValue();
      }

      for (int i = 0; i < total_series;i++) {
        max_longs[i] = Math.max(max_longs[i], longs[i]);
      }

      has_longs = true;
      return 0;
    }

    @Override
    public double runDouble(Doubles values) {
      // TODO(cl) - Can we get anything other than a DataPoint?
      if (values instanceof DataPoint) {
        long ts = ((DataPoint) values).timestamp();
        //data point falls outside required range
        if (ts < start || ts > end) {
          return 0;
        }
      }

      final double[] doubles = new double[total_series];
      int ix = 0;
      doubles[ix++] = values.nextDoubleValue();
      while (values.hasNextValue()) {
        doubles[ix++] = values.nextDoubleValue();
      }
      for (int i = 0; i < total_series;i++) {
        // TODO(cl) - Properly handle NaNs here
        max_doubles[i] = Math.max(max_doubles[i], doubles[i]);
      }

      has_doubles = true;
      return 0;
    }

    public long[] getLongMaxes() {
      return max_longs;
    }

    public double[] getDoubleMaxes() {
      return max_doubles;
    }

    public boolean hasLongs() {
      return has_longs;
    }

    public boolean hasDoubles() {
      return has_doubles;
    }
    
  }
}
