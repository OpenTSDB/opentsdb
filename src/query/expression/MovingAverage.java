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
package net.opentsdb.query.expression;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import net.opentsdb.core.AggregationIterator;
import net.opentsdb.core.Aggregator;
import net.opentsdb.core.Aggregators;
import net.opentsdb.core.DataPoint;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.IllegalDataException;
import net.opentsdb.core.MutableDataPoint;
import net.opentsdb.core.SeekableView;
import net.opentsdb.core.TSQuery;
import net.opentsdb.core.Aggregators.Interpolation;

/**
 * Implements a moving average function windowed on either the number of 
 * data points or a unit of time.
 * @since 2.3
 */
public class MovingAverage implements Expression {
  
  @Override
  public DataPoints[] evaluate(final TSQuery data_query, 
      final List<DataPoints[]> query_results, final List<String> params) {
    if (data_query == null) {
      throw new IllegalArgumentException("Missing time series query");
    }
    if (query_results == null || query_results.isEmpty()) {
      return new DataPoints[]{};
    }
    if (params == null || params.isEmpty()) {
      throw new IllegalArgumentException("Missing moving average window size");
    }

    String param = params.get(0);
    if (param == null || param.isEmpty()) {
      throw new IllegalArgumentException("Missing moving average window size");
    }
    param = param.trim();
    
    long condition = -1;
    boolean is_time_unit = false;
    if (param.matches("^[0-9]+$")) {
      try {
        condition = Integer.parseInt(param);
      } catch (NumberFormatException nfe) {
        throw new IllegalArgumentException(
            "Invalid parameter, must be an integer", nfe);
      }
    } else if (param.startsWith("'") && param.endsWith("'")) {
      condition = parseParam(param);
      is_time_unit = true;
    } else {
      throw new IllegalArgumentException("Unparseable window size: " + param);
    }
    if (condition <= 0) {
      throw new IllegalArgumentException("Moving average window must be an "
          + "integer greater than zero");
    }

    int num_results = 0;
    for (final DataPoints[] results : query_results) {
      num_results += results.length;
    }
    
    final PostAggregatedDataPoints[] post_agg_results = 
        new PostAggregatedDataPoints[num_results];
    int ix = 0;
    // one or more queries (m=...&m=...&m=...)
    for (final DataPoints[] sub_query_result : query_results) {
      // group bys (m=sum:foo{host=*})
      for (final DataPoints dps: sub_query_result) {
        // TODO(cl) - Avoid iterating and copying if we can help it. We should
        // be able to pass the original DataPoints object to the seekable view
        // and then iterate through it.
        final List<DataPoint> mutable_points = new ArrayList<DataPoint>();
        for (final DataPoint point: dps) {
          // avoid flip-flopping between integers and floats, always use double
          // for average.
          mutable_points.add(
              MutableDataPoint.ofDoubleValue(point.timestamp(), point.toDouble()));
        }
        
        post_agg_results[ix++] = new PostAggregatedDataPoints(dps,
                mutable_points.toArray(new DataPoint[mutable_points.size()]));
      }
    }
    
    final DataPoints[] results = new DataPoints[num_results];
    for (int i = 0; i < num_results; i++) {
      final Aggregator moving_average = new MovingAverageAggregator(
          Aggregators.Interpolation.LERP, "movingAverage", 
          condition, is_time_unit);
      final SeekableView[] metrics_groups = new SeekableView[] { 
          post_agg_results[i].iterator() };
      final SeekableView view = new AggregationIterator(metrics_groups,
              data_query.startTime(), data_query.endTime(),
              moving_average, 
              Aggregators.Interpolation.LERP, false);
      final List<DataPoint> points = new ArrayList<DataPoint>();
      while (view.hasNext()) {
        final DataPoint mdp = view.next();
        points.add(MutableDataPoint.ofDoubleValue(mdp.timestamp(), mdp.toDouble()));
      }
      results[i] = new PostAggregatedDataPoints(post_agg_results[i],
        points.toArray(new DataPoint[points.size()]));
    }
    return results;
  }
  
  /**
   * Parses the parameter string to fetch the window size
   * <p>
   * Package private for UTs
   * @param param The string to parse
   * @return The window size (number of points or a unit of time in ms)
   */
  long parseParam(final String param) {
    if (param == null || param.isEmpty()) {
      throw new IllegalArgumentException(
          "Window parameter may not be null or empty");
    }
    final char[] chars = param.toCharArray();
    int idx = 0;
    for (int c = 1; c < chars.length; c++) {
      if (Character.isDigit(chars[c])) {
        idx++;
      } else {
        break;
      }
    }
    if (idx < 1) {
      throw new IllegalArgumentException("Invalid moving window parameter: " 
          + param);
    }

    try {
      final int time = Integer.parseInt(param.substring(1, idx + 1));
      final String unit = param.substring(idx + 1, param.length() - 1);
  
      // TODO(CL) - add a Graphite unit parser to DateTime for this kind of conversion
      if ("day".equals(unit) || "d".equals(unit)) {
        return TimeUnit.MILLISECONDS.convert(time, TimeUnit.DAYS);
      } else if ("hr".equals(unit) || "hour".equals(unit) || "h".equals(unit)) {
        return TimeUnit.MILLISECONDS.convert(time, TimeUnit.HOURS);
      } else if ("min".equals(unit) || "m".equals(unit)) {
        return TimeUnit.MILLISECONDS.convert(time, TimeUnit.MINUTES); 
      } else if ("sec".equals(unit) || "s".equals(unit)) {
        return TimeUnit.MILLISECONDS.convert(time, TimeUnit.SECONDS);
      } else {
        throw new IllegalArgumentException("Unknown time unit=" + unit 
            + " in window=" + param);
      }
    } catch (NumberFormatException nfe) {
      throw new IllegalArgumentException("Unable to parse moving window "
          + "parameter: " + param, nfe);
    }
  }

  @Override
  public String writeStringField(final List<String> query_params, 
      final String inner_expression) {
    return "movingAverage(" + inner_expression + ")";
  }

  /**
   * An aggregator that expects a single data point for each iteration. The
   * values are prepended to a linked list. Next it iterates over the list until
   * it either runs out of values (and returns a 0 with the proper timestamp) or
   * returns the average of all values in the given window (time or number based).
   * <p>
   * Package private for unit testing
   */
  static final class MovingAverageAggregator extends Aggregator {
    /** The individual values in the window */
    private final LinkedList<DataPoint> accumulation;
    /** The condition to satisfy, either a time unit or # of data points */
    private final long condition;
    /** Whether or not the condition is a time unit or the # of data points */
    private final boolean is_time_unit;
    /** Sentinel used to kick out the first timed window value */
    private boolean window_started;
    
    /**
     * Ctor for this implementation
     * @param method The interpolation method to use (ignored)
     * @param name The name of this aggregator
     * @param condition The windowing condition
     * @param is_time_unit Whether or not the condition is a time unit or 
     * the # of data points
     */
    public MovingAverageAggregator(final Interpolation method, final String name, 
        final long condition, final boolean is_time_unit) {
      super(method, name);
      this.condition = condition;
      this.is_time_unit = is_time_unit;
      accumulation = new LinkedList<DataPoint>();
    }

    @Override
    public long runLong(final Longs values) {
      final long value = values.nextLongValue();
      if (values.hasNextValue()) {
        throw new IllegalDataException(
            "There should only be one value in " + values);
      }
      final long ts = ((DataPoint) values).timestamp();
      accumulation.addFirst(MutableDataPoint.ofLongValue(ts, value));

      // for timed windows we need to skip the first data point in the series
      // as we have no idea what the previous value's timestamp was.
      if (is_time_unit && !window_started) {
        window_started = true;
        return 0;
      }
      
      long sum = 0; 
      int count = 0;
      final Iterator<DataPoint> iter = accumulation.iterator();
      boolean condition_met = false;
      long time_window_cumulation = 0; // how many ms are in our window
      long last_ts = -1; // the timestamp of the previous dp

      // now sum up the preceding points
      while(iter.hasNext()) {
        final DataPoint dp = iter.next();
        if (is_time_unit) {
          if (last_ts < 0) {
            last_ts = dp.timestamp();
          } else {
            time_window_cumulation += last_ts - dp.timestamp();
            last_ts = dp.timestamp();
            if (time_window_cumulation >= condition) {
              condition_met = true;
              break;
            }
          }
        }
        // cast to long if we dumped a double in there
        sum += dp.isInteger() ? dp.longValue() : dp.doubleValue();
        count++;
        if (!is_time_unit && count >= condition) {
          condition_met = true;
          break;
        }
      }
      while (iter.hasNext()) {
        // should drop the last entry in the linked list to avoid accumulating
        // everything in memory
        iter.next();
        iter.remove();
      }

      if (!condition_met || count == 0) {
        return 0;
      }
      return sum / count;
    }

    @Override
    public double runDouble(Doubles values) {
      final double value = values.nextDoubleValue();
      if (values.hasNextValue()) {
        throw new IllegalDataException(
            "There should only be one value in " + values);
      }
      final long ts = ((DataPoint) values).timestamp();
      accumulation.addFirst(MutableDataPoint.ofDoubleValue(ts, value));
      
      // for timed windows we need to skip the first data point in the series
      // as we have no idea what the previous value's timestamp was.
      if (is_time_unit && !window_started) {
        window_started = true;
        return 0;
      }
      
      double sum = 0;
      int count = 0;
      final Iterator<DataPoint> iter = accumulation.iterator();
      boolean condition_met = false;
      long time_window_cumulation = 0; // how many ms are in our window
      long last_ts = -1; // the timestamp of the previous dp
      
      // now sum up the preceding points
      while(iter.hasNext()) {
        final DataPoint dp = iter.next();
        
        if (is_time_unit) {
          if (last_ts < 0) {
            last_ts = dp.timestamp();
          } else {
            time_window_cumulation += last_ts - dp.timestamp();
            last_ts = dp.timestamp();
            if (time_window_cumulation >= condition) {
              condition_met = true;
              break;
            }
          }
        }
        
        // cast to double if we dumped a long in there
        final double v = dp.isInteger() ? dp.longValue() : dp.doubleValue();
        if (!Double.isNaN(v)) {
          // skip NaNs to avoid NaNing everything in the window.
          sum += v;
          count++;
        }
        
        if (!is_time_unit && count >= condition) {
          condition_met = true;
          break;
        }
      }
      
      while (iter.hasNext()) {
        // should drop the last entry in the linked list to avoid accumulating
        // everything in memory
        iter.next();
        iter.remove();
      }

      if (!condition_met || count == 0) {
        return 0;
      }
      return sum/count;
    }
  }
}
