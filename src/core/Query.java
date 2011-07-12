// This file is part of OpenTSDB.
// Copyright (C) 2010  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.core;

import java.util.Map;

import org.hbase.async.HBaseException;

import net.opentsdb.uid.NoSuchUniqueName;

/**
 * A query to retreive data from the TSDB.
 */
public interface Query {

  /**
   * Sets the start time of the graph.
   * @param timestamp The start time, all the data points returned will have a
   * timestamp greater than or equal to this one.
   * @throws IllegalArgumentException if timestamp is less than or equal to 0,
   * or if it can't fit on 32 bits.
   * @throws IllegalArgumentException if
   * {@code timestamp >= }{@link #getEndTime getEndTime}.
   */
  void setStartTime(long timestamp);

  /**
   * Returns the start time of the graph.
   * @return A strictly positive integer.
   * @throws IllegalStateException if {@link #setStartTime} was never called on
   * this instance before.
   */
  long getStartTime();

  /**
   * Sets the end time of the graph.
   * @param timestamp The end time, all the data points returned will have a
   * timestamp less than or equal to this one.
   * @throws IllegalArgumentException if timestamp is less than or equal to 0,
   * or if it can't fit on 32 bits.
   * @throws IllegalArgumentException if
   * {@code timestamp <= }{@link #getStartTime getStartTime}.
   */
  void setEndTime(long timestamp);

  /**
   * Returns the end time of the graph.
   * <p>
   * If {@link #setEndTime} was never called before, this method will
   * automatically execute
   * {@link #setEndTime setEndTime}{@code (System.currentTimeMillis() / 1000)}
   * to set the end time.
   * @return A strictly positive integer.
   */
  long getEndTime();

  /**
   * Sets the time series to the query.
   * @param metric The metric to retreive from the TSDB.
   * @param tags The set of tags of interest.
   * @param function The aggregation function to use.
   * @param rate If true, the rate of the series will be used instead of the
   * actual values.
   * @throws NoSuchUniqueName if the name of a metric, or a tag name/value
   * does not exist.
   */
  void setTimeSeries(String metric, Map<String, String> tags,
                     Aggregator function, boolean rate) throws NoSuchUniqueName;

  /**
   * Downsamples the results by specifying a fixed interval between points.
   * <p>
   * Technically, downsampling means reducing the sampling interval.  Here
   * the idea is similar.  Instead of returning every single data point that
   * matched the query, we want one data point per fixed time interval.  The
   * way we get this one data point is by aggregating all the data points of
   * that interval together using an {@link Aggregator}.  This enables you
   * to compute things like the 5-minute average or 10 minute 99th percentile.
   * @param interval Number of seconds wanted between each data point.
   * @param downsampler Aggregation function to use to group data points
   * within an interval.
   */
  void downsample(int interval, Aggregator downsampler);

  /**
   * Runs this query.
   * @return The data points matched by this query.
   * <p>
   * Each element in the non-{@code null} but possibly empty array returned
   * corresponds to one time series for which some data points have been
   * matched by the query.
   * @throws HBaseException if there was a problem communicating with HBase to
   * perform the search.
   */
  DataPoints[] run() throws HBaseException;

}
