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

import com.stumbleupon.async.Deferred;

import org.hbase.async.HBaseException;

/**
 * Represents a mutable sequence of continuous data points.
 * <p>
 * Implementations of this interface aren't expected to be synchronized.
 */
public interface WritableDataPoints extends DataPoints {

  /**
   * Sets the metric name and tags of the series.
   * <p>
   * This method can be called multiple times on the same instance to start
   * adding data points to another time series without having to create a new
   * instance.
   * @param metric A non-empty string.
   * @param tags The tags on this series.  This map must be non-empty.
   * @throws IllegalArgumentException if the metric name is empty or contains
   * illegal characters.
   * @throws IllegalArgumentException if the tags list is empty or one of the
   * elements contains illegal characters.
   */
  void setSeries(String metric, Map<String, String> tags);

  /**
   * Adds a {@code long} data point to the TSDB.
   * <p>
   * The data point is immediately persisted unless {@link #setBufferingTime}
   * is used.  Data points must be added in chronological order.
   * @param timestamp The timestamp associated with the value.
   * @param value The value of the data point.
   * @return A deferred object that indicates the completion of the request.
   * The {@link Object} has not special meaning and can be {@code null} (think
   * of it as {@code Deferred<Void>}). But you probably want to attach at
   * least an errback to this {@code Deferred} to handle failures.
   * @throws IllegalArgumentException if the timestamp is less than or equal
   * to the previous timestamp added or 0 for the first timestamp, or if the
   * difference with the previous timestamp is too large.
   * @throws HBaseException (deferred) if there was a problem while persisting
   * data.
   */
  Deferred<Object> addPoint(long timestamp, long value);

  /**
   * Appends a {@code float} data point to this sequence.
   * <p>
   * The data point is immediately persisted unless {@link #setBufferingTime}
   * is used.  Data points must be added in chronological order.
   * @param timestamp The timestamp associated with the value.
   * @param value The value of the data point.
   * @return A deferred object that indicates the completion of the request.
   * The {@link Object} has not special meaning and can be {@code null} (think
   * of it as {@code Deferred<Void>}). But you probably want to attach at
   * least an errback to this {@code Deferred} to handle failures.
   * @throws IllegalArgumentException if the timestamp is less than or equal
   * to the previous timestamp added or 0 for the first timestamp, or if the
   * difference with the previous timestamp is too large.
   * @throws IllegalArgumentException if the value is {@code NaN} or
   * {@code Infinite}.
   * @throws HBaseException (deferred) if there was a problem while persisting
   * data.
   */
  Deferred<Object> addPoint(long timestamp, float value);

  /**
   * Specifies for how long to buffer edits, in milliseconds.
   * <p>
   * By calling this method, you're allowing new data points to be buffered
   * before being sent to HBase.  {@code 0} (the default) means data points
   * are persisted immediately.
   * <p>
   * Buffering improves performance, reduces the number of RPCs sent to HBase,
   * but can cause data loss if we die before we get a chance to send buffered
   * edits to HBase.  It also entails that buffered data points aren't visible
   * to other applications using the TSDB until they're flushed to HBase.
   * @param time The approximate maximum number of milliseconds for which data
   * points should be buffered before being sent to HBase.  This deadline will
   * be honored on a "best effort" basis.
   */
  void setBufferingTime(short time);

  /**
   * Specifies whether or not this is a batch import.
   * <p>
   * It is preferred that this method be called for anything importing a batch
   * of data points (as opposed to streaming in new data points in real time).
   * <p>
   * Calling this method changes a few important things:
   * <ul>
   * <li>Data points may not be persisted immediately.  In the event of an
   * outage in HBase during or slightly after the import, un-persisted data
   * points will be lost.</li>
   * <li>{@link #setBufferingTime} may be called with an argument
   * chosen by the implementation.</li>
   * </ul>
   * @param batchornot if true, then this is a batch import.
   */
  void setBatchImport(boolean batchornot);

}
