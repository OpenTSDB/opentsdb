// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.package net.opentsdb.data;
package net.opentsdb.query.processor;

import java.util.List;
import java.util.Map;

import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.data.TimeSeriesGroupId;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.iterators.IteratorStatus;
import net.opentsdb.data.iterators.TimeSeriesIterator;

/**
 * A time series data processor that iterates over one or more time series
 * synchronized on timestamps. The processor can either pass through the 
 * source series or apply mutations on top of the data and return the 
 * modifications without modifying the source.
 * <p>
 * Processors work across all data types. If a type is not supported it may be
 * passed through the the upstream processor.
 * <p>
 * Processors may also perform multiple passes across the source data (e.g. for
 * standard deviation calculations). In such a case, the underlying iterators 
 * should support buffering of the data to improve query speed.
 * <b>Workflow</b>
 * <ol>
 * <li>Instantiate the processor using the registry and processor factory.</li>
 * <li>Instantiate and initialize one or more {@link TimeSeriesIterator}s.</li>
 * <li>Pass the iterators to the processor via {@link #addSeries(TimeSeriesGroupId, TimeSeriesIterator)}.</li>
 * <li>Chain processors together and place the final processor in 
 * net.opentsdb.query.sink.TimeSeriesSink.</li>
 * </ol>
 * <p>
 * <b>Requirements:</b>
 * <ul>
 * <li>The source data cannot be modified. If the processor will be applying 
 * functions on the data, it must return a copy of the data.</li>
 * <li>All series under the processor must be iterated so that the values for
 * each series return the same timestamp regardless of group or data type</li>
 * </ul>
 * TODO - methods for reading/modifying the processor graph.
 * @since 3.0
 */
public interface TimeSeriesProcessor {
  
  /**
   * Initializes the processor if initialization is required. If a processor 
   * requires multiple passes over the data set, it may copy the source and run
   * through it one or more times. Otherwise this method should setup the output
   * iterators.
   * <b>Requirement:</b> This method may be called more than once before {@link #next()} 
   * thus subsequent calls must be no-ops.
   * @return A Deferred resolving to a null on success or an exception if 
   * initialization failed.
   * @throws IllegalStateException if this method was called after {@link #next()}.
   */
  public Deferred<Object> initialize();
  
  /**
   * The set of iterators for use upstream by another processor or a sink.
   * @return A non-null map of zero or more groups, types and iterators.
   */
  public Map<TimeSeriesGroupId, Map<TypeToken<?>, 
      List<TimeSeriesIterator<?>>>> iterators();
  
  /**
   * Adds the given series to the iterator set with the proper group.
   * @param group A non-null time series group the series should be associated 
   * with.
   * @param series A non-null time series iterator that has already been 
   * initialized.
   */
  public void addSeries(final TimeSeriesGroupId group, 
      final TimeSeriesIterator<?> series);

  /**
   * The <i>next</i> timestamp that child iterators and/or processors should use
   * when advancing to the next value. 
   * <b>Warning:</b> Do not use this timestamp for serialization. Use the 
   * timestamps returned with individual values instead. 
   * <b>Warning:</b> Do NOT modify the timestamp object returned by this method.
   * @return A non-null timestamp for child processors and/or iterators to 
   * synchronize to.
   */
  public TimeStamp syncTimestamp();
  
  /**
   * Returns the current status of the processor, allowing callers to determine
   * if the iterator set has data, needs to fetch more data, is finished or has
   * an exception.
   * @return A non-null iterator status.
   */
  public IteratorStatus status();
  
  /**
   * Advances underlying sources to the next timestamp, updating 
   * {@link #setSyncTime(TimeStamp)} as appropriate with their next time.
   */
  public void next();
  
  /**
   * Used by source iterators to update the processor with their status so that
   * the entire iterator can determine whether or not there is any more data,
   * an exception or if more data should be fetched.
   * <b>Warning:</b> Should only be used by child iterators. 
   * @param status A non-null iterator status.
   */
  public void markStatus(final IteratorStatus status);
  
  /**
   * Updates the synchronization timestamp from a child iterator. The iterator
   * will choose the earliest time amongst all child iterators.
   * @param timestamp A non-null timestamp.
   */
  public void setSyncTime(final TimeStamp timestamp);
  
  /**
   * If {@link IteratorStatus#END_OF_CHUNK} was returned via the last 
   * {@link #status()} call, this method will fetch the next set of data 
   * asynchronously. The following status may have data, be end of stream
   * or have an exception. 
   * @return A Deferred resolving to a null if the next chunk was fetched 
   * successfully or an exception if fetching data failed.
   */
  public Deferred<Object> fetchNext();
  
  /**
   * Creates and returns a deep copy of this processor and all sources/child 
   * iterators for another view on the time series.
   * <p>Requirements:
   * <ul>
   * <li>The copy must return a new view of the underlying data. If this method
   * was called in the middle of iteration, the copy must start at the top of
   * the beginning of the data and the original iterator left in it's current 
   * state.</li>
   * <li>If the source iterator has not been initialized, the copy will not
   * be initialized either. Likewise if the source <i>has</i> been initialized
   * then the copy will have been as well.</li>
   * </ul>
   * @return A non-null copy of the processor and underlying iterators.
   */
  public TimeSeriesProcessor getCopy();
  
  /**
   * Returns the parent processor if this processor is a copy. Allows for walking
   * up the copy tree to close the parent.
   * @return The parent copy. May be null if this was not a copy.
   */
  public TimeSeriesProcessor getCopyParent();
  
  /**
   * Closes and releases any resources held by this processor. If this processor
   * is a copy, the method is a no-op.
   * @return A deferred resolving to a null on success, an exception on failure.
   */
  public Deferred<Object> close();
}
