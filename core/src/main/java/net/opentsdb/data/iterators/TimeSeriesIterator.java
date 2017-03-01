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
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.data.iterators;

import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.query.processor.TimeSeriesProcessor;

/**
 * Represents an interface used for iterating over a set of data for a single
 * time series for a single data type. An iterator may be used independently
 * for working with "raw" data or it may be tied to a processor.
 * <p>
 * <b>Stand-alone</b>
 * <p>
 * When used as a stand-alone iterator, the workflow is as follows:
 * <ol>
 * <li>Call {@link #initialize()} to load the first "chunk" of data and determine
 * the ID of the series.</li>
 * <li>Call {@link #status()} to determine if the iterator has data ready to 
 * process ({@link IteratorStatus#HAS_DATA}), requires one or more (possibly)
 * remote calls for more data ({@link IteratorStatus#END_OF_CHUNK}), has no
 * data ({@link IteratorStatus#END_OF_DATA}) or has an exception.</li>
 * <li> If the iterator has data, call {@link #next()} to retrieve the next 
 * value. Repeat steps #2 and #3 until a different status is returned. </li>
 * <li> If the status is {@link IteratorStatus#END_OF_CHUNK}, call 
 * {@link #fetchNext()} then return to step #2.</li>
 * </ol>
 * <p>
 * Note that if {@link #next()} is called when the status is anything other than
 * {@link IteratorStatus#HAS_DATA} an {@link IllegalStateException} may be thrown.
 * <p>
 * <b>Processor</b>
 * <p>
 * When used with a processor the workflow is as follows:
 * <ol>
 * <li>Call {@link #initialize()} to load the first "chunk" of data and determine
 * the ID of the series.</li>
 * <li>Pass the iterator to 
 * {@link TimeSeriesProcessor#addSeries(net.opentsdb.data.TimeSeriesGroupId, TimeSeriesIterator)}
 * </li>
 * <li>Call {@link TimeSeriesProcessor#status()} to determine if the iterator 
 * group has data ready to process ({@link IteratorStatus#HAS_DATA}), 
 * requires one or more (possibly) remote calls for more data 
 * ({@link IteratorStatus#END_OF_CHUNK}), has no data 
 * ({@link IteratorStatus#END_OF_DATA}) or has an exception.</li>
 * <li> If the iterator has data, call {@link TimeSeriesProcessor#next()} to 
 * advance all iterators to the next value.</li>
 * <li>Then call {@link #next()} on <i>this</i> iterator to retrieve the 
 * value for this series. Repeat steps #3 to #5 until a different status is 
 * returned.
 * <li> If the processor status is {@link IteratorStatus#END_OF_CHUNK}, call 
 * {@link TimeSeriesProcessor#fetchNext()} then return to step #3.</li>
 * </ol>
 * <p>
 * <b>Warning:</b> Do <i>not</i> call {@link #advance()} or {@link #fetchNext()}
 * directly when the series is tied to a processor. The processor will handle
 * iteration as it may require synchronization with additional time series.
 * <p>
 * <b>Warning:</b> When tied to a processor, you must always call 
 * {@link TimeSeriesProcessor#next()} to advance the value for this series. Calling
 * {@link #next()} will return the same value over and over.
 * <p>
 * <b>Notes</b>
 * <p>
 * <b>Thread-Safety:</b> This iterator is <i>not</i> thread safe. Do not call 
 * methods on iterators across multiple threads. If multiple threads require a
 * view into the same data, use {@link #getCopy()} to retrieve a separate view
 * of the same data that may be iterated separately on a different thread.
 * <p>
 * <b>Mutability:</b> Data returned from the iterator is immutable (so that we
 * can provide a copy of the iterator). 
 * 
 * @param <T> A {@link net.opentsdb.data.TimeSeriesDataType} data type that this 
 * iterator handles.
 * 
 * @since 3.0
 */
public interface TimeSeriesIterator<T extends TimeSeriesValue<?>> {
  
  /**
   * Initializes the iterator if initialization is required. E.g. it may load
   * the first chunk of data or setup the underlying data store. This should be
   * called before any other operations on the iterator (aside from {@link #type()}.
   * <b>Requirement:</b> This method may be called more than once before {@link #next()} 
   * thus subsequent calls must be no-ops.
   * @return A Deferred resolving to a null on success or an exception if 
   * initialization failed.
   * @throws IllegalStateException if this method was called after {@link #next()} or
   * {@link #advance()}.
   */
  public Deferred<Object> initialize();
  
  /**
   * The {@link TimeSeriesDataType} data type of this data returned by this iterator.
   * @return A non-null type token.
   */
  public TypeToken<?> type();
  
  /**
   * A reference to the time series ID associated with all data points returned
   * by this iterator.
   * @return A non-null {@link TimeSeriesId}.
   */
  public TimeSeriesId id();
  
  /**
   * Configures the iterator to be handled by a time series processor. 
   * @param processor A non-null time series processor.
   */
  public void setProcessor(final TimeSeriesProcessor processor);
  
  /**
   * Returns the current status of the iterator, allowing callers to determine
   * if the iterator has data, needs to fetch more data, is finished or has
   * an exception.
   * @return A non-null iterator status.
   */
  public IteratorStatus status();
  
  /**
   * Returns the next value for this iterator. Behavior depends on whether the
   * iterator is in stand-alone mode or a part of a processor.
   * <b>NOTE:</b> The value returned may be mutated after the next call to 
   * {@link #next()} so make sure to call {@link TimeSeriesValue#getCopy()} if
   * the value must remain in memory unmutated.
   * @return A non-null value.
   * @throws IllegalStateException if the iterator is not ready to return another
   * value.
   */
  public T next();
  
  /**
   * Advances to the next value when the iterator is associated with a processor.
   * If the iterator does not have any more data, another chunk must be fetched, 
   * or the iterator is in stand-alone mode, then this method is a no-op.
   */
  public void advance();
  
  /**
   * Returns the next timestamp available for this iterator, i.e. what will be
   * seen when {@link #next()} is called. If the iterator is at the end of data
   * or another chunk must be fetched, the result may be null.
   * @return A timestamp representing the next point in time or null if no more
   * data is available.
   */
  public TimeStamp nextTimestamp();
  
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
   * Creates and returns a deep copy of this iterator for another view on the 
   * time series.
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
   * @return A non-null copy of the iterator.
   */
  public TimeSeriesIterator<T> getCopy();
  
  /**
   * Returns the parent iterator if the current iterator is a copy. Allows
   * for walking up the copy tree to close the parent.
   * @return The parent iterator. May be null if this was not a copy.
   */
  public TimeSeriesIterator<T> getCopyParent();
  
  /**
   * Closes and releases any resources held by this iterator. If this iterator
   * is a copy, the method is a no-op.
   * @return A deferred resolving to a null on success, an exception on failure.
   */
  public Deferred<Object> close();
}
