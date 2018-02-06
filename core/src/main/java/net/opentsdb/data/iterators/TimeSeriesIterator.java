// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.data.iterators;

import java.util.NoSuchElementException;

import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.query.context.QueryContext;

/**
 * An abstract class used for iterating over a set of data for a single
 * time series of a single data type. In order to use an iterator properly, it
 * must be tied to a {@link QueryContext}.
 * <p>
 * TODO - doc usage
 * <p>
 * <b>Thread-Safety:</b> This iterator is <i>not</i> thread safe. Do not call 
 * methods on iterators across multiple threads. If multiple threads require a
 * view into the same data, use {@link #getShallowCopy(QueryContext)} to retrieve a 
 * separate view of the same data that may be iterated separately on a different 
 * thread.
 * <p>
 * <b>Mutability:</b> Data returned from the iterator is immutable (so that we
 * can provide a copy of the iterator). 
 * 
 * @param <T> A {@link net.opentsdb.data.TimeSeriesDataType} data type that this 
 * iterator handles.
 * 
 * @since 3.0
 */
public abstract class TimeSeriesIterator<T extends TimeSeriesDataType> {
  /** The ID of the time series represented by this iterator. */
  protected TimeSeriesId id;

  /** An order if iterator is part of a slice config. */
  protected int order;
  
  /** The context this iterator belongs to. May be null. */
  protected QueryContext context;
  
  /** The source of data for this iterator. Purposely wildcarded so that we 
   * could write a type converter. */
  protected TimeSeriesIterator<? extends TimeSeriesDataType> source;
  
  /** A link to the parent iterator if this is a source feeding into another. 
   * Purposely wildcarded so we can write type converters.*/
  protected TimeSeriesIterator<? extends TimeSeriesDataType> parent;
  
  /**
   * Protected ctor that sets the order to -1 and allows an implementing class
   * to set the ID later on.
   */
  protected TimeSeriesIterator() {
    order = -1;
  }
  
  /** Default ctor with everything initialized to null.
   * @param id A non-null time series ID.
   * @throws IllegalArgumentException if the ID was null.
   */
  public TimeSeriesIterator(final TimeSeriesId id) {
    if (id == null) {
      throw new IllegalArgumentException("ID cannot be null.");
    }
    this.id = id;
    order = -1;
  }
  
  /** 
   * Ctor for setting the context and registering with it.
   * @param id A non-null time series ID. 
   * @param context A context to register with.
   * @throws IllegalArgumentException if the ID was null.
   */
  public TimeSeriesIterator(final TimeSeriesId id, final QueryContext context) {
    if (id == null) {
      throw new IllegalArgumentException("ID cannot be null.");
    }
    this.id = id;
    setContext(context);
    order = -1;
  }
  
  /**
   * Ctor for iterators that consume from other iterators.
   * @param context A context to register with.
   * @param source A non-null source to consume from.
   * @throws IllegalArgumentException if the source was null.
   */
  public TimeSeriesIterator(final QueryContext context, 
      final TimeSeriesIterator<? extends TimeSeriesDataType> source) {
    if (source == null) {
      throw new IllegalArgumentException("Source cannot be null.");
    }
    if (!source.type().equals(type())) {
      throw new IllegalArgumentException("Source type [" + source.type() 
        + "] is not the same as our type [" + type() + "]");
    }
    this.id = source.id();
    setContext(context);
    order = source.order();
    this.source = source;
    source.setParent(this);
  }
  
  /**
   * Initializes the iterator if initialization is required. E.g. it may load
   * the first chunk of data or setup the underlying data store. This should be
   * called before any other operations on the iterator (aside from {@link #type()}.
   * <b>Requirement:</b> This method may be called more than once before {@link #next()} 
   * thus subsequent calls must be no-ops.
   * @return A Deferred resolving to a null on success or an exception if 
   * initialization failed.
   */
  public Deferred<Object> initialize() {
    return source != null ? source.initialize() : 
      Deferred.fromError(new UnsupportedOperationException("Not implemented"));
  }
  
  /** @return An optional order within a slice config. -1 by default. */
  public int order() {
    return order;
  }
  
  /**
   * The {@link TimeSeriesDataType} data type of this data returned by this iterator.
   * @return A non-null type token.
   */
  public abstract TypeToken<? extends TimeSeriesDataType> type();
  
  /** @return A non-null base time stamp shared by all values in this set. 
   * Usually set to the query start time or some offset of it. */
  public abstract TimeStamp startTime();
  
  /** @return A non-null final inclusive timestamp of the shard for which there 
   * may be data. Usually set to to the query end time. */ 
  public abstract TimeStamp endTime();
  
  /**
   * A reference to the time series ID associated with all data points returned
   * by this iterator.
   * @return A non-null {@link TimeSeriesId}.
   */
  public TimeSeriesId id() {
    return id;
  }
  
  /**
   * Sets the context this iterator belongs to. 
   * If the context was already set, this method unregisters from it and registers
   * with the new context if it isn't null.
   * @param context A context, may be null.
   */
  public void setContext(final QueryContext context) {
    if (this.context != null && this.context != context) {
      this.context.unregister(this);
    }
    this.context = context;
    if (context != null) {
      context.register(this);
    }
  }
  
  /**
   * Used when working with an iterator in stand-alone mode to determine if 
   * there is more data. If this iterator is part of a context, it should throw 
   * an IllegalStateException. 
   * @return A non-null iterator status reflecting the state of the iterator.
   * @throws IllegalStateException if the iterator is a part of a context.
   */
  public abstract IteratorStatus status();
  
  /**
   * Returns the next value for this iterator. Behavior depends on whether the
   * iterator is in stand-alone mode or a part of a processor.
   * <b>NOTE:</b> The value returned may be mutated after the next call to 
   * {@link #next()} so make sure to call {@link TimeSeriesValue#getCopy()} if
   * the value must remain in memory unmutated.
   * @return A non-null value.
   * @throws NoSuchElementException if the iterator is not ready to return another
   * value.
   */
  public abstract TimeSeriesValue<T> next();
  
  /**
   * Returns the current value (the result of calling {@link #next()}) without
   * advancing the iterator. If the iterator is empty, has reached the end of
   * it's data or needs to fetch another chunk, the value returned should be
   * {@code null}.
   * @return The next value to be read or null if no value is present.
   */
  public abstract TimeSeriesValue<T> peek();
  
  /**
   * If {@link IteratorStatus#END_OF_CHUNK} was returned via the last 
   * context advance call, this method will fetch the next set of data 
   * asynchronously. The following status may have data, be end of stream
   * or have an exception. 
   * @return A Deferred resolving to a null if the next chunk was fetched 
   * successfully or an exception if fetching data failed.
   */
  public Deferred<Object> fetchNext() {
    return source != null ? source.fetchNext() : 
      Deferred.fromError(new UnsupportedOperationException("Not implemented"));
  }
  
  /**
   * Creates and returns a shallow copy of this iterator for another view on the 
   * time series. This allows for multi-pass operations over data by associating
   * iterators with a separate context and iterating over them without effecting
   * the parent context.
   * <p>Requirements:
   * <ul>
   * <li>The copy must return a new view of the underlying data. If this method
   * was called in the middle of iteration, the copy must start at the top of
   * the beginning of the data and the original iterator left in it's current 
   * state.</li>
   * <li>If the source iterator has not been initialized, the copy will not
   * be initialized either. Likewise if the source <i>has</i> been initialized
   * then the copy will have been as well.</li>
   * <li>The {@code parent} of the returned copy must be {@code null}. The caller
   * should set the parent if desired.</li>
   * <li>The copy's {@code context} must be set to the next context.</li>
   * </ul>
   * @param context A context for the iterator to associate with.
   * @return A non-null copy of the iterator.
   */
  public abstract TimeSeriesIterator<T> getShallowCopy(final QueryContext context);

  /**
   * Creates and returns a deep copy of the iterator containing data only within
   * the given time range. This is used for serialization and slicing purposes. 
   * @param context An optional context if required.
   * @param start A non-null start timestamp.
   * @param end A non-null end timestamp.
   * @return A deep copy of the data within the time boundary. May be empty if
   * no data is within the range.
   * @throws IllegalArgumentException if either timestamp is null or the end
   * time was greater or equal to the start time.
   */
  public abstract TimeSeriesIterator<T> getDeepCopy(final QueryContext context, 
                                                final TimeStamp start, 
                                                final TimeStamp end);
  
  /**
   * Sets the parent for this iterator.
   * @param parent A parent iterator. May be null if uncoupling.
   */
  public void setParent(final TimeSeriesIterator<?> parent) {
    this.parent = parent;
  }
  
  /**
   * Closes and releases any resources held by this iterator. If this iterator
   * is a copy, the method is a no-op.
   * @return A deferred resolving to a null on success, an exception on failure.
   */
  public Deferred<Object> close() {
    return source != null ? source.close() : 
      Deferred.fromError(new UnsupportedOperationException("Not implemented"));
  }
  
  /**
   * A package private method that all iterators must implement that will update
   * the context (if not null) with the next sync time and status.
   */
  protected abstract void updateContext();
  
}
