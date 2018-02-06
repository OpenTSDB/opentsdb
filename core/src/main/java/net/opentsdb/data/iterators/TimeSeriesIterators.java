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

import java.util.List;

import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.query.context.QueryContext;

/**
 * A collection of time series with various data types all sharing the same
 * identity. Useful, for example, if a series has numeric values and strings
 * and histograms.
 * 
 * @since 3.0
 */
public abstract class TimeSeriesIterators 
    implements Iterable<TimeSeriesIterator<?>> {
  
  /** The ID shared by all time series in this group. */
  protected final TimeSeriesId id;
  
  /** An order if shard is part of a slice config. */
  protected int order;
  
  /**
   * Default ctor.
   * @param id A non-null ID for the series in this set.
   * @throws IllegalArgumentException if the ID was null.
   */
  public TimeSeriesIterators(final TimeSeriesId id) {
    if (id == null) {
      throw new IllegalArgumentException("ID cannot be null.");
    }
    this.id = id;
    order = -1;
  }
  
  /** @return The ID shared by all time series in this group. */
  public TimeSeriesId id() {
    return id;
  }
  
  /** @return An optional order within a slice config. -1 by default. */
  public int order() {
    return order;
  }
  
  /**
   * Initializes all of the iterators in this set.
   * @return A deferred resolving to a null on success or an exception on 
   * failure.
   */
  public abstract Deferred<Object> initialize();
  
  /**
   * Adds the iterator to the list. Does not check for duplicates and sets the
   * type of the set to that of the first iterator. If another iterator with a
   * different type is passed, an exception is thrown.
   * @param iterator A non-null iterator to store in the list.
   * @throws IllegalArgumentException if the iterator was null or it's type did
   * not match that of previous iterators in the list.
   */
  public abstract void addIterator(final TimeSeriesIterator<?> iterator);
  
  /** @return An unmodifiable list of iterators. May be empty. */
  public abstract List<TimeSeriesIterator<?>> iterators();
  
  /**
   * Returns an iterator on the data for the given type if present.
   * @param type A non-null type of iterator to fetch.
   * @return A time series iterator of the given type if present, null if not.
   */
  public abstract TimeSeriesIterator<?> iterator(final TypeToken<?> type);

  /**
   * Runs through the entire set of iterators and calls 
   * {@link TimeSeriesIterator#setContext(QueryContext)} on each.
   * @param context The context to apply to each iterator.
   */
  public abstract void setContext(final QueryContext context);
  
  /**
   * Closes all of the underlying iterators in the set.
   * @return A deferred to wait on for completion resolving to null on success
   * or an exception if closing failed.
   */
  public abstract Deferred<Object> close();
  
  /**
   * Returns a collection of cloned iterators using the given context.
   * @param context A query context.
   * @return A cloned iterator list.
   */
  public abstract TimeSeriesIterators getCopy(final QueryContext context);
  
}
