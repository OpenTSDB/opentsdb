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

import java.util.List;

import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.data.TimeSeriesGroupId;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.query.context.QueryContext;

/**
 * A collection of zero or more unique time series sets 
 * (different {@link TimeSeriesId}s) under the same {@link TimeSeriesGroupId}.
 * <p>
 * <b>Note:</b> The {@link #order()} is set on the first call to 
 * {@link #addIterator(TimeSeriesIterator)} or 
 * {@link #addIterators(TimeSeriesIterators)}.
 *
 * @since 3.0
 */
public abstract class IteratorGroup implements Iterable<TimeSeriesIterators> {
  /** The group this set of iterator lists belong to. */
  protected final TimeSeriesGroupId group;
  
  /** An order if shard is part of a slice config. */
  protected int order;
  
  /**
   * Default ctor.
   * @param group A non-null time series group ID.
   * @throws IllegalArgumentException if the group ID was null.
   */
  public IteratorGroup(final TimeSeriesGroupId group) {
    if (group == null) {
      throw new IllegalArgumentException("Group cannot be null.");
    }
    this.group = group;
    order = -1;
  }
  
  /** @return The time series group Id */
  public TimeSeriesGroupId id() {
    return group;
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
   * Adds the iterator to the proper {@link TimeSeriesIterators} set.
   * @param iterator A non-null iterator to add.
   * @throws IllegalArgumentException if the iterator was null, if the
   * iterator set already had a value for the type or if the order of the 
   * iterator was different than the group's order.
   */
  public abstract void addIterator(final TimeSeriesIterator<?> iterator);
  
  /**
   * Adds the iterator set to the list. <b>Note:</b> Does not check for duplicate
   * sets and does not merge sets.
   * @param set The non-null set of time series iterators to add.
   * @throws IllegalArgumentException if the set was null.
   */
  public abstract void addIterators(final TimeSeriesIterators set);
  
  /** @return An unmodifiable list of the iterator sets. May be empty. */
  public abstract List<TimeSeriesIterators> iterators();
  
  /** @return A list of flattened iterators of the given type. May be null. 
   * @throws IllegalArgumentException if the type was null. */
  public abstract List<TimeSeriesIterator<?>> iterators(final TypeToken<?> type);

  /** @return A flattened list of all iterators in all sets. */
  public abstract List<TimeSeriesIterator<?>> flattenedIterators();
  
  /**
   * Runs through the entire set of iterators and calls 
   * {@link TimeSeriesIterator#setContext(QueryContext)} on each.
   * @param context The context to apply to each iterator.
   */
  public abstract void setContext(final QueryContext context);
  
  /**
   * Closes all underlying iterators.
   * @return A deferred to wait on for completion resolving to a null on success
   * or an exception on failure.
   */
  public abstract Deferred<Object> close();
  
  /**
   * Clones the group and underlying iterators.
   * @param context An optional query context to assign to the cloned iterators.
   * @return A duplicate of this iterator group.
   */
  public abstract IteratorGroup getCopy(final QueryContext context);
  
}
