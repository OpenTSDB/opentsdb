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

import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.data.TimeSeriesGroupId;
import net.opentsdb.query.context.QueryContext;

/**
 * A collection of zero or more groups of iterators.
 * <p>
 * <b>Note:</b> The {@link #order()} is set on the first call to 
 * {@link #addGroup(IteratorGroup)} or 
 * {@link #addIterator(TimeSeriesGroupId, TimeSeriesIterator)}.
 * 
 * @since 3.0
 */
public abstract class IteratorGroups 
  implements Iterable<Entry<TimeSeriesGroupId, IteratorGroup>> {
  
  /** The type of this class. */
  public static final TypeToken<IteratorGroups> TYPE = 
      TypeToken.of(IteratorGroups.class);

  /** An order if shard is part of a slice config. */
  protected int order;
  
  /**
   * Default ctor. Inits the order to -1.
   */
  public IteratorGroups() {
    order = -1;
  }
  
  /**
   * Initializes all of the iterators in this set.
   * @return A deferred resolving to a null on success or an exception on 
   * failure.
   */
  public abstract Deferred<Object> initialize();
  
  /** @return An optional order within a slice config. -1 by default. */
  public int order() {
    return order;
  }

  /**
   * Adds the iterator to the proper group and set.
   * @param id A non-null group ID.
   * @param iterator A non-null iterator.
   * @throws IllegalArgumentException if the group Id or iterator was null, 
   * if the iterator set already had a value for the type or the order of the
   * iterator was different from the set's order.
   */
  public abstract void addIterator(final TimeSeriesGroupId id, 
                                   final TimeSeriesIterator<?> iterator);
  
  /**
   * Adds the group to the set.
   * @param group A non-null group.
   * @throws IllegalArgumentException if the group was null or it's order was
   * different from the set's order.
   */
  public abstract void addGroup(final IteratorGroup group);

  /**
   * Returns the group associated with the given ID if present.
   * @param id A non-null ID to search for.
   * @return The iterator group if found or null if not.
   * @throws IllegalArgumentException if the ID was null.
   */
  public abstract IteratorGroup group(final TimeSeriesGroupId id);
  
  @Override
  public abstract Iterator<Entry<TimeSeriesGroupId, IteratorGroup>> iterator();
  
  /**
   * @return An unmodifiable and flattened list of all of the iterators in all 
   * groups in the set. May be empty.
   */
  public abstract List<TimeSeriesIterator<?>> flattenedIterators();
  
  /**
   * @return An unmodifiable list of the iterator groups in the set. May be empty.
   */
  public abstract List<IteratorGroup> groups();
  
  /**
   * Runs through the entire set of iterators and calls 
   * {@link TimeSeriesIterator#setContext(QueryContext)} on each.
   * @param context The context to apply to each iterator.
   */
  public abstract void setContext(final QueryContext context);
  
  /**
   * Closes all iterators in the set.
   * @return A deferred that resolves to null on success or an exception on
   * failure.
   */
  public abstract Deferred<Object> close();
  
  /**
   * Creates a deep copy of the set and lists but not necessarily the data.
   * @param context A context to pass to apply to the iterator clones.
   * @return A non-null clone.
   */
  public abstract IteratorGroups getCopy(final QueryContext context);
  
}
