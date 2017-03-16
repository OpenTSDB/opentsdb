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

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.stumbleupon.async.Deferred;

import net.opentsdb.data.TimeSeriesGroupId;
import net.opentsdb.query.context.QueryContext;

/**
 * Contains a set of {@link GroupedAndTypedIteratorLists} mapped by a 
 * {@link TimeSeriesGroupId}.
 * <p>
 * The Iterator and collections returned from this object are immutable. While
 * calls to {@link #addIterator(TimeSeriesGroupId, TimeSeriesIterator)} and 
 * {@link #addGroup(GroupedAndTypedIteratorLists)} can be made at any time, it 
 * is recommended not to do so after using one or more of the methods to read
 * the data out.
 * <p>
 * <b>WARNING:</b> This method is not thread safe. If multiple threads will be
 * adding iterators or calling one of the other mutable methods, make sure they 
 * synchronize on the container.
 * 
 * @since 3.0
 */
public class GroupedIterators 
    implements Iterable<Entry<TimeSeriesGroupId, GroupedAndTypedIteratorLists>> {

  /** The map of iterators keyed by group. */
  private final Map<TimeSeriesGroupId, GroupedAndTypedIteratorLists> iterators;
  
  /** Default ctor initializes the map with a size of 1. */
  public GroupedIterators() {
    iterators = Maps.newHashMapWithExpectedSize(1);
  }
  
  /**
   * Ctor that initializes the map to the expected size.
   * @param expected_size How many groups are expected in the set.
   */
  public GroupedIterators(final int expected_size) {
    iterators = Maps.newHashMapWithExpectedSize(expected_size);
  }
  
  /**
   * Adds the given iterator to the proper typed list for the group.
   * @param id A non-null group ID.
   * @param iterator A non-null iterator.
   * @throws IllegalArgumentException if the ID or iterator was null.
   */
  public void addIterator(final TimeSeriesGroupId id, 
      final TimeSeriesIterator<?> iterator) {
    if (id == null) {
      throw new IllegalArgumentException("ID cannot be null.");
    }
    if (iterator == null) {
      throw new IllegalArgumentException("Iterator cannot be null.");
    }
    GroupedAndTypedIteratorLists group = iterators.get(id);
    if (group == null) {
      group = new GroupedAndTypedIteratorLists(id);
      iterators.put(id, group);
    }
    group.addIterator(iterator);
  }
  
  /**
   * Adds the given group set to the set if it isn't already present.
   * @param group A non-null group to add.
   * @throws IllegalArgumentException if a group with the same ID is already
   * present.
   */
  public void addGroup(final GroupedAndTypedIteratorLists group) {
    if (group == null) {
      throw new IllegalArgumentException("Group cannot be null.");
    }
    if (iterators.containsKey(group.id())) {
      throw new IllegalArgumentException("Group " + group.id() 
        + " already has an iterator group");
    }
    iterators.put(group.id(), group);
  }
  
  /**
   * Returns the typed iterators for the given group if they exist.
   * @param id A non-null group ID.
   * @return A typed iterator list if present, null if not.
   * @throws IllegalArgumentException if the id was null.
   */
  public GroupedAndTypedIteratorLists getGroup(final TimeSeriesGroupId id) {
    if (id == null) {
      throw new IllegalArgumentException("ID cannot be null.");
    }
    return iterators.get(id);
  }
  
  @Override
  public Iterator<Entry<TimeSeriesGroupId, GroupedAndTypedIteratorLists>> iterator() {
    return Collections.unmodifiableMap(iterators).entrySet().iterator();
  }

  /** @return The collection of grouped and typed iterators.  */
  public Collection<GroupedAndTypedIteratorLists> iteratorLists() {
    return Collections.unmodifiableCollection(iterators.values());
  }
  
  /** @return A flattened list of all iterators, regardless of type or group. */
  public List<TimeSeriesIterator<?>> flattenedIterators() {
    final List<TimeSeriesIterator<?>> its = Lists.newArrayList();
    for (final GroupedAndTypedIteratorLists group : iterators.values()) {
      for (final TypedIteratorList set : group.iterators()) {
        its.addAll(set.iterators());
      }
    }
    return Collections.unmodifiableList(its);
  }
  
  /**
   * Runs through the entire set of iterators and calls 
   * {@link TimeSeriesIterator#initialize()} on each, adding the deferred to 
   * the given deferred list.
   * @param deferreds A non-null list to accumulate the deferreds in.
   * @throws IllegalArgumentException if the deferreds list was null.
   */
  public void initializeIterators(final List<Deferred<Object>> deferreds) {
    if (deferreds == null) {
      throw new IllegalArgumentException("Deferreds list cannot be null.");
    }
    for (final GroupedAndTypedIteratorLists group : iterators.values()) {
      for (final TypedIteratorList its : group.iterators()) {
        for (final TimeSeriesIterator<?> it : its.iterators()) {
          deferreds.add(it.initialize());
        }
      }
    }
  }
  
  /**
   * Runs through the entire set of iterators and calls 
   * {@link TimeSeriesIterator#setContext(QueryContext)} on each.
   * @param context The context to apply to each iterator.
   */
  public void setContext(final QueryContext context) {
    for (final GroupedAndTypedIteratorLists group : iterators.values()) {
      for (final TypedIteratorList its : group.iterators()) {
        for (final TimeSeriesIterator<?> it : its.iterators()) {
          it.setContext(context);
        }
      }
    }
  }
  
  /**
   * Runs through the entire set of iterators and calls 
   * {@link TimeSeriesIterator#close()} on each, adding the deferred to 
   * the given deferred list.
   * @param deferreds A non-null list to accumulate the deferreds in.
   * @throws IllegalArgumentException if the deferreds list was null.
   */
  public void close(final List<Deferred<Object>> deferreds) {
    if (deferreds == null) {
      throw new IllegalArgumentException("Deferreds list cannot be null.");
    }
    for (final GroupedAndTypedIteratorLists group : iterators.values()) {
      for (final TypedIteratorList its : group.iterators()) {
        for (final TimeSeriesIterator<?> it : its.iterators()) {
          deferreds.add(it.close());
        }
      }
    }
  }
  
  /**
   * Creates a deep copy of the set and lists.
   * @param context A context to pass to apply to the iterator clones.
   * @return A non-null clone.
   */
  public GroupedIterators getClone(final QueryContext context) {
    final GroupedIterators clone = new GroupedIterators(iterators.size());
    for (final GroupedAndTypedIteratorLists group : iterators.values()) {
      clone.addGroup(group.getClone(context));
    }
    return clone;
  }

}
