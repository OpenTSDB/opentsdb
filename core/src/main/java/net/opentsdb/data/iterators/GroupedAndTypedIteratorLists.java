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
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeriesGroupId;
import net.opentsdb.query.context.QueryContext;

/**
 * Contains a set of {@link TypedIteratorList}s of different types belonging to
 * the same {@link TimeSeriesGroupId}. 
 * <p>
 * Iterator and collections returned from this object are immutable. While calls
 * to {@link #addIterator(TimeSeriesIterator)} and {@link #addIterators(TypedIteratorList)}
 * can be made at any time, it's recommended not to do so after using one or more
 * of the methods to read it out.
 * <p>
 * <b>WARNING:</b> This method is not thread safe. If multiple threads will be
 * adding iterators, make sure they synchronize on the container.
 * 
 * @since 3.0
 */
public class GroupedAndTypedIteratorLists implements 
    Iterable<Entry<TypeToken<?>, TypedIteratorList>>{

  /** The group this set of iterator lists belong to. */
  private final TimeSeriesGroupId group;
  
  /** The map of types to iterator lists for quick lookups. */
  private final Map<TypeToken<?>, TypedIteratorList> iterators;
  
  /**
   * Default ctor that initializes the group.
   * @param group A non-null group ID.
   * @throws IllegalArgumentException if the group ID was null.
   */
  public GroupedAndTypedIteratorLists(final TimeSeriesGroupId group) {
    if (group == null) {
      throw new IllegalArgumentException("Group cannot be null.");
    }
    this.group = group;
    // TODO - once we have the registry, we can size this per registered types.
    // But for now, queries will almost always return just data points so
    // save a bit of space.
    iterators = Maps.newHashMapWithExpectedSize(1);
  }
  
  /** @return The Group ID associated with this collection. */
  public TimeSeriesGroupId id() {
    return group;
  }
  
  /**
   * Returns the iterator list associated with the given type if present.
   * @param type A non-null type to look for.
   * @return An iterator list or null if not present.
   * @throws IllegalArgumentException if the type was null.
   */
  public TypedIteratorList iterators(final TypeToken<?> type) {
    if (type == null) {
      throw new IllegalArgumentException("Type cannot be null.");
    }
    return iterators.get(type);
  }
  
  /** @return The collection of iterator lists. May be empty. */
  public Collection<TypedIteratorList> iterators() {
    return Collections.unmodifiableCollection(iterators.values());
  }
  
  /**
   * Adds the given iterator to the proper typed list, creating one if necessary.
   * @param iterator A non-null iterator to add.
   * @throws IllegalArgumentException if the iterator was null.
   */
  public void addIterator(final TimeSeriesIterator<?> iterator) {
    if (iterator == null) {
      throw new IllegalArgumentException("Iterator cannot be null.");
    }
    TypedIteratorList set = iterators.get(iterator.type());
    if (set == null) {
      set = new TypedIteratorList();
      iterators.put(iterator.type(), set);
    }
    set.addIterator(iterator);
  }
  
  /**
   * Adds the set to the list. If the list is already present, throws an
   * IllegalArgumentException. 
   * @param list A non-null list to add.
   * @throws IllegalArgumentException if the list was null or already present.
   */
  public void addIterators(final TypedIteratorList list) {
    if (list == null) {
      throw new IllegalArgumentException("Iterator cannot be null.");
    }
    if (iterators.containsKey(list.type())) {
      throw new IllegalArgumentException("A list with type " + list.type() 
        + " already exists.");
    }
    iterators.put(list.type(), list);
  }

  @Override
  public Iterator<Entry<TypeToken<?>, TypedIteratorList>> iterator() {
    return Collections.unmodifiableMap(iterators).entrySet().iterator();
  }
  
  /**
   * Creates a deep copy of the set and lists.
   * @param context A context to pass to apply to the iterator clones.
   * @return A non-null clone.
   */
  public GroupedAndTypedIteratorLists getClone(final QueryContext context) {
    final GroupedAndTypedIteratorLists clone = new GroupedAndTypedIteratorLists(group);
    for (final TypedIteratorList list : iterators.values()) {
      clone.addIterators(list.getClone(context));
    }
    return clone;
  }
}
