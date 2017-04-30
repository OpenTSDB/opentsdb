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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.data.TimeSeriesGroupId;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.query.context.QueryContext;
import net.opentsdb.utils.Deferreds;

/**
 * A collection of zero or more unique time series sets 
 * (different {@link TimeSeriesId}s) under the same {@link TimeSeriesGroupId}.
 *
 * @since 3.0
 */
public class DefaultIteratorGroup extends IteratorGroup {
  
  /** An array of the iterator sets for different time series. It's an array
   * instead of a map as lookups are rare and adding individual iterators
   * should also be fairly rare. */
  protected final List<TimeSeriesIterators> iterators;
  
  /**
   * Default ctor.
   * @param group A non-null time series group ID.
   * @throws IllegalArgumentException if the group ID was null.
   */
  public DefaultIteratorGroup(final TimeSeriesGroupId group) {
    super(group);
    iterators = Lists.newArrayListWithExpectedSize(1);
  }
  
  @Override
  public Deferred<Object> initialize() {
    if (iterators.isEmpty()) {
      return Deferred.fromResult(null);
    }
    if (iterators.size() == 1) {
      return iterators.get(0).initialize();
    }
    final List<Deferred<Object>> deferreds = 
        Lists.newArrayListWithExpectedSize(iterators.size());
    for (final TimeSeriesIterators iterator : iterators) {
      deferreds.add(iterator.initialize());
    }
    return Deferred.group(deferreds).addCallback(Deferreds.NULL_GROUP_CB);
  }
  
  @Override
  public void addIterator(final TimeSeriesIterator<?> iterator) {
    if (iterator == null) {
      throw new IllegalArgumentException("Iterator cannot be null.");
    }
    if (iterators.isEmpty()) {
      order = iterator.order();
    } else {
      if (iterator.order() != order) {
        throw new IllegalArgumentException("Iterator " + iterator 
            + " order was different from our order: " + order);
      }
    }
    for (final TimeSeriesIterators set : iterators) {
      if (set.id().equals(iterator.id())) {
        set.addIterator(iterator);
        return;
      }
    }
    
    final TimeSeriesIterators set = 
        new DefaultTimeSeriesIterators(iterator.id());
    set.addIterator(iterator);
    iterators.add(set);
  }

  @Override
  public void addIterators(final TimeSeriesIterators iterators) {
    if (iterators == null) {
      throw new IllegalArgumentException("Iterators cannot be null.");
    }
    if (this.iterators.isEmpty()) {
      order = iterators.order();
    } else {
      if (iterators.order() != order) {
        throw new IllegalArgumentException("Iterator set " + iterators 
            + " order was different from our order: " + order);
      }
    }
    this.iterators.add(iterators);
  }
  
  @Override
  public List<TimeSeriesIterators> iterators() {
    return Collections.unmodifiableList(iterators);
  }

  @Override
  public List<TimeSeriesIterator<?>> iterators(final TypeToken<?> type) {
    if (type == null) {
      throw new IllegalArgumentException("Type cannot be null.");
    }
    final List<TimeSeriesIterator<?>> its = Lists.newArrayList();
    for (final TimeSeriesIterators set : iterators) {
      final TimeSeriesIterator<?> typed = set.iterator(type);
      if (typed != null) {
        its.add(typed);
      }
    }
    return its;
  }

  @Override
  public List<TimeSeriesIterator<?>> flattenedIterators() {
    final List<TimeSeriesIterator<?>> its = Lists.newArrayList();
    for (final TimeSeriesIterators set : iterators) {
      its.addAll(set.iterators());
    }
    return its;
  }

  @Override
  public Iterator<TimeSeriesIterators> iterator() {
    return iterators.iterator();
  }

  @Override
  public void setContext(final QueryContext context) {
    for (final TimeSeriesIterators set : iterators) {
      set.setContext(context);
    }
  }
  
  @Override
  public Deferred<Object> close() {
    if (iterators.isEmpty()) {
      return Deferred.fromResult(null);
    }
    if (iterators.size() == 1) {
      return iterators.get(0).close();
    }
    final List<Deferred<Object>> deferreds = 
        Lists.newArrayListWithExpectedSize(iterators.size());
    for (final TimeSeriesIterators iterator : iterators) {
      deferreds.add(iterator.close());
    }
    return Deferred.group(deferreds).addCallback(Deferreds.NULL_GROUP_CB);
  }
  
  @Override
  public IteratorGroup getCopy(final QueryContext context) {
    final DefaultIteratorGroup clone = new DefaultIteratorGroup(group);
    for (final TimeSeriesIterators its : iterators) {
      clone.addIterators(its.getCopy(context));
    }
    return clone;
  }

}
