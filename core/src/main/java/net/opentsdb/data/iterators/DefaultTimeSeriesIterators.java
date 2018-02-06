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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.query.context.QueryContext;
import net.opentsdb.utils.Deferreds;

/**
 * A basic, in memory set of iterators implementing {@link TimeSeriesIterators}.
 * 
 * @since 3.0
 */
public class DefaultTimeSeriesIterators extends TimeSeriesIterators {

  /** The list of iterators. Even though they're typed, we don't use a Map
   * because the Map overhead would be a waste for usually 1 data type. */
  protected final List<TimeSeriesIterator<?>> iterators;
  
  /**
   * Default Ctor.
   * @param id A non-null ID associated with all series in the set.
   * @throws IllegalArgumentException if the ID was null.
   */
  public DefaultTimeSeriesIterators(final TimeSeriesId id) {
    super(id);
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
    for (final TimeSeriesIterator<?> iterator : iterators) {
      deferreds.add(iterator.initialize());
    }
    return Deferred.group(deferreds).addCallback(Deferreds.NULL_GROUP_CB);
  }
  
  @Override
  public void addIterator(TimeSeriesIterator<?> iterator) {
    if (iterator == null) {
      throw new IllegalArgumentException("Iterator cannot be null.");
    }
    if (!(id.equals(iterator.id()))) {
      throw new IllegalArgumentException("ID must be the same");
    }
    for (final TimeSeriesIterator<?> it : iterators) {
      if (it.type().equals(iterator.type())) {
        throw new IllegalArgumentException("Type already exists: " 
            + iterator.type());
      }
    }
    if (iterators.isEmpty()) {
      order = iterator.order();
    } else {
      if (iterator.order() != order) {
        throw new IllegalArgumentException("Iterator " + iterator 
            + " had a different order than: " + order);
      }
    }
    iterators.add(iterator);
  }

  @Override
  public List<TimeSeriesIterator<?>> iterators() {
    return Collections.unmodifiableList(iterators);
  }

  @Override
  public TimeSeriesIterator<?> iterator(final TypeToken<?> type) {
    for (final TimeSeriesIterator<?> it : iterators) {
      if (it.type().equals(type)) {
        return it;
      }
    }
    return null;
  }

  @Override
  public Iterator<TimeSeriesIterator<?>> iterator() {
    return iterators.iterator();
  }
  
  @Override
  public void setContext(final QueryContext context) {
    for (final TimeSeriesIterator<?> iterator : iterators) {
      iterator.setContext(context);
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
    for (final TimeSeriesIterator<?> iterator : iterators) {
      deferreds.add(iterator.close());
    }
    return Deferred.group(deferreds).addCallback(Deferreds.NULL_GROUP_CB);
  }
  
  @Override
  public TimeSeriesIterators getCopy(final QueryContext context) {
    final DefaultTimeSeriesIterators clone = new DefaultTimeSeriesIterators(id);
    for (final TimeSeriesIterator<?> it : iterators) {
      clone.addIterator(it.getShallowCopy(context));
    }
    return clone;
  }

}
