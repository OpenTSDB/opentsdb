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
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.stumbleupon.async.Deferred;

import net.opentsdb.data.TimeSeriesGroupId;
import net.opentsdb.query.context.QueryContext;
import net.opentsdb.utils.Deferreds;

/**
 * Simple implementation of {@link IteratorGroups} using a map.
 * 
 * @since 3.0
 */
public class DefaultIteratorGroups extends IteratorGroups {

  /** The map of group IDs to groups. */
  protected final Map<TimeSeriesGroupId, IteratorGroup> groups;
  
  /**
   * Default ctor.
   */
  public DefaultIteratorGroups() {
    groups = Maps.newHashMapWithExpectedSize(1);
  }
  
  @Override
  public void addIterator(final TimeSeriesGroupId id,
                          final TimeSeriesIterator<?> iterator) {
    if (id == null) {
      throw new IllegalArgumentException("Group ID cannot be null.");
    }
    if (iterator == null) {
      throw new IllegalArgumentException("Iterator cannot be null.");
    }
    if (groups.isEmpty()) {
      order = iterator.order();
    } else {
      if (iterator.order() != order) {
        throw new IllegalArgumentException("Iterator " + iterator 
            + " order was different than the groups order: " + order);
      }
    }
    IteratorGroup group = groups.get(id);
    if (group == null) {
      group = new DefaultIteratorGroup(id);
      groups.put(id, group);
    }
    group.addIterator(iterator);
  }

  @Override
  public void addGroup(final IteratorGroup group) {
    if (group == null) {
      throw new IllegalArgumentException("Group cannot be null.");
    }
    if (groups.isEmpty()) {
      order = group.order();
    } else {
      if (group.order() != order) {
        throw new IllegalArgumentException("Group " + group 
            + " order was different than the groups order: " + order);
      }
    }
    groups.put(group.id(), group);
  }

  @Override
  public IteratorGroup group(final TimeSeriesGroupId id) {
    if (id == null) {
      throw new IllegalArgumentException("ID cannot be null.");
    }
    return groups.get(id);
  }

  @Override
  public Iterator<Entry<TimeSeriesGroupId, IteratorGroup>> iterator() {
    return groups.entrySet().iterator();
  }

  @Override
  public List<IteratorGroup> groups() {
    return Collections.unmodifiableList(Lists.newArrayList(groups.values()));
  }
  
  @Override
  public List<TimeSeriesIterator<?>> flattenedIterators() {
    List<TimeSeriesIterator<?>> its = Lists.newArrayList();
    for (final IteratorGroup it : groups.values()) {
      its.addAll(it.flattenedIterators());
    }
    return its;
  }

  @Override
  public Deferred<Object> initialize() {
    if (groups.isEmpty()) {
      return Deferred.fromResult(null);
    }
    if (groups.size() == 1) {
      return groups.values().iterator().next().initialize();
    }
    final List<Deferred<Object>> deferreds = 
        Lists.newArrayListWithExpectedSize(groups.size());
    for (final IteratorGroup group : groups.values()) {
      deferreds.add(group.initialize());
    }
    return Deferred.group(deferreds).addCallback(Deferreds.NULL_GROUP_CB);
  }

  @Override
  public void setContext(final QueryContext context) {
    for (final IteratorGroup group : groups.values()) {
      group.setContext(context);
    }
  }

  @Override
  public Deferred<Object> close() {
    if (groups.isEmpty()) {
      return Deferred.fromResult(null);
    }
    if (groups.size() == 1) {
      return groups.values().iterator().next().close();
    }
    final List<Deferred<Object>> deferreds = 
        Lists.newArrayListWithExpectedSize(groups.size());
    for (final IteratorGroup group : groups.values()) {
      deferreds.add(group.close());
    }
    return Deferred.group(deferreds).addCallback(Deferreds.NULL_GROUP_CB);
  }

  @Override
  public IteratorGroups getCopy(final QueryContext context) {
    final IteratorGroups clone = new DefaultIteratorGroups();
    for (final IteratorGroup group : groups.values()) {
      clone.addGroup(group.getCopy(context));
    }
    return clone;
  }


}
