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
package net.opentsdb.data;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

/**
 * A class for unit tests that allows the user to simply store a bunch of 
 * time series values of various types to mimic a time series. The value
 * arrays are sorted on timestamp when iterators are fetched.
 * 
 * @since 3.0
 */
public class MockTimeSeries implements TimeSeries {

  /** The non-null ID. */
  protected final TimeSeriesStringId id;
  
  /** Whether or not closed has been called. */
  protected boolean closed;
  
  /** The map of types to lists of time series. */
  protected Map<TypeToken<?>, List<TimeSeriesValue<?>>> data;
  
  /**
   * Default ctor.
   * @param id A non-null Id.
   */
  public MockTimeSeries(final TimeSeriesStringId id) {
    if (id == null) {
      throw new IllegalArgumentException("ID cannot be null.");
    }
    this.id = id;
    data = Maps.newHashMap();
  }
  
  /**
   * @param value A non-null value to add to the proper array. Must return a type.
   */
  public void addValue(final TimeSeriesValue<?> value) {
    if (value == null) {
      throw new IllegalArgumentException("Can't store null values.");
    }
    List<TimeSeriesValue<?>> types = data.get(value.type());
    if (types == null) {
      types = Lists.newArrayList();
      data.put(value.type(), types);
    }
    types.add(value);
  }
  
  /** Flushes the map of data but leaves the ID alone. Also resets 
   * the closed flag. */
  public void clear() {
    data.clear();
    closed = false;
  }
  
  @Override
  public TimeSeriesStringId id() {
    return id;
  }

  @Override
  public Optional<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> iterator(
      final TypeToken<?> type) {
    List<TimeSeriesValue<?>> types = data.get(type);
    if (types == null) {
      return Optional.empty();
    }
    Collections.sort(types, new TimeSeriesValue.TimeSeriesValueComparator());
    return Optional.of(new MockTimeSeriesIterator(types.iterator()));
  }

  @Override
  public Collection<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> iterators() {
    final List<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> iterators
      = Lists.newArrayListWithCapacity(data.size());
    for (final List<TimeSeriesValue<?>> types : data.values()) {
      iterators.add(new MockTimeSeriesIterator(types.iterator()));
    }
    return iterators;
  }

  @Override
  public Collection<TypeToken<?>> types() {
    return data.keySet();
  }

  @Override
  public void close() {
    closed = true;
  }

  public boolean closed() {
    return closed;
  }
  
  /**
   * Iterator over the list of values.
   */
  class MockTimeSeriesIterator implements Iterator<TimeSeriesValue<?>> {
    private final Iterator<TimeSeriesValue<?>> iterator;
    
    MockTimeSeriesIterator(final Iterator<TimeSeriesValue<?>> iterator) {
      this.iterator = iterator;
    }
    
    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public TimeSeriesValue<?> next() {
      return iterator.next();
    }
    
  }
  
}
