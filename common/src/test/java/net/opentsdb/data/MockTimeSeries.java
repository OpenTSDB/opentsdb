// This file is part of OpenTSDB.
// Copyright (C) 2017-2020  The OpenTSDB Authors.
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
import java.util.Map.Entry;
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
  
  /** Whether or not we should sort when returning iterators. */
  protected final boolean sort;
  
  /** Whether or not closed has been called. */
  protected boolean closed;
  
  /** The map of types to lists of time series. */
  protected Map<TypeToken<? extends TimeSeriesDataType>, 
    List<TimeSeriesValue<?>>> data;
  
  /**
   * Default ctor.
   * @param id A non-null Id.
   */
  public MockTimeSeries(final TimeSeriesStringId id) {
    this(id, false);
  }
  
  /**
   * Alternate ctor to set sorting.
   * @param id A non-null Id.
   * @param sort Whether or not to sort on timestamps on the output.
   */
  public MockTimeSeries(final TimeSeriesStringId id, final boolean sort) {
    this.sort = sort;
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
  
  /**
   * Attempts to remove the value at the given index. Doesn't perform a check
   * to make sure the value is there.
   * @param type The non-null type to remove.
   * @param index The index to remove at.
   */
  public void remove(final TypeToken<? extends TimeSeriesDataType> type, 
                     final int index) {
    List<TimeSeriesValue<?>> list = data.get(type);
    if (list != null) {
      list.remove(index);
    }
  }
  
  /**
   * Attempts to replace the value at the given index. Doesn't perform an index check.
   * @param index The index to replace at.
   * @param value The value to insert.
   */
  public void replace(final int index, final TimeSeriesValue<?> value) {
    List<TimeSeriesValue<?>> list = data.get(value.type());
    if (list != null) {
      list.set(index, value);
    }
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
  public Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterator(
      final TypeToken<? extends TimeSeriesDataType> type) {
    List<TimeSeriesValue<? extends TimeSeriesDataType>> types = data.get(type);
    if (types == null) {
      return Optional.empty();
    }
    if (sort) {
      Collections.sort(types, new TimeSeriesValue.TimeSeriesValueComparator());
    }
    final TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator =
        new MockTimeSeriesIterator(types.iterator(), type);
    return Optional.of(iterator);
  }

  @Override
  public Collection<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterators() {
    final List<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterators
      = Lists.newArrayListWithCapacity(data.size());
    for (final Entry<TypeToken<? extends TimeSeriesDataType>, 
        List<TimeSeriesValue<?>>> entry : data.entrySet()) {
      
      iterators.add(new MockTimeSeriesIterator(entry.getValue().iterator(), entry.getKey()));
    }
    return iterators;
  }

  @Override
  public Collection<TypeToken<? extends TimeSeriesDataType>> types() {
    return data.keySet();
  }

  @Override
  public void close() {
    closed = true;
  }

  public boolean closed() {
    return closed;
  }
  
  public Map<TypeToken<? extends TimeSeriesDataType>, 
      List<TimeSeriesValue<?>>> data() {
    return data;
  }
  
  /**
   * Iterator over the list of values.
   */
  class MockTimeSeriesIterator implements TypedTimeSeriesIterator {
    private final Iterator<TimeSeriesValue<?>> iterator;
    private final TypeToken<? extends TimeSeriesDataType> type;
    
    MockTimeSeriesIterator(final Iterator<TimeSeriesValue<?>> iterator,
                           final TypeToken<? extends TimeSeriesDataType> type) {
      this.iterator = iterator;
      this.type = type;
    }
    
    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public TimeSeriesValue<?> next() {
      return iterator.next();
    }
    
    @Override
    public TypeToken<? extends TimeSeriesDataType> getType() {
      return type;
    }
    
    @Override
    public void close() {
      // no-op for now
    }
    
  }
  
}
