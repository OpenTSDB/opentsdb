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
// see <http://www.gnu.org/licenses/>.package net.opentsdb.data;
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
  protected final TimeSeriesId id;
  
  /** Whether or not closed has been called. */
  protected boolean closed;
  
  /** The map of types to lists of time series. */
  protected Map<TypeToken<?>, List<TimeSeriesValue<?>>> data;
  
  /**
   * Default ctor.
   * @param id A non-null Id.
   */
  public MockTimeSeries(final TimeSeriesId id) {
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
  
  @Override
  public TimeSeriesId id() {
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
