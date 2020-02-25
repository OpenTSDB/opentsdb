// This file is part of OpenTSDB.
// Copyright (C) 2017-2018  The OpenTSDB Authors.
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
package net.opentsdb.data.types.numeric;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TypedTimeSeriesIterator;
import org.junit.Ignore;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/**
 * Simple little class for mocking out a source.
 * <p>
 * Set the individual deferreds with exceptions or just leave them as nulls.
 * If you set an exception, it will be thrown or returned in the appropriate 
 * calls.
 * <p>
 * To use this mock, add lists of 1 or more data points, sorted in time order,
 * to the {@link #data} list. At the end of each list, the status is set to
 * return {@link IteratorStatus#END_OF_CHUNK}.
 */
@Ignore
public class MockNumericTimeSeries implements TimeSeries {
  
  public final TimeSeriesId id;
  public RuntimeException ex;
  public boolean throw_ex;
  public List<TimeSeriesValue<? extends TimeSeriesDataType>> data = Lists.newArrayList();
  
  public MockNumericTimeSeries(final TimeSeriesId id) {
    this.id = id;
  }
  
  @Override
  public TimeSeriesId id() {
    return id;
  }
  
  /**
   * WARN: Doesn't check time order. Make sure you do.
   * @param value A non-null value. The value of the value can be null.
   */
  public void add(final TimeSeriesValue<NumericType> value) {
    if (value == null) {
      throw new IllegalArgumentException("Value can't be null!");
    }
    data.add(value);
  }
  
  public void add(final long second_timestamp, final long value) {
    data.add(new MutableNumericValue(new SecondTimeStamp(second_timestamp), value));
  }
  
  public void add(final long second_timestamp, final double value) {
    data.add(new MutableNumericValue(new SecondTimeStamp(second_timestamp), value));
  }
  
  @Override
  public Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterator(
      final TypeToken<? extends TimeSeriesDataType> type) {
    if (type != NumericType.TYPE) {
      return Optional.empty();
    }
    final TypedTimeSeriesIterator it = new LocalIterator(data.iterator());
    return Optional.of(it);
  }

  @Override
  public Collection<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterators() {
    final List<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterators =
        Lists.newArrayListWithCapacity(1);
    iterators.add(new LocalIterator(data.iterator()));
    return iterators;
  }

  @Override
  public Collection<TypeToken<? extends TimeSeriesDataType>> types() {
    return Lists.newArrayList(NumericType.TYPE);
  }

  @Override
  public void close() { }
  
  class LocalIterator implements TypedTimeSeriesIterator {

    private final Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> iterator;
    
    public LocalIterator(
        final Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> iterator) {
      this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public TimeSeriesValue<?> next() {
      if (ex != null && throw_ex) {
        throw ex;
      }
      return iterator.next();
    }

    @Override
    public TypeToken<? extends TimeSeriesDataType> getType() {
      return NumericType.TYPE;
    }
    
    @Override
    public void close() {
      // no-op for now
    }
    
  }
}
