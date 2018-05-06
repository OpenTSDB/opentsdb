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

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import org.junit.Ignore;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.Op;
import net.opentsdb.data.iterators.IteratorStatus;
import net.opentsdb.data.iterators.TimeSeriesIterator;
import net.opentsdb.query.context.QueryContext;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.pojo.NumericFillPolicy;
import net.opentsdb.query.processor.TimeSeriesProcessor;

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
  public List<NumericType> data = Lists.newArrayList();
  
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
  public void add(final NumericType value) {
    if (value == null) {
      throw new IllegalArgumentException("Value can't be null!");
    }
    data.add(value);
  }
  
  @Override
  public Optional<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> iterator(
      final TypeToken<?> type) {
    if (type != NumericType.TYPE) {
      return Optional.empty();
    }
    return Optional.of(new LocalIterator());
  }

  @Override
  public Collection<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> iterators() {
    final List<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> iterators = 
        Lists.newArrayListWithCapacity(1);
    iterators.add(new LocalIterator());
    return iterators;
  }

  @Override
  public Collection<TypeToken<?>> types() {
    return Lists.newArrayList(NumericType.TYPE);
  }

  @Override
  public void close() { }
  
  class LocalIterator implements Iterator<TimeSeriesValue<?>> {
    final Iterator<NumericType> iterator = data.iterator();
    
    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public TimeSeriesValue<?> next() {
      if (ex != null && throw_ex) {
        throw ex;
      }
      return (TimeSeriesValue<?>) iterator.next();
    }
    
  }
}
