// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
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
import java.util.List;
import java.util.Optional;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TypedTimeSeriesIterator;

/**
 * A simple implementation of the {@link NumericArrayType} primarily
 * for testing.
 * <p>
 * <b>WARNING</b> Don't add to this series after fetching iterators.
 * 
 * @since 3.0
 */
public class NumericArrayTimeSeries implements TimeSeries {

  /** ID of the time series. */
  private final TimeSeriesId id;
  
  /** The first timestamp for this series. */
  private final TimeStamp timestamp;
  
  /** Long values. */
  private long[] long_values;
  
  /** Double values. */
  private double[] double_values;
  
  /** The write index. */
  private int idx;
  
  /**
   * Default ctor.
   * @param id A non-null time series ID.
   * @param timestamp A non-null timestamp.
   */
  public NumericArrayTimeSeries(final TimeSeriesId id, 
                                final TimeStamp timestamp) {
    this.id = id;
    this.timestamp = timestamp;
    long_values = new long[8];
  }
  
  /**
   * Adds an integer value to the end of the array.
   * @param value The value to add.
   */
  public void add(final long value) {
    if (long_values != null) {
      addLong(value);
    } else {
      addDouble((double) value);
    }
  }
  
  /**
   * Adds a floating point value to the end of the array.
   * @param value The value to add.
   */
  public void add(final double value) {
    if (long_values != null) {
      // shift!
      double_values = new double[long_values.length];
      for (int i = 0; i < long_values.length; i++) {
        double_values[i] = (double) long_values[i];
      }
      long_values = null;
    }
    
    addDouble(value);
  }
  
  private void addLong(final long value) {
    if (idx >= long_values.length) {
      final long[] temp = new long[long_values.length < 1024 ? 
          long_values.length * 2 : long_values.length + 16];
      for (int i = 0; i < long_values.length; i++) {
        temp[i] = long_values.length;
      }
      long_values = temp;
    }
    long_values[idx++] = value;
  }
  
  private void addDouble(final double value) {
    if (idx >= double_values.length) {
      final double[] temp = new double[double_values.length < 1024 ? 
          double_values.length * 2 : double_values.length + 16];
      for (int i = 0; i < double_values.length; i++) {
        temp[i] = double_values[i];
      }
      double_values = temp;
    }
    double_values[idx++] = value;
  }

  @Override
  public TimeSeriesId id() {
    return id;
  }

  @Override
  public Optional<TypedTimeSeriesIterator> iterator(
      final TypeToken<? extends TimeSeriesDataType> type) {
    if (type == NumericArrayType.TYPE) {
      return Optional.of(new LocalIterator());
    }
    return Optional.empty();
  }

  @Override
  public Collection<TypedTimeSeriesIterator> iterators() {
    final List<TypedTimeSeriesIterator> iterators = 
        Lists.newArrayListWithExpectedSize(1);
    iterators.add(new LocalIterator());
    return iterators;
  }

  @Override
  public Collection<TypeToken<? extends TimeSeriesDataType>> types() {
    return Lists.newArrayList(NumericArrayType.TYPE);
  }

  @Override
  public void close() {
    // no-op
  }

  /**
   * Just reflects the data in the arrays.
   */
  class LocalIterator implements TypedTimeSeriesIterator, 
                                 TimeSeriesValue<NumericArrayType>, 
                                 NumericArrayType {
    private boolean read;
    
    @Override
    public int offset() {
      return 0;
    }
    
    @Override
    public int end() {
      return idx;
    }
    
    @Override
    public boolean isInteger() {
      return long_values != null;
    }

    @Override
    public long[] longArray() {
      return long_values;
    }

    @Override
    public double[] doubleArray() {
      return double_values;
    }

    @Override
    public boolean hasNext() {
      return idx > 0 ? !read : false;
    }

    @Override
    public TimeSeriesValue<?> next() {
      read = true;
      return this;
    }

    @Override
    public TypeToken<NumericArrayType> type() {
      return NumericArrayType.TYPE;
    }

    @Override
    public TimeStamp timestamp() {
      return timestamp;
    }

    @Override
    public NumericArrayType value() {
      return this;
    }
  
    @Override
    public TypeToken<? extends TimeSeriesDataType> getType() {
      return NumericArrayType.TYPE;
    }
  }
}