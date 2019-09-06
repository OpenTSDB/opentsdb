// This file is part of OpenTSDB.
// Copyright (C) 2019  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.summarizer;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryIterator;

public class SummarizerPassThroughNumericIterator implements QueryIterator {
  SummarizedTimeSeries sts;
  
  /** The source iterator. */
  private TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator;
  
  /** Results from the source. */
  private long[] long_values;
  private double[] double_values;
  
  /** The index into the results. */
  private int idx;
  
  SummarizerPassThroughNumericIterator(final SummarizedTimeSeries sts) {
    this.sts = sts;
    iterator = sts.source.iterator(NumericType.TYPE).get();
    if (!iterator.hasNext()) {
      sts.fillEmpty();
    } else {
      long_values = new long[8];
    }
  }
  
  @Override
  public boolean hasNext() {
    if (!iterator.hasNext()) {
      if (long_values != null) {
        sts.summarize(long_values, 0, idx);
      } else {
        sts.summarize(double_values, 0, idx);
      }
      return false;
    }
    return true;
  }

  @Override
  public Object next() {
    final TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    if (value.value() != null) {
      if (value.value().isInteger()) {
        store(value.value().longValue());
      } else {
        store(value.value().doubleValue());
      }
    }
    return value;
  }
  
  @Override
  public TypeToken getType() {
    return NumericType.TYPE;
  }
  
  /**
   * Stores a long.
   * @param value The value.
   */
  void store(final long value) {
    if (long_values == null) {
      store((double) value);
      return;
    }
    
    if (idx >= long_values.length) {
      long[] temp = new long[long_values.length + 16];
      for (int i = 0; i < long_values.length; i++) {
        temp[i] = long_values[i];
      }
      long_values = temp;
    }
    long_values[idx++] = value;
  }
  
  /**
   * Stores a double.
   * @param value The value.
   */
  void store(final double value) {
    if (long_values != null) {
      double_values = new double[long_values.length];
      for (int i = 0; i < long_values.length; i++) {
        double_values[i] = long_values[i];
      }
      long_values = null;
    }
    
    if (double_values == null) {
      double_values = new double[8];
    }
    
    if (idx >= double_values.length) {
      double[] temp = new double[double_values.length + 16];
      for (int i = 0; i < double_values.length; i++) {
        temp[i] = double_values[i];
      }
      double_values = temp;
    }
    double_values[idx++] = value;
  }
}
