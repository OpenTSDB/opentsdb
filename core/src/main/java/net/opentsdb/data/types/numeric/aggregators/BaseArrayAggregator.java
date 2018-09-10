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
package net.opentsdb.data.types.numeric.aggregators;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.types.numeric.NumericArrayType;

/**
 * A base implementation for numeric array aggregation functions.
 * 
 * @since 3.0
 */
public abstract class BaseArrayAggregator implements NumericArrayAggregator {

  /** Whether or not infectious NaNs are enabled. */
  protected final boolean infectious_nans;
  
  /** The long accumulator. */
  protected long[] long_accumulator;
  
  /** The double accumulator. */
  protected double[] double_accumulator;

  /**
   * Default ctor.
   * @param infectious_nans Whether or not infectious NaNs are enabled.
   */
  public BaseArrayAggregator(final boolean infectious_nans) {
    this.infectious_nans = infectious_nans;
  }
  
  @Override
  public void accumulate(final long[] values) {
    accumulate(values, 0, values.length);
  }
  
  @Override
  public void accumulate(final double[] values) {
    accumulate(values, 0, values.length);
  }
  
  @Override
  public boolean isInteger() {
    return long_accumulator == null ? false : true;
  }

  @Override
  public long[] longArray() {
    return long_accumulator;
  }

  @Override
  public double[] doubleArray() {
    return double_accumulator;
  }

  @Override
  public TypeToken<? extends TimeSeriesDataType> type() {
    return NumericArrayType.TYPE;
  }

  @Override
  public int offset() {
    return 0;
  }

  @Override
  public int end() {
    return long_accumulator != null ? long_accumulator.length : 
      double_accumulator.length;
  }
  
}