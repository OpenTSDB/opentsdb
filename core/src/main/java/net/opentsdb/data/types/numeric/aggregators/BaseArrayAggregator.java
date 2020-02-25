// This file is part of OpenTSDB.
// Copyright (C) 2018-2020  The OpenTSDB Authors.
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
import net.opentsdb.pools.ArrayObjectPool;
import net.opentsdb.pools.PooledObject;

import java.util.Arrays;

/**
 * A base implementation for numeric array aggregation functions.
 * 
 * @since 3.0
 */
public abstract class BaseArrayAggregator implements NumericArrayAggregator {

  /** Whether or not infectious NaNs are enabled. */
  protected final boolean infectious_nans;
  
  /** The config if given. */
  protected final NumericArrayAggregatorConfig config;
  
  protected final BaseArrayFactory factory;
  
  /** An optional pooled obj to release. */
  protected PooledObject pooled;
  
  /** The long accumulator. */
  protected long[] long_accumulator;
  
  /** The double accumulator. */
  protected double[] double_accumulator;
  
  protected int end;
  
  protected ArrayObjectPool long_pool;
  protected ArrayObjectPool double_pool;

  /**
   * Default ctor.
   * @param infectious_nans Whether or not infectious NaNs are enabled.
   * @param factory The factory from whence we came.
   */
  public BaseArrayAggregator(final boolean infectious_nans,
                             final BaseArrayFactory factory) {
    this.infectious_nans = infectious_nans;
    this.factory = factory;
    config = null;
  }
  
  public BaseArrayAggregator(final NumericArrayAggregatorConfig config,
                             final BaseArrayFactory factory) {
    this.config = config;
    this.factory = factory;
    this.infectious_nans = config.infectiousNan();
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
  public void accumulate(final long value, final int idx) {
    accumulate((double) value, idx);
  }
  
  @Override
  public void combine(final NumericArrayAggregator aggregator) {
    if (((BaseArrayAggregator) aggregator).double_accumulator != null) {
      accumulate(((BaseArrayAggregator) aggregator).double_accumulator, 0, 
          aggregator.end());
    }
    if (((BaseArrayAggregator) aggregator).long_accumulator != null) {
      accumulate(((BaseArrayAggregator) aggregator).long_accumulator, 0, 
          aggregator.end());
    }
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
    return end;
  }
  
  @Override
  public String toString() {
    if (isInteger()) {
      return Arrays.toString(long_accumulator);
    } else {
      return Arrays.toString(double_accumulator);
    }
  }

  @Override
  public void close() {
    if (pooled != null) {
      pooled.release();
      pooled = null;
    }
    long_accumulator = null;
    double_accumulator = null;
  }
  
  protected void initLongPooled(final int size) {
    pooled = factory.longPool().claim(size);
    long_accumulator = (long[]) pooled.object();
    end = size;
  }
  
  protected void initLong(final int size) {
    if (factory.longPool() != null) {
      initLongPooled(size);
    } else {
      long_accumulator = new long[size];
    }
    end = size;
  }
  
  protected void initLong(final long[] values, 
                          final int from, 
                          final int to) {
    if (factory.longPool() != null) {
      pooled = factory.longPool().claim(to - from);
      long_accumulator = (long[]) pooled.object();
      System.arraycopy(values, from, long_accumulator, 0, to - from);
    } else {
      long_accumulator = Arrays.copyOfRange(values, from, to);
    }
    end = to - from;
  }
  
  protected void initDoublePooled(final int size) {
    pooled = factory.doublePool().claim(size);
    double_accumulator = (double[]) pooled.object();
    end = size;
  }
  
  protected void initDouble(final double[] values, 
                            final int from, 
                            final int to) {
    if (factory.doublePool() != null) {
      pooled = factory.doublePool().claim(to - from);
      double_accumulator = (double[]) pooled.object();
      System.arraycopy(values, from, double_accumulator, 0, to - from);
    } else {
      double_accumulator = Arrays.copyOfRange(values, from, to);
    }
    end = to - from;
  }
  
  protected void initDouble(final int size) {
    if (factory.doublePool() != null) {
      initDoublePooled(size);
      for (int i = 0; i < size; i++) {
        double_accumulator[i] = Double.NaN;
      }
      Arrays.fill(double_accumulator, 0, size, Double.NaN);
    } else {
      double_accumulator = new double[size];
      Arrays.fill(double_accumulator, Double.NaN);
    }
    end = size;
  }
  
  protected void toDouble() {
    PooledObject double_pooled = null;
    if (factory.doublePool() != null) {
      double_pooled = factory.doublePool().claim(long_accumulator.length);
      double_accumulator = (double[]) double_pooled.object();
    } else {
      double_accumulator = new double[long_accumulator.length];
    }
    for (int i = 0; i < long_accumulator.length; i++) {
      double_accumulator[i] = long_accumulator[i];
    }
    long_accumulator = null;
    
    if (double_pooled != null) {
      pooled.release();
      pooled = double_pooled;
    }
  }
}