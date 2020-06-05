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

import java.util.Arrays;

import com.google.common.base.Strings;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.data.AggregatorConfig;
import net.opentsdb.pools.ArrayObjectPool;
import net.opentsdb.pools.IntArrayPool;
import net.opentsdb.pools.PooledObject;

/**
 * Computes the average across the array. Returns a double array always.
 * 
 * @since 3.0
 */
public class ArrayAverageFactory extends BaseArrayFactoryWithIntPool {

  public static final String TYPE = "Avg";
  
  @Override
  public String type() {
    return TYPE;
  }
  
  @Override
  public NumericArrayAggregator newAggregator() {
    return new ArrayAverage(false, this);
  }
  
  @Override
  public NumericArrayAggregator newAggregator(final AggregatorConfig config) {
    if (config != null && config instanceof NumericArrayAggregatorConfig) {
      return new ArrayAverage((NumericArrayAggregatorConfig) config, this);
    }
    return new ArrayAverage(false, this);
  }
  
  @Override
  public NumericArrayAggregator newAggregator(final boolean infectious_nan) {
    return new ArrayAverage(infectious_nan, this);
  }

  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
    tsdb.getRegistry().registerPlugin(NumericArrayAggregatorFactory.class, 
        "average", this);
    setPools(tsdb);
    int_pool = (ArrayObjectPool) tsdb.getRegistry().getObjectPool(IntArrayPool.TYPE);
    return Deferred.fromResult(null);
  }
  
  public static class ArrayAverage extends BaseArrayAggregatorWithIntPool {
    
    protected double[] results;
    
    public ArrayAverage(final boolean infectious_nans,
                        final BaseArrayFactory factory) {
      super(infectious_nans, factory);
    }
    
    public ArrayAverage(final NumericArrayAggregatorConfig config,
                        final BaseArrayFactory factory) {
      super(config, factory);
    }

    @Override
    public void accumulate(final long[] values, 
                           final int from, 
                           final int to) {
      if (results != null) {
        throw new IllegalStateException("Accumulator cannot accumulate after "
            + "calling doubleArray().");
      }
      
      if (double_accumulator == null && long_accumulator == null) {
        initLong(values, from, to);
        
        if (((BaseArrayFactoryWithIntPool) factory).intPool() != null) {
          int_pooled = ((BaseArrayFactoryWithIntPool) factory).intPool().claim(to - from);
          int_array = (int[]) int_pooled.object();
        } else {
          int_array = new int[to - from];
        }
        
        Arrays.fill(int_array, 0, end, 1);
        return;
      }
      
      if (long_accumulator != null) {
        if (to - from != end) {
          throw new IllegalArgumentException("Values of length " 
              + (to - from) + " did not match the original lengh of " + end);
        }
        int idx = 0;
        for (int i = from; i < to; i++) {
          int_array[idx]++;
          long_accumulator[idx++] += values[i];
        }
      } else {
        if (to - from != end) {
          throw new IllegalArgumentException("Values of length " 
              + (to - from) + " did not match the original lengh of " + end);
        }
        int idx = 0;
        for (int i = from; i < to; i++) {
          int_array[idx]++;
          double_accumulator[idx++] += values[i];
        }
      }
    }
    
    @Override
    public void accumulate(final double[] values, 
                           final int from, 
                           final int to) {
      if (results != null) {
        throw new IllegalStateException("Accumulator cannot accumulate after "
            + "calling doubleArray().");
      }
      
      if (double_accumulator == null && long_accumulator == null) {
        initDouble(values, from, to);
        
        if (((BaseArrayFactoryWithIntPool) factory).intPool() != null) {
          int_pooled = ((BaseArrayFactoryWithIntPool) factory).intPool().claim(to - from);
          int_array = (int[]) int_pooled.object();
        } else {
          int_array = new int[to - from];
        }
        
        for (int i = 0; i < double_accumulator.length; i++) {
          if (!Double.isNaN(double_accumulator[i])){
            int_array[i] = 1;
          } else {
            int_array[i] = 0;
          }
        }
        return;
      } else if (double_accumulator == null) {
        toDouble();
      }
      
      if (to - from != end) {
        throw new IllegalArgumentException("Values of length " 
            + (to - from) + " did not match the original lengh of " + end);
      }
      
      int idx = 0;
      for (int i = from; i < to; i++) {
        accumulate(values[i], idx);
        idx++;
      }
    }
    
    @Override
    public void accumulate(final double value, final int idx) {
      if (results != null) {
        throw new IllegalStateException("Accumulator cannot accumulate after "
            + "calling doubleArray().");
      }
      
      if (long_accumulator == null && double_accumulator == null) {
        if (config == null || config.arraySize() < 1) {
          throw new IllegalStateException("The accumulator has not been initialized.");
        } else {
          initDouble(config.arraySize());
          if (((BaseArrayFactoryWithIntPool) factory).intPool() != null) {
            int_pooled = 
                ((BaseArrayFactoryWithIntPool) factory).intPool().claim(config.arraySize());
            int_array = (int[]) int_pooled.object();
            Arrays.fill(int_array, 0);
          } else {
            int_array = new int[config.arraySize()];
          }
        }
      } else if (long_accumulator != null) {
        toDouble();
      }
      
      if (idx >= end) {
        throw new IllegalArgumentException("Index [" + idx 
            + "] is out of bounds [" + end + "]");
      }
      
      if (Double.isNaN(value)) {
        if (infectious_nans && !Double.isNaN(double_accumulator[idx])) {
          double_accumulator[idx] = Double.NaN;
          int_array[idx] = 0;
        }
      } else {
        if (!infectious_nans || !Double.isNaN(double_accumulator[idx])) {
          if (Double.isNaN(double_accumulator[idx])) {
            double_accumulator[idx] = value;
          } else {
            double_accumulator[idx] += value;
          }
          int_array[idx]++;
        }
      }
    }
    
    @Override
    public void combine(final NumericArrayAggregator aggregator) {
      if (results != null) {
        throw new IllegalStateException("Accumulator cannot accumulate after "
            + "calling doubleArray().");
      }
      
      final ArrayAverage arrayAverage = (ArrayAverage) aggregator;
      if (arrayAverage.double_accumulator != null) {
        combine(arrayAverage.double_accumulator, arrayAverage.int_array, arrayAverage.end);
      }
      if (arrayAverage.long_accumulator != null) {
        combine(arrayAverage.long_accumulator, arrayAverage.int_array, arrayAverage.end);
      }
    }

    private void combine(final long[] values, final int[] int_array, final int end) {
      if (long_accumulator == null && double_accumulator == null) {
        initLong(values, 0, end);
        if (((BaseArrayFactoryWithIntPool) factory).intPool() != null) {
          int_pooled = ((BaseArrayFactoryWithIntPool) factory).intPool().claim(end);
          this.int_array = (int[]) int_pooled.object();
          System.arraycopy(int_array, 0, this.int_array, 0, end);
        } else {
          this.int_array = Arrays.copyOf(int_array, end);
        }
      } else if (long_accumulator != null) {
        if (this.end != end) {
          throw new IllegalArgumentException("Values of length " 
              + end + " did not match the original lengh of " + this.end);
        }
        
        for (int i = 0; i < end; i++) {
          this.long_accumulator[i] += values[i];
          this.int_array[i] += int_array[i];
        }
      } else {
        if (this.end != end) {
          throw new IllegalArgumentException("Values of length " 
              + end + " did not match the original lengh of " + this.end);
        }
        
        for (int i = 0; i < end; i++) {
          this.double_accumulator[i] += values[i];
          this.int_array[i] += int_array[i];
        }
      }
    }

    private void combine(final double[] values, 
                         final int[] int_array, 
                         final int end) {
      if (double_accumulator == null && long_accumulator == null) {
        initDouble(values, 0, end);
        
        if (((BaseArrayFactoryWithIntPool) factory).intPool() != null) {
          int_pooled = 
              ((BaseArrayFactoryWithIntPool) factory).intPool().claim(int_array.length);
          this.int_array = (int[]) int_pooled.object();
          System.arraycopy(int_array, 0, this.int_array, 0, end);
        } else {
          this.int_array = Arrays.copyOf(int_array, end);
        }
        return;
      } 
      
      if (this.end != end) {
        throw new IllegalArgumentException("Values of length " 
            + end + " did not match the original lengh of " + this.end);
      }
      
      if (long_accumulator != null) {
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
      
      for (int i = 0; i < end; i++) {
        final double value = values[i];
        if (Double.isNaN(value)) {
          if (infectious_nans && !Double.isNaN(this.double_accumulator[i])) {
            this.double_accumulator[i] = Double.NaN;
            this.int_array[i] = 0;
          }
        } else {
          if (!infectious_nans || !Double.isNaN(double_accumulator[i])) {
            if (Double.isNaN(double_accumulator[i])) {
              double_accumulator[i] = value;
            } else {
              double_accumulator[i] += value;
            }
            this.int_array[i] += int_array[i];
          }
        }
      }
    }

    @Override
    public boolean isInteger() {
      return false;
    }
    
    @Override
    public long[] longArray() {
      return null;
    }
    
    @Override
    public double[] doubleArray() {
      if (results == null) {
        PooledObject double_pooled = null;
        if (double_accumulator != null) {
          // reuse when we can.
          results = double_accumulator;
        } else if (factory.doublePool() != null) {
          double_pooled = factory.doublePool().claim(end);
          results = (double[]) double_pooled.object();
        } else {
          results = new double[int_array.length];
        }
        
        for (int i = 0; i < int_array.length; i++) {
          // zero / zero will give us NaN.
          if (long_accumulator != null) {
            results[i] = (double) long_accumulator[i] / (double) int_array[i];
          } else {
            results[i] = double_accumulator[i] / (double) int_array[i];
          }
        }
        
        // allow the other arrays to be free.
        long_accumulator = null;
        double_accumulator = null;
        if (int_pooled != null) {
          int_pooled.release();
          int_pooled = null;
        }
        if (double_pooled != null) {
          pooled.release();
          pooled = double_pooled;
        }
      }
      return results;
    }
    
    @Override
    public String name() {
      return ArrayAverageFactory.TYPE;
    }
    
    @Override
    public void close() {
      super.close();
      if (int_pooled != null) {
        int_pooled.release();
        int_pooled = null;
      }
      int_array = null;
    }
  }
}