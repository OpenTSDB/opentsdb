// This file is part of OpenTSDB.
// Copyright (C) 2019-2020  The OpenTSDB Authors.
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

/**
 * Just the count.
 * <b>Note:</b> This aggregator will always return a long array and ignores
 * infectious NaNs, treating NaNs as non-values.
 * 
 * @since 3.0
 */
public class ArrayCountFactory extends BaseArrayFactory {

  public static final String TYPE = "Count";
  
  @Override
  public NumericArrayAggregator newAggregator() {
    return new ArrayCount(false, this);
  }
  
  @Override
  public NumericArrayAggregator newAggregator(final AggregatorConfig config) {
    if (config != null && config instanceof NumericArrayAggregatorConfig) {
      return new ArrayCount((NumericArrayAggregatorConfig) config, this);
    }
    return new ArrayCount(false, this);
  }
  
  @Override
  public NumericArrayAggregator newAggregator(final boolean infectious_nan) {
    return new ArrayCount(infectious_nan, this);
  }

  @Override
  public String type() {
    return TYPE;
  }

  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
    setPools(tsdb);
    return Deferred.fromResult(null);
  }
  
  public static class ArrayCount extends BaseArrayAggregator {

    public ArrayCount(final boolean infectious_nans,
                      final BaseArrayFactory factory) {
      super(infectious_nans, factory);
    }
    
    public ArrayCount(final NumericArrayAggregatorConfig config,
                      final BaseArrayFactory factory) {
      super(config, factory);
    }
    
    @Override
    public void accumulate(final long[] values, 
                           final int from, 
                           final int to) {
      if (long_accumulator == null) {
        if (factory.longPool() != null) {
          pooled = factory.longPool().claim(to - from);
          long_accumulator = (long[]) pooled.object();
        } else {
          long_accumulator = new long[to - from];
        }
        Arrays.fill(long_accumulator, 0, to - from, 1);
        end = to - from;
        return;
      }
      
      if (to - from != end) {
        throw new IllegalArgumentException("Values of length " 
            + (to - from) + " did not match the original lengh of " + end);
      }
      int idx = 0;
      for (int i = from; i < to; i++) {
        long_accumulator[idx++]++;
      }
    }

    @Override
    public void accumulate(final double[] values, 
                           final int from, 
                           final int to) {
      if (long_accumulator == null) {
        if (factory.longPool() != null) {
          pooled = factory.longPool().claim(to - from);
          long_accumulator = (long[]) pooled.object();
          Arrays.fill(long_accumulator, 0, to - from, 0);
        } else {
          long_accumulator = new long[to - from];
        }
        end = to - from;
      }
      
      if (to - from != end) {
        throw new IllegalArgumentException("Values of length " 
            + (to - from) + " did not match the original lengh of " + end);
      }
      
      int idx = 0;
      for (int i = from; i < to; i++) {
        if(!Double.isNaN(values[i])) {
          long_accumulator[idx]++;
        }
        idx++;
      }
    }

    @Override
    public void accumulate(final double value, final int index) {
      if (long_accumulator == null && double_accumulator == null) {
        if (config == null || config.arraySize() < 1) {
          throw new IllegalStateException("The accumulator has not been initialized.");
        } else {
          if (factory.longPool() != null) {
            pooled = factory.longPool().claim(config.arraySize());
            long_accumulator = (long[]) pooled.object();
            Arrays.fill(long_accumulator, 0, config.arraySize(), 0);
          } else {
            long_accumulator = new long[config.arraySize()];
          }
          end = config.arraySize();
        }
      }
      
      if (index >= end) {
        throw new IllegalArgumentException("Index [" + index 
            + "] is out of bounds [" + end + "]");
      }
      
      if (!Double.isNaN(value)) {
        long_accumulator[index]++;
      }
    }

    /**
     * Hacky method to allow summing counts in another location and updating
     * it in this aggregator.
     * @param value The value to sum or compare to increment.
     * @param index The index into the array we need to update.
     * @param partialCounts Whether or not to treat the value as a count.
     */
    public void accumulate(final double value, 
                           final int index, 
                           final boolean partialCounts) {
      if (long_accumulator == null && double_accumulator == null) {
        if (config == null || config.arraySize() < 1) {
          throw new IllegalStateException("The accumulator has not been initialized.");
        } else {
          initLong(config.arraySize());
          if (pooled != null) {
            Arrays.fill(long_accumulator, 0, config.arraySize(), 0);
          }
        }
      }
      
      if (!Double.isNaN(value)) {
        if (partialCounts) {
          long_accumulator[index] += value;
        } else {
          long_accumulator[index]++;
        }
      }
    }

    @Override
    public void combine(final NumericArrayAggregator aggregator) {
      final BaseArrayAggregator agg = (BaseArrayAggregator) aggregator;
      if (long_accumulator == null && double_accumulator == null) {
        initLong(agg.long_accumulator, 0, agg.end);
        return;
      }
      
      for (int i = 0; i < agg.end; i++) {
        long_accumulator[i] += agg.long_accumulator[i];
      }
    }
    
    @Override
    public String name() {
      return ArrayCountFactory.TYPE;
    }
  }
}