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

import com.google.common.base.Strings;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.data.AggregatorConfig;

/**
 * Returns a Sum array aggregator.
 * 
 * @since 3.0
 */
public class ArraySumFactory extends BaseArrayFactory {

  public static final String TYPE = "Sum";
  
  @Override
  public NumericArrayAggregator newAggregator() {
    return new ArraySum(false, this);
  }
  
  @Override
  public NumericArrayAggregator newAggregator(final AggregatorConfig config) {
    if (config != null && config instanceof NumericArrayAggregatorConfig) {
      return new ArraySum((NumericArrayAggregatorConfig) config, this);
    }
    return new ArraySum(false, this);
  }
  
  @Override
  public NumericArrayAggregator newAggregator(final boolean infectious_nan) {
    return new ArraySum(infectious_nan, this);
  }
  
  @Override
  public String type() {
    return TYPE;
  }
  
  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
    tsdb.getRegistry().registerPlugin(NumericArrayAggregatorFactory.class, 
        "ZimSum", this);
    setPools(tsdb);
    return Deferred.fromResult(null);
  }
  
  public static class ArraySum extends BaseArrayAggregator {

    public ArraySum(final boolean infectious_nans, 
                    final BaseArrayFactory factory) {
      super(infectious_nans, factory);
    }
    
    public ArraySum(final NumericArrayAggregatorConfig config,
                    final BaseArrayFactory factory) {
      super(config, factory);
    }
    
    @Override
    public void accumulate(final long[] values,
                           final int from,
                           final int to) {
      if (double_accumulator == null && long_accumulator == null) {
        initLong(values, from, to);
        return;
      }
      
      if (long_accumulator != null) {
        if (to - from != end) {
          throw new IllegalArgumentException("Values of length " 
              + (to - from) + " did not match the original lengh of " + end);
        }
        int idx = 0;
        for (int i = from; i < to; i++) {
          long_accumulator[idx++] += values[i];
        }
      } else {
        if (to - from != end) {
          throw new IllegalArgumentException("Values of length " 
              + (to - from) + " did not match the original lengh of " + end);
        }
        int idx = 0;
        for (int i = from; i < to; i++) {
          if (Double.isNaN(double_accumulator[idx]) && !infectious_nans) {
            double_accumulator[idx++] = values[i];
          } else {
            double_accumulator[idx++] += values[i];
          }
        }
      }
    }
    
    @Override
    public void accumulate(final double[] values, 
                           final int from, 
                           final int to) {
      if (double_accumulator == null && long_accumulator == null) {
        initDouble(values, from, to);
        return;
      }
      
      if (double_accumulator == null) {
        toDouble();
      }
      
      if (to - from != end) {
        throw new IllegalArgumentException("Values of length "
            + (to - from) + " did not match the original lengh of " + end);
      }
      
      int idx = 0;
      for (int i = from; i < to; i++) {
        if (Double.isNaN(values[i])) {
          if (infectious_nans) {
            double_accumulator[idx++] += values[i];
          } else {
            idx++;
          }
        } else {
          if (Double.isNaN(double_accumulator[idx]) && !infectious_nans) {
            double_accumulator[idx++] = values[i];
          } else {
            double_accumulator[idx++] += values[i];
          }
        }
      }
    }
    
    @Override
    public void accumulate(final double value, final int index) {
      if (long_accumulator == null && double_accumulator == null) {
        if (config == null || config.arraySize() < 1) {
          throw new IllegalStateException("The accumulator has not been initialized.");
        } else {
          initDouble(config.arraySize());
        }
      } else if (long_accumulator != null) {
        toDouble();
      }
      
      if (index >= end) {
        throw new IllegalArgumentException("Index [" + index 
            + "] is out of bounds [" + end + "]");
      }
      
      if (Double.isNaN(value)) {
        if (infectious_nans) {
          double_accumulator[index] = Double.NaN;
        }
      } else {
        if (Double.isNaN(double_accumulator[index]) && !infectious_nans) {
          double_accumulator[index] = value;
        } else {
          double_accumulator[index] += value;
        }
      }
    }
    
    @Override
    public String name() {
      return ArraySumFactory.TYPE;
    }
    
  }
  
}