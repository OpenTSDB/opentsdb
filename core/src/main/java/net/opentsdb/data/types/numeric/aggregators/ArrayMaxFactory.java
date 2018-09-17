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

import java.util.Arrays;

import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;

/**
 * Just the max.
 * 
 * @since 3.0
 */
public class ArrayMaxFactory extends BaseArrayFactory {

  @Override
  public NumericArrayAggregator newAggregator(final boolean infectious_nan) {
    return new ArrayMax(infectious_nan);
  }

  @Override
  public String id() {
    return "max";
  }

  @Override
  public Deferred<Object> initialize(TSDB tsdb) {
    tsdb.getRegistry().registerPlugin(NumericArrayAggregatorFactory.class, 
        "mimmax", this);
    return Deferred.fromResult(null);
  }
  
  public static class ArrayMax extends BaseArrayAggregator {

    public ArrayMax(final boolean infectious_nans) {
      super(infectious_nans);
    }

    @Override
    public void accumulate(final long[] values, 
                           final int from, 
                           final int to) {
      if (double_accumulator == null && long_accumulator == null) {
        long_accumulator = Arrays.copyOfRange(values, from, to);
        return;
      }
      
      if (long_accumulator != null) {
        if (to - from != long_accumulator.length) {
          throw new IllegalArgumentException("Values of length " 
              + (to - from) + " did not match the original lengh of " 
              + long_accumulator.length);
        }
        int idx = 0;
        for (int i = from; i < to; i++) {
          if (values[i] > long_accumulator[idx]) {
            long_accumulator[idx] = values[i];
          }
          idx++;
        }
      } else {
        if (to - from != double_accumulator.length) {
          throw new IllegalArgumentException("Values of length " 
              + (to - from) + " did not match the original lengh of " 
              + double_accumulator.length);
        }
        int idx = 0;
        for (int i = from; i < to; i++) {
          if (values[i] > double_accumulator[idx]) {
            double_accumulator[idx] = values[i];
          }
          idx++;
        }
      }
    }

    @Override
    public void accumulate(final double[] values, 
                           final int from, 
                           final int to) {
      if (double_accumulator == null && long_accumulator == null) {
        double_accumulator = Arrays.copyOfRange(values, from, to);
      }
      
      if (double_accumulator == null) {
        double_accumulator = new double[long_accumulator.length];
        for (int i = 0; i < long_accumulator.length; i++) {
          double_accumulator[i] = long_accumulator[i];
        }
        long_accumulator = null;
      }
      
      if (to - from != double_accumulator.length) {
        throw new IllegalArgumentException("Values of length " 
            + (to - from) + " did not match the original lengh of " 
            + double_accumulator.length);
      }
      
      int idx = 0;
      for (int i = from; i < to; i++) {
        if (Double.isNaN(values[i])) {
          if (infectious_nans) {
            double_accumulator[idx++] = values[i];
          } else {
            idx++;
          }
        } else {
          if (Double.isNaN(double_accumulator[idx]) && !infectious_nans) {
            double_accumulator[idx] = values[i];
          } else if (values[i] > double_accumulator[idx]) {
            double_accumulator[idx] = values[i];
          }
          idx++;
        }
      }
    }
    
  }
}
