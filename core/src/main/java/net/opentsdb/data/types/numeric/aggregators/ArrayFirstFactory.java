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
package net.opentsdb.data.types.numeric.aggregators;

import java.util.Arrays;

import com.google.common.base.Strings;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.data.AggregatorConfig;

/**
 * Returns the first values honoring infectious NaN.
 * 
 * @since 3.0
 */
public class ArrayFirstFactory extends BaseArrayFactory {

  public static final String TYPE = "First";
  
  @Override
  public NumericArrayAggregator newAggregator() {
    return new ArrayFirst(false);
  }
  
  @Override
  public NumericArrayAggregator newAggregator(final AggregatorConfig config) {
    if (config != null && config instanceof NumericAggregatorConfig) {
      return new ArrayFirst(((NumericAggregatorConfig) config).infectiousNan());
    }
    return new ArrayFirst(false);
  }
  
  @Override
  public NumericArrayAggregator newAggregator(final boolean infectious_nan) {
    return new ArrayFirst(infectious_nan);
  }

  @Override
  public String type() {
    return TYPE;
  }

  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
    return Deferred.fromResult(null);
  }
  
  public static class ArrayFirst extends BaseArrayAggregator {

    public ArrayFirst(final boolean infectious_nans) {
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
      
      if (long_accumulator == null) {
        if (to - from != double_accumulator.length) {
          throw new IllegalArgumentException("Values of length " 
              + (to - from) + " did not match the original lengh of " 
              + double_accumulator.length);
        }
        int idx = 0;
        for (int i = from; i < to; i++) {
          if (Double.isNaN(double_accumulator[idx])) {
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
        return;
      }
      
      if (double_accumulator == null) {
        // no-op as it means we had long values.
        return;
      }
      
      if (to - from != double_accumulator.length) {
        throw new IllegalArgumentException("Values of length " 
            + (to - from) + " did not match the original lengh of " 
            + double_accumulator.length);
      }
      
      int idx = 0;
      for (int i = from; i < to; i++) {
        if (Double.isNaN(double_accumulator[idx]) && !infectious_nans) {
          double_accumulator[idx] = values[i];
        }
        idx++;
      }
    }
    
    @Override
    public String name() {
      return ArrayFirstFactory.TYPE;
    }
  }
}
