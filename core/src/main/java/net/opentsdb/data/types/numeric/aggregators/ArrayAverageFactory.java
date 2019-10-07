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

import com.google.common.base.Strings;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.data.AggregatorConfig;

/**
 * Computes the average across the array. Returns a double array always.
 * 
 * @since 3.0
 */
public class ArrayAverageFactory extends BaseArrayFactory {

  public static final String TYPE = "Avg";
  
  @Override
  public String type() {
    return TYPE;
  }
  
  @Override
  public NumericArrayAggregator newAggregator() {
    return new ArrayAverage(false);
  }
  
  @Override
  public NumericArrayAggregator newAggregator(final AggregatorConfig config) {
    if (config != null && config instanceof NumericAggregatorConfig) {
      return new ArrayAverage(((NumericAggregatorConfig) config).infectiousNan());
    }
    return new ArrayAverage(false);
  }
  
  @Override
  public NumericArrayAggregator newAggregator(final boolean infectious_nan) {
    return new ArrayAverage(infectious_nan);
  }

  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
    tsdb.getRegistry().registerPlugin(NumericArrayAggregatorFactory.class, 
        "average", this);
    return Deferred.fromResult(null);
  }
  
  public static class ArrayAverage extends BaseArrayAggregator {

    protected int[] counts;
    protected double[] results;
    
    public ArrayAverage(final boolean infectious_nans) {
      super(infectious_nans);
    }

    @Override
    public void accumulate(final long[] values, 
                           final int from, 
                           final int to) {
      if (double_accumulator == null && long_accumulator == null) {
        long_accumulator = Arrays.copyOfRange(values, from, to);
        counts = new int[to - from];
        Arrays.fill(counts, 1);
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
          counts[idx]++;
          long_accumulator[idx++] += values[i];
        }
      } else {
        if (to - from != double_accumulator.length) {
          throw new IllegalArgumentException("Values of length " 
              + (to - from) + " did not match the original lengh of " 
              + double_accumulator.length);
        }
        int idx = 0;
        for (int i = from; i < to; i++) {
          counts[idx]++;
          double_accumulator[idx++] += values[i];
        }
      }
    }

    @Override
    public void accumulate(double value, int idx) {
      if (Double.isNaN(value)) {
        if (infectious_nans && !Double.isNaN(double_accumulator[idx])) {
          double_accumulator[idx] = Double.NaN;
          counts[idx] = 0;
        }
      } else {
        if (!infectious_nans || !Double.isNaN(double_accumulator[idx])) {
          if (Double.isNaN(double_accumulator[idx])) {
            double_accumulator[idx] = value;
          } else {
            double_accumulator[idx] += value;
          }
          counts[idx]++;
        }
      }
    }

    @Override
    public void accumulate(final double[] values, 
                           final int from, 
                           final int to) {
      if (double_accumulator == null && long_accumulator == null) {
        double_accumulator = Arrays.copyOfRange(values, from, to);
        counts = new int[double_accumulator.length];

        for (int i = 0; i < double_accumulator.length; i++) {
          if(!Double.isNaN(double_accumulator[i])){
            counts[i] = 1;
          }
        }
        return;
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
        accumulate(values[i], idx);
        idx++;
      }
    }

    @Override
    public void combine(NumericArrayAggregator aggregator) {
      ArrayAverage arrayAverage = (ArrayAverage) aggregator;
      double[] double_accumulator = arrayAverage.double_accumulator;
      long[] long_accumulator = arrayAverage.long_accumulator;
      int[] counts = arrayAverage.counts;

      if (double_accumulator != null) {
        combine(double_accumulator, counts);
      }
      if (long_accumulator != null) {
        combine(long_accumulator, counts);
      }
    }

    private void combine(long[] values, int[] counts) {
      if (this.long_accumulator == null) {
        this.long_accumulator = Arrays.copyOfRange(values, 0, values.length);
        this.counts = Arrays.copyOfRange(counts, 0, counts.length);
      } else {
        for (int i = 0; i < values.length; i++) {
          this.long_accumulator[i] += values[i];
          this.counts[i] += counts[i];
        }
      }
    }

    private void combine(double[] values, int[] counts) {
      if (this.double_accumulator == null) {
        this.double_accumulator = Arrays.copyOfRange(values, 0, values.length);
        this.counts = Arrays.copyOfRange(counts, 0, counts.length);
      } else {
        for (int i = 0; i < values.length; i++) {
          double value = values[i];
          if (Double.isNaN(value)) {
            if (infectious_nans && !Double.isNaN(this.double_accumulator[i])) {
              this.double_accumulator[i] = Double.NaN;
              this.counts[i] = 0;
            }
          } else {
            if (!infectious_nans || !Double.isNaN(double_accumulator[i])) {
              if (Double.isNaN(double_accumulator[i])) {
                double_accumulator[i] = value;
              } else {
                double_accumulator[i] += value;
              }
              this.counts[i] += counts[i];
            }
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
        results = new double[counts.length];
        for (int i = 0; i < counts.length; i++) {
          // zero / zero will give us NaN.
          if (long_accumulator != null) {
            results[i] = (double) long_accumulator[i] / (double) counts[i];
          } else {
            results[i] = double_accumulator[i] / (double) counts[i];
          }
        }
        // allow the other arrays to be free.
        long_accumulator = null;
        double_accumulator = null;
      }
      return results;
    }
    
    @Override
    public int end() {
      return counts.length;
    }

    
    @Override
    public String name() {
      return ArrayAverageFactory.TYPE;
    }
    
  }
}
