// This file is part of OpenTSDB.
// Copyright (C) 202020  The OpenTSDB Authors.
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

import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.apache.commons.math3.stat.descriptive.rank.Percentile.EstimationType;

import com.google.common.base.Strings;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.data.AggregatorConfig;

/**
 * Instantiates a bunch of factories for various percentile functions, the same
 * we had in TSDB 2.x and {@link PercentilesFactories}. 
 * 
 * TODO - <b>WARNING</b> Super memory and GC inefficient with the 2d array. We
 * should use a 1d with proper offsets and shifting.
 * 
 * @since 3.0
 */
public class ArrayPercentileFactories extends BaseArrayFactory {
  static enum PercentileType {
    P999(99.9),
    P99(99.0),
    P95(95.0),
    P90(90.0),
    P75(75.0),
    P50(50.0),
    EP999R3(99.9, EstimationType.R_3),
    EP99R3(99.0, EstimationType.R_3),
    EP95R3(95.0, EstimationType.R_3),
    EP90R3(90.0, EstimationType.R_3),
    EP75R3(75.0, EstimationType.R_3),
    EP50R3(50.0, EstimationType.R_3),
    EP999R7(99.9, EstimationType.R_7),
    EP99R7(99.0, EstimationType.R_7),
    EP95R7(95.0, EstimationType.R_7),
    EP90R7(90.0, EstimationType.R_7),
    EP75R7(75.0, EstimationType.R_7),
    EP50R7(50.0, EstimationType.R_7);
    
    final double p;
    final EstimationType estimation_type;
    
    PercentileType(final double p) {
      this.p = p;
      estimation_type = null;
    }
    
    PercentileType(final double p, final EstimationType estimation_type) {
      this.p = p;
      this.estimation_type = estimation_type;
    }
  }
  
  private PercentileType percentile;
  
  @Override
  public NumericArrayAggregator newAggregator() {
    return new ArrayPercentile(false, this, percentile);
  }
  
  @Override
  public NumericArrayAggregator newAggregator(final AggregatorConfig config) {
    if (config != null && config instanceof NumericArrayAggregatorConfig) {
      return new ArrayPercentile((NumericArrayAggregatorConfig) config, 
          this, percentile);
    }
    return new ArrayPercentile(false, this, percentile);
  }
  
  @Override
  public NumericArrayAggregator newAggregator(final boolean infectious_nan) {
    return new ArrayPercentile(infectious_nan, this, percentile);
  }
  
  @Override
  public String type() {
    return percentile == null ? "BaseArrayPercentileFactory" : percentile.toString();
  }
  
  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.id = Strings.isNullOrEmpty(id) ? (percentile == null ? "BaseArrayPercentileFactory" : percentile.toString()) : id;
    for (final PercentileType type : PercentileType.values()) {
      ArrayPercentileFactories agg = new ArrayPercentileFactories();
      agg.percentile = type;
      agg.setPools(tsdb);      
      tsdb.getRegistry().registerPlugin(NumericArrayAggregatorFactory.class, 
          type.toString(), agg);
    }
    return Deferred.fromResult(null);
  }
  
  public static class ArrayPercentile extends BaseArrayAggregator {
    private static final int INIT_CAP = 8;
    
    // TODO - pool it!
    private double[][] accumulator;
    private int[] indices;
    private final PercentileType p;
    
    public ArrayPercentile(final boolean infectious_nans, 
                           final BaseArrayFactory factory,
                           final PercentileType percentile) {
      super(infectious_nans, factory);
      p = percentile;
    }
    
    public ArrayPercentile(final NumericArrayAggregatorConfig config,
                           final BaseArrayFactory factory,
                           final PercentileType percentile) {
      super(config, factory);
      p = percentile;
    }
    
    @Override
    public void accumulate(final long[] values,
                           final int from,
                           final int to) {
      if (accumulator == null) {
        accumulator = new double[to - from][];
        indices = new int[to - from];
        int idx = from;
        for (int i = 0; i < (to - from); i++) {
          accumulator[i] = new double[INIT_CAP];
          accumulator[i][0] = values[idx++];
          indices[i]++;
        }
        return;
      }
      
      if (to - from != accumulator.length) {
        throw new IllegalArgumentException("Incoming length [" + (to - from) 
            + "] is longer than [" + accumulator.length + "]");
      }
      
      int idx = from;
      for (int i = 0; i < (to - from); i++) {
        double[] ac = accumulator[i];
        if (ac == null) {
          idx++;
          continue;
        }
        if (indices[i] + 1 >= ac.length) {
          double[] temp = new double[ac.length * 2];
          System.arraycopy(ac, 0, temp, 0, ac.length);
          ac = temp;
          accumulator[i] = ac;
        }
        
        ac[indices[i]++] = values[idx++];
      }
    }
    
    @Override
    public void accumulate(final double[] values, 
                           final int from, 
                           final int to) {
      if (accumulator == null) {
        accumulator = new double[to - from][];
        indices = new int[to - from];
        int idx = from;
        for (int i = 0; i < (to - from); i++) {
          accumulator[i] = new double[INIT_CAP];
          if (Double.isNaN(values[idx])) {
            if (!infectious_nans) {
              idx++;
              continue;
            }
            
            if (indices[i] < 0) {
              idx++;
              continue;
            }
            accumulator[i] = null;
            indices[i] = -1;
            idx++;
            continue;
          }
          
          accumulator[i][0] = values[idx++];
          indices[i]++;
        }
        return;
      }
      
      if (to - from != accumulator.length) {
        throw new IllegalArgumentException("Incoming length [" + (to - from) 
            + "] is longer than [" + accumulator.length + "]");
      }
      
      int idx = from;
      for (int i = 0; i < (to - from); i++) {
        if (Double.isNaN(values[idx])) {
          if (!infectious_nans) {
            idx++;
            continue;
          }
          
          if (indices[i] < 0) {
            idx++;
            continue;
          }
          accumulator[i] = null;
          indices[i] = -1;
          idx++;
          continue;
        }
        
        double[] ac = accumulator[i];
        if (ac == null) {
          idx++;
          continue;
        }
        if (indices[i] + 1 >= ac.length) {
          double[] temp = new double[ac.length * 2];
          System.arraycopy(ac, 0, temp, 0, ac.length);
          ac = temp;
          accumulator[i] = ac;
        }
        
        ac[indices[i]++] = values[idx++];
      }
    }
    
    @Override
    public void accumulate(final double value, final int index) {
      if (accumulator == null) {
        if (config == null || config.arraySize() < 1) {
          throw new IllegalStateException("The accumulator has not been initialized.");
        } else {
          accumulator = new double[config.arraySize()][];
          indices = new int[config.arraySize()];
          for (int i = 0; i < config.arraySize(); i++) {
            accumulator[i] = new double[INIT_CAP];
          }
        }
      }
      
      if (index >= accumulator.length) {
        throw new IllegalArgumentException("Index [" + index 
            + "] is out of bounds [" + accumulator.length + "]");
      }
      
      if (Double.isNaN(value)) {
        if (!infectious_nans) {
          return;
        }
        
        if (indices[index] < 0) {
          return;
        }
        accumulator[index] = null;
        indices[index] = -1;
        return;
      } else {
        if (accumulator[index] == null) {
          return;
        }
        
        if (indices[index] + 1 >= accumulator[index].length) {
          double[] ac = accumulator[index];
          double[] temp = new double[ac.length * 2];
          System.arraycopy(ac, 0, temp, 0, ac.length);
          ac = temp;
          accumulator[index] = ac;
        }
        accumulator[index][indices[index]++] = value;
      }
    }
    
    @Override
    public double[] doubleArray() {
      if (double_accumulator == null) {
        if (accumulator == null) {
          throw new IllegalStateException("Aggregator didn't have any data.");
        }
        // run it!
        initDouble(accumulator.length);
        Percentile percentile = null;
        for (int i = 0; i < accumulator.length; i++) {
          if (indices[i] < 1) {
            double_accumulator[i] = Double.NaN;
          } else if (indices[i] == 1) {
            double_accumulator[i] = accumulator[i][0];
          } else {
            if (percentile == null) {
              percentile = p.estimation_type == null ?
                  new Percentile(p.p) :
                    new Percentile(p.p).withEstimationType(p.estimation_type);
              
            }
            percentile.setData(accumulator[i], 0, indices[i]);
            double_accumulator[i] = percentile.evaluate();
          }
        }
      }    
      return double_accumulator;
    }
    
    @Override
    public void combine(final NumericArrayAggregator aggregator) {
      if (!(aggregator instanceof ArrayPercentile)) {
        throw new IllegalArgumentException("Cannot combine an aggregator of type " 
            + aggregator.getClass());
      }
      
      final ArrayPercentile agg = (ArrayPercentile) aggregator;
      if (agg.accumulator == null) {
        return;
      }
      
      // TODO - We may be able to get away with simply using the ref to the first
      // array.
      if (accumulator == null) {
        accumulator = new double[agg.accumulator.length][];
        indices = new int[agg.accumulator.length];
        for (int i = 0; i < agg.accumulator.length; i++) {
          if (agg.accumulator[i] == null) {
            accumulator[i] = null;
            indices[i] = -1;
            continue;
          }
          accumulator[i] = Arrays.copyOf(agg.accumulator[i], agg.accumulator[i].length);
          indices[i] = agg.indices[i];
        }
      } else {
        for (int i = 0; i < agg.accumulator.length; i++) {
          if (agg.accumulator[i] == null) {
            indices[i] = -1;
            accumulator[i] = null;
            continue;
          } else if (accumulator[i] == null) {
            continue;
          }
          
          double[] ac = agg.accumulator[i];
          if (indices[i] + agg.indices[i] >= accumulator[i].length) {
            double[] temp = new double[accumulator[i].length * 2];
            System.arraycopy(accumulator[i], 0, temp, 0, indices[i]);
            ac = temp;
            accumulator[i] = ac;
          }
          
          System.arraycopy(ac, 0, accumulator[i], indices[i], agg.indices[i]);
          indices[i] += agg.indices[i];
        }
      }
    }
    
    @Override
    public boolean isInteger() {
      return false;
    }
    
    @Override
    public int end() {
      return accumulator == null ? 0 : accumulator.length;
    }
    
    @Override
    public void close() {
      super.close();
      accumulator = null;
    }
    
    @Override
    public String name() {
      return p.toString();
    }
    
  }
  
}