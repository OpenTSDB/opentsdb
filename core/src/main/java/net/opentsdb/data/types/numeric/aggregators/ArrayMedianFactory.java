// This file is part of OpenTSDB.
// Copyright (C) 2020  The OpenTSDB Authors.
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
 * Computes the median across the array.
 * 
 * TODO - <b>WARNING</b> Super memory and GC inefficient with the 2d array. We
 * should use a 1d with proper offsets and shifting.
 * 
 * @since 3.0
 */
public class ArrayMedianFactory extends BaseArrayFactoryWithIntPool {

  public static final String TYPE = "Median";
  
  @Override
  public String type() {
    return TYPE;
  }
  
  @Override
  public NumericArrayAggregator newAggregator() {
    return new ArrayMedian(false, this);
  }
  
  @Override
  public NumericArrayAggregator newAggregator(final AggregatorConfig config) {
    if (config != null && config instanceof NumericArrayAggregatorConfig) {
      return new ArrayMedian((NumericArrayAggregatorConfig) config, this);
    }
    return new ArrayMedian(false, this);
  }
  
  @Override
  public NumericArrayAggregator newAggregator(final boolean infectious_nan) {
    return new ArrayMedian(infectious_nan, this);
  }

  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
    setPools(tsdb);
    return Deferred.fromResult(null);
  }
  
  public static class ArrayMedian extends BaseArrayAggregatorWithIntPool {
    private static final int INIT_CAP = 8;
    
    private long[][] long_long_accumulator;
    private double[][] double_double_accumulator;
    
    public ArrayMedian(final boolean infectious_nans,
                        final BaseArrayFactory factory) {
      super(infectious_nans, factory);
    }
    
    public ArrayMedian(final NumericArrayAggregatorConfig config,
                        final BaseArrayFactory factory) {
      super(config, factory);
    }

    @Override
    public void accumulate(final long[] values, 
                           final int from, 
                           final int to) {
      if (double_double_accumulator == null && long_long_accumulator == null) {
        initLongAccumulator(to - from);
        
        int idx = from;
        for (int i = 0; i < long_long_accumulator.length; i++) {
          long_long_accumulator[i] = new long[INIT_CAP];
          long_long_accumulator[i][0] = values[idx++];
        }
        
        Arrays.fill(int_array, 0, to - from, 1);
        return;
      }
      if (long_long_accumulator != null) {
        if (to - from != long_long_accumulator.length) {
          throw new IllegalArgumentException("Values of length " 
              + (to - from) + " did not match the original lengh of " 
              + long_long_accumulator.length);
        }
        
        int idx = from;
        for (int i = from; i < (to - from); i++) {
          long[] ac = long_long_accumulator[i];
          if (int_array[i] + 1 > ac.length) {
            long[] temp = new long[ac.length * 2];
            System.arraycopy(ac, 0, temp, 0, ac.length);
            ac = temp;
            long_long_accumulator[i] = ac;
          }
          ac[int_array[i]++] = values[idx++];
        }
      } else {
        if (to - from != double_double_accumulator.length) {
          throw new IllegalArgumentException("Values of length " 
              + (to - from) + " did not match the original lengh of " 
              + double_double_accumulator.length);
        }
        int idx = from;
        for (int i = from; i < (to - from); i++) {
          double[] ac = double_double_accumulator[i];
          if (int_array[i] + 1 > ac.length) {
            double[] temp = new double[ac.length * 2];
            System.arraycopy(ac, 0, temp, 0, ac.length);
            ac = temp;
            double_double_accumulator[i] = ac;
          }
          ac[int_array[i]++] = values[idx++];
        }
      }
    }
    
    @Override
    public void accumulate(final double[] values, 
                           final int from, 
                           final int to) {
      if (double_double_accumulator == null && long_long_accumulator == null) {
        initDoubleAccumulator(to - from);
        
        int idx = from;
        for (int i = 0; i < double_double_accumulator.length; i++) {
          double_double_accumulator[i] = new double[INIT_CAP];
          if (Double.isNaN(values[idx])) {
            if (!infectious_nans) {
              idx++;
              continue;
            }
            
            if (int_array[i] < 0) {
              idx++;
              continue;
            }
            double_double_accumulator[i] = null;
            int_array[i] = -1;
            idx++;
            continue;
          }
          double_double_accumulator[i][0] = values[idx++];
          int_array[i]++;
        }
        
        return;
      } else if (double_double_accumulator == null) {
        longToDouble();
      }
      
      if (to - from != double_double_accumulator.length) {
        throw new IllegalArgumentException("Values of length " 
            + (to - from) + " did not match the original lengh of " 
            + double_double_accumulator.length);
      }
      
      int idx = from;
      for (int i = 0; i < (to - from); i++) {
        if (Double.isNaN(values[idx])) {
          if (!infectious_nans) {
            idx++;
            continue;
          }
          
          if (int_array[i] < 0) {
            idx++;
            continue;
          }
          double_double_accumulator[i] = null;
          int_array[i] = -1;
          idx++;
          continue;
        }
        
        double[] ac = double_double_accumulator[i];
        if (ac == null) {
          idx++;
          continue;
        }
        if (int_array[i] + 1 >= ac.length) {
          double[] temp = new double[ac.length * 2];
          System.arraycopy(ac, 0, temp, 0, ac.length);
          ac = temp;
          double_double_accumulator[i] = ac;
        }
        ac[int_array[i]++] = values[idx++];
      }
    }
    
    @Override
    public void accumulate(final double value, final int index) {
      if (long_long_accumulator == null && double_double_accumulator == null) {
        if (config == null || config.arraySize() < 1) {
          throw new IllegalStateException("The accumulator has not been initialized.");
        } else {
          initDoubleAccumulator(config.arraySize());
        }
      } else if (long_long_accumulator != null) {
        longToDouble();
      }
      
      if (index >= double_double_accumulator.length) {
        throw new IllegalArgumentException("Index [" + index 
            + "] is out of bounds [" + double_double_accumulator.length + "]");
      }
      
      if (Double.isNaN(value)) {
        if (!infectious_nans) {
          return;
        }
        
        if (int_array[index] < 0) {
          return;
        }
        double_double_accumulator[index] = null;
        int_array[index] = -1;
        return;
      } else if (int_array[index] < 0) {
        return;
      }
      
      if (double_double_accumulator[index] == null) {
        double_double_accumulator[index] = new double[INIT_CAP];
        double_double_accumulator[index][0] = value;
        int_array[index]++;
        return;
      }
      
      if (int_array[index] + 1 >= double_double_accumulator[index].length) {
        double[] ac = double_double_accumulator[index];
        double[] temp = new double[ac.length * 2];
        System.arraycopy(ac, 0, temp, 0, ac.length);
        ac = temp;
        double_double_accumulator[index] = ac;
      }
      double_double_accumulator[index][int_array[index]++] = value;
    }
    
    @Override
    public void combine(final NumericArrayAggregator aggregator) {
      if (!(aggregator instanceof ArrayMedian)) {
        throw new IllegalArgumentException("Cannot combine an aggregator of type " 
            + aggregator.getClass());
      }
      
      final ArrayMedian agg = (ArrayMedian) aggregator;
      if (agg.long_long_accumulator == null && agg.double_double_accumulator == null) {
        return;
      }
      
      // init
      if (long_long_accumulator == null && double_double_accumulator == null) {
        int length;
        if (agg.long_long_accumulator != null) {
          // longs
          length = agg.long_long_accumulator.length;
          long_long_accumulator = new long[length][];
          for (int i = 0; i < length; i++) {
            long_long_accumulator[i] = Arrays.copyOf(agg.long_long_accumulator[i], 
                length);
          }
        } else {
          length = agg.double_double_accumulator.length;
          double_double_accumulator = new double[length][];
          for (int i = 0; i < length; i++) {
            if (agg.double_double_accumulator[i] == null) {
              continue;
            }
            double_double_accumulator[i] = Arrays.copyOf(agg.double_double_accumulator[i], 
                length);
          }
        }
        
        if (((BaseArrayFactoryWithIntPool) factory).intPool() != null) {
          int_pooled = ((BaseArrayFactoryWithIntPool) factory).intPool().claim(length);
          int_array = (int[]) int_pooled.object();
          Arrays.fill(int_array, 0);
        } else {
          int_array = new int[length];
        }
        System.arraycopy(agg.int_array, 0, int_array, 0, int_array.length);
      } else if (agg.long_long_accumulator != null) {
        // remote longs
        if (long_long_accumulator != null) {
          if (long_long_accumulator.length != agg.long_long_accumulator.length) {
            throw new IllegalArgumentException("Incoming agg with length " 
                + agg.long_long_accumulator.length + " is not the same as the "
                    + "local " + long_long_accumulator.length);
          }
          
          for (int i = 0; i < agg.long_long_accumulator.length; i++) {
            long[] remote = agg.long_long_accumulator[i];
            long[] ac = long_long_accumulator[i];
            if (int_array[i] + agg.int_array[i] >= ac.length) {
              long[] temp = new long[int_array[i] + agg.int_array[i]];
              System.arraycopy(ac, 0, temp, 0, int_array[i]);
              ac = temp;
              long_long_accumulator[i] = ac;
            }
            System.arraycopy(remote, 0, ac, int_array[i], agg.int_array[i]);
            int_array[i] += agg.int_array[i];
          }
        } else {
          if (double_double_accumulator.length != agg.long_long_accumulator.length) {
            throw new IllegalArgumentException("Incoming agg with length " 
                + agg.long_long_accumulator.length + " is not the same as the "
                    + "local " + double_double_accumulator.length);
          }
          
          for (int i = 0; i < agg.long_long_accumulator.length; i++) {
            long[] remote = agg.long_long_accumulator[i];
            double[] ac = double_double_accumulator[i];
            if (ac == null) {
              continue;
            }
            
            if (int_array[i] + agg.int_array[i] >= ac.length) {
              double[] temp = new double[int_array[i] + agg.int_array[i]];
              System.arraycopy(ac, 0, temp, 0, int_array[i]);
              ac = temp;
              double_double_accumulator[i] = ac;
            }
            
            int idx = 0;
            for (int x = 0; x < agg.int_array[i]; x++) {
              ac[int_array[i]++] = remote[idx++];
            }
          }
        }
      } else {
        // doubles remotely
        if ((double_double_accumulator != null ? double_double_accumulator.length : 
            long_long_accumulator.length)!= agg.double_double_accumulator.length) {
          throw new IllegalArgumentException("Incoming agg with length " 
              + agg.double_double_accumulator.length + " is not the same as the "
              + "local " + double_double_accumulator.length);
        }
        
        if (long_long_accumulator != null) {
          longToDouble();
        }
        
        for (int i = 0; i < agg.double_double_accumulator.length; i++) {
          double[] remote = agg.double_double_accumulator[i];
          if (remote == null) {
            // nan'd
            int_array[i] = -1;
            double_double_accumulator[i] = null;
            continue;
          }
          double[] ac = double_double_accumulator[i];
          if (ac == null) {
            // nan'd
            continue;
          }
          if (int_array[i] + agg.int_array[i] >= ac.length) {
            double[] temp = new double[int_array[i] + agg.int_array[i]];
            System.arraycopy(ac, 0, temp, 0, int_array[i]);
            ac = temp;
            double_double_accumulator[i] = ac;
          }
          System.arraycopy(remote, 0, ac, int_array[i], agg.int_array[i]);
          int_array[i] += agg.int_array[i];
        }
      }
    }
    
    @Override
    public boolean isInteger() {
      return long_long_accumulator != null || long_accumulator != null;
    }
    
    @Override
    public long[] longArray() {
      if (long_long_accumulator == null && long_accumulator == null) {
        throw new UnsupportedOperationException("It's a double.");
      }
      if (long_accumulator == null) {
        initLong(long_long_accumulator.length);
        for (int i = 0; i < long_long_accumulator.length; i++) {
          if (int_array[i] < 1) {
            throw new IllegalStateException("Shouldn't be here, empty array at " + i);
          } else if (int_array[i] < 2) {
            long_accumulator[i] = long_long_accumulator[i][0];
          } else {
            Arrays.parallelSort(long_long_accumulator[i], 0, int_array[i]);
            long_accumulator[i] = long_long_accumulator[i][int_array[i] / 2];
          }
        }
        end = long_long_accumulator.length;
        long_long_accumulator = null;
      }
      
      return long_accumulator;
    }
    
    @Override
    public double[] doubleArray() {
      if (double_double_accumulator == null && double_accumulator == null) {
        throw new UnsupportedOperationException("It's a long.");
      }
      
      if (double_accumulator == null) {
        initDouble(double_double_accumulator.length);
        for (int i = 0; i < double_double_accumulator.length; i++) {
          if (int_array[i] < 1) {
            double_accumulator[i] = Double.NaN;
          } else if (int_array[i] < 2) {
            double_accumulator[i] = double_double_accumulator[i][0];
          } else {
            Arrays.parallelSort(double_double_accumulator[i], 0, int_array[i]);
            double_accumulator[i] = double_double_accumulator[i][int_array[i] / 2];
          }
        }
        end = double_double_accumulator.length;
        double_double_accumulator = null;
      }
      return double_accumulator;
    }
    
    @Override
    public int end() {
      if (end > 0) {
        return end;
      }
      if (long_long_accumulator == null && double_double_accumulator == null) {
        return 0;
      }
      return long_long_accumulator != null ? long_long_accumulator.length :
        double_double_accumulator.length;
    }
    
    @Override
    public void close() {
      super.close();
      long_long_accumulator = null;
      double_double_accumulator = null;
    }
    
    @Override
    public String name() {
      return ArrayMedianFactory.TYPE;
    }
  
    private void longToDouble() {
      double_double_accumulator = new double[long_long_accumulator.length][];
      for (int i = 0; i < long_long_accumulator.length; i++) {
        long[] lac = long_long_accumulator[i];
        double[] ac = new double[lac.length];
        for (int x = 0; x < lac.length; x++) {
          ac[x] = lac[x];
        }
        double_double_accumulator[i] = ac;
      }
      long_long_accumulator = null;
    }
    
    private void initLongAccumulator(final int length) {
      long_long_accumulator = new long[length][];
      if (((BaseArrayFactoryWithIntPool) factory).intPool() != null) {
        int_pooled = ((BaseArrayFactoryWithIntPool) factory).intPool().claim(length);
        int_array = (int[]) int_pooled.object();
        Arrays.fill(int_array, 0);
      } else {
        int_array = new int[length];
      }
    }
    
    private void initDoubleAccumulator(final int length) {
      double_double_accumulator = new double[length][];
      if (((BaseArrayFactoryWithIntPool) factory).intPool() != null) {
        int_pooled = ((BaseArrayFactoryWithIntPool) factory).intPool().claim(length);
        int_array = (int[]) int_pooled.object();
        Arrays.fill(int_array, 0);
      } else {
        int_array = new int[length];
      }
    }
  }
}