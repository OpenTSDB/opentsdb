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

import com.google.common.base.Strings;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.AggregatorConfig;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.exceptions.IllegalDataException;

/**
 * Computes the exponential moving average using the values between the two 
 * offsets. It uses a provided alpha or the default calculated alpha based on 
 * the number of values. In the future we could add an efficient implementation 
 * of something like the Marquardt procedure but for now we'll use the Panda's 
 * method: https://pandas.pydata.org/pandas-docs/version/0.17.0/generated/pandas.ewma.html.
 * <p>
 * For the initial value, by default we'll compute the mean of the values but
 * the flag can be set to use just the raw initial value.
 * 
 * See:
 * https://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average
 * https://www.itl.nist.gov/div898/handbook/pmc/section4/pmc431.htm
 * https://medium.com/@abhinav.mahapatra10/beginners-ml-basics-exponentially-weighted-moving-average-8ce3e75768f6
 * 
 * @since 3.0
 */
public class ExponentialWeightedMovingAverageFactory extends BaseTSDBPlugin implements 
    NumericAggregatorFactory {

  public static final String TYPE = "ExponentialWeightedMovingAverage";
    
  public static final ExponentialWeightedMovingAverageConfig DEFAULT_CONFIG =
      ExponentialWeightedMovingAverageConfig.newBuilder()
        .setAverageInitial(true)
        .build();
  
  @Override
  public NumericAggregator newAggregator() {
    return AGGREGATOR;
  }
  
  @Override
  public NumericAggregator newAggregator(final AggregatorConfig config) {
    return new ExponentialWeightedMovingAverage(TYPE, config);
  }
  
  @Override
  public NumericAggregator newAggregator(final boolean infectious_nan) {
    return AGGREGATOR;
  }

  @Override
  public String type() {
    return TYPE;
  }

  @Override
  public String version() {
    return "3.0.0";
  }

  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.tsdb = tsdb;
    this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
    return Deferred.fromResult(null);
  }
 
  private static final class ExponentialWeightedMovingAverage extends 
      BaseNumericAggregator {
    private final ExponentialWeightedMovingAverageConfig config;
    
    public ExponentialWeightedMovingAverage(final String name,
                                            final AggregatorConfig config) {
      super(name);
      this.config = (ExponentialWeightedMovingAverageConfig) config;
    }

    @Override
    public void run(final long[] values, 
                    final int start_offset,
                    final int end_offset, 
                    final MutableNumericValue dp) {
      // short circuit
      if (end_offset - start_offset == 1) {
        dp.resetValue(values[start_offset]);
        return;
      } else if (end_offset < 1) {
        throw new IllegalDataException("End offset must be greater than 0");
      }
      
      double ewma = values[start_offset];
      if (config.averageInitial()) {
        double sum = 0;
        for (int i = start_offset; i < end_offset; i++) {
          sum += values[i];
        }
        ewma = sum / (end_offset - start_offset); 
      }
      
      double alpha = config.alpha();
      if (alpha == 0) {
        // TODO - pick a nice optimization algo like Marquardt to find the
        // alpha for now we use the Pandas method based on the # of samples.
        alpha = 1d / (1d + ((double) (end_offset - start_offset) / (double) 2));
      }
      
      for (int i = start_offset + 1; i < end_offset; i++) {
        ewma = (alpha * (double) values[i]) + ((1d - alpha) * ewma);
      }
      
      dp.resetValue(ewma);
    }
    
    @Override
    public void run(final double[] values, 
                    final int start_offset,
                    final int end_offset, 
                    final boolean infectious_nans,
                    final MutableNumericValue dp) {
      // short circuit
      if (end_offset - start_offset == 1) {
        dp.resetValue(values[start_offset]);
        return;
      } else if (end_offset < 1) {
        throw new IllegalDataException("End offset must be greater than 0");
      }
      
      int offset = start_offset;
      double ewma = values[start_offset];
      while (Double.isNaN(ewma) && offset < end_offset) {
        ewma = values[offset++];
      }
      if (config.averageInitial()) {
        double sum = 0;
        int count = 0;
        for (int i = start_offset; i < end_offset; i++) {
          if (Double.isNaN(values[i])) {
            continue;
          }
          sum += values[i];
          count++;
        }
        ewma = count == 0 ? values[start_offset] : sum / count; 
      }
      
      double alpha = config.alpha();
      if (alpha == 0) {
        // TODO - pick a nice optimization algo like Marquardt to find the
        // alpha for now we use the Pandas method based on the # of samples.
        double span = 0;
        for (int i = start_offset + 1; i < end_offset; i++) {
          if (Double.isNaN(values[i]) && 
              (!config.infectiousNan() && !infectious_nans)) {
            continue;
          }
          span++;
        }
        alpha = 1d / (1d + (span / (double) 2));
      }
      
      for (int i = start_offset + 1; i < end_offset; i++) {
        if (Double.isNaN(values[i]) && 
            (!config.infectiousNan() && !infectious_nans)) {
          continue;
        }
        ewma = (alpha * values[i]) + ((1d - alpha) * ewma);
      }
      dp.resetValue(ewma);
    }
    
  }
  private static final NumericAggregator AGGREGATOR = 
      new ExponentialWeightedMovingAverage(TYPE, DEFAULT_CONFIG);
  
}
