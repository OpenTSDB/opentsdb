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
 * Computes the moving average weighted by the number of values in the array
 * (between the offsets). Heaviest weighting applied to higher indexed values
 * so we assume the inputs are in-order.
 * 
 * @since 3.0
 */
public class WeightedMovingAverageFactory extends BaseTSDBPlugin implements 
    NumericAggregatorFactory {

  public static final String TYPE = "WeightedMovingAverage";
  
  @Override
  public NumericAggregator newAggregator() {
    return AGGREGATOR;
  }
  
  @Override
  public NumericAggregator newAggregator(final AggregatorConfig config) {
    return AGGREGATOR;
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
    this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
    return Deferred.fromResult(null);
  }
 
  private static final class WeightedMovingAverage extends BaseNumericAggregator {
    public WeightedMovingAverage(final String name) {
      super(name);
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
      
      final int denom = end_offset - start_offset + 1;
      double sum = 0;
      double weights = 0;
      for (int i = start_offset; i < end_offset; i++) {
        final double weight = (double) (i - start_offset + 1) / (double) denom;
        weights += weight;
        sum += (double) values[i] * weight;
      }
      
      dp.resetValue(sum / weights);
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
      
      final int denom = end_offset - start_offset + 1;
      double sum = 0;
      double weights = 0;
      for (int i = end_offset -1; i >= start_offset; i--) {
        if (Double.isNaN(values[i]) && !infectious_nans) {
          continue;
        }
        final double weight = (double) (i - start_offset + 1) / (double) denom;
        weights += weight;
        sum += (double) values[i] * weight;
      }
      
      dp.resetValue(sum / weights);
    }
    
  }
  private static final NumericAggregator AGGREGATOR = new WeightedMovingAverage(TYPE);
}
