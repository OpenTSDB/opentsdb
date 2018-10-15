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

import com.stumbleupon.async.Deferred;

import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.exceptions.IllegalDataException;

/**
 * Aggregator that simply sums all of the values in the array.
 * TODO - handle integer overflows.
 * 
 * @since 3.0
 */
public class SumFactory extends BaseTSDBPlugin implements 
    NumericAggregatorFactory {

  public static final String ID = "sum";
  
  @Override
  public String id() {
    return ID;
  }

  @Override
  public String version() {
    return "3.0.0";
  }

  @Override
  public Deferred<Object> initialize(TSDB tsdb) {
    tsdb.getRegistry().registerPlugin(NumericAggregatorFactory.class, 
        "zimsum", this);
    return Deferred.fromResult(null);
  }
  
  @Override
  public NumericAggregator newAggregator(boolean infectious_nan) {
    return AGGREGATOR;
  }

  /**
   * Aggregator that simply sums all of the values in the array.
   * TODO - handle integer overflows.
   */
  private static final class Sum extends BaseNumericAggregator {
    public Sum(final String name) {
      super(name);
    }

    @Override
    public void run(final long[] values, 
                    final int start_offset,
                    final int end_offset, 
                    final MutableNumericValue dp) {
      if (end_offset < 1) {
        throw new IllegalDataException("End offset must be greater than 0");
      }
      // TODO - overflow check
      long sum = 0;
      for (int i = start_offset; i < end_offset; i++) {
        sum += values[i];
      }
      dp.resetValue(sum);
    }
    
    @Override
    public void run(final double[] values, 
                    final int start_offset,
                    final int end_offset, 
                    final boolean infectious_nans,
                    final MutableNumericValue dp) {
      if (end_offset < 1) {
        throw new IllegalDataException("End offset must be greater than 0");
      }
      double sum = 0;
      int nans = 0;
      for (int i = start_offset; i < end_offset; i++) {
        if (Double.isNaN(values[i]) && !infectious_nans) {
          nans++;
          continue;
        }
        sum += values[i];
      }
      if (nans == (end_offset - start_offset) || (nans > 0 && infectious_nans)) {
        dp.resetValue(Double.NaN);
      } else {
        dp.resetValue(sum);
      }
    }
  }
  private static final Sum AGGREGATOR = new Sum(ID);
}
