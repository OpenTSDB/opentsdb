// This file is part of OpenTSDB.
// Copyright (C) 2012-2018  The OpenTSDB Authors.
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
 * Finds the largest value in the array.
 * 
 * @since 3.0
 */
public class MaxFactory extends BaseTSDBPlugin implements 
    NumericAggregatorFactory {

  public static final String ID = "max";
  
  @Override
  public NumericAggregator newAggregator(boolean infectious_nan) {
    return AGGREGATOR;
  }

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
        "mimmax", this);
    return Deferred.fromResult(null);
  }
  
  private static final class Max extends BaseNumericAggregator {
    public Max(final String name) {
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
      long max = values[start_offset];
      for (int i = start_offset + 1; i < end_offset; i++) {
        if (values[i] > max) {
          max = values[i];
        }
      }
      dp.resetValue(max);
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
      double max = values[start_offset];
      int nans = 0;
      for (int i = start_offset + 1; i < end_offset; i++) {
        if (Double.isNaN(values[i]) && infectious_nans) {
          nans++;
          continue;
        }
        if (values[i] > max) {
          max = values[i];
        }
      }
      if (nans == (end_offset - start_offset) || (nans > 0 && infectious_nans)) {
        dp.resetValue(Double.NaN);
      } else {
        dp.resetValue(max);
      }
    }
    
  }
  private static final NumericAggregator AGGREGATOR = new Max(ID);
}