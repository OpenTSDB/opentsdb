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

import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.exceptions.IllegalDataException;

/**
 * Computes the average. For longs, if the result is a whole number, it will
 * return a long, otherwise it will return a double.
 * TODO - handle integer overflows.
 * 
 * @since 3.0
 */
public class AverageFactory extends BaseTSDBPlugin implements 
    NumericAggregatorFactory {

  public static final String ID = "avg";
  
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
  
  private static final class Avg extends BaseNumericAggregator {
    public Avg(final String name) {
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
      
      // TODO - overflow check
      long sum = 0;
      for (int i = start_offset; i < end_offset; i++) {
        sum += values[i];
      }
      double avg = (double) sum / (double) (end_offset - start_offset);
      if (avg % 1 == 0) {
        dp.resetValue(sum / (end_offset - start_offset));
      } else {
        dp.resetValue(avg);
      }
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
      
      double sum = 0;
      int nans = 0;
      for (int i = start_offset; i < end_offset; i++) {
        if (Double.isNaN(values[i]) && !infectious_nans) {
          nans++;
          continue;
        }
        sum += values[i];
      }
      if (nans == end_offset - start_offset || (nans > 0 && infectious_nans)) {
        dp.resetValue(Double.NaN);
      } else {
        dp.resetValue(sum / (double) (end_offset - start_offset - nans));
      }
    }
  }
  private static final NumericAggregator AGGREGATOR = new Avg(ID);
}
