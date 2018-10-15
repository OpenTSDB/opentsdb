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

import java.util.Arrays;

import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.exceptions.IllegalDataException;

/**
 * Returns the median value of the set. For even set sizes, the upper most
 * value of the median is returned.
 * 
 * @since 3.0
 */
public class MedianFactory extends BaseTSDBPlugin implements 
    NumericAggregatorFactory {

  public static final String ID = "median";
  
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

  private static final class Median extends BaseNumericAggregator {
    public Median(final String name) {
      super(name);
    }

    @Override
    public void run(final long[] values, 
                    final int start_offset,
                    final int end_offset, 
                    final MutableNumericValue dp) {
      if (end_offset - start_offset == 1) {
        dp.resetValue(values[start_offset]);
        return;
      } else if (end_offset < 1) {
        throw new IllegalDataException("End offset must be greater than 0");
      }
      
      // ugg, we can't violate the sorting of the source and we can't
      // sort anyway since the limit may be less than the length with
      // garbage in a previously used array. so we have to copy.
      final long[] copy = end_offset - start_offset == values.length ? values : 
          Arrays.copyOfRange(values, start_offset, end_offset);
      Arrays.sort(copy);
      
      dp.resetValue(copy[copy.length / 2]);
    }

    @Override
    public void run(final double[] values, 
                    final int start_offset,
                    final int end_offset, 
                    final boolean infectious_nans,
                    final MutableNumericValue dp) {
      if (end_offset - start_offset == 1) {
        dp.resetValue(values[0]);
        return;
      } else if (end_offset < 1) {
        throw new IllegalDataException("End offset must be greater than 0");
      }
      
      final double[] copy = end_offset - start_offset == values.length ? values : 
        Arrays.copyOfRange(values, start_offset, end_offset);
      Arrays.sort(copy);
      if (Double.isNaN(copy[copy.length - 1]) && infectious_nans) {
        dp.resetValue(Double.NaN);
      } else {
        int end = copy.length - 1;
        while (Double.isNaN(copy[end])) {
          end--;
          if (end < 0) {
            dp.resetValue(Double.NaN);
            return;
          }
        }
        dp.resetValue(copy[(end + 1) / 2]);
      }
    }
    
  }
  private static final NumericAggregator AGGREGATOR = new Median(ID);
}
