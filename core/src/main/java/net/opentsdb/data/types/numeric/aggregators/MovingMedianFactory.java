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

import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.AggregatorConfig;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.exceptions.IllegalDataException;

/**
 * Computes the moving median without weighting. Just looks at the values 
 * between the offsets, sorts them and finds the median.
 * 
 * <b>WARNING:</b> This sorts in place, modifying the array.
 * 
 * @since 3.0
 */
public class MovingMedianFactory extends BaseTSDBPlugin implements 
    NumericAggregatorFactory {

  public static final String TYPE = "MovingMedian";
  
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
 
  private static final class MovingMedian extends BaseNumericAggregator {
    public MovingMedian(final String name) {
      super(name);
    }

    @Override
    public void run(final long[] values, 
                    final int start_offset,
                    final int end_offset, 
                    final MutableNumericValue dp) {
      // short circuit
      if (end_offset - start_offset < 3) {
        if (end_offset < 1) {
          throw new IllegalDataException("End offset must be greater than 0");
        }
        dp.resetValue(values[start_offset]);
        return;
      }
      
      Arrays.sort(values, start_offset, end_offset);
      dp.resetValue(values[start_offset + ((end_offset - 1 - start_offset) / 2)]);
    }
    
    @Override
    public void run(final double[] values, 
                    final int start_offset,
                    final int end_offset, 
                    final boolean infectious_nans,
                    final MutableNumericValue dp) {
      // short circuit
      if (end_offset - start_offset < 3) {
        if (end_offset < 1) {
          throw new IllegalDataException("End offset must be greater than 0");
        }
        dp.resetValue(values[start_offset]);
        return;
      }
      Arrays.sort(values, start_offset, end_offset);
      int end = end_offset - 1;
      if (!infectious_nans) {
        while (end > start_offset && Double.isNaN(values[end])) {
          end--;
        }
      }
      end++;
      if (end <= start_offset) {
        dp.resetValue(Double.NaN);
      } else {
        dp.resetValue(values[start_offset + ((end - start_offset) / 2)]);
      }
    }
    
  }
  private static final NumericAggregator AGGREGATOR = new MovingMedian(TYPE);
}
