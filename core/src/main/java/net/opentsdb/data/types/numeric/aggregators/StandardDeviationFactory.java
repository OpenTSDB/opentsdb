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
 * Standard Deviation aggregator.
 * Can compute without storing all of the data points in memory at the same
 * time.  This implementation is based upon a
 * <a href="http://www.johndcook.com/standard_deviation.html">paper by John
 * D. Cook</a>, which itself is based upon a method that goes back to a 1962
 * paper by B.  P. Welford and is presented in Donald Knuth's Art of
 * Computer Programming, Vol 2, page 232, 3rd edition
 * 
 * @since 3.0
 */
public class StandardDeviationFactory extends BaseTSDBPlugin implements 
    NumericAggregatorFactory {

  public static final String ID = "dev";
  
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

   private static final class StdDev extends BaseNumericAggregator {
    public StdDev(final String name) {
      super(name);
    }

    @Override
    public void run(final long[] values, 
                    final int start_offset,
                    final int end_offset, 
                    final MutableNumericValue dp) {
      if (end_offset - start_offset == 1) {
        dp.resetValue(0L);
        return;
      } else if (end_offset < 1) {
        throw new IllegalDataException("End offset must be greater than 0");
      }
      
      double old_mean = values[start_offset];
      long n = 2;
      double new_mean = 0.;
      double M2 = 0.;
      for (int i = start_offset + 1; i < end_offset; i++) {
        final double x = values[i];
        new_mean = old_mean + (x - old_mean) / n;
        M2 += (x - old_mean) * (x - new_mean);
        old_mean = new_mean;
        n++;
      }

      double stdev = Math.sqrt(M2 / (n - 1));
      if (stdev % 1 == 0) {
        dp.resetValue((long) stdev);
      } else {
        dp.resetValue(stdev);
      }
    }

    @Override
    public void run(final double[] values, 
                    final int start_offset,
                    final int end_offset, 
                    final boolean infectious_nans,
                    final MutableNumericValue dp) {
      if (end_offset - start_offset == 1) {
        dp.resetValue(0.0);
        return;
      } else if (end_offset < 1) {
        throw new IllegalDataException("End offset must be greater than 0");
      }
      
      int nans = Double.isNaN(values[0]) ? 1 : 0;
      double old_mean = values[start_offset];
      long n = 2;
      double new_mean = 0.;
      double M2 = 0.;
      for (int i = start_offset + 1; i < end_offset; i++) {
        if (Double.isNaN(values[i]) && !infectious_nans) {
          nans++;
          continue;
        }
        final double x = values[i];
        new_mean = old_mean + (x - old_mean) / n;
        M2 += (x - old_mean) * (x - new_mean);
        old_mean = new_mean;
        n++;
      }
      
      if (nans == end_offset - start_offset || (nans > 0 && infectious_nans)) {
        dp.resetValue(Double.NaN);
      } else {
        dp.resetValue(Math.sqrt(M2 / (n - 1)));
      }
    }
    
  }
  private static final NumericAggregator AGGREGATOR = new StdDev(ID);
}
