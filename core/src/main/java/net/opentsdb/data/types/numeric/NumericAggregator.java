// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
package net.opentsdb.data.types.numeric;

import net.opentsdb.data.Aggregator;

/**
 * An interface for numeric aggregators. Uses arrays of primitives in order to
 * take advantage of SIMD instructions whenever possible.
 * 
 * @since 3.0
 */
public interface NumericAggregator extends Aggregator<NumericType> {

  /**
   * Aggregates the values in the array.
   * @param values An array of 1 or more integer values.
   * @param max_index The maximum index within the array where real values are 
   * present.
   * @param dp A non-null data point that will have it's 
   * {@link MutableNumericValue#resetValue(double)} or 
   * {@link MutableNumericValue#resetValue(long)} called.
   * @return An aggregated value.
   * @throws IllegalDataException if the max_index was less than 1
   */
  public void run(final long[] values, 
                  final int max_index, 
                  final MutableNumericValue dp);
  
  /**
   * Aggregates the values in the array.
   * @param values An array of 1 or more floating point values.
   * @param max_index The maximum index within the array where real values are 
   * present.
   * @param infectious_nans When false we ignore NaNs, if true we include 
   * them in calculations meaning the output will generally be NaN.
   * @param dp A non-null data point that will have it's 
   * {@link MutableNumericValue#resetValue(double)} called.
   * @return An aggregated value.
   * @throws IllegalDataException if the max_index was less than 1
   */
  public void run(final double[] values, 
                  final int max_index, 
                  final boolean infectious_nans,
                  final MutableNumericValue dp);
}
