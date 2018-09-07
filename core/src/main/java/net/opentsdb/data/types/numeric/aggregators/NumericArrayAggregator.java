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

import net.opentsdb.data.types.numeric.NumericArrayType;

/**
 * An interface for using SIMD instructions and L2 cache to improve
 * performance when aggregating normalized/downsampled numeric values.
 * 
 * @since 3.0
 */
public interface NumericArrayAggregator extends NumericArrayType {

  /**
   * Accumulates the integer values.
   * @param values A non-null (potentially empty) values array.
   */
  public void accumulate(final long[] values);
  
  /**
   * Accumulates the integer values.
   * @param values A non-null (potentially empty) values array.
   * @param from the initial index of the range to be copied, inclusive.
   * @param to the final index of the range to be copied, exclusive. 
   * (This index may lie outside the array)
   */
  public void accumulate(final long[] values, 
                         final int from, 
                         final int to);
  
  /**
   * Accumulates the double values.
   * @param values A non-null (potentially empty) values array.
   */
  public void accumulate(final double[] values);
  
  /**
   * Accumulates the double values.
   * @param values A non-null (potentially empty) values array.
   * @param from the initial index of the range to be copied, inclusive.
   * @param to the final index of the range to be copied, exclusive. 
   * (This index may lie outside the array)
   */
  public void accumulate(final double[] values,
                         final int from, 
                         final int to);
  
}