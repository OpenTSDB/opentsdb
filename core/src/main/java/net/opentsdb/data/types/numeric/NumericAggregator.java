// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
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
   * @return An aggregated value.
   */
  public NumericType run(final long[] values, final int max_index);
  
  /**
   * Aggregates the values in the array.
   * @param values An array of 1 or more floating point values.
   * @param max_index The maximum index within the array where real values are 
   * present.
   * @return An aggregated value.
   */
  public NumericType run(final double[] values, final int max_index);
}
