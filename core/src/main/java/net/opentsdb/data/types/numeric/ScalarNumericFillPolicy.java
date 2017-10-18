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

import net.opentsdb.query.pojo.FillPolicy;

/**
 * Implements a fixed numeric fill policy, returning the same value every time
 * {@link #fill()} is called.
 * 
 * @since 3.0
 */
public class ScalarNumericFillPolicy extends BaseNumericFillPolicy {
  
  /** Whether or not the value is an integer. */
  protected boolean is_integer;
  
  /** The value to fill with. */
  protected long fill;
  
  /**
   * Ctor for integers.
   * @param scalar An integer value.
   */
  public ScalarNumericFillPolicy(final long scalar) {
    super(FillPolicy.SCALAR);
    is_integer = true;
    fill = scalar;
  }
  
  /**
   * Ctor for floats.
   * @param scalar A double value.
   */
  public ScalarNumericFillPolicy(final double scalar) {
    super(FillPolicy.SCALAR);
    is_integer = false;
    fill = Double.doubleToRawLongBits(scalar);
  }
  
  @Override
  public NumericType fill() {
    return this;
  }
  
  @Override
  public boolean isInteger() {
    return is_integer;
  }
  
  @Override
  public long longValue() {
    if (!is_integer) {
      throw new ClassCastException("Not a long in " + toString());
    }
    return fill;
  }
  
  @Override
  public double doubleValue() {
    if (is_integer) {
      throw new ClassCastException("Not a long in " + toString());
    }
    return Double.longBitsToDouble(fill);
  }
  
  @Override
  public double toDouble() {
    if (is_integer) {
      return (double) fill;
    }
    return Double.longBitsToDouble(fill);
  }
}
