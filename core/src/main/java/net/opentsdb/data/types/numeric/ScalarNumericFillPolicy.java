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

import net.opentsdb.query.interpolation.types.numeric.ScalarNumericInterpolatorConfig;

/**
 * Implements a fixed numeric fill policy, returning the same value every time
 * {@link #fill()} is called.
 * 
 * @since 3.0
 */
public class ScalarNumericFillPolicy extends BaseNumericFillPolicy {
  
  /** The config policy. */
  protected ScalarNumericInterpolatorConfig config;
  
  /** The value to fill with. */
  protected long fill;
  
  /**
   * Ctor for integers.
   * @param config The config policy that includes the value to fill with.
   */
  public ScalarNumericFillPolicy(final ScalarNumericInterpolatorConfig config) {
    super(config);
    this.config = config;
  }
  
  @Override
  public NumericType fill() {
    return this;
  }
  
  @Override
  public boolean isInteger() {
    return config.isInteger();
  }
  
  @Override
  public long longValue() {
    return config.longValue();
  }
  
  @Override
  public double doubleValue() {
    return config.doubleValue();
  }
  
  @Override
  public double toDouble() {
    return config.toDouble();
  }
}
