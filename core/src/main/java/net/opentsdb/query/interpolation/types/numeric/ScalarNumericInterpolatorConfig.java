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
package net.opentsdb.query.interpolation.types.numeric;

import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.data.types.numeric.ScalarNumericFillPolicy;
import net.opentsdb.query.QueryFillPolicy;

/**
 * Simple scalar interpolator config that fills with a single value when it
 * needs to.
 * <p>
 * <b>NOTE:</b> If the value is not set, it defaults integer 0.
 * 
 * @since 3.0
 */
public class ScalarNumericInterpolatorConfig extends NumericInterpolatorConfig
    implements NumericType {
  
  /** The value encoded as a long. */
  private final long value;
  
  /** Whether or not the value is an integer or float. */
  private final boolean is_integer;
  
  /**
   * Protected ctor for use with the builder.
   * @param builder A non-null builder.
   * @throws IllegalArgumentException if the fill policy was null or empty.
   */
  ScalarNumericInterpolatorConfig(final Builder builder) {
    super(builder);
    value = builder.value;
    is_integer = builder.is_integer;
  }
  
  @Override
  public QueryFillPolicy<NumericType> queryFill() {
    return new ScalarNumericFillPolicy(this);
  }
  
  @Override
  public boolean isInteger() {
    return is_integer;
  }

  @Override
  public long longValue() {
    if (!is_integer) {
      throw new ClassCastException("Value is a floating point.");
    }
    return value;
  }

  @Override
  public double doubleValue() {
    if (is_integer) {
      throw new ClassCastException("Value is an integer.");
    }
    return Double.longBitsToDouble(value);
  }

  @Override
  public double toDouble() {
    if (is_integer) {
      return (double) value;
    }
    return Double.longBitsToDouble(value);
  }
  
  public static Builder newBuilder() {
    return new Builder();
  }
  
  public static class Builder extends NumericInterpolatorConfig.Builder {
    private long value;
    private boolean is_integer = true;
    
    /**
     * @param value An integer value to fill with.
     * @return The builder.
     */
    public Builder setValue(final long value) {
      this.value = value;
      is_integer = true;
      return this;
    }
    
    /**
     * @param value A floating point value to fill with.
     * @return The builder.
     */
    public Builder setValue(final double value) {
      this.value = Double.doubleToRawLongBits(value);
      is_integer = false;
      return this;
    }
    
    /** @return An instantiated interpolator config. */
    public ScalarNumericInterpolatorConfig build() {
      return new ScalarNumericInterpolatorConfig(this);
    }
  }
}
