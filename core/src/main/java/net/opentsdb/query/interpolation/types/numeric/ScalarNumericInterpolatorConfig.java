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
