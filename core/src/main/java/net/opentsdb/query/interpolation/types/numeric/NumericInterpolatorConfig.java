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

import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.data.types.numeric.BaseNumericFillPolicy;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryFillPolicy;
import net.opentsdb.query.QueryIteratorInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;

/**
 * A simple config for the base {@link NumericInterpolator}. Stores the real
 * fill policy.
 * 
 * @since 3.0
 */
public class NumericInterpolatorConfig implements QueryIteratorInterpolatorConfig {

  /** The numeric fill policy. */
  protected final FillPolicy fill_policy;
  
  /** The real value fill policy. */
  protected final FillWithRealPolicy real_fill;
  
  /**
   * Protected ctor for use with the builder.
   * @param builder A non-null builder.
   * @throws IllegalArgumentException if the fill policy was null or empty.
   */
  NumericInterpolatorConfig(final Builder builder) {
    if (builder.fill_policy == null) {
      throw new IllegalArgumentException("Fill policy cannot be null.");
    }
    if (builder.real_fill == null) {
      throw new IllegalArgumentException("Real fill policy cannot be null.");
    }
    fill_policy = builder.fill_policy;
    real_fill = builder.real_fill;
  }
  
  /** @return The numeric fill policy. */
  public FillPolicy fillPolicy() {
    return fill_policy;
  }
  
  /** @return The real fill policy. */
  public FillWithRealPolicy realFillPolicy() {
    return real_fill;
  }
  
  /** @return The base numeric fill using the {@link #fillPolicy()}. */
  public QueryFillPolicy<NumericType> queryFill() {
    return new BaseNumericFillPolicy(this);
  }
  
  /** @return A new builder for the config. */
  public static Builder newBuilder() {
    return new Builder();
  }
  
  /**
   * A builder class for the config.
   */
  public static class Builder {
    private FillPolicy fill_policy;
    private FillWithRealPolicy real_fill;
    
    /**
     * @param fill_policy A non-null numeric fill policy.
     * @return The builder.
     */
    public Builder setFillPolicy(final FillPolicy fill_policy) {
      this.fill_policy = fill_policy;
      return this;
    }
    
    /**
     * @param real_fill A non-null real fill policy.
     * @return The builder.
     */
    public Builder setRealFillPolicy(final FillWithRealPolicy real_fill) {
      this.real_fill = real_fill;
      return this;
    }
    
    /** @return An instantiated interpolator config. */
    public NumericInterpolatorConfig build() {
      return new NumericInterpolatorConfig(this);
    }
  }

}
