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

import net.opentsdb.query.QueryFillPolicy;
import net.opentsdb.query.QueryInterpolatorConfig;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;

/**
 * A base class that implements some of the numeric fill policies. For those
 * that are added later that this class does not support, it will throw
 * {@link UnsupportedOperationException}s.
 * <p>
 * TODO - find a way to determine if we should return a long or double.
 * For now min and max will return a double.
 * <p>
 * Implementation:
 * <ul>
 * <li>NONE: isInteger=false, value=null</li>
 * <li>NULL: isInteger=false, value=null</li>
 * <li>ZERO: isInteger=true, value=0</li>
 * <li>NOT_A_NUMBER: isInteger=false, value=NaN</li>
 * <li>MIN: isInteger=false, value=Double.MIN_VALUE</li>
 * <li>MAX: isInteger=false, value=Double.MAX_VALUE</li>
 * <li>SCALAR: isInteger=false, value=UnsupportedOperationException</li>
 * </ul>
 * @since 3.0
 */
public class BaseNumericFillPolicy implements QueryFillPolicy<NumericType>, 
    NumericType {
  
  /** The fill policy config for this implementation. */
  protected final NumericInterpolatorConfig config;
  
  /**
   * Default Ctor.
   * @param policy A non-null policy to implement.
   * @throws IllegalArgumentException if the policy was null.
   */
  public BaseNumericFillPolicy(final QueryInterpolatorConfig config) {
    if (config == null) {
      throw new IllegalArgumentException("Config cannot be null.");
    }
    this.config = (NumericInterpolatorConfig) config;
  }
  
  @Override
  public NumericType fill() {
    switch(config.fillPolicy()) {
    case NONE:
    case NULL:
      return null;
    case ZERO:
    case NOT_A_NUMBER:
    case MIN:
    case MAX:
      return this;
    default:
      throw new UnsupportedOperationException("This class must be overidden to "
          + "support the policy.");
    }
  }

  @Override
  public boolean isInteger() {
    switch(config.fillPolicy()) {
    case ZERO:
      return true;
    default:
      return false;
    }
  }

  @Override
  public long longValue() {
    // Always zero for this use case.
    return 0;
  }

  @Override
  public double doubleValue() {
    // If here then we're a NaN fill
    switch(config.fillPolicy()) {
    case NOT_A_NUMBER:
      return Double.NaN;
    case MIN:
      return Double.MIN_VALUE;
    case MAX:
      return Double.MAX_VALUE;
    default:
      throw new UnsupportedOperationException("This class must be "
          + "overidden to support the policy.");
    }
  }

  @Override
  public double toDouble() {
    switch(config.fillPolicy()) {
    case ZERO:
      return 0D;
    case NOT_A_NUMBER:
      return Double.NaN;
    default:
      throw new UnsupportedOperationException("This class must be overidden to "
          + "support the policy.");
    }
  }
  
  @Override
  public FillWithRealPolicy realPolicy() {
    return config.realFillPolicy();
  }

  @Override
  public QueryInterpolatorConfig config() {
    return config;
  }

}
