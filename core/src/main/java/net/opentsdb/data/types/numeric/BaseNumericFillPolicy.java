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

import net.opentsdb.query.QueryFillPolicy;
import net.opentsdb.query.pojo.FillPolicy;

/**
 * A base class that implements some of the numeric fill policies. For those
 * that are added later that this class does not support, it will throw
 * {@link UnsupportedOperationException}s.
 * 
 * @since 3.0
 */
public class BaseNumericFillPolicy implements QueryFillPolicy<NumericType>, 
    NumericType {
  
  /** The fill policy for this implementation. */
  protected final FillPolicy policy;
  
  /**
   * Default Ctor.
   * @param policy A non-null policy to implement.
   * @throws IllegalArgumentException if the policy was null.
   */
  public BaseNumericFillPolicy(final FillPolicy policy) {
    if (policy == null) {
      throw new IllegalArgumentException("Policy cannot be null.");
    }
    this.policy = policy;
  }
  
  @Override
  public NumericType fill() {
    switch(policy) {
    case NONE:
    case NULL:
      return null;
    case ZERO:
    case NOT_A_NUMBER:
      return this;
    default:
      throw new UnsupportedOperationException("This class must be overidden to "
          + "support the policy.");
    }
  }

  @Override
  public boolean isInteger() {
    switch(policy) {
    case ZERO:
      return true;
    case NOT_A_NUMBER:
      return false;
    default:
      throw new UnsupportedOperationException("This class must be overidden to "
          + "support the policy.");
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
    return Double.NaN;
  }

  @Override
  public double toDouble() {
    switch(policy) {
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
  public net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy realPolicy() {
    // TODO Auto-generated method stub
    return null;
  }

}
