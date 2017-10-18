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

import com.google.common.base.Strings;

/**
 * A base implementation for numeric iterators that stores the name as well as
 * a numeric value implementation to populate with a result.
 * 
 * @since 3.0
 */
public abstract class BaseNumericAggregator implements NumericAggregator {

  /** The name of the aggregator. */
  protected final String name;
  
  /**
   * Default ctor.
   * @param name A non-null and non-empty string.
   * @throws IllegalArgumentException if the name was null or empty.
   */
  public BaseNumericAggregator(final String name) {
    if (Strings.isNullOrEmpty(name)) {
      throw new IllegalArgumentException("Name cannot be null or empty.");
    }
    this.name = name;
  }
  
  @Override
  public String name() {
    return name;
  }
  
  @Override
  public String toString() {
    return name;
  }
  
  /**
   * An implementation of a numeric value. Takes either a long or a double and
   * implements the {@link NumericType} over it.
   */
  protected class NumericValue implements NumericType {
    private final boolean is_integer;
    private final long value;
    
    /**
     * Ctor from an integer value.
     * @param value A value.
     */
    public NumericValue(final long value) {
      is_integer = true;
      this.value = value;
    }
    
    /**
     * Ctor from a double value.
     * @param value A value.
     */
    public NumericValue(final double value) {
      is_integer = false;
      this.value = Double.doubleToRawLongBits(value);
    }
     
    @Override
    public boolean isInteger() {
      return is_integer;
    }
    
    @Override
    public long longValue() {
      if (!is_integer) {
        throw new ClassCastException("Value is not a long.");
      }
      return value;
    }
    
    @Override
    public double doubleValue() {
      if (is_integer) {
        throw new ClassCastException("Value is not a long.");
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
    
  }
}
