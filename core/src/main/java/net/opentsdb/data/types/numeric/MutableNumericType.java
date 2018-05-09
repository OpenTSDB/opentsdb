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
package net.opentsdb.data.types.numeric;

/**
 * A simple mutable value holding primitive signed numbers including a
 * {@link Long} or {@link Double}. This class does not include timestamps.
 * <b>NOTE:</b> The default value of this DP is {@link Double#NaN};
 * 
 * @since 3.0
 */
public class MutableNumericType implements NumericType {
  /** True if the value is stored as a long. */
  private boolean is_integer = false;
  
  /** A long value or a double encoded on a long if {@code is_integer} is false. */
  private long value = Double.doubleToLongBits(Double.NaN);
  
  /**
   * Default ctor sets the value to {@link Double#NaN}.
   */
  public MutableNumericType() { }
  
  /**
   * Ctor setting the value.
   * @param value The value to set.
   */
  public MutableNumericType(final long value) {
    set(value);
  }
  
  /**
   * Ctor setting the value.
   * @param value The value to set.
   */
  public MutableNumericType(final double value) {
    set(value);
  }
  
  /**
   * Ctor cloning the value from another NumericType.
   * @param value A non-null value to clone.
   * @throws IllegalArgumentException if the value was null.
   */
  public MutableNumericType(final NumericType value) {
    if (value == null) {
      throw new IllegalArgumentException("Value cannot be null.");
    }
    if (value.isInteger()) {
      set(value.longValue());
    } else {
      set(value.doubleValue());
    }
  }
  
  /**
   * Sets the value to a long.
   * @param value A value to store.
   */
  public void set(final long value) {
    this.value = value;
    is_integer = true;
  }
  
  /**
   * Sets the value to a double.
   * @param value A value to store.
   */
  public void set(final double value) {
    this.value = Double.doubleToLongBits(value);
    is_integer = false;
  }
  
  /**
   * Copies the value from a non-null type.
   * <b>NOTE</b> The value is not checked for null.
   * @param value A non-null value to copy from.
   */
  public void set(final NumericType value) {
    if (value.isInteger()) {
      this.value = value.longValue();
      is_integer = true;
    } else {
      this.value = Double.doubleToRawLongBits(value.doubleValue());
      is_integer = false;
    }
  }
  
  @Override
  public boolean isInteger() {
    return is_integer;
  }

  @Override
  public long longValue() {
    if (is_integer) {
      return value;
    }
    throw new ClassCastException("Not a long in " + toString());
  }

  @Override
  public double doubleValue() {
    if (!is_integer) {
      return Double.longBitsToDouble(value);
    }
    throw new ClassCastException("Not a double in " + toString());
  }

  @Override
  public double toDouble() {
    if (is_integer) {
      return value;
    }
    return Double.longBitsToDouble(value);
  }
  
  @Override
  public String toString() {
    return is_integer ? Long.toString(value) : 
      Double.toString(Double.longBitsToDouble(value));
  }
}