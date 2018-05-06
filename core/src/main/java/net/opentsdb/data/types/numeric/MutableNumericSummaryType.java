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

import java.util.Collection;
import java.util.Map;

import com.google.common.collect.Maps;

/**
 * A simple map based implementation of the {@link NumericSummaryType}.
 * Note that if the number or type of summaries may change, call 
 * {@link #clear()} before calling any of the {@code set} functions.
 * 
 * @since 3.0
 */
public class MutableNumericSummaryType implements NumericSummaryType {

  /** The map of summary ID to values. */
  private Map<Integer, MutableNumericType> values;
  
  /**
   * Default ctor. Initializes the map to an expected size of one.
   */
  public MutableNumericSummaryType() {
    values = Maps.newHashMapWithExpectedSize(1);
  }
  
  /**
   * Ctor setting the summary map to a size of one and storing the given
   * summary.
   * @param summary A summary ID.
   * @param value A value.
   */
  public MutableNumericSummaryType(final int summary, final long value) {
    values = Maps.newHashMapWithExpectedSize(1);
    set(summary, value);
  }
  
  /**
   * Ctor setting the summary map to a size of one and storing the given
   * summary.
   * @param summary A summary ID.
   * @param value A value.
   */
  public MutableNumericSummaryType(final int summary, final double value) {
    values = Maps.newHashMapWithExpectedSize(1);
    set(summary, value);
  }
  
  /**
   * Ctor cloning the given non-null value.
   * @param value A non-null value.
   * @throws IllegalArgumentException if the value was null();
   */
  public MutableNumericSummaryType(final NumericSummaryType value) {
    if (value == null) {
      throw new IllegalArgumentException("Value cannot be null.");
    }
    values = Maps.newHashMapWithExpectedSize(value.summariesAvailable().size());
    for (final int summary : value.summariesAvailable()) {
      final NumericType other = value.value(summary);
      if (other != null) {
        if (other.isInteger()) {
          set(summary, other.longValue());
        } else {
          set(summary, other.doubleValue());
        }
      }
    }
  }
  
  @Override
  public Collection<Integer> summariesAvailable() {
    return values.keySet();
  }

  @Override
  public NumericType value(final int summary) {
    return values.get(summary);
  }

  /** Clears all the values of the map. */
  public void clear() {
    values.clear();
  }
  
  /**
   * Sets the value, overriding the present value.
   * @param summary The summary ID.
   * @param value The value to set.
   */
  public void set(final int summary, final long value) {
    MutableNumericType v = values.get(summary);
    if (v == null) {
      values.put(summary, new MutableNumericType(value));
    } else {
      v.set(value);
    }
  }
  
  /**
   * Sets the value, overriding the present value.
   * @param summary The summary ID.
   * @param value The value to set.
   */
  public void set(final int summary, final double value) {
    MutableNumericType v = values.get(summary);
    if (v == null) {
      values.put(summary, new MutableNumericType(value));
    } else {
      v.set(value);
    }
  }
  
  /**
   * Resets the current map and clones the given non-null value.
   * @param value A non-null value to clone.
   * @throws IllegalArgumentException if the value was null.
   */
  public void set(final NumericSummaryType value) {
    if (value == null) {
      throw new IllegalArgumentException("Value cannot be null.");
    }
    clear();
    for (final int summary : value.summariesAvailable()) {
      final NumericType other = value.value(summary);
      if (other != null) {
        if (other.isInteger()) {
          set(summary, other.longValue());
        } else {
          set(summary, other.doubleValue());
        }
      }
    }
  }
}