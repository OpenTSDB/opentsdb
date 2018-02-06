// This file is part of OpenTSDB.
// Copyright (C) 2014-2017  The OpenTSDB Authors.
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

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;

/**
 * A simple mutable data point for holding primitive signed numbers including 
 * {@link Long}s or {@link Double}s. The class is also nullable so that if the
 * owner calls {@link #resetNull(TimeStamp)} then the calls to {@link #value()}
 * will return null but the timestamp will be accurate.
 * 
 * @since 3.0
 */
public final class MutableNumericType implements NumericType, 
                                                 TimeSeriesValue<NumericType> {

  //NOTE: Fields are not final to make an instance available to store a new
  // pair of a timestamp and a value to reduce memory burden.
  
  /** The timestamp for this data point. */
  private TimeStamp timestamp;
  
  /** True if the value is stored as a long. */
  private boolean is_integer = true;
  
  /** A long value or a double encoded on a long if {@code is_integer} is false. */
  private long value = 0;
  
  /** Whether or not the current value is null. */
  private boolean nulled; 
  
  /**
   * Initialize a new mutable data point with a {@link Long} value of 0.
   */
  public MutableNumericType() {
    nulled = false;
    timestamp = new MillisecondTimeStamp(0);
  }
  
  /**
   * Initialize the data point with a timestamp and value .
   * @param timestamp A non-null timestamp.
   * @param value A numeric value.
   * @throws IllegalArgumentException if the timestamp was null.
   */
  public MutableNumericType(final TimeStamp timestamp, 
                            final long value) {
    if (timestamp == null) {
      throw new IllegalArgumentException("Timestamp cannot be null.");
    }
    this.timestamp = timestamp.getCopy();
    this.value = value;
    nulled = false;
  }
  
  /**
   * Initialize the data point with a timestamp and value.
   * @param timestamp A non-null timestamp.
   * @param value A numeric value.
   * @throws IllegalArgumentException if the timestamp was null.
   */
  public MutableNumericType(final TimeStamp timestamp, 
                            final double value) {
    if (timestamp == null) {
      throw new IllegalArgumentException("Timestamp cannot be null.");
    }
    this.timestamp = timestamp.getCopy();
    this.value = Double.doubleToRawLongBits(value);
    is_integer = false;
    nulled = false;
  }

  /**
   * Initializes the data point with a timestamp and value, making copies of the
   * arguments.
   * @param timestamp A non-null timestamp.
   * @param value A numeric value.
   * @throws IllegalArgumentException if the timestamp or value was null.
   */
  public MutableNumericType(final TimeStamp timestamp, final NumericType value) {
    if (timestamp == null) {
      throw new IllegalArgumentException("Timestamp cannot be null.");
    }
    if (value == null) {
      throw new IllegalArgumentException("Value cannot be null.");
    }
    this.timestamp = timestamp.getCopy();
    if (value.isInteger()) {
      is_integer = true;
      this.value = value.longValue();
    } else {
      is_integer = false;
      this.value = Double.doubleToRawLongBits(value.doubleValue());
    }
    nulled = false;
  }
  
  /**
   * Initialize the data point from the given value, copying the timestamp
   * but passing the reference to the ID.
   * @param value A non-null value to copy from.
   * @throws IllegalArgumentException if the value was null.
   */
  public MutableNumericType(final TimeSeriesValue<NumericType> value) {
    if (value == null) {
      throw new IllegalArgumentException("Value cannot be null.");
    }
    timestamp = value.timestamp().getCopy();
    if (value.value() == null) {
      nulled = true;
    } else {
      is_integer = value.value().isInteger();
      this.value = value.value().isInteger() ? value.value().longValue() : 
        Double.doubleToRawLongBits(value.value().doubleValue());
      nulled = false;
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
  public TimeStamp timestamp() {
    return timestamp;
  }

  @Override
  public NumericType value() {
    return nulled ? null : this;
  }
  
  /**
   * Reset the value given the timestamp and value.
   * @param timestamp A non-null timestamp.
   * @param value A numeric value.
   * @throws IllegalArgumentException if the timestamp was null.
   */
  public void reset(final TimeStamp timestamp, final long value) {
    if (timestamp == null) {
      throw new IllegalArgumentException("Timestamp cannot be null");
    }
    if (timestamp.units().ordinal() < this.timestamp.units().ordinal()) {
      this.timestamp = timestamp.getCopy();
    } else {
      this.timestamp.update(timestamp);
    }
    this.value = value;
    is_integer = true;
    nulled = false;
  }
  
  /**
   * Reset the value given the timestamp and value.
   * @param timestamp A non-null timestamp.
   * @param value A numeric value.
   * @throws IllegalArgumentException if the timestamp was null.
   */
  public void reset(final TimeStamp timestamp, final double value) {
    if (timestamp == null) {
      throw new IllegalArgumentException("Timestamp cannot be null");
    }
    if (timestamp.units().ordinal() < this.timestamp.units().ordinal()) {
      this.timestamp = timestamp.getCopy();
    } else {
      this.timestamp.update(timestamp);
    }
    this.value = Double.doubleToRawLongBits(value);
    is_integer = false;
    nulled = false;
  }
  
  /**
   * Resets the local value by copying the timestamp and value from the source
   * value.
   * @param value A non-null value.
   * @throws IllegalArgumentException if the value was null or the value's 
   * timestamp was null.
   */
  public void reset(final TimeSeriesValue<NumericType> value) {
    if (value == null) {
      throw new IllegalArgumentException("Value cannot be null");
    }
    if (value.timestamp() == null) {
      throw new IllegalArgumentException("Value's timestamp cannot be null");
    }
    if (value.timestamp().units().ordinal() < this.timestamp.units().ordinal()) {
      this.timestamp = value.timestamp().getCopy();
    } else {
      this.timestamp.update(value.timestamp());
    }
    if (value.value() != null) {
      this.value = value.value().isInteger() ? value.value().longValue() : 
        Double.doubleToRawLongBits(value.value().doubleValue());
      is_integer = value.value().isInteger();
      nulled = false;
    } else {
      nulled = true;
    }
  }

  /**
   * Resets the local value by copying the timestamp and value from the arguments.
   * @param A non-null timestamp.
   * @param value A numeric value.
   * @throws IllegalArgumentException if the timestamp or value was null.
   */
  public void reset(final TimeStamp timestamp, final NumericType value) {
    if (timestamp == null) {
      throw new IllegalArgumentException("Timestamp cannot be null.");
    }
    if (value == null) {
      throw new IllegalArgumentException("Value cannot be null.");
    }
    if (timestamp.units().ordinal() < this.timestamp.units().ordinal()) {
      this.timestamp = timestamp.getCopy();
    } else {
      this.timestamp.update(timestamp);
    }
    if (value.isInteger()) {
      is_integer = true;
      this.value = value.longValue();
    } else {
      is_integer = false;
      this.value = Double.doubleToRawLongBits(value.doubleValue());
    }
    nulled = false;
  }
  
  /**
   * Resets the value to null with the given timestamp.
   * @param timestamp A non-null timestamp to update from.
   * @throws IllegalArgumentException if the timestamp was null.
   */
  public void resetNull(final TimeStamp timestamp) {
    if (timestamp == null) {
      throw new IllegalArgumentException("Timestamp cannot be null.");
    }
    if (timestamp.units().ordinal() < this.timestamp.units().ordinal()) {
      this.timestamp = timestamp.getCopy();
    } else {
      this.timestamp.update(timestamp);
    }
    nulled = true;
  }
  
  @Override
  public TypeToken<NumericType> type() {
    return NumericType.TYPE;
  }

}
