// This file is part of OpenTSDB.
// Copyright (C) 2014-2017  The OpenTSDB Authors.
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

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;

/**
 * A simple mutable data point for holding primitive signed numbers including 
 * {@link Long}s or {@link Double}s.
 * 
 * @since 3.0
 */
public final class MutableNumericType extends NumericType 
  implements TimeSeriesValue<NumericType> {

  //NOTE: Fields are not final to make an instance available to store a new
  // pair of a timestamp and a value to reduce memory burden.
  
  /** A reference to the ID of the series this data point belongs to. */
  private final TimeSeriesId id;
  
  /** The timestamp for this data point. */
  private TimeStamp timestamp;
  
  /** True if the value is stored as a long. */
  private boolean is_integer = true;
  
  /** A long value or a double encoded on a long if {@code is_integer} is false. */
  private long value = 0;
  
  /** The number of real values behind this data point. */
  private int reals = 0;
  
  /**
   * Initialize a new mutable data point with a {@link Long} value of 0.
   * @param id A non-null time series ID.
   * @throws IllegalArgumentException if the ID was null.
   */
  public MutableNumericType(final TimeSeriesId id) {
    if (id == null) {
      throw new IllegalArgumentException("ID cannot be null.");
    }
    this.id = id;
    timestamp = new MillisecondTimeStamp(0);
  }
  
  /**
   * Initialize the data point with an ID, timestamp and value. Reals will be 
   * considered 0.
   * @param id A non-null time series ID.
   * @param timestamp A non-null timestamp.
   * @param value A numeric value.
   * @throws IllegalArgumentException if the id or timestamp were null.
   */
  public MutableNumericType(final TimeSeriesId id, 
                            final TimeStamp timestamp, 
                            final long value) {
    this(id, timestamp, value, 0);
  }
  
  /**
   * Initialize the data point with an ID, timestamp and value with a reals count.
   * @param id A non-null time series ID.
   * @param timestamp A non-null timestamp.
   * @param value A numeric value.
   * @param reals The number of real values behind this value.
   * @throws IllegalArgumentException if the id or timestamp were null.
   */
  public MutableNumericType(final TimeSeriesId id, 
                            final TimeStamp timestamp, 
                            final long value, 
                            final int reals) {
    if (id == null) {
      throw new IllegalArgumentException("ID cannot be null.");
    }
    if (timestamp == null) {
      throw new IllegalArgumentException("Timestamp cannot be null.");
    }
    this.id = id;
    this.timestamp = timestamp.getCopy();
    this.value = value;
    this.reals = reals;
  }
  
  /**
   * Initialize the data point with an ID, timestamp and value. Reals will be 
   * considered 0.
   * @param id A non-null time series ID.
   * @param timestamp A non-null timestamp.
   * @param value A numeric value.
   * @throws IllegalArgumentException if the id or timestamp were null.
   */
  public MutableNumericType(final TimeSeriesId id, 
                            final TimeStamp timestamp, 
                            final double value) {
    this(id, timestamp, value, 0);
  }
  
  /**
   * Initialize the data point with an ID, timestamp and value with a reals count.
   * @param id A non-null time series ID.
   * @param timestamp A non-null timestamp.
   * @param value A numeric value.
   * @param reals The number of real values behind this value.
   * @throws IllegalArgumentException if the id or timestamp were null.
   */
  public MutableNumericType(final TimeSeriesId id, 
                            final TimeStamp timestamp, 
                            final double value, 
                            final int reals) {
    if (id == null) {
      throw new IllegalArgumentException("ID cannot be null.");
    }
    if (timestamp == null) {
      throw new IllegalArgumentException("Timestamp cannot be null.");
    }
    this.id = id;
    this.timestamp = timestamp.getCopy();
    this.value = Double.doubleToRawLongBits(value);
    is_integer = false;
    this.reals = reals;
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
    id = value.id();
    timestamp = value.timestamp().getCopy();
    is_integer = value.value().isInteger();
    this.value = value.value().isInteger() ? value.value().longValue() : 
      Double.doubleToRawLongBits(value.value().doubleValue());
    reals = value.realCount();
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
  public long valueCount() {
    return 1;
  }
  
  @Override
  public TimeSeriesId id() {
    return id;
  }

  @Override
  public TimeStamp timestamp() {
    return timestamp;
  }

  @Override
  public NumericType value() {
    return this;
  }

  @Override
  public int realCount() {
    return reals;
  }

  @Override
  public TypeToken<?> type() {
    return NumericType.TYPE;
  }

  @Override
  public TimeSeriesValue<NumericType> getCopy() {
    return is_integer ? new MutableNumericType(id, timestamp.getCopy(), value, reals) :
      new MutableNumericType(id, timestamp.getCopy(), Double.longBitsToDouble(value), reals);
  }

  /**
   * Reset the value given the timestamp, value and reals.
   * @param timestamp A non-null timestamp.
   * @param value A numeric value.
   * @param reals The number of real values behind this value.
   * @throws IllegalArgumentException if the timestamp was null.
   */
  public void reset(final TimeStamp timestamp, final long value, final int reals) {
    if (timestamp == null) {
      throw new IllegalArgumentException("Timestamp cannot be null");
    }
    this.timestamp.update(timestamp);
    this.value = value;
    this.reals = reals;
    is_integer = true;
  }
  
  /**
   * Reset the value given the timestamp, value and reals.
   * @param timestamp A non-null timestamp.
   * @param value A numeric value.
   * @param reals The number of real values behind this value.
   * @throws IllegalArgumentException if the timestamp was null.
   */
  public void reset(final TimeStamp timestamp, final double value, final int reals) {
    if (timestamp == null) {
      throw new IllegalArgumentException("Timestamp cannot be null");
    }
    this.timestamp.update(timestamp);
    this.value = Double.doubleToRawLongBits(value);
    this.reals = reals;
    is_integer = false;
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
    timestamp = value.timestamp().getCopy();
    this.value = value.value().isInteger() ? value.value().longValue() : 
      Double.doubleToRawLongBits(value.value().doubleValue());
    is_integer = value.value().isInteger();
    reals = value.realCount();
  }

}
