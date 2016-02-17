// This file is part of OpenTSDB.
// Copyright (C) 2014  The OpenTSDB Authors.
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
package net.opentsdb.core;


/**
 * A mutable {@link DataPoint} that stores a value and a timestamp.
 */
public final class MutableDataPoint implements DataPoint {

  // NOTE: Fields are not final to make an instance available to store a new
  // pair of a timestamp and a value to reduce memory burden.
  /** The timestamp of the value. */
  private long timestamp = Long.MAX_VALUE;
  /** True if the value is stored as a long. */
  private boolean is_integer = true;
  /** A long value or a double encoded on a long if {@code is_integer} is false. */
  private long value = 0;

  /**
   * Resets with a new pair of a timestamp and a double value.
   *
   * @param timestamp A timestamp.
   * @param value A double value.
   */
  public void reset(final long timestamp, final double value) {
    this.timestamp = timestamp;
    this.is_integer = false;
    this.value = Double.doubleToRawLongBits(value);
  }

  /**
   * Resets with a new pair of a timestamp and a long value.
   *
   * @param timestamp A timestamp.
   * @param value A double value.
   */
  public void reset(final long timestamp, final long value) {
    this.timestamp = timestamp;
    this.is_integer = true;
    this.value = value;
  }

  /**
   * Resets with a new data point.
   *
   * @param dp A new data point to store.
   */
  public void reset(DataPoint dp) {
    this.timestamp = dp.timestamp();
    this.is_integer = dp.isInteger();
    if (is_integer) {
      this.value = dp.longValue();
    } else {
      this.value = Double.doubleToRawLongBits(dp.doubleValue());
    }
  }

  /**
   * Resets with a new pair of a timestamp and a double value.
   *
   * @param timestamp A timestamp.
   * @param value A double value.
   */
  public static MutableDataPoint ofDoubleValue(final long timestamp,
                                               final double value) {
    final MutableDataPoint dp = new MutableDataPoint();
    dp.reset(timestamp, value);
    return dp;
  }

  /**
   * Resets with a new pair of a timestamp and a long value.
   *
   * @param timestamp A timestamp.
   * @param value A double value.
   */
  public static MutableDataPoint ofLongValue(final long timestamp,
                                             final long value) {
    final MutableDataPoint dp = new MutableDataPoint();
    dp.reset(timestamp, value);
    return dp;
  }

  /**
   * Copy constructor
   *
   * @param value     A datapoint value.
   */
  public static MutableDataPoint fromPoint(final DataPoint value) {
    if (value.isInteger()) return ofLongValue(value.timestamp(), value.longValue());
    else return ofDoubleValue(value.timestamp(), value.doubleValue());
  }

  @Override
  public long timestamp() {
    return timestamp;
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
    return "MutableDataPoint(timestamp=" + timestamp + ", is_integer=" +
        is_integer + ", value=" + 
        (is_integer ? value : Double.longBitsToDouble(value)) + ")";
  }
}
