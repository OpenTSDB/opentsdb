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


/** A mutable {@link DataPoint} that stores a double value and timestamp. */
public final class MutableDoubleDataPoint implements DataPoint {

  // NOTE: Fields are not final to make an instance available to hold a new
  // pair of a timestamp and a value to reduce memory overhead.
  /** The timestamp of a double value. */
  private long timestamp = Long.MAX_VALUE;
  /** The double value. */
  private double value = 0;

  /**
   * Resets with a new pair of a timestamp and a value to reduce memory burden.
   * @param timestamp An actual timestamp
   * @param value An actual value
   */
  public void reset(final long timestamp, final double value) {
    this.timestamp = timestamp;
    this.value = value;
  }

  /**
   * Resets with a new data point.
   * @param dp A new data point
   */
  public void reset(final DataPoint dp) {
    this.timestamp = dp.timestamp();
    this.value = dp.toDouble();
  }

  @Override
  public long timestamp() {
    return timestamp;
  }

  @Override
  public boolean isInteger() {
    return false;
  }

  @Override
  public long longValue() {
    throw new ClassCastException("This value is not a long in " + toString());
  }

  @Override
  public double doubleValue() {
    return value;
  }

  @Override
  public double toDouble() {
    return value;
  }

  @Override
  public String toString() {
    return String.format("timestamp = %d, double:value = %g", timestamp, value);
  }
}
