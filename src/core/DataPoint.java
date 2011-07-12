// This file is part of OpenTSDB.
// Copyright (C) 2010  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.core;

/**
 * Represents a single data point.
 * <p>
 * Implementations of this interface aren't expected to be synchronized.
 */
public interface DataPoint {

  /**
   * Returns the timestamp (in seconds) associated with this data point.
   * @return A strictly positive, 32 bit integer.
   */
  long timestamp();

  /**
   * Tells whether or not the this data point is a value of integer type.
   * @return {@code true} if the {@code i}th value is of integer type,
   * {@code false} if it's of doubleing point type.
   */
  boolean isInteger();

  /**
   * Returns the value of the this data point as a {@code long}.
   * @throws ClassCastException if the {@code isInteger() == false}.
   */
  long longValue();

  /**
   * Returns the value of the this data point as a {@code double}.
   * @throws ClassCastException if the {@code isInteger() == true}.
   */
  double doubleValue();

  /**
   * Returns the value of the this data point as a {@code double}, even if
   * it's a {@code long}.
   * @return When {@code isInteger() == false}, this method returns the same
   * thing as {@link #doubleValue}.  Otherwise, it returns the same thing as
   * {@link #longValue}'s return value casted to a {@code double}.
   */
  double toDouble();

}
