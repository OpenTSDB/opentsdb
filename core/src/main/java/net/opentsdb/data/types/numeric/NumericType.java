// This file is part of OpenTSDB.
// Copyright (C) 2010-2017  The OpenTSDB Authors.
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

import net.opentsdb.data.TimeSeriesDataType;

/**
 * Represents a single numeric data point.
 * <p>
 * Implementations of this interface aren't expected to be synchronized.
 */
public abstract class NumericType implements TimeSeriesDataType {

  /** The data type reference to pass around. */
  public static final TypeToken<NumericType> TYPE = TypeToken.of(NumericType.class);
  
  /**
   * Tells whether or not the this data point is a value of integer type.
   * @return {@code true} if the value is of integer type (call {@link #longValue()}), 
   * {@code false} if it's of double point type (call {@link #doubleValue()}.
   */
  public abstract boolean isInteger();

  /**
   * Returns the value of the this data point as a {@code long}.
   * @throws ClassCastException if the {@code isInteger() == false}.
   */
  public abstract long longValue();

  /**
   * Returns the value of the this data point as a {@code double}.
   * @throws ClassCastException if the {@code isInteger() == true}.
   */
  public abstract double doubleValue();

  /**
   * Returns the value of the this data point as a {@code double}, even if
   * it's a {@code long}.
   * @return When {@code isInteger() == false}, this method returns the same
   * thing as {@link #doubleValue}.  Otherwise, it returns the same thing as
   * {@link #longValue}'s return value casted to a {@code double}.
   */
  public abstract double toDouble();

  /**
   * Represents the number of real values behind this data point when referring
   * to a pre-aggregated and/or rolled up value.
   * @return The number of real values represented in this data point. Usually
   * just 1 for raw values.
   */
  public abstract long valueCount();

}
