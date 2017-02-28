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
package net.opentsdb.data;

import com.google.common.reflect.TypeToken;

/**
 * Represents a single object at a point in time of the given 
 * {@link TimeSeriesDataType}. I.e. this is a single data point and the 
 * {@link #value()} can be any data type supported by OpenTSDB.
 * <p>
 * <b>WARNING:</b> Note that when a value is extracted from an iterator, a copy
 * should be made using {@link #getCopy()}.
 *
 * @param <T> A {@link TimeSeriesDataType} object.
 * @since 3.0
 */
public interface TimeSeriesValue<T> {
  /**
   * A reference to the time series ID associated with this value.
   * @return A non-null {@link TimeSeriesId}.
   */
  public TimeSeriesId id();
  
  /**
   * The timestamp associated with this value.
   * @return A non-null {@link TimeStamp}.
   */
  public TimeStamp timestamp();
  
  /**
   * Returns a non-null value at the given {@link #timestamp()}.
   * @return A non-null value of the {@link TimeSeriesDataType} type. 
   */
  public T value();
  
  /**
   * A count of the number of real underlying values represented in this data
   * point. If this was a completely synthetic or "filled" data point then the
   * count will be zero. Non-zero values indicates that at least one actual
   * value went into the presentation of this point.
   * @return A non-negative value.
   */
  public int realCount();
  
  /**
   * The {@link TimeSeriesDataType} data type of this value.
   * @return A non-null type token.
   */
  public TypeToken<?> type();
  
  /**
   * Creates and returns a deep copy of this value. Useful if the value was
   * retrieved from an iterator that may modify the underlying data.
   * @return A non-null deep copy of the current value.
   */
  public TimeSeriesValue<T> getCopy();
}
