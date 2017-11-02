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

import java.util.Comparator;

import com.google.common.reflect.TypeToken;

/**
 * Represents a single object at a point in time of the given 
 * {@link TimeSeriesDataType}. I.e. this is a single data point and the 
 * {@link #value()} can be any data type supported by OpenTSDB.
 * <p>
 * <b>WARNING:</b> Note that when a value is extracted from an iterator, a copy
 * should be made as the iterator may change the actual value on the next
 * iteration.
 *
 * @param <T> A {@link TimeSeriesDataType} object.
 * @since 3.0
 */
public interface TimeSeriesValue<T extends TimeSeriesDataType> {
  
  /** Public singleton comparator. */
  public static final TimeSeriesValueComparator COMPARATOR = 
      new TimeSeriesValueComparator();
  
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
   * The type of data this value represents.
   * @return A non-null type token for the given data type.
   */
  public TypeToken<T> type();
  
  /**
   * A comparator for TimeSeriesValues that <b>only</b> compares the timestamps
   * for sorting and ordering. Accepts null values.
   */
  public static class TimeSeriesValueComparator implements 
    Comparator<TimeSeriesValue<?>> {

    @Override
    public int compare(final TimeSeriesValue<?> v1, final TimeSeriesValue<?> v2) {
      if (v1 == null && v2 == null) {
        return 0;
      }
      if (v1 != null && v2 == null) {
        return -1;
      }
      if (v1 == null && v2 != null) {
        return 1;
      }
      if (v1 == v2) {
        return 0;
      }
      return TimeStamp.COMPARATOR.compare(v1.timestamp(), v2.timestamp());
    }
    
  }
}
