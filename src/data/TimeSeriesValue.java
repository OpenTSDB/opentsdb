// This file is part of OpenTSDB.
// Copyright (C) 2016  The OpenTSDB Authors.
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

import java.util.concurrent.TimeUnit;

/**
 * Represents a single object at a point in time. Time is represented in a 
 * Unix Epoch formatted integer with {@link timeUnits()} precision.
 * 
 * @param <T> A concrete type for the value.
 * @since 3.0
 */
public interface TimeSeriesValue<T> {
  
  /**
   * The units of time the timestamp is encoded to represent. The timestamp
   * is a Unix Epoch value in one of:
   * TimeUnits.SECONDS
   * TimeUnits.MILLISECONDS
   * TimeUnits.MICROSECONDS
   * TimeUnits.NANOSECONDS
   * 
   * @return The precision of the timestamp, may not be null.
   */
  public TimeUnit timeUnits();
  
  /**
   * The timestamp for the value in Unix Epoch format with {@link #timeUnits()}
   * encoding.
   * @return A zero or positive integer representing the timestamp for the value. 
   */
  public long timestamp();
  
  /**
   * Returns a non-null value at the given {@link timestamp()}.
   * @return A non-null value object. 
   */
  public T value();
}
