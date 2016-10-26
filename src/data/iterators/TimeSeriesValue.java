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
package net.opentsdb.data.iterators;

/**
 * Represents a single time series value.
 * 
 * @param <T> The type of the value at this timestamp.
 * @since 3.0
 */
public interface TimeSeriesValue<T> {
  
  /**
   * @return The timestamp associated with this value.
   */
  public long timestamp();
  
  /**
   * @return The value at this timestamp.
   */
  public T value();

}
