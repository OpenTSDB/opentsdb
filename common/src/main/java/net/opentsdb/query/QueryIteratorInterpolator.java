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
package net.opentsdb.query;

import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;

/**
 * A synthetic time series that implements an interpolator on top of a real
 * time series data source. When the underlying time series does not have a 
 * value for the requested time stamp, a fill policy is used to return a value.
 *
 * @param <T> The data type of the source and returned by the interpolator.
 * 
 * @since 3.0
 */
public interface QueryIteratorInterpolator<T extends TimeSeriesDataType> {
  
  /** @return Whether or not the underlying source has another real value. */
  public boolean hasNext();
  
  /**
   * @param timestamp A non-null timestamp to fetch a value at. 
   * @return A real or filled value. May be null. 
   */
  public TimeSeriesValue<T> next(final TimeStamp timestamp);
  
  /** @return The timestamp for the next real value if available. Returns null
   * if no real value is available. */
  public TimeStamp nextReal();
  
  /** @return The fill policy used by the interpolator. */
  public QueryFillPolicy<T> fillPolicy();
  
}
