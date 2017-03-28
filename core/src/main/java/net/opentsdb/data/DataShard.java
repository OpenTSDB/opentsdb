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

import net.opentsdb.data.iterators.TimeSeriesIterator;

/**
 * A collection of data points for a specific {@link TimeSeriesDataType}. All
 * values belong to the same {@link TimeSeriesId} as well as the same base time
 * and time interval.
 * 
 * @param <T> A {@link TimeSeriesDataType} representing the data.
 * 
 * @since 3.0
 */
public interface DataShard<T extends TimeSeriesDataType> {

  /** @return A non-null time series Id for all values in this series. */
  public TimeSeriesId id();
  
  /** @return A non-null base time stamp shared by all values in this set. 
   * Usually set to the query start time or some offset of it. */
  public TimeStamp startTime();
  
  /** @return A non-null final inclusive timestamp of the shard for which there 
   * may be data. Usually set to the query end time. */ 
  public TimeStamp endTime();
  
  /** @return The type of data stored in this shard. */
  public TypeToken<T> type();
  
  /** @return An optional order within a slice config. */
  public int order();
  
  /** @return Whether or not this shard was fetched from a cache. */
  public boolean cached();
  
  /** @return A non-null iterator to fetch values out of the shard. */
  public TimeSeriesIterator<T> iterator();
}
