// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
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
public interface QueryInterpolator<T extends TimeSeriesDataType> {
  
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
