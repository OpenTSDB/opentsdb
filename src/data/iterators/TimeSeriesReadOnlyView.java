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

import java.util.List;

/**
 * Represents a view into a collection of time series. This interface is used 
 * to fetch a view of the set so that the source data can be iterated and 
 * processed.
 * <p>
 * Invariate: The underlying source must be immutable. 
 * @since 3.0
 */
public interface TimeSeriesReadOnlyView {
  
  /**
   * Returns a collection of time series objects as a "view" on the source data
   * for use in iteration.
   * 
   * NOTE that this method may be called many times by multiple sinks so each
   * set of series returned must be independent and not interact with each other.
   * 
   * @return A non-null list of time series. If the source is empty or no data
   * was found, the views should return an empty list.
   */
  public List<TimeSeries<? extends TimeSeriesValue<?>>> views();
}
