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

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;

/**
 * A factory that returns iterators for a {@link QueryNode} to mutate time series
 * flowing through them.
 * 
 * @since 3.0
 */
public interface QueryIteratorFactory {
  
  /**
   * Returns an iterator using a non-keyed collection of time series sources.
   * @param node A non-null query node the iterator belongs to.
   * @param sources A non-null and non-empty list of time series sources to
   * read from. 
   * @return A non-null iterator over a specific data type.
   */
  public Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> newIterator(
      final QueryNode node,
      final Collection<TimeSeries> sources);

  /**
   * Returns an iterator using a keyed collection of time series sources.
   * @param node A non-null query node the iterator belongs to.
   * @param sources A non-null and non-empty list of time series sources to
   * read from. 
   * @return A non-null iterator over a specific data type.
   */
  public Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> newIterator(
      final QueryNode node,
      final Map<String, TimeSeries> sources);
}
