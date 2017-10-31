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

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;

/**
 * A factory interface for generating interator interpolators.
 * 
 * @since 3.0
 */
public interface QueryIteratorInterpolatorFactory {

  /**
   * Returns a new interpolator for the given data type if present.
   * @param type A non-null data type for the interpolator to work on.
   * @param source A non-null time series source.
   * @param config An optional config for the interpolator.
   * @return An instantiated interpolator or null if an implementation does not
   * exist for the requested type. In such an event, the series should log and 
   * be dropped.
   */
  public QueryIteratorInterpolator<? extends TimeSeriesDataType> newInterpolator(
      final TypeToken<? extends TimeSeriesDataType> type, 
      final TimeSeries source, 
      final QueryIteratorInterpolatorConfig config);
}
