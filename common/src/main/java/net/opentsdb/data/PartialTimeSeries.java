//This file is part of OpenTSDB.
//Copyright (C) 2019  The OpenTSDB Authors.
//
//This program is free software: you can redistribute it and/or modify it
//under the terms of the GNU Lesser General Public License as published by
//the Free Software Foundation, either version 2.1 of the License, or (at your
//option) any later version.  This program is distributed in the hope that it
//will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
//of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
//General Public License for more details.  You should have received a copy
//of the GNU Lesser General Public License along with this program.  If not,
//see <http://www.gnu.org/licenses/>.
package net.opentsdb.data;

import com.google.common.reflect.TypeToken;

/**
 * Represents data for a single type for a single time series for a slice of
 * time as defined in the {@link PartialTimeSeriesSet}. This will be pushed
 * upstream from downstream nodes.
 * 
 * @since 3.0
 */
public interface PartialTimeSeries<T extends TimeSeriesDataType> extends AutoCloseable {

  /** @return A hash to the time series ID. */
  public long idHash();
  
  /** @return The type of ID the series is associated with. */
  public TypeToken<? extends TimeSeriesId> idType();
  
  /** @return The non-null set this series is a part of. */
  public PartialTimeSeriesSet set();
  
  /** @return The data for this timeseries. May be null. See 
   * {@link TimeSeriesDataType#type()} for decoding the data. */
  public TimeSeriesDataType value();
  
}