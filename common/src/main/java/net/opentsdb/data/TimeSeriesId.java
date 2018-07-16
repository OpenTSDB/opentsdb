//This file is part of OpenTSDB.
//Copyright (C) 2018  The OpenTSDB Authors.
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
 * An identifier for a time series. The identity can be as simple as the alias
 * or a combination of namespace, metrics, tags, etc.
 * <p>
 * TODO - further docs
 * 
 * @since 3.0
 */
public interface TimeSeriesId {

  /**
   * @return True if the fields are encoded using a format specified by the 
   * storage engine.
   */
  public boolean encoded();
  
  /**
   * The type of series dealt with. Either a {@link TimeSeriesByteId} or
   * {@link TimeSeriesStringId}
   * @return A non-null type token.
   */
  public TypeToken<? extends TimeSeriesId> type();

}
