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
// see <http://www.gnu.org/licenses/>.package net.opentsdb.data;
package net.opentsdb.query.execution.serdes;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * TODO - better description and docs
 * 
 * @param <T> The type of data handled by this serdes class.
 * 
 * @since 3.0
 */
public abstract class TimeSeriesSerdes<T> {

  /**
   * Writes the given data to the stream.
   * @param stream A non-null stream to write to.
   * @param data A non-null data set.
   */
  public abstract void serialize(final OutputStream stream, 
                                 final T data);
  
  /**
   * Parses the given stream into the proper data object.
   * @param stream A non-null stream. May be empty.
   * @return A non-null data object.
   */
  public abstract T deserialize(final InputStream stream);
}
