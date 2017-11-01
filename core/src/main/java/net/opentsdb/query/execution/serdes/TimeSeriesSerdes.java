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

import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryResult;

/**
 * TODO - better description and docs
 * 
 * @param <T> The type of data handled by this serdes class.
 * 
 * @since 3.0
 */
public interface TimeSeriesSerdes {

  /**
   * Writes the given data to the stream.
   * @param context A non-null query context.
   * @param options Options for serialization.
   * @param stream A non-null stream to write to.
   * @param result A non-null data set.
   */
  public void serialize(final QueryContext context,
                        final SerdesOptions options,
                        final OutputStream stream, 
                        final QueryResult result);
  
  /**
   * Parses the given stream into the proper data object.
   * @param options Options for deserialization.
   * @param stream A non-null stream. May be empty.
   * @return A non-null query result.
   */
  public QueryResult deserialize(final SerdesOptions options, 
                                 final InputStream stream);
}
