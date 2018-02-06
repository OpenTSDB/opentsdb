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
