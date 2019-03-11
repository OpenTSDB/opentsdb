// This file is part of OpenTSDB.
// Copyright (C) 2017-2018  The OpenTSDB Authors.
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
package net.opentsdb.query.serdes;

import com.stumbleupon.async.Deferred;

import net.opentsdb.data.PartialTimeSeries;
import net.opentsdb.data.PartialTimeSeriesSet;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.stats.Span;

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
   * @param span An optional tracer span.
   * @return A non-null deferred resolving to a null on successful
   * serialization or an exception if there was a failure.
   */
  public Deferred<Object> serialize(final QueryResult result,
                                    final Span span);
  
  /**
   * Writes the given data to the stream.
   * TODO - may change the response here a bit.
   * @param series The non-null partial time series to serialize.
   * @param span An optional tracer span.
   * @return A non-null deferred resolving to a null on successful
   * serialization or an exception if there was a failure.
   */
  public Deferred<Object> serialize(final PartialTimeSeries series, 
                                    final Span span);
  
  /**
   * Called when the set has finished fetching data and the total number of
   * {@link PartialTimeSeries} is known.
   * TODO - may change the response here a bit.
   * @param set The non-null set that is done.
   * @param span An optional tracing span.
   * @return A non-null deferred resolving to a null on successful
   * serialization or an exception if there was a failure.
   */
  public Deferred<Object> complete(final PartialTimeSeriesSet set, 
                                   final Span span);
  
  /**
   * Called when serialization is complete so the implementation can 
   * release resources.
   * @param span An optional tracer span.
   */
  public void serializeComplete(final Span span);
  
  /**
   * Parses the given stream into the proper data object.
   * @param node A non-null node to send the query results to on 
   * successful deserialization or call with an exception.
   * @param span An optional tracer span.
   * @return A non-null query result.
   */
  public void deserialize(final QueryNode node,
                          final Span span);
}
