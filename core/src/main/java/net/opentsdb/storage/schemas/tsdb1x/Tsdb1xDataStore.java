// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
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
package net.opentsdb.storage.schemas.tsdb1x;

import com.stumbleupon.async.Deferred;

import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.stats.Span;

/**
 * An interface for data stores that implement the TSDB v1 schema of
 * encoding data in wide column store like HBase and Bigtable.
 * 
 * TODO - more work on this
 * 
 * @since 3.0
 */
public interface Tsdb1xDataStore {
  
  /**
   * Instantiates a new node using the given context and config.
   * @param context A non-null query pipeline context.
   * @param id An ID for this node.
   * @param config A query node config. May be null if the node does not
   * require a configuration.
   * @return An instantiated node if successful.
   */
  public QueryNode newNode(final QueryPipelineContext context, 
                           final String id,
                           final QueryNodeConfig config);
  
  /**
   * Write the given value to the data store.
   * @param id The time series ID.
   * @param value The time series value.
   * @param span An optional tracing span.
   * @return A deferred resolving to null on success or an exception on
   * error.
   */
  public Deferred<Object> write(final TimeSeriesStringId id, 
                                final TimeSeriesValue<?> value, 
                                final Span span);
  
  /** @return The ID of this time series data store instance. */
  public String id();
}
