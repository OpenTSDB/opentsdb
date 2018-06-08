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
package net.opentsdb.storage;

import java.util.List;

import com.stumbleupon.async.Deferred;

import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.stats.Span;

/**
 * The class for reading or writing time series data to a local data store. 
 * <p>
 * This class is generally meant to implement a time series storage schema on
 * either:
 * <ul>
 * <li>A local system such as using flat files or an LSM implementation on the
 * local disk using something like RocksDB or LevelDB.<li>
 * <li>A remote distributed store such as HBase, Bigtable or Cassandra.<li>
 * </ul>
 *
 * TODO - more complete calls and documentation
 * 
 * @since 3.0
 */
public interface TimeSeriesDataStore {
  

  /**
   * Instantiates a new node using the given context and the default
   * configuration for this node.
   * @param context A non-null query pipeline context.
   * @param id An ID for this node.
   * @return An instantiated node if successful.
   */
  public QueryNode newNode(final QueryPipelineContext context, 
                           final String id);
  
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
   * The descriptive ID of the factory used when parsing queries.
   * @return A non-null unique ID of the factory.
   */
  public String id();
  
  /** @return A class to use for serdes for configuring nodes of this
   * type. */
  public Class<? extends QueryNodeConfig> nodeConfigClass();
  
  /**
   * Writes the given value to the data store.
   * @param id A non-null ID for the value.
   * @param value A non-null value to write.
   * @param trace An optional tracer.
   * @param span An optional span for tracing.
   * @return A deferred resolving to null on success or an exception if the 
   * value was unable to be written.
   */
  public Deferred<Object> write(final TimeSeriesStringId id,
                                         final TimeSeriesValue<?> value, 
                                         final Span span);
  
  /**
   * For stores that are able to encode time series IDs, this method should
   * resolve the IDs to a string ID suitable for display or further 
   * processing.
   * 
   * @param id A non-null byte ID.
   * @param span An optional tracing span.
   * @return A deferred resolving to the string ID or an exception on
   * failure.
   */
  public Deferred<TimeSeriesStringId> resolveByteId(final TimeSeriesByteId id, 
                                                    final Span span);

  /**
   * TODO - doc
   * @param join_keys
   * @param span
   * @return
   */
  public Deferred<List<byte[]>> encodeJoinKeys(final List<String> join_keys, 
                                               final Span span);
  
  /**
   * Releases resources held by the store. 
   * @return A deferred resolving to null. 
   */
  public Deferred<Object> shutdown();
}
