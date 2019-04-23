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
package net.opentsdb.data;

import java.util.List;

import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDBPlugin;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.TimeSeriesQuery;
import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.stats.Span;

/**
 * A data source factory that returns query nodes for a query execution
 * plan. This could return data from a local store or make remote calls.
 * 
 * @since 3.0
 */
public interface TimeSeriesDataSourceFactory extends TSDBPlugin, 
                                                     QueryNodeFactory{

  /**
   * The type of {@link TimeSeriesId}s returned from this store by default.
   * Byte IDs may need to be decoded.
   * @return A non-null type token.
   */
  public TypeToken<? extends TimeSeriesId> idType();
  
  /**
   * Called, potentially, before 
   * {@link #setupGraph(TimeSeriesQuery, QueryNodeConfig, net.opentsdb.query.plan.QueryPlanner)}
   * to determine if the data source handles the particular query. E.g. if
   * the source would handle the namespace or metric for the given query.
   * 
   * @param query The non-null parent query.
   * @param config The non-null query that would be passed to this factory.
   * @return True if the data source supports the query (even if the results
   * may be empty) or false if the source does not support the query, e.g.
   * maybe the source doesn't handle the particular namespace or metric.
   */
  public boolean supportsQuery(final TimeSeriesQuery query, 
                               final TimeSeriesDataSourceConfig config);
  
  /**
   * Whether or not the store supports pushing down the operation into
   * it's driver.
   * @param operation The operation we want to push down.
   * @return True if pushdown is supported, false if not.
   */
  public boolean supportsPushdown(
      final Class<? extends QueryNodeConfig> operation);
  
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
   * Converts the tag keys to byte arrays for stores that perform UID
   * encoding of strings.
   * @param join_keys A non-null and non-empty list of tag keys.
   * @param span An optional tracing span.
   * @return A non-null deferred resolving to a non-null list of results
   * of the same length and order as the given list. May resolve to an
   * exception.
   */
  public Deferred<List<byte[]>> encodeJoinKeys(final List<String> join_keys, 
                                               final Span span);
  
  /**
   * Converts the given metric names to byte arrays for stores that perform
   * UID encoding of strings.
   * @param join_metrics A non-null and non-emtpy list of of metrics.
   * @param spanAn optional tracing span.
   * @return A non-null deferred resolving to a non-null list of results
   * of the same length and order as the given list. May resolve to an
   * exception.
   */
  public Deferred<List<byte[]>> encodeJoinMetrics(
      final List<String> join_metrics, 
      final Span span);
  
  /** 
   * Return a rollup config for the source.
   * @return A potentially null rollup config for the source.
   */
  public RollupConfig rollupConfig();

}
