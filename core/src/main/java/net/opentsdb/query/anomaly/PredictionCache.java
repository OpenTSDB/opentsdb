// This file is part of OpenTSDB.
// Copyright (C) 2019  The OpenTSDB Authors.
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
package net.opentsdb.query.anomaly;

import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDBPlugin;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.stats.Span;

/**
 * A cache for dealing with anomaly prediction results and state.
 * 
 * @since 3.0
 */
public interface PredictionCache extends TSDBPlugin {
  
  /**
   * Attempt to get a prediction from the cache.
   * @param context A non-null context.
   * @param key A non-null and non-empty key to fetch.
   * @param upstream_span An optional tracing span.
   * @return A deferred resolving to null on a cache miss or the result on 
   * success. Exceptions should be logged and swallowed.
   */
  public Deferred<QueryResult> fetch(final QueryPipelineContext context, 
                                     final byte[] key,  
                                     final Span upstream_span);
  
  /**
   * Attempts to write the entry to the cache for the given period.
   * @param key A non-null and non-empty key to cache.
   * @param expiration An expiration time span in milliseconds.
   * @param results A non-null result to cache. Can be empty though.
   * @param upstream_span An optional tracing span.
   * @return A deferred resolving to a null on success or an exception on failure.
   */
  public Deferred<Void> cache(final byte[] key,
                              final long expiration,
                              final QueryResult results,
                              final Span upstream_span);
  
  /**
   * Attempts to delete the cache entry.
   * @param key A non-null and non-empty key to cache.
   * @return A deferred resolving to a null on success or an exception on failure.
   */
  public Deferred<Void> delete(final byte[] key);
  
  /**
   * Attempts to fetch the prediction state for the given key.
   * @param key A non-null and non-empty key.
   * @return The state if found, null if not.
   * TODO - deferred? For now we keep it synchronous.
   */
  public AnomalyPredictionState getState(final byte[] key);
  
  /**
   * Attempts to store the state in the cache.
   * @param key key A non-null and non-empty key.
   * @param state A non-null state object.
   * @param expiration An expration time span in milliseconds.
   */
  public void setState(final byte[] key, 
                       final AnomalyPredictionState state, 
                       final long expiration);
  
  /**
   * Attempts to delete the state from the cache.
   * @param key key A non-null and non-empty key.
   */
  public void deleteState(final byte[] key);
  
}