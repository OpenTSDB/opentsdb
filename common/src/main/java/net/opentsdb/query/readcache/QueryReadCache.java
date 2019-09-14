// This file is part of OpenTSDB.
// Copyright (C) 2017-2019  The OpenTSDB Authors.
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
package net.opentsdb.query.readcache;

import java.util.Collection;

import com.stumbleupon.async.Deferred;

import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.stats.Span;

/**
 * A read cache interface that is responsible for caching QueryResults and slicing
 * them into segments.
 * <p>
 * Implementations should:
 * <ul>
 * <li>Throw {@link IllegalStateException} if the cache has not been 
 * initialized prior to calling the cache or fetch methods.</li>
 * <li>Throw {@link IllegalArgumentException} if an invalid argument was 
 * given in a cache or fetch call.</li>
 * </ul>
 * 
 * @since 3.0
 */
public interface QueryReadCache {
  
 /**
  * Attempts to fetch multiple keys from the cache in a single call. The 
  * resulting deferred array of byte arrays must have the same length of the
  * keys array and may not be null. Results must appear in the same order as
  * the given keys. Note that temporary cache exceptions should be logged and
  * not returned upstream.
  * @param context A non-null query context used for tracing.
  * @param keys A non-null and non-empty array of non-null and non-empty
  * byte arrays representing keys in the cache.
  * @param A callback executed with results or errors.
  * @param upstream_span An optional span for tracing.
  */
  public void fetch(final QueryPipelineContext context, 
                    final byte[][] keys, 
                    final ReadCacheCallback callback, 
                    final Span upstream_span);

 /**
  * Adds the given data to the cache using the given key. For expiring caches
  * the expiration and units should be set, otherwise they can be 0 to keep it
  * in cache as long as possible.
  * @param timestamp The Unix epoch timestamp in seconds.
  * @param key A non-null and non-empty key.
  * @param expiration A zero or positive integer indicating when the value
  * should expire in the future.
  * @param results A collection of zero or more results. An empty collection
  * can be used for negative caching.
  * @param upstream_span An optional span for tracing.
  * @return A deferred resolving to null or an exception if something failed.
  * @throws IllegalStateException of the cache has not been initialized.
  * @throws IllegalArgumentException if the key was null or empty.
  */
  public Deferred<Void> cache(final int timestamp,
                              final byte[] key,
                              final long expiration,
                              final Collection<QueryResult> results,
                              final Span upstream_span);

 /**
  * Adds the given data to the cache using the keys. The key and data arrays
  * must be the same length. For expiring caches, the expiration and units
  * should be set, otherwise they can be 0 and null. The query results will be
  * sliced up into segments.
  * @param timestamps An array of Unix epoch timestamps in seconds, must be
  * the same size as the keys and expirations.
  * @param keys A non-null and non-empty array of non-null and non-empty keys.
  * @param expirations A non-null and non-empty array of zero or positive 
  * integers indicating when the value should expire in the future.
  * @param results A collection of query results to slice up into segments based
  * on the timestamp.
  * @param upstream_span An optional span for tracing.
  * @return A deferred resolving to null or an exception if something failed.
  * @throws IllegalStateException of the cache has not been initialized.
  * @throws IllegalArgumentException if the keys were null or empty or the
  * key and data arrays differed in length.
  */
  public Deferred<Void> cache(final int[] timestamps,
                              final byte[][] keys,
                              final long[] expirations,
                              final Collection<QueryResult> results,
                              final Span upstream_span);
   
}
