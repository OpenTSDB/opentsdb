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
package net.opentsdb.query.execution.cache;

import java.util.concurrent.TimeUnit;

import io.opentracing.Span;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.query.context.QueryContext;
import net.opentsdb.query.execution.QueryExecution;

/**
 * An executor plugin that allows for various caching implementations, either
 * local or distributed. The interface works with byte arrays for maximum
 * flexibility and allows for either single key or multi-key writes and
 * reads.
 * <p>
 * Implementations should:
 * <ul>
 * <li>Throw {@link IllegalStateException} if the cache has not been initialized
 * prior to calling the cache or fetch methods.</li>
 * <li>Throw {@link IllegalArgumentException} if an invalid argument was given
 * in a cache or fetch call.</li>
 * </ul>
 * @since 3.0
 */
public abstract class QueryCachePlugin extends BaseTSDBPlugin {

  /**
   * Attempts to fetch a key from the cache. If no results were found, the 
   * deferred should resolve to a null. Note that temporary cache exceptions
   * should be logged and not returned upstream.
   * @param context A non-null query context used for tracing.
   * @param key A non-null and non-empty byte array key.
   * @param upstream_span An optional span for tracing.
   * @return A QueryExecution resolving to a null on cache miss, a value on 
   * cache hit or an exception if something went terribly wrong.
   * @throws IllegalStateException of the cache has not been initialized.
   * @throws IllegalArgumentException if the key was null or empty.
   */
  public abstract QueryExecution<byte[]> fetch(final QueryContext context, 
                                               final byte[] key,
                                               final Span upstream_span);
  
  /**
   * Attempts to fetch multiple keys from the cache in a single call. The 
   * resulting deferred array of byte arrays must have the same length of the
   * keys array and may not be null. Results must appear in the same order as
   * the given keys. Note that temporary cache exceptions should be logged and
   * not returned upstream.
   * @param context A non-null query context used for tracing.
   * @param keys A non-null and non-empty array of non-null and non-empty
   * byte arrays representing keys in the cache.
   * @param upstream_span An optional span for tracing.
   * @return A QueryExecution resolving to an array with nulls or values 
   * depending on key hits and misses or an exception if something went 
   * terribly wrong.
   * @throws IllegalStateException of the cache has not been initialized.
   * @throws IllegalArgumentException if the keys were null or empty.
   */
  public abstract QueryExecution<byte[][]> fetch(final QueryContext context,
                                                 final byte[][] keys,
                                                 final Span upstream_span);
  
  /**
   * Adds the given data to the cache using the given key. For expiring caches
   * the expiration and units should be set, otherwise they can be 0 and null.
   * @param key A non-null and non-empty key.
   * @param data A potentially empty data (for negative caching).
   * @param expiration A zero or positive integer indicating when the value
   * should expire in the future.
   * @param units The optional time units for the expiration.
   * @throws IllegalStateException of the cache has not been initialized.
   * @throws IllegalArgumentException if the key was null or empty.
   */
  public abstract void cache(final byte[] key, 
                             final byte[] data, 
                             final long expiration, 
                             final TimeUnit units);
  
  /**
   * Adds the given data to the cache using the keys. The key and data arrays
   * must be the same length. For expiring caches, the expiration and units
   * should be set, otherwise they can be 0 and null. 
   * @param keys A non-null and non-empty array of non-null and non-empty keys.
   * @param data A non-null and non-empty array of values matching the keys.
   * @param expirations A non-null and non-empty array of zero or positive 
   * integers indicating when the value should expire in the future.
   * @param units The optional time units for the expirations.
   * @throws IllegalStateException of the cache has not been initialized.
   * @throws IllegalArgumentException if the keys were null or empty or the
   * key and data arrays differed in length.
   */
  public abstract void cache(final byte[][] keys, 
                             final byte[][] data, 
                             final long[] expirations, 
                             final TimeUnit units);
  
}
