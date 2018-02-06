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

import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.query.pojo.TimeSeriesQuery;

/**
 * A class used to generate cache keys and expirations for a time series
 * query result.
 * 
 * @since 3.0
 */
public abstract class TimeSeriesCacheKeyGenerator extends BaseTSDBPlugin {

  /**
   * Generates a cache key based on the given query and whether or not to
   * include the time.
   * @param query A non-null query to generate a hash from.
   * @param with_timestamps Whether or not to include times when generating
   * the key.
   * @return A cache key.
   */
  public abstract byte[] generate(final TimeSeriesQuery query, 
                                  final boolean with_timestamps);
  
  /**
   * Generates an array of cache keys based on the given query and time ranges.
   * This is used for sliced queries where the same query is cut up into smaller
   * time slices.
   * @param query A non-null query to generate the hashes from.
   * @param time_ranges A non-null list of time ranges to generate keys from.
   * @return A non-null and non-empty array of keys.
   */
  public abstract byte[][] generate(final TimeSeriesQuery query, 
                                    final TimeStamp[][] time_ranges);
  
  /**
   * Generates an expiration duration (not timestamp) in milliseconds when the
   * cache should expire this query result.
   * @param query A query (if null, defaults are used).
   * @param expiration An expiration in milliseconds. 0 == do not cache, returns
   * 0. &lt; 0 returns expiration and nothing is calculated. &lt; 0 determines the
   * cache expiration using the query and current timestamp.
   * @return An expiration duration.
   */
  public abstract long expiration(final TimeSeriesQuery query, long expiration);
  
}
