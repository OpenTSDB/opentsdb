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

import net.opentsdb.core.BaseTSDBPlugin;

/**
 * A class used to generate cache keys and expirations for a time series
 * query result.
 * 
 * @since 3.0
 */
public abstract class ReadCacheKeyGenerator extends BaseTSDBPlugin {

  /**
   * Generates an array of cache keys based on the given query has and time ranges 
   * and populates an array with expirations in milliseconds.
   * This is used for sliced queries where the same query is cut up into smaller
   * time slices.
   * <b>NOTE:</b> As an ugly hack we expect expirations[0] to contain the 
   * downsample interval in ms if present.
   * @param query_hash The hash of a query.
   * @param interval An interval for the size of the cached segment.
   * @param time_ranges A non-null list of time ranges to generate keys from in
   * epoch seconds.
   * @param expirations An array of expiration timestamps the method will fill
   * with durations in milliseconds.
   * @return A non-null and non-empty array of keys.
   */
  public abstract byte[][] generate(final long query_hash,
                                    final String interval,
                                    final int[] time_ranges,
                                    final long[] expirations);
}
