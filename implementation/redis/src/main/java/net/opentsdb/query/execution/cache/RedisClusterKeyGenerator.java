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

import java.util.Arrays;

import net.opentsdb.core.Const;
import net.opentsdb.query.readcache.DefaultReadCacheKeyGenerator;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.DateTime;

/**
 * A key generator for Redis cluster that wraps the hash of the query in 
 * curly braces so that multiple blocks of cache data will route to the same
 * slot in Redis. This allows for multi-key queries.
 * 
 * @since 3.0
 */
public class RedisClusterKeyGenerator extends DefaultReadCacheKeyGenerator {

  @Override
  public byte[][] generate(final long query_hash, 
                           final String interval,
                           final int[] timestamps,
                           final long[] expirations) {
    final byte[] hash = Bytes.fromLong(query_hash);
    final byte[] interval_bytes = interval.getBytes(Const.ASCII_CHARSET);
    final byte[] key = new byte[CACHE_PREFIX.length 
                                + hash.length 
                                + interval.length() 
                                + 4       // timestamp
                                + 2];     // the brackets.
    int idx = 0;
    key[idx++] = '{';
    System.arraycopy(CACHE_PREFIX, 0, key, idx, CACHE_PREFIX.length);
    idx += CACHE_PREFIX.length;
    System.arraycopy(interval_bytes, 0, key, idx, interval_bytes.length);
    idx += interval_bytes.length;
    System.arraycopy(hash, 0, key, idx, hash.length);
    idx += hash.length;
    key[idx++] = '}';
    
    final int now = (int) (DateTime.currentTimeMillis() / 1000L);
    final long ds_interval = expirations[0] > 0 ? expirations[0] : default_interval;
    final long segment_interval = DateTime.parseDuration(interval) / 1000;
    final byte[][] keys = new byte[timestamps.length][];
    for (int i = 0; i < timestamps.length; i++) {
      final byte[] copy = Arrays.copyOf(key, key.length);
      System.arraycopy(Bytes.fromInt(timestamps[i]), 0, copy, idx, 4);
      keys[i] = copy;
      
      // expiration
      if (timestamps[i] >= now) {
        expirations[i] = default_expiration;
      } else {
        long delta = now - timestamps[i];
        if (historical_cutoff > 0 && delta > (historical_cutoff / 1000)) {
          // we have an old segment we don't expect to update so just keep it as
          // long as configured.
          expirations[i] = default_max_expiration;
        } else if (delta < (ds_interval / 1000)) {
          // we're less than a full ds interval into the segment so we want to 
          // cache even less.
          expirations[i] = delta * 1000;
        } else if (delta <= segment_interval) {
          // "now" is within the current segment so we keep it only for a ds
          // interval.
          expirations[i] = ds_interval;
        } else {
          // now we can backoff, but we need to account for the possibility 
          // that this segment is adjacent to the "now" segment and may have 
          // data that would be updated. So we'll subtract a segment worth of
          // ds from the diff.
          delta /= (ds_interval / 1000);
          delta -= (segment_interval / (ds_interval / 1000));
          expirations[i] = delta * ds_interval;
        }
        if (expirations[i] < default_expiration) {
          expirations[i] = default_expiration;
        }
      }
    }
    return keys;
  }
  
  @Override
  public String id() {
    return "RedisClusterKeyGenerator";
  }

  @Override
  public String version() {
    return "3.0.0";
  }

}
