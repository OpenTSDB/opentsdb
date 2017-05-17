// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.query.execution.cache;

import java.util.Arrays;

import net.opentsdb.data.TimeStamp;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.utils.Bytes;

/**
 * A key generator for Redis cluster that wraps the hash of the query in 
 * curly braces so that multiple blocks of cache data will route to the same
 * slot in Redis. This allows for multi-key queries.
 * 
 * @since 3.0
 */
public class RedisClusterKeyGenerator extends DefaultTimeSeriesCacheKeyGenerator {

  @Override
  public byte[] generate(final TimeSeriesQuery query, 
                         final boolean with_timestamps) {
    if (query == null) {
      throw new IllegalArgumentException("Query cannot be null.");
    }
    final byte[] hash = with_timestamps ? 
        query.buildHashCode().asBytes() : 
        query.buildTimelessHashCode().asBytes();
    final byte[] key = new byte[hash.length + CACHE_PREFIX.length + 2];
    key[0] = '{';
    System.arraycopy(CACHE_PREFIX, 0, key, 1, CACHE_PREFIX.length);
    System.arraycopy(hash, 0, key, CACHE_PREFIX.length + 1, hash.length);
    key[key.length - 1] = '}';
    return key;
  }

  @Override
  public byte[][] generate(final TimeSeriesQuery query, 
                           final TimeStamp[][] time_ranges) {
    if (query == null) {
      throw new IllegalArgumentException("Query cannot be null.");
    }
    if (time_ranges == null) {
      throw new IllegalArgumentException("Time ranges cannot be null.");
    }
    if (time_ranges.length < 1) {
      throw new IllegalArgumentException("Time ranges cannot be empty.");
    }
    final byte[] hash = query.buildTimelessHashCode().asBytes();
    final byte[] key = new byte[hash.length + CACHE_PREFIX.length + 10];
    key[0] = '{';
    System.arraycopy(CACHE_PREFIX, 0, key, 1, CACHE_PREFIX.length);
    System.arraycopy(hash, 0, key, CACHE_PREFIX.length + 1, hash.length);
    key[CACHE_PREFIX.length + 1 + hash.length] = '}';
    
    final byte[][] keys = new byte[time_ranges.length][];
    for (int i = 0; i < time_ranges.length; i++) {
      final byte[] copy = Arrays.copyOf(key, key.length);
      System.arraycopy(Bytes.fromLong(time_ranges[i][0].msEpoch()), 0, 
          copy, hash.length + CACHE_PREFIX.length + 2, 8);
      keys[i] = copy;
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
