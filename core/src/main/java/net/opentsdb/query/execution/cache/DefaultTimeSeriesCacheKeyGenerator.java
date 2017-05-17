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

import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.DateTime;

/**
 * Simple implementation of the key generator that prepends keys with
 * "TSDBQ".
 * 
 * Two properties are read from the config:
 * <ul>
 * <li>tsd.query.cache.expiration - The default min expiration for the current
 * block in milliseconds.</li>
 * <li>tsd.query.cache.max_expiration - The default max expiration for older 
 * blocks.</li>
 * </ul>
 * 
 * TODO - a stepdown function.
 * 
 * @since 3.0
 */
public class DefaultTimeSeriesCacheKeyGenerator 
  extends TimeSeriesCacheKeyGenerator {
  
  /** Default minimum expiration for current block in ms. */
  public static final long DEFAULT_EXPIRATION = 60000;
  
  /** Defautl maximum expiration for older blocks in ms. */
  public static final long DEFAULT_MAX_EXPIRATION = 86400000;
  
  /** The prefix to prepend */
  public static final byte[] CACHE_PREFIX = 
      new byte[] { 'T', 'S', 'D', 'B', 'Q' };

  /** The default expiration in milliseconds if no expiration was given. */
  private long default_expiration;
  
  /** The default max expiration for old data. */
  private long default_max_expiration;
    
  @Override
  public Deferred<Object> initialize(final TSDB tsdb) {
    if (tsdb.getConfig().hasProperty("tsd.query.cache.expiration")) {
      default_expiration = tsdb.getConfig().getLong("tsd.query.cache.expiration");
    } else {
      default_expiration = DEFAULT_EXPIRATION;
    }
    if (tsdb.getConfig().hasProperty("tsd.query.cache.max_expiration")) {
      default_max_expiration = 
          tsdb.getConfig().getLong("tsd.query.cache.max_expiration");
    } else {
      default_max_expiration = DEFAULT_MAX_EXPIRATION;
    }
    
    return Deferred.fromResult(null);
  }
  
  @Override
  public byte[] generate(final TimeSeriesQuery query, 
                         final boolean with_timestamps) {
    if (query == null) {
      throw new IllegalArgumentException("Query cannot be null.");
    }
    final byte[] hash = with_timestamps ? 
        query.buildHashCode().asBytes() : 
        query.buildTimelessHashCode().asBytes();
    final byte[] key = new byte[hash.length + CACHE_PREFIX.length];
    System.arraycopy(CACHE_PREFIX, 0, key, 0, CACHE_PREFIX.length);
    System.arraycopy(hash, 0, key, CACHE_PREFIX.length, hash.length);
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
    final byte[] key = new byte[hash.length + CACHE_PREFIX.length + 8];
    System.arraycopy(CACHE_PREFIX, 0, key, 0, CACHE_PREFIX.length);
    System.arraycopy(hash, 0, key, CACHE_PREFIX.length, hash.length);
    
    final byte[][] keys = new byte[time_ranges.length][];
    for (int i = 0; i < time_ranges.length; i++) {
      final byte[] copy = Arrays.copyOf(key, key.length);
      System.arraycopy(Bytes.fromLong(time_ranges[i][0].msEpoch()), 0, 
          copy, hash.length + CACHE_PREFIX.length, 8);
      keys[i] = copy;
    }
    return keys;
  }
  
  @Override
  public long expiration(final TimeSeriesQuery query, final long expiration) {
    if (expiration == 0) {
      return 0;
    }
    if (expiration > 0) {
      return expiration;
    }
    
    if (query == null) {
      return default_expiration;
    }
    
    // calculate it
    final TimeStamp end = query.getTime().endTime();
    final long timestamp = DateTime.currentTimeMillis();
    
    // TODO - proper step-down function.
    if (timestamp - end.msEpoch() > (3600000 * 2)) {
      return default_max_expiration;
    }
    
    if (query.getTime().getDownsampler() == null) {
      return default_expiration;
    }
    
    final long interval = DateTime.parseDuration(
        query.getTime().getDownsampler().getInterval());
    final long result = (timestamp - (timestamp - (timestamp % interval)));
    return result > default_max_expiration ? default_max_expiration : result;
  }

  @Override
  public String id() {
    return "DefaultTimeSeriesCacheKeyGenerator";
  }

  @Override
  public String version() {
    return "3.0.0";
  }

}
