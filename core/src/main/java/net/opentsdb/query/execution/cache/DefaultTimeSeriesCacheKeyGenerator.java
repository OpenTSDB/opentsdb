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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * TODO - a log function
 * TODO - this needs to know about storage intervals for better expiration
 * calculations.
 * 
 * @since 3.0
 */
public class DefaultTimeSeriesCacheKeyGenerator 
  extends TimeSeriesCacheKeyGenerator {
  private static final Logger LOG = LoggerFactory.getLogger(
      DefaultTimeSeriesCacheKeyGenerator.class);
  
  /** Default minimum expiration for current block in ms. */
  public static final long DEFAULT_EXPIRATION = 60000;
  
  /** Default maximum expiration for older blocks in ms. */
  public static final long DEFAULT_MAX_EXPIRATION = 86400000;

  /** Default interval for downsampler-les queries. */
  public static final long DEFAULT_INTERVAL = 60000;
  
  /** Default historical cautoff. Disables it. */
  public static final long DEFAULT_HISTORICAL_CUTOFF = 0;
  
  /** The prefix to prepend */
  public static final byte[] CACHE_PREFIX = 
      new byte[] { 'T', 'S', 'D', 'B', 'Q' };

  /** The default expiration in milliseconds if no expiration was given. */
  protected long default_expiration;
  
  /** The default max expiration for old data. */
  protected long default_max_expiration;
  
  /** Default interval if no downsampler is given. E.g. 1m */
  protected long default_interval;
  
  /** How long in the past (in millis) we expect data to be immutable. */
  protected long historical_cutoff;
  
  /** If set along with historical cutoff, this is the interval to step up. */
  protected long step_interval;
    
  @Override
  public Deferred<Object> initialize(final TSDB tsdb) {
    if (tsdb.getConfig().hasProperty("tsd.query.cache.expiration")) {
      try {
        default_expiration = tsdb.getConfig().getLong("tsd.query.cache.expiration");
      } catch (NumberFormatException e) {
        return Deferred.fromResult(new IllegalArgumentException(
            "Failed to parse 'tsd.query.cache.expiration'"));
      }
    } else {
      default_expiration = DEFAULT_EXPIRATION;
    }
    if (tsdb.getConfig().hasProperty("tsd.query.cache.max_expiration")) {
      try {
        default_max_expiration = 
            tsdb.getConfig().getLong("tsd.query.cache.max_expiration");
      } catch (NumberFormatException e) {
        return Deferred.fromResult(new IllegalArgumentException(
            "Failed to parse 'tsd.query.cache.max_expiration'"));
      }
    } else {
      default_max_expiration = DEFAULT_MAX_EXPIRATION;
    }
    if (tsdb.getConfig().hasProperty("tsd.query.cache.default_interval")) {
      try {
        default_interval = DateTime.parseDuration(
            tsdb.getConfig().getString("tsd.query.cache.default_interval"));
      } catch (Exception e) {
        return Deferred.fromResult(new IllegalArgumentException(
            "Failed to parse 'tsd.query.cache.default_interval'"));
      }
    } else {
      default_interval = DEFAULT_INTERVAL;
    }
    if (tsdb.getConfig().hasProperty("tsd.query.cache.historical_cutoff")) {
      try {
        historical_cutoff = 
            tsdb.getConfig().getLong("tsd.query.cache.historical_cutoff");
      } catch (NumberFormatException e) {
        return Deferred.fromResult(new IllegalArgumentException(
            "Failed to parse 'tsd.query.cache.historical_cutoff'"));
      }
    } else {
      historical_cutoff = DEFAULT_HISTORICAL_CUTOFF;
    }
    if (tsdb.getConfig().hasProperty("tsd.query.cache.step_interval")) {
      try {
        step_interval = 
            tsdb.getConfig().getLong("tsd.query.cache.step_interval");
      } catch (NumberFormatException e) {
        return Deferred.fromResult(new IllegalArgumentException(
            "Failed to parse 'tsd.query.cache.step_interval'"));
      }
    } else {
      step_interval = 0;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Default expiration: " + default_expiration);
      LOG.debug("Default max expiration: " + default_max_expiration);
      LOG.debug("Default interval: " + default_interval);
      LOG.debug("Historical cutoff: " + historical_cutoff);
      LOG.debug("Step interval: " + step_interval);
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
    
    final TimeStamp end = query.getTime().endTime();
    final long timestamp = DateTime.currentTimeMillis();
    
    // for data older than the cutoff, always return the max
    if (historical_cutoff > 0 && (timestamp - end.msEpoch() > historical_cutoff)) {
      return default_max_expiration;
    }
    
    final long interval;
    if (query.getTime().getDownsampler() == null) {
      interval = default_interval;
    } else {
      interval = DateTime.parseDuration(query.getTime().getDownsampler()
          .getInterval());
    }
    
    long min_cache = ((timestamp - (timestamp % interval)) + interval) - timestamp;
    if (timestamp - end.msEpoch() < 0) {
      // this is the "now" block so only cache it for a tiny bit of time, till the
      // end of the interval.
      return min_cache;
    }
    
    // TODO - handle rolled up data.
    
    final long delta = (timestamp - end.msEpoch());
    if (historical_cutoff > 0 && delta > historical_cutoff) {
      return default_max_expiration;
    }
    
    // use step or not
    if (historical_cutoff > 0 && step_interval > 0) {
      // step
      if (step_interval > delta) {
        // this is the adjacent block and we don't want to cache it for very
        // long as it may receive updates.
        return min_cache;
      }
      long step = ((delta / step_interval) * step_interval) / 
          (historical_cutoff / step_interval);
      return step;
    }
    
    if (historical_cutoff > 0) {
      if (delta > historical_cutoff) {
        return default_max_expiration;
      }
    }
    return min_cache;
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
