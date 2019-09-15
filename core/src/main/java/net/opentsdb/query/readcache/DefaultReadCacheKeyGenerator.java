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

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.stumbleupon.async.Deferred;

import net.opentsdb.configuration.ConfigurationCallback;
import net.opentsdb.core.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.query.readcache.ReadCacheKeyGenerator;
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
 * @since 3.0
 */
public class DefaultReadCacheKeyGenerator 
  extends ReadCacheKeyGenerator implements ConfigurationCallback<Object> {
  
  public static final String TYPE = 
      DefaultReadCacheKeyGenerator.class.getSimpleName().toString();
  
  private static final Logger LOG = LoggerFactory.getLogger(
      DefaultReadCacheKeyGenerator.class);
  
  /** Default minimum expiration for current block in ms. */
  public static final String EXPIRATION_KEY = "tsd.query.cache.expiration";
  public static final long DEFAULT_EXPIRATION = 60000;
  
  /** Default maximum expiration for older blocks in ms. */
  public static final String MAX_EXPIRATION_KEY = "tsd.query.cache.max_expiration";
  public static final long DEFAULT_MAX_EXPIRATION = 86400000;

  /** Default interval for downsampler-less queries. */
  public static final String INTERVAL_KEY = "tsd.query.cache.default_interval";
  public static final String DEFAULT_INTERVAL = "1m";
  
  /** Default historical cutoff. 0 disables it. */
  public static final String HISTORICAL_CUTOFF_KEY = 
      "tsd.query.cache.historical_cutoff";
  public static final long DEFAULT_HISTORICAL_CUTOFF = 0;
  
  /** Default step interval for sliced query results. */
  public static final String STEP_INTERVAL_KEY = 
      "tsd.query.cache.step_interval";
  public static final long DEFAULT_STEP_INTERVAL = 0;
  
  /** The prefix to prepend */
  public static final byte[] CACHE_PREFIX = 
      new byte[] { 'T', 'S', 'D', 'B', 'Q' };

  /** The default expiration in milliseconds if no expiration was given. */
  protected volatile long default_expiration;
  
  /** The default max expiration for old data. */
  protected volatile long default_max_expiration;
  
  /** Default interval if no downsampler is given. E.g. 1m */
  protected volatile long default_interval;
  
  /** How long in the past (in millis) we expect data to be immutable. */
  protected volatile long historical_cutoff;
  
  /** If set along with historical cutoff, this is used as a denominator
   * to specify the size of blocks in sliced caching. */
  protected volatile long step_interval;
    
  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
    if (!tsdb.getConfig().hasProperty(EXPIRATION_KEY)) {
      tsdb.getConfig().register(EXPIRATION_KEY, DEFAULT_EXPIRATION, true,
          "The default expiration time for cache entries in milliseconds.");
    }
    default_expiration = tsdb.getConfig().getLong(EXPIRATION_KEY);
    tsdb.getConfig().bind(EXPIRATION_KEY, this);
    
    if (!tsdb.getConfig().hasProperty(MAX_EXPIRATION_KEY)) {
      tsdb.getConfig().register(MAX_EXPIRATION_KEY, DEFAULT_MAX_EXPIRATION, true,
          "The default maximum expiration time for cache entries "
          + "in milliseconds.");
    }
    default_max_expiration = tsdb.getConfig().getLong(MAX_EXPIRATION_KEY);
    tsdb.getConfig().bind(MAX_EXPIRATION_KEY, this);
    
    if (!tsdb.getConfig().hasProperty(INTERVAL_KEY)) {
      tsdb.getConfig().register(INTERVAL_KEY, DEFAULT_INTERVAL, true,
          "The default interval, in milliseconds, considered when a "
          + "query lacks a downsampler.");
    }
    default_interval = DateTime.parseDuration(
        tsdb.getConfig().getString(INTERVAL_KEY));
    tsdb.getConfig().bind(INTERVAL_KEY, this);
    
    if (!tsdb.getConfig().hasProperty(HISTORICAL_CUTOFF_KEY)) {
      tsdb.getConfig().register(HISTORICAL_CUTOFF_KEY, 
          DEFAULT_HISTORICAL_CUTOFF, true,
          "The cutoff in the past, in milliseconds, when data should be "
          + "immutable so we keep a query in the cache longer.");
    }
    historical_cutoff = tsdb.getConfig().getLong(HISTORICAL_CUTOFF_KEY);
    tsdb.getConfig().bind(HISTORICAL_CUTOFF_KEY, this);
    
    if (!tsdb.getConfig().hasProperty(STEP_INTERVAL_KEY)) {
      tsdb.getConfig().register(STEP_INTERVAL_KEY, 
          DEFAULT_STEP_INTERVAL, true,
          "The cutoff in the past, in milliseconds, when data should be "
          + "immutable so we keep a query in the cache longer.");
    }
    step_interval = tsdb.getConfig().getLong(STEP_INTERVAL_KEY);
    tsdb.getConfig().bind(STEP_INTERVAL_KEY, this);
    
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
  public byte[][] generate(final long query_hash, 
                           final String interval,
                           final int[] timestamps,
                           final long[] expirations) {
    final byte[] hash = Bytes.fromLong(query_hash);
    final byte[] interval_bytes = interval.getBytes(Const.ASCII_CHARSET);
    final byte[] key = new byte[CACHE_PREFIX.length + hash.length + interval.length() + 4];
    System.arraycopy(CACHE_PREFIX, 0, key, 0, CACHE_PREFIX.length);
    System.arraycopy(interval_bytes, 0, key, CACHE_PREFIX.length, interval_bytes.length);
    System.arraycopy(hash, 0, key, CACHE_PREFIX.length + interval_bytes.length, hash.length);
    
    final int now = (int) (DateTime.currentTimeMillis() / 1000L);
    final long ds_interval = expirations[0] > 0 ? expirations[0] : default_interval;
    final long segment_interval = DateTime.parseDuration(interval) / 1000;
    final byte[][] keys = new byte[timestamps.length][];
    for (int i = 0; i < timestamps.length; i++) {
      final byte[] copy = Arrays.copyOf(key, key.length);
      System.arraycopy(Bytes.fromInt(timestamps[i]), 0, 
          copy, CACHE_PREFIX.length + interval_bytes.length + hash.length, 4);
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
  public String type() {
    return TYPE;
  }

  @Override
  public String version() {
    return "3.0.0";
  }

  @Override
  public void update(final String key, final Object value) {
    if (key.equals(EXPIRATION_KEY)) {
      default_expiration = (long) value;
    } else if (key.equals(MAX_EXPIRATION_KEY)) {
      default_max_expiration = (long) value;
    } else if (key.equals(INTERVAL_KEY)) {
      default_interval = DateTime.parseDuration((String) value);
    } else if (key.equals(HISTORICAL_CUTOFF_KEY)) {
      historical_cutoff = (long) value;
    } else if (key.equals(STEP_INTERVAL_KEY)) {
      step_interval = (long) value;
    }
  }  
}
