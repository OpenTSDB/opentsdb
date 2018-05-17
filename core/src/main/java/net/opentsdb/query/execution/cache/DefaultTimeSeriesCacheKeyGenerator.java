// This file is part of OpenTSDB.
// Copyright (C) 2017-2018  The OpenTSDB Authors.
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

import net.opentsdb.configuration.ConfigurationCallback;
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
  extends TimeSeriesCacheKeyGenerator implements ConfigurationCallback<Object> {
  private static final Logger LOG = LoggerFactory.getLogger(
      DefaultTimeSeriesCacheKeyGenerator.class);
  
  /** Default minimum expiration for current block in ms. */
  public static final String EXPIRATION_KEY = "tsd.query.cache.expiration";
  public static final long DEFAULT_EXPIRATION = 60000;
  
  /** Default maximum expiration for older blocks in ms. */
  public static final String MAX_EXPIRATION_KEY = "tsd.query.cache.max_expiration";
  public static final long DEFAULT_MAX_EXPIRATION = 86400000;

  /** Default interval for downsampler-less queries. */
  public static final String INTERVAL_KEY = "tsd.query.cache.default_interval";
  public static final String DEFAULT_INTERVAL = "1m";
  
  /** Default historical cautoff. 0 disables it. */
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
  public Deferred<Object> initialize(final TSDB tsdb) {
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
    if (query.getMetrics().size() == 1 && 
        query.getMetrics().get(0).getDownsampler() != null) {
      // in this case we have a split query with one metric per query so
      // we can look to the metric's downsampler. If there were multiple
      // metrics then we can't really judge so we need to use the common
      // denominator.
      long ds_interval = DateTime.parseDuration(query.getMetrics().get(0).getDownsampler()
          .getInterval());
      if (ds_interval < 1) {
        interval = default_interval;
      } else {
        interval = ds_interval;
      }
    } else if (query.getTime().getDownsampler() != null) {
      long ds_interval = DateTime.parseDuration(query.getTime().getDownsampler()
          .getInterval());
      if (ds_interval < 1) {
        interval = default_interval;
      } else {
        interval = ds_interval;
      }
    } else {
      interval = default_interval;
    }
    
    long min_cache = ((timestamp - (timestamp % interval)) + interval) - timestamp;
    if (timestamp - end.msEpoch() < 0) {
      // this is the "now" block so only cache it for a tiny bit of time, till the
      // end of the interval.
      return min_cache;
    }
    
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
