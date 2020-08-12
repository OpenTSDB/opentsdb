// This file is part of OpenTSDB.
// Copyright (C) 2019-2020  The OpenTSDB Authors.
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
package net.opentsdb.query.anomaly;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QueryResultId;
import net.opentsdb.query.readcache.ReadCacheQueryResult;
import net.opentsdb.query.readcache.ReadCacheSerdes;
import net.opentsdb.query.readcache.ReadCacheSerdesFactory;
import net.opentsdb.stats.Span;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.JSON;
import net.opentsdb.utils.Bytes.ByteArrayKey;

/**
 * Super simple in-memory prediction cache used for testing purposes.
 * 
 * @since 3.0
 */
public class MemoryPredictionCache extends BaseTSDBPlugin implements PredictionCache {
  public static final String TYPE = MemoryPredictionCache.class.getSimpleName().toString();
  private static final Logger LOG = LoggerFactory.getLogger(MemoryPredictionCache.class);
  
  public static final String KEY_PREFIX = "anomaly.MemoryPredictionCache.";
  public static final String SERDES_KEY = "serdes.id";
  public static final byte[] STATE_KEY = "TSDBANOMALYSTATE".getBytes(Const.ASCII_CHARSET);
  
  /** The Guava cache implementation. */
  private Cache<ByteArrayKey, ExpiringValue> cache;
  
  /** The serdes plugin for this cache instance. */
  private ReadCacheSerdes serdes;
  
  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
    registerConfigs(tsdb);
    try {
      final String serdes_id = tsdb.getConfig().getString(getConfigKey(SERDES_KEY));
      final ReadCacheSerdesFactory factory = tsdb.getRegistry().getPlugin(
          ReadCacheSerdesFactory.class, serdes_id);
      if (factory == null) {
        return Deferred.fromError(new IllegalArgumentException("No serdes factory found for " +
            (Strings.isNullOrEmpty(serdes_id) ? "Default" : serdes_id)));
      }
      serdes = factory.getSerdes();
      cache = CacheBuilder.newBuilder()
          .recordStats()
          .build();
      LOG.info("Initialized memory prediction cache");
      return Deferred.fromResult(null);
    } catch (Exception e) {
      return Deferred.<Object>fromResult(e);
    }
  }
  
  @Override
  public Deferred<QueryResult> fetch(QueryPipelineContext context, byte[] key,
      Span upstream_span) {
    final ByteArrayKey cache_key = new ByteArrayKey(key);
    final ExpiringValue value = cache.getIfPresent(cache_key);
    if (value== null || value.expired()) {
      return Deferred.fromResult(null);
    } else {
      Map<QueryResultId, ReadCacheQueryResult> map = serdes.deserialize(context, value.value);
      ReadCacheQueryResult result = null;
      for (final Entry<QueryResultId, ReadCacheQueryResult> entry : map.entrySet()) {
        if (entry.getValue() != null) {
          result = entry.getValue();
          break;
        }
      }
      return Deferred.fromResult(result);
    }
  }

  @Override
  public Deferred<Void> cache(byte[] key, long expiration, QueryResult results,
      Span upstream_span) {
    try {
      final byte[] data = serdes.serialize(Lists.newArrayList(results));
      cache.put(new ByteArrayKey(key), 
          new ExpiringValue(data, expiration, TimeUnit.MILLISECONDS));
      return Deferred.fromResult(null);
    } catch (Exception e) {
      return Deferred.fromError(e);
    }
  }

  @Override
  public Deferred<Void> delete(byte[] key) {
    cache.invalidate(new ByteArrayKey(key));
    return Deferred.fromResult(null);
  }

  @Override
  public AnomalyPredictionState getState(byte[] key) {
    byte[] prefixed_key = new byte[key.length + STATE_KEY.length];
    System.arraycopy(STATE_KEY, 0, prefixed_key, 0, STATE_KEY.length);
    System.arraycopy(key, 0, prefixed_key, STATE_KEY.length, key.length);
    final ByteArrayKey cache_key = new ByteArrayKey(prefixed_key);
    final ExpiringValue value = cache.getIfPresent(cache_key);
    if (value== null || value.expired()) {
      return null;
    } else {
      return JSON.parseToObject(value.value, AnomalyPredictionState.class);
    }
  }

  @Override
  public void setState(byte[] key, AnomalyPredictionState state, final long expiration) {
    byte[] prefixed_key = new byte[key.length + STATE_KEY.length];
    System.arraycopy(STATE_KEY, 0, prefixed_key, 0, STATE_KEY.length);
    System.arraycopy(key, 0, prefixed_key, STATE_KEY.length, key.length);
    byte[] data = JSON.serializeToBytes(state);
    cache.put(new ByteArrayKey(prefixed_key), 
        new ExpiringValue(data, expiration, TimeUnit.MILLISECONDS));
  }

  @Override
  public void deleteState(final byte[] key) {
    byte[] prefixed_key = new byte[key.length + STATE_KEY.length];
    System.arraycopy(STATE_KEY, 0, prefixed_key, 0, STATE_KEY.length);
    System.arraycopy(key, 0, prefixed_key, STATE_KEY.length, key.length);
    cache.invalidate(new ByteArrayKey(prefixed_key));
  }
  
  @Override
  public String type() {
    return TYPE;
  }  
  
  /**
   * Helper to build the config key with a factory id.
   *
   * @param suffix The non-null and non-empty config suffix.
   * @return The key containing the id.
   */
  @VisibleForTesting
  String getConfigKey(final String suffix) {
    if (id == null || id == TYPE) { // yes, same addy here.
      return KEY_PREFIX + suffix;
    } else {
      return KEY_PREFIX + id + "." + suffix;
    }
  }
  
  void registerConfigs(final TSDB tsdb) {
    if (!tsdb.getConfig().hasProperty(getConfigKey(SERDES_KEY))) {
      tsdb.getConfig().register(getConfigKey(SERDES_KEY), null, false, 
          "The ID of a cache serdes plugin to load.");
    }
  }
  
  /** Wrapper around a value that stores the expiration timestamp. */
  private class ExpiringValue {
    /** The value stored in the cache. */
    private final byte[] value;
    
    /** The expiration timestamp in unix epoch nanos. */
    private final long expires;
    
    /**
     * Default ctor.
     * @param value A value (may be null)
     * @param expiration The expiration value count in time units.
     * @param units The time units of the expiration.
     */
    public ExpiringValue(final byte[] value, 
                         final long expiration, 
                         final TimeUnit units) {
      this.value = value;
      switch (units) {
      case SECONDS:
        expires = DateTime.nanoTime() + (expiration * 1000 * 1000 * 1000);
        break;
      case MILLISECONDS:
        expires = DateTime.nanoTime() + (expiration * 1000 * 1000);
        break;
      case NANOSECONDS:
        expires = DateTime.nanoTime() + expiration;
        break;
      default:
        throw new IllegalArgumentException("Unsupported units: " + units);
      }
    }
    
    /** @return Whether or not the value has expired. */
    public boolean expired() {
      return expires == 0 ? false : DateTime.nanoTime() > expires;
    }
    
    @Override
    public String toString() {
      return new StringBuilder()
          .append("{value=")
          .append(Bytes.pretty(value))
          .append(", expires=")
          .append(expires)
          .toString();
    }
  }

}
