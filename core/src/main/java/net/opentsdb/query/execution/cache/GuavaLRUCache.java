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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.utils.Bytes.ByteArrayKey;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.DateTime;

/**
 * A very simple and basic implementation of an on-heap, in-memory LRU cache 
 * using the Guava {@link Cache} class for configurable size and thread safety.
 * <p>
 * This implementation wraps byte array keys in a {@code ByteArrayKey} for 
 * proper use in Guava's map implementation (otherwise we'd be comparing the
 * byte array addresses and that's no use to us!). It also wraps all of the
 * values in a tiny class that captures the insert timestamp and an expiration
 * time. In this manner we can imitate an expiring cache as well in that if a
 * value is queried after it's computed expiration time, it's kicked out of
 * the cache.
 * <b>Note:</b> This implementation is super basic so there is a race condition
 * during read and write that may blow out valid cache objects when:
 * <ol>
 * <li>A thread performs a lookup and finds that the object has expired.</li>
 * <li>Another thread writes a new cache object with the new expiration.<li>
 * <li>The first thread calls {@link Cache#invalidate(Object)} with the key
 * and the new object is deleted.</li>
 * </ol>
 * This shouldn't happen too often for small installs and for bigger, distributed
 * installs, users should use a distributed cache instead.
 * <p>
 * Also note that the cache attempts to track the number of actual bytes of
 * values in the store (doesn't include Guava overhead, the keys or the 8 bytes
 * of expiration timestamp). Some objects will NOT be cached if the size is too
 * large. Guava will kick out some objects an invalidation, the size counter 
 * will be decremented, allowing the next cache call to hopefully write some 
 * data.
 * <p>
 * Also note that this version allows for null values and empty values. Keys
 * may not be null or empty though.
 * 
 * @since 3.0
 */
public class GuavaLRUCache extends CachingQueryExecutorPlugin {
  private static final Logger LOG = LoggerFactory.getLogger(GuavaLRUCache.class);
  
  /** The default size limit in bytes. 128MB. */
  public static final long DEFAULT_SIZE_LIMIT = 134217728;
  
  /** Default number of objects to maintain in the cache. */
  public static final int DEFAULT_MAX_OBJECTS = 1024;
  
  /** A counter used to track how many butes are in the cache. */
  private final AtomicLong size;
  
  /** A counter to track how many values have been expired out of the cache. */
  private final AtomicLong expired;
  
  /** The Guava cache implementation. */
  private Cache<ByteArrayKey, ExpiringValue> cache;
  
  /** The configured sized limit. */
  private long size_limit;
  
  /** The configured maximum number of objects. */
  private int max_objects; 
  
  /**
   * Default ctor.
   */
  public GuavaLRUCache() {
    size = new AtomicLong();
    expired = new AtomicLong();
    size_limit = DEFAULT_SIZE_LIMIT;
    max_objects = DEFAULT_MAX_OBJECTS;
  }
  
  @Override
  public Deferred<Object> initialize(final TSDB tsdb) {
    try {
      if (tsdb.getConfig().hasProperty("tsd.executor.plugin.guava.limit.objects")) {
        max_objects = tsdb.getConfig().getInt(
            "tsd.executor.plugin.guava.limit.objects");
      }
      if (tsdb.getConfig().hasProperty("tsd.executor.plugin.guava.limit.bytes")) {
        size_limit = tsdb.getConfig().getInt(
            "tsd.executor.plugin.guava.limit.bytes");
      }
      cache = CacheBuilder.newBuilder()
          .maximumSize(max_objects)
          .removalListener(new Decrementer())
          .recordStats()
          .build();
      
      return Deferred.fromResult(null);
    } catch (Exception e) {
      return Deferred.<Object>fromResult(e);
    }
  }
  
  @Override
  public Deferred<byte[]> fetch(final byte[] key) {
    if (cache == null) {
      throw new IllegalStateException("Cache has not been initialized.");
    }
    if (key == null) {
      throw new IllegalArgumentException("Key cannot be null.");
    }
    if (key.length < 1) {
      throw new IllegalArgumentException("Key must be at least 1 byte long.");
    }
    final ByteArrayKey cache_key = new ByteArrayKey(key);
    final ExpiringValue value = cache.getIfPresent(cache_key);
    if (value == null) {
      return Deferred.fromResult(null);
    }
    if (value.expired()) {
      // Note: there is a race condition here where a call to cache() can write
      // an updated version of the same key with a newer expiration. Since this
      // isn't a full, solid implementation of an expiring cache yet, this is
      // a best-effort run and may invalidate new data.
      cache.invalidate(cache_key);
      expired.incrementAndGet();
      return Deferred.fromResult(null);
    }
    return Deferred.fromResult(value.value);
  }

  @Override
  public Deferred<byte[][]> fetch(final byte[][] keys) {
    if (cache == null) {
      throw new IllegalStateException("Cache has not been initialized.");
    }
    if (keys == null) {
      throw new IllegalArgumentException("Keys cannot be null.");
    }
    if (keys.length < 1) {
      throw new IllegalArgumentException("Keys must be at least 1 byte long.");
    }
    final byte[][] results = new byte[keys.length][];
    for (int i = 0; i < keys.length; i++) {
      if (keys[i] == null) {
        throw new IllegalArgumentException("Key at index " + i + " was null.");
      }
      final ByteArrayKey cache_key = new ByteArrayKey(keys[i]);
      final ExpiringValue value = cache.getIfPresent(cache_key);
      if (value != null) { 
        if (value.expired()) {
          // Note: there is a race condition here where a call to cache() can write
          // an updated version of the same key with a newer expiration. Since this
          // isn't a full, solid implementation of an expiring cache yet, this is
          // a best-effort run and may invalidate new data.
          cache.invalidate(cache_key);
          expired.incrementAndGet();
        } else {
          results[i] = value.value;
        }
      }
    }
    return Deferred.fromResult(results);
  }

  @Override
  public void cache(final byte[] key, 
                    final byte[] data, 
                    final long expiration, 
                    final TimeUnit units) {
    if (cache == null) {
      throw new IllegalStateException("Cache has not been initialized.");
    }
    if (key == null) {
      throw new IllegalArgumentException("Key cannot be null.");
    }
    if (key.length < 1) {
      throw new IllegalArgumentException("Key length must be at least 1 byte.");
    }
    if (expiration < 1) {
      return;
    }
    // best effort
    if (size.get() + (data == null ? 0  : data.length) >= size_limit) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Will not cache key [" + Bytes.pretty(key) 
          + "] due to size limit.");
      }
      return;
    }
    cache.put(new ByteArrayKey(key), new ExpiringValue(data, expiration, units));
    if (data != null) {
      size.addAndGet(data.length);
    }
  }

  @Override
  public void cache(final byte[][] keys, 
                    final byte[][] data, 
                    final long expiration,
                    final TimeUnit units) {
    if (cache == null) {
      throw new IllegalStateException("Cache has not been initialized.");
    }
    if (keys == null) {
      throw new IllegalArgumentException("Keys array cannot be null.");
    }
    if (data == null) {
      throw new IllegalArgumentException("Data array cannot be null.");
    }
    if (keys.length != data.length) {
      throw new IllegalArgumentException("Key and data arrays must be of the "
          + "same length.");
    }
    if (expiration < 1) {
      return;
    }
    for (int i = 0; i < keys.length; i++) {
      if (keys[i] == null) {
        throw new IllegalArgumentException("Key at index " + i + " was null "
            + "and cannot be.");
      }
      // best effort
      if (size.get() + (data == null ? 0  : data.length) >= size_limit) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Will not cache key [" + Bytes.pretty(keys[i]) 
            + "] due to size limit.");
        }
        continue;
      }
      cache.put(new ByteArrayKey(keys[i]), 
          new ExpiringValue(data[i], expiration, units));
      if (data[i] != null) {
        size.addAndGet(data[i].length);
      }
    }
  }

  @Override
  public String id() {
    return "GuavaLRUCache";
  }

  @Override
  public String version() {
    return "3.0.0";
  }

  @Override
  public void collectStats(final StatsCollector collector) {
    if (collector == null) {
      return;
    }
    final CacheStats stats = cache.stats();
    collector.record("executor.plugin.guava.requestCount", stats.requestCount());
    collector.record("executor.plugin.guava.hitCount", stats.hitCount());
    collector.record("executor.plugin.guava.hitRate", stats.hitRate());
    collector.record("executor.plugin.guava.missCount", stats.missCount());
    collector.record("executor.plugin.guava.missRate", stats.missRate());
    collector.record("executor.plugin.guava.evictionCount", stats.evictionCount());
    collector.record("executor.plugin.guava.expiredCount", expired.get());
  }
  
  @VisibleForTesting
  Cache<ByteArrayKey, ExpiringValue> cache() {
    return cache;
  }
  
  @VisibleForTesting
  long bytesStored() {
    return size.get();
  }
  
  @VisibleForTesting
  long sizeLimit() {
    return size_limit;
  }
  
  @VisibleForTesting
  int maxObjects() {
    return max_objects;
  }
  
  @VisibleForTesting
  long expired() {
    return expired.get();
  }
  
  /** Super simple listener that decrements our size counter. */
  private class Decrementer implements 
      RemovalListener<ByteArrayKey, ExpiringValue> {
    @Override
    public void onRemoval(
        final RemovalNotification<ByteArrayKey, ExpiringValue> notification) {
      if (notification.getValue().value != null) {
        size.addAndGet(-notification.getValue().value.length);
      }
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
      return DateTime.nanoTime() > expires;
    }
    
  }
}
