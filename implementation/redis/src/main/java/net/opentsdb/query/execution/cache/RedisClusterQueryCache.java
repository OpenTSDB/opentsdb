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

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.readcache.QueryReadCache;
import net.opentsdb.query.readcache.ReadCacheCallback;
import net.opentsdb.query.readcache.ReadCacheQueryResult;
import net.opentsdb.query.readcache.ReadCacheQueryResultSet;
import net.opentsdb.query.readcache.ReadCacheSerdes;
import net.opentsdb.query.readcache.ReadCacheSerdesFactory;
import net.opentsdb.stats.Span;
import net.opentsdb.utils.ByteCache;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

/**
 * A cache implementation that supports a Redis cluster, i.e. native Redis 
 * clustering. All methods will try to swallow non-data related exceptions and 
 * simply log them instead of impeding upstream processing.
 * 
 * @since 3.0
 */
public class RedisClusterQueryCache extends BaseTSDBPlugin 
    implements ByteCache, QueryReadCache {
  private static final Logger LOG = LoggerFactory.getLogger(
      RedisClusterQueryCache.class);

  public static final String TYPE = "RedisCluterQueryCache";
  
  /** Configuration keys. */
  public static final String HOSTS_KEY = "redis.query.cache.hosts";
  public static final String SHARED_OBJECT_KEY = 
      "redis.query.cache.shared_object";
  public static final String SERDES_KEY = "redis.query.cache.serdes.id";
  
  /** Redis flag: Write if the key does not exist. */
  public static final byte[] NX = new byte[] { 'N', 'X' };
  
  /** Redis flag: Expiration time is in ms. */
  public static final byte[] EXP = new byte[] { 'P', 'X' };
  
  /** The TSDB we belong to. */
  private TSDB tsdb;
  
  /** The cluster object. */
  private JedisCluster cluster;
  
  /** The serdes plugin for this cache instance. */
  private ReadCacheSerdes serdes;
  
  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
    this.tsdb = tsdb;
    // Two or more implementations may be in play so check first
    if (!tsdb.getConfig().hasProperty(HOSTS_KEY)) {
      tsdb.getConfig().register(HOSTS_KEY, (String) null, false /* todo */, 
          "A comma separated list of hosts in the format "
          + "<host>:<port>,<host>:<port>.");
    }
    if (!tsdb.getConfig().hasProperty(SHARED_OBJECT_KEY)) {
      tsdb.getConfig().register(SHARED_OBJECT_KEY, (String) null, false, 
          "The string ID of an optional shared object to use for "
          + "sharing a client connection pool.");
    }
    if (!tsdb.getConfig().hasProperty(SERDES_KEY)) {
      tsdb.getConfig().register(SERDES_KEY, (String) null, false, 
          "The ID of a read cache serdes plugin factory.");
    }
    
    String hosts = tsdb.getConfig().getString(HOSTS_KEY);
    if (hosts == null || hosts.isEmpty()) {
      return Deferred.fromError(new IllegalArgumentException("Missing the "
          + "'" + HOSTS_KEY +"' config"));
    }
    final Set<HostAndPort> nodes = new HashSet<HostAndPort>();
    try {
      final String[] pairs = hosts.split(",");
      for (final String pair : pairs) {
        final String[] host_and_port = pair.split(":");
        nodes.add(new HostAndPort(host_and_port[0], 
            Integer.parseInt(host_and_port[1])));
      }
    } catch (RuntimeException e) {
      return Deferred.fromError(new IllegalArgumentException(
          "Failure parsing host and ports for redis cluster: " + hosts, e));
    }
    
    final String shared_object = tsdb.getConfig().getString(SHARED_OBJECT_KEY);
    if (!Strings.isNullOrEmpty(shared_object)) {
      final Object obj = tsdb.getRegistry().getSharedObject(shared_object);
      if (obj != null) {
        if (!(obj instanceof JedisCluster)) {
          return Deferred.fromError(new IllegalArgumentException(
              "Shared object: " + shared_object + " was an instance of " 
                  + obj.getClass() + " instead of JedisCluster."));
        }
        cluster = (JedisCluster) obj;
        LOG.info("Configured Redis caching query executor plugin "
            + "from shared object: " + shared_object);
      }
    }
    
    if (cluster == null) {
      cluster = new JedisCluster(nodes);
      if (!Strings.isNullOrEmpty(shared_object)) {
        final Object obj = tsdb.getRegistry()
            .registerSharedObject(shared_object, cluster);
        if (obj != null && obj instanceof JedisCluster) {
          LOG.warn("Lost race instantiating shared Redis cluster connection. "
              + "Using cluster connection: " + obj);
          try {
            cluster.close();
          } catch (Exception e) {
            LOG.error("Unexpected exception closing down duplicate "
                + "cluster connection.", e);
          }
          cluster = (JedisCluster) obj;
          LOG.info("Configured Redis caching query executor plugin "
              + "from shared object: " + shared_object);
        }
      } else {
        LOG.info("Setup Redis caching query executor plugin with hosts: " 
            + hosts);
      }
    }
    
    final String serdes_id = tsdb.getConfig().getString(SERDES_KEY);
    final ReadCacheSerdesFactory factory = tsdb.getRegistry().getPlugin(
        ReadCacheSerdesFactory.class, serdes_id);
    if (factory == null) {
      return Deferred.fromError(new IllegalArgumentException(
          "No serdes factory found for " + (Strings.isNullOrEmpty(serdes_id) ? 
              "Default" : serdes_id)));
    }
    serdes = factory.getSerdes();
    return Deferred.fromResult(null);
  }

  @Override
  public Deferred<byte[]> fetch(final byte[] key,
                                final Span span) {
    if (cluster == null) {
      final IllegalStateException ex = 
          new IllegalStateException("Cache has not been initialized.");
      if (span != null) {
        span.setErrorTags(ex)
             .finish();
      }
      return Deferred.fromError(ex);
    }
    if (key == null) {
      final IllegalArgumentException ex = 
          new IllegalArgumentException("Key cannot be null.");
      if (span != null) {
        span.setErrorTags(ex)
             .finish();
      }
      return Deferred.fromError(ex);
    }
    if (key.length < 1) {
      final IllegalArgumentException ex = 
          new IllegalArgumentException("Key must be at least 1 byte long.");
      if (span != null) {
        span.setErrorTags(ex)
             .finish();
      }
      return Deferred.fromError(ex);
    }
    
    byte[] raw = null;
    Exception ex = null;
    try {
      raw = cluster.get(key);
      tsdb.getStatsCollector().incrementCounter("query.cache.redis.get", 
          (String[]) null);
    } catch (Exception e) {
      LOG.warn("Exception querying Redis for cache data", e);
      ex = e;
    }
    if (ex != null) {
      // don't return the exception, just trace it.
      if (span != null) {
        span.setErrorTags(ex)
             .finish();
      }
      return Deferred.fromResult(null);
    } else {
      if (span != null) {
        span.setSuccessTags()
             .setTag("bytes", raw == null ? "0" : Integer.toString(raw.length))
             .setTag("cacheHit", raw == null ? "false" : "true")
             .finish();
      }
      return Deferred.fromResult(raw);
    }
  }

  @Override
  public Deferred<byte[][]> fetch(final byte[][] keys,
                                  final Span span) {
   if (cluster == null) {
      final IllegalStateException ex = 
          new IllegalStateException("Cache has not been initialized.");
      if (span != null) {
        span.setErrorTags(ex)
             .finish();
      }
      return Deferred.fromError(ex);
    }
    if (keys == null) {
      final IllegalArgumentException ex = 
          new IllegalArgumentException("Keys cannot be null.");
      if (span != null) {
        span.setErrorTags(ex)
             .finish();
      }
      return Deferred.fromError(ex);
    }
    if (keys.length < 1) {
      final IllegalArgumentException ex = 
          new IllegalArgumentException("Keys must have at least one value.");
      if (span != null) {
        span.setErrorTags(ex)
             .finish();
      }
      return Deferred.fromError(ex);
    }
    
    int bytes = 0;
    int hits = 0;
    final byte[][] results = new byte[keys.length][];
    List<byte[]> raw = null;
    Exception ex = null;
    try {
      raw = cluster.mget(keys);
      tsdb.getStatsCollector().incrementCounter("query.cache.redis.get", 
          (String[]) null);
    } catch (Exception e) {
      LOG.warn("Exception querying Redis for cache data", e);
      ex = e;
    }
    if (raw != null && raw.size() != results.length) {
      ex = new IllegalStateException("Redis returned " + raw.size() 
        + " values from a multi-get when we expected " + results.length);
      LOG.warn("Exception querying Redis for cache data", ex);
    }
    
    if (raw != null) {
      for (int i = 0; i < raw.size(); i++) {
        results[i] = raw.get(i);
        if (results[i] != null) {
          bytes += results[i].length;
          ++hits;
        }
      }
    }
    if (ex != null) {
      // don't return the exception, just trace it.
      if (span != null) {
        span.setErrorTags(ex)
             .finish();
      }
      return Deferred.fromResult(results);
    } else {
      if (span != null) {
        span.setSuccessTags()
             .setTag("bytes", Integer.toString(bytes))
             .setTag("cacheHitRatio", Double.toString(
                 ((double) hits / (double) results.length) * 100))
             .finish();
      }
      return Deferred.fromResult(results);
    }
  }

  @Override
  public void fetch(final QueryPipelineContext context, 
                    final byte[][] keys, 
                    final ReadCacheCallback callback, 
                    final Span upstream_span) {
    List<byte[]> raw = null;
    Exception ex = null;
    try {
      raw = cluster.mget(keys);
      tsdb.getStatsCollector().incrementCounter("query.cache.redis.mget", 
          (String[]) null);
    } catch (Exception e) {
      tsdb.getStatsCollector().incrementCounter("query.cache.redis.mget.exception", 
          (String[]) null);
      LOG.warn("Exception querying Redis for cache data", e);
      ex = e;
    }
  
    class CQR implements ReadCacheQueryResultSet {
      final byte[] key;
      final Map<String, ReadCacheQueryResult> results;
      final int idx;
      
      CQR(final int idx, final byte[] raw) {
        this.idx = idx;
        key = keys[idx];
        if (raw == null) {
          results = null;
        } else {
          results = serdes.deserialize(context, raw);
        }
      }
      
      @Override
      public byte[] key() {
        return key;
      }

      @Override
      public int index() {
        return idx;
      }
      
      @Override
      public Map<String, ReadCacheQueryResult> results() {
        return results;
      }

      @Override
      public TimeStamp lastValueTimestamp() {
        // TODO Auto-generated method stub
        return null;
      }
    }
    
    if (ex != null) {
      for (int i = 0; i < keys.length; i++) {
        callback.onCacheError(i, ex);
      }
    } else {
      for (int i = 0; i < keys.length; i++) {
        callback.onCacheResult(new CQR(i, raw.get(i)));
      }
    }
  }
  
  @Override
  public void cache(final byte[] key, 
                    final byte[] data, 
                    final long expiration, 
                    final TimeUnit units,
                    final Span upstream_span) {
    if (cluster == null) {
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
    if (units != TimeUnit.MILLISECONDS) {
      // TODO - easy to convert.
      throw new IllegalArgumentException("Units must be in milliseconds.");
    }
    try {
      cluster.set(key, data, NX, EXP, expiration);
      tsdb.getStatsCollector().incrementCounter("query.cache.redis.set", 
          (String[]) null);
    } catch (Exception e) {
      LOG.error("Unexpected exception writing to Redis.", e);
      tsdb.getStatsCollector().incrementCounter("query.cache.redis.set.exception", 
          (String[]) null);
    }
  }

  @Override
  public void cache(final byte[][] keys, 
                    final byte[][] data, 
                    final long[] expirations,
                    final TimeUnit units,
                    final Span upstream_span) {
    if (cluster == null) {
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
    if (expirations == null) {
      throw new IllegalArgumentException("Expirations cannot be null.");
    }
    if (expirations.length != data.length) {
      throw new IllegalArgumentException("Expirations and data arrays must be "
          + "of the same length.");
    }
    if (units != TimeUnit.MILLISECONDS) {
      // TODO - easy to convert.
      throw new IllegalArgumentException("Units must be in milliseconds.");
    }
    
    // can't use mset as it doesn't allow expiration.
    for (int i = 0; i < keys.length; i++) {
      try {
        if (expirations[i] < 1) {
          continue;
        }
        cluster.set(keys[i], data[i], NX, EXP, expirations[i]);
        tsdb.getStatsCollector().incrementCounter("query.cache.redis.mset", 
            (String[]) null);
      } catch (Exception e) {
        LOG.error("Unexpected exception writing to Redis.", e);
        tsdb.getStatsCollector().incrementCounter("query.cache.redis.mset.exception", 
            (String[]) null);
      }
    }
  }

  @Override
  public Deferred<Void> delete(final int timestamp, final byte[] key) {
    try {
      cluster.del(key);
      tsdb.getStatsCollector().incrementCounter("query.cache.redis.del", 
          (String[]) null);
      return Deferred.fromResult(null);
    } catch (Exception e) {
      LOG.error("Unexpected exception deleting from Redis.", e);
      tsdb.getStatsCollector().incrementCounter("query.cache.redis.del.exception", 
          (String[]) null);
      return Deferred.fromError(e);
    }
  }
  
  @Override
  public Deferred<Void> delete(final int[] timestamps, final byte[][] keys) {
    try {
      for (int i = 0; i < keys.length; i++) {
        cluster.del(keys[i]);
        tsdb.getStatsCollector().incrementCounter("query.cache.redis.del", 
            (String[]) null);
      }
      return Deferred.fromResult(null);
    } catch (Exception e) {
      LOG.error("Unexpected exception deleting from Redis.", e);
      tsdb.getStatsCollector().incrementCounter("query.cache.redis.del.exception", 
          (String[]) null);
      return Deferred.fromError(e);
    }
  }
  
  @Override
  public Deferred<Void> cache(final int timestamp, 
                              final byte[] key, 
                              final long expiration,
                              final Collection<QueryResult> results,
                              final Span span) {
    if (key == null) {
      return Deferred.fromError(new IllegalArgumentException(
          "Key cannot be null."));
    }
    if (key.length < 1) {
      return Deferred.fromError(new IllegalArgumentException(
          "Key length must be at least 1 byte."));
    }
    if (expiration < 0) {
      return Deferred.fromResult(null);
    }

    // best effort
    try {
      final byte[] data = serdes.serialize(results);
      
      try {
        cluster.set(key, data, NX, EXP, expiration);
        tsdb.getStatsCollector().incrementCounter("query.cache.redis.set", 
            (String[]) null);
      } catch (Exception e) {
        LOG.error("Unexpected exception writing to Redis.", e);
        return Deferred.fromError(e);
      }
      return Deferred.fromResult(null);
    } catch (Exception e) {
      return Deferred.fromError(e);
    }
  }
  
  @Override
  public Deferred<Void> cache(final int[] timestamps, 
                              final byte[][] keys,
                              final long[] expirations,
                              final Collection<QueryResult> results,
                              final Span span) {
    if (timestamps == null) {
      return Deferred.fromError(new IllegalArgumentException(
          "Timestamps array cannot be null."));
    }
    if (keys == null) {
      return Deferred.fromError(new IllegalArgumentException(
          "Keys array cannot be null."));
    }
    if (results == null) {
      return Deferred.fromError(new IllegalArgumentException(
          "Results cannot be null."));
    }
    if (expirations == null) {
      return Deferred.fromError(new IllegalArgumentException(
          "Expirations cannot be null."));
    }
    if (timestamps.length != keys.length) {
      return Deferred.fromError(new IllegalArgumentException(
          "Timestamps and keys arrays must be of the same length."));
    }
    if (expirations.length != keys.length) {
      return Deferred.fromError(new IllegalArgumentException(
          "Expirations and keys arrays must be of the same length."));
    }
    
    try {
      final byte[][] data = serdes.serialize(timestamps, results);
      for (int i = 0; i < keys.length; i++) {
        if (keys[i] == null) {
          return Deferred.fromError(new IllegalArgumentException(
              "Key at index " + i + " was null and cannot be."));
        }
        if (expirations[i] < 0) {
          continue;
        }
        
        cluster.set(keys[i], data[i], NX, EXP, expirations[i]);
        tsdb.getStatsCollector().incrementCounter("query.cache.redis.mset", 
            (String[]) null);
      }
    } catch (Exception e) {
      tsdb.getStatsCollector().incrementCounter("query.cache.redis.mset.exception", 
          (String[]) null);
      return Deferred.fromError(e);
    }
    return Deferred.fromResult(null);
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
  public Deferred<Object> shutdown() {
    try {
      if (cluster != null) {
        cluster.close();
      }
    } catch (Exception e) {
      LOG.error("Unexpected exception closing Redis cluster connection.", e);
    }
    LOG.info("Closed Redis query cache cluster connection.");
    return Deferred.fromResult(null);
  }
  
  @VisibleForTesting
  JedisCluster getJedisCluster() {
    return cluster;
  }
}
