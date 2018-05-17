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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSDBPlugin;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.execution.QueryExecution;
import net.opentsdb.stats.Span;
import net.opentsdb.stats.TsdbTrace;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

/**
 * A cache implementation that supports a Redis cluster, i.e. native Redis 
 * clustering. All methods will try to swallow non-data related exceptions and 
 * simply log them instead of impeding upstream processing.
 * 
 * @since 3.0
 */
public class RedisClusterQueryCache implements QueryCachePlugin, TSDBPlugin {
  private static final Logger LOG = LoggerFactory.getLogger(
      RedisClusterQueryCache.class);

  /** Configuration keys. */
  public static final String HOSTS_KEY = "redis.query.cache.hosts";
  public static final String SHARED_OBJECT_KEY = 
      "redis.query.cache.shared_object";
  
  /** Redis flag: Write if the key does not exist. */
  static final byte[] NX = new byte[] { 'N', 'X' };
  
  /** Redis flag: Expiration time is in ms. */
  static final byte[] EXP = new byte[] { 'P', 'X' };
  
  /** The TSDB we belong to. */
  private TSDB tsdb;
  
  /** The cluster object. */
  private JedisCluster cluster;
  
  @Override
  public Deferred<Object> initialize(final TSDB tsdb) {
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
    return Deferred.fromResult(null);
  }

  @Override
  public QueryExecution<byte[]> fetch(final QueryContext context, 
                                      final byte[] key,
                                      final Span upstream_span) {
    /** The execution class. */
    class LocalExecution extends QueryExecution<byte[]> {

      public LocalExecution() {
        super(null);
//        if (context.getTracer() != null) {
//          setSpan(context, 
//              RedisClusterQueryCache.this.getClass().getSimpleName(), 
//              upstream_span,
//              TsdbTrace.addTags(
//                  "key", Bytes.pretty(key),
//                  "startThread", Thread.currentThread().getName()));
//        }
      }
      
      /** Do da work */
      void execute() {
        if (cluster == null) {
          final IllegalStateException ex = 
              new IllegalStateException("Cache has not been initialized.");
          callback(ex,
              TsdbTrace.exceptionTags(ex),
              TsdbTrace.exceptionAnnotation(ex));
          return;
        }
        if (key == null) {
          final IllegalArgumentException ex = 
              new IllegalArgumentException("Key cannot be null.");
          callback(ex,
              TsdbTrace.exceptionTags(ex),
              TsdbTrace.exceptionAnnotation(ex));
          return;
        }
        if (key.length < 1) {
          final IllegalArgumentException ex = 
              new IllegalArgumentException("Key must be at least 1 byte long.");
          callback(ex,
              TsdbTrace.exceptionTags(ex),
              TsdbTrace.exceptionAnnotation(ex));
          return;
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
          callback(raw, 
              TsdbTrace.exceptionTags(ex), 
              TsdbTrace.exceptionAnnotation(ex));
        } else {
          callback(raw, TsdbTrace.successfulTags(
               "bytes", raw == null ? "0" : Integer.toString(raw.length),
               "cacheHit", raw == null ? "false" : "true"));
        }
      }
      
      @Override
      public void cancel() {
        // No-op.
      }
    }
    
    final LocalExecution execution = new LocalExecution();
    execution.execute();
    return execution;
  }

  @Override
  public QueryExecution<byte[][]> fetch(final QueryContext context, 
                                        final byte[][] keys,
                                        final Span upstream_span) {
    /** The execution class. */
    class LocalExecution extends QueryExecution<byte[][]> {

      public LocalExecution() {
        super(null);
//        if (context.getTracer() != null) {
//          setSpan(context, 
//              RedisClusterQueryCache.this.getClass().getSimpleName(), 
//              upstream_span,
//              TsdbTrace.addTags(
//                  "keys", Integer.toString(keys.length),
//                  "startThread", Thread.currentThread().getName()));
//        }
      }
      
      /** Do da work */
      void execute() {
        if (cluster == null) {
          final IllegalStateException ex = 
              new IllegalStateException("Cache has not been initialized.");
          callback(ex,
              TsdbTrace.exceptionTags(ex),
              TsdbTrace.exceptionAnnotation(ex));
          return;
        }
        if (keys == null) {
          final IllegalArgumentException ex = 
              new IllegalArgumentException("Keys cannot be null.");
          callback(ex,
              TsdbTrace.exceptionTags(ex),
              TsdbTrace.exceptionAnnotation(ex));
          return;
        }
        if (keys.length < 1) {
          final IllegalArgumentException ex = 
              new IllegalArgumentException("Keys must have at least one value.");
          callback(ex,
              TsdbTrace.exceptionTags(ex),
              TsdbTrace.exceptionAnnotation(ex));
          return;
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
          callback(results, 
              TsdbTrace.exceptionTags(ex), 
              TsdbTrace.exceptionAnnotation(ex));
        } else {
          callback(results, TsdbTrace.successfulTags(
               "bytes", Integer.toString(bytes),
               "cacheHitRatio", Double.toString(
                   ((double) hits / (double) results.length) * 100)));
        }
      }
      
      @Override
      public void cancel() {
        // No-op.
      }
    }
    
    final LocalExecution execution = new LocalExecution();
    execution.execute();
    return execution;
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
        tsdb.getStatsCollector().incrementCounter("query.cache.redis.set", 
            (String[]) null);
      } catch (Exception e) {
        LOG.error("Unexpected exception writing to Redis.", e);
      }
    }
  }

  @Override
  public String id() {
    return getClass().getName();
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
