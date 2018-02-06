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

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.stumbleupon.async.Deferred;

import io.opentracing.Span;
import net.opentsdb.core.DefaultTSDB;
import net.opentsdb.core.TSDB;
import net.opentsdb.query.context.QueryContext;
import net.opentsdb.query.execution.QueryExecution;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.stats.TsdbTrace;
import net.opentsdb.utils.Bytes;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

/**
 * A cache implementation that supports a single Redis instance with or without
 * simple authentication. All methods will try to swallow non-data related
 * exceptions and simply log them instead of impeding upstream processing.
 * 
 * @since 3.0
 */
public class RedisQueryCache extends QueryCachePlugin {
  private static final Logger LOG = LoggerFactory.getLogger(
      RedisQueryCache.class);

  /** Redis flag: Write if the key does not exist. */
  static final byte[] NX = new byte[] { 'N', 'X' };
  
  /** Redis flag: Expiration time is in ms. */
  static final byte[] EXP = new byte[] { 'P', 'X' };
  
  /** Default maximum connection pool objects. */
  static final int DEFAULT_MAX_POOL = 50;
  
  /** Default wait time for a read in milliseconds. */
  static final int DEFAULT_WAIT_TIME = 1000;
  
  /** The cache pool object. */
  private JedisPool connection_pool;
  
  /** The cache config object. */
  private JedisPoolConfig config;
  
  /** Stats counters */
  private final LongAdder set_called;
  private final LongAdder get_called;
  
  public RedisQueryCache() {
    set_called = new LongAdder();
    get_called = new LongAdder();
  }
  
  @Override
  public Deferred<Object> initialize(final TSDB tsdb) {
    String host = tsdb.getConfig().getString("redis.query.cache.hosts");
    if (host == null || host.isEmpty()) {
      return Deferred.fromError(new IllegalArgumentException("Missing the "
          + "'redis.query.cache.hosts' config"));
    }
    int port = Protocol.DEFAULT_PORT;
    if (host.contains(":")) {
      port = Integer.parseInt(host.substring(host.indexOf(":") + 1));
      host = host.substring(0, host.indexOf(":"));
    }
    
    int maxPool = DEFAULT_MAX_POOL;
    if (tsdb.getConfig().hasProperty("redis.query.cache.max_pool")) {
      maxPool = tsdb.getConfig().getInt("redis.query.cache.max_pool");
    }
    long maxWait = DEFAULT_WAIT_TIME;
    if (tsdb.getConfig().hasProperty("redis.query.cache.wait_time")) {
      maxWait = tsdb.getConfig().getLong("redis.query.cache.wait_time");
    }
    
    final String shared_object = 
        tsdb.getConfig().getString("redis.query.cache.shared_object");
    if (!Strings.isNullOrEmpty(shared_object)) {
      final Object obj = tsdb.getRegistry().getSharedObject(shared_object);
      if (obj != null) {
        if (!(obj instanceof JedisPool)) {
          return Deferred.fromError(new IllegalArgumentException(
              "Shared object: " + shared_object + " was an instance of " 
                  + obj.getClass() + " instead of JedisPool."));
        }
        connection_pool = (JedisPool) obj;
        LOG.info("Configured Redis caching query executor plugin "
            + "from shared object: " + shared_object);
      }
    }

    if (connection_pool == null) {
      config = new JedisPoolConfig();
      config.setMaxTotal(maxPool);
      config.setMaxWaitMillis(maxWait);
      
      final String pass = tsdb.getConfig().getString("redis.query.cache.auth");
      
      connection_pool = (pass != null && !pass.isEmpty() ? 
          new JedisPool(config, host, port, Protocol.DEFAULT_TIMEOUT, pass) : 
            new JedisPool(config, host, port));
      if (!Strings.isNullOrEmpty(shared_object)) {
        final Object obj = tsdb.getRegistry()
            .registerSharedObject(shared_object, connection_pool);
        if (obj != null && obj instanceof JedisPool) {
          LOG.warn("Lost race instantiating shared Redis connection pool. "
              + "Using pool: " + obj);
          try {
            connection_pool.close();
            config = null;
          } catch (Exception e) {
            LOG.error("Unexpected exception closing down duplicate pool", e);
          }
          connection_pool = (JedisPool) obj;
          LOG.info("Configured Redis caching query executor plugin "
              + "from shared object: " + shared_object);
        }
      } else {
        LOG.info("Setup Redis caching query executor plugin with hosts: " + host);
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
        if (context.getTracer() != null) {
          setSpan(context, 
              RedisQueryCache.this.getClass().getSimpleName(), 
              upstream_span,
              TsdbTrace.addTags(
                  "key", Bytes.pretty(key),
                  "startThread", Thread.currentThread().getName()));
        }
      }
      
      /** Do da work */
      void execute() {
        if (connection_pool == null) {
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
        try (Jedis connection = connection_pool.getResource()) {
          raw = connection.get(key);
          get_called.increment();
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
        if (context.getTracer() != null) {
          setSpan(context, 
              RedisQueryCache.this.getClass().getSimpleName(), 
              upstream_span,
              TsdbTrace.addTags(
                  "keys", Integer.toString(keys.length),
                  "startThread", Thread.currentThread().getName()));
        }
      }
      
      /** Do da work */
      void execute() {
        if (connection_pool == null) {
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
        Exception ex = null;
        try (Jedis connection = connection_pool.getResource()) {
          final List<byte[]> raw = connection.mget(keys);
          if (raw.size() != results.length) {
            throw new IllegalStateException("Redis returned " + raw.size() 
              + " values from a multi-get when we expected " + results.length);
          }
          get_called.increment();
          for (int i = 0; i < raw.size(); i++) {
            results[i] = raw.get(i);
            if (results[i] != null) {
              bytes += results[i].length;
              ++hits;
            }
          }
        } catch (Exception e) {
          LOG.warn("Exception querying Redis for cache data", e);
          ex = e;
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
                    final TimeUnit units) {
    if (connection_pool == null) {
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
    
    try (Jedis connection = connection_pool.getResource()) {
      connection.set(key, data, NX, EXP, expiration);
      set_called.increment();
    } catch (Exception e) {
      LOG.error("Unexpected exception writing to Redis.", e);
    }
  }

  @Override
  public void cache(final byte[][] keys, 
                    final byte[][] data, 
                    final long[] expirations,
                    final TimeUnit units) {
    if (connection_pool == null) {
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
    try (Jedis connection = connection_pool.getResource()) {
      for (int i = 0; i < keys.length; i++) {
        if (expirations[i] < 1) {
          continue;
        }
        connection.set(keys[i], data[i], NX, EXP, expirations[i]);
        set_called.increment();  
      }
    } catch (Exception e) {
      LOG.error("Unexpected exception writing to Redis.", e);
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
  public void collectStats(final StatsCollector collector) {
    if (collector == null) {
      return;
    }
    collector.record("cachingQueryExecutor.Redis.setCalled", 
        set_called.longValue());
    collector.record("cachingQueryExecutor.Redis.getCalled", 
        get_called.longValue());
  }
  
  @Override
  public Deferred<Object> shutdown() {
    try {
      if (connection_pool != null) {
        connection_pool.close();
      }
    } catch (Exception e) {
      LOG.error("Unexpected exception closing Redis pool", e);
    }
    LOG.info("Closed Redis query cache pool");
    return Deferred.fromResult(null);
  }

  @VisibleForTesting
  JedisPoolConfig getJedisConfig() {
    return config;
  }
  
  @VisibleForTesting
  JedisPool getJedisPool() {
    return connection_pool;
  }
}
