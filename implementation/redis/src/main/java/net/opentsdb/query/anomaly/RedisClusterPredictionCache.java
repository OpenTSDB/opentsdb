// This file is part of OpenTSDB.
// Copyright (C) 2019  The OpenTSDB Authors.
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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.anomaly.AnomalyPredictionState;
import net.opentsdb.query.anomaly.PredictionCache;
import net.opentsdb.query.execution.cache.RedisClusterQueryCache;
import net.opentsdb.query.readcache.ReadCacheQueryResult;
import net.opentsdb.query.readcache.ReadCacheSerdes;
import net.opentsdb.query.readcache.ReadCacheSerdesFactory;
import net.opentsdb.stats.Span;
import net.opentsdb.utils.JSON;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

/**
 * A plugin to use a Redis Cluster for Anomaly state and prediction caching.
 * 
 * @since 3.0
 */
public class RedisClusterPredictionCache extends BaseTSDBPlugin 
    implements PredictionCache {
  private static final Logger LOG = LoggerFactory.getLogger(
      RedisClusterPredictionCache.class);
  
  public static final String TYPE = "RedisClusterPredictionCache";
  public static final byte[] STATE_KEY = "TSDBANOMALYSTATE".getBytes(
      Const.ASCII_CHARSET);
  
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
    if (!tsdb.getConfig().hasProperty(RedisClusterQueryCache.HOSTS_KEY)) {
      tsdb.getConfig().register(RedisClusterQueryCache.HOSTS_KEY, 
          (String) null, false /* todo */, 
          "A comma separated list of hosts in the format "
          + "<host>:<port>,<host>:<port>.");
    }
    if (!tsdb.getConfig().hasProperty(RedisClusterQueryCache.SHARED_OBJECT_KEY)) {
      tsdb.getConfig().register(RedisClusterQueryCache.SHARED_OBJECT_KEY, (String) null, false, 
          "The string ID of an optional shared object to use for "
          + "sharing a client connection pool.");
    }
    if (!tsdb.getConfig().hasProperty(RedisClusterQueryCache.SERDES_KEY)) {
      tsdb.getConfig().register(RedisClusterQueryCache.SERDES_KEY, (String) null, false, 
          "The ID of a read cache serdes plugin factory.");
    }
    
    String hosts = tsdb.getConfig().getString(RedisClusterQueryCache.HOSTS_KEY);
    if (hosts == null || hosts.isEmpty()) {
      return Deferred.fromError(new IllegalArgumentException("Missing the "
          + "'" + RedisClusterQueryCache.HOSTS_KEY +"' config"));
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
    
    final String shared_object = tsdb.getConfig().getString(
        RedisClusterQueryCache.SHARED_OBJECT_KEY);
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
    
    final String serdes_id = tsdb.getConfig().getString(
        RedisClusterQueryCache.SERDES_KEY);
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
  public Deferred<QueryResult> fetch(final QueryPipelineContext context, 
                                     final byte[] key,
                                     final Span span) {
    byte[] raw = null;
    try {
      raw = cluster.get(key);
      tsdb.getStatsCollector().incrementCounter("anomaly.cache.redis.get", 
          (String[]) null);
    } catch (Exception e) {
      tsdb.getStatsCollector().incrementCounter("anomaly.cache.redis.get.exception", 
          (String[]) null);
      LOG.warn("Exception querying Redis for cache data", e);
    }
    
    if (raw == null) {
      return Deferred.fromResult(null);
    }
    
    try {
      final Map<String, ReadCacheQueryResult> results = 
          serdes.deserialize(context, raw);
      ReadCacheQueryResult result = null;
      for (final Entry<String, ReadCacheQueryResult> entry : results.entrySet()) {
        if (entry.getValue() != null) {
          result = entry.getValue();
          break;
        }
      }
      return Deferred.fromResult(result);
    } catch (Exception e) {
      LOG.warn("Exception parsing cache data", e);
    }
    return Deferred.fromResult(null);
  }

  @Override
  public Deferred<Void> cache(final byte[] key, 
                              final long expiration, 
                              final QueryResult results,
                              final Span upstream_span) {
    try {
      final byte[] data = serdes.serialize(Lists.newArrayList(results));
      cluster.set(key, data, RedisClusterQueryCache.NX, 
          RedisClusterQueryCache.EXP, expiration);
      tsdb.getStatsCollector().incrementCounter("anomaly.cache.redis.set", 
          (String[]) null);
      return Deferred.fromResult(null);
    } catch (Exception e) {
      LOG.error("Unexpected exception writing to Redis.", e);
      tsdb.getStatsCollector().incrementCounter("anomaly.cache.redis.set.exception", 
          (String[]) null);
      return Deferred.fromError(e);
    }
  }

  @Override
  public Deferred<Void> delete(final byte[] key) {
    try {
      cluster.del(key);
      tsdb.getStatsCollector().incrementCounter("anomaly.cache.redis.del", 
          (String[]) null);
      return Deferred.fromResult(null);
    } catch (Exception e) {
      LOG.error("Unexpected exception deleting from Redis.", e);
      tsdb.getStatsCollector().incrementCounter("anomaly.cache.redis.del.exception", 
          (String[]) null);
      return Deferred.fromError(e);
    }
  }

  @Override
  public AnomalyPredictionState getState(final byte[] key) {
    byte[] prefixed_key = new byte[key.length + STATE_KEY.length];
    System.arraycopy(STATE_KEY, 0, prefixed_key, 0, STATE_KEY.length);
    System.arraycopy(key, 0, prefixed_key, STATE_KEY.length, key.length);
    
    byte[] raw = null;
    try {
      raw = cluster.get(prefixed_key);
      tsdb.getStatsCollector().incrementCounter("anomaly.cache.redis.get", 
          (String[]) null);
    } catch (Exception e) {
      tsdb.getStatsCollector().incrementCounter("anomaly.cache.redis.get.exception", 
          (String[]) null);
      LOG.warn("Exception querying Redis for cache data", e);
    }
    
    if (raw == null) {
      return null;
    }
    
    return JSON.parseToObject(raw, AnomalyPredictionState.class);
  }

  @Override
  public void setState(final byte[] key, 
                       final AnomalyPredictionState state,
                       final long expiration) {
    byte[] prefixed_key = new byte[key.length + STATE_KEY.length];
    System.arraycopy(STATE_KEY, 0, prefixed_key, 0, STATE_KEY.length);
    System.arraycopy(key, 0, prefixed_key, STATE_KEY.length, key.length);
    
    final byte[] data = JSON.serializeToBytes(state);
    try {
      cluster.setex(prefixed_key, (int) expiration / 1000, data);
      tsdb.getStatsCollector().incrementCounter("anomaly.cache.redis.setex", 
          (String[]) null);
    } catch (Exception e) {
      LOG.error("Unexpected exception writing to Redis.", e);
      tsdb.getStatsCollector().incrementCounter("anomaly.cache.redis.set.exception", 
          (String[]) null);
    }
  }
  
  @Override
  public void deleteState(final byte[] key) {
    byte[] prefixed_key = new byte[key.length + STATE_KEY.length];
    System.arraycopy(STATE_KEY, 0, prefixed_key, 0, STATE_KEY.length);
    System.arraycopy(key, 0, prefixed_key, STATE_KEY.length, key.length);
    
    try {
      cluster.del(prefixed_key);
      tsdb.getStatsCollector().incrementCounter("anomaly.cache.redis.del", 
          (String[]) null);
    } catch (Exception e) {
      LOG.error("Unexpected exception deleting from Redis.", e);
      tsdb.getStatsCollector().incrementCounter("anomaly.cache.redis.del.exception", 
          (String[]) null);
    }
  }

  @Override
  public String type() {
    return TYPE;
  }

}