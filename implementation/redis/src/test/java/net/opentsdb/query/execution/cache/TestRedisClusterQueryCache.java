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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anySet;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.common.collect.Lists;

import net.opentsdb.core.Registry;
import net.opentsdb.core.TSDB;
import net.opentsdb.query.context.QueryContext;
import net.opentsdb.query.execution.QueryExecution;
import net.opentsdb.utils.Config;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ TSDB.class, RedisClusterQueryCache.class })
public class TestRedisClusterQueryCache {
  private TSDB tsdb;
  private Registry registry;
  private Config config;
  private JedisCluster cluster;
  private Set<HostAndPort> nodes;
  private QueryContext context;
  
  @Before
  public void before() throws Exception {
    tsdb = mock(TSDB.class);
    registry = mock(Registry.class);
    config = new Config(false);
    cluster = mock(JedisCluster.class);
    context = mock(QueryContext.class);
    
    config.overrideConfig("redis.query.cache.hosts", 
        "localhost:2424,localhost:4242");
    
    when(tsdb.getConfig()).thenReturn(config);
    when(tsdb.getRegistry()).thenReturn(registry);
    
    PowerMockito.whenNew(JedisCluster.class).withAnyArguments()
      .thenAnswer(new Answer<JedisCluster>() {
      @SuppressWarnings("unchecked")
      @Override
      public JedisCluster answer(final InvocationOnMock invocation) throws Throwable {
        nodes = (Set<HostAndPort>) invocation.getArguments()[0];
        return cluster;
      }
    });
  }
  
  @Test
  public void ctor() throws Exception {
    new RedisClusterQueryCache();
    PowerMockito.verifyNew(JedisCluster.class, never()).withArguments(anySet());
    verify(cluster, never()).close();
  }
  
  @Test
  public void initialize() throws Exception {
    final RedisClusterQueryCache cache = new RedisClusterQueryCache();
    assertNull(cache.initialize(tsdb).join(1));
    PowerMockito.verifyNew(JedisCluster.class, times(1)).withArguments(anySet());
    verify(cluster, never()).close();
    assertEquals(2, nodes.size());
    for (final HostAndPort host : nodes) {
      assertEquals("localhost", host.getHost());
      assertTrue(host.getPort() == 2424 || host.getPort() == 4242);
    }
    verify(registry, never()).registerSharedObject(anyString(), any());
  }
  
  @Test
  public void initializeShared() throws Exception {
    config.overrideConfig("redis.query.cache.shared_object", "RedisCache");
    final RedisClusterQueryCache cache = new RedisClusterQueryCache();
    assertNull(cache.initialize(tsdb).join(1));
    PowerMockito.verifyNew(JedisCluster.class, times(1)).withArguments(anySet());
    verify(cluster, never()).close();
    assertEquals(2, nodes.size());
    for (final HostAndPort host : nodes) {
      assertEquals("localhost", host.getHost());
      assertTrue(host.getPort() == 2424 || host.getPort() == 4242);
    }
    verify(registry, times(1)).registerSharedObject("RedisCache", cluster);
  }
  
  @Test
  public void initializeSharedAlreadyThere() throws Exception {
    config.overrideConfig("redis.query.cache.shared_object", "RedisCache");
    when(registry.getSharedObject("RedisCache")).thenReturn(cluster);
    
    final RedisClusterQueryCache cache = new RedisClusterQueryCache();
    assertNull(cache.initialize(tsdb).join(1));
    PowerMockito.verifyNew(JedisCluster.class, never()).withArguments(anySet());
    verify(cluster, never()).close();
    assertNull(nodes);
    verify(registry, never()).registerSharedObject("RedisCache", cluster);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void initializeSharedWrongType() throws Exception {
    config.overrideConfig("redis.query.cache.shared_object", "RedisCache");
    when(registry.getSharedObject("RedisCache")).thenReturn(tsdb);
    
    final RedisClusterQueryCache cache = new RedisClusterQueryCache();
    cache.initialize(tsdb).join(1);
  }
  
  @Test
  public void initializeSharedRace() throws Exception {
    config.overrideConfig("redis.query.cache.shared_object", "RedisCache");
    final JedisCluster extant = mock(JedisCluster.class);
    when(registry.registerSharedObject("RedisCache", cluster))
      .thenReturn(extant);
    
    final RedisClusterQueryCache cache = new RedisClusterQueryCache();
    assertNull(cache.initialize(tsdb).join(1));
    PowerMockito.verifyNew(JedisCluster.class, times(1)).withArguments(anySet());
    verify(cluster, times(1)).close();
    assertEquals(2, nodes.size());
    for (final HostAndPort host : nodes) {
      assertEquals("localhost", host.getHost());
      assertTrue(host.getPort() == 2424 || host.getPort() == 4242);
    }
    verify(registry, times(1)).registerSharedObject("RedisCache", cluster);
    assertSame(extant, cache.getJedisCluster());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void initializeNullHosts() throws Exception {
    config.overrideConfig("redis.query.cache.hosts", null);
    new RedisClusterQueryCache().initialize(tsdb).join(1);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void initializeEmptyHost() throws Exception {
    config.overrideConfig("redis.query.cache.hosts", "");
    new RedisClusterQueryCache().initialize(tsdb).join(1);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void initializeBadHostPort() throws Exception {
    config.overrideConfig("redis.query.cache.hosts", "localhost:notanum");
    new RedisClusterQueryCache().initialize(tsdb).join(1);
  }
  
  
  @Test
  public void shutdown() throws Exception {
    final RedisClusterQueryCache cache = new RedisClusterQueryCache();
    assertNull(cache.initialize(tsdb).join(1));
    assertNull(cache.shutdown().join(1));
    PowerMockito.verifyNew(JedisCluster.class, times(1)).withArguments(anySet());
    verify(cluster, times(1)).close();
  }

  @Test
  public void cache() throws Exception {
    byte[] key = new byte[] { 0, 0, 1 };
    byte[] data = new byte[] { 42 };
    
    RedisClusterQueryCache cache = new RedisClusterQueryCache();
    
    try {
      cache.cache(key, data, 600000, TimeUnit.MILLISECONDS);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    
    assertNull(cache.initialize(tsdb).join(1));

    cache.cache(key, data, 600000, TimeUnit.MILLISECONDS);
    verify(cluster, times(1)).set(key, data, RedisClusterQueryCache.NX, 
        RedisClusterQueryCache.EXP, 600000L);
    verify(cluster, never()).close();
    
    try {
      cache.cache(null, data, 600000, TimeUnit.MILLISECONDS);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      cache.cache(new byte[] { }, data, 600000, TimeUnit.MILLISECONDS);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    cache.cache(key, data, 0, TimeUnit.MILLISECONDS);
    verify(cluster, times(1)).set(key, data, RedisClusterQueryCache.NX, 
        RedisClusterQueryCache.EXP, 600000L);
    verify(cluster, never()).close();
    
    try {
      cache.cache(key, data, 600000, TimeUnit.NANOSECONDS);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    when(cluster.set(key, data, RedisClusterQueryCache.NX, RedisClusterQueryCache.EXP, 
        600000L)).thenThrow(new IllegalArgumentException("Boo!"));
    cache.cache(key, data, 600000, TimeUnit.MILLISECONDS);
    verify(cluster, times(2)).set(key, data, RedisClusterQueryCache.NX, 
        RedisClusterQueryCache.EXP, 600000L);
    verify(cluster, never()).close();
  }
  
  @Test
  public void cacheMultiKey() throws Exception {
    byte[][] keys = new byte[][] { { 0, 0, 1 }, { 0, 0, 2 } };
    byte[][] data = new byte[][] { { 42 }, { 24 } };
    long[] expirations = new long[] { 600000, 300000 };
    
    RedisClusterQueryCache cache = new RedisClusterQueryCache();
    
    try {
      cache.cache(keys, data, expirations, TimeUnit.MILLISECONDS);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    
    assertNull(cache.initialize(tsdb).join(1));

    cache.cache(keys, data, expirations, TimeUnit.MILLISECONDS);
    verify(cluster, times(1)).set(new byte[] { 0, 0, 1 }, new byte[] { 42 }, 
        RedisClusterQueryCache.NX, RedisClusterQueryCache.EXP, 600000L);
    verify(cluster, times(1)).set(new byte[] { 0, 0, 2 }, new byte[] { 24 }, 
        RedisClusterQueryCache.NX, RedisClusterQueryCache.EXP, 300000L);
    verify(cluster, never()).close();
    
    try {
      cache.cache(null, data, expirations, TimeUnit.MILLISECONDS);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      cache.cache(new byte[][] { }, data, expirations, TimeUnit.MILLISECONDS);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    cache.cache(keys, data, new long[] { 600000, 0 }, TimeUnit.MILLISECONDS);
    verify(cluster, times(2)).set(new byte[] { 0, 0, 1 }, new byte[] { 42 }, 
        RedisClusterQueryCache.NX, RedisClusterQueryCache.EXP, 600000L);
    verify(cluster, times(1)).set(new byte[] { 0, 0, 2 }, new byte[] { 24 }, 
        RedisClusterQueryCache.NX, RedisClusterQueryCache.EXP, 300000L);
    verify(cluster, never()).set(new byte[] { 0, 0, 2 }, new byte[] { 24 }, 
        RedisClusterQueryCache.NX, RedisClusterQueryCache.EXP, 0L);
    verify(cluster, never()).close();
    
    try {
      cache.cache(keys, data, expirations, TimeUnit.NANOSECONDS);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      cache.cache(keys, data, null, TimeUnit.MILLISECONDS);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      cache.cache(keys, data, new long[] { 30000L }, TimeUnit.MILLISECONDS);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    when(cluster.set(new byte[] { 0, 0, 1 }, new byte[] { 42 }, 
        RedisClusterQueryCache.NX, RedisClusterQueryCache.EXP, 600000L))
        .thenThrow(new IllegalArgumentException("Boo!"));
    cache.cache(keys, data, expirations, TimeUnit.MILLISECONDS);
    verify(cluster, times(3)).set(new byte[] { 0, 0, 1 }, new byte[] { 42 }, 
        RedisClusterQueryCache.NX, RedisClusterQueryCache.EXP, 600000L);
    // not called
    verify(cluster, times(2)).set(new byte[] { 0, 0, 2 }, new byte[] { 24 }, 
        RedisClusterQueryCache.NX, RedisClusterQueryCache.EXP, 300000L);
    verify(cluster, never()).close();
  }

  @Test
  public void fetch() throws Exception {
    byte[] key = new byte[] { 0, 0, 1 };
    byte[] data = new byte[] { 42 };
    
    RedisClusterQueryCache cache = new RedisClusterQueryCache();
    QueryExecution<byte[]> exec = cache.fetch(context, key, null);
    
    try {
      exec.deferred().join(1);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    
    cache.initialize(tsdb).join(1);
    
    exec = cache.fetch(context, key, null);
    assertNull(exec.deferred().join(1));
    
    when(cluster.get(key)).thenReturn(data);
    exec = cache.fetch(context, key, null);
    assertArrayEquals(data, exec.deferred().join(1));
    
    exec = cache.fetch(context, (byte[]) null, null);
    try {
      exec.deferred().join(1);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    exec = cache.fetch(context, new byte[] { }, null);
    try {
      exec.deferred().join(1);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    when(cluster.get(key)).thenThrow(new IllegalStateException("Boo!"));
    exec = cache.fetch(context, key, null);
    assertNull(exec.deferred().join(1));
  }
  
  @Test
  public void fetchMultiKey() throws Exception {
    byte[][] keys = new byte[][] { { 0, 0, 1 }, { 0, 0, 2 } };
    byte[] data_a = new byte[] { 42 };
    byte[] data_b = new byte[] { 24 };
    
    RedisClusterQueryCache cache = new RedisClusterQueryCache();
    QueryExecution<byte[][]> exec = cache.fetch(context, keys, null);
    
    try {
      exec.deferred().join(1);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    
    cache.initialize(tsdb).join(1);
    
    exec = cache.fetch(context, keys, null);
    byte[][] response = exec.deferred().join(1);
    assertEquals(2, response.length);
    assertNull(response[0]);
    assertNull(response[1]);
    
    List<byte[]> cached = Lists.newArrayList(data_a, data_b);
    when(cluster.mget(keys)).thenReturn(cached);
    exec = cache.fetch(context, keys, null);
    response = exec.deferred().join(1);
    assertEquals(2, response.length);
    assertArrayEquals(data_a, response[0]);
    assertArrayEquals(data_b, response[1]);
    
    cached = Lists.newArrayList(null, data_b);
    when(cluster.mget(keys)).thenReturn(cached);
    exec = cache.fetch(context, keys, null);
    response = exec.deferred().join(1);
    assertEquals(2, response.length);
    assertNull(response[0]);
    assertArrayEquals(data_b, response[1]);
    
    exec = cache.fetch(context, (byte[][]) null, null);
    try {
      exec.deferred().join(1);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    exec = cache.fetch(context, new byte[][] { }, null);
    try {
      exec.deferred().join(1);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    when(cluster.mget(keys)).thenThrow(new IllegalStateException("Boo!"));
    exec = cache.fetch(context, keys, null);
    response = exec.deferred().join(1);
    assertEquals(2, response.length);
    assertNull(response[0]);
    assertNull(response[1]);
  }
}
