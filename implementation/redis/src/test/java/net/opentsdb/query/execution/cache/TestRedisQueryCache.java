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
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.common.collect.Lists;

import net.opentsdb.core.DefaultRegistry;
import net.opentsdb.core.TSDB;
import net.opentsdb.query.context.QueryContext;
import net.opentsdb.query.execution.QueryExecution;
import net.opentsdb.utils.Config;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ TSDB.class, RedisQueryCache.class })
public class TestRedisQueryCache {
  private TSDB tsdb;
  private DefaultRegistry registry;
  private Config config;
  private JedisPool connection_pool;
  private Jedis instance;
  private QueryContext context;
  
  @Before
  public void before() throws Exception {
    tsdb = mock(TSDB.class);
    registry = mock(DefaultRegistry.class);
    config = new Config(false);
    connection_pool = mock(JedisPool.class);
    instance = mock(Jedis.class);
    context = mock(QueryContext.class);
    
    config.overrideConfig("redis.query.cache.hosts", "localhost");
    
    when(tsdb.getConfig()).thenReturn(config);
    when(tsdb.getRegistry()).thenReturn(registry);
    
    PowerMockito.whenNew(JedisPool.class).withAnyArguments()
      .thenReturn(connection_pool);
    when(connection_pool.getResource()).thenReturn(instance);
  }
  
  @Test
  public void ctor() throws Exception {
    final RedisQueryCache cache = new RedisQueryCache();
    PowerMockito.verifyNew(JedisPool.class, never()).withArguments(
        any(JedisPoolConfig.class), anyString(), anyInt());
    PowerMockito.verifyNew(JedisPool.class, never()).withArguments(
        any(JedisPoolConfig.class), anyString(), anyInt(), anyInt(), anyString());
    verify(connection_pool, never()).close();
    assertNull(cache.getJedisConfig());
  }
  
  @Test
  public void initialize() throws Exception {
    final RedisQueryCache cache = new RedisQueryCache();
    assertNull(cache.initialize(tsdb).join(1));
    PowerMockito.verifyNew(JedisPool.class, times(1)).withArguments(
        any(JedisPoolConfig.class), eq("localhost"), eq(Protocol.DEFAULT_PORT));
    PowerMockito.verifyNew(JedisPool.class, never()).withArguments(
        any(JedisPoolConfig.class), anyString(), anyInt(), anyInt(), anyString());
    verify(connection_pool, never()).close();
    assertEquals(cache.getJedisConfig().getMaxWaitMillis(),
        RedisQueryCache.DEFAULT_WAIT_TIME);
    assertEquals(cache.getJedisConfig().getMaxTotal(), 
        RedisQueryCache.DEFAULT_MAX_POOL);
    verify(registry, never()).registerSharedObject(anyString(), any());
  }
  
  @Test
  public void initializeShared() throws Exception {
    config.overrideConfig("redis.query.cache.shared_object", "RedisCache");
    final RedisQueryCache cache = new RedisQueryCache();
    assertNull(cache.initialize(tsdb).join(1));
    PowerMockito.verifyNew(JedisPool.class, times(1)).withArguments(
        any(JedisPoolConfig.class), eq("localhost"), eq(Protocol.DEFAULT_PORT));
    PowerMockito.verifyNew(JedisPool.class, never()).withArguments(
        any(JedisPoolConfig.class), anyString(), anyInt(), anyInt(), anyString());
    verify(connection_pool, never()).close();
    assertEquals(cache.getJedisConfig().getMaxWaitMillis(),
        RedisQueryCache.DEFAULT_WAIT_TIME);
    assertEquals(cache.getJedisConfig().getMaxTotal(), 
        RedisQueryCache.DEFAULT_MAX_POOL);
    verify(registry, times(1)).registerSharedObject("RedisCache", connection_pool);
  }
  
  @Test
  public void initializeSharedAlreadyThere() throws Exception {
    config.overrideConfig("redis.query.cache.shared_object", "RedisCache");
    when(registry.getSharedObject("RedisCache")).thenReturn(connection_pool);
    
    final RedisQueryCache cache = new RedisQueryCache();
    assertNull(cache.initialize(tsdb).join(1));
    PowerMockito.verifyNew(JedisPool.class, never()).withArguments(
        any(JedisPoolConfig.class), eq("localhost"), eq(Protocol.DEFAULT_PORT));
    PowerMockito.verifyNew(JedisPool.class, never()).withArguments(
        any(JedisPoolConfig.class), anyString(), anyInt(), anyInt(), anyString());
    verify(connection_pool, never()).close();
    assertNull(cache.getJedisConfig());
    verify(registry, never()).registerSharedObject("RedisCache", connection_pool);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void initializeSharedWrongType() throws Exception {
    config.overrideConfig("redis.query.cache.shared_object", "RedisCache");
    when(registry.getSharedObject("RedisCache")).thenReturn(tsdb);
    
    final RedisQueryCache cache = new RedisQueryCache();
    cache.initialize(tsdb).join(1);
  }
  
  @Test
  public void initializeSharedRace() throws Exception {
    config.overrideConfig("redis.query.cache.shared_object", "RedisCache");
    final JedisPool extant = mock(JedisPool.class);
    when(registry.registerSharedObject("RedisCache", connection_pool))
      .thenReturn(extant);
    
    final RedisQueryCache cache = new RedisQueryCache();
    assertNull(cache.initialize(tsdb).join(1));
    PowerMockito.verifyNew(JedisPool.class, times(1)).withArguments(
        any(JedisPoolConfig.class), eq("localhost"), eq(Protocol.DEFAULT_PORT));
    PowerMockito.verifyNew(JedisPool.class, never()).withArguments(
        any(JedisPoolConfig.class), anyString(), anyInt(), anyInt(), anyString());
    verify(connection_pool, times(1)).close();
    assertNull(cache.getJedisConfig());
    verify(registry, times(1)).registerSharedObject("RedisCache", connection_pool);
    assertSame(extant, cache.getJedisPool());
  }
  
  @Test
  public void initializeOverrides() throws Exception {
    config.overrideConfig("redis.query.cache.max_pool", "42");
    config.overrideConfig("redis.query.cache.wait_time", "60000");
    
    final RedisQueryCache cache = new RedisQueryCache();
    assertNull(cache.initialize(tsdb).join(1));
    PowerMockito.verifyNew(JedisPool.class, times(1)).withArguments(
        any(JedisPoolConfig.class), eq("localhost"), eq(Protocol.DEFAULT_PORT));
    PowerMockito.verifyNew(JedisPool.class, never()).withArguments(
        any(JedisPoolConfig.class), anyString(), anyInt(), anyInt(), anyString());
    verify(connection_pool, never()).close();
    assertEquals(cache.getJedisConfig().getMaxWaitMillis(), 60000);
    assertEquals(cache.getJedisConfig().getMaxTotal(), 42);
  }
  
  @Test
  public void initializeHostWithPort() throws Exception {
    config.overrideConfig("redis.query.cache.hosts", "redis.mysite.com:2424");
    
    final RedisQueryCache cache = new RedisQueryCache();
    assertNull(cache.initialize(tsdb).join(1));
    PowerMockito.verifyNew(JedisPool.class, times(1)).withArguments(
        any(JedisPoolConfig.class), eq("redis.mysite.com"), eq(2424));
    PowerMockito.verifyNew(JedisPool.class, never()).withArguments(
        any(JedisPoolConfig.class), anyString(), anyInt(), anyInt(), anyString());
    verify(connection_pool, never()).close();
    assertEquals(cache.getJedisConfig().getMaxWaitMillis(),
        RedisQueryCache.DEFAULT_WAIT_TIME);
    assertEquals(cache.getJedisConfig().getMaxTotal(), 
        RedisQueryCache.DEFAULT_MAX_POOL);
  }
  
  @Test
  public void initializeAuth() throws Exception {
    config.overrideConfig("redis.query.cache.auth", "foobar");
    
    final RedisQueryCache cache = new RedisQueryCache();
    assertNull(cache.initialize(tsdb).join(1));
    PowerMockito.verifyNew(JedisPool.class, never()).withArguments(
        any(JedisPoolConfig.class), anyString(), eq(Protocol.DEFAULT_PORT));
    PowerMockito.verifyNew(JedisPool.class, times(1)).withArguments(
        any(JedisPoolConfig.class), eq("localhost"), eq(Protocol.DEFAULT_PORT), 
        eq(Protocol.DEFAULT_TIMEOUT), eq("foobar"));
    verify(connection_pool, never()).close();
    assertEquals(cache.getJedisConfig().getMaxWaitMillis(),
        RedisQueryCache.DEFAULT_WAIT_TIME);
    assertEquals(cache.getJedisConfig().getMaxTotal(), 
        RedisQueryCache.DEFAULT_MAX_POOL);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void initializeNullHosts() throws Exception {
    config.overrideConfig("redis.query.cache.hosts", null);
    new RedisQueryCache().initialize(tsdb).join(1);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void initializeEmptyHost() throws Exception {
    config.overrideConfig("redis.query.cache.hosts", "");
    new RedisQueryCache().initialize(tsdb).join(1);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void initializeBadHostPort() throws Exception {
    config.overrideConfig("redis.query.cache.hosts", "localhost:notanum");
    new RedisQueryCache().initialize(tsdb).join(1);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void initializeInvalidMaxPool() throws Exception {
    config.overrideConfig("redis.query.cache.max_pool", "notanumber");
    new RedisQueryCache().initialize(tsdb).join(1);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void initializeWaitTime() throws Exception {
    config.overrideConfig("redis.query.cache.wait_time", "notanumber");
    new RedisQueryCache().initialize(tsdb).join(1);
  }
  
  @Test
  public void shutdown() throws Exception {
    final RedisQueryCache cache = new RedisQueryCache();
    assertNull(cache.initialize(tsdb).join(1));
    assertNull(cache.shutdown().join(1));
    PowerMockito.verifyNew(JedisPool.class, times(1)).withArguments(
        any(JedisPoolConfig.class), anyString(), anyInt());
    verify(connection_pool, times(1)).close();
  }

  @Test
  public void cache() throws Exception {
    byte[] key = new byte[] { 0, 0, 1 };
    byte[] data = new byte[] { 42 };
    
    RedisQueryCache cache = new RedisQueryCache();
    
    try {
      cache.cache(key, data, 600000, TimeUnit.MILLISECONDS);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    
    assertNull(cache.initialize(tsdb).join(1));

    cache.cache(key, data, 600000, TimeUnit.MILLISECONDS);
    verify(connection_pool, times(1)).getResource();
    verify(instance, times(1)).set(key, data, RedisQueryCache.NX, 
        RedisQueryCache.EXP, 600000L);
    verify(connection_pool, never()).close();
    verify(instance, times(1)).close();
    
    try {
      cache.cache(null, data, 600000, TimeUnit.MILLISECONDS);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      cache.cache(new byte[] { }, data, 600000, TimeUnit.MILLISECONDS);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    cache.cache(key, data, 0, TimeUnit.MILLISECONDS);
    verify(connection_pool, times(1)).getResource();
    verify(instance, times(1)).set(key, data, RedisQueryCache.NX, 
        RedisQueryCache.EXP, 600000L);
    verify(connection_pool, never()).close();
    verify(instance, times(1)).close();
    
    try {
      cache.cache(key, data, 600000, TimeUnit.NANOSECONDS);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    when(instance.set(key, data, RedisQueryCache.NX, RedisQueryCache.EXP, 
        600000L)).thenThrow(new IllegalArgumentException("Boo!"));
    cache.cache(key, data, 600000, TimeUnit.MILLISECONDS);
    verify(connection_pool, times(2)).getResource();
    verify(instance, times(2)).set(key, data, RedisQueryCache.NX, 
        RedisQueryCache.EXP, 600000L);
    verify(connection_pool, never()).close();
    verify(instance, times(2)).close();
  }
  
  @Test
  public void cacheMultiKey() throws Exception {
    byte[][] keys = new byte[][] { { 0, 0, 1 }, { 0, 0, 2 } };
    byte[][] data = new byte[][] { { 42 }, { 24 } };
    long[] expirations = new long[] { 600000, 300000 };
    
    RedisQueryCache cache = new RedisQueryCache();
    
    try {
      cache.cache(keys, data, expirations, TimeUnit.MILLISECONDS);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    
    assertNull(cache.initialize(tsdb).join(1));

    cache.cache(keys, data, expirations, TimeUnit.MILLISECONDS);
    verify(connection_pool, times(1)).getResource();
    verify(instance, times(1)).set(new byte[] { 0, 0, 1 }, new byte[] { 42 }, 
        RedisQueryCache.NX, RedisQueryCache.EXP, 600000L);
    verify(instance, times(1)).set(new byte[] { 0, 0, 2 }, new byte[] { 24 }, 
        RedisQueryCache.NX, RedisQueryCache.EXP, 300000L);
    verify(connection_pool, never()).close();
    verify(instance, times(1)).close();
    
    try {
      cache.cache(null, data, expirations, TimeUnit.MILLISECONDS);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      cache.cache(new byte[][] { }, data, expirations, TimeUnit.MILLISECONDS);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    cache.cache(keys, data, new long[] { 600000, 0 }, TimeUnit.MILLISECONDS);
    verify(connection_pool, times(2)).getResource();
    verify(instance, times(2)).set(new byte[] { 0, 0, 1 }, new byte[] { 42 }, 
        RedisQueryCache.NX, RedisQueryCache.EXP, 600000L);
    verify(instance, times(1)).set(new byte[] { 0, 0, 2 }, new byte[] { 24 }, 
        RedisQueryCache.NX, RedisQueryCache.EXP, 300000L);
    verify(instance, never()).set(new byte[] { 0, 0, 2 }, new byte[] { 24 }, 
        RedisQueryCache.NX, RedisQueryCache.EXP, 0L);
    verify(connection_pool, never()).close();
    verify(instance, times(2)).close();
    
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
    
    when(instance.set(new byte[] { 0, 0, 1 }, new byte[] { 42 }, 
        RedisQueryCache.NX, RedisQueryCache.EXP, 600000L))
        .thenThrow(new IllegalArgumentException("Boo!"));
    cache.cache(keys, data, expirations, TimeUnit.MILLISECONDS);
    verify(connection_pool, times(3)).getResource();
    verify(instance, times(3)).set(new byte[] { 0, 0, 1 }, new byte[] { 42 }, 
        RedisQueryCache.NX, RedisQueryCache.EXP, 600000L);
    // not called
    verify(instance, times(1)).set(new byte[] { 0, 0, 2 }, new byte[] { 24 }, 
        RedisQueryCache.NX, RedisQueryCache.EXP, 300000L);
    verify(connection_pool, never()).close();
    verify(instance, times(3)).close();
  }

  @Test
  public void fetch() throws Exception {
    byte[] key = new byte[] { 0, 0, 1 };
    byte[] data = new byte[] { 42 };
    
    RedisQueryCache cache = new RedisQueryCache();
    QueryExecution<byte[]> exec = cache.fetch(context, key, null);
    
    try {
      exec.deferred().join(1);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    
    cache.initialize(tsdb).join(1);
    
    exec = cache.fetch(context, key, null);
    assertNull(exec.deferred().join(1));
    
    when(instance.get(key)).thenReturn(data);
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
    
    when(instance.get(key)).thenThrow(new IllegalStateException("Boo!"));
    exec = cache.fetch(context, key, null);
    assertNull(exec.deferred().join(1));
  }
  
  @Test
  public void fetchMultiKey() throws Exception {
    byte[][] keys = new byte[][] { { 0, 0, 1 }, { 0, 0, 2 } };
    byte[] data_a = new byte[] { 42 };
    byte[] data_b = new byte[] { 24 };
    
    RedisQueryCache cache = new RedisQueryCache();
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
    when(instance.mget(keys)).thenReturn(cached);
    exec = cache.fetch(context, keys, null);
    response = exec.deferred().join(1);
    assertEquals(2, response.length);
    assertArrayEquals(data_a, response[0]);
    assertArrayEquals(data_b, response[1]);
    
    cached = Lists.newArrayList(null, data_b);
    when(instance.mget(keys)).thenReturn(cached);
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
    
    when(instance.mget(keys)).thenThrow(new IllegalStateException("Boo!"));
    exec = cache.fetch(context, keys, null);
    response = exec.deferred().join(1);
    assertEquals(2, response.length);
    assertNull(response[0]);
    assertNull(response[1]);
  }
}
