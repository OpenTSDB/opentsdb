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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import net.opentsdb.configuration.Configuration;
import net.opentsdb.configuration.UnitTestConfiguration;
import net.opentsdb.core.DefaultTSDB;
import net.opentsdb.query.QueryContext;
import net.opentsdb.stats.Span;
import net.opentsdb.utils.DateTime;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ DateTime.class, GuavaLRUCache.class })
public class TestGuavaLRUCache {

  private DefaultTSDB tsdb;
  private Configuration config;
  private QueryContext context;
  private Span span;
  
  @Before
  public void before() throws Exception {
    tsdb = mock(DefaultTSDB.class);
    config = UnitTestConfiguration.getConfiguration();
    context = mock(QueryContext.class);
    span = mock(Span.class);
    when(tsdb.getConfig()).thenReturn(config);
  }
  
  @Test
  public void initialize() throws Exception {
    GuavaLRUCache cache = new GuavaLRUCache();
    cache.initialize(tsdb).join();
    assertEquals(GuavaLRUCache.DEFAULT_SIZE_LIMIT, cache.sizeLimit());
    assertEquals(GuavaLRUCache.DEFAULT_MAX_OBJECTS, cache.maxObjects());
    assertEquals(0, cache.bytesStored());
    assertEquals(0, cache.cache().size());
    
    config.register("tsd.executor.plugin.guava.limit.objects", 42, false, "UT");
    cache = new GuavaLRUCache();
    cache.initialize(tsdb).join();
    assertEquals(GuavaLRUCache.DEFAULT_SIZE_LIMIT, cache.sizeLimit());
    assertEquals(42, cache.maxObjects());
    assertEquals(0, cache.bytesStored());
    assertEquals(0, cache.cache().size());
    
    config.register("tsd.executor.plugin.guava.limit.bytes", 16, false, "UT");
    cache = new GuavaLRUCache();
    cache.initialize(tsdb).join();
    assertEquals(16, cache.sizeLimit());
    assertEquals(42, cache.maxObjects());
    assertEquals(0, cache.bytesStored());
    assertEquals(0, cache.cache().size());
  }
  
  @Test
  public void cacheAndFetchSingleEntry() throws Exception {
    final GuavaLRUCache cache = new GuavaLRUCache();
    cache.initialize(tsdb).join();
    assertEquals(0, cache.cache().size());
    
    cache.cache(new byte[] { 0, 0, 1 }, new byte[] { 0, 0, 1 }, 60000, 
        TimeUnit.MILLISECONDS, span);
    cache.cache(new byte[] { 0, 0, 2 }, new byte[] { 0, 0, 2 }, 60000, 
        TimeUnit.MILLISECONDS, span);
    cache.cache(new byte[] { 0, 0, 3 }, new byte[] { 0, 0, 3 }, 60000, 
        TimeUnit.MILLISECONDS, span);
    
    assertEquals(3, cache.cache().size());
    assertEquals(9, cache.bytesStored());
    
    assertArrayEquals(new byte[] { 0, 0, 1 }, 
        cache.fetch(context, new byte[] { 0, 0, 1 }, span).deferred().join());
    assertArrayEquals(new byte[] { 0, 0, 2 }, 
        cache.fetch(context, new byte[] { 0, 0, 2 }, span).deferred().join());
    assertArrayEquals(new byte[] { 0, 0, 3 }, 
        cache.fetch(context, new byte[] { 0, 0, 3 }, span).deferred().join());
    
    // no expirations
    assertEquals(3, cache.cache().size());
    assertEquals(9, cache.bytesStored());
    assertEquals(3, cache.cache().stats().requestCount());
    assertEquals(3, cache.cache().stats().hitCount());
    assertEquals(0, cache.cache().stats().missCount());
    
    assertArrayEquals(new byte[] { 0, 0, 1 }, 
        cache.fetch(context, new byte[] { 0, 0, 1 }, span).deferred().join());
    assertArrayEquals(new byte[] { 0, 0, 2 }, 
        cache.fetch(context, new byte[] { 0, 0, 2 }, span).deferred().join());
    assertArrayEquals(new byte[] { 0, 0, 3 }, 
        cache.fetch(context, new byte[] { 0, 0, 3 }, span).deferred().join());
    
    // no change, just double the hits
    assertEquals(3, cache.cache().size());
    assertEquals(9, cache.bytesStored());
    assertEquals(6, cache.cache().stats().requestCount());
    assertEquals(6, cache.cache().stats().hitCount());
    assertEquals(0, cache.cache().stats().missCount());
    
    // cache miss
    assertNull(cache.fetch(context, new byte[] { 0, 0, 4 }, span)
        .deferred().join());
    assertEquals(3, cache.cache().size());
    assertEquals(9, cache.bytesStored());
    assertEquals(7, cache.cache().stats().requestCount());
    assertEquals(6, cache.cache().stats().hitCount());
    assertEquals(1, cache.cache().stats().missCount());
    
    // null value
    cache.cache(new byte[] { 0, 0, 5 }, null, 60000, 
        TimeUnit.MILLISECONDS, span);
    assertNull(cache.fetch(context, new byte[] { 0, 0, 5 }, span)
        .deferred().join());
    
    // empty value
    cache.cache(new byte[] { 0, 0, 5 }, new byte[] { }, 60000, 
        TimeUnit.MILLISECONDS, span);
    assertEquals(0, cache.fetch(context, new byte[] { 0, 0, 5 }, span)
        .deferred().join().length);
  }
  
  @Test
  public void cacheAndFetchSingleEntryExpired() throws Exception {
    final GuavaLRUCache cache = new GuavaLRUCache();
    cache.initialize(tsdb).join();
    assertEquals(0, cache.cache().size());
    
    PowerMockito.mockStatic(DateTime.class);
    when(DateTime.nanoTime())
      .thenReturn(0L)
      .thenReturn(30000000000L)
      .thenReturn(50000000000L)
      .thenReturn(61000000000L);
    
    cache.cache(new byte[] { 0, 0, 1 }, new byte[] { 0, 0, 1 }, 60000, 
        TimeUnit.MILLISECONDS, span);
    
    assertArrayEquals(new byte[] { 0, 0, 1 }, 
        cache.fetch(context, new byte[] { 0, 0, 1 }, span).deferred().join());
    assertEquals(1, cache.cache().size());
    assertEquals(3, cache.bytesStored());
    assertEquals(1, cache.cache().stats().requestCount());
    assertEquals(1, cache.cache().stats().hitCount());
    assertEquals(0, cache.cache().stats().missCount());
    assertEquals(0, cache.cache().stats().evictionCount());
    assertEquals(0, cache.expired());
    
    assertArrayEquals(new byte[] { 0, 0, 1 }, 
        cache.fetch(context, new byte[] { 0, 0, 1 }, span).deferred().join());
    assertEquals(1, cache.cache().size());
    assertEquals(3, cache.bytesStored());
    assertEquals(2, cache.cache().stats().requestCount());
    assertEquals(2, cache.cache().stats().hitCount());
    assertEquals(0, cache.cache().stats().missCount());
    assertEquals(0, cache.cache().stats().evictionCount());
    assertEquals(0, cache.expired());
    
    assertNull(cache.fetch(context, new byte[] { 0, 0, 1 }, span)
        .deferred().join());
    assertEquals(0, cache.cache().size());
    assertEquals(0, cache.bytesStored());
    assertEquals(3, cache.cache().stats().requestCount());
    assertEquals(3, cache.cache().stats().hitCount());
    assertEquals(0, cache.cache().stats().missCount());
    assertEquals(0, cache.cache().stats().evictionCount());
    assertEquals(1, cache.expired());
  }
  
  @Test
  public void cacheAndFetchMultiEntry() throws Exception {
    final GuavaLRUCache cache = new GuavaLRUCache();
    cache.initialize(tsdb).join();
    assertEquals(0, cache.cache().size());
    
    byte[][] keys = new byte[][] {
        new byte[] { 0, 0, 1 },
        new byte[] { 0, 0, 2 },
        new byte[] { 0, 0, 3 }
    };
    
    final byte[][] values = new byte[][] {
      new byte[] { 0, 0, 1 },
      new byte[] { 0, 0, 2 },
      new byte[] { 0, 0, 3 }
    };
    
    cache.cache(keys, values, new long[] { 60000, 60000, 60000 }, 
        TimeUnit.MILLISECONDS, span);

    assertEquals(3, cache.cache().size());
    assertEquals(9, cache.bytesStored());
    
    // fetch out via singles
    assertArrayEquals(new byte[] { 0, 0, 1 }, 
        cache.fetch(context, new byte[] { 0, 0, 1 }, span).deferred().join());
    assertArrayEquals(new byte[] { 0, 0, 2 }, 
        cache.fetch(context, new byte[] { 0, 0, 2 }, span).deferred().join());
    assertArrayEquals(new byte[] { 0, 0, 3 }, 
        cache.fetch(context, new byte[] { 0, 0, 3 }, span).deferred().join());
    // no expirations
    assertEquals(3, cache.cache().size());
    assertEquals(9, cache.bytesStored());
    assertEquals(3, cache.cache().stats().requestCount());
    assertEquals(3, cache.cache().stats().hitCount());
    assertEquals(0, cache.cache().stats().missCount());
    
    // fetch all
    byte[][] results = cache.fetch(context, keys, span).deferred().join();
    assertEquals(3, results.length);
    assertArrayEquals(new byte[] { 0, 0, 1 }, results[0]);
    assertArrayEquals(new byte[] { 0, 0, 2 }, results[1]);
    assertArrayEquals(new byte[] { 0, 0, 3 }, results[2]);
    
    // no change, just double the hits
    assertEquals(3, cache.cache().size());
    assertEquals(9, cache.bytesStored());
    assertEquals(6, cache.cache().stats().requestCount());
    assertEquals(6, cache.cache().stats().hitCount());
    assertEquals(0, cache.cache().stats().missCount());
    
    // cache miss
    assertNull(cache.fetch(context, new byte[] { 0, 0, 4 }, span)
        .deferred().join());
    assertEquals(3, cache.cache().size());
    assertEquals(9, cache.bytesStored());
    assertEquals(7, cache.cache().stats().requestCount());
    assertEquals(6, cache.cache().stats().hitCount());
    assertEquals(1, cache.cache().stats().missCount());
    
    // multi cache miss
    keys = new byte[][] {
      new byte[] { 0, 0, 5 },
      new byte[] { 0, 0, 2 },
      new byte[] { 0, 0, 6 }
    };
    
    results = cache.fetch(context, keys, span).deferred().join();
    assertEquals(3, results.length);
    assertNull(results[0]);
    assertArrayEquals(new byte[] { 0, 0, 2 }, results[1]);
    assertNull(results[2]);
    assertEquals(3, cache.cache().size());
    assertEquals(9, cache.bytesStored());
    assertEquals(10, cache.cache().stats().requestCount());
    assertEquals(7, cache.cache().stats().hitCount());
    assertEquals(3, cache.cache().stats().missCount());
  }
  
  @Test
  public void cacheAndFetchMultiEntryExpired() throws Exception {
    final GuavaLRUCache cache = new GuavaLRUCache();
    cache.initialize(tsdb).join();
    assertEquals(0, cache.cache().size());
    
    PowerMockito.mockStatic(DateTime.class);
    when(DateTime.nanoTime())
      .thenReturn(0L)
      .thenReturn(0L)
      .thenReturn(30000000000L)
      .thenReturn(30000000000L)
      .thenReturn(61000000000L)
      .thenReturn(61000000000L);
    
    byte[][] keys = new byte[][] {
        new byte[] { 0, 0, 1 },
        new byte[] { 0, 0, 2 }
    };
    
    byte[][] values = new byte[][] {
      new byte[] { 0, 0, 1 },
      new byte[] { 0, 0, 2 }
    };
    
    cache.cache(keys, values, new long[] { 60000, 60000 }, 
        TimeUnit.MILLISECONDS, span);

    assertEquals(2, cache.cache().size());
    assertEquals(6, cache.bytesStored());
    
    // fetch all
    byte[][] results = cache.fetch(context, keys, span).deferred().join();
    assertEquals(2, results.length);
    assertArrayEquals(new byte[] { 0, 0, 1 }, results[0]);
    assertArrayEquals(new byte[] { 0, 0, 2 }, results[1]);
    
    // no expirations
    assertEquals(2, cache.cache().size());
    assertEquals(6, cache.bytesStored());
    assertEquals(2, cache.cache().stats().requestCount());
    assertEquals(2, cache.cache().stats().hitCount());
    assertEquals(0, cache.cache().stats().missCount());
    assertEquals(0, cache.expired());
    
    // cache miss
    results = cache.fetch(context, keys, span).deferred().join();
    assertEquals(2, results.length);
    assertNull(results[0]);
    assertNull(results[1]);
    assertEquals(0, cache.cache().size());
    assertEquals(0, cache.bytesStored());
    assertEquals(4, cache.cache().stats().requestCount());
    assertEquals(4, cache.cache().stats().hitCount());
    assertEquals(0, cache.cache().stats().missCount());
    assertEquals(2, cache.expired());
    
    // null values
    values = new byte[][] {
      null,
      null
    };
    cache.cache(keys, values, new long[] { 60000, 60000 }, 
        TimeUnit.MILLISECONDS, span);
    results = cache.fetch(context, keys, span).deferred().join();
    assertNull(results[0]);
    assertNull(results[1]);
    
    // empty values
    values = new byte[][] {
      new byte[] { },
      new byte[] { }
    };
    cache.cache(keys, values, new long[] { 60000, 60000 }, 
        TimeUnit.MILLISECONDS, span);
    results = cache.fetch(context, keys, span).deferred().join();
    assertEquals(0, results[0].length);
    assertEquals(0, results[1].length);
  }
  
  @Test
  public void cacheExceptions() throws Exception {
    byte[][] keys = new byte[][] {
        new byte[] { 0, 0, 1 },
        new byte[] { 0, 0, 2 },
        new byte[] { 0, 0, 3 }
    };
    
    final byte[][] values = new byte[][] {
      new byte[] { 0, 0, 1 },
      new byte[] { 0, 0, 2 },
      new byte[] { 0, 0, 3 }
    };
    
    final GuavaLRUCache cache = new GuavaLRUCache();
    try {
      cache.cache(new byte[] { 0, 0, 1 }, new byte[] { 0, 0, 1 }, 60000, 
          TimeUnit.MILLISECONDS, span);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    
    try {
      cache.cache(keys, values, new long[] { 60000, 60000, 6000 }, 
          TimeUnit.MILLISECONDS, span);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    
    cache.initialize(tsdb).join();
    
    try {
      cache.cache(null, new byte[] { 0, 0, 1 }, 60000, TimeUnit.MILLISECONDS, span);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      cache.cache(new byte[] { }, new byte[] { 0, 0, 1 }, 60000, 
          TimeUnit.MILLISECONDS, span);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      cache.cache(null, values, new long[] { 60000, 60000, 60000 }, 
          TimeUnit.MILLISECONDS, span);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      cache.cache(new byte[][] { }, values, new long[] { 60000, 60000, 60000 }, 
          TimeUnit.MILLISECONDS, span);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    keys = new byte[][] {
        new byte[] { 0, 0, 1 },
        new byte[] { 0, 0, 2 },
        //new byte[] { 0, 0, 3 }
    };
  
    try {
      cache.cache(keys, values, new long[] { 60000, 60000, 60000 }, 
          TimeUnit.MILLISECONDS, span);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    keys = new byte[][] {
        new byte[] { 0, 0, 1 },
        new byte[] { 0, 0, 2 },
        new byte[] { 0, 0, 3 }
    };
  
    try {
      cache.cache(keys, null, new long[] { 60000, 60000, 60000 }, 
          TimeUnit.MILLISECONDS, span);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      cache.cache(keys, keys, null, TimeUnit.MILLISECONDS, span);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // wrong expirations length
    try {
      cache.cache(keys, keys, new long[] { 60000, 60000 }, 
          TimeUnit.MILLISECONDS, span);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void fetchExceptions() throws Exception {
    byte[][] keys = new byte[][] {
        new byte[] { 0, 0, 1 },
        new byte[] { 0, 0, 2 },
        new byte[] { 0, 0, 3 }
    };

    final GuavaLRUCache cache = new GuavaLRUCache();
    try {
      cache.fetch(context, new byte[] { 0, 0, 1 }, span).deferred().join();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    
    try {
      cache.fetch(context, keys, span).deferred().join();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    
    cache.initialize(tsdb).join();
    
    try {
      cache.fetch(context, (byte[]) null, span).deferred().join();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      cache.fetch(context, (byte[][]) null, span).deferred().join();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      cache.fetch(context, new byte[] { }, span).deferred().join();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      cache.fetch(context, new byte[][] { }, span).deferred().join();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
}
