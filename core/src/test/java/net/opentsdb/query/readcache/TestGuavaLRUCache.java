// This file is part of OpenTSDB.
// Copyright (C) 2017-2019  The OpenTSDB Authors.
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
package net.opentsdb.query.readcache;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stumbleupon.async.Deferred;

import net.opentsdb.core.MockTSDB;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.readcache.GuavaLRUCache;
import net.opentsdb.query.readcache.ReadCacheCallback;
import net.opentsdb.query.readcache.ReadCacheQueryResult;
import net.opentsdb.query.readcache.ReadCacheQueryResultSet;
import net.opentsdb.query.readcache.ReadCacheSerdes;
import net.opentsdb.query.readcache.ReadCacheSerdesFactory;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.Bytes.ByteMap;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.UnitTestException;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ DateTime.class, GuavaLRUCache.class })
public class TestGuavaLRUCache {
  private static final int BASE_TIME = 1546300800;
  
  private MockTSDB tsdb;
  private QueryPipelineContext context;
  private ReadCacheSerdesFactory factory;
  private ReadCacheSerdes serdes;
  private ByteMap<SerdesObj> serdes_calls;
  private int[] incrementer;
  
  @Before
  public void before() throws Exception {
    tsdb = new MockTSDB();
    context = mock(QueryPipelineContext.class);
    factory = mock(ReadCacheSerdesFactory.class);
    serdes = mock(ReadCacheSerdes.class);
    
    when(tsdb.registry.getPlugin(eq(ReadCacheSerdesFactory.class), anyString()))
    .thenReturn(factory);
    when(factory.getSerdes()).thenReturn(serdes);
    serdes_calls = new ByteMap<SerdesObj>();
    incrementer = new int[1];
    when(serdes.serialize(any(Collection.class))).thenAnswer(
        new Answer<byte[]>() {
      @Override
      public byte[] answer(InvocationOnMock invocation) throws Throwable {
        byte[] serialized = Bytes.fromInt(incrementer[0]++);
        SerdesObj obj = new SerdesObj((Collection<QueryResult>) invocation.getArguments()[0]);
        serdes_calls.put(serialized, obj);
        return serialized;
      }
    });
    
    when(serdes.serialize(any(int[].class), any(Collection.class)))
    .thenAnswer(new Answer<byte[][]>() {
      @Override
      public byte[][] answer(InvocationOnMock invocation) throws Throwable {
        byte[][] results = new byte[((int[]) invocation.getArguments()[0]).length][];
        int i = 0;
        for (int ts : (int[]) invocation.getArguments()[0]) {
          byte[] serialized = Bytes.fromInt(ts);
          results[i++] = serialized;
          SerdesObj obj = new SerdesObj((Collection<QueryResult>) invocation.getArguments()[1]);
          serdes_calls.put(serialized, obj);
        }
        return results;
      }
    });
    
    when(serdes.deserialize(any(byte[].class))).thenAnswer(
        new Answer<Map<String, ReadCacheQueryResult>>() {
          @Override
          public Map<String, ReadCacheQueryResult> answer(
              InvocationOnMock invocation) throws Throwable {
            SerdesObj obj = serdes_calls.get((byte[]) invocation.getArguments()[0]);
            return obj.deserialized;
          }
    });
  }
  
  @Test
  public void initialize() throws Exception {
    GuavaLRUCache cache = new GuavaLRUCache();
    cache.initialize(tsdb, null).join();
    assertEquals(GuavaLRUCache.DEFAULT_SIZE_LIMIT, cache.sizeLimit());
    assertEquals(GuavaLRUCache.DEFAULT_MAX_OBJECTS, cache.maxObjects());
    assertEquals(0, cache.bytesStored());
    assertEquals(0, cache.cache().size());
    
    tsdb.config.override(cache.getConfigKey(GuavaLRUCache.OBJECTS_LIMIT_KEY), 42);
    cache = new GuavaLRUCache();
    cache.initialize(tsdb, null).join();
    assertEquals(GuavaLRUCache.DEFAULT_SIZE_LIMIT, cache.sizeLimit());
    assertEquals(42, cache.maxObjects());
    assertEquals(0, cache.bytesStored());
    assertEquals(0, cache.cache().size());
    
    tsdb.config.override(cache.getConfigKey(GuavaLRUCache.SIZE_LIMIT_KEY), 16);
    cache = new GuavaLRUCache();
    cache.initialize(tsdb, null).join();
    assertEquals(16, cache.sizeLimit());
    assertEquals(42, cache.maxObjects());
    assertEquals(0, cache.bytesStored());
    assertEquals(0, cache.cache().size());
  }
  
  @Test
  public void cacheAndFetchSingle() throws Exception {
    final GuavaLRUCache cache = new GuavaLRUCache();
    cache.initialize(tsdb, null).join();
    assertEquals(0, cache.cache().size());
    
    byte[] key1 = new byte[] { 0, 0, 1 };
    Collection<QueryResult> qr1 = mock(Collection.class);
    
    Deferred<Void> deferred = cache.cache(BASE_TIME, key1, 60000, qr1, null);
    assertEquals(1, cache.cache().size());
    assertEquals(4, cache.bytesStored());
    assertEquals(1, serdes_calls.size());
    assertNull(deferred.join());
    assertEquals(1, cache.cache().size());
    assertEquals(4, cache.bytesStored());
    
    byte[] key2 = new byte[] { 0, 0, 2 };
    Collection<QueryResult> qr2 = mock(Collection.class);
    
    deferred = cache.cache(BASE_TIME, key2, 60000, qr2, null);
    assertEquals(2, cache.cache().size());
    assertEquals(8, cache.bytesStored());
    assertEquals(2, serdes_calls.size());
    assertNull(deferred.join());
    assertEquals(2, cache.cache().size());
    assertEquals(8, cache.bytesStored());
    
    // overwrite
    Collection<QueryResult> qr3 = mock(Collection.class);
    deferred = cache.cache(BASE_TIME, key1, 60000, qr3, null);
    assertEquals(2, cache.cache().size());
    assertEquals(8, cache.bytesStored());
    assertEquals(3, serdes_calls.size());
    assertNull(deferred.join());
    assertEquals(2, cache.cache().size());
    assertEquals(8, cache.bytesStored());
    
    // not stored due to expiration
    byte[] key3 = new byte[] { 0, 0, 3 };
    Collection<QueryResult> qr4 = mock(Collection.class);
    deferred = cache.cache(BASE_TIME, key3, -1, qr4, null);
    assertEquals(2, cache.cache().size());
    assertEquals(8, cache.bytesStored());
    assertEquals(3, serdes_calls.size());
    assertNull(deferred.join());
    assertEquals(2, cache.cache().size());
    assertEquals(8, cache.bytesStored());
    
    // fetch
    ReadCacheQueryResultSet[] results = new ReadCacheQueryResultSet[2];
    Throwable[] errors = new Throwable[2];
    class CB implements ReadCacheCallback {
      @Override
      public void onCacheResult(final ReadCacheQueryResultSet result) {
        results[result.index()] = result;
      }

      @Override
      public void onCacheError(final int index, final Throwable t) {
        errors[index] = t;
      }
    }
    
    cache.fetch(context, new byte[][] { key1, key2 }, new CB(), null);
    assertNull(errors[0]);
    SerdesObj obj = serdes_calls.get(Bytes.fromInt(2));
    assertSame(qr3, obj.source);
    assertEquals(0, results[0].index());
    assertArrayEquals(key1, results[0].key());
    assertSame(obj.deserialized, results[0].results());
    obj = serdes_calls.get(Bytes.fromInt(1));
    assertSame(qr2, obj.source);
    assertEquals(1, results[1].index());
    assertArrayEquals(key2, results[1].key());
    assertSame(obj.deserialized, results[1].results());
    
    // one hit one miss
    results[0] = null;
    results[1] = null;    
    cache.fetch(context, new byte[][] { new byte[] { 0, 0, 4 }, key2 }, new CB(), null);
    assertNull(errors[0]);
    assertEquals(0, results[0].index());
    assertArrayEquals(new byte[] { 0, 0, 4 }, results[0].key());
    assertNull(results[0].results());
    obj = serdes_calls.get(Bytes.fromInt(1));
    assertSame(qr2, obj.source);
    assertEquals(1, results[1].index());
    assertArrayEquals(key2, results[1].key());
    assertSame(obj.deserialized, results[1].results());
    
    // expired
    long ts = DateTime.nanoTime();
    PowerMockito.mockStatic(DateTime.class);
    when(DateTime.nanoTime())
      .thenReturn(ts + 61000000000L);
    
    cache.fetch(context, new byte[][] { key1, key2 }, new CB(), null);
    assertNull(errors[0]);
    assertEquals(0, results[0].index());
    assertArrayEquals(key1, results[0].key());
    assertNull(results[0].results());
    assertEquals(1, results[1].index());
    assertArrayEquals(key2, results[1].key());
    assertNull(results[1].results());
    assertEquals(2, cache.expired());
    assertEquals(0, cache.cache().size());
    assertEquals(0, cache.bytesStored());
  }
  
  @Test
  public void cacheAndFetchSingleExceptions() throws Exception {
    final GuavaLRUCache cache = new GuavaLRUCache();
    cache.initialize(tsdb, null).join();
    
    byte[] key1 = new byte[] { 0, 0, 1 };
    Collection<QueryResult> qr1 = mock(Collection.class);
    
    Deferred<Void> deferred = cache.cache(BASE_TIME, null, 60000, qr1, null);
    try {
      deferred.join();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    deferred = cache.cache(BASE_TIME, new byte[0], 60000, qr1, null);
    try {
      deferred.join();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // store a good one first to test the deser throw
    deferred = cache.cache(BASE_TIME, key1, 60000, qr1, null);
    assertEquals(1, cache.cache().size());
    assertEquals(4, cache.bytesStored());
    assertEquals(1, serdes_calls.size());
    assertNull(deferred.join());
    assertEquals(1, cache.cache().size());
    assertEquals(4, cache.bytesStored());
    
    when(serdes.serialize(any(Collection.class))).thenThrow(new UnitTestException());
    deferred = cache.cache(BASE_TIME, key1, 60000, qr1, null);
    try {
      deferred.join();
      fail("Expected UnitTestException");
    } catch (UnitTestException e) { }
    
    doThrow(new UnitTestException()).when(serdes).deserialize(any(byte[].class));
    ReadCacheQueryResultSet[] results = new ReadCacheQueryResultSet[2];
    Throwable[] errors = new Throwable[2];
    class CB implements ReadCacheCallback {
      @Override
      public void onCacheResult(final ReadCacheQueryResultSet result) {
        results[result.index()] = result;
      }

      @Override
      public void onCacheError(final int index, final Throwable t) {
        errors[index] = t;
      }
    }
    cache.fetch(context, new byte[][] { key1, new byte[] { 0, 0, 2 } }, new CB(), null);
    assertNull(results[0]);
    assertEquals(1, results[1].index());
    assertArrayEquals(new byte[] { 0, 0, 2 }, results[1].key());
    assertNull(results[1].results());
    assertTrue(errors[0] instanceof UnitTestException);
  }

  @Test
  public void cacheAndFetchMultiple() throws Exception {
    final GuavaLRUCache cache = new GuavaLRUCache();
    cache.initialize(tsdb, null).join();
    assertEquals(0, cache.cache().size());
    
    byte[] key1 = new byte[] { 0, 0, 1 };
    byte[] key2 = new byte[] { 0, 0, 2 };
    Collection<QueryResult> qr1 = mock(Collection.class);
    Collection<QueryResult> qr2 = mock(Collection.class);
    
    Deferred<Void> deferred = cache.cache(new int[] { BASE_TIME, BASE_TIME + 3600}, 
        new byte[][] { key1, key2 }, 
        new long[] { 60000, 30000 }, 
        qr1, null);
    assertNull(deferred.join());
    assertEquals(2, cache.cache().size());
    assertEquals(8, cache.bytesStored());
    assertEquals(2, serdes_calls.size());
    assertEquals(2, cache.cache().size());
    assertEquals(8, cache.bytesStored());
    SerdesObj obj = serdes_calls.get(Bytes.fromInt(BASE_TIME));
    assertSame(qr1, obj.source);
    obj = serdes_calls.get(Bytes.fromInt(BASE_TIME + 3600));
    assertSame(qr1, obj.source);
    assertEquals(2, cache.cache().size());
    assertEquals(8, cache.bytesStored());
    
    // replace as if we replaced the tip
    deferred = cache.cache(new int[] { BASE_TIME + 3600}, 
        new byte[][] { key2 }, 
        new long[] { 30000 }, 
        qr2, null);
    assertNull(deferred.join());
    assertEquals(2, cache.cache().size());
    assertEquals(8, cache.bytesStored());
    assertEquals(2, serdes_calls.size());
    assertEquals(2, cache.cache().size());
    assertEquals(8, cache.bytesStored());
    obj = serdes_calls.get(Bytes.fromInt(BASE_TIME));
    assertSame(qr1, obj.source);
    obj = serdes_calls.get(Bytes.fromInt(BASE_TIME + 3600));
    assertSame(qr2, obj.source);
    assertEquals(2, cache.cache().size());
    assertEquals(8, cache.bytesStored());
    
    // fetch
    ReadCacheQueryResultSet[] results = new ReadCacheQueryResultSet[2];
    Throwable[] errors = new Throwable[2];
    class CB implements ReadCacheCallback {
      @Override
      public void onCacheResult(final ReadCacheQueryResultSet result) {
        results[result.index()] = result;
      }

      @Override
      public void onCacheError(final int index, final Throwable t) {
        errors[index] = t;
      }
    }
    
    cache.fetch(context, new byte[][] { key1, key2 }, new CB(), null);
    assertNull(errors[0]);
    obj = serdes_calls.get(Bytes.fromInt(BASE_TIME));
    assertSame(qr1, obj.source);
    assertEquals(0, results[0].index());
    assertArrayEquals(key1, results[0].key());
    assertSame(obj.deserialized, results[0].results());
    obj = serdes_calls.get(Bytes.fromInt(BASE_TIME + 3600));
    assertSame(qr2, obj.source);
    assertEquals(1, results[1].index());
    assertArrayEquals(key2, results[1].key());
    assertSame(obj.deserialized, results[1].results());
    
    // tip expired
    long ts = DateTime.nanoTime();
    PowerMockito.mockStatic(DateTime.class);
    when(DateTime.nanoTime())
      .thenReturn(ts + 31000000000L);
    
    cache.fetch(context, new byte[][] { key1, key2 }, new CB(), null);
    assertNull(errors[0]);
    obj = serdes_calls.get(Bytes.fromInt(BASE_TIME));
    assertSame(qr1, obj.source);
    assertEquals(0, results[0].index());
    assertArrayEquals(key1, results[0].key());
    assertSame(obj.deserialized, results[0].results());
    assertEquals(1, results[1].index());
    assertArrayEquals(key2, results[1].key());
    assertNull(results[1].results());
    assertEquals(1, cache.expired());
    assertEquals(1, cache.cache().size());
    assertEquals(4, cache.bytesStored());
  }

  @Test
  public void cacheAndFetchMultipleExceptions() throws Exception {
    final GuavaLRUCache cache = new GuavaLRUCache();
    cache.initialize(tsdb, null).join();
    
    byte[] key1 = new byte[] { 0, 0, 1 };
    byte[] key2 = new byte[] { 0, 0, 2 };
    Collection<QueryResult> qr1 = mock(Collection.class);
    
    Deferred<Void> deferred = cache.cache(null, 
        new byte[][] { key1, key2 }, 
        new long[] { 60000, 30000 }, 
        qr1, null);
    try {
      deferred.join();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    deferred = cache.cache(new int[0], 
        new byte[][] { key1, key2 }, 
        new long[] { 60000, 30000 }, 
        qr1, null);
    try {
      deferred.join();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    deferred = cache.cache(new int[] { BASE_TIME, BASE_TIME + 3600}, 
        null, 
        new long[] { 60000, 30000 }, 
        qr1, null);
    try {
      deferred.join();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    deferred = cache.cache(new int[] { BASE_TIME, BASE_TIME + 3600}, 
        new byte[][] { key1, key2 }, 
        null, 
        qr1, null);
    try {
      deferred.join();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    deferred = cache.cache(new int[] { BASE_TIME, BASE_TIME + 3600}, 
        new byte[][] { key1 }, 
        new long[] { 60000, 30000 }, 
        qr1, null);
    try {
      deferred.join();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    deferred = cache.cache(new int[] { BASE_TIME, BASE_TIME + 3600}, 
        new byte[][] { key1, key2 }, 
        new long[] { 60000, 30000 }, 
        null,
        null);
    try {
      deferred.join();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    doThrow(new UnitTestException()).when(serdes)
      .serialize(any(int[].class), any(Collection.class));
    deferred = cache.cache(new int[] { BASE_TIME, BASE_TIME + 3600}, 
        new byte[][] { key1, key2 }, 
        new long[] { 60000, 30000 }, 
        qr1, null);
    try {
      deferred.join();
      fail("Expected UnitTestException");
    } catch (UnitTestException e) { }
  }
  
  static class SerdesObj {
    final Collection<QueryResult> source;
    final Map<String, ReadCacheQueryResult> deserialized;
    
    SerdesObj(final Collection<QueryResult> source) {
      this.source = source;
      deserialized = mock(Map.class);
    }
  }
}