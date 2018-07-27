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
package net.opentsdb.query.execution;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.common.Const;
import net.opentsdb.configuration.Configuration;
import net.opentsdb.data.iterators.IteratorGroups;
import net.opentsdb.query.ConvertedQueryResult;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.execution.CachingQueryExecutor.Config;
import net.opentsdb.query.execution.CachingQueryExecutor.LocalExecution;
import net.opentsdb.query.execution.TestQueryExecutor.MockDownstream;
import net.opentsdb.query.execution.cache.TimeSeriesCacheKeyGenerator;
import net.opentsdb.query.execution.cache.DefaultTimeSeriesCacheKeyGenerator;
import net.opentsdb.query.execution.cache.QueryCachePlugin;
import net.opentsdb.query.execution.graph.ExecutionGraphNode;
import net.opentsdb.query.pojo.Metric;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.query.pojo.Timespan;
import net.opentsdb.stats.Span;
import net.opentsdb.query.serdes.SerdesOptions;
import net.opentsdb.query.serdes.TimeSeriesSerdes;
import net.opentsdb.utils.JSON;
import net.opentsdb.utils.UnitTestException;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ CachingQueryExecutor.class, ConvertedQueryResult.class })
public class TestCachingQueryExecutor extends BaseExecutorTest {
  private QueryExecutor<IteratorGroups> executor;
  private MockDownstream<IteratorGroups> cache_execution;
  private Configuration tsd_config;
  private Config config;
  private QueryCachePlugin plugin;
  private TimeSeriesSerdes serdes;
  private TimeSeriesCacheKeyGenerator key_generator;
  
  @SuppressWarnings("unchecked")
  @Before
  public void beforeLocal() throws Exception {
    node = mock(ExecutionGraphNode.class);
    executor = mock(QueryExecutor.class);
    plugin = mock(QueryCachePlugin.class);
    config = (Config) Config.newBuilder()
        .setExpiration(60000)
        .setKeyGeneratorId("MyKeyGen")
        .setId("LocalCache")
        .build();
    serdes = mock(TimeSeriesSerdes.class);
    
    tsd_config = new Configuration(new String[] { 
        "--" + Configuration.CONFIG_PROVIDERS_KEY + "=RuntimeOverride" });
    
    when(tsdb.getConfig()).thenReturn(tsd_config);
    key_generator = new DefaultTimeSeriesCacheKeyGenerator();
    key_generator.initialize(tsdb).join();
    
    when(executor.close()).thenReturn(Deferred.fromResult(null));
    when(registry.getPlugin(eq(QueryCachePlugin.class), anyString()))
      .thenReturn(plugin);
    when(registry.getDefaultPlugin(eq(QueryCachePlugin.class)))
      .thenReturn(plugin);
    when(registry.getPlugin(eq(TimeSeriesCacheKeyGenerator.class), anyString()))
      .thenReturn(key_generator);
    when(registry.getDefaultPlugin(eq(TimeSeriesCacheKeyGenerator.class)))
    .thenReturn(key_generator);
    when(registry.getSerdes(anyString()))
      .thenAnswer(new Answer<TimeSeriesSerdes>() {
      @Override
      public TimeSeriesSerdes answer(
          final InvocationOnMock invocation) throws Throwable {
        return serdes;
      }
    });
    when(registry.getDefaultPlugin(eq(TimeSeriesSerdes.class)))
      .thenReturn(serdes);
    query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("1h-ago")
            .setAggregator("sum"))
        .addMetric(Metric.newBuilder()
            .setId("m1")
            .setMetric("system.cpu.user"))
        .build()
        .convert().build();
    when(pcontext.query()).thenReturn(query);
   // cache_execution = new MockDownstream<IteratorGroups>(query);
    when(plugin.fetch(any(QueryContext.class), any(byte[].class), any(Span.class)))
      .thenAnswer(new Answer<QueryExecution<IteratorGroups>>() {
        @Override
        public QueryExecution<IteratorGroups> answer(
            final InvocationOnMock invocation) throws Throwable {
          return cache_execution;
        }
      });
    when(serdes.serialize(any(QueryContext.class), 
        any(SerdesOptions.class), any(OutputStream.class), 
        any(QueryResult.class), any(Span.class)))
      .thenReturn(Deferred.fromResult(null));
  }
  
//  @Test
//  public void ctor() throws Exception {
//    CachingQueryExecutor executor = new CachingQueryExecutor(tsdb);
//    assertSame(plugin, executor.plugin());
//    assertSame(serdes, executor.serdes());
//    assertTrue(executor.keyGenerator() instanceof 
//        DefaultTimeSeriesCacheKeyGenerator);
//    
//    try {
//      new CachingQueryExecutor((ExecutionGraphNode) null);
//      fail("Expected IllegalArgumentException");
//    } catch (IllegalArgumentException e) { }
//    
//    when(node.getConfig()).thenReturn(null);
//    try {
//      new CachingQueryExecutor(tsdb);
//      fail("Expected IllegalArgumentException");
//    } catch (IllegalArgumentException e) { }
//    
//    when(node.getConfig()).thenReturn(config);
//    when(registry.getPlugin(eq(QueryCachePlugin.class), anyString()))
//      .thenReturn(plugin);
//    when(registry.getSerdes(anyString())).thenReturn(null);
//    try {
//      new CachingQueryExecutor(tsdb);
//      fail("Expected IllegalArgumentException");
//    } catch (IllegalArgumentException e) { }
//    
//    when(registry.getSerdes(anyString()))
//      .thenAnswer(new Answer<TimeSeriesSerdes>() {
//      @Override
//      public TimeSeriesSerdes answer(
//          final InvocationOnMock invocation) throws Throwable {
//        return serdes;
//      }
//    });
//    when(registry.getPlugin(eq(TimeSeriesCacheKeyGenerator.class), anyString()))
//      .thenReturn(null);
//    try {
//      new CachingQueryExecutor(tsdb);
//      fail("Expected IllegalArgumentException");
//    } catch (IllegalArgumentException e) { }
//    
//    executor = new CachingQueryExecutor(tsdb);
//    assertSame(plugin, executor.plugin());
//    assertSame(serdes, executor.serdes());
//    assertTrue(executor.keyGenerator() instanceof 
//        DefaultTimeSeriesCacheKeyGenerator);
//  }
//
//  @Test
//  public void executeCacheMiss() throws Exception {
//    final CachingQueryExecutor executor = new CachingQueryExecutor(tsdb);
//    LocalExecution execution = (LocalExecution) executor.newNode(pcontext, null, config);
//    execution.initialize(null);
//    execution.fetchNext(null);
//    
//    verify(plugin, times(1))
//      .fetch(any(QueryContext.class), any(byte[].class), any(Span.class));
//    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
//        anyLong(), any(TimeUnit.class), any(Span.class));
//    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
//    verify(upstream, never()).onNext(any(QueryResult.class));
//    verify(upstream, never()).onError(any(Throwable.class));
//    verify(source, never()).fetchNext(any(Span.class));
//    verify(serdes, never()).deserialize(any(SerdesOptions.class), 
//        any(InputStream.class), any(QueryNode.class), any(Span.class));
//    verify(serdes, never()).serialize(any(QueryContext.class), 
//        any(SerdesOptions.class), any(OutputStream.class), 
//        any(QueryResult.class), any(Span.class));
//
//    // cache miss
//    cache_execution.callback(null);
//
//    verify(plugin, times(1))
//      .fetch(any(QueryContext.class), any(byte[].class), any(Span.class));
//    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
//        anyLong(), any(TimeUnit.class), any(Span.class));
//    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
//    verify(upstream, never()).onNext(any(QueryResult.class));
//    verify(upstream, never()).onError(any(Throwable.class));
//    verify(source, times(1)).fetchNext(any(Span.class));
//    verify(serdes, never()).deserialize(any(SerdesOptions.class), 
//        any(InputStream.class), any(QueryNode.class), any(Span.class));
//    verify(serdes, never()).serialize(any(QueryContext.class), 
//        any(SerdesOptions.class), any(OutputStream.class), 
//        any(QueryResult.class), any(Span.class));
//  }
//  
//  @Test
//  public void executeCacheHit() throws Exception {
//    final CachingQueryExecutor executor = new CachingQueryExecutor(tsdb);
//    LocalExecution execution = (LocalExecution) executor.newNode(pcontext, null, config);
//    execution.initialize(null);
//    execution.fetchNext(null);
//    
//    verify(plugin, times(1))
//      .fetch(any(QueryContext.class), any(byte[].class), any(Span.class));
//    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
//        anyLong(), any(TimeUnit.class), any(Span.class));
//    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
//    verify(upstream, never()).onNext(any(QueryResult.class));
//    verify(upstream, never()).onError(any(Throwable.class));
//    verify(source, never()).fetchNext(any(Span.class));
//    verify(serdes, never()).deserialize(any(SerdesOptions.class), 
//        any(InputStream.class), any(QueryNode.class), any(Span.class));
//    verify(serdes, never()).serialize(any(QueryContext.class), 
//        any(SerdesOptions.class), any(OutputStream.class), 
//        any(QueryResult.class), any(Span.class));
//    
//    // cache hit
//    cache_execution.callback(new byte[] { 42 });
//    
//    verify(plugin, times(1))
//      .fetch(any(QueryContext.class), any(byte[].class), any(Span.class));
//    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
//        anyLong(), any(TimeUnit.class), any(Span.class));
//    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
//    verify(upstream, never()).onNext(any(QueryResult.class));
//    verify(upstream, never()).onError(any(Throwable.class));
//    verify(source, never()).fetchNext(any(Span.class));
//    verify(serdes, times(1)).deserialize(any(SerdesOptions.class), 
//        any(InputStream.class), any(QueryNode.class), any(Span.class));
//    verify(serdes, never()).serialize(any(QueryContext.class), 
//        any(SerdesOptions.class), any(OutputStream.class), 
//        any(QueryResult.class), any(Span.class));
//  }
//  
//  @Test
//  public void executeSimultaneous() throws Exception {
//    config = (Config) Config.newBuilder()
//        .setExpiration(60000)
//        .setSimultaneous(true)
//        .setId("LocalCache")
//        .build();
//    final CachingQueryExecutor executor = new CachingQueryExecutor(tsdb);
//    LocalExecution execution = (LocalExecution) executor.newNode(pcontext, null, config);
//    execution.initialize(null);
//    execution.fetchNext(null);
//    
//    verify(plugin, times(1))
//      .fetch(any(QueryContext.class), any(byte[].class), any(Span.class));
//    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
//        anyLong(), any(TimeUnit.class), any(Span.class));
//    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
//    verify(upstream, never()).onNext(any(QueryResult.class));
//    verify(upstream, never()).onError(any(Throwable.class));
//    verify(source, times(1)).fetchNext(any(Span.class));
//    verify(serdes, never()).deserialize(any(SerdesOptions.class), 
//        any(InputStream.class), any(QueryNode.class), any(Span.class));
//    verify(serdes, never()).serialize(any(QueryContext.class), 
//        any(SerdesOptions.class), any(OutputStream.class), 
//        any(QueryResult.class), any(Span.class));
//  }
//  
//  @Test
//  public void executeBypass() throws Exception {
//    config = (Config) Config.newBuilder()
//        .setExpiration(60000)
//        .setBypass(true)
//        .setId("LocalCache")
//        .build();
//    final CachingQueryExecutor executor = new CachingQueryExecutor(tsdb);
//    LocalExecution execution = (LocalExecution) executor.newNode(pcontext, null, config);
//    execution.initialize(null);
//    execution.fetchNext(null);
//    
//    verify(plugin, never())
//      .fetch(any(QueryContext.class), any(byte[].class), any(Span.class));
//    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
//        anyLong(), any(TimeUnit.class), any(Span.class));
//    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
//    verify(upstream, never()).onNext(any(QueryResult.class));
//    verify(upstream, never()).onError(any(Throwable.class));
//    verify(source, times(1)).fetchNext(any(Span.class));
//    verify(serdes, never()).deserialize(any(SerdesOptions.class), 
//        any(InputStream.class), any(QueryNode.class), any(Span.class));
//    verify(serdes, never()).serialize(any(QueryContext.class), 
//        any(SerdesOptions.class), any(OutputStream.class), 
//        any(QueryResult.class), any(Span.class));
//  }
//  
//  @Test
//  public void executeCacheException() throws Exception {
//    final CachingQueryExecutor executor = new CachingQueryExecutor(tsdb);
//    LocalExecution execution = (LocalExecution) executor.newNode(pcontext, null, config);
//    execution.initialize(null);
//    execution.fetchNext(null);
//    
//    verify(plugin, times(1))
//      .fetch(any(QueryContext.class), any(byte[].class), any(Span.class));
//    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
//        anyLong(), any(TimeUnit.class), any(Span.class));
//    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
//    verify(upstream, never()).onNext(any(QueryResult.class));
//    verify(upstream, never()).onError(any(Throwable.class));
//    verify(source, never()).fetchNext(any(Span.class));
//    verify(serdes, never()).deserialize(any(SerdesOptions.class), 
//        any(InputStream.class), any(QueryNode.class), any(Span.class));
//    verify(serdes, never()).serialize(any(QueryContext.class), 
//        any(SerdesOptions.class), any(OutputStream.class), 
//        any(QueryResult.class), any(Span.class));
//    
//    // cache exception
//    cache_execution.callback(new IllegalStateException("Boo!"));
//    
//    verify(plugin, times(1))
//      .fetch(any(QueryContext.class), any(byte[].class), any(Span.class));
//    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
//        anyLong(), any(TimeUnit.class), any(Span.class));
//    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
//    verify(upstream, never()).onNext(any(QueryResult.class));
//    verify(upstream, never()).onError(any(Throwable.class));
//    verify(source, times(1)).fetchNext(any(Span.class));
//    verify(serdes, never()).deserialize(any(SerdesOptions.class), 
//        any(InputStream.class), any(QueryNode.class), any(Span.class));
//    verify(serdes, never()).serialize(any(QueryContext.class), 
//        any(SerdesOptions.class), any(OutputStream.class), 
//        any(QueryResult.class), any(Span.class));
//  }
//  
//  @Test
//  public void executeCacheExceptionSimultaneous() throws Exception {
//    config = (Config) Config.newBuilder()
//        .setExpiration(60000)
//        .setSimultaneous(true)
//        .setId("LocalCache")
//        .build();
//    final CachingQueryExecutor executor = new CachingQueryExecutor(tsdb);
//    LocalExecution execution = (LocalExecution) executor.newNode(pcontext, null, config);
//    execution.initialize(null);
//    execution.fetchNext(null);
//    
//    verify(plugin, times(1))
//      .fetch(any(QueryContext.class), any(byte[].class), any(Span.class));
//    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
//        anyLong(), any(TimeUnit.class), any(Span.class));
//    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
//    verify(upstream, never()).onNext(any(QueryResult.class));
//    verify(upstream, never()).onError(any(Throwable.class));
//    verify(source, times(1)).fetchNext(any(Span.class));
//    verify(serdes, never()).deserialize(any(SerdesOptions.class), 
//        any(InputStream.class), any(QueryNode.class), any(Span.class));
//    verify(serdes, never()).serialize(any(QueryContext.class), 
//        any(SerdesOptions.class), any(OutputStream.class), 
//        any(QueryResult.class), any(Span.class));
//    
//    // cache exception
//    cache_execution.callback(new IllegalStateException("Boo!"));
//    
//    verify(plugin, times(1))
//      .fetch(any(QueryContext.class), any(byte[].class), any(Span.class));
//    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
//        anyLong(), any(TimeUnit.class), any(Span.class));
//    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
//    verify(upstream, never()).onNext(any(QueryResult.class));
//    verify(upstream, never()).onError(any(Throwable.class));
//    verify(source, times(1)).fetchNext(any(Span.class));
//    verify(serdes, never()).deserialize(any(SerdesOptions.class), 
//        any(InputStream.class), any(QueryNode.class), any(Span.class));
//    verify(serdes, never()).serialize(any(QueryContext.class), 
//        any(SerdesOptions.class), any(OutputStream.class), 
//        any(QueryResult.class), any(Span.class));
//  }
//  
//  @Test
//  public void executeExceptionThrown() throws Exception {
//    when(plugin.fetch(any(QueryContext.class), any(byte[].class), 
//        any(Span.class))).thenThrow(new UnitTestException());
//    final CachingQueryExecutor executor = new CachingQueryExecutor(tsdb);
//    LocalExecution execution = (LocalExecution) executor.newNode(pcontext, null, config);
//    execution.initialize(null);
//    execution.fetchNext(null);
//    
//    verify(plugin, times(1))
//      .fetch(any(QueryContext.class), any(byte[].class), any(Span.class));
//    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
//        anyLong(), any(TimeUnit.class), any(Span.class));
//    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
//    verify(upstream, never()).onNext(any(QueryResult.class));
//    verify(upstream, times(1)).onError(any(Throwable.class));
//    verify(source, never()).fetchNext(any(Span.class));
//    verify(serdes, never()).deserialize(any(SerdesOptions.class), 
//        any(InputStream.class), any(QueryNode.class), any(Span.class));
//    verify(serdes, never()).serialize(any(QueryContext.class), 
//        any(SerdesOptions.class), any(OutputStream.class), 
//        any(QueryResult.class), any(Span.class));
//  }
//  
//  @Test
//  public void onNextFromDownstream() throws Exception {
//    final QueryResult next = mock(QueryResult.class);
//    when(next.source()).thenReturn(downstream);
//    final CachingQueryExecutor executor = new CachingQueryExecutor(tsdb);
//    LocalExecution execution = (LocalExecution) executor.newNode(pcontext, null, config);
//    execution.initialize(null);
//    
//    execution.onNext(next);
//    
//    verify(plugin, never())
//      .fetch(any(QueryContext.class), any(byte[].class), any(Span.class));
//    verify(plugin, times(1)).cache(any(byte[].class), any(byte[].class), 
//        anyLong(), any(TimeUnit.class), any(Span.class));
//    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
//    verify(upstream, times(1)).onNext(any(QueryResult.class));
//    verify(upstream, never()).onError(any(Throwable.class));
//    verify(source, never()).fetchNext(any(Span.class));
//    verify(serdes, never()).deserialize(any(SerdesOptions.class), 
//        any(InputStream.class), any(QueryNode.class), any(Span.class));
//    verify(serdes, times(1)).serialize(any(QueryContext.class), 
//        any(SerdesOptions.class), any(OutputStream.class), 
//        any(QueryResult.class), any(Span.class));
//    assertTrue(execution.complete.get());
//  }
//
//  @Test
//  public void onNextSimultaneousCacheFirst() throws Exception {
//    config = (Config) Config.newBuilder()
//        .setExpiration(60000)
//        .setSimultaneous(true)
//        .setId("LocalCache")
//        .build();
//    QueryResult next = mock(QueryResult.class);
//    
//    final CachingQueryExecutor executor = new CachingQueryExecutor(tsdb);
//    LocalExecution execution = (LocalExecution) executor.newNode(pcontext, null, config);
//    when(next.source()).thenReturn(execution);
//    execution.initialize(null);
//    execution.fetchNext(null);
//    execution.cache_execution = null;
//    execution.onNext(next);
//    
//    verify(plugin, times(1))
//      .fetch(any(QueryContext.class), any(byte[].class), any(Span.class));
//    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
//        anyLong(), any(TimeUnit.class), any(Span.class));
//    verify(upstream, times(1)).onComplete(any(QueryNode.class), anyLong(), anyLong());
//    verify(upstream, times(1)).onNext(any(QueryResult.class));
//    verify(upstream, never()).onError(any(Throwable.class));
//    verify(source, times(1)).fetchNext(any(Span.class));
//    verify(serdes, never()).deserialize(any(SerdesOptions.class), 
//        any(InputStream.class), any(QueryNode.class), any(Span.class));
//    verify(serdes, never()).serialize(any(QueryContext.class), 
//        any(SerdesOptions.class), any(OutputStream.class), 
//        any(QueryResult.class), any(Span.class));
//    assertTrue(execution.complete.get());
//    assertFalse(cache_execution.cancelled);
//    
//    // downstream
//    next = mock(QueryResult.class);
//    when(next.source()).thenReturn(downstream);
//    execution.onNext(next);
//    
//    verify(plugin, times(1))
//      .fetch(any(QueryContext.class), any(byte[].class), any(Span.class));
//    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
//        anyLong(), any(TimeUnit.class), any(Span.class));
//    verify(upstream, times(1)).onComplete(any(QueryNode.class), anyLong(), anyLong());
//    verify(upstream, times(1)).onNext(any(QueryResult.class));
//    verify(upstream, never()).onError(any(Throwable.class));
//    verify(source, times(1)).fetchNext(any(Span.class));
//    verify(serdes, never()).deserialize(any(SerdesOptions.class), 
//        any(InputStream.class), any(QueryNode.class), any(Span.class));
//    verify(serdes, never()).serialize(any(QueryContext.class), 
//        any(SerdesOptions.class), any(OutputStream.class), 
//        any(QueryResult.class), any(Span.class));
//    assertTrue(execution.complete.get());
//    assertFalse(cache_execution.cancelled);
//  }
//  
//  @Test
//  public void onNextSimultaneousDownstreamFirst() throws Exception {
//    config = (Config) Config.newBuilder()
//        .setExpiration(60000)
//        .setSimultaneous(true)
//        .setId("LocalCache")
//        .build();
//    QueryResult next = mock(QueryResult.class);
//    when(next.source()).thenReturn(downstream);
//    final CachingQueryExecutor executor = new CachingQueryExecutor(tsdb);
//    LocalExecution execution = (LocalExecution) executor.newNode(pcontext, null, config);
//    execution.initialize(null);
//    execution.fetchNext(null);
//    
//    execution.onNext(next);
//    
//    verify(plugin, times(1))
//      .fetch(any(QueryContext.class), any(byte[].class), any(Span.class));
//    verify(plugin, times(1)).cache(any(byte[].class), any(byte[].class), 
//        anyLong(), any(TimeUnit.class), any(Span.class));
//    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
//    verify(upstream, times(1)).onNext(any(QueryResult.class));
//    verify(upstream, never()).onError(any(Throwable.class));
//    verify(source, times(1)).fetchNext(any(Span.class));
//    verify(serdes, never()).deserialize(any(SerdesOptions.class), 
//        any(InputStream.class), any(QueryNode.class), any(Span.class));
//    verify(serdes, times(1)).serialize(any(QueryContext.class), 
//        any(SerdesOptions.class), any(OutputStream.class), 
//        any(QueryResult.class), any(Span.class));
//    assertTrue(execution.complete.get());
//    assertTrue(cache_execution.cancelled);
//    
//    // cache hit
//    next = mock(QueryResult.class);
//    when(next.source()).thenReturn(execution);
//    execution.onNext(next);
//    
//    verify(plugin, times(1))
//      .fetch(any(QueryContext.class), any(byte[].class), any(Span.class));
//    verify(plugin, times(1)).cache(any(byte[].class), any(byte[].class), 
//        anyLong(), any(TimeUnit.class), any(Span.class));
//    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
//    verify(upstream, times(1)).onNext(any(QueryResult.class));
//    verify(upstream, never()).onError(any(Throwable.class));
//    verify(source, times(1)).fetchNext(any(Span.class));
//    verify(serdes, never()).deserialize(any(SerdesOptions.class), 
//        any(InputStream.class), any(QueryNode.class), any(Span.class));
//    verify(serdes, times(1)).serialize(any(QueryContext.class), 
//        any(SerdesOptions.class), any(OutputStream.class), 
//        any(QueryResult.class), any(Span.class));
//    assertTrue(execution.complete.get());
//    assertTrue(cache_execution.cancelled);
//  }
//  
//  @Test
//  public void onNextNullSource() throws Exception {
//    // Would happen if a plugin fails to set the source.
//    QueryResult next = mock(QueryResult.class);
//    
//    final CachingQueryExecutor executor = new CachingQueryExecutor(tsdb);
//    LocalExecution execution = (LocalExecution) executor.newNode(pcontext, null, config);
//    execution.initialize(null);
//    execution.fetchNext(null);
//    execution.cache_execution = null;
//    execution.onNext(next);
//    
//    verify(plugin, times(1))
//      .fetch(any(QueryContext.class), any(byte[].class), any(Span.class));
//    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
//        anyLong(), any(TimeUnit.class), any(Span.class));
//    verify(upstream, times(1)).onComplete(any(QueryNode.class), anyLong(), anyLong());
//    verify(upstream, times(1)).onNext(any(QueryResult.class));
//    verify(upstream, never()).onError(any(Throwable.class));
//    verify(source, never()).fetchNext(any(Span.class));
//    verify(serdes, never()).deserialize(any(SerdesOptions.class), 
//        any(InputStream.class), any(QueryNode.class), any(Span.class));
//    verify(serdes, never()).serialize(any(QueryContext.class), 
//        any(SerdesOptions.class), any(OutputStream.class), 
//        any(QueryResult.class), any(Span.class));
//    assertTrue(execution.complete.get());
//    assertFalse(cache_execution.cancelled);
//  }
//  
//  @Test
//  public void onNextConvertIDs() throws Exception {
//    QueryResult next = mock(QueryResult.class);
//    when(next.source()).thenReturn(downstream);
//    when(next.idType()).thenAnswer(new Answer<TypeToken<?>>() {
//      @Override
//      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
//        return Const.TS_BYTE_ID;
//      }
//    });
//    PowerMockito.mockStatic(ConvertedQueryResult.class);
//    
//    final CachingQueryExecutor executor = new CachingQueryExecutor(tsdb);
//    LocalExecution execution = (LocalExecution) executor.newNode(pcontext, null, config);
//    execution.initialize(null);
//    execution.fetchNext(null);
//    execution.cache_execution = null;
//    execution.onNext(next);
//    
//    verify(plugin, times(1))
//      .fetch(any(QueryContext.class), any(byte[].class), any(Span.class));
//    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
//        anyLong(), any(TimeUnit.class), any(Span.class));
//    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
//    verify(upstream, never()).onNext(any(QueryResult.class));
//    verify(upstream, never()).onError(any(Throwable.class));
//    verify(source, never()).fetchNext(any(Span.class));
//    verify(serdes, never()).deserialize(any(SerdesOptions.class), 
//        any(InputStream.class), any(QueryNode.class), any(Span.class));
//    verify(serdes, never()).serialize(any(QueryContext.class), 
//        any(SerdesOptions.class), any(OutputStream.class), 
//        any(QueryResult.class), any(Span.class));
//    assertFalse(execution.complete.get());
//    assertFalse(cache_execution.cancelled);
//    PowerMockito.verifyStatic(times(1));
//    ConvertedQueryResult.convert(next, execution, null);
//  }
//  
//  @Test
//  public void onNextSerdesThrowsException() throws Exception {
//    final QueryResult next = mock(QueryResult.class);
//    when(next.source()).thenReturn(downstream);
//    when(serdes.serialize(any(QueryContext.class), 
//        any(SerdesOptions.class), any(OutputStream.class), 
//        any(QueryResult.class), any(Span.class)))
//      .thenThrow(new UnitTestException());
//    final CachingQueryExecutor executor = new CachingQueryExecutor(tsdb);
//    LocalExecution execution = (LocalExecution) executor.newNode(pcontext, null, config);
//    execution.initialize(null);
//    
//    execution.onNext(next);
//    
//    verify(plugin, never())
//      .fetch(any(QueryContext.class), any(byte[].class), any(Span.class));
//    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
//        anyLong(), any(TimeUnit.class), any(Span.class));
//    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
//    verify(upstream, times(1)).onNext(any(QueryResult.class));
//    verify(upstream, never()).onError(any(Throwable.class));
//    verify(source, never()).fetchNext(any(Span.class));
//    verify(serdes, never()).deserialize(any(SerdesOptions.class), 
//        any(InputStream.class), any(QueryNode.class), any(Span.class));
//    verify(serdes, times(1)).serialize(any(QueryContext.class), 
//        any(SerdesOptions.class), any(OutputStream.class), 
//        any(QueryResult.class), any(Span.class));
//    assertTrue(execution.complete.get());
//  }
//  
//  @Test
//  public void onNextSerdesReturnsException() throws Exception {
//    final QueryResult next = mock(QueryResult.class);
//    when(next.source()).thenReturn(downstream);
//    when(serdes.serialize(any(QueryContext.class), 
//        any(SerdesOptions.class), any(OutputStream.class), 
//        any(QueryResult.class), any(Span.class)))
//      .thenReturn(Deferred.fromError(new UnitTestException()));
//    final CachingQueryExecutor executor = new CachingQueryExecutor(tsdb);
//    LocalExecution execution = (LocalExecution) executor.newNode(pcontext, null, config);
//    execution.initialize(null);
//    
//    execution.onNext(next);
//    
//    verify(plugin, never())
//      .fetch(any(QueryContext.class), any(byte[].class), any(Span.class));
//    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
//        anyLong(), any(TimeUnit.class), any(Span.class));
//    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
//    verify(upstream, times(1)).onNext(any(QueryResult.class));
//    verify(upstream, never()).onError(any(Throwable.class));
//    verify(source, never()).fetchNext(any(Span.class));
//    verify(serdes, never()).deserialize(any(SerdesOptions.class), 
//        any(InputStream.class), any(QueryNode.class), any(Span.class));
//    verify(serdes, times(1)).serialize(any(QueryContext.class), 
//        any(SerdesOptions.class), any(OutputStream.class), 
//        any(QueryResult.class), any(Span.class));
//    assertTrue(execution.complete.get());
//  }
//  
//  @Test
//  public void onNextZeroExpiration() throws Exception {
//    config = (Config) Config.newBuilder()
//        .setExpiration(0)
//        .setId("LocalCache")
//        .build();
//    QueryResult next = mock(QueryResult.class);
//    when(next.source()).thenReturn(downstream);
//    final CachingQueryExecutor executor = new CachingQueryExecutor(tsdb);
//    LocalExecution execution = (LocalExecution) executor.newNode(pcontext, null, config);
//    execution.initialize(null);
//    execution.fetchNext(null);
//    
//    execution.onNext(next);
//    
//    verify(plugin, times(1))
//      .fetch(any(QueryContext.class), any(byte[].class), any(Span.class));
//    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
//        anyLong(), any(TimeUnit.class), any(Span.class));
//    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
//    verify(upstream, times(1)).onNext(any(QueryResult.class));
//    verify(upstream, never()).onError(any(Throwable.class));
//    verify(source, never()).fetchNext(any(Span.class));
//    verify(serdes, never()).deserialize(any(SerdesOptions.class), 
//        any(InputStream.class), any(QueryNode.class), any(Span.class));
//    verify(serdes, never()).serialize(any(QueryContext.class), 
//        any(SerdesOptions.class), any(OutputStream.class), 
//        any(QueryResult.class), any(Span.class));
//    assertTrue(execution.complete.get());
//  }
//  
//  @Test
//  public void onErrorDownstreamExceptionNotComplete() throws Exception {
//    final CachingQueryExecutor executor = new CachingQueryExecutor(tsdb);
//    LocalExecution execution = (LocalExecution) executor.newNode(pcontext, null, config);
//    execution.initialize(null);
//    
//    execution.onError(new UnitTestException());
//    
//    verify(plugin, never())
//      .fetch(any(QueryContext.class), any(byte[].class), any(Span.class));
//    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
//        anyLong(), any(TimeUnit.class), any(Span.class));
//    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
//    verify(upstream, never()).onNext(any(QueryResult.class));
//    verify(upstream, times(1)).onError(any(Throwable.class));
//    verify(source, never()).fetchNext(any(Span.class));
//    verify(serdes, never()).deserialize(any(SerdesOptions.class), 
//        any(InputStream.class), any(QueryNode.class), any(Span.class));
//    verify(serdes, never()).serialize(any(QueryContext.class), 
//        any(SerdesOptions.class), any(OutputStream.class), 
//        any(QueryResult.class), any(Span.class));
//    assertTrue(execution.complete.get());
//  }
//  
//  @Test
//  public void onErrorDownstreamExceptionComplete() throws Exception {
//    final CachingQueryExecutor executor = new CachingQueryExecutor(tsdb);
//    LocalExecution execution = (LocalExecution) executor.newNode(pcontext, null, config);
//    execution.initialize(null);
//    execution.complete.set(true);
//    
//    execution.onError(new UnitTestException());
//    
//    verify(plugin, never())
//      .fetch(any(QueryContext.class), any(byte[].class), any(Span.class));
//    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
//        anyLong(), any(TimeUnit.class), any(Span.class));
//    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
//    verify(upstream, never()).onNext(any(QueryResult.class));
//    verify(upstream, never()).onError(any(Throwable.class));
//    verify(source, never()).fetchNext(any(Span.class));
//    verify(serdes, never()).deserialize(any(SerdesOptions.class), 
//        any(InputStream.class), any(QueryNode.class), any(Span.class));
//    verify(serdes, never()).serialize(any(QueryContext.class), 
//        any(SerdesOptions.class), any(OutputStream.class), 
//        any(QueryResult.class), any(Span.class));
//    assertTrue(execution.complete.get());
//  }
//  
//  @Test
//  public void onComplete() throws Exception {
//    final CachingQueryExecutor executor = new CachingQueryExecutor(tsdb);
//    LocalExecution execution = (LocalExecution) executor.newNode(pcontext, null, config);
//    execution.initialize(null);
//    
//    execution.onComplete(mock(QueryNode.class), 42, 42);
//    verify(upstream, times(1)).onComplete(any(QueryNode.class), anyLong(), anyLong());
//  }
//  
//  @Test
//  public void closeNotComplete() throws Exception {
//    final CachingQueryExecutor executor = new CachingQueryExecutor(tsdb);
//    LocalExecution execution = (LocalExecution) executor.newNode(pcontext, null, config);
//    execution.initialize(null);
//    execution.fetchNext(null);
//    
//    verify(plugin, times(1))
//      .fetch(any(QueryContext.class), any(byte[].class), any(Span.class));
//    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
//        anyLong(), any(TimeUnit.class), any(Span.class));
//    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
//    verify(upstream, never()).onNext(any(QueryResult.class));
//    verify(upstream, never()).onError(any(Throwable.class));
//    verify(source, never()).fetchNext(any(Span.class));
//    verify(serdes, never()).deserialize(any(SerdesOptions.class), 
//        any(InputStream.class), any(QueryNode.class), any(Span.class));
//    verify(serdes, never()).serialize(any(QueryContext.class), 
//        any(SerdesOptions.class), any(OutputStream.class), 
//        any(QueryResult.class), any(Span.class));
//    
//    execution.close();
//    
//    verify(plugin, times(1))
//      .fetch(any(QueryContext.class), any(byte[].class), any(Span.class));
//    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
//        anyLong(), any(TimeUnit.class), any(Span.class));
//    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
//    verify(upstream, never()).onNext(any(QueryResult.class));
//    verify(upstream, times(1)).onError(any(Throwable.class));
//    verify(source, never()).fetchNext(any(Span.class));
//    verify(serdes, never()).deserialize(any(SerdesOptions.class), 
//        any(InputStream.class), any(QueryNode.class), any(Span.class));
//    verify(serdes, never()).serialize(any(QueryContext.class), 
//        any(SerdesOptions.class), any(OutputStream.class), 
//        any(QueryResult.class), any(Span.class));
//    assertTrue(cache_execution.cancelled);
//  }
//  
//  @Test
//  public void closeComplete() throws Exception {
//    final CachingQueryExecutor executor = new CachingQueryExecutor(tsdb);
//    LocalExecution execution = (LocalExecution) executor.newNode(pcontext, null, config);
//    execution.initialize(null);
//    execution.fetchNext(null);
//    
//    verify(plugin, times(1))
//      .fetch(any(QueryContext.class), any(byte[].class), any(Span.class));
//    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
//        anyLong(), any(TimeUnit.class), any(Span.class));
//    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
//    verify(upstream, never()).onNext(any(QueryResult.class));
//    verify(upstream, never()).onError(any(Throwable.class));
//    verify(source, never()).fetchNext(any(Span.class));
//    verify(serdes, never()).deserialize(any(SerdesOptions.class), 
//        any(InputStream.class), any(QueryNode.class), any(Span.class));
//    verify(serdes, never()).serialize(any(QueryContext.class), 
//        any(SerdesOptions.class), any(OutputStream.class), 
//        any(QueryResult.class), any(Span.class));
//    
//    execution.complete.set(true);
//    execution.close();
//    
//    verify(plugin, times(1))
//      .fetch(any(QueryContext.class), any(byte[].class), any(Span.class));
//    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
//        anyLong(), any(TimeUnit.class), any(Span.class));
//    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
//    verify(upstream, never()).onNext(any(QueryResult.class));
//    verify(upstream, never()).onError(any(Throwable.class));
//    verify(source, never()).fetchNext(any(Span.class));
//    verify(serdes, never()).deserialize(any(SerdesOptions.class), 
//        any(InputStream.class), any(QueryNode.class), any(Span.class));
//    verify(serdes, never()).serialize(any(QueryContext.class), 
//        any(SerdesOptions.class), any(OutputStream.class), 
//        any(QueryResult.class), any(Span.class));
//    assertTrue(cache_execution.cancelled);
//  }
//
//  @Test
//  public void builder() throws Exception {
//    String json = JSON.serializeToString(config);
//    assertTrue(json.contains("\"simultaneous\":false"));
//    assertTrue(json.contains("\"expiration\":60000"));
//    assertTrue(json.contains("\"id\":\"LocalCache\""));
//    assertTrue(json.contains("\"useTimestamps\":false"));
//    assertTrue(json.contains("\"keyGeneratorId\":\"MyKeyGen\""));
//    
//    json = "{\"simultaneous\":false,"
//        + "\"expiration\":60000,\"bypass\":true,\"keyGeneratorId\":\"MyKeyGen\","
//        + "\"useTimestamps\":false,\"id\":\"LocalCache\"}";
//    config = JSON.parseToObject(json, Config.class);
//    assertEquals("LocalCache", config.getId());
//    assertFalse(config.getSimultaneous());
//    assertEquals(60000, config.getExpiration());
//    assertTrue(config.getBypass());
//    assertFalse(config.getUseTimestamps());
//    assertEquals("MyKeyGen", config.getKeyGeneratorId());
//  }
//  
  @Test
  public void hashCodeEqualsCompareTo() throws Exception {
    final Config c1 = (Config) Config.newBuilder()
        .setExpiration(60000)
        .setBypass(true)
        .setSimultaneous(true)
        .setUseTimestamps(true)
        .setKeyGeneratorId("MyKeyGen")
        .setId("LocalCache")
        .build();
    
    Config c2 = (Config) Config.newBuilder()
        .setExpiration(60000)
        .setBypass(true)
        .setSimultaneous(true)
        .setUseTimestamps(true)
        .setKeyGeneratorId("MyKeyGen")
        .setId("LocalCache")
        .build();
    assertEquals(c1.hashCode(), c2.hashCode());
    assertEquals(c1, c2);
    assertEquals(0, c1.compareTo(c2));
    
    c2 = (Config) Config.newBuilder()
        .setExpiration(30000)  // <-- Diff
        .setBypass(true)
        .setSimultaneous(true)
        .setUseTimestamps(true)
        .setKeyGeneratorId("MyKeyGen")
        .setId("LocalCache")
        .build();
    assertNotEquals(c1.hashCode(), c2.hashCode());
    assertNotEquals(c1, c2);
    assertEquals(1, c1.compareTo(c2));
    
    c2 = (Config) Config.newBuilder()
        .setExpiration(60000)
        //.setBypass(true)  // <-- Diff
        .setSimultaneous(true)
        .setUseTimestamps(true)
        .setKeyGeneratorId("MyKeyGen")
        .setId("LocalCache")
        .build();
    assertNotEquals(c1.hashCode(), c2.hashCode());
    assertNotEquals(c1, c2);
    assertEquals(-1, c1.compareTo(c2));
    
    c2 = (Config) Config.newBuilder()
        .setExpiration(60000)
        .setBypass(true)
        //.setSimultaneous(true)  // <-- Diff
        .setUseTimestamps(true)
        .setKeyGeneratorId("MyKeyGen")
        .setId("LocalCache")
        .build();
    assertNotEquals(c1.hashCode(), c2.hashCode());
    assertNotEquals(c1, c2);
    assertEquals(-1, c1.compareTo(c2));
    
    c2 = (Config) Config.newBuilder()
        .setExpiration(60000)
        .setBypass(true)
        .setSimultaneous(true)
        //.setUseTimestamps(true)  // <-- Diff
        .setKeyGeneratorId("MyKeyGen")
        .setId("LocalCache")
        .build();
    assertNotEquals(c1.hashCode(), c2.hashCode());
    assertNotEquals(c1, c2);
    assertEquals(-1, c1.compareTo(c2));
    
    c2 = (Config) Config.newBuilder()
        .setExpiration(60000)
        .setBypass(true)
        .setSimultaneous(true)
        .setUseTimestamps(true)
        .setKeyGeneratorId("MyKeyGen2")  // <-- Diff
        .setId("LocalCache")
        .build();
    assertNotEquals(c1.hashCode(), c2.hashCode());
    assertNotEquals(c1, c2);
    assertEquals(-1, c1.compareTo(c2));
    
    c2 = (Config) Config.newBuilder()
        .setExpiration(60000)
        .setBypass(true)
        .setSimultaneous(true)
        .setUseTimestamps(true)
        //.setKeyGeneratorId("MyKeyGen")  // <-- Diff
        .setId("LocalCache")
        .build();
    assertNotEquals(c1.hashCode(), c2.hashCode());
    assertNotEquals(c1, c2);
    assertEquals(1, c1.compareTo(c2));
    
    c2 = (Config) Config.newBuilder()
        .setExpiration(60000)
        .setBypass(true)
        .setSimultaneous(true)
        .setUseTimestamps(true)
        .setKeyGeneratorId("MyKeyGen")
        .setId("TestCache")  // <-- Diff
        .build();
    assertNotEquals(c1.hashCode(), c2.hashCode());
    assertNotEquals(c1, c2);
    assertEquals(-1, c1.compareTo(c2));
  }
}
