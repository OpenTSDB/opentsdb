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

import java.io.ByteArrayOutputStream;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.TimeoutException;
import io.opentracing.Span;
import net.opentsdb.data.iterators.DefaultIteratorGroups;
import net.opentsdb.data.iterators.IteratorGroups;
import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.query.context.QueryContext;
import net.opentsdb.query.execution.CachingQueryExecutor.Config;
import net.opentsdb.query.execution.TestQueryExecutor.MockDownstream;
import net.opentsdb.query.execution.cache.QueryCachePlugin;
import net.opentsdb.query.execution.cache.TimeSeriesCacheKeyGenerator;
import net.opentsdb.query.execution.cache.DefaultTimeSeriesCacheKeyGenerator;
import net.opentsdb.query.execution.graph.ExecutionGraphNode;
import net.opentsdb.query.execution.serdes.TimeSeriesSerdes;
import net.opentsdb.query.execution.serdes.UglyByteIteratorGroupsSerdes;
import net.opentsdb.query.pojo.Metric;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.query.pojo.Timespan;
import net.opentsdb.utils.JSON;

public class TestCachingQueryExecutor extends BaseExecutorTest {
  private QueryExecutor<IteratorGroups> executor;
  private MockDownstream<IteratorGroups> cache_execution;
  private Config config;
  private QueryCachePlugin plugin;
  private TimeSeriesSerdes serdes;
  private TimeSeriesCacheKeyGenerator key_generator;
  private MockDownstream<IteratorGroups> downstream;
  
  @SuppressWarnings("unchecked")
  @Before
  public void beforeLocal() throws Exception {
    node = mock(ExecutionGraphNode.class);
    executor = mock(QueryExecutor.class);
    plugin = mock(QueryCachePlugin.class);
    serdes = new UglyByteIteratorGroupsSerdes();
    config = (Config) Config.newBuilder()
        .setExpiration(60000)
        .setKeyGeneratorId("MyKeyGen")
        .setExecutorId("LocalCache")
        .setExecutorType("CachingQueryExecutor")
        .build();
    
    when(tsdb.getConfig()).thenReturn(new net.opentsdb.utils.Config(false));
    key_generator = new DefaultTimeSeriesCacheKeyGenerator();
    key_generator.initialize(tsdb).join();
    when(node.graph()).thenReturn(graph);
    when(node.getDefaultConfig()).thenReturn(config);
    when(graph.getDownstreamExecutor(anyString()))
      .thenAnswer(new Answer<QueryExecutor<?>>() {
      @Override
      public QueryExecutor<?> answer(InvocationOnMock invocation)
          throws Throwable {
        return executor;
      }
    });
    when(executor.close()).thenReturn(Deferred.fromResult(null));
    when(registry.getPlugin(eq(QueryCachePlugin.class), anyString()))
      .thenReturn(plugin);
    when(registry.getPlugin(eq(TimeSeriesCacheKeyGenerator.class), anyString()))
      .thenReturn(key_generator);
    when(registry.getSerdes(anyString()))
      .thenAnswer(new Answer<TimeSeriesSerdes>() {
      @Override
      public TimeSeriesSerdes answer(
          final InvocationOnMock invocation) throws Throwable {
        return serdes;
      }
    });
    query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("1h-ago"))
        .addMetric(Metric.newBuilder()
            .setMetric("system.cpu.user"))
        .build();
    downstream = new MockDownstream<IteratorGroups>(query);
    when(executor.executeQuery(context, query, null))
      .thenAnswer(new Answer<QueryExecution<IteratorGroups>>() {
        @Override
        public QueryExecution<IteratorGroups> answer(
            InvocationOnMock invocation) throws Throwable {
          return downstream;
        }
    });
    cache_execution = new MockDownstream<IteratorGroups>(query);
    when(plugin.fetch(any(QueryContext.class), any(byte[].class), any(Span.class)))
      .thenAnswer(new Answer<QueryExecution<IteratorGroups>>() {
        @Override
        public QueryExecution<IteratorGroups> answer(
            final InvocationOnMock invocation) throws Throwable {
          return cache_execution;
        }
      });
  }
  
  @Test
  public void ctor() throws Exception {
    CachingQueryExecutor<IteratorGroups> executor = 
        new CachingQueryExecutor<IteratorGroups>(node);
    assertSame(plugin, executor.plugin());
    assertSame(serdes, executor.serdes());
    assertTrue(executor.keyGenerator() instanceof 
        DefaultTimeSeriesCacheKeyGenerator);
    assertEquals(1, executor.downstreamExecutors().size());
    assertSame(this.executor, executor.downstreamExecutors().get(0));
    
    try {
      new CachingQueryExecutor<IteratorGroups>(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    when(node.getDefaultConfig()).thenReturn(null);
    try {
      new CachingQueryExecutor<IteratorGroups>(node);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    when(node.getDefaultConfig()).thenReturn(config);
    when(graph.getDownstreamExecutor(anyString())).thenReturn(null);
    try {
      new CachingQueryExecutor<IteratorGroups>(node);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    final QueryExecutor<?> ex = executor;
    when(graph.getDownstreamExecutor(anyString()))
      .thenAnswer(new Answer<QueryExecutor<?>>() {
      @Override
      public QueryExecutor<?> answer(InvocationOnMock invocation)
          throws Throwable {
        return ex;
      }
    });
    when(registry.getPlugin(eq(QueryCachePlugin.class), anyString()))
      .thenReturn(null);
    try {
      new CachingQueryExecutor<IteratorGroups>(node);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    when(registry.getPlugin(eq(QueryCachePlugin.class), anyString()))
      .thenReturn(plugin);
    when(registry.getSerdes(anyString())).thenReturn(null);
    try {
      new CachingQueryExecutor<IteratorGroups>(node);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    when(registry.getSerdes(anyString()))
      .thenAnswer(new Answer<TimeSeriesSerdes>() {
      @Override
      public TimeSeriesSerdes answer(
          final InvocationOnMock invocation) throws Throwable {
        return serdes;
      }
    });
    when(registry.getPlugin(eq(TimeSeriesCacheKeyGenerator.class), anyString()))
      .thenReturn(null);
    try {
      new CachingQueryExecutor<IteratorGroups>(node);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void executeCacheMiss() throws Exception {
    final CachingQueryExecutor<IteratorGroups> executor = 
        new CachingQueryExecutor<IteratorGroups>(node);
    final QueryExecution<IteratorGroups> exec = 
        executor.executeQuery(context, query, span);
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    verify(plugin, times(1))
      .fetch(any(QueryContext.class), any(byte[].class), any(Span.class));
    verify(this.executor, never()).executeQuery(context, query, null);
    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
        anyLong(), any(TimeUnit.class));
    assertTrue(executor.outstandingRequests().contains(exec));
    
    // cache miss
    cache_execution.callback(null);
    
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    
    final IteratorGroups results = new DefaultIteratorGroups();
    downstream.callback(results);
    assertSame(results, exec.deferred().join());
    verify(this.executor, times(1)).executeQuery(context, query, null);
    verify(plugin, times(1)).cache(any(byte[].class), any(byte[].class), 
        anyLong(), any(TimeUnit.class));
    assertFalse(executor.outstandingRequests().contains(exec));
    assertFalse(downstream.cancelled);
    assertFalse(cache_execution.cancelled);
  }
  
//  @Test
//  public void executeCacheHit() throws Exception {
//    final CachingQueryExecutor<IteratorGroups> executor = 
//        new CachingQueryExecutor<IteratorGroups>(node);
//    final QueryExecution<IteratorGroups> exec = 
//        executor.executeQuery(context, query, span);
//    try {
//      exec.deferred().join(1);
//      fail("Expected TimeoutException");
//    } catch (TimeoutException e) { }
//    verify(plugin, times(1))
//      .fetch(any(QueryContext.class), any(byte[].class), any(Span.class));
//    verify(this.executor, never()).executeQuery(context, query, null);
//    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
//        anyLong(), any(TimeUnit.class));
//    assertTrue(executor.outstandingRequests().contains(exec));
//    
//    // cache hit
//    IteratorGroups results = new DefaultIteratorGroups();
//    final ByteArrayOutputStream output = new ByteArrayOutputStream();
//    serdes.serialize(query, null, output, results);
//    output.close();
//    
//    cache_execution.callback(output.toByteArray());
//    
//    results = exec.deferred().join();
//    assertTrue(results.groups().isEmpty());
//    verify(this.executor, never()).executeQuery(context, query, null);
//    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
//        anyLong(), any(TimeUnit.class));
//    assertFalse(executor.outstandingRequests().contains(exec));
//    assertFalse(downstream.cancelled);
//    assertFalse(cache_execution.cancelled);
//  }

  @Test
  public void executeCacheMissNoCaching() throws Exception {
    config = (Config) Config.newBuilder()
        .setExpiration(0)
        .setExecutorId("LocalCache")
        .setExecutorType("CachingQueryExecutor")
        .build();
    when(node.getDefaultConfig()).thenReturn(config);
    
    final CachingQueryExecutor<IteratorGroups> executor = 
        new CachingQueryExecutor<IteratorGroups>(node);
    final QueryExecution<IteratorGroups> exec = 
        executor.executeQuery(context, query, span);
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    verify(plugin, times(1))
      .fetch(any(QueryContext.class), any(byte[].class), any(Span.class));
    verify(this.executor, never()).executeQuery(context, query, null);
    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
        anyLong(), any(TimeUnit.class));
    assertTrue(executor.outstandingRequests().contains(exec));
    
    // cache miss
    cache_execution.callback(null);
    
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    
    final IteratorGroups results = new DefaultIteratorGroups();
    downstream.callback(results);
    assertSame(results, exec.deferred().join());
    verify(this.executor, times(1)).executeQuery(context, query, null);
    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
        anyLong(), any(TimeUnit.class));
    assertFalse(executor.outstandingRequests().contains(exec));
    assertFalse(downstream.cancelled);
    assertFalse(cache_execution.cancelled);
  }
  
//  @Test
//  public void executeSimultaneousCacheFirst() throws Exception {
//    config = (Config) Config.newBuilder()
//        .setExpiration(60000)
//        .setSimultaneous(true)
//        .setExecutorId("LocalCache")
//        .setExecutorType("CachingQueryExecutor")
//        .build();
//    when(node.getDefaultConfig()).thenReturn(config);
//    
//    final CachingQueryExecutor<IteratorGroups> executor = 
//        new CachingQueryExecutor<IteratorGroups>(node);
//    final QueryExecution<IteratorGroups> exec = 
//        executor.executeQuery(context, query, span);
//    try {
//      exec.deferred().join(1);
//      fail("Expected TimeoutException");
//    } catch (TimeoutException e) { }
//    verify(plugin, times(1))
//      .fetch(any(QueryContext.class), any(byte[].class), any(Span.class));
//    verify(this.executor, times(1)).executeQuery(context, query, null);
//    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
//        anyLong(), any(TimeUnit.class));
//    assertTrue(executor.outstandingRequests().contains(exec));
//    
//    // cache hit
//    IteratorGroups results = new DefaultIteratorGroups();
//    final ByteArrayOutputStream output = new ByteArrayOutputStream();
//    serdes.serialize(query, null, output, results);
//    output.close();
//    
//    cache_execution.callback(output.toByteArray());
//    
//    results = exec.deferred().join();
//    assertTrue(results.groups().isEmpty());
//    verify(this.executor, times(1)).executeQuery(context, query, null);
//    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
//        anyLong(), any(TimeUnit.class));
//    assertFalse(executor.outstandingRequests().contains(exec));
//    assertTrue(downstream.cancelled);
//    assertFalse(cache_execution.cancelled);
//  }
  
  @Test
  public void executeSimultaneousDownstreamFirst() throws Exception {
    config = (Config) Config.newBuilder()
        .setExpiration(60000)
        .setSimultaneous(true)
        .setExecutorId("LocalCache")
        .setExecutorType("CachingQueryExecutor")
        .build();
    when(node.getDefaultConfig()).thenReturn(config);
    
    final CachingQueryExecutor<IteratorGroups> executor = 
        new CachingQueryExecutor<IteratorGroups>(node);
    final QueryExecution<IteratorGroups> exec = 
        executor.executeQuery(context, query, span);
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    verify(plugin, times(1))
      .fetch(any(QueryContext.class), any(byte[].class), any(Span.class));
    verify(this.executor, times(1)).executeQuery(context, query, null);
    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
        anyLong(), any(TimeUnit.class));
    assertTrue(executor.outstandingRequests().contains(exec));
    
    // downstream
    IteratorGroups results = new DefaultIteratorGroups();
    downstream.callback(results);
    
    assertSame(results, exec.deferred().join());
    verify(this.executor, times(1)).executeQuery(context, query, null);
    verify(plugin, times(1)).cache(any(byte[].class), any(byte[].class), 
        anyLong(), any(TimeUnit.class));
    assertFalse(executor.outstandingRequests().contains(exec));
    assertFalse(downstream.cancelled);
    assertTrue(cache_execution.cancelled);
  }
  
  @Test
  public void executeCacheException() throws Exception {
    final CachingQueryExecutor<IteratorGroups> executor = 
        new CachingQueryExecutor<IteratorGroups>(node);
    final QueryExecution<IteratorGroups> exec = 
        executor.executeQuery(context, query, span);
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    verify(plugin, times(1))
      .fetch(any(QueryContext.class), any(byte[].class), any(Span.class));
    verify(this.executor, never()).executeQuery(context, query, null);
    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
        anyLong(), any(TimeUnit.class));
    assertTrue(executor.outstandingRequests().contains(exec));
    
    // cache exception
    cache_execution.callback(new IllegalStateException("Boo!"));
    
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    
    final IteratorGroups results = new DefaultIteratorGroups();
    downstream.callback(results);
    assertSame(results, exec.deferred().join());
    verify(this.executor, times(1)).executeQuery(context, query, null);
    verify(plugin, times(1)).cache(any(byte[].class), any(byte[].class), 
        anyLong(), any(TimeUnit.class));
    assertFalse(executor.outstandingRequests().contains(exec));
    assertFalse(downstream.cancelled);
    assertFalse(cache_execution.cancelled);
  }
  
  @Test
  public void executeCacheMissDownstreamException() throws Exception {
    final CachingQueryExecutor<IteratorGroups> executor = 
        new CachingQueryExecutor<IteratorGroups>(node);
    final QueryExecution<IteratorGroups> exec = 
        executor.executeQuery(context, query, span);
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    verify(plugin, times(1))
      .fetch(any(QueryContext.class), any(byte[].class), any(Span.class));
    verify(this.executor, never()).executeQuery(context, query, null);
    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
        anyLong(), any(TimeUnit.class));
    assertTrue(executor.outstandingRequests().contains(exec));
    
    // cache exception
    cache_execution.callback(null);
    
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    
    downstream.callback(new IllegalStateException("Boo!"));
    final Deferred<IteratorGroups> deferred = exec.deferred();
    try {
      deferred.join();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    verify(this.executor, times(1)).executeQuery(context, query, null);
    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
        anyLong(), any(TimeUnit.class));
    assertFalse(executor.outstandingRequests().contains(exec));
    assertFalse(downstream.cancelled);
    assertFalse(cache_execution.cancelled);
  }
  
  @Test
  public void executeSimultaneousCacheException() throws Exception {
    config = (Config) Config.newBuilder()
        .setExpiration(60000)
        .setSimultaneous(true)
        .setExecutorId("LocalCache")
        .setExecutorType("CachingQueryExecutor")
        .build();
    when(node.getDefaultConfig()).thenReturn(config);
    
    final CachingQueryExecutor<IteratorGroups> executor = 
        new CachingQueryExecutor<IteratorGroups>(node);
    final QueryExecution<IteratorGroups> exec = 
        executor.executeQuery(context, query, span);
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    verify(plugin, times(1))
      .fetch(any(QueryContext.class), any(byte[].class), any(Span.class));
    verify(this.executor, times(1)).executeQuery(context, query, null);
    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
        anyLong(), any(TimeUnit.class));
    assertTrue(executor.outstandingRequests().contains(exec));
    
    // cache exception
    cache_execution.callback(new IllegalStateException("Boo!"));
    
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    
    IteratorGroups results = new DefaultIteratorGroups();
    downstream.callback(results);
    
    assertSame(results, exec.deferred().join());
    verify(this.executor, times(1)).executeQuery(context, query, null);
    verify(plugin, times(1)).cache(any(byte[].class), any(byte[].class), 
        anyLong(), any(TimeUnit.class));
    assertFalse(executor.outstandingRequests().contains(exec));
    assertFalse(downstream.cancelled);
    assertFalse(cache_execution.cancelled);
  }
  
  @Test
  public void executeSimultaneousDownstreamException() throws Exception {
    config = (Config) Config.newBuilder()
        .setExpiration(60000)
        .setSimultaneous(true)
        .setExecutorId("LocalCache")
        .setExecutorType("CachingQueryExecutor")
        .build();
    when(node.getDefaultConfig()).thenReturn(config);
    
    final CachingQueryExecutor<IteratorGroups> executor = 
        new CachingQueryExecutor<IteratorGroups>(node);
    final QueryExecution<IteratorGroups> exec = 
        executor.executeQuery(context, query, span);
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    verify(plugin, times(1))
      .fetch(any(QueryContext.class), any(byte[].class), any(Span.class));
    verify(this.executor, times(1)).executeQuery(context, query, null);
    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
        anyLong(), any(TimeUnit.class));
    assertTrue(executor.outstandingRequests().contains(exec));
    
    downstream.callback(new IllegalStateException("Boo!"));
    
    try {
      exec.deferred.join();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    verify(this.executor, times(1)).executeQuery(context, query, null);
    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
        anyLong(), any(TimeUnit.class));
    assertFalse(executor.outstandingRequests().contains(exec));
    assertFalse(downstream.cancelled);
    assertTrue(cache_execution.cancelled);
  }
  
  @Test
  public void executeCacheWaitCancel() throws Exception {
    final CachingQueryExecutor<IteratorGroups> executor = 
        new CachingQueryExecutor<IteratorGroups>(node);
    final QueryExecution<IteratorGroups> exec = 
        executor.executeQuery(context, query, span);
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    verify(plugin, times(1))
      .fetch(any(QueryContext.class), any(byte[].class), any(Span.class));
    verify(this.executor, never()).executeQuery(context, query, null);
    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
        anyLong(), any(TimeUnit.class));
    assertTrue(executor.outstandingRequests().contains(exec));
    
    exec.cancel();
    
    try {
      exec.deferred().join();
      fail("Expected QueryExecutionException");
    } catch (QueryExecutionException e) { }
    verify(this.executor, never()).executeQuery(context, query, null);
    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
        anyLong(), any(TimeUnit.class));
    assertFalse(executor.outstandingRequests().contains(exec));
    assertFalse(downstream.cancelled);
    assertTrue(cache_execution.cancelled);
  }
  
  @Test
  public void executeDownstreamWaitCancel() throws Exception {
    final CachingQueryExecutor<IteratorGroups> executor = 
        new CachingQueryExecutor<IteratorGroups>(node);
    final QueryExecution<IteratorGroups> exec = 
        executor.executeQuery(context, query, span);
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    verify(plugin, times(1))
      .fetch(any(QueryContext.class), any(byte[].class), any(Span.class));
    verify(this.executor, never()).executeQuery(context, query, null);
    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
        anyLong(), any(TimeUnit.class));
    assertTrue(executor.outstandingRequests().contains(exec));
    
    cache_execution.callback(null);
    
    exec.cancel();
    
    try {
      exec.deferred().join();
      fail("Expected QueryExecutionException");
    } catch (QueryExecutionException e) { }
    verify(this.executor, times(1)).executeQuery(context, query, null);
    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
        anyLong(), any(TimeUnit.class));
    assertFalse(executor.outstandingRequests().contains(exec));
    assertTrue(downstream.cancelled);
    assertFalse(cache_execution.cancelled);
  }

  @Test
  public void executeBypassDefault() throws Exception {
    config = (Config) Config.newBuilder()
        .setExpiration(60000)
        .setBypass(true)
        .setKeyGeneratorId("MyKeyGen")
        .setExecutorId("LocalCache")
        .setExecutorType("CachingQueryExecutor")
        .build();
    when(node.getDefaultConfig()).thenReturn(config);
    
    final CachingQueryExecutor<IteratorGroups> executor = 
        new CachingQueryExecutor<IteratorGroups>(node);
    final QueryExecution<IteratorGroups> exec = 
        executor.executeQuery(context, query, span);
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    verify(plugin, never())
      .fetch(any(QueryContext.class), any(byte[].class), any(Span.class));
    verify(this.executor, times(1)).executeQuery(context, query, null);
    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
        anyLong(), any(TimeUnit.class));
    assertTrue(executor.outstandingRequests().contains(exec));

    final IteratorGroups results = new DefaultIteratorGroups();
    downstream.callback(results);
    assertSame(results, exec.deferred().join());
    verify(this.executor, times(1)).executeQuery(context, query, null);
    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
        anyLong(), any(TimeUnit.class));
    assertFalse(executor.outstandingRequests().contains(exec));
    assertFalse(downstream.cancelled);
    assertFalse(cache_execution.cancelled);
  }
  
  @Test
  public void executeBypassOverride() throws Exception {
    config = (Config) Config.newBuilder()
        .setBypass(true)
        .setExecutorId("LocalCache")
        .setExecutorType("CachingQueryExecutor")
        .build();
    when(context.getConfigOverride(anyString()))
      .thenReturn(config);
    
    final CachingQueryExecutor<IteratorGroups> executor = 
        new CachingQueryExecutor<IteratorGroups>(node);
    final QueryExecution<IteratorGroups> exec = 
        executor.executeQuery(context, query, span);
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    verify(plugin, never())
      .fetch(any(QueryContext.class), any(byte[].class), any(Span.class));
    verify(this.executor, times(1)).executeQuery(context, query, null);
    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
        anyLong(), any(TimeUnit.class));
    assertTrue(executor.outstandingRequests().contains(exec));

    final IteratorGroups results = new DefaultIteratorGroups();
    downstream.callback(results);
    assertSame(results, exec.deferred().join());
    verify(this.executor, times(1)).executeQuery(context, query, null);
    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
        anyLong(), any(TimeUnit.class));
    assertFalse(executor.outstandingRequests().contains(exec));
    assertFalse(downstream.cancelled);
    assertFalse(cache_execution.cancelled);
  }
  
  @Test
  public void close() throws Exception {
    final CachingQueryExecutor<IteratorGroups> executor = 
        new CachingQueryExecutor<IteratorGroups>(node);
    final QueryExecution<IteratorGroups> exec = 
        executor.executeQuery(context, query, span);
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    verify(plugin, times(1))
      .fetch(any(QueryContext.class), any(byte[].class), any(Span.class));
    verify(this.executor, never()).executeQuery(context, query, null);
    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
        anyLong(), any(TimeUnit.class));
    assertTrue(executor.outstandingRequests().contains(exec));
    
    executor.close();
    
    try {
      exec.deferred().join();
      fail("Expected QueryExecutionException");
    } catch (QueryExecutionException e) { }
    verify(this.executor, never()).executeQuery(context, query, null);
    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
        anyLong(), any(TimeUnit.class));
    assertFalse(executor.outstandingRequests().contains(exec));
    assertFalse(downstream.cancelled);
    assertTrue(cache_execution.cancelled);
  }

  @Test
  public void builder() throws Exception {
    String json = JSON.serializeToString(config);
    assertTrue(json.contains("\"executorType\":\"CachingQueryExecutor\""));
    assertTrue(json.contains("\"simultaneous\":false"));
    assertTrue(json.contains("\"expiration\":60000"));
    assertTrue(json.contains("\"executorId\":\"LocalCache\""));
    assertTrue(json.contains("\"useTimestamps\":false"));
    assertTrue(json.contains("\"keyGeneratorId\":\"MyKeyGen\""));
    
    json = "{\"executorType\":\"CachingQueryExecutor\",\"simultaneous\":false,"
        + "\"expiration\":60000,\"bypass\":true,\"keyGeneratorId\":\"MyKeyGen\","
        + "\"useTimestamps\":false,\"executorId\":\"LocalCache\"}";
    config = JSON.parseToObject(json, Config.class);
    assertEquals("CachingQueryExecutor", config.executorType());
    assertEquals("LocalCache", config.getExecutorId());
    assertFalse(config.getSimultaneous());
    assertEquals(60000, config.getExpiration());
    assertTrue(config.getBypass());
    assertFalse(config.getUseTimestamps());
    assertEquals("MyKeyGen", config.getKeyGeneratorId());
  }
  
  @Test
  public void hashCodeEqualsCompareTo() throws Exception {
    final Config c1 = (Config) Config.newBuilder()
        .setExpiration(60000)
        .setBypass(true)
        .setSimultaneous(true)
        .setUseTimestamps(true)
        .setKeyGeneratorId("MyKeyGen")
        .setExecutorId("LocalCache")
        .setExecutorType("CachingQueryExecutor")
        .build();
    
    Config c2 = (Config) Config.newBuilder()
        .setExpiration(60000)
        .setBypass(true)
        .setSimultaneous(true)
        .setUseTimestamps(true)
        .setKeyGeneratorId("MyKeyGen")
        .setExecutorId("LocalCache")
        .setExecutorType("CachingQueryExecutor")
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
        .setExecutorId("LocalCache")
        .setExecutorType("CachingQueryExecutor")
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
        .setExecutorId("LocalCache")
        .setExecutorType("CachingQueryExecutor")
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
        .setExecutorId("LocalCache")
        .setExecutorType("CachingQueryExecutor")
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
        .setExecutorId("LocalCache")
        .setExecutorType("CachingQueryExecutor")
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
        .setExecutorId("LocalCache")
        .setExecutorType("CachingQueryExecutor")
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
        .setExecutorId("LocalCache")
        .setExecutorType("CachingQueryExecutor")
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
        .setExecutorId("TestCache")  // <-- Diff
        .setExecutorType("CachingQueryExecutor")
        .build();
    assertNotEquals(c1.hashCode(), c2.hashCode());
    assertNotEquals(c1, c2);
    assertEquals(-1, c1.compareTo(c2));
    
    c2 = (Config) Config.newBuilder()
        .setExpiration(60000)
        .setBypass(true)
        .setSimultaneous(true)
        .setUseTimestamps(true)
        .setKeyGeneratorId("MyKeyGen")
        .setExecutorId("LocalCache")
        .setExecutorType("CachingQueryExecutor2")  // <-- Diff
        .build();
    assertNotEquals(c1.hashCode(), c2.hashCode());
    assertNotEquals(c1, c2);
    assertEquals(-1, c1.compareTo(c2));
  }
}
