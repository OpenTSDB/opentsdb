//// This file is part of OpenTSDB.
//// Copyright (C) 2017  The OpenTSDB Authors.
////
//// Licensed under the Apache License, Version 2.0 (the "License");
//// you may not use this file except in compliance with the License.
//// You may obtain a copy of the License at
////
////   http://www.apache.org/licenses/LICENSE-2.0
////
//// Unless required by applicable law or agreed to in writing, software
//// distributed under the License is distributed on an "AS IS" BASIS,
//// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//// See the License for the specific language governing permissions and
//// limitations under the License.
//package net.opentsdb.query.execution;
//
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertFalse;
//import static org.junit.Assert.assertNotEquals;
//import static org.junit.Assert.assertNull;
//import static org.junit.Assert.assertSame;
//import static org.junit.Assert.assertTrue;
//import static org.junit.Assert.fail;
//import static org.mockito.Matchers.any;
//import static org.mockito.Matchers.anyList;
//import static org.mockito.Matchers.anyLong;
//import static org.mockito.Matchers.anyString;
//import static org.mockito.Matchers.eq;
//import static org.mockito.Mockito.doAnswer;
//import static org.mockito.Mockito.doThrow;
//import static org.mockito.Mockito.mock;
//import static org.mockito.Mockito.never;
//import static org.mockito.Mockito.spy;
//import static org.mockito.Mockito.times;
//import static org.mockito.Mockito.verify;
//import static org.mockito.Mockito.when;
//
//import java.io.ByteArrayOutputStream;
//import java.io.InputStream;
//import java.util.List;
//import java.util.concurrent.TimeUnit;
//
//import org.junit.Before;
//import org.junit.Test;
//import org.mockito.invocation.InvocationOnMock;
//import org.mockito.stubbing.Answer;
//
//import com.google.common.collect.Lists;
//import com.stumbleupon.async.Deferred;
//import com.stumbleupon.async.TimeoutException;
//import net.opentsdb.configuration.Configuration;
//import net.opentsdb.data.TimeSeriesValue;
//import net.opentsdb.data.iterators.IteratorGroup;
//import net.opentsdb.data.iterators.IteratorGroups;
//import net.opentsdb.data.iterators.IteratorStatus;
//import net.opentsdb.data.iterators.IteratorTestUtils;
//import net.opentsdb.data.iterators.TimeSeriesIterator;
//import net.opentsdb.data.types.numeric.NumericType;
//import net.opentsdb.exceptions.QueryExecutionCanceled;
//import net.opentsdb.exceptions.QueryExecutionException;
//import net.opentsdb.query.QueryContext;
//import net.opentsdb.query.execution.TimeSlicedCachingExecutor.Config;
//import net.opentsdb.query.execution.TestQueryExecutor.MockDownstream;
//import net.opentsdb.query.execution.cache.DefaultTimeSeriesCacheKeyGenerator;
//import net.opentsdb.query.execution.cache.QueryCachePlugin;
//import net.opentsdb.query.execution.cache.TimeSeriesCacheKeyGenerator;
//import net.opentsdb.query.execution.graph.ExecutionGraphNode;
//import net.opentsdb.query.plan.IteratorGroupsSlicePlanner;
//import net.opentsdb.query.plan.QueryPlannnerFactory;
//import net.opentsdb.query.plan.QueryPlanner;
//import net.opentsdb.query.plan.SplitMetricPlanner;
//import net.opentsdb.query.pojo.Metric;
//import net.opentsdb.query.pojo.TimeSeriesQuery;
//import net.opentsdb.query.pojo.Timespan;
//import net.opentsdb.stats.Span;
//import net.opentsdb.query.serdes.TimeSeriesSerdes;
//import net.opentsdb.utils.Bytes.ByteMap;
//import net.opentsdb.utils.JSON;
//
//public class TestTimeSlicedCachingExecutor extends BaseExecutorTest {
//
//  private QueryExecutor<IteratorGroups> executor;
//  private MockDownstream<IteratorGroups> cache_execution;
//  private Configuration tsd_config;
//  private Config config;
//  private QueryCachePlugin plugin;
//  private TimeSeriesSerdes serdes;
//  private List<MockDownstream<IteratorGroups>> downstreams;
//  private IteratorGroupsSlicePlanner planner;
//  private ByteMap<byte[]> cache;
//  private TimeSeriesCacheKeyGenerator key_generator;
//  private QueryPlannnerFactory<IteratorGroups> plan_factory;
//  
//  private long ts_start;
//  private long ts_end;
//  
//  @SuppressWarnings("unchecked")
//  @Before
//  public void beforeLocal() throws Exception {
//    node = mock(ExecutionGraphNode.class);
//    executor = mock(QueryExecutor.class);
//    plugin = mock(QueryCachePlugin.class);
//    config = (Config) Config.newBuilder()
//        .setExpiration(60000)
//        .setPlannerId("IteratorGroupsSlicePlanner")
//        .setSerdesId("Default")
//        .setKeyGeneratorId("MyKeyGen")
//        .setExecutorId("LocalCache")
//        .setExecutorType("CachingQueryExecutor")
//        .build();
//    downstreams = Lists.newArrayList();
//    cache = new ByteMap<byte[]>();
//    
//    plan_factory = mock(QueryPlannnerFactory.class);
//    
//    tsd_config = new Configuration(new String[] { 
//        "--" + Configuration.CONFIG_PROVIDERS_KEY + "=RuntimeOverride" });
//    when(tsdb.getConfig()).thenReturn(tsd_config);
//    key_generator = new DefaultTimeSeriesCacheKeyGenerator();
//    key_generator.initialize(tsdb).join();
//    when(node.graph()).thenReturn(graph);
//    when(node.getConfig()).thenReturn(config);
//    when(graph.getDownstreamExecutor(anyString()))
//      .thenAnswer(new Answer<QueryExecutor<?>>() {
//      @Override
//      public QueryExecutor<?> answer(InvocationOnMock invocation)
//          throws Throwable {
//        return executor;
//      }
//    });
//    when(executor.close()).thenReturn(Deferred.fromResult(null));
//    when(registry.getPlugin(eq(QueryCachePlugin.class), anyString()))
//      .thenReturn(plugin);
//    when(registry.getPlugin(eq(TimeSeriesCacheKeyGenerator.class), anyString()))
//      .thenReturn(key_generator);
//    when(registry.getQueryPlanner(anyString()))
//      .thenAnswer(new Answer<QueryPlannnerFactory<?>>() {
//      @Override
//      public QueryPlannnerFactory<?> answer(InvocationOnMock invocation)
//          throws Throwable {
//        return plan_factory;
//      }
//    });
//    when(registry.getSerdes(anyString()))
//      .thenAnswer(new Answer<TimeSeriesSerdes>() {
//      @Override
//      public TimeSeriesSerdes answer(
//          final InvocationOnMock invocation) throws Throwable {
//        return serdes;
//      }
//    });
//    
//    ts_start = 1493942400000L;
//    ts_end = 1493956800000L;
//    
//    query = TimeSeriesQuery.newBuilder()
//        .setTime(Timespan.newBuilder()
//            .setStart(Long.toString(ts_start))
//            .setEnd(Long.toString(ts_end)))
//        .addMetric(Metric.newBuilder()
//            .setMetric("system.cpu.user"))
//        .build();
//    planner = spy(new IteratorGroupsSlicePlanner(query));
//    when(plan_factory.newPlanner(any(TimeSeriesQuery.class))).thenReturn(planner);
//    when(executor.executeQuery(eq(context), any(TimeSeriesQuery.class), any(io.opentracing.Span.class)))
//      .thenAnswer(new Answer<QueryExecution<IteratorGroups>>() {
//        @Override
//        public QueryExecution<IteratorGroups> answer(
//            InvocationOnMock invocation) throws Throwable {
//          final MockDownstream<IteratorGroups> downstream = 
//              new MockDownstream<IteratorGroups>(query);
//          downstreams.add(downstream);
//          return downstream;
//        }
//    });
//    cache_execution = new MockDownstream<IteratorGroups>(query);
//    when(plugin.fetch(any(QueryContext.class), any(byte[][].class), any(Span.class)))
//      .thenAnswer(new Answer<QueryExecution<IteratorGroups>>() {
//        @Override
//        public QueryExecution<IteratorGroups> answer(
//            final InvocationOnMock invocation) throws Throwable {
//          return cache_execution;
//        }
//      });
//    doAnswer(new Answer<Void>() {
//      @Override
//      public Void answer(InvocationOnMock invocation) throws Throwable {
//        final byte[][] keys = (byte[][]) invocation.getArguments()[0];
//        final byte[][] values = (byte[][]) invocation.getArguments()[1];
//        for (int i = 0; i < keys.length; i++) {
//          cache.put(keys[i], values[i]);
//        }
//        return null;
//      }
//    }).when(plugin).cache(any(byte[][].class), any(byte[][].class), 
//        any(long[].class), eq(TimeUnit.MILLISECONDS), any(Span.class));
//  }
//  
////  @Test
////  public void ctor() throws Exception {
////    TimeSlicedCachingExecutor<IteratorGroups> executor = 
////        new TimeSlicedCachingExecutor<IteratorGroups>(node);
////    assertSame(plugin, executor.plugin());
////    assertSame(serdes, executor.serdes());
////    assertTrue(executor.keyGenerator() instanceof 
////        DefaultTimeSeriesCacheKeyGenerator);
////    assertEquals(1, executor.downstreamExecutors().size());
////    assertSame(this.executor, executor.downstreamExecutors().get(0));
////    
////    try {
////      new TimeSlicedCachingExecutor<IteratorGroups>(null);
////      fail("Expected IllegalArgumentException");
////    } catch (IllegalArgumentException e) { }
////    
////    when(node.getDefaultConfig()).thenReturn(null);
////    try {
////      new TimeSlicedCachingExecutor<IteratorGroups>(node);
////      fail("Expected IllegalArgumentException");
////    } catch (IllegalArgumentException e) { }
////    
////    when(node.getDefaultConfig()).thenReturn(config);
////    when(graph.getDownstreamExecutor(anyString())).thenReturn(null);
////    try {
////      new TimeSlicedCachingExecutor<IteratorGroups>(node);
////      fail("Expected IllegalArgumentException");
////    } catch (IllegalArgumentException e) { }
////    
////    final QueryExecutor<?> ex = executor;
////    when(graph.getDownstreamExecutor(anyString()))
////      .thenAnswer(new Answer<QueryExecutor<?>>() {
////      @Override
////      public QueryExecutor<?> answer(InvocationOnMock invocation)
////          throws Throwable {
////        return ex;
////      }
////    });
////    when(registry.getPlugin(eq(QueryCachePlugin.class), anyString()))
////      .thenReturn(null);
////    try {
////      new TimeSlicedCachingExecutor<IteratorGroups>(node);
////      fail("Expected IllegalArgumentException");
////    } catch (IllegalArgumentException e) { }
////    
////    when(registry.getPlugin(eq(QueryCachePlugin.class), anyString()))
////      .thenReturn(plugin);
////    when(registry.getSerdes(anyString())).thenReturn(null);
////    try {
////      new TimeSlicedCachingExecutor<IteratorGroups>(node);
////      fail("Expected IllegalArgumentException");
////    } catch (IllegalArgumentException e) { }
////    
////    when(registry.getSerdes(anyString()))
////      .thenAnswer(new Answer<TimeSeriesSerdes>() {
////      @Override
////      public TimeSeriesSerdes answer(
////          final InvocationOnMock invocation) throws Throwable {
////        return serdes;
////      }
////    });
////    when(registry.getPlugin(eq(TimeSeriesCacheKeyGenerator.class), anyString()))
////      .thenReturn(null);
////    try {
////      new TimeSlicedCachingExecutor<IteratorGroups>(node);
////      fail("Expected IllegalArgumentException");
////    } catch (IllegalArgumentException e) { }
////  }
////  
////  @SuppressWarnings("unchecked")
////  @Test
////  public void executeCacheMissAll() throws Exception {
////    final TimeSlicedCachingExecutor<IteratorGroups> executor = 
////        new TimeSlicedCachingExecutor<IteratorGroups>(node);
////    final QueryExecution<IteratorGroups> exec = 
////        executor.executeQuery(context, query, span);
////    try {
////      exec.deferred().join(1);
////      fail("Expected TimeoutException");
////    } catch (TimeoutException e) { }
////    verify(plugin, times(1))
////      .fetch(any(QueryContext.class), any(byte[][].class), any(Span.class));
////    verify(this.executor, never()).executeQuery(context, query, null);
////    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
////        anyLong(), any(TimeUnit.class));
////    assertTrue(executor.outstandingRequests().contains(exec));
////    
////    // cache miss
////    cache_execution.callback(new byte[4][]);
////    
////    try {
////      exec.deferred().join(1);
////      fail("Expected TimeoutException");
////    } catch (TimeoutException e) { }
////    
////    final IteratorGroups data = 
////        IteratorTestUtils.generateData(ts_start, ts_end, 0, 300000);
////    downstreams.get(0).callback(data);
////    final IteratorGroups results = exec.deferred().join(1);
////    assertEquals(4, results.flattenedIterators().size());
////    
////    // validate data returned
////    IteratorGroup group = results.group(IteratorTestUtils.GROUP_A);
////    assertEquals(2, group.flattenedIterators().size());
////    assertSame(IteratorTestUtils.ID_A, group.flattenedIterators().get(0).id());
////    assertSame(IteratorTestUtils.ID_B, group.flattenedIterators().get(1).id());
////    
////    TimeSeriesIterator<NumericType> iterator = 
////        (TimeSeriesIterator<NumericType>) group.flattenedIterators().get(0);
////    assertEquals(1493942400000L, iterator.startTime().msEpoch());
////    assertEquals(1493956800000L, iterator.endTime().msEpoch());
////    long ts = ts_start;
////    int count = 0;
////    while (iterator.status() == IteratorStatus.HAS_DATA) {
////      TimeSeriesValue<NumericType> v = iterator.next();
////      assertEquals(ts, v.timestamp().msEpoch());
////      assertEquals(ts, v.value().longValue());
////        ts += 300000;
////      ++count;
////    }
////    assertEquals(49, count);
////    
////    iterator = 
////        (TimeSeriesIterator<NumericType>) group.flattenedIterators().get(1);
////    assertEquals(1493942400000L, iterator.startTime().msEpoch());
////    assertEquals(1493956800000L, iterator.endTime().msEpoch());
////    ts = ts_start;
////    count = 0;
////    while (iterator.status() == IteratorStatus.HAS_DATA) {
////      TimeSeriesValue<NumericType> v = iterator.next();
////      assertEquals(ts, v.timestamp().msEpoch());
////      assertEquals(ts, v.value().longValue());
////        ts += 300000;
////      ++count;
////    }
////    assertEquals(49, count);
////    
////    group = results.group(IteratorTestUtils.GROUP_B);
////    assertEquals(2, group.flattenedIterators().size());
////    assertSame(IteratorTestUtils.ID_A, group.flattenedIterators().get(0).id());
////    assertSame(IteratorTestUtils.ID_B, group.flattenedIterators().get(1).id());
////    
////    iterator = 
////        (TimeSeriesIterator<NumericType>) group.flattenedIterators().get(0);
////    assertEquals(1493942400000L, iterator.startTime().msEpoch());
////    assertEquals(1493956800000L, iterator.endTime().msEpoch());
////    ts = ts_start;
////    count = 0;
////    while (iterator.status() == IteratorStatus.HAS_DATA) {
////      TimeSeriesValue<NumericType> v = iterator.next();
////      assertEquals(ts, v.timestamp().msEpoch());
////      assertEquals(ts, v.value().longValue());
////        ts += 300000;
////      ++count;
////    }
////    assertEquals(49, count);
////    
////    iterator = 
////        (TimeSeriesIterator<NumericType>) group.flattenedIterators().get(1);
////    assertEquals(1493942400000L, iterator.startTime().msEpoch());
////    assertEquals(1493956800000L, iterator.endTime().msEpoch());
////    ts = ts_start;
////    count = 0;
////    while (iterator.status() == IteratorStatus.HAS_DATA) {
////      TimeSeriesValue<NumericType> v = iterator.next();
////      assertEquals(ts, v.timestamp().msEpoch());
////      assertEquals(ts, v.value().longValue());
////        ts += 300000;
////      ++count;
////    }
////    assertEquals(49, count);
////    
////    verify(this.executor, times(1)).executeQuery(eq(context), 
////        any(TimeSeriesQuery.class), any(Span.class));
////    verify(plugin, times(1)).fetch(eq(context), any(byte[][].class), any(Span.class));
////    verify(plugin, times(1)).cache(any(byte[][].class), any(byte[][].class), 
////        any(long[].class), any(TimeUnit.class));
////    assertFalse(executor.outstandingRequests().contains(exec));
////    assertFalse(downstreams.get(0).cancelled);
////    assertFalse(cache_execution.cancelled);
////    assertEquals(4, cache.size());
////    
////    final byte[][] keys = key_generator.generate(query, planner.getTimeRanges());
////    assertTrue(cache.containsKey(keys[0]));
////    assertTrue(cache.containsKey(keys[1]));
////    assertTrue(cache.containsKey(keys[2]));
////    assertTrue(cache.containsKey(keys[3]));
////  }
////  
////  @SuppressWarnings("unchecked")
////  @Test
////  public void executeCacheHitAll() throws Exception {
////    final TimeSlicedCachingExecutor<IteratorGroups> executor = 
////        new TimeSlicedCachingExecutor<IteratorGroups>(node);
////    final QueryExecution<IteratorGroups> exec = 
////        executor.executeQuery(context, query, span);
////    try {
////      exec.deferred().join(1);
////      fail("Expected TimeoutException");
////    } catch (TimeoutException e) { }
////    verify(plugin, times(1))
////      .fetch(any(QueryContext.class), any(byte[][].class), any(Span.class));
////    verify(this.executor, never()).executeQuery(context, query, null);
////    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
////        anyLong(), any(TimeUnit.class));
////    assertTrue(executor.outstandingRequests().contains(exec));
////    
////    // cache hit
////    final byte[][] cache_results = new byte[4][];
////    final List<IteratorGroups> slices = planner.sliceResult(
////        IteratorTestUtils.generateData(ts_start, ts_end, 0, 300000), 0, 3);
////    for (int i = 0; i < slices.size(); i++) {
////      final ByteArrayOutputStream output = new ByteArrayOutputStream();
////      serdes.serialize(query, null, output, slices.get(i));
////      output.close();
////      cache_results[i] = output.toByteArray();
////    }
////    cache_execution.callback(cache_results);
////
////    final IteratorGroups results = exec.deferred().join(1);
////    assertEquals(4, results.flattenedIterators().size());
////    
////    // validate data returned
////    IteratorGroup group = results.group(IteratorTestUtils.GROUP_A);
////    assertEquals(2, group.flattenedIterators().size());
////    assertEquals(IteratorTestUtils.ID_A, group.flattenedIterators().get(0).id());
////    assertEquals(IteratorTestUtils.ID_B, group.flattenedIterators().get(1).id());
////    
////    TimeSeriesIterator<NumericType> iterator = 
////        (TimeSeriesIterator<NumericType>) group.flattenedIterators().get(0);
////    assertEquals(1493942400000L, iterator.startTime().msEpoch());
////    assertEquals(1493956800000L, iterator.endTime().msEpoch());
////    long ts = ts_start;
////    int count = 0;
////    while (iterator.status() == IteratorStatus.HAS_DATA) {
////      TimeSeriesValue<NumericType> v = iterator.next();
////      assertEquals(ts, v.timestamp().msEpoch());
////      assertEquals(ts, v.value().longValue());
////        ts += 300000;
////      ++count;
////    }
////    assertEquals(49, count);
////    
////    iterator = 
////        (TimeSeriesIterator<NumericType>) group.flattenedIterators().get(1);
////    assertEquals(1493942400000L, iterator.startTime().msEpoch());
////    assertEquals(1493956800000L, iterator.endTime().msEpoch());
////    ts = ts_start;
////    count = 0;
////    while (iterator.status() == IteratorStatus.HAS_DATA) {
////      TimeSeriesValue<NumericType> v = iterator.next();
////      assertEquals(ts, v.timestamp().msEpoch());
////      assertEquals(ts, v.value().longValue());
////        ts += 300000;
////      ++count;
////    }
////    assertEquals(49, count);
////    
////    group = results.group(IteratorTestUtils.GROUP_B);
////    assertEquals(2, group.flattenedIterators().size());
////    assertEquals(IteratorTestUtils.ID_A, group.flattenedIterators().get(0).id());
////    assertEquals(IteratorTestUtils.ID_B, group.flattenedIterators().get(1).id());
////    
////    iterator = 
////        (TimeSeriesIterator<NumericType>) group.flattenedIterators().get(0);
////    assertEquals(1493942400000L, iterator.startTime().msEpoch());
////    assertEquals(1493956800000L, iterator.endTime().msEpoch());
////    ts = ts_start;
////    count = 0;
////    while (iterator.status() == IteratorStatus.HAS_DATA) {
////      TimeSeriesValue<NumericType> v = iterator.next();
////      assertEquals(ts, v.timestamp().msEpoch());
////      assertEquals(ts, v.value().longValue());
////        ts += 300000;
////      ++count;
////    }
////    assertEquals(49, count);
////    
////    iterator = 
////        (TimeSeriesIterator<NumericType>) group.flattenedIterators().get(1);
////    assertEquals(1493942400000L, iterator.startTime().msEpoch());
////    assertEquals(1493956800000L, iterator.endTime().msEpoch());
////    ts = ts_start;
////    count = 0;
////    while (iterator.status() == IteratorStatus.HAS_DATA) {
////      TimeSeriesValue<NumericType> v = iterator.next();
////      assertEquals(ts, v.timestamp().msEpoch());
////      assertEquals(ts, v.value().longValue());
////        ts += 300000;
////      ++count;
////    }
////    assertEquals(49, count);
////    
////    verify(this.executor, never()).executeQuery(eq(context), 
////        any(TimeSeriesQuery.class), any(Span.class));
////    verify(plugin, times(1)).fetch(eq(context), any(byte[][].class), any(Span.class));
////    verify(plugin, never()).cache(any(byte[][].class), any(byte[][].class), 
////        any(long[].class), any(TimeUnit.class));
////    assertFalse(executor.outstandingRequests().contains(exec));
////    assertTrue(downstreams.isEmpty());
////    assertFalse(cache_execution.cancelled);
////    assertEquals(0, cache.size());
////  }
////
////  @SuppressWarnings("unchecked")
////  @Test
////  public void executeCachePartialHitTip() throws Exception {
////    final TimeSlicedCachingExecutor<IteratorGroups> executor = 
////        new TimeSlicedCachingExecutor<IteratorGroups>(node);
////    final QueryExecution<IteratorGroups> exec = 
////        executor.executeQuery(context, query, span);
////    try {
////      exec.deferred().join(1);
////      fail("Expected TimeoutException");
////    } catch (TimeoutException e) { }
////    verify(plugin, times(1))
////      .fetch(any(QueryContext.class), any(byte[][].class), any(Span.class));
////    verify(this.executor, never()).executeQuery(context, query, null);
////    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
////        anyLong(), any(TimeUnit.class));
////    assertTrue(executor.outstandingRequests().contains(exec));
////    
////    // cache hit
////    final byte[][] cache_results = new byte[4][];
////    final List<IteratorGroups> slices = planner.sliceResult(
////        IteratorTestUtils.generateData(ts_start, 1493949600000L, 0, 300000), 0, 3);
////    for (int i = 0; i < 2; i++) {
////      final ByteArrayOutputStream output = new ByteArrayOutputStream();
////      serdes.serialize(query, null, output, slices.get(i));
////      output.close();
////      cache_results[i] = output.toByteArray();
////    }
////    cache_execution.callback(cache_results);
////
////    try {
////      exec.deferred().join(1);
////      fail("Expected TimeoutException");
////    } catch (TimeoutException e) { }
////    
////    assertEquals(1, downstreams.size());
////    downstreams.get(0).callback(
////        IteratorTestUtils.generateData(1493949600000L, ts_end, 0, 300000));
////    
////    final IteratorGroups results = exec.deferred().join(1);
////    assertEquals(4, results.flattenedIterators().size());
////    
////    // validate data returned
////    IteratorGroup group = results.group(IteratorTestUtils.GROUP_A);
////    assertEquals(2, group.flattenedIterators().size());
////    assertEquals(IteratorTestUtils.ID_A, group.flattenedIterators().get(0).id());
////    assertEquals(IteratorTestUtils.ID_B, group.flattenedIterators().get(1).id());
////    
////    TimeSeriesIterator<NumericType> iterator = 
////        (TimeSeriesIterator<NumericType>) group.flattenedIterators().get(0);
////    assertEquals(1493942400000L, iterator.startTime().msEpoch());
////    assertEquals(1493956800000L, iterator.endTime().msEpoch());
////    long ts = ts_start;
////    int count = 0;
////    while (iterator.status() == IteratorStatus.HAS_DATA) {
////      TimeSeriesValue<NumericType> v = iterator.next();
////      assertEquals(ts, v.timestamp().msEpoch());
////      assertEquals(ts, v.value().longValue());
////        ts += 300000;
////      ++count;
////    }
////    assertEquals(49, count);
////    
////    iterator = 
////        (TimeSeriesIterator<NumericType>) group.flattenedIterators().get(1);
////    assertEquals(1493942400000L, iterator.startTime().msEpoch());
////    assertEquals(1493956800000L, iterator.endTime().msEpoch());
////    ts = ts_start;
////    count = 0;
////    while (iterator.status() == IteratorStatus.HAS_DATA) {
////      TimeSeriesValue<NumericType> v = iterator.next();
////      assertEquals(ts, v.timestamp().msEpoch());
////      assertEquals(ts, v.value().longValue());
////        ts += 300000;
////      ++count;
////    }
////    assertEquals(49, count);
////    
////    group = results.group(IteratorTestUtils.GROUP_B);
////    assertEquals(2, group.flattenedIterators().size());
////    assertEquals(IteratorTestUtils.ID_A, group.flattenedIterators().get(0).id());
////    assertEquals(IteratorTestUtils.ID_B, group.flattenedIterators().get(1).id());
////    
////    iterator = 
////        (TimeSeriesIterator<NumericType>) group.flattenedIterators().get(0);
////    assertEquals(1493942400000L, iterator.startTime().msEpoch());
////    assertEquals(1493956800000L, iterator.endTime().msEpoch());
////    ts = ts_start;
////    count = 0;
////    while (iterator.status() == IteratorStatus.HAS_DATA) {
////      TimeSeriesValue<NumericType> v = iterator.next();
////      assertEquals(ts, v.timestamp().msEpoch());
////      assertEquals(ts, v.value().longValue());
////        ts += 300000;
////      ++count;
////    }
////    assertEquals(49, count);
////    
////    iterator = 
////        (TimeSeriesIterator<NumericType>) group.flattenedIterators().get(1);
////    assertEquals(1493942400000L, iterator.startTime().msEpoch());
////    assertEquals(1493956800000L, iterator.endTime().msEpoch());
////    ts = ts_start;
////    count = 0;
////    while (iterator.status() == IteratorStatus.HAS_DATA) {
////      TimeSeriesValue<NumericType> v = iterator.next();
////      assertEquals(ts, v.timestamp().msEpoch());
////      assertEquals(ts, v.value().longValue());
////        ts += 300000;
////      ++count;
////    }
////    assertEquals(49, count);
////    
////    verify(this.executor, times(1)).executeQuery(eq(context), 
////        any(TimeSeriesQuery.class), any(Span.class));
////    verify(plugin, times(1)).fetch(eq(context), any(byte[][].class), any(Span.class));
////    verify(plugin, times(1)).cache(any(byte[][].class), any(byte[][].class), 
////        any(long[].class), any(TimeUnit.class));
////    assertFalse(executor.outstandingRequests().contains(exec));
////    assertEquals(1, downstreams.size());
////    assertFalse(cache_execution.cancelled);
////    assertEquals(2, cache.size());
////    
////    final byte[][] keys = key_generator.generate(query, planner.getTimeRanges());
////    assertFalse(cache.containsKey(keys[0]));
////    assertFalse(cache.containsKey(keys[1]));
////    assertTrue(cache.containsKey(keys[2]));
////    assertTrue(cache.containsKey(keys[3]));
////  }
////  
////  @SuppressWarnings("unchecked")
////  @Test
////  public void executeCachePartialHitTail() throws Exception {
////    final TimeSlicedCachingExecutor<IteratorGroups> executor = 
////        new TimeSlicedCachingExecutor<IteratorGroups>(node);
////    final QueryExecution<IteratorGroups> exec = 
////        executor.executeQuery(context, query, span);
////    try {
////      exec.deferred().join(1);
////      fail("Expected TimeoutException");
////    } catch (TimeoutException e) { }
////    verify(plugin, times(1))
////      .fetch(any(QueryContext.class), any(byte[][].class), any(Span.class));
////    verify(this.executor, never()).executeQuery(context, query, null);
////    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
////        anyLong(), any(TimeUnit.class));
////    assertTrue(executor.outstandingRequests().contains(exec));
////    
////    // cache hit
////    final byte[][] cache_results = new byte[4][];
////    final List<IteratorGroups> slices = planner.sliceResult(
////        IteratorTestUtils.generateData(1493949600000L, ts_end, 0, 300000), 2, 3);
////    for (int i = 2; i < 4; i++) {
////      final ByteArrayOutputStream output = new ByteArrayOutputStream();
////      serdes.serialize(query, null, output, slices.get(i - 2));
////      output.close();
////      cache_results[i] = output.toByteArray();
////    }
////    cache_execution.callback(cache_results);
////
////    try {
////      exec.deferred().join(1);
////      fail("Expected TimeoutException");
////    } catch (TimeoutException e) { }
////    
////    assertEquals(1, downstreams.size());
////    downstreams.get(0).callback(
////        IteratorTestUtils.generateData(ts_start, 1493949600000L, 0, 300000));
////    
////    final IteratorGroups results = exec.deferred().join(1);
////    assertEquals(4, results.flattenedIterators().size());
////    
////    // validate data returned
////    IteratorGroup group = results.group(IteratorTestUtils.GROUP_A);
////    assertEquals(2, group.flattenedIterators().size());
////    assertEquals(IteratorTestUtils.ID_A, group.flattenedIterators().get(0).id());
////    assertEquals(IteratorTestUtils.ID_B, group.flattenedIterators().get(1).id());
////    
////    TimeSeriesIterator<NumericType> iterator = 
////        (TimeSeriesIterator<NumericType>) group.flattenedIterators().get(0);
////    assertEquals(1493942400000L, iterator.startTime().msEpoch());
////    assertEquals(1493956800000L, iterator.endTime().msEpoch());
////    long ts = ts_start;
////    int count = 0;
////    while (iterator.status() == IteratorStatus.HAS_DATA) {
////      TimeSeriesValue<NumericType> v = iterator.next();
////      assertEquals(ts, v.timestamp().msEpoch());
////      assertEquals(ts, v.value().longValue());
////        ts += 300000;
////      ++count;
////    }
////    assertEquals(49, count);
////    
////    iterator = 
////        (TimeSeriesIterator<NumericType>) group.flattenedIterators().get(1);
////    assertEquals(1493942400000L, iterator.startTime().msEpoch());
////    assertEquals(1493956800000L, iterator.endTime().msEpoch());
////    ts = ts_start;
////    count = 0;
////    while (iterator.status() == IteratorStatus.HAS_DATA) {
////      TimeSeriesValue<NumericType> v = iterator.next();
////      assertEquals(ts, v.timestamp().msEpoch());
////      assertEquals(ts, v.value().longValue());
////        ts += 300000;
////      ++count;
////    }
////    assertEquals(49, count);
////    
////    group = results.group(IteratorTestUtils.GROUP_B);
////    assertEquals(2, group.flattenedIterators().size());
////    assertEquals(IteratorTestUtils.ID_A, group.flattenedIterators().get(0).id());
////    assertEquals(IteratorTestUtils.ID_B, group.flattenedIterators().get(1).id());
////    
////    iterator = 
////        (TimeSeriesIterator<NumericType>) group.flattenedIterators().get(0);
////    assertEquals(1493942400000L, iterator.startTime().msEpoch());
////    assertEquals(1493956800000L, iterator.endTime().msEpoch());
////    ts = ts_start;
////    count = 0;
////    while (iterator.status() == IteratorStatus.HAS_DATA) {
////      TimeSeriesValue<NumericType> v = iterator.next();
////      assertEquals(ts, v.timestamp().msEpoch());
////      assertEquals(ts, v.value().longValue());
////        ts += 300000;
////      ++count;
////    }
////    assertEquals(49, count);
////    
////    iterator = 
////        (TimeSeriesIterator<NumericType>) group.flattenedIterators().get(1);
////    assertEquals(1493942400000L, iterator.startTime().msEpoch());
////    assertEquals(1493956800000L, iterator.endTime().msEpoch());
////    ts = ts_start;
////    count = 0;
////    while (iterator.status() == IteratorStatus.HAS_DATA) {
////      TimeSeriesValue<NumericType> v = iterator.next();
////      assertEquals(ts, v.timestamp().msEpoch());
////      assertEquals(ts, v.value().longValue());
////        ts += 300000;
////      ++count;
////    }
////    assertEquals(49, count);
////    
////    verify(this.executor, times(1)).executeQuery(eq(context), 
////        any(TimeSeriesQuery.class), any(Span.class));
////    verify(plugin, times(1)).fetch(eq(context), any(byte[][].class), any(Span.class));
////    verify(plugin, times(1)).cache(any(byte[][].class), any(byte[][].class), 
////        any(long[].class), any(TimeUnit.class));
////    assertFalse(executor.outstandingRequests().contains(exec));
////    assertEquals(1, downstreams.size());
////    assertFalse(cache_execution.cancelled);
////    assertEquals(2, cache.size());
////    
////    final byte[][] keys = key_generator.generate(query, planner.getTimeRanges());
////    assertTrue(cache.containsKey(keys[0]));
////    assertTrue(cache.containsKey(keys[1]));
////    assertFalse(cache.containsKey(keys[2]));
////    assertFalse(cache.containsKey(keys[3]));
////  }
////  
////  @SuppressWarnings("unchecked")
////  @Test
////  public void executeCacheMissHitMissHit() throws Exception {
////    final TimeSlicedCachingExecutor<IteratorGroups> executor = 
////        new TimeSlicedCachingExecutor<IteratorGroups>(node);
////    final QueryExecution<IteratorGroups> exec = 
////        executor.executeQuery(context, query, span);
////    try {
////      exec.deferred().join(1);
////      fail("Expected TimeoutException");
////    } catch (TimeoutException e) { }
////    verify(plugin, times(1))
////      .fetch(any(QueryContext.class), any(byte[][].class), any(Span.class));
////    verify(this.executor, never()).executeQuery(context, query, null);
////    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
////        anyLong(), any(TimeUnit.class));
////    assertTrue(executor.outstandingRequests().contains(exec));
////    
////    // cache hit
////    final byte[][] cache_results = new byte[4][];
////    List<IteratorGroups> slices = planner.sliceResult(
////        IteratorTestUtils.generateData(1493946000000L, 1493949600000L, 0, 300000), 1, 1);
////    ByteArrayOutputStream output = new ByteArrayOutputStream();
////    serdes.serialize(query, null, output, slices.get(0));
////    output.close();
////    cache_results[1] = output.toByteArray();
////    
////    slices = planner.sliceResult(
////        IteratorTestUtils.generateData(1493953200000L, ts_end, 0, 300000), 3, 3);
////    output = new ByteArrayOutputStream();
////    serdes.serialize(query, null, output, slices.get(0));
////    output.close();
////    cache_results[3] = output.toByteArray();
////    
////    cache_execution.callback(cache_results);
////
////    try {
////      exec.deferred().join(1);
////      fail("Expected TimeoutException");
////    } catch (TimeoutException e) { }
////    
////    assertEquals(2, downstreams.size());
////    
////    downstreams.get(0).callback(
////        IteratorTestUtils.generateData(ts_start, 1493946000000L, 0, 300000));
////    
////    try {
////      exec.deferred().join(1);
////      fail("Expected TimeoutException");
////    } catch (TimeoutException e) { }
////    
////    downstreams.get(1).callback(
////        IteratorTestUtils.generateData(1493949600000L, 1493953200000L, 1, 300000));
////    
////    final IteratorGroups results = exec.deferred().join(1);
////    assertEquals(4, results.flattenedIterators().size());
////    
////    // validate data returned
////    IteratorGroup group = results.group(IteratorTestUtils.GROUP_A);
////    assertEquals(2, group.flattenedIterators().size());
////    assertEquals(IteratorTestUtils.ID_A, group.flattenedIterators().get(0).id());
////    assertEquals(IteratorTestUtils.ID_B, group.flattenedIterators().get(1).id());
////    
////    TimeSeriesIterator<NumericType> iterator = 
////        (TimeSeriesIterator<NumericType>) group.flattenedIterators().get(0);
////    assertEquals(1493942400000L, iterator.startTime().msEpoch());
////    assertEquals(1493956800000L, iterator.endTime().msEpoch());
////    long ts = ts_start;
////    int count = 0;
////    while (iterator.status() == IteratorStatus.HAS_DATA) {
////      TimeSeriesValue<NumericType> v = iterator.next();
////      assertEquals(ts, v.timestamp().msEpoch());
////      assertEquals(ts, v.value().longValue());
////        ts += 300000;
////      ++count;
////    }
////    assertEquals(49, count);
////    
////    iterator = 
////        (TimeSeriesIterator<NumericType>) group.flattenedIterators().get(1);
////    assertEquals(1493942400000L, iterator.startTime().msEpoch());
////    assertEquals(1493956800000L, iterator.endTime().msEpoch());
////    ts = ts_start;
////    count = 0;
////    while (iterator.status() == IteratorStatus.HAS_DATA) {
////      TimeSeriesValue<NumericType> v = iterator.next();
////      assertEquals(ts, v.timestamp().msEpoch());
////      assertEquals(ts, v.value().longValue());
////        ts += 300000;
////      ++count;
////    }
////    assertEquals(49, count);
////    
////    group = results.group(IteratorTestUtils.GROUP_B);
////    assertEquals(2, group.flattenedIterators().size());
////    assertEquals(IteratorTestUtils.ID_A, group.flattenedIterators().get(0).id());
////    assertEquals(IteratorTestUtils.ID_B, group.flattenedIterators().get(1).id());
////    
////    iterator = 
////        (TimeSeriesIterator<NumericType>) group.flattenedIterators().get(0);
////    assertEquals(1493942400000L, iterator.startTime().msEpoch());
////    assertEquals(1493956800000L, iterator.endTime().msEpoch());
////    ts = ts_start;
////    count = 0;
////    while (iterator.status() == IteratorStatus.HAS_DATA) {
////      TimeSeriesValue<NumericType> v = iterator.next();
////      assertEquals(ts, v.timestamp().msEpoch());
////      assertEquals(ts, v.value().longValue());
////        ts += 300000;
////      ++count;
////    }
////    assertEquals(49, count);
////    
////    iterator = 
////        (TimeSeriesIterator<NumericType>) group.flattenedIterators().get(1);
////    assertEquals(1493942400000L, iterator.startTime().msEpoch());
////    assertEquals(1493956800000L, iterator.endTime().msEpoch());
////    ts = ts_start;
////    count = 0;
////    while (iterator.status() == IteratorStatus.HAS_DATA) {
////      TimeSeriesValue<NumericType> v = iterator.next();
////      assertEquals(ts, v.timestamp().msEpoch());
////      assertEquals(ts, v.value().longValue());
////        ts += 300000;
////      ++count;
////    }
////    assertEquals(49, count);
////    
////    verify(this.executor, times(2)).executeQuery(eq(context), 
////        any(TimeSeriesQuery.class), any(Span.class));
////    verify(plugin, times(1)).fetch(eq(context), any(byte[][].class), any(Span.class));
////    verify(plugin, times(2)).cache(any(byte[][].class), any(byte[][].class), 
////        any(long[].class), any(TimeUnit.class));
////    assertFalse(executor.outstandingRequests().contains(exec));
////    assertEquals(2, downstreams.size());
////    assertFalse(cache_execution.cancelled);
////    assertEquals(2, cache.size());
////    
////    final byte[][] keys = key_generator.generate(query, planner.getTimeRanges());
////    assertTrue(cache.containsKey(keys[0]));
////    assertFalse(cache.containsKey(keys[1]));
////    assertTrue(cache.containsKey(keys[2]));
////    assertFalse(cache.containsKey(keys[3]));
////  }
////  
////  @SuppressWarnings("unchecked")
////  @Test
////  public void executeCacheHitMissHitMiss() throws Exception {
////    final TimeSlicedCachingExecutor<IteratorGroups> executor = 
////        new TimeSlicedCachingExecutor<IteratorGroups>(node);
////    final QueryExecution<IteratorGroups> exec = 
////        executor.executeQuery(context, query, span);
////    try {
////      exec.deferred().join(1);
////      fail("Expected TimeoutException");
////    } catch (TimeoutException e) { }
////    verify(plugin, times(1))
////      .fetch(any(QueryContext.class), any(byte[][].class), any(Span.class));
////    verify(this.executor, never()).executeQuery(context, query, null);
////    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
////        anyLong(), any(TimeUnit.class));
////    assertTrue(executor.outstandingRequests().contains(exec));
////    
////    // cache hit
////    final byte[][] cache_results = new byte[4][];
////    List<IteratorGroups> slices = planner.sliceResult(
////        IteratorTestUtils.generateData(ts_start, 1493946000000L, 0, 300000), 0, 0);
////    ByteArrayOutputStream output = new ByteArrayOutputStream();
////    serdes.serialize(query, null, output, slices.get(0));
////    output.close();
////    cache_results[0] = output.toByteArray();
////    
////    slices = planner.sliceResult(
////        IteratorTestUtils.generateData(1493949600000L, 1493953200000L, 0, 300000), 2, 2);
////    output = new ByteArrayOutputStream();
////    serdes.serialize(query, null, output, slices.get(0));
////    output.close();
////    cache_results[2] = output.toByteArray();
////    
////    cache_execution.callback(cache_results);
////
////    try {
////      exec.deferred().join(1);
////      fail("Expected TimeoutException");
////    } catch (TimeoutException e) { }
////    
////    assertEquals(2, downstreams.size());
////    
////    downstreams.get(0).callback(
////        IteratorTestUtils.generateData(1493946000000L, 1493949600000L, 0, 300000));
////    
////    try {
////      exec.deferred().join(1);
////      fail("Expected TimeoutException");
////    } catch (TimeoutException e) { }
////    
////    downstreams.get(1).callback(
////        IteratorTestUtils.generateData(1493953200000L, ts_end, 1, 300000));
////    
////    final IteratorGroups results = exec.deferred().join(1);
////    assertEquals(4, results.flattenedIterators().size());
////    
////    // validate data returned
////    IteratorGroup group = results.group(IteratorTestUtils.GROUP_A);
////    assertEquals(2, group.flattenedIterators().size());
////    assertEquals(IteratorTestUtils.ID_A, group.flattenedIterators().get(0).id());
////    assertEquals(IteratorTestUtils.ID_B, group.flattenedIterators().get(1).id());
////    
////    TimeSeriesIterator<NumericType> iterator = 
////        (TimeSeriesIterator<NumericType>) group.flattenedIterators().get(0);
////    assertEquals(1493942400000L, iterator.startTime().msEpoch());
////    assertEquals(1493956800000L, iterator.endTime().msEpoch());
////    long ts = ts_start;
////    int count = 0;
////    while (iterator.status() == IteratorStatus.HAS_DATA) {
////      TimeSeriesValue<NumericType> v = iterator.next();
////      assertEquals(ts, v.timestamp().msEpoch());
////      assertEquals(ts, v.value().longValue());
////        ts += 300000;
////      ++count;
////    }
////    assertEquals(49, count);
////    
////    iterator = 
////        (TimeSeriesIterator<NumericType>) group.flattenedIterators().get(1);
////    assertEquals(1493942400000L, iterator.startTime().msEpoch());
////    assertEquals(1493956800000L, iterator.endTime().msEpoch());
////    ts = ts_start;
////    count = 0;
////    while (iterator.status() == IteratorStatus.HAS_DATA) {
////      TimeSeriesValue<NumericType> v = iterator.next();
////      assertEquals(ts, v.timestamp().msEpoch());
////      assertEquals(ts, v.value().longValue());
////        ts += 300000;
////      ++count;
////    }
////    assertEquals(49, count);
////    
////    group = results.group(IteratorTestUtils.GROUP_B);
////    assertEquals(2, group.flattenedIterators().size());
////    assertEquals(IteratorTestUtils.ID_A, group.flattenedIterators().get(0).id());
////    assertEquals(IteratorTestUtils.ID_B, group.flattenedIterators().get(1).id());
////    
////    iterator = 
////        (TimeSeriesIterator<NumericType>) group.flattenedIterators().get(0);
////    assertEquals(1493942400000L, iterator.startTime().msEpoch());
////    assertEquals(1493956800000L, iterator.endTime().msEpoch());
////    ts = ts_start;
////    count = 0;
////    while (iterator.status() == IteratorStatus.HAS_DATA) {
////      TimeSeriesValue<NumericType> v = iterator.next();
////      assertEquals(ts, v.timestamp().msEpoch());
////      assertEquals(ts, v.value().longValue());
////        ts += 300000;
////      ++count;
////    }
////    assertEquals(49, count);
////    
////    iterator = 
////        (TimeSeriesIterator<NumericType>) group.flattenedIterators().get(1);
////    assertEquals(1493942400000L, iterator.startTime().msEpoch());
////    assertEquals(1493956800000L, iterator.endTime().msEpoch());
////    ts = ts_start;
////    count = 0;
////    while (iterator.status() == IteratorStatus.HAS_DATA) {
////      TimeSeriesValue<NumericType> v = iterator.next();
////      assertEquals(ts, v.timestamp().msEpoch());
////      assertEquals(ts, v.value().longValue());
////        ts += 300000;
////      ++count;
////    }
////    assertEquals(49, count);
////    
////    verify(this.executor, times(2)).executeQuery(eq(context), 
////        any(TimeSeriesQuery.class), any(Span.class));
////    verify(plugin, times(1)).fetch(eq(context), any(byte[][].class), any(Span.class));
////    verify(plugin, times(2)).cache(any(byte[][].class), any(byte[][].class), 
////        any(long[].class), any(TimeUnit.class));
////    assertFalse(executor.outstandingRequests().contains(exec));
////    assertEquals(2, downstreams.size());
////    assertFalse(cache_execution.cancelled);
////    assertEquals(2, cache.size());
////    
////    final byte[][] keys = key_generator.generate(query, planner.getTimeRanges());
////    assertFalse(cache.containsKey(keys[0]));
////    assertTrue(cache.containsKey(keys[1]));
////    assertFalse(cache.containsKey(keys[2]));
////    assertTrue(cache.containsKey(keys[3]));
////  }
////
////  @Test
////  public void executeCacheExceptionThrow() throws Exception {
////    plugin = mock(QueryCachePlugin.class);
////    when(registry.getPlugin(eq(QueryCachePlugin.class), anyString()))
////      .thenReturn(plugin);
////    when(registry.getPlugin(eq(TimeSeriesCacheKeyGenerator.class), anyString()))
////      .thenReturn(key_generator);
////    when(plugin.fetch(any(QueryContext.class), any(byte[][].class), any(Span.class)))
////      .thenThrow(new IllegalArgumentException("Boo!"));
////    
////    final TimeSlicedCachingExecutor<IteratorGroups> executor = 
////        new TimeSlicedCachingExecutor<IteratorGroups>(node);
////    final QueryExecution<IteratorGroups> exec = 
////        executor.executeQuery(context, query, span);
////    
////    try {
////      exec.deferred().join(1);
////      fail("Expected TimeoutException");
////    } catch (TimeoutException e) { }
////    
////    final IteratorGroups data = 
////        IteratorTestUtils.generateData(ts_start, ts_end, 0, 300000);
////    downstreams.get(0).callback(data);
////    final IteratorGroups results = exec.deferred().join(1);
////    assertEquals(4, results.flattenedIterators().size());
////    
////    verify(this.executor, times(1)).executeQuery(eq(context), 
////        any(TimeSeriesQuery.class), any(Span.class));
////    verify(plugin, times(1)).fetch(eq(context), any(byte[][].class), any(Span.class));
////    verify(plugin, times(1)).cache(any(byte[][].class), any(byte[][].class), 
////        any(long[].class), any(TimeUnit.class));
////    assertFalse(executor.outstandingRequests().contains(exec));
////    assertFalse(downstreams.get(0).cancelled);
////    assertFalse(cache_execution.cancelled);
////  }
////  
////  @Test
////  public void executeCacheExceptionReturned() throws Exception {
////    final TimeSlicedCachingExecutor<IteratorGroups> executor = 
////        new TimeSlicedCachingExecutor<IteratorGroups>(node);
////    final QueryExecution<IteratorGroups> exec = 
////        executor.executeQuery(context, query, span);
////    
////    cache_execution.callback(new IllegalStateException("Boo!"));
////    
////    try {
////      exec.deferred().join(1);
////      fail("Expected TimeoutException");
////    } catch (TimeoutException e) { }
////    
////    final IteratorGroups data = 
////        IteratorTestUtils.generateData(ts_start, ts_end, 0, 300000);
////    downstreams.get(0).callback(data);
////    final IteratorGroups results = exec.deferred().join(1);
////    assertEquals(4, results.flattenedIterators().size());
////    
////    verify(this.executor, times(1)).executeQuery(eq(context), 
////        any(TimeSeriesQuery.class), any(Span.class));
////    verify(plugin, times(1)).fetch(eq(context), any(byte[][].class), any(Span.class));
////    verify(plugin, times(1)).cache(any(byte[][].class), any(byte[][].class), 
////        any(long[].class), any(TimeUnit.class));
////    assertFalse(executor.outstandingRequests().contains(exec));
////    assertFalse(downstreams.get(0).cancelled);
////    assertFalse(cache_execution.cancelled);
////  }
////  
////  @Test
////  public void executeCacheReturnedNull() throws Exception {
////    final TimeSlicedCachingExecutor<IteratorGroups> executor = 
////        new TimeSlicedCachingExecutor<IteratorGroups>(node);
////    final QueryExecution<IteratorGroups> exec = 
////        executor.executeQuery(context, query, span);
////    
////    cache_execution.callback(null);
////    
////    try {
////      exec.deferred().join(1);
////      fail("Expected TimeoutException");
////    } catch (TimeoutException e) { }
////    
////    final IteratorGroups data = 
////        IteratorTestUtils.generateData(ts_start, ts_end, 0, 300000);
////    downstreams.get(0).callback(data);
////    final IteratorGroups results = exec.deferred().join(1);
////    assertEquals(4, results.flattenedIterators().size());
////    
////    verify(this.executor, times(1)).executeQuery(eq(context), 
////        any(TimeSeriesQuery.class), any(Span.class));
////    verify(plugin, times(1)).fetch(eq(context), any(byte[][].class), any(Span.class));
////    verify(plugin, times(1)).cache(any(byte[][].class), any(byte[][].class), 
////        any(long[].class), any(TimeUnit.class));
////    assertFalse(executor.outstandingRequests().contains(exec));
////    assertFalse(downstreams.get(0).cancelled);
////    assertFalse(cache_execution.cancelled);
////  }
////
////  @SuppressWarnings("unchecked")
////  @Test
////  public void executeCacheHitCorruptEntries() throws Exception {
////    final byte[][] cache_results = new byte[4][];
////    final List<IteratorGroups> slices = planner.sliceResult(
////        IteratorTestUtils.generateData(ts_start, ts_end, 0, 300000), 0, 3);
////    
////    serdes = mock(TimeSeriesSerdes.class);
////    when(registry.getSerdes(anyString()))
////      .thenAnswer(new Answer<TimeSeriesSerdes<IteratorGroups>>() {
////      @Override
////      public TimeSeriesSerdes<IteratorGroups> answer(
////          final InvocationOnMock invocation) throws Throwable {
////        return serdes;
////      }
////    });
////    when(serdes.deserialize(any(), any(InputStream.class)))
////      .thenReturn(slices.get(0))
////      .thenReturn(slices.get(1))
////      .thenThrow(new IllegalStateException("Boo!"))
////      .thenReturn(slices.get(3));
////    
////    final TimeSlicedCachingExecutor<IteratorGroups> executor = 
////        new TimeSlicedCachingExecutor<IteratorGroups>(node);
////    final QueryExecution<IteratorGroups> exec = 
////        executor.executeQuery(context, query, span);
////    try {
////      exec.deferred().join(1);
////      fail("Expected TimeoutException");
////    } catch (TimeoutException e) { }
////    verify(plugin, times(1))
////      .fetch(any(QueryContext.class), any(byte[][].class), any(Span.class));
////    verify(this.executor, never()).executeQuery(context, query, null);
////    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
////        anyLong(), any(TimeUnit.class));
////    assertTrue(executor.outstandingRequests().contains(exec));
////    
////    for (int i = 0; i < slices.size(); i++) {
////      final ByteArrayOutputStream output = new ByteArrayOutputStream();
////      serdes.serialize(query, null, output, slices.get(i));
////      output.close();
////      cache_results[i] = output.toByteArray();
////    }
////    
////    cache_execution.callback(cache_results);
////    
////    try {
////      exec.deferred().join(1);
////      fail("Expected TimeoutException");
////    } catch (TimeoutException e) { }
////    
////    downstreams.get(0).callback(slices.get(2));
////
////    final IteratorGroups results = exec.deferred().join(1);
////    assertEquals(4, results.flattenedIterators().size());
////    
////    // validate data returned
////    IteratorGroup group = results.group(IteratorTestUtils.GROUP_A);
////    assertEquals(2, group.flattenedIterators().size());
////    assertEquals(IteratorTestUtils.ID_A, group.flattenedIterators().get(0).id());
////    assertEquals(IteratorTestUtils.ID_B, group.flattenedIterators().get(1).id());
////    
////    TimeSeriesIterator<NumericType> iterator = 
////        (TimeSeriesIterator<NumericType>) group.flattenedIterators().get(0);
////    assertEquals(1493942400000L, iterator.startTime().msEpoch());
////    assertEquals(1493956800000L, iterator.endTime().msEpoch());
////    long ts = ts_start;
////    int count = 0;
////    while (iterator.status() == IteratorStatus.HAS_DATA) {
////      TimeSeriesValue<NumericType> v = iterator.next();
////      assertEquals(ts, v.timestamp().msEpoch());
////      assertEquals(ts, v.value().longValue());
////        ts += 300000;
////      ++count;
////    }
////    assertEquals(49, count);
////    
////    iterator = 
////        (TimeSeriesIterator<NumericType>) group.flattenedIterators().get(1);
////    assertEquals(1493942400000L, iterator.startTime().msEpoch());
////    assertEquals(1493956800000L, iterator.endTime().msEpoch());
////    ts = ts_start;
////    count = 0;
////    while (iterator.status() == IteratorStatus.HAS_DATA) {
////      TimeSeriesValue<NumericType> v = iterator.next();
////      assertEquals(ts, v.timestamp().msEpoch());
////      assertEquals(ts, v.value().longValue());
////        ts += 300000;
////      ++count;
////    }
////    assertEquals(49, count);
////    
////    group = results.group(IteratorTestUtils.GROUP_B);
////    assertEquals(2, group.flattenedIterators().size());
////    assertEquals(IteratorTestUtils.ID_A, group.flattenedIterators().get(0).id());
////    assertEquals(IteratorTestUtils.ID_B, group.flattenedIterators().get(1).id());
////    
////    iterator = 
////        (TimeSeriesIterator<NumericType>) group.flattenedIterators().get(0);
////    assertEquals(1493942400000L, iterator.startTime().msEpoch());
////    assertEquals(1493956800000L, iterator.endTime().msEpoch());
////    ts = ts_start;
////    count = 0;
////    while (iterator.status() == IteratorStatus.HAS_DATA) {
////      TimeSeriesValue<NumericType> v = iterator.next();
////      assertEquals(ts, v.timestamp().msEpoch());
////      assertEquals(ts, v.value().longValue());
////        ts += 300000;
////      ++count;
////    }
////    assertEquals(49, count);
////    
////    iterator = 
////        (TimeSeriesIterator<NumericType>) group.flattenedIterators().get(1);
////    assertEquals(1493942400000L, iterator.startTime().msEpoch());
////    assertEquals(1493956800000L, iterator.endTime().msEpoch());
////    ts = ts_start;
////    count = 0;
////    while (iterator.status() == IteratorStatus.HAS_DATA) {
////      TimeSeriesValue<NumericType> v = iterator.next();
////      assertEquals(ts, v.timestamp().msEpoch());
////      assertEquals(ts, v.value().longValue());
////        ts += 300000;
////      ++count;
////    }
////    assertEquals(49, count);
////    
////    verify(this.executor, times(1)).executeQuery(eq(context), 
////        any(TimeSeriesQuery.class), any(Span.class));
////    verify(plugin, times(1)).fetch(eq(context), any(byte[][].class), any(Span.class));
////    verify(plugin, times(1)).cache(any(byte[][].class), any(byte[][].class), 
////        any(long[].class), any(TimeUnit.class));
////    assertFalse(executor.outstandingRequests().contains(exec));
////    assertEquals(1, downstreams.size());
////    assertFalse(cache_execution.cancelled);
////    assertEquals(1, cache.size());
////    
////    final byte[][] keys = key_generator.generate(query, planner.getTimeRanges());
////    assertFalse(cache.containsKey(keys[0]));
////    assertFalse(cache.containsKey(keys[1]));
////    assertTrue(cache.containsKey(keys[2]));
////    assertFalse(cache.containsKey(keys[3]));
////  }
////  
////  @Test
////  public void executeDownstreamThrowsException() throws Exception {
////    when(executor.executeQuery(eq(context), any(TimeSeriesQuery.class), any(Span.class)))
////      .thenThrow(new IllegalStateException("Boo!"));
////    
////    final TimeSlicedCachingExecutor<IteratorGroups> executor = 
////        new TimeSlicedCachingExecutor<IteratorGroups>(node);
////    final QueryExecution<IteratorGroups> exec = 
////        executor.executeQuery(context, query, span);
////    try {
////      exec.deferred().join(1);
////      fail("Expected TimeoutException");
////    } catch (TimeoutException e) { }
////    verify(plugin, times(1))
////      .fetch(any(QueryContext.class), any(byte[][].class), any(Span.class));
////    verify(this.executor, never()).executeQuery(context, query, null);
////    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
////        anyLong(), any(TimeUnit.class));
////    assertTrue(executor.outstandingRequests().contains(exec));
////    
////    // partial cache hit
////    final byte[][] cache_results = new byte[4][];
////    List<IteratorGroups> slices = planner.sliceResult(
////        IteratorTestUtils.generateData(1493946000000L, 1493949600000L, 0, 300000), 1, 1);
////    ByteArrayOutputStream output = new ByteArrayOutputStream();
////    serdes.serialize(query, null, output, slices.get(0));
////    output.close();
////    cache_results[1] = output.toByteArray();
////    
////    slices = planner.sliceResult(
////        IteratorTestUtils.generateData(1493953200000L, ts_end, 0, 300000), 3, 3);
////    output = new ByteArrayOutputStream();
////    serdes.serialize(query, null, output, slices.get(0));
////    output.close();
////    cache_results[3] = output.toByteArray();
////    
////    cache_execution.callback(cache_results);
////        
////    try {
////      exec.deferred().join(1);
////      fail("Expected QueryExecutionException");
////    } catch (QueryExecutionException e) { 
////      assertEquals("Boo!", e.getCause().getMessage());
////    }
////    
////    verify(this.executor, times(2)).executeQuery(eq(context), 
////        any(TimeSeriesQuery.class), any(Span.class));
////    verify(plugin, times(1)).fetch(eq(context), any(byte[][].class), any(Span.class));
////    verify(plugin, never()).cache(any(byte[][].class), any(byte[][].class), 
////        any(long[].class), any(TimeUnit.class));
////    assertFalse(executor.outstandingRequests().contains(exec));
////    assertTrue(downstreams.isEmpty());
////    assertFalse(cache_execution.cancelled);
////  }
////  
////  @Test
////  public void executeDownstreamReturnsException() throws Exception {
////    final TimeSlicedCachingExecutor<IteratorGroups> executor = 
////        new TimeSlicedCachingExecutor<IteratorGroups>(node);
////    final QueryExecution<IteratorGroups> exec = 
////        executor.executeQuery(context, query, span);
////    try {
////      exec.deferred().join(1);
////      fail("Expected TimeoutException");
////    } catch (TimeoutException e) { }
////    verify(plugin, times(1))
////      .fetch(any(QueryContext.class), any(byte[][].class), any(Span.class));
////    verify(this.executor, never()).executeQuery(context, query, null);
////    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
////        anyLong(), any(TimeUnit.class));
////    assertTrue(executor.outstandingRequests().contains(exec));
////    
////    // partial cache hit
////    final byte[][] cache_results = new byte[4][];
////    List<IteratorGroups> slices = planner.sliceResult(
////        IteratorTestUtils.generateData(1493946000000L, 1493949600000L, 0, 300000), 1, 1);
////    ByteArrayOutputStream output = new ByteArrayOutputStream();
////    serdes.serialize(query, null, output, slices.get(0));
////    output.close();
////    cache_results[1] = output.toByteArray();
////    
////    slices = planner.sliceResult(
////        IteratorTestUtils.generateData(1493953200000L, ts_end, 0, 300000), 3, 3);
////    output = new ByteArrayOutputStream();
////    serdes.serialize(query, null, output, slices.get(0));
////    output.close();
////    cache_results[3] = output.toByteArray();
////    
////    cache_execution.callback(cache_results);
////    
////    try {
////      exec.deferred().join(1);
////      fail("Expected TimeoutException");
////    } catch (TimeoutException e) { }
////    
////    final IllegalArgumentException ex = new IllegalArgumentException("Boo!");
////    downstreams.get(0).callback(ex);
////    
////    try {
////      exec.deferred().join(1);
////      fail("Expected IllegalArgumentException");
////    } catch (IllegalArgumentException e) { 
////      assertEquals("Boo!", e.getMessage());
////    }
////    
////    verify(this.executor, times(2)).executeQuery(eq(context), 
////        any(TimeSeriesQuery.class), any(Span.class));
////    verify(plugin, times(1)).fetch(eq(context), any(byte[][].class), any(Span.class));
////    verify(plugin, never()).cache(any(byte[][].class), any(byte[][].class), 
////        any(long[].class), any(TimeUnit.class));
////    assertFalse(executor.outstandingRequests().contains(exec));
////    assertEquals(2, downstreams.size());
////    assertFalse(downstreams.get(0).cancelled);
////    assertTrue(downstreams.get(1).cancelled);
////    assertFalse(cache_execution.cancelled);
////  }
////
////  @SuppressWarnings("unchecked")
////  @Test
////  public void executeMergeException() throws Exception {
////    when(planner.mergeSlicedResults(anyList()))
////      .thenThrow(new IllegalArgumentException("Boo!"));
////    
////    final TimeSlicedCachingExecutor<IteratorGroups> executor = 
////        new TimeSlicedCachingExecutor<IteratorGroups>(node);
////    final QueryExecution<IteratorGroups> exec = 
////        executor.executeQuery(context, query, span);
////    try {
////      exec.deferred().join(1);
////      fail("Expected TimeoutException");
////    } catch (TimeoutException e) { }
////    verify(plugin, times(1))
////      .fetch(any(QueryContext.class), any(byte[][].class), any(Span.class));
////    verify(this.executor, never()).executeQuery(context, query, null);
////    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
////        anyLong(), any(TimeUnit.class));
////    assertTrue(executor.outstandingRequests().contains(exec));
////    
////    // cache hit
////    final byte[][] cache_results = new byte[4][];
////    final List<IteratorGroups> slices = planner.sliceResult(
////        IteratorTestUtils.generateData(ts_start, ts_end, 0, 300000), 0, 3);
////    for (int i = 0; i < slices.size(); i++) {
////      final ByteArrayOutputStream output = new ByteArrayOutputStream();
////      serdes.serialize(query, null, output, slices.get(i));
////      output.close();
////      cache_results[i] = output.toByteArray();
////    }
////    cache_execution.callback(cache_results);
////
////    try {
////      exec.deferred().join(1);
////      fail("Expected QueryExecutionException");
////    } catch (QueryExecutionException e) { }
////  }
////  
////  @SuppressWarnings("unchecked")
////  @Test
////  public void executeCacheWriteException() throws Exception {
////    doThrow(new IllegalArgumentException("Boo!"))
////      .when(plugin).cache(any(byte[][].class), any(byte[][].class), 
////          any(long[].class), eq(TimeUnit.MILLISECONDS));
////    
////    final TimeSlicedCachingExecutor<IteratorGroups> executor = 
////        new TimeSlicedCachingExecutor<IteratorGroups>(node);
////    final QueryExecution<IteratorGroups> exec = 
////        executor.executeQuery(context, query, span);
////    try {
////      exec.deferred().join(1);
////      fail("Expected TimeoutException");
////    } catch (TimeoutException e) { }
////    verify(plugin, times(1))
////      .fetch(any(QueryContext.class), any(byte[][].class), any(Span.class));
////    verify(this.executor, never()).executeQuery(context, query, null);
////    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
////        anyLong(), any(TimeUnit.class));
////    assertTrue(executor.outstandingRequests().contains(exec));
////    
////    // cache miss
////    cache_execution.callback(new byte[4][]);
////    
////    try {
////      exec.deferred().join(1);
////      fail("Expected TimeoutException");
////    } catch (TimeoutException e) { }
////    
////    final IteratorGroups data = 
////        IteratorTestUtils.generateData(ts_start, ts_end, 0, 300000);
////    downstreams.get(0).callback(data);
////    final IteratorGroups results = exec.deferred().join(1);
////    assertEquals(4, results.flattenedIterators().size());
////    
////    // validate data returned
////    IteratorGroup group = results.group(IteratorTestUtils.GROUP_A);
////    assertEquals(2, group.flattenedIterators().size());
////    assertSame(IteratorTestUtils.ID_A, group.flattenedIterators().get(0).id());
////    assertSame(IteratorTestUtils.ID_B, group.flattenedIterators().get(1).id());
////    
////    TimeSeriesIterator<NumericType> iterator = 
////        (TimeSeriesIterator<NumericType>) group.flattenedIterators().get(0);
////    assertEquals(1493942400000L, iterator.startTime().msEpoch());
////    assertEquals(1493956800000L, iterator.endTime().msEpoch());
////    long ts = ts_start;
////    int count = 0;
////    while (iterator.status() == IteratorStatus.HAS_DATA) {
////      TimeSeriesValue<NumericType> v = iterator.next();
////      assertEquals(ts, v.timestamp().msEpoch());
////      assertEquals(ts, v.value().longValue());
////        ts += 300000;
////      ++count;
////    }
////    assertEquals(49, count);
////    
////    iterator = 
////        (TimeSeriesIterator<NumericType>) group.flattenedIterators().get(1);
////    assertEquals(1493942400000L, iterator.startTime().msEpoch());
////    assertEquals(1493956800000L, iterator.endTime().msEpoch());
////    ts = ts_start;
////    count = 0;
////    while (iterator.status() == IteratorStatus.HAS_DATA) {
////      TimeSeriesValue<NumericType> v = iterator.next();
////      assertEquals(ts, v.timestamp().msEpoch());
////      assertEquals(ts, v.value().longValue());
////        ts += 300000;
////      ++count;
////    }
////    assertEquals(49, count);
////    
////    group = results.group(IteratorTestUtils.GROUP_B);
////    assertEquals(2, group.flattenedIterators().size());
////    assertSame(IteratorTestUtils.ID_A, group.flattenedIterators().get(0).id());
////    assertSame(IteratorTestUtils.ID_B, group.flattenedIterators().get(1).id());
////    
////    iterator = 
////        (TimeSeriesIterator<NumericType>) group.flattenedIterators().get(0);
////    assertEquals(1493942400000L, iterator.startTime().msEpoch());
////    assertEquals(1493956800000L, iterator.endTime().msEpoch());
////    ts = ts_start;
////    count = 0;
////    while (iterator.status() == IteratorStatus.HAS_DATA) {
////      TimeSeriesValue<NumericType> v = iterator.next();
////      assertEquals(ts, v.timestamp().msEpoch());
////      assertEquals(ts, v.value().longValue());
////        ts += 300000;
////      ++count;
////    }
////    assertEquals(49, count);
////    
////    iterator = 
////        (TimeSeriesIterator<NumericType>) group.flattenedIterators().get(1);
////    assertEquals(1493942400000L, iterator.startTime().msEpoch());
////    assertEquals(1493956800000L, iterator.endTime().msEpoch());
////    ts = ts_start;
////    count = 0;
////    while (iterator.status() == IteratorStatus.HAS_DATA) {
////      TimeSeriesValue<NumericType> v = iterator.next();
////      assertEquals(ts, v.timestamp().msEpoch());
////      assertEquals(ts, v.value().longValue());
////        ts += 300000;
////      ++count;
////    }
////    assertEquals(49, count);
////    
////    verify(this.executor, times(1)).executeQuery(eq(context), 
////        any(TimeSeriesQuery.class), any(Span.class));
////    verify(plugin, times(1)).fetch(eq(context), any(byte[][].class), any(Span.class));
////    verify(plugin, times(1)).cache(any(byte[][].class), any(byte[][].class), 
////        any(long[].class), any(TimeUnit.class));
////    assertFalse(executor.outstandingRequests().contains(exec));
////    assertFalse(downstreams.get(0).cancelled);
////    assertFalse(cache_execution.cancelled);
////    assertEquals(0, cache.size());
////  }
////
////  @SuppressWarnings("unchecked")
////  @Test
////  public void executeBypassDefault() throws Exception {
////    config = (Config) Config.newBuilder()
////        .setExpiration(60000)
////        .setPlannerId("IteratorGroupsSlicePlanner")
////        .setSerdesId("Default")
////        .setBypass(true)
////        .setKeyGeneratorId("MyKeyGen")
////        .setExecutorId("LocalCache")
////        .setExecutorType("CachingQueryExecutor")
////        .build();
////    when(node.getDefaultConfig()).thenReturn(config);
////    final TimeSlicedCachingExecutor<IteratorGroups> executor = 
////        new TimeSlicedCachingExecutor<IteratorGroups>(node);
////    final QueryExecution<IteratorGroups> exec = 
////        executor.executeQuery(context, query, span);
////    try {
////      exec.deferred().join(1);
////      fail("Expected TimeoutException");
////    } catch (TimeoutException e) { }
////    verify(plugin, never())
////      .fetch(any(QueryContext.class), any(byte[][].class), any(Span.class));
////    verify(this.executor, times(1)).executeQuery(context, query, null);
////    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
////        anyLong(), any(TimeUnit.class));
////    assertTrue(executor.outstandingRequests().contains(exec));
////
////    final IteratorGroups data = 
////        IteratorTestUtils.generateData(ts_start, ts_end, 0, 300000);
////    downstreams.get(0).callback(data);
////    final IteratorGroups results = exec.deferred().join(1);
////    assertEquals(4, results.flattenedIterators().size());
////    
////    // validate data returned
////    IteratorGroup group = results.group(IteratorTestUtils.GROUP_A);
////    assertEquals(2, group.flattenedIterators().size());
////    assertSame(IteratorTestUtils.ID_A, group.flattenedIterators().get(0).id());
////    assertSame(IteratorTestUtils.ID_B, group.flattenedIterators().get(1).id());
////    
////    TimeSeriesIterator<NumericType> iterator = 
////        (TimeSeriesIterator<NumericType>) group.flattenedIterators().get(0);
////    assertEquals(1493942400000L, iterator.startTime().msEpoch());
////    assertEquals(1493956800000L, iterator.endTime().msEpoch());
////    long ts = ts_start;
////    int count = 0;
////    while (iterator.status() == IteratorStatus.HAS_DATA) {
////      TimeSeriesValue<NumericType> v = iterator.next();
////      assertEquals(ts, v.timestamp().msEpoch());
////      assertEquals(ts, v.value().longValue());
////        ts += 300000;
////      ++count;
////    }
////    assertEquals(49, count);
////    
////    iterator = 
////        (TimeSeriesIterator<NumericType>) group.flattenedIterators().get(1);
////    assertEquals(1493942400000L, iterator.startTime().msEpoch());
////    assertEquals(1493956800000L, iterator.endTime().msEpoch());
////    ts = ts_start;
////    count = 0;
////    while (iterator.status() == IteratorStatus.HAS_DATA) {
////      TimeSeriesValue<NumericType> v = iterator.next();
////      assertEquals(ts, v.timestamp().msEpoch());
////      assertEquals(ts, v.value().longValue());
////        ts += 300000;
////      ++count;
////    }
////    assertEquals(49, count);
////    
////    group = results.group(IteratorTestUtils.GROUP_B);
////    assertEquals(2, group.flattenedIterators().size());
////    assertSame(IteratorTestUtils.ID_A, group.flattenedIterators().get(0).id());
////    assertSame(IteratorTestUtils.ID_B, group.flattenedIterators().get(1).id());
////    
////    iterator = 
////        (TimeSeriesIterator<NumericType>) group.flattenedIterators().get(0);
////    assertEquals(1493942400000L, iterator.startTime().msEpoch());
////    assertEquals(1493956800000L, iterator.endTime().msEpoch());
////    ts = ts_start;
////    count = 0;
////    while (iterator.status() == IteratorStatus.HAS_DATA) {
////      TimeSeriesValue<NumericType> v = iterator.next();
////      assertEquals(ts, v.timestamp().msEpoch());
////      assertEquals(ts, v.value().longValue());
////        ts += 300000;
////      ++count;
////    }
////    assertEquals(49, count);
////    
////    iterator = 
////        (TimeSeriesIterator<NumericType>) group.flattenedIterators().get(1);
////    assertEquals(1493942400000L, iterator.startTime().msEpoch());
////    assertEquals(1493956800000L, iterator.endTime().msEpoch());
////    ts = ts_start;
////    count = 0;
////    while (iterator.status() == IteratorStatus.HAS_DATA) {
////      TimeSeriesValue<NumericType> v = iterator.next();
////      assertEquals(ts, v.timestamp().msEpoch());
////      assertEquals(ts, v.value().longValue());
////        ts += 300000;
////      ++count;
////    }
////    assertEquals(49, count);
////    
////    verify(this.executor, times(1)).executeQuery(eq(context), 
////        any(TimeSeriesQuery.class), any(Span.class));
////    verify(plugin, never()).fetch(eq(context), any(byte[][].class), any(Span.class));
////    verify(plugin, never()).cache(any(byte[][].class), any(byte[][].class), 
////        any(long[].class), any(TimeUnit.class));
////    assertFalse(executor.outstandingRequests().contains(exec));
////    assertFalse(downstreams.get(0).cancelled);
////    assertFalse(cache_execution.cancelled);
////  }
////  
////  @SuppressWarnings("unchecked")
////  @Test
////  public void executeBypassOverride() throws Exception {
////    config = (Config) Config.newBuilder()
////        .setExpiration(60000)
////        .setPlannerId("IteratorGroupsSlicePlanner")
////        .setSerdesId("Default")
////        .setBypass(true)
////        .setKeyGeneratorId("MyKeyGen")
////        .setExecutorId("LocalCache")
////        .setExecutorType("CachingQueryExecutor")
////        .build();
////    when(context.getConfigOverride(anyString())).thenReturn(config);
////    final TimeSlicedCachingExecutor<IteratorGroups> executor = 
////        new TimeSlicedCachingExecutor<IteratorGroups>(node);
////    final QueryExecution<IteratorGroups> exec = 
////        executor.executeQuery(context, query, span);
////    try {
////      exec.deferred().join(1);
////      fail("Expected TimeoutException");
////    } catch (TimeoutException e) { }
////    verify(plugin, never())
////      .fetch(any(QueryContext.class), any(byte[][].class), any(Span.class));
////    verify(this.executor, times(1)).executeQuery(context, query, null);
////    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
////        anyLong(), any(TimeUnit.class));
////    assertTrue(executor.outstandingRequests().contains(exec));
////
////    final IteratorGroups data = 
////        IteratorTestUtils.generateData(ts_start, ts_end, 0, 300000);
////    downstreams.get(0).callback(data);
////    final IteratorGroups results = exec.deferred().join(1);
////    assertEquals(4, results.flattenedIterators().size());
////    
////    // validate data returned
////    IteratorGroup group = results.group(IteratorTestUtils.GROUP_A);
////    assertEquals(2, group.flattenedIterators().size());
////    assertSame(IteratorTestUtils.ID_A, group.flattenedIterators().get(0).id());
////    assertSame(IteratorTestUtils.ID_B, group.flattenedIterators().get(1).id());
////    
////    TimeSeriesIterator<NumericType> iterator = 
////        (TimeSeriesIterator<NumericType>) group.flattenedIterators().get(0);
////    assertEquals(1493942400000L, iterator.startTime().msEpoch());
////    assertEquals(1493956800000L, iterator.endTime().msEpoch());
////    long ts = ts_start;
////    int count = 0;
////    while (iterator.status() == IteratorStatus.HAS_DATA) {
////      TimeSeriesValue<NumericType> v = iterator.next();
////      assertEquals(ts, v.timestamp().msEpoch());
////      assertEquals(ts, v.value().longValue());
////        ts += 300000;
////      ++count;
////    }
////    assertEquals(49, count);
////    
////    iterator = 
////        (TimeSeriesIterator<NumericType>) group.flattenedIterators().get(1);
////    assertEquals(1493942400000L, iterator.startTime().msEpoch());
////    assertEquals(1493956800000L, iterator.endTime().msEpoch());
////    ts = ts_start;
////    count = 0;
////    while (iterator.status() == IteratorStatus.HAS_DATA) {
////      TimeSeriesValue<NumericType> v = iterator.next();
////      assertEquals(ts, v.timestamp().msEpoch());
////      assertEquals(ts, v.value().longValue());
////        ts += 300000;
////      ++count;
////    }
////    assertEquals(49, count);
////    
////    group = results.group(IteratorTestUtils.GROUP_B);
////    assertEquals(2, group.flattenedIterators().size());
////    assertSame(IteratorTestUtils.ID_A, group.flattenedIterators().get(0).id());
////    assertSame(IteratorTestUtils.ID_B, group.flattenedIterators().get(1).id());
////    
////    iterator = 
////        (TimeSeriesIterator<NumericType>) group.flattenedIterators().get(0);
////    assertEquals(1493942400000L, iterator.startTime().msEpoch());
////    assertEquals(1493956800000L, iterator.endTime().msEpoch());
////    ts = ts_start;
////    count = 0;
////    while (iterator.status() == IteratorStatus.HAS_DATA) {
////      TimeSeriesValue<NumericType> v = iterator.next();
////      assertEquals(ts, v.timestamp().msEpoch());
////      assertEquals(ts, v.value().longValue());
////        ts += 300000;
////      ++count;
////    }
////    assertEquals(49, count);
////    
////    iterator = 
////        (TimeSeriesIterator<NumericType>) group.flattenedIterators().get(1);
////    assertEquals(1493942400000L, iterator.startTime().msEpoch());
////    assertEquals(1493956800000L, iterator.endTime().msEpoch());
////    ts = ts_start;
////    count = 0;
////    while (iterator.status() == IteratorStatus.HAS_DATA) {
////      TimeSeriesValue<NumericType> v = iterator.next();
////      assertEquals(ts, v.timestamp().msEpoch());
////      assertEquals(ts, v.value().longValue());
////        ts += 300000;
////      ++count;
////    }
////    assertEquals(49, count);
////    
////    verify(this.executor, times(1)).executeQuery(eq(context), 
////        any(TimeSeriesQuery.class), any(Span.class));
////    verify(plugin, never()).fetch(eq(context), any(byte[][].class), any(Span.class));
////    verify(plugin, never()).cache(any(byte[][].class), any(byte[][].class), 
////        any(long[].class), any(TimeUnit.class));
////    assertFalse(executor.outstandingRequests().contains(exec));
////    assertFalse(downstreams.get(0).cancelled);
////    assertFalse(cache_execution.cancelled);
////  }
////  
////  @Test
////  public void executeCancel() throws Exception {
////    final TimeSlicedCachingExecutor<IteratorGroups> executor = 
////        new TimeSlicedCachingExecutor<IteratorGroups>(node);
////    final QueryExecution<IteratorGroups> exec = 
////        executor.executeQuery(context, query, span);
////    try {
////      exec.deferred().join(1);
////      fail("Expected TimeoutException");
////    } catch (TimeoutException e) { }
////    verify(plugin, times(1))
////      .fetch(any(QueryContext.class), any(byte[][].class), any(Span.class));
////    verify(this.executor, never()).executeQuery(context, query, null);
////    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
////        anyLong(), any(TimeUnit.class));
////    assertTrue(executor.outstandingRequests().contains(exec));
////    
////    // cache hit
////    final byte[][] cache_results = new byte[4][];
////    List<IteratorGroups> slices = planner.sliceResult(
////        IteratorTestUtils.generateData(1493946000000L, 1493949600000L, 0, 300000), 1, 1);
////    ByteArrayOutputStream output = new ByteArrayOutputStream();
////    serdes.serialize(query, null, output, slices.get(0));
////    output.close();
////    cache_results[1] = output.toByteArray();
////    
////    slices = planner.sliceResult(
////        IteratorTestUtils.generateData(1493953200000L, ts_end, 0, 300000), 3, 3);
////    output = new ByteArrayOutputStream();
////    serdes.serialize(query, null, output, slices.get(0));
////    output.close();
////    cache_results[3] = output.toByteArray();
////    
////    cache_execution.callback(cache_results);
////
////    try {
////      exec.deferred().join(1);
////      fail("Expected TimeoutException");
////    } catch (TimeoutException e) { }
////    
////    assertEquals(2, downstreams.size());
////    
////    exec.cancel();
////    
////    try {
////      exec.deferred().join(1);
////      fail("Expected QueryExecutionCanceled");
////    } catch (QueryExecutionCanceled e) { }
////    
////    verify(this.executor, times(2)).executeQuery(eq(context), 
////        any(TimeSeriesQuery.class), any(Span.class));
////    verify(plugin, times(1)).fetch(eq(context), any(byte[][].class), any(Span.class));
////    verify(plugin, never()).cache(any(byte[][].class), any(byte[][].class), 
////        any(long[].class), any(TimeUnit.class));
////    assertFalse(executor.outstandingRequests().contains(exec));
////    assertEquals(2, downstreams.size());
////    assertTrue(downstreams.get(0).cancelled);
////    assertTrue(downstreams.get(1).cancelled);
////    assertFalse(cache_execution.cancelled);
////    assertEquals(0, cache.size());
////  }
////  
////  @Test
////  public void executeClose() throws Exception {
////    final TimeSlicedCachingExecutor<IteratorGroups> executor = 
////        new TimeSlicedCachingExecutor<IteratorGroups>(node);
////    final QueryExecution<IteratorGroups> exec = 
////        executor.executeQuery(context, query, span);
////    try {
////      exec.deferred().join(1);
////      fail("Expected TimeoutException");
////    } catch (TimeoutException e) { }
////    verify(plugin, times(1))
////      .fetch(any(QueryContext.class), any(byte[][].class), any(Span.class));
////    verify(this.executor, never()).executeQuery(context, query, null);
////    verify(plugin, never()).cache(any(byte[].class), any(byte[].class), 
////        anyLong(), any(TimeUnit.class));
////    assertTrue(executor.outstandingRequests().contains(exec));
////    
////    // cache hit
////    final byte[][] cache_results = new byte[4][];
////    List<IteratorGroups> slices = planner.sliceResult(
////        IteratorTestUtils.generateData(1493946000000L, 1493949600000L, 0, 300000), 1, 1);
////    ByteArrayOutputStream output = new ByteArrayOutputStream();
////    serdes.serialize(query, null, output, slices.get(0));
////    output.close();
////    cache_results[1] = output.toByteArray();
////    
////    slices = planner.sliceResult(
////        IteratorTestUtils.generateData(1493953200000L, ts_end, 0, 300000), 3, 3);
////    output = new ByteArrayOutputStream();
////    serdes.serialize(query, null, output, slices.get(0));
////    output.close();
////    cache_results[3] = output.toByteArray();
////    
////    cache_execution.callback(cache_results);
////
////    try {
////      exec.deferred().join(1);
////      fail("Expected TimeoutException");
////    } catch (TimeoutException e) { }
////    
////    assertEquals(2, downstreams.size());
////    
////    assertNull(executor.close().join());
////    
////    try {
////      exec.deferred().join(1);
////      fail("Expected QueryExecutionCanceled");
////    } catch (QueryExecutionCanceled e) { }
////    
////    verify(this.executor, times(2)).executeQuery(eq(context), 
////        any(TimeSeriesQuery.class), any(Span.class));
////    verify(plugin, times(1)).fetch(eq(context), any(byte[][].class), any(Span.class));
////    verify(plugin, never()).cache(any(byte[][].class), any(byte[][].class), 
////        any(long[].class), any(TimeUnit.class));
////    assertFalse(executor.outstandingRequests().contains(exec));
////    assertEquals(2, downstreams.size());
////    assertTrue(downstreams.get(0).cancelled);
////    assertTrue(downstreams.get(1).cancelled);
////    assertFalse(cache_execution.cancelled);
////    assertEquals(0, cache.size());
////  }
////
////  @Test (expected = IllegalArgumentException.class)
////  public void executeNoFactoryPlan() throws Exception {
////    when(registry.getQueryPlanner(anyString())).thenReturn(null);
////    final TimeSlicedCachingExecutor<IteratorGroups> executor = 
////        new TimeSlicedCachingExecutor<IteratorGroups>(node);
////    executor.executeQuery(context, query, span);
////  }
////  
////  @Test (expected = IllegalStateException.class)
////  public void executeNullPlanner() throws Exception {
////    when(plan_factory.newPlanner(any(TimeSeriesQuery.class))).thenReturn(null);
////    final TimeSlicedCachingExecutor<IteratorGroups> executor = 
////        new TimeSlicedCachingExecutor<IteratorGroups>(node);
////    executor.executeQuery(context, query, span);
////  }
////  
////  @Test (expected = IllegalStateException.class)
////  public void executeWrongPlannerType() throws Exception {
////    final SplitMetricPlanner bad_plan = new SplitMetricPlanner(query);
////    final QueryPlannnerFactory<?> mock_factory = mock(QueryPlannnerFactory.class);
////    when(mock_factory.newPlanner(any(TimeSeriesQuery.class)))
////      .thenAnswer(new Answer<QueryPlanner<?>>() {
////        @Override
////        public QueryPlanner<?> answer(InvocationOnMock invocation) throws Throwable {
////          return bad_plan;
////        }
////      });
////    when(registry.getQueryPlanner(anyString()))
////      .thenAnswer(new Answer<QueryPlannnerFactory<?>>() {
////      @Override
////      public QueryPlannnerFactory<?> answer(InvocationOnMock invocation)
////          throws Throwable {
////        return mock_factory;
////      }
////    });
////      
////    final TimeSlicedCachingExecutor<IteratorGroups> executor = 
////        new TimeSlicedCachingExecutor<IteratorGroups>(node);
////    executor.executeQuery(context, query, span);
////  }
//
//  @Test
//  public void builder() throws Exception {
//    String json = JSON.serializeToString(config);
//    assertTrue(json.contains("\"executorType\":\"TimeSlicedCachingExecutor\""));
//    assertTrue(json.contains("\"serdesId\":\"Default\""));
//    assertTrue(json.contains("\"expiration\":60000"));
//    assertTrue(json.contains("\"executorId\":\"LocalCache\""));
//    assertTrue(json.contains("\"plannerId\":\"IteratorGroupsSlicePlanner\""));
//    assertTrue(json.contains("\"keyGeneratorId\":\"MyKeyGen\""));
//
//    json = "{\"executorType\":\"TimeSlicedCachingExecutor\",\"expiration\":"
//        + "60000,\"serdesId\":\"Default\",\"plannerId\":"
//        + "\"IteratorGroupsSlicePlanner\",\"keyGeneratorId\":\"MyKeyGen\","
//        + "\"bypass\":true,\"executorId\":\"LocalCache\"}";
//    config = JSON.parseToObject(json, Config.class);
//    assertEquals("TimeSlicedCachingExecutor", config.executorType());
//    assertEquals("LocalCache", config.getExecutorId());
//    assertEquals("Default", config.getSerdesId());
//    assertEquals(60000, config.getExpiration());
//    assertTrue(config.getBypass());
//    assertEquals("IteratorGroupsSlicePlanner", config.getPlannerId());
//    assertEquals("MyKeyGen", config.getKeyGeneratorId());
//  }
//  
//  @Test
//  public void hashCodeEqualsCompareTo() throws Exception {
//    final Config c1 = (Config) Config.newBuilder()
//        .setExpiration(60000)
//        .setBypass(true)
//        .setPlannerId("IteratorGroupsSlicePlanner")
//        .setSerdesId("Default")
//        .setKeyGeneratorId("MyKeyGen")
//        .setExecutorId("LocalCache")
//        .setExecutorType("CachingQueryExecutor")
//        .build();
//    
//    Config c2 = (Config) Config.newBuilder()
//        .setExpiration(60000)
//        .setBypass(true)
//        .setPlannerId("IteratorGroupsSlicePlanner")
//        .setSerdesId("Default")
//        .setKeyGeneratorId("MyKeyGen")
//        .setExecutorId("LocalCache")
//        .setExecutorType("CachingQueryExecutor")
//        .build();
//    assertEquals(c1.hashCode(), c2.hashCode());
//    assertEquals(c1, c2);
//    assertEquals(0, c1.compareTo(c2));
//    
//    c2 = (Config) Config.newBuilder()
//        .setExpiration(30000) // <-- Diff
//        .setBypass(true)
//        .setPlannerId("IteratorGroupsSlicePlanner")
//        .setSerdesId("Default")
//        .setKeyGeneratorId("MyKeyGen")
//        .setExecutorId("LocalCache")
//        .setExecutorType("CachingQueryExecutor")
//        .build();
//    assertNotEquals(c1.hashCode(), c2.hashCode());
//    assertNotEquals(c1, c2);
//    assertEquals(1, c1.compareTo(c2));
//    
//    c2 = (Config) Config.newBuilder()
//        .setExpiration(60000)
//        //.setBypass(true) // <-- Diff
//        .setPlannerId("IteratorGroupsSlicePlanner")
//        .setSerdesId("Default")
//        .setKeyGeneratorId("MyKeyGen")
//        .setExecutorId("LocalCache")
//        .setExecutorType("CachingQueryExecutor")
//        .build();
//    assertNotEquals(c1.hashCode(), c2.hashCode());
//    assertNotEquals(c1, c2);
//    assertEquals(-1, c1.compareTo(c2));
//    
//    c2 = (Config) Config.newBuilder()
//        .setExpiration(60000)
//        .setBypass(true)
//        .setPlannerId("IteratorGroupsSlicePlanner2") // <-- Diff
//        .setSerdesId("Default")
//        .setKeyGeneratorId("MyKeyGen")
//        .setExecutorId("LocalCache")
//        .setExecutorType("CachingQueryExecutor")
//        .build();
//    assertNotEquals(c1.hashCode(), c2.hashCode());
//    assertNotEquals(c1, c2);
//    assertEquals(-1, c1.compareTo(c2));
//    
//    c2 = (Config) Config.newBuilder()
//        .setExpiration(60000)
//        .setBypass(true)
//        .setPlannerId("IteratorGroupsSlicePlanner")
//        .setSerdesId("Somethingelse") // <-- Diff
//        .setKeyGeneratorId("MyKeyGen")
//        .setExecutorId("LocalCache")
//        .setExecutorType("CachingQueryExecutor")
//        .build();
//    assertNotEquals(c1.hashCode(), c2.hashCode());
//    assertNotEquals(c1, c2);
//    assertEquals(-1, c1.compareTo(c2));
//    
//    c2 = (Config) Config.newBuilder()
//        .setExpiration(60000)
//        .setBypass(true)
//        .setPlannerId("IteratorGroupsSlicePlanner")
//        .setSerdesId("Default")
//        .setKeyGeneratorId("MyKeyGen2") // <-- Diff
//        .setExecutorId("LocalCache")
//        .setExecutorType("CachingQueryExecutor")
//        .build();
//    assertNotEquals(c1.hashCode(), c2.hashCode());
//    assertNotEquals(c1, c2);
//    assertEquals(-1, c1.compareTo(c2));
//    
//    c2 = (Config) Config.newBuilder()
//        .setExpiration(60000)
//        .setBypass(true)
//        .setPlannerId("IteratorGroupsSlicePlanner")
//        .setSerdesId("Default")
//        //.setKeyGeneratorId("MyKeyGen") // <-- Diff
//        .setExecutorId("LocalCache")
//        .setExecutorType("CachingQueryExecutor")
//        .build();
//    assertNotEquals(c1.hashCode(), c2.hashCode());
//    assertNotEquals(c1, c2);
//    assertEquals(1, c1.compareTo(c2));
//    
//    c2 = (Config) Config.newBuilder()
//        .setExpiration(60000)
//        .setBypass(true)
//        .setPlannerId("IteratorGroupsSlicePlanner")
//        .setSerdesId("Default")
//        .setKeyGeneratorId("MyKeyGen")
//        .setExecutorId("LocalCache2") // <-- Diff
//        .setExecutorType("CachingQueryExecutor")
//        .build();
//    assertNotEquals(c1.hashCode(), c2.hashCode());
//    assertNotEquals(c1, c2);
//    assertEquals(-1, c1.compareTo(c2));
//    
//    c2 = (Config) Config.newBuilder()
//        .setExpiration(60000)
//        .setBypass(true)
//        .setPlannerId("IteratorGroupsSlicePlanner")
//        .setSerdesId("Default")
//        .setKeyGeneratorId("MyKeyGen")
//        .setExecutorId("LocalCache")
//        .setExecutorType("CachingQueryExecutor2") // <-- Diff
//        .build();
//    assertNotEquals(c1.hashCode(), c2.hashCode());
//    assertNotEquals(c1, c2);
//    assertEquals(-1, c1.compareTo(c2));
//  }
//}
