// This file is part of OpenTSDB.
// Copyright (C) 2017-2020  The OpenTSDB Authors.
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
package net.opentsdb.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.auth.AuthState;
import net.opentsdb.core.MockTSDB;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.data.TimeSeriesDataSourceFactory;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.TimeSeriesQuery.CacheMode;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.query.processor.downsample.DownsampleFactory;
import net.opentsdb.query.readcache.QueryReadCache;
import net.opentsdb.query.readcache.ReadCacheKeyGenerator;
import net.opentsdb.query.readcache.ReadCacheQueryResult;
import net.opentsdb.query.readcache.ReadCacheQueryResultSet;
import net.opentsdb.stats.QueryStats;
import net.opentsdb.stats.Span;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.UnitTestException;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ DateTime.class, ReadCacheQueryPipelineContext.class })
public class TestReadCacheQueryPipelineContext {

  private static MockTSDB TSDB;
  private static NumericInterpolatorConfig NUMERIC_CONFIG;
  private static TimeSeriesDataSourceFactory STORE_FACTORY;
  private static List<TimeSeriesDataSource> STORE_NODES;
  private static QuerySink SINK;
  private static QuerySinkConfig SINK_CONFIG;
  
  private TimeSeriesQuery query;
  private QuerySink sink;
  private QueryContext context;
  private QueryReadCache cache_plugin;
  private ReadCacheKeyGenerator keygen_plugin;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    TSDB = new MockTSDB();
    STORE_FACTORY = mock(TimeSeriesDataSourceFactory.class);
    STORE_NODES = Lists.newArrayList();
    SINK = mock(QuerySink.class);
    SINK_CONFIG = mock(QuerySinkConfig.class);
    
    NUMERIC_CONFIG = (NumericInterpolatorConfig) 
        NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
        .setDataType(NumericType.TYPE.toString())
        .build();
    
    DownsampleFactory factory = new DownsampleFactory();
    factory.initialize(TSDB, null).join();
    when(TSDB.registry.getQueryNodeFactory(anyString()))
      .thenReturn(factory);
    
    QuerySinkFactory sink_factory = mock(QuerySinkFactory.class);
    when(sink_factory.newSink(any(QueryContext.class), any(QuerySinkConfig.class)))
      .thenReturn(SINK);
    when(TSDB.registry.getPlugin(eq(QuerySinkFactory.class), anyString()))
      .thenReturn(sink_factory);
  }
  
  @Before
  public void before() throws Exception {
    TSDB.runnables.clear();
    
    sink = mock(QuerySink.class);
    context = mock(BaseQueryContext.class);
    cache_plugin = mock(QueryReadCache.class);
    keygen_plugin = mock(ReadCacheKeyGenerator.class);
    
    query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514786400")
        .addExecutionGraphNode(DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setId("m1")
            .build())
        .build();
    when(context.query()).thenReturn(query);
    when(context.tsdb()).thenReturn(TSDB);
    
    when(TSDB.getRegistry().getPlugin(eq(QueryReadCache.class), anyString()))
      .thenReturn(cache_plugin);
    when(TSDB.getRegistry().getPlugin(eq(ReadCacheKeyGenerator.class), anyString()))
      .thenReturn(keygen_plugin);
    
    when(keygen_plugin.generate(anyLong(), anyString(), any(int[].class), any(long[].class)))
      .thenAnswer(new Answer<byte[][]>() {
        @Override
        public byte[][] answer(InvocationOnMock invocation) throws Throwable {
          int[] slices = (int[]) invocation.getArguments()[2];
          byte[][] keys = new byte[slices.length][];
          for (int i = 0; i < slices.length; i++) {
            keys[i] = Bytes.fromInt(i);
          }
          return keys;
        }
      });
    
    PowerMockito.mockStatic(ReadCacheQueryPipelineContext.class);
    when(ReadCacheQueryPipelineContext.buildQuery(anyInt(), anyInt(), 
        any(QueryContext.class), any(QuerySink.class))).thenAnswer(
        new Answer<QueryContext>() {
          @Override
          public QueryContext answer(InvocationOnMock invocation)
              throws Throwable {
            final int start = (int) invocation.getArguments()[0];
            final QuerySink sink = (QuerySink) invocation.getArguments()[3];
            return new MockQueryContext(start, sink);
          }
    });
  }
  
  @Test
  public void ctor() throws Exception {
    ReadCacheQueryPipelineContext ctx = new ReadCacheQueryPipelineContext(context,
        Lists.newArrayList());
    assertEquals(0, ctx.sinks.size());
    
    ctx = new ReadCacheQueryPipelineContext(context, Lists.newArrayList(SINK));
    assertEquals(1, ctx.sinks.size());
    assertSame(SINK, ctx.sinks.get(0));
  }
  // TODO - we'll redo the cache in a little bit.
//  @Test
//  public void initializeNoDownsample() throws Exception {
//    ReadCacheQueryPipelineContext ctx = new ReadCacheQueryPipelineContext(context,
//        Lists.newArrayList());
//    ctx.initialize(null).join();
//    
//    assertSame(cache_plugin, ctx.cache);
//    assertSame(keygen_plugin, ctx.key_gen);
//    assertFalse(ctx.skip_cache);
//    assertEquals("1h", ctx.string_interval);
//    assertEquals(0, ctx.min_interval);
//    assertEquals(3600, ctx.interval_in_seconds);
//    assertEquals(6, ctx.slices.length);
//    int ts = 1514764800;
//    for (int i = 0; i < 6; i++) {
//      assertEquals(ts, ctx.slices[i]);
//      ts += ctx.interval_in_seconds;
//    }
//    verify(keygen_plugin, times(1)).generate(
//        ctx.hash(2147483647), "1h", ctx.slices, ctx.expirations);
//    assertEquals(0, ctx.sinks.size());
//  }
//  
//  @Test
//  public void initializeNoDownsampleOffsetQueryTimes() throws Exception {
//    setQuery(1514768087, 1514789687, null, false);
//    ReadCacheQueryPipelineContext ctx = new ReadCacheQueryPipelineContext(context,
//        Lists.newArrayList());
//    ctx.initialize(null).join();
//    
//    assertSame(cache_plugin, ctx.cache);
//    assertSame(keygen_plugin, ctx.key_gen);
//    assertFalse(ctx.skip_cache);
//    assertEquals("1h", ctx.string_interval);
//    assertEquals(0, ctx.min_interval);
//    assertEquals(3600, ctx.interval_in_seconds);
//    assertEquals(7, ctx.slices.length);
//    int ts = 1514764800;
//    for (int i = 0; i < 7; i++) {
//      assertEquals(ts, ctx.slices[i]);
//      ts += ctx.interval_in_seconds;
//    }
//    verify(keygen_plugin, times(1)).generate(
//        ctx.hash(2147483647), "1h", ctx.slices, ctx.expirations);
//  }
//  
//  @Test
//  public void initializeSingleDownsample1m() throws Exception {
//    setQuery(1514764800, 1514786400, "1m", false);
//    ReadCacheQueryPipelineContext ctx = new ReadCacheQueryPipelineContext(context,
//        Lists.newArrayList());
//    ctx.initialize(null).join();
//    
//    assertSame(cache_plugin, ctx.cache);
//    assertSame(keygen_plugin, ctx.key_gen);
//    assertFalse(ctx.skip_cache);
//    assertEquals("1h", ctx.string_interval);
//    assertEquals(60, ctx.min_interval);
//    assertEquals(3600, ctx.interval_in_seconds);
//    assertEquals(6, ctx.slices.length);
//    int ts = 1514764800;
//    for (int i = 0; i < 6; i++) {
//      assertEquals(ts, ctx.slices[i]);
//      ts += ctx.interval_in_seconds;
//    }
//    verify(keygen_plugin, times(1)).generate(
//        ctx.hash(60), "1h", ctx.slices, ctx.expirations);
//  }
//  
//  @Test
//  public void initializeSingleDownsample1h() throws Exception {
//    setQuery(1514764800, 1514786400, "1h", false);
//    ReadCacheQueryPipelineContext ctx = new ReadCacheQueryPipelineContext(context,
//        Lists.newArrayList());
//    ctx.initialize(null).join();
//    
//    assertSame(cache_plugin, ctx.cache);
//    assertSame(keygen_plugin, ctx.key_gen);
//    assertFalse(ctx.skip_cache);
//    assertEquals("1d", ctx.string_interval);
//    assertEquals(3600, ctx.min_interval);
//    assertEquals(86400, ctx.interval_in_seconds);
//    assertEquals(1, ctx.slices.length);
//    assertEquals(1514764800, ctx.slices[0]);
//    verify(keygen_plugin, times(1)).generate(
//        ctx.hash(3600), "1d", ctx.slices, ctx.expirations);
//  }
//  
//  @Test
//  public void initializeSingleDownsampleRunAllSmall() throws Exception {
//    setQuery(1514764800, 1514786400, "1m", true);
//    ReadCacheQueryPipelineContext ctx = new ReadCacheQueryPipelineContext(context,
//        Lists.newArrayList());
//    ctx.initialize(null).join();
//    
//    assertSame(cache_plugin, ctx.cache);
//    assertSame(keygen_plugin, ctx.key_gen);
//    assertTrue(ctx.skip_cache);
//    verify(keygen_plugin, never()).generate(
//        ctx.hash(60), "1h", ctx.slices, ctx.expirations);
//  }
//  
//  @Test
//  public void initializeSingleDownsampleRunAllBig() throws Exception {
//    setQuery(1514764800, 1514876087, "1m", true);
//    ReadCacheQueryPipelineContext ctx = new ReadCacheQueryPipelineContext(context,
//        Lists.newArrayList());
//    ctx.initialize(null).join();
//    
//    assertSame(cache_plugin, ctx.cache);
//    assertSame(keygen_plugin, ctx.key_gen);
//    assertTrue(ctx.skip_cache);
//    verify(keygen_plugin, never()).generate(
//        ctx.hash(60), "1h", ctx.slices, ctx.expirations);
//  }
//  
//  @Test
//  public void initializeSingleDownsampleAutoSmall() throws Exception {
//    setQuery(1514764800, 1514786400, "auto", false);
//    ReadCacheQueryPipelineContext ctx = new ReadCacheQueryPipelineContext(context,
//        Lists.newArrayList());
//    ctx.initialize(null).join();
//    
//    assertSame(cache_plugin, ctx.cache);
//    assertSame(keygen_plugin, ctx.key_gen);
//    assertFalse(ctx.skip_cache);
//    assertEquals("1h", ctx.string_interval);
//    assertEquals(60, ctx.min_interval);
//    assertEquals(3600, ctx.interval_in_seconds);
//    assertEquals(6, ctx.slices.length);
//    int ts = 1514764800;
//    for (int i = 0; i < 6; i++) {
//      assertEquals(ts, ctx.slices[i]);
//      ts += ctx.interval_in_seconds;
//    }
//    verify(keygen_plugin, times(1)).generate(
//        ctx.hash(60), "1h", ctx.slices, ctx.expirations);
//  }
//  
//  @Test
//  public void initializeSingleDownsampleAutoBig() throws Exception {
//    setQuery(1514764800, 1515048887, "auto", false);
//    ReadCacheQueryPipelineContext ctx = new ReadCacheQueryPipelineContext(context,
//        Lists.newArrayList());
//    ctx.initialize(null).join();
//    
//    assertSame(cache_plugin, ctx.cache);
//    assertSame(keygen_plugin, ctx.key_gen);
//    assertFalse(ctx.skip_cache);
//    assertEquals("1d", ctx.string_interval);
//    assertEquals(3600, ctx.min_interval);
//    assertEquals(86400, ctx.interval_in_seconds);
//    assertEquals(4, ctx.slices.length);
//    int ts = 1514764800;
//    for (int i = 0; i < 4; i++) {
//      assertEquals(ts, ctx.slices[i]);
//      ts += ctx.interval_in_seconds;
//    }
//    verify(keygen_plugin, times(1)).generate(
//        ctx.hash(3600), "1d", ctx.slices, ctx.expirations);
//  }
//  
//  @Test
//  public void initializeMultipleDownsamples() throws Exception {
//    SemanticQuery.Builder builder = SemanticQuery.newBuilder()
//        .setMode(QueryMode.SINGLE)
//        .setStart(Integer.toString(1514764800))
//        .setEnd(Integer.toString(1515048887))
//        .addExecutionGraphNode(DefaultTimeSeriesDataSourceConfig.newBuilder()
//            .setMetric(MetricLiteralFilter.newBuilder()
//                .setMetric("sys.cpu.user")
//                .build())
//            .setId("m1")
//            .build());
//      builder.addExecutionGraphNode(DownsampleConfig.newBuilder()
//          .setAggregator("sum")
//          .setInterval("1m")
//          .addInterpolatorConfig(NUMERIC_CONFIG)
//          .setRunAll(false)
//          .addSource("m1")
//          .setId("downsample")
//          .build());
//      builder.addExecutionGraphNode(DownsampleConfig.newBuilder()
//          .setAggregator("avg")
//          .setInterval("1h")
//          .addInterpolatorConfig(NUMERIC_CONFIG)
//          .setRunAll(false)
//          .addSource("m1")
//          .setId("ds2")
//          .build());
//    query = builder.build();
//    when(context.query()).thenReturn(query);
//    
//    ReadCacheQueryPipelineContext ctx = new ReadCacheQueryPipelineContext(context,
//        Lists.newArrayList());
//    ctx.initialize(null).join();
//    
//    assertSame(cache_plugin, ctx.cache);
//    assertSame(keygen_plugin, ctx.key_gen);
//    assertFalse(ctx.skip_cache);
//    assertEquals("1d", ctx.string_interval);
//    assertEquals(60, ctx.min_interval);
//    assertEquals(86400, ctx.interval_in_seconds);
//    assertEquals(4, ctx.slices.length);
//    int ts = 1514764800;
//    for (int i = 0; i < 4; i++) {
//      assertEquals(ts, ctx.slices[i]);
//      ts += ctx.interval_in_seconds;
//    }
//    verify(keygen_plugin, times(1)).generate(
//        ctx.hash(60), "1d", ctx.slices, ctx.expirations);
//  }
//  
//  @Test
//  public void initializeMultipleDownsamplesWithRunall() throws Exception {
//    SemanticQuery.Builder builder = SemanticQuery.newBuilder()
//        .setMode(QueryMode.SINGLE)
//        .setStart(Integer.toString(1514764800))
//        .setEnd(Integer.toString(1515048887))
//        .addExecutionGraphNode(DefaultTimeSeriesDataSourceConfig.newBuilder()
//            .setMetric(MetricLiteralFilter.newBuilder()
//                .setMetric("sys.cpu.user")
//                .build())
//            .setId("m1")
//            .build());
//      builder.addExecutionGraphNode(DownsampleConfig.newBuilder()
//          .setAggregator("sum")
//          .setInterval("1m")
//          .addInterpolatorConfig(NUMERIC_CONFIG)
//          .setRunAll(false)
//          .addSource("m1")
//          .setId("downsample")
//          .build());
//      builder.addExecutionGraphNode(DownsampleConfig.newBuilder()
//          .setAggregator("avg")
//          .setInterval("0all")
//          .addInterpolatorConfig(NUMERIC_CONFIG)
//          .setRunAll(true)
//          .addSource("m1")
//          .setId("ds2")
//          .build());
//    query = builder.build();
//    when(context.query()).thenReturn(query);
//    
//    ReadCacheQueryPipelineContext ctx = new ReadCacheQueryPipelineContext(context,
//        Lists.newArrayList());
//    ctx.initialize(null).join();
//    
//    assertSame(cache_plugin, ctx.cache);
//    assertSame(keygen_plugin, ctx.key_gen);
//    assertTrue(ctx.skip_cache);
//    verify(keygen_plugin, never()).generate(
//        ctx.hash(60), "1h", ctx.slices, ctx.expirations);
//  }
//  
//  @Test
//  public void initializeSinkFromConfig() throws Exception {
//    SemanticQuery.Builder builder = SemanticQuery.newBuilder()
//        .setMode(QueryMode.SINGLE)
//        .setStart("1514764800")
//        .setEnd("1514786400")
//        .addExecutionGraphNode(DefaultTimeSeriesDataSourceConfig.newBuilder()
//            .setMetric(MetricLiteralFilter.newBuilder()
//                .setMetric("sys.cpu.user")
//                .build())
//            .setId("m1")
//            .build());
//      builder.addExecutionGraphNode(DownsampleConfig.newBuilder()
//          .setAggregator("sum")
//          .setInterval("1m")
//          .addInterpolatorConfig(NUMERIC_CONFIG)
//          .setRunAll(false)
//          .addSource("m1")
//          .setId("downsample")
//          .build());
//    query = builder.build();
//    when(context.query()).thenReturn(query);
//    when(context.sinkConfigs()).thenReturn(Lists.newArrayList(SINK_CONFIG));
//    
//    ReadCacheQueryPipelineContext ctx = new ReadCacheQueryPipelineContext(context,
//        Lists.newArrayList());
//    ctx.initialize(null).join();
//    
//    assertSame(cache_plugin, ctx.cache);
//    assertSame(keygen_plugin, ctx.key_gen);
//    assertFalse(ctx.skip_cache);
//    assertEquals("1h", ctx.string_interval);
//    assertEquals(3600, ctx.interval_in_seconds);
//    assertEquals(6, ctx.slices.length);
//    int ts = 1514764800;
//    for (int i = 0; i < 6; i++) {
//      assertEquals(ts, ctx.slices[i]);
//      ts += ctx.interval_in_seconds;
//    }
//    verify(keygen_plugin, times(1)).generate(
//        ctx.hash(60), "1h", ctx.slices, ctx.expirations);
//    assertEquals(1, ctx.sinks.size());
//    assertSame(SINK, ctx.sinks.get(0));
//  }
//  
//  @Test
//  public void fetchNext() throws Exception {
//    ReadCacheQueryPipelineContext ctx = new ReadCacheQueryPipelineContext(context,
//        Lists.newArrayList());
//    ctx.initialize(null).join();
//    ctx.fetchNext(null);
//    assertEquals(6, ctx.cache_latch.get());
//    verify(cache_plugin, times(1)).fetch(ctx, ctx.keys, ctx, null);
//  }
//  
//  @Test
//  public void onCacheError() throws Exception {
//    ReadCacheQueryPipelineContext ctx = new ReadCacheQueryPipelineContext(context,
//        Lists.newArrayList(SINK));
//    ctx.initialize(null).join();
//    ctx.onCacheError(-1, new UnitTestException());
//    ctx.onCacheError(-1, new UnitTestException());
//    verify(SINK, never()).onError(any(UnitTestException.class));
//    // TODO - test full query run.
//  }
//  
//  @Test
//  public void onCacheResultsGoodOld() throws Exception {
//    mockDateTime(1514851200000L);
//    ReadCacheQueryPipelineContext ctx = new ReadCacheQueryPipelineContext(context,
//        Lists.newArrayList(sink));
//    ctx.initialize(null).join();
//    ctx.fetchNext(null);
//    
//    assertEquals(1514851200000L, ctx.current_time);
//    assertEquals(6, ctx.cache_latch.get());
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 4));
//    assertEquals(5, ctx.cache_latch.get());
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 0));
//    assertEquals(4, ctx.cache_latch.get());
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 1));
//    assertEquals(3, ctx.cache_latch.get());
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 5));
//    assertEquals(2, ctx.cache_latch.get());
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 3));
//    assertEquals(1, ctx.cache_latch.get());
//    verify(sink, never()).onNext(any(QueryResult.class));
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 2));
//    assertEquals(0, ctx.cache_latch.get());
//    
//    verify(sink, times(1)).onNext(any(QueryResult.class));
//    assertTrue(TSDB.runnables.isEmpty());
//  }
//  
//  @Test
//  public void onCacheResultsGoodOneTipNotLast() throws Exception {
//    setQuery(1514765700, 1514787300, "5m", false);
//    mockDateTime(1514787360000L);
//    ReadCacheQueryPipelineContext ctx = spy(new ReadCacheQueryPipelineContext(context,
//        Lists.newArrayList(sink)));
//    ctx.initialize(null).join();
//    ctx.fetchNext(null);
//    assertEquals(7, ctx.results.length);
//    
//    assertEquals(1514787360000L, ctx.current_time);
//    assertEquals(7, ctx.cache_latch.get());
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 4));
//    assertEquals(6, ctx.cache_latch.get());
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 0));
//    assertEquals(5, ctx.cache_latch.get());
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 1));
//    assertEquals(4, ctx.cache_latch.get());
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 6, 1514787300));
//    assertEquals(3, ctx.cache_latch.get());
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 3));
//    assertEquals(2, ctx.cache_latch.get());
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 5));
//    assertEquals(1, ctx.cache_latch.get());
//    verify(sink, never()).onNext(any(QueryResult.class));
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 2));
//    assertEquals(0, ctx.cache_latch.get()); 
//    
//    verify(sink, never()).onNext(any(QueryResult.class));
//    assertTrue(TSDB.runnables.isEmpty());
//    
//    assertTrue(((MockQueryContext) ctx.results[6].sub_context).initialized);
//    assertTrue(((MockQueryContext) ctx.results[6].sub_context).fetched);
//    ctx.results[6].onNext(mockResult(new DefaultQueryResultId("m1", "m1")));
//    ctx.results[6].onComplete();
//    
//    assertEquals(0, ctx.cache_latch.get());
//    verify(sink, times(1)).onNext(any(QueryResult.class));
//    assertFalse(TSDB.runnables.isEmpty()); // cached!
//  }
//  
//  @Test
//  public void onCacheResultsGoodOneTipLast() throws Exception {
//    setQuery(1514765700, 1514787300, "5m", false);
//    mockDateTime(1514787360000L);
//    ReadCacheQueryPipelineContext ctx = spy(new ReadCacheQueryPipelineContext(context,
//        Lists.newArrayList(sink)));
//    ctx.initialize(null).join();
//    ctx.fetchNext(null);
//    assertEquals(7, ctx.results.length);
//    
//    assertEquals(1514787360000L, ctx.current_time);
//    assertEquals(7, ctx.cache_latch.get());
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 4));
//    assertEquals(6, ctx.cache_latch.get());
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 0));
//    assertEquals(5, ctx.cache_latch.get());
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 1));
//    assertEquals(4, ctx.cache_latch.get());
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 3));
//    assertEquals(3, ctx.cache_latch.get());
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 5));
//    assertEquals(2, ctx.cache_latch.get());
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 2));
//    assertEquals(1, ctx.cache_latch.get());
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 6, 1514787300));
//    assertEquals(0, ctx.cache_latch.get());
//    
//    verify(sink, never()).onNext(any(QueryResult.class));
//    assertTrue(TSDB.runnables.isEmpty());
//    
//    assertTrue(((MockQueryContext) ctx.results[6].sub_context).initialized);
//    assertTrue(((MockQueryContext) ctx.results[6].sub_context).fetched);
//    ctx.results[6].onNext(mockResult(new DefaultQueryResultId("m1", "m1")));
//    ctx.results[6].onComplete();
//    
//    assertEquals(0, ctx.cache_latch.get());
//    verify(sink, times(1)).onNext(any(QueryResult.class));
//    assertFalse(TSDB.runnables.isEmpty()); // cached!
//  }
//  
//  @Test
//  public void onCacheResultsGoodOneTipLastReadOnly() throws Exception {
//    setQuery(1514765700, 1514787300, "5m", false, CacheMode.READONLY);
//    mockDateTime(1514787360000L);
//    ReadCacheQueryPipelineContext ctx = spy(new ReadCacheQueryPipelineContext(context,
//        Lists.newArrayList(sink)));
//    ctx.initialize(null).join();
//    ctx.fetchNext(null);
//    assertEquals(7, ctx.results.length);
//    
//    assertEquals(1514787360000L, ctx.current_time);
//    assertEquals(7, ctx.cache_latch.get());
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 4));
//    assertEquals(6, ctx.cache_latch.get());
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 0));
//    assertEquals(5, ctx.cache_latch.get());
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 1));
//    assertEquals(4, ctx.cache_latch.get());
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 3));
//    assertEquals(3, ctx.cache_latch.get());
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 5));
//    assertEquals(2, ctx.cache_latch.get());
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 2));
//    assertEquals(1, ctx.cache_latch.get());
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 6, 1514787300));
//    assertEquals(0, ctx.cache_latch.get());
//    
//    verify(sink, never()).onNext(any(QueryResult.class));
//    assertTrue(TSDB.runnables.isEmpty());
//    
//    assertTrue(((MockQueryContext) ctx.results[6].sub_context).initialized);
//    assertTrue(((MockQueryContext) ctx.results[6].sub_context).fetched);
//    ctx.results[6].onNext(mockResult(new DefaultQueryResultId("m1", "m1")));
//    ctx.results[6].onComplete();
//    
//    assertEquals(0, ctx.cache_latch.get());
//    verify(sink, times(1)).onNext(any(QueryResult.class));
//    assertTrue(TSDB.runnables.isEmpty()); // cached!
//  }
//  
//  @Test
//  public void onCacheResultsGoodTwoTipsNotLast() throws Exception {
//    setQuery(1514765700, 1514787300, "5m", false);
//    mockDateTime(1514786520000L);
//    ReadCacheQueryPipelineContext ctx = spy(new ReadCacheQueryPipelineContext(context,
//        Lists.newArrayList(sink)));
//    ctx.initialize(null).join();
//    ctx.fetchNext(null);
//    assertEquals(7, ctx.results.length);
//    
//    assertEquals(1514786520000L, ctx.current_time);
//    assertEquals(7, ctx.cache_latch.get());
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 4));
//    assertEquals(6, ctx.cache_latch.get());
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 0));
//    assertEquals(5, ctx.cache_latch.get());
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 1));
//    assertEquals(4, ctx.cache_latch.get());
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 6, 1514786460));
//    assertEquals(3, ctx.cache_latch.get());
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 3));
//    assertEquals(2, ctx.cache_latch.get());
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 5)); // tip so we fire another
//    assertEquals(1, ctx.cache_latch.get());
//    verify(sink, never()).onNext(any(QueryResult.class));
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 2));
//    assertEquals(0, ctx.cache_latch.get()); 
//    
//    verify(sink, never()).onNext(any(QueryResult.class));
//    assertTrue(TSDB.runnables.isEmpty());
//    
//    assertTrue(((MockQueryContext) ctx.results[5].sub_context).initialized);
//    assertTrue(((MockQueryContext) ctx.results[5].sub_context).fetched);
//    
//    // one result in
//    assertTrue(((MockQueryContext) ctx.results[6].sub_context).initialized);
//    assertTrue(((MockQueryContext) ctx.results[6].sub_context).fetched);
//    ctx.results[6].onNext(mockResult(new DefaultQueryResultId("m1", "m1")));
//    ctx.results[6].onComplete();
//    
//    assertEquals(0, ctx.cache_latch.get()); // #5 is left
//    verify(sink, never()).onNext(any(QueryResult.class));
//    assertTrue(TSDB.runnables.isEmpty());
//    
//    // last result came in
//    ctx.results[5].onNext(mockResult(new DefaultQueryResultId("m1", "m1")));
//    ctx.results[5].onComplete();
//    
//    assertEquals(0, ctx.cache_latch.get());
//    verify(sink, times(1)).onNext(any(QueryResult.class));
//    assertFalse(TSDB.runnables.isEmpty()); // cached!
//  }
//  
//  @Test
//  public void onCacheResultsGoodTwoTipsLast() throws Exception {
//    setQuery(1514765700, 1514787300, "5m", false);
//    mockDateTime(1514786520000L);
//    ReadCacheQueryPipelineContext ctx = spy(new ReadCacheQueryPipelineContext(context,
//        Lists.newArrayList(sink)));
//    ctx.initialize(null).join();
//    ctx.fetchNext(null);
//    assertEquals(7, ctx.results.length);
//    
//    assertEquals(1514786520000L, ctx.current_time);
//    assertEquals(7, ctx.cache_latch.get());
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 4));
//    assertEquals(6, ctx.cache_latch.get());
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 0));
//    assertEquals(5, ctx.cache_latch.get());
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 1));
//    assertEquals(4, ctx.cache_latch.get());    
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 2));
//    assertEquals(3, ctx.cache_latch.get()); 
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 3));
//    assertEquals(2, ctx.cache_latch.get());
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 5)); // tip so we fire another
//    assertEquals(1, ctx.cache_latch.get());
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 6, 1514786460));
//    assertEquals(0, ctx.cache_latch.get());
//    
//    verify(sink, never()).onNext(any(QueryResult.class));
//    assertTrue(TSDB.runnables.isEmpty());
//    
//    assertTrue(((MockQueryContext) ctx.results[5].sub_context).initialized);
//    assertTrue(((MockQueryContext) ctx.results[5].sub_context).fetched);
//    
//    // one result in
//    assertTrue(((MockQueryContext) ctx.results[6].sub_context).initialized);
//    assertTrue(((MockQueryContext) ctx.results[6].sub_context).fetched);
//    ctx.results[6].onNext(mockResult(new DefaultQueryResultId("m1", "m1")));
//    ctx.results[6].onComplete();
//    
//    assertEquals(0, ctx.cache_latch.get()); // #5 is left
//    verify(sink, never()).onNext(any(QueryResult.class));
//    assertTrue(TSDB.runnables.isEmpty());
//    
//    // last result came in
//    ctx.results[5].onNext(mockResult(new DefaultQueryResultId("m1", "m1")));
//    ctx.results[5].onComplete();
//    
//    assertEquals(0, ctx.cache_latch.get());
//    verify(sink, times(1)).onNext(any(QueryResult.class));
//    assertFalse(TSDB.runnables.isEmpty()); // cached!
//  }
//  
//  @Test
//  public void onCacheResultsGoodTwoTipsLastResultInBetween() throws Exception {
//    setQuery(1514765700, 1514787300, "5m", false);
//    mockDateTime(1514786520000L);
//    ReadCacheQueryPipelineContext ctx = spy(new ReadCacheQueryPipelineContext(context,
//        Lists.newArrayList(sink)));
//    ctx.initialize(null).join();
//    ctx.fetchNext(null);
//    assertEquals(7, ctx.results.length);
//    
//    assertEquals(1514786520000L, ctx.current_time);
//    assertEquals(7, ctx.cache_latch.get());
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 4));
//    assertEquals(6, ctx.cache_latch.get());
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 0));
//    assertEquals(5, ctx.cache_latch.get());
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 1));
//    assertEquals(4, ctx.cache_latch.get());    
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 2));
//    assertEquals(3, ctx.cache_latch.get()); 
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 3));
//    assertEquals(2, ctx.cache_latch.get());
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 5)); // tip so we fire another
//    assertEquals(1, ctx.cache_latch.get());
//    
//    // result in between
//    assertTrue(((MockQueryContext) ctx.results[5].sub_context).initialized);
//    assertTrue(((MockQueryContext) ctx.results[5].sub_context).fetched);
//    ctx.results[5].onNext(mockResult(new DefaultQueryResultId("m1", "m1")));
//    ctx.results[5].onComplete();
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 6, 1514786460));
//    assertEquals(0, ctx.cache_latch.get());
//    
//    verify(sink, never()).onNext(any(QueryResult.class));
//    assertTrue(TSDB.runnables.isEmpty());
//    
//    // one result in
//    assertTrue(((MockQueryContext) ctx.results[6].sub_context).initialized);
//    assertTrue(((MockQueryContext) ctx.results[6].sub_context).fetched);
//    ctx.results[6].onNext(mockResult(new DefaultQueryResultId("m1", "m1")));
//    ctx.results[6].onComplete();
//    
//    assertEquals(0, ctx.cache_latch.get());
//    verify(sink, times(1)).onNext(any(QueryResult.class));
//    assertFalse(TSDB.runnables.isEmpty()); // cached!
//  }
//  
//  @Test
//  public void onCacheResultsGoodTwoTipsExceptionFromSubQuery() throws Exception {
//    setQuery(1514765700, 1514787300, "5m", false);
//    mockDateTime(1514786520000L);
//    ReadCacheQueryPipelineContext ctx = spy(new ReadCacheQueryPipelineContext(context,
//        Lists.newArrayList(sink)));
//    ctx.initialize(null).join();
//    ctx.fetchNext(null);
//    assertEquals(7, ctx.results.length);
//    
//    assertEquals(1514786520000L, ctx.current_time);
//    assertEquals(7, ctx.cache_latch.get());
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 4));
//    assertEquals(6, ctx.cache_latch.get());
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 0));
//    assertEquals(5, ctx.cache_latch.get());
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 1));
//    assertEquals(4, ctx.cache_latch.get());    
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 2));
//    assertEquals(3, ctx.cache_latch.get()); 
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 3));
//    assertEquals(2, ctx.cache_latch.get());
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 5)); // tip so we fire another
//    assertEquals(1, ctx.cache_latch.get());
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 6, 1514786460));
//    assertEquals(0, ctx.cache_latch.get());
//    System.out.println(" 000000000000000000000000000000000");
//    verify(sink, never()).onNext(any(QueryResult.class));
//    assertTrue(TSDB.runnables.isEmpty());
//    
//    assertTrue(((MockQueryContext) ctx.results[5].sub_context).initialized);
//    assertTrue(((MockQueryContext) ctx.results[5].sub_context).fetched);
//    
//    // one result in
//    assertTrue(((MockQueryContext) ctx.results[6].sub_context).initialized);
//    assertTrue(((MockQueryContext) ctx.results[6].sub_context).fetched);
//    ctx.results[6].onError(new UnitTestException());
//    
//    verify(sink, times(1)).onError(any(UnitTestException.class));
//    
//    assertEquals(0, ctx.cache_latch.get());
//    verify(sink, never()).onNext(any(QueryResult.class));
//    assertTrue(TSDB.runnables.isEmpty());
//    
//    // last result came in
//    ctx.results[5].onNext(mockResult(new DefaultQueryResultId("m1", "m1")));
//    ctx.results[5].onComplete();
//    
//    assertEquals(0, ctx.cache_latch.get());
//    verify(sink, never()).onNext(any(QueryResult.class));
//    verify(sink, times(1)).onError(any(UnitTestException.class));
//    assertTrue(TSDB.runnables.isEmpty());
//  }
//  
//  @Test
//  public void onCacheResultsGoodButOneFailed() throws Exception {
//    mockDateTime(1514851200000L);
//    ReadCacheQueryPipelineContext ctx = new ReadCacheQueryPipelineContext(context,
//        Lists.newArrayList(sink));
//    ctx.initialize(null).join();
//    ctx.fetchNext(null);
//    
//    assertEquals(1514851200000L, ctx.current_time);
//    assertEquals(6, ctx.cache_latch.get());
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 4));
//    assertEquals(5, ctx.cache_latch.get());
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 0));
//    assertEquals(4, ctx.cache_latch.get());
//    
//    // whoops!
//    ctx.onCacheError(-1, new UnitTestException());
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 1));
//    assertEquals(4, ctx.cache_latch.get());
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 5));
//    assertEquals(4, ctx.cache_latch.get());
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 3));
//    assertEquals(4, ctx.cache_latch.get());
//    verify(sink, never()).onNext(any(QueryResult.class));
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 2));
//    assertEquals(4, ctx.cache_latch.get());
//    
//    // TODO - fix it up
//    //verify(sink, times(1)).onNext(any(QueryResult.class));
//    verify(sink, never()).onError(any(UnitTestException.class));
//    assertTrue(TSDB.runnables.isEmpty());
//  }
//  
//  @Test
//  public void onCacheResultsBelowThresholdThenRecovers() throws Exception {
//    mockDateTime(1514851200000L);
//    ReadCacheQueryPipelineContext ctx = new ReadCacheQueryPipelineContext(context,
//        Lists.newArrayList(sink));
//    ctx.initialize(null).join();
//    ctx.fetchNext(null);
//    
//    assertEquals(1514851200000L, ctx.current_time);
//    assertEquals(6, ctx.cache_latch.get());
//    
//    ctx.onCacheResult(buildCacheMiss(ctx, 4));
//    assertEquals(5, ctx.cache_latch.get());
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 0));
//    assertEquals(4, ctx.cache_latch.get());
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 1));
//    assertEquals(3, ctx.cache_latch.get());
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 5));
//    assertEquals(2, ctx.cache_latch.get());
//    assertNull(ctx.results[4].sub_context);
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 3));
//    assertEquals(1, ctx.cache_latch.get());
//    verify(sink, never()).onNext(any(QueryResult.class));
//    // now we run 4
//    assertTrue(((MockQueryContext) ctx.results[4].sub_context).initialized);
//    assertTrue(((MockQueryContext) ctx.results[4].sub_context).fetched);
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 2));
//    assertEquals(0, ctx.cache_latch.get());
//    
//    verify(sink, never()).onNext(any(QueryResult.class));
//    assertTrue(TSDB.runnables.isEmpty());
//    assertNull(ctx.full_query_context);
//    
//    ctx.results[4].onNext(mockResult(new DefaultQueryResultId("m1", "m1")));
//    ctx.results[4].onComplete();
//    
//    assertEquals(0, ctx.cache_latch.get());
//    // TODO - fix me
//    //verify(sink, times(1)).onNext(any(QueryResult.class));
//  }
//  
//  @Test
//  public void onCacheResultsBelowThresholdAtEnd() throws Exception {
//    mockDateTime(1514851200000L);
//    ReadCacheQueryPipelineContext ctx = new ReadCacheQueryPipelineContext(context,
//        Lists.newArrayList(sink));
//    ctx.initialize(null).join();
//    ctx.fetchNext(null);
//    
//    assertEquals(1514851200000L, ctx.current_time);
//    assertEquals(6, ctx.cache_latch.get());
//    
//    ctx.onCacheResult(buildCacheMiss(ctx, 4));
//    assertEquals(5, ctx.cache_latch.get());
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 0));
//    assertEquals(4, ctx.cache_latch.get());
//    
//    ctx.onCacheResult(buildCacheMiss(ctx, 1));
//    assertEquals(3, ctx.cache_latch.get());
//    
//    ctx.onCacheResult(buildCacheMiss(ctx, 5));
//    assertEquals(2, ctx.cache_latch.get());
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 3));
//    assertEquals(1, ctx.cache_latch.get());
//    verify(sink, never()).onNext(any(QueryResult.class));
//    
//    ctx.onCacheResult(buildFakeFullResult(ctx, 2));
//    assertEquals(0, ctx.cache_latch.get());
//    
//    verify(sink, never()).onNext(any(QueryResult.class));
//    assertTrue(TSDB.runnables.isEmpty());
//    assertTrue(((MockQueryContext) ctx.full_query_context).initialized);
//    assertTrue(((MockQueryContext) ctx.full_query_context).fetched);
//    
//    ((MockQueryContext) ctx.full_query_context).sink.onNext(mockResult(new DefaultQueryResultId("m1", "m1")));
//    verify(sink, times(1)).onNext(any(QueryResult.class));
//    
//    // cleaned up
//    assertNull(ctx.results[4].map);
//  }
//  
  ReadCacheQueryResultSet buildFakeFullResult(final ReadCacheQueryPipelineContext ctx,
                                        final int idx) {
    ReadCacheQueryResultSet result = mock(ReadCacheQueryResultSet.class);
    when(result.key()).thenReturn(ctx.keys[idx]);
    Map<QueryResultId, ReadCacheQueryResult> results = Maps.newHashMap();
    ReadCacheQueryResult cqr = mock(ReadCacheQueryResult.class);
    results.put(new DefaultQueryResultId("m1", "m1"), cqr);
    QueryNode node = mock(QueryNode.class);
    QueryNodeConfig config = mock(QueryNodeConfig.class);
    when(config.getId()).thenReturn("m1");
    when(node.config()).thenReturn(config);
    when(cqr.dataSource()).thenReturn(new DefaultQueryResultId("m1", "m1"));
    when(cqr.source()).thenReturn(node);
    when(result.results()).thenReturn(results);
    when(result.lastValueTimestamp()).thenReturn(new SecondTimeStamp(
        ctx.slices[idx] + ctx.interval_in_seconds));
    return result;
  }
  
  ReadCacheQueryResultSet buildFakeFullResult(final ReadCacheQueryPipelineContext ctx,
                                       final int idx,
                                       final int timestamp) {
    ReadCacheQueryResultSet result = mock(ReadCacheQueryResultSet.class);
    when(result.key()).thenReturn(ctx.keys[idx]);
    Map<QueryResultId, ReadCacheQueryResult> results = Maps.newHashMap();
    results.put(new DefaultQueryResultId("m1", "m1"), mock(ReadCacheQueryResult.class));
    when(result.results()).thenReturn(results);
    when(result.lastValueTimestamp()).thenReturn(new SecondTimeStamp(timestamp));
    return result;
  }
  
  ReadCacheQueryResultSet buildCacheMiss(final ReadCacheQueryPipelineContext ctx,
                                  final int idx) {
    ReadCacheQueryResultSet result = mock(ReadCacheQueryResultSet.class);
    when(result.key()).thenReturn(ctx.keys[idx]);
    return result;
  }
  
  void setQuery(final int start, 
                final int end, 
                final String interval, 
                final boolean runall) {
    setQuery(start, end, interval, runall, CacheMode.NORMAL);
  }
  
  void setQuery(final int start, 
                final int end, 
                final String interval, 
                final boolean runall,
                final CacheMode cache_mode) {
    SemanticQuery.Builder builder = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setCacheMode(cache_mode)
        .setStart(Integer.toString(start))
        .setEnd(Integer.toString(end))
        .addExecutionGraphNode(DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setId("m1")
            .build());
    if (!Strings.isNullOrEmpty(interval)) {
      builder.addExecutionGraphNode(DownsampleConfig.newBuilder()
          .setAggregator("sum")
          .setInterval(interval)
          .addInterpolatorConfig(NUMERIC_CONFIG)
          .setRunAll(runall)
          .addSource("m1")
          .setId("downsample")
          .build());
    }
    query = builder.build();
    when(context.query()).thenReturn(query);
  }
  
  QueryResult mockResult(final QueryResultId source) {
    QueryResult result = mock(QueryResult.class);
    when(result.dataSource()).thenReturn(source);
    QueryNode node = mock(QueryNode.class);
    QueryNodeConfig config = mock(QueryNodeConfig.class);
    when(node.config()).thenReturn(config);
    when(config.getId()).thenReturn(source.nodeID());
    when(result.source()).thenReturn(node);
    return result;
  }
  
  void mockDateTime(final long timestamp) {
    PowerMockito.mockStatic(DateTime.class);
    when(DateTime.currentTimeMillis()).thenReturn(timestamp);
    when(DateTime.parseDuration(anyString())).thenCallRealMethod();
    when(DateTime.getDurationInterval(anyString())).thenCallRealMethod();
    when(DateTime.getDurationUnits(anyString())).thenCallRealMethod();
  }
  
  class MockQueryContext implements QueryContext {
    final int start;
    final QuerySink sink;
    boolean initialized;
    boolean fetched;
    boolean closed;
    
    MockQueryContext(final int start, final QuerySink sink) {
      this.start = start;
      this.sink = sink;
    }

    @Override
    public Collection<QuerySink> sinks() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public QueryMode mode() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void fetchNext(Span span) {
      fetched = true;
    }

    @Override
    public void close() {
      closed = true;
    }

    @Override
    public QueryStats stats() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public List<QuerySinkConfig> sinkConfigs() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public TimeSeriesQuery query() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public net.opentsdb.core.TSDB tsdb() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public AuthState authState() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Map<String, String> headers() {
      return null;
    }
    
    @Override
    public boolean cacheable() {
      return true;
    }
    
    @Override
    public Deferred<Void> initialize(Span span) {
      initialized = true;
      return Deferred.fromResult(null);
    }

    @Override
    public TimeSeriesId getId(long hash,
        TypeToken<? extends TimeSeriesId> type) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public List<String> logs() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void logError(String log) {
      // TODO Auto-generated method stub
      
    }

    @Override
    public void logError(QueryNode node, String log) {
      // TODO Auto-generated method stub
      
    }

    @Override
    public void logWarn(String log) {
      // TODO Auto-generated method stub
      
    }

    @Override
    public void logWarn(QueryNode node, String log) {
      // TODO Auto-generated method stub
      
    }

    @Override
    public void logInfo(String log) {
      // TODO Auto-generated method stub
      
    }

    @Override
    public void logInfo(QueryNode node, String log) {
      // TODO Auto-generated method stub
      
    }

    @Override
    public void logDebug(String log) {
      // TODO Auto-generated method stub
      
    }

    @Override
    public void logDebug(QueryNode node, String log) {
      // TODO Auto-generated method stub
      
    }

    @Override
    public void logTrace(String log) {
      // TODO Auto-generated method stub
      
    }

    @Override
    public void logTrace(QueryNode node, String log) {
      // TODO Auto-generated method stub
      
    }
    
    @Override
    public boolean isClosed() {
      // TODO Auto-generated method stub
      return false;
    }
  }

}