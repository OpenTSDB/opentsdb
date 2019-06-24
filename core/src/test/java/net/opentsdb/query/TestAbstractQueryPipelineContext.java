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
package net.opentsdb.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.common.Const;
import net.opentsdb.core.DefaultRegistry;
import net.opentsdb.core.MockTSDB;
import net.opentsdb.core.TSDBPlugin;
import net.opentsdb.data.PartialTimeSeries;
import net.opentsdb.data.PartialTimeSeriesSet;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.data.TimeSeriesDataSourceFactory;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.plan.DefaultQueryPlanner;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.stats.Span;

public class TestAbstractQueryPipelineContext {

  private static MockTSDB TSDB;
  private static NumericInterpolatorConfig NUMERIC_CONFIG;
  private static TimeSeriesDataSourceFactory STORE_FACTORY;
  private static List<TimeSeriesDataSource> STORE_NODES;
  private static QuerySinkFactory SINK_FACTORY;
  private static QueryNode NODE_A;
  private static QueryNode NODE_B;
  
  private TimeSeriesQuery query;
  private QueryContext context;
  private QuerySink sink;
  private QuerySinkConfig sink_config;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    TSDB = new MockTSDB();
    STORE_FACTORY = mock(TimeSeriesDataSourceFactory.class);
    STORE_NODES = Lists.newArrayList();
    SINK_FACTORY = mock(QuerySinkFactory.class);
    NODE_A = mock(QueryNode.class);
    NODE_B = mock(QueryNode.class);
    
    QueryNodeConfig config_a = mock(QueryNodeConfig.class);
    when(config_a.getId()).thenReturn("NODEA");
    when(NODE_A.config()).thenReturn(config_a);
    QueryNodeConfig config_b = mock(QueryNodeConfig.class);
    when(config_b.getId()).thenReturn("NODEB");
    when(NODE_B.config()).thenReturn(config_b);
    
    TSDB.registry = new DefaultRegistry(TSDB);
    ((DefaultRegistry) TSDB.registry).initialize(true).join();
    ((DefaultRegistry) TSDB.registry).registerPlugin(
        TimeSeriesDataSourceFactory.class, null, (TSDBPlugin) STORE_FACTORY);
    ((DefaultRegistry) TSDB.registry).registerPlugin(
        QuerySinkFactory.class, null, (TSDBPlugin) SINK_FACTORY);
    
    when(STORE_FACTORY.newNode(any(QueryPipelineContext.class), 
        any(QueryNodeConfig.class)))
      .thenAnswer(new Answer<QueryNode>() {
        @Override
        public QueryNode answer(InvocationOnMock invocation) throws Throwable {
          final TimeSeriesDataSource node = mock(TimeSeriesDataSource.class);
          when(node.initialize(null)).thenReturn(Deferred.fromResult(null));
          when(node.config()).thenReturn((QueryNodeConfig) invocation.getArguments()[1]);
          STORE_NODES.add(node);
          return node;
        }
      });
    when(STORE_FACTORY.idType()).thenAnswer(new Answer<TypeToken<? extends TimeSeriesId>>() {
      @Override
      public TypeToken<? extends TimeSeriesId> answer(
          InvocationOnMock invocation) throws Throwable {
        return Const.TS_STRING_ID;
      }
    });
    
    NUMERIC_CONFIG = (NumericInterpolatorConfig) 
        NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
        .setDataType(NumericType.TYPE.toString())
        .build();
  }
  
  @Before
  public void before() throws Exception {
    context = mock(QueryContext.class);
    sink = mock(QuerySink.class);
    sink_config = mock(QuerySinkConfig.class);
    
    query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .addExecutionGraphNode(DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setId("m1")
            .build())
        .build();
    
    when(SINK_FACTORY.newSink(any(QueryContext.class), any(QuerySinkConfig.class)))
      .thenReturn(sink);
    when(context.query()).thenReturn(query);
    when(context.tsdb()).thenReturn(TSDB);
    when(context.sinkConfigs()).thenReturn(Lists.newArrayList(sink_config));
  }
  
  @Test
  public void ctor() throws Exception {
    AbstractQueryPipelineContext ctx = new TestContext(context);
    assertSame(context, ctx.queryContext());
    assertSame(ctx, ctx.pipelineContext());
    assertSame(query, ctx.query());
    assertTrue(ctx.sinks().isEmpty());
    assertNull(ctx.config());
    
    try {
      new TestContext(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void initializeGraph1Node() throws Exception {
    AbstractQueryPipelineContext ctx = new TestContext(context);
    assertNull(ctx.initialize(null).join());
    
    assertEquals(1, ctx.sinks().size());
    DefaultQueryPlanner plan = ctx.plan();
    assertEquals(1, plan.sources().size());
  }
  
  @Test
  public void onComplete1Node1Set() throws Exception {
    AbstractQueryPipelineContext ctx = new TestContext(context);
    assertNull(ctx.initialize(null).join());
    
    assertEquals(1, ctx.sinks().size());
    DefaultQueryPlanner plan = ctx.plan();
    assertEquals(1, plan.sources().size());
    verify(sink, never()).onComplete();
    
    PartialTimeSeriesSet set = mock(PartialTimeSeriesSet.class);
    PartialTimeSeries pts = mock(PartialTimeSeries.class);
    when(pts.set()).thenReturn(set);
    when(set.totalSets()).thenReturn(1);
    when(set.timeSeriesCount()).thenReturn(1);
    when(set.node()).thenReturn(NODE_A);
    when(set.dataSource()).thenReturn("m1");
    when(set.start()).thenReturn(new SecondTimeStamp(1546300800));
    
    // 1 of two time series
    ctx.onComplete(pts);
    verify(sink, never()).onComplete();
    assertEquals(1, ctx.pts.size());
    assertEquals(0, ctx.finished_sources.size());
    assertEquals(0, ctx.total_finished.get());
    
    // second time series.
    when(set.timeSeriesCount()).thenReturn(2);
    when(set.complete()).thenReturn(true);
    ctx.onComplete(pts);
    verify(sink, times(1)).onComplete();
    assertEquals(1, ctx.pts.size());
    assertEquals(1, ctx.finished_sources.size());
    assertEquals(1, ctx.total_finished.get());
  }
  
  @Test
  public void onComplete1Node1SetOutOfOrder() throws Exception {
    AbstractQueryPipelineContext ctx = new TestContext(context);
    assertNull(ctx.initialize(null).join());
    
    assertEquals(1, ctx.sinks().size());
    DefaultQueryPlanner plan = ctx.plan();
    assertEquals(1, plan.sources().size());
    verify(sink, never()).onComplete();
    
    PartialTimeSeriesSet set = mock(PartialTimeSeriesSet.class);
    PartialTimeSeries pts = mock(PartialTimeSeries.class);
    when(pts.set()).thenReturn(set);
    when(set.totalSets()).thenReturn(1);
    when(set.node()).thenReturn(NODE_A);
    when(set.dataSource()).thenReturn("m1");
    when(set.start()).thenReturn(new SecondTimeStamp(1546300800));
    
    // 2 of two time series
    when(set.timeSeriesCount()).thenReturn(2);
    when(set.complete()).thenReturn(true);
    ctx.onComplete(pts);
    verify(sink, never()).onComplete();
    assertEquals(1, ctx.pts.size());
    assertEquals(0, ctx.finished_sources.size());
    assertEquals(0, ctx.total_finished.get());
    
    // first time series.
    when(set.timeSeriesCount()).thenReturn(1);
    when(set.complete()).thenReturn(false);
    ctx.onComplete(pts);
    verify(sink, times(1)).onComplete();
    assertEquals(1, ctx.pts.size());
    assertEquals(1, ctx.finished_sources.size());
    assertEquals(1, ctx.total_finished.get());
  }
  
  @Test
  public void onComplete1Node2Sets() throws Exception {
    AbstractQueryPipelineContext ctx = new TestContext(context);
    assertNull(ctx.initialize(null).join());
    
    assertEquals(1, ctx.sinks().size());
    DefaultQueryPlanner plan = ctx.plan();
    assertEquals(1, plan.sources().size());
    verify(sink, never()).onComplete();
    
    PartialTimeSeriesSet set = mock(PartialTimeSeriesSet.class);
    PartialTimeSeries pts = mock(PartialTimeSeries.class);
    when(pts.set()).thenReturn(set);
    when(set.totalSets()).thenReturn(2);
    when(set.timeSeriesCount()).thenReturn(1);
    when(set.node()).thenReturn(NODE_A);
    when(set.dataSource()).thenReturn("m1");
    when(set.start()).thenReturn(new SecondTimeStamp(1546300800));
    
    // 1 of two time series
    ctx.onComplete(pts);
    verify(sink, never()).onComplete();
    assertEquals(1, ctx.pts.size());
    assertEquals(0, ctx.finished_sources.size());
    assertEquals(0, ctx.total_finished.get());
    
    // second time series.
    when(set.timeSeriesCount()).thenReturn(2);
    when(set.complete()).thenReturn(true);
    ctx.onComplete(pts);
    verify(sink, never()).onComplete();
    assertEquals(1, ctx.pts.size());
    assertEquals(1, ctx.finished_sources.size());
    assertEquals(0, ctx.total_finished.get());
    
    // next set
    set = mock(PartialTimeSeriesSet.class);
    pts = mock(PartialTimeSeries.class);
    when(pts.set()).thenReturn(set);
    when(set.totalSets()).thenReturn(2);
    when(set.timeSeriesCount()).thenReturn(1);
    when(set.node()).thenReturn(NODE_A);
    when(set.dataSource()).thenReturn("m1");
    when(set.start()).thenReturn(new SecondTimeStamp(1546304400));
    
    // 1 of two time series
    ctx.onComplete(pts);
    verify(sink, never()).onComplete();
    assertEquals(1, ctx.pts.size());
    assertEquals(1, ctx.finished_sources.size());
    assertEquals(0, ctx.total_finished.get());
    
    // second time series.
    when(set.timeSeriesCount()).thenReturn(2);
    when(set.complete()).thenReturn(true);
    ctx.onComplete(pts);
    verify(sink, times(1)).onComplete();
    assertEquals(1, ctx.pts.size());
    assertEquals(1, ctx.finished_sources.size());
    assertEquals(1, ctx.total_finished.get());
  }

  @Test
  public void onComplete1Node2SetsOutOfOrderTS() throws Exception {
    AbstractQueryPipelineContext ctx = new TestContext(context);
    assertNull(ctx.initialize(null).join());
    
    assertEquals(1, ctx.sinks().size());
    DefaultQueryPlanner plan = ctx.plan();
    assertEquals(1, plan.sources().size());
    verify(sink, never()).onComplete();
    
    PartialTimeSeriesSet set = mock(PartialTimeSeriesSet.class);
    PartialTimeSeries pts = mock(PartialTimeSeries.class);
    when(pts.set()).thenReturn(set);
    when(set.totalSets()).thenReturn(2);
    when(set.node()).thenReturn(NODE_A);
    when(set.dataSource()).thenReturn("m1");
    when(set.start()).thenReturn(new SecondTimeStamp(1546300800));
    
    // 2 of two time series
    when(set.timeSeriesCount()).thenReturn(2);
    when(set.complete()).thenReturn(true);
    ctx.onComplete(pts);
    verify(sink, never()).onComplete();
    assertEquals(1, ctx.pts.size());
    assertEquals(0, ctx.finished_sources.size());
    assertEquals(0, ctx.total_finished.get());
    
    // first time series.
    when(set.timeSeriesCount()).thenReturn(1);
    when(set.complete()).thenReturn(false);
    ctx.onComplete(pts);
    verify(sink, never()).onComplete();
    assertEquals(1, ctx.pts.size());
    assertEquals(1, ctx.finished_sources.size());
    assertEquals(0, ctx.total_finished.get());
    
    // next set
    set = mock(PartialTimeSeriesSet.class);
    pts = mock(PartialTimeSeries.class);
    when(pts.set()).thenReturn(set);
    when(set.totalSets()).thenReturn(2);
    when(set.node()).thenReturn(NODE_A);
    when(set.dataSource()).thenReturn("m1");
    when(set.start()).thenReturn(new SecondTimeStamp(1546304400));
    
    // 2 of two time series
    when(set.timeSeriesCount()).thenReturn(2);
    when(set.complete()).thenReturn(true);
    ctx.onComplete(pts);
    verify(sink, never()).onComplete();
    assertEquals(1, ctx.pts.size());
    assertEquals(1, ctx.finished_sources.size());
    assertEquals(0, ctx.total_finished.get());
    
    // first time series.
    when(set.timeSeriesCount()).thenReturn(1);
    when(set.complete()).thenReturn(false);
    ctx.onComplete(pts);
    verify(sink, times(1)).onComplete();
    assertEquals(1, ctx.pts.size());
    assertEquals(1, ctx.finished_sources.size());
    assertEquals(1, ctx.total_finished.get());
  }
  
  @Test
  public void onComplete1Node2SetsNoData() throws Exception {
    AbstractQueryPipelineContext ctx = new TestContext(context);
    assertNull(ctx.initialize(null).join());
    
    assertEquals(1, ctx.sinks().size());
    DefaultQueryPlanner plan = ctx.plan();
    assertEquals(1, plan.sources().size());
    verify(sink, never()).onComplete();
    
    PartialTimeSeriesSet set = mock(PartialTimeSeriesSet.class);
    PartialTimeSeries pts = mock(PartialTimeSeries.class);
    when(pts.set()).thenReturn(set);
    when(set.totalSets()).thenReturn(2);
    when(set.node()).thenReturn(NODE_A);
    when(set.dataSource()).thenReturn("m1");
    when(set.start()).thenReturn(new SecondTimeStamp(1546300800));
    
    // 1 of 1 series
    when(set.timeSeriesCount()).thenReturn(0);
    when(set.complete()).thenReturn(true);
    ctx.onComplete(pts);
    verify(sink, never()).onComplete();
    assertEquals(1, ctx.pts.size());
    assertEquals(1, ctx.finished_sources.size());
    assertEquals(0, ctx.total_finished.get());
    
    // next set
    set = mock(PartialTimeSeriesSet.class);
    pts = mock(PartialTimeSeries.class);
    when(pts.set()).thenReturn(set);
    when(set.totalSets()).thenReturn(2);
    when(set.node()).thenReturn(NODE_A);
    when(set.dataSource()).thenReturn("m1");
    when(set.start()).thenReturn(new SecondTimeStamp(1546304400));
    
    // 1 of 1 time series
    when(set.timeSeriesCount()).thenReturn(0);
    when(set.complete()).thenReturn(true);
    ctx.onComplete(pts);
    verify(sink, times(1)).onComplete();
    assertEquals(1, ctx.pts.size());
    assertEquals(1, ctx.finished_sources.size());
    assertEquals(1, ctx.total_finished.get());
  }
  
  @Test
  public void onComplete2Nodes1Set() throws Exception {
    query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .addExecutionGraphNode(DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setId("m1")
            .build())
        .addExecutionGraphNode(DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.sys")
                .build())
            .setId("m2")
            .build())
        .build();
    when(context.query()).thenReturn(query);
    
    AbstractQueryPipelineContext ctx = new TestContext(context);
    assertNull(ctx.initialize(null).join());
    
    assertEquals(1, ctx.sinks().size());
    DefaultQueryPlanner plan = ctx.plan();
    assertEquals(2, plan.sources().size());
    verify(sink, never()).onComplete();
    
    PartialTimeSeriesSet set = mock(PartialTimeSeriesSet.class);
    PartialTimeSeries pts = mock(PartialTimeSeries.class);
    when(pts.set()).thenReturn(set);
    when(set.totalSets()).thenReturn(1);
    when(set.timeSeriesCount()).thenReturn(1);
    when(set.node()).thenReturn(NODE_A);
    when(set.dataSource()).thenReturn("m1");
    when(set.start()).thenReturn(new SecondTimeStamp(1546300800));
    
    // 1 of two time series
    ctx.onComplete(pts);
    verify(sink, never()).onComplete();
    assertEquals(1, ctx.pts.size());
    assertEquals(0, ctx.finished_sources.size());
    assertEquals(0, ctx.total_finished.get());
    
    // second time series.
    when(set.timeSeriesCount()).thenReturn(2);
    when(set.complete()).thenReturn(true);
    ctx.onComplete(pts);
    verify(sink, never()).onComplete();
    assertEquals(1, ctx.pts.size());
    assertEquals(1, ctx.finished_sources.size());
    assertEquals(1, ctx.total_finished.get());
    
    set = mock(PartialTimeSeriesSet.class);
    pts = mock(PartialTimeSeries.class);
    when(pts.set()).thenReturn(set);
    when(set.totalSets()).thenReturn(1);
    when(set.timeSeriesCount()).thenReturn(1);
    when(set.node()).thenReturn(NODE_B);
    when(set.dataSource()).thenReturn("m2");
    when(set.start()).thenReturn(new SecondTimeStamp(1546300800));
    
    // 1 of two time series
    ctx.onComplete(pts);
    verify(sink, never()).onComplete();
    assertEquals(2, ctx.pts.size());
    assertEquals(1, ctx.finished_sources.size());
    assertEquals(1, ctx.total_finished.get());
    
    // second time series.
    when(set.timeSeriesCount()).thenReturn(2);
    when(set.complete()).thenReturn(true);
    ctx.onComplete(pts);
    verify(sink, times(1)).onComplete();
    assertEquals(2, ctx.pts.size());
    assertEquals(2, ctx.finished_sources.size());
    assertEquals(2, ctx.total_finished.get());
  }
  
  @Test
  public void onComplete2Nodes1SetNoData() throws Exception {
    query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .addExecutionGraphNode(DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setId("m1")
            .build())
        .addExecutionGraphNode(DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.sys")
                .build())
            .setId("m2")
            .build())
        .build();
    when(context.query()).thenReturn(query);
    
    AbstractQueryPipelineContext ctx = new TestContext(context);
    assertNull(ctx.initialize(null).join());
    
    assertEquals(1, ctx.sinks().size());
    DefaultQueryPlanner plan = ctx.plan();
    assertEquals(2, plan.sources().size());
    verify(sink, never()).onComplete();
    
    PartialTimeSeriesSet set = mock(PartialTimeSeriesSet.class);
    PartialTimeSeries pts = mock(PartialTimeSeries.class);
    when(pts.set()).thenReturn(set);
    when(set.totalSets()).thenReturn(1);
    when(set.timeSeriesCount()).thenReturn(0);
    when(set.node()).thenReturn(NODE_A);
    when(set.dataSource()).thenReturn("m1");
    when(set.start()).thenReturn(new SecondTimeStamp(1546300800));
    
    // 1 of 1 time series
    when(set.complete()).thenReturn(true);
    ctx.onComplete(pts);
    verify(sink, never()).onComplete();
    assertEquals(1, ctx.pts.size());
    assertEquals(1, ctx.finished_sources.size());
    assertEquals(1, ctx.total_finished.get());
    
    set = mock(PartialTimeSeriesSet.class);
    pts = mock(PartialTimeSeries.class);
    when(pts.set()).thenReturn(set);
    when(set.totalSets()).thenReturn(1);
    when(set.timeSeriesCount()).thenReturn(0);
    when(set.node()).thenReturn(NODE_B);
    when(set.dataSource()).thenReturn("m2");
    when(set.start()).thenReturn(new SecondTimeStamp(1546300800));
    
    // 1 of two time series
    when(set.complete()).thenReturn(true);
    ctx.onComplete(pts);
    verify(sink, times(1)).onComplete();
    assertEquals(2, ctx.pts.size());
    assertEquals(2, ctx.finished_sources.size());
    assertEquals(2, ctx.total_finished.get());
  }
  
  @Test
  public void ids() throws Exception {
    AbstractQueryPipelineContext ctx = new TestContext(context);
    assertNull(ctx.initialize(null).join());
    
    TimeSeriesId id_a = mockId(Const.TS_BYTE_ID);
    TimeSeriesId id_b = mockId(Const.TS_STRING_ID);
    ctx.addId(42, id_a);
    ctx.addId(42, id_b);
    assertTrue(ctx.hasId(42, Const.TS_BYTE_ID));
    assertTrue(ctx.hasId(42, Const.TS_STRING_ID));
    assertFalse(ctx.hasId(24, Const.TS_STRING_ID));
    assertSame(id_a, ctx.getId(42, Const.TS_BYTE_ID));
    assertSame(id_b, ctx.getId(42, Const.TS_STRING_ID));
    assertNull(ctx.getId(24, Const.TS_STRING_ID));
    
    try {
      ctx.addId(42, mockId(Const.TS_STRING_ID));
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  class TestContext extends AbstractQueryPipelineContext {
    public TestContext(final QueryContext context) {
      super(context);
    }
  
    @Override
    public Deferred<Void> initialize(Span span) {
      return initializeGraph(span);
    }
    
  }
  
  private TimeSeriesId mockId(final TypeToken<? extends TimeSeriesId> type) {
    TimeSeriesId id = mock(TimeSeriesId.class);
    when(id.type()).thenAnswer(new Answer<TypeToken>() {
      @Override
      public TypeToken answer(InvocationOnMock invocation) throws Throwable {
        return type;
      }
    });
    return id;
  }
}
