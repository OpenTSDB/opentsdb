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
package net.opentsdb.query.processor.rate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.DefaultRegistry;
import net.opentsdb.core.MockTSDB;
import net.opentsdb.core.MockTSDBDefault;
import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.data.TimeSeriesDataSourceFactory;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.types.numeric.NumericMillisecondShard;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.DefaultTimeSeriesDataSourceConfig;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryIteratorFactory;
import net.opentsdb.query.QueryMode;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.plan.DefaultQueryPlanner;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.stats.QueryStats;
import net.opentsdb.stats.Span;

public class TestRateFactory {


  private static MockTSDB TSDB;
  private static RateFactory FACTORY;
  private static NumericInterpolatorConfig NUMERIC_CONFIG;
  private static TimeSeriesDataSource SRC_MOCK;

  @BeforeClass
  public static void beforeClass() throws Exception {
    TSDB = new MockTSDB();
    TSDB.registry = spy(new DefaultRegistry(TSDB));
    SRC_MOCK = mock(TimeSeriesDataSource.class);
    ((DefaultRegistry) TSDB.registry).initialize(true).join(60_000);
    TimeSeriesDataSourceFactory s1 = mock(TimeSeriesDataSourceFactory.class);
    when(s1.newNode(any(QueryPipelineContext.class), any(QueryNodeConfig.class)))
      .thenReturn(SRC_MOCK);
    TSDB.registry.registerPlugin(TimeSeriesDataSourceFactory.class, null, s1);
    FACTORY = new RateFactory();
    FACTORY.registerConfigs(TSDB);
    FACTORY.initialize(TSDB, null).join(250);
    
    NUMERIC_CONFIG = (NumericInterpolatorConfig)
        NumericInterpolatorConfig.newBuilder()
            .setFillPolicy(FillPolicy.NOT_A_NUMBER)
            .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
            .setDataType(NumericType.TYPE.toString())
            .build();
    
    QueryNodeConfig config = mock(QueryNodeConfig.class);
    when(config.getId()).thenReturn("mock");
    when(SRC_MOCK.config()).thenReturn(config);
    when(SRC_MOCK.initialize(any(Span.class))).thenReturn(
        Deferred.fromResult(null));
  }
  
  @Test
  public void ctor() throws Exception {
    final RateFactory factory = new RateFactory();
    assertEquals(3, factory.types().size());
    assertTrue(factory.types().contains(NumericType.TYPE));
    factory.initialize(MockTSDBDefault.getMockTSDB(), null).join(1);
    assertEquals(RateFactory.TYPE, factory.id());
  }
  
  @Test
  public void registerIteratorFactory() throws Exception {
    final RateFactory factory = new RateFactory();
    assertEquals(3, factory.types().size());
    
    QueryIteratorFactory mock = mock(QueryIteratorFactory.class);
    factory.registerIteratorFactory(NumericType.TYPE, mock);
    assertEquals(3, factory.types().size());
    
    try {
      factory.registerIteratorFactory(null, mock);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      factory.registerIteratorFactory(NumericType.TYPE, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void newIterator() throws Exception {
    final QueryResult result = mock(QueryResult.class);
    RateConfig config = (RateConfig) RateConfig.newBuilder()
        .setInterval("15s")
        .setId("foo")
        .build();
    
    final RateFactory factory = new RateFactory();
    
    final NumericMillisecondShard source = new NumericMillisecondShard(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(60000));
    source.add(30000, 24);
    source.add(60000, 42);
    final Rate node = mock(Rate.class);
    when(node.config()).thenReturn(config);

    Iterator<TimeSeriesValue<?>> iterator = factory.newTypedIterator(
        NumericType.TYPE, node, result, ImmutableMap.<String, TimeSeries>builder()
        .put("a", source)
        .build());
    assertTrue(iterator.hasNext());
    
    try {
      factory.newTypedIterator(null, node, result, ImmutableMap.<String, TimeSeries>builder()
          .put("a", source)
          .build());
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      factory.newTypedIterator(NumericType.TYPE, null, result, ImmutableMap.<String, TimeSeries>builder()
          .put("a", source)
          .build());
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      factory.newTypedIterator(NumericType.TYPE, node, result, (Map) null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      factory.newTypedIterator(NumericType.TYPE, node, result, Collections.emptyMap());
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    iterator = factory.newTypedIterator(NumericType.TYPE, node, result,
        Lists.<TimeSeries>newArrayList(source));
    assertTrue(iterator.hasNext());
    
    try {
      factory.newTypedIterator(null, node, result, Lists.<TimeSeries>newArrayList(source));
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      factory.newTypedIterator(NumericType.TYPE, null, result,
          Lists.<TimeSeries>newArrayList(source));
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      factory.newTypedIterator(NumericType.TYPE, node, result, (List) null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      factory.newTypedIterator(NumericType.TYPE, node, result, Collections.emptyList());
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void setupGraph() throws Exception {
    SemanticQuery query = SemanticQuery.newBuilder()
        .setStart("1546304400")
        .setEnd("1546308000")
        .setMode(QueryMode.SINGLE)
        .addExecutionGraphNode(DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.if.in")
                .build())
            .setId("m1")
            .build())
        .addExecutionGraphNode(RateConfig.newBuilder()
            .setInterval("1s")
            .addSource("m1")
            .setId("rate")
            .build())
        .build();
    
    QueryContext ctx = mock(QueryContext.class);
    when(ctx.stats()).thenReturn(mock(QueryStats.class));
    QueryPipelineContext context = mock(QueryPipelineContext.class);
    when(context.tsdb()).thenReturn(TSDB);
    when(context.query()).thenReturn(query);
    when(context.queryContext()).thenReturn(ctx);
    when(context.downstreamSources(any(QueryNode.class)))
        .thenReturn(Lists.newArrayList(SRC_MOCK));
    
    QueryNode ctx_node = mock(QueryNode.class);
    DefaultQueryPlanner planner = new DefaultQueryPlanner(context, ctx_node);
    planner.plan(null).join(250);
    
    assertEquals(3, planner.configGraph().nodes().size());
    Rate node = (Rate) planner.nodeForId("rate");
    assertEquals("1s", node.config().getInterval());
  }
  
  @Test
  public void setupGraphAuto() throws Exception {
    SemanticQuery query = SemanticQuery.newBuilder()
        .setStart("1546304400")
        .setEnd("1546308000")
        .setMode(QueryMode.SINGLE)
        .addExecutionGraphNode(DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.if.in")
                .build())
            .setId("m1")
            .build())
        .addExecutionGraphNode(RateConfig.newBuilder()
            .setInterval("auto")
            .addSource("m1")
            .setId("rate")
            .build())
        .build();
    
    QueryContext ctx = mock(QueryContext.class);
    when(ctx.stats()).thenReturn(mock(QueryStats.class));
    QueryPipelineContext context = mock(QueryPipelineContext.class);
    when(context.tsdb()).thenReturn(TSDB);
    when(context.query()).thenReturn(query);
    when(context.queryContext()).thenReturn(ctx);
    when(context.downstreamSources(any(QueryNode.class)))
        .thenReturn(Lists.newArrayList(SRC_MOCK));
    
    QueryNode ctx_node = mock(QueryNode.class);
    DefaultQueryPlanner planner = new DefaultQueryPlanner(context, ctx_node);
    planner.plan(null).join(250);
    
    assertEquals(3, planner.configGraph().nodes().size());
    Rate node = (Rate) planner.nodeForId("rate");
    assertEquals("1m", node.config().getInterval());
  }
}
