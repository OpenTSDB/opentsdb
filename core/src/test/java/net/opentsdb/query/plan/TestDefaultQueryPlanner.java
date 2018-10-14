// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
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
package net.opentsdb.query.plan;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.traverse.DepthFirstIterator;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.collect.Lists;

import net.opentsdb.core.DefaultRegistry;
import net.opentsdb.core.MockTSDB;
import net.opentsdb.core.TSDBPlugin;
import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryMode;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QuerySourceConfig;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.joins.JoinConfig.JoinType;
import net.opentsdb.query.joins.JoinConfig;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.processor.downsample.Downsample;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.query.processor.expressions.BinaryExpressionNode;
import net.opentsdb.query.processor.expressions.ExpressionConfig;
import net.opentsdb.query.processor.groupby.GroupBy;
import net.opentsdb.query.processor.groupby.GroupByConfig;
import net.opentsdb.query.serdes.SerdesOptions;
import net.opentsdb.storage.ReadableTimeSeriesDataStore;
import net.opentsdb.storage.TimeSeriesDataStoreFactory;

public class TestDefaultQueryPlanner {

  private static MockTSDB TSDB;
  private static TimeSeriesDataStoreFactory STORE_FACTORY;
  private static ReadableTimeSeriesDataStore STORE;
  private static NumericInterpolatorConfig NUMERIC_CONFIG;
  private static QueryNode SINK;
  private static List<TimeSeriesDataSource> STORE_NODES;
  
  private QueryPipelineContext context;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    TSDB = new MockTSDB();
    STORE_FACTORY = mock(TimeSeriesDataStoreFactory.class);
    STORE = mock(ReadableTimeSeriesDataStore.class);
    SINK = mock(QueryNode.class);
    STORE_NODES = Lists.newArrayList();
    
    TSDB.registry = new DefaultRegistry(TSDB);
    ((DefaultRegistry) TSDB.registry).initialize(true);
    ((DefaultRegistry) TSDB.registry).registerPlugin(
        TimeSeriesDataStoreFactory.class, null, (TSDBPlugin) STORE_FACTORY);
    ((DefaultRegistry) TSDB.registry).registerReadStore(STORE, null);
    
    when(STORE_FACTORY.newInstance(TSDB, null)).thenReturn(STORE);
    when(STORE.newNode(any(QueryPipelineContext.class), anyString(), 
        any(QueryNodeConfig.class)))
      .thenAnswer(new Answer<QueryNode>() {
        @Override
        public QueryNode answer(InvocationOnMock invocation) throws Throwable {
          final TimeSeriesDataSource node = mock(TimeSeriesDataSource.class);
          when(node.config()).thenReturn((QueryNodeConfig) invocation.getArguments()[2]);
          STORE_NODES.add(node);
          return node;
        }
      });
    
    NUMERIC_CONFIG = (NumericInterpolatorConfig) 
        NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
        .setDataType(NumericType.TYPE.toString())
        .build();
    
    QueryNodeConfig sink_config = mock(QueryNodeConfig.class);
    when(sink_config.getId()).thenReturn("SINK");
    when(SINK.config()).thenReturn(sink_config);
  }
  
  @Before
  public void before() throws Exception {
    context = mock(QueryPipelineContext.class);
    when(context.tsdb()).thenReturn(TSDB);
    
    STORE_NODES.clear();
    when(STORE_FACTORY.supportsPushdown(any(Class.class)))
      .thenReturn(false);
  }
  
  @Test
  public void oneMetricOneGraphNoPushdown() throws Exception {
    List<QueryNodeConfig> graph = Lists.newArrayList(
        QuerySourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setFilterId("f1")
            .setId("m1")
            .build(),
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds")
            .addSource("m1")
            .build(),
        GroupByConfig.newBuilder()
            .setAggregator("sum")
            .addTagKey("host")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("gb")
            .addSource("ds")
            .build());
    
    SemanticQuery query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .setExecutionGraph(graph)
        .addSerdesConfig(serdesConfigs(Lists.newArrayList("gb")))
        .build();
    
    when(context.query()).thenReturn(query);
    
    DefaultQueryPlanner planner = 
        new DefaultQueryPlanner(context, SINK);
    planner.plan(null).join();
    
    // validate
    assertSame(STORE_NODES.get(0), planner.sources().get(0));
    assertEquals(4, planner.graph().vertexSet().size());
    assertTrue(planner.graph().containsEdge(SINK, planner.nodesMap().get("gb")));
    assertFalse(planner.graph().containsEdge(SINK, planner.nodesMap().get("ds")));
    assertFalse(planner.graph().containsEdge(SINK, planner.nodesMap().get("m1")));
    assertTrue(planner.graph().containsEdge(planner.nodesMap().get("ds"), 
        planner.nodesMap().get("m1")));
    assertTrue(planner.graph().containsEdge(planner.nodesMap().get("gb"), 
        planner.nodesMap().get("ds")));
    
    assertEquals(1, planner.serializationSources().size());
    
    DepthFirstIterator<QueryNode, DefaultEdge> iterator = 
        new DepthFirstIterator<QueryNode, DefaultEdge>(planner.graph());
    QueryNode node = iterator.next();
    assertSame(SINK, node);
    
    node = iterator.next();
    assertTrue(node instanceof GroupBy);
    
    node = iterator.next();
    assertTrue(node instanceof Downsample);
    assertEquals(1514764800, ((DownsampleConfig) node.config()).startTime().epoch());
    assertEquals(1514768400, ((DownsampleConfig) node.config()).endTime().epoch());
    
    node = iterator.next();
    assertSame(STORE_NODES.get(0), node);
    
    // no filter
    query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .setExecutionGraph(graph)
        .build();
    when(context.query()).thenReturn(query);
    planner = new DefaultQueryPlanner(context, SINK);
    planner.plan(null).join();
    assertEquals(1, planner.serializationSources().size());
  }
  
  @Test
  public void oneMetricOneGraphPushdown() throws Exception {
    when(STORE_FACTORY.supportsPushdown(DownsampleConfig.class))
      .thenReturn(true);
    
    List<QueryNodeConfig> graph = Lists.newArrayList(
        QuerySourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setFilterId("f1")
            .setId("m1")
            .build(),
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds")
            .addSource("m1")
            .build(),
        GroupByConfig.newBuilder()
            .setAggregator("sum")
            .addTagKey("host")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("gb")
            .addSource("ds")
            .build());
    
    SemanticQuery query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .setExecutionGraph(graph)
        .addSerdesConfig(serdesConfigs(Lists.newArrayList("gb")))
        .build();
    
    when(context.query()).thenReturn(query);
    
    DefaultQueryPlanner planner = 
        new DefaultQueryPlanner(context, SINK);
    planner.plan(null).join();
    
    // validate
    assertSame(STORE_NODES.get(0), planner.sources().get(0));
    assertEquals(3, planner.graph().vertexSet().size());
    assertTrue(planner.graph().containsEdge(SINK, planner.nodesMap().get("gb")));
    assertFalse(planner.graph().containsEdge(SINK, planner.nodesMap().get("ds")));
    assertFalse(planner.graph().containsEdge(SINK, planner.nodesMap().get("m1")));
    assertFalse(planner.graph().containsEdge(planner.nodesMap().get("downsample"), 
        planner.nodesMap().get("m1"))); // pushed
    assertFalse(planner.graph().containsEdge(planner.nodesMap().get("gb"), 
        planner.nodesMap().get("downsample")));
    
    assertEquals(1, planner.serializationSources().size());
    
    DepthFirstIterator<QueryNode, DefaultEdge> iterator = 
        new DepthFirstIterator<QueryNode, DefaultEdge>(planner.graph());
    QueryNode node = iterator.next();
    assertSame(SINK, node);
    
    node = iterator.next();
    assertTrue(node instanceof GroupBy);

    node = iterator.next();
    assertSame(STORE_NODES.get(0), node);
    QuerySourceConfig source_config = (QuerySourceConfig) STORE_NODES.get(0).config();
    assertEquals(1, source_config.getPushDownNodes().size());
    assertTrue(source_config.getPushDownNodes().get(0) instanceof DownsampleConfig);
    assertEquals("ds", source_config.getPushDownNodes().get(0).getId());
    
    // no filter
    query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .setExecutionGraph(graph)
        .build();
    when(context.query()).thenReturn(query);
    planner = new DefaultQueryPlanner(context, SINK);
    planner.plan(null).join();
    assertEquals(1, planner.serializationSources().size());
  }
  
  @Test
  public void oneMetricOneGraphPushdownAll() throws Exception {
    when(STORE_FACTORY.supportsPushdown(DownsampleConfig.class))
      .thenReturn(true);
    
    List<QueryNodeConfig> graph = Lists.newArrayList(
        QuerySourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setFilterId("f1")
            .setId("m1")
            .build(),
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("downsample")
            .addSource("m1")
            .build());
    
    SemanticQuery query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .setExecutionGraph(graph)
        .addSerdesConfig(serdesConfigs(Lists.newArrayList("downsample")))
        .build();
    
    when(context.query()).thenReturn(query);
    
    DefaultQueryPlanner planner = 
        new DefaultQueryPlanner(context, SINK);
    planner.plan(null).join();
    
    // validate
    assertSame(STORE_NODES.get(0), planner.sources().get(0));
    assertEquals(2, planner.graph().vertexSet().size());
    assertFalse(planner.graph().containsEdge(SINK, planner.nodesMap().get("downsample")));
    assertTrue(planner.graph().containsEdge(SINK, planner.nodesMap().get("m1")));
    assertFalse(planner.graph().containsEdge(planner.nodesMap().get("downsample"), 
        planner.nodesMap().get("m1")));
    
    assertEquals(1, planner.serializationSources().size());
    
    DepthFirstIterator<QueryNode, DefaultEdge> iterator = 
        new DepthFirstIterator<QueryNode, DefaultEdge>(planner.graph());
    QueryNode node = iterator.next();
    assertSame(SINK, node);
    
    node = iterator.next();
    assertSame(STORE_NODES.get(0), node);
    QuerySourceConfig source_config = (QuerySourceConfig) STORE_NODES.get(0).config();
    assertEquals(1, source_config.getPushDownNodes().size());
    assertTrue(source_config.getPushDownNodes().get(0) instanceof DownsampleConfig);
    assertEquals("downsample", source_config.getPushDownNodes().get(0).getId());
    
    // no filter
    query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .setExecutionGraph(graph)
        .build();
    when(context.query()).thenReturn(query);
    planner = new DefaultQueryPlanner(context, SINK);
    planner.plan(null).join();
    assertEquals(1, planner.serializationSources().size());
  }

  @Test
  public void twoMetricsAlone() throws Exception {
    List<QueryNodeConfig> graph = Lists.newArrayList(
        QuerySourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setFilterId("f1")
            .setId("m1")
            .build(),
        QuerySourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.sys")
                .build())
            .setFilterId("f1")
            .setId("m2")
            .build());
    
    SemanticQuery query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .setExecutionGraph(graph)
        .addSerdesConfig(serdesConfigs(Lists.newArrayList("m1", "m2")))
        .build();
    
    when(context.query()).thenReturn(query);

    DefaultQueryPlanner planner = 
        new DefaultQueryPlanner(context, SINK);
    planner.plan(null).join();
    
    // validate
    assertEquals(2, planner.sources().size());
    assertTrue(planner.sources().contains(STORE_NODES.get(0)));
    assertTrue(planner.sources().contains(STORE_NODES.get(1)));
    assertEquals(3, planner.graph().vertexSet().size());

    assertEquals(2, planner.serializationSources().size());
    
    DepthFirstIterator<QueryNode, DefaultEdge> iterator = 
        new DepthFirstIterator<QueryNode, DefaultEdge>(planner.graph());
    QueryNode node = iterator.next();
    assertSame(SINK, node);

    node = iterator.next();
    assertSame(STORE_NODES.get(1), node);
    
    node = iterator.next();
    assertSame(STORE_NODES.get(0), node);
    
    // no filter
    query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .setExecutionGraph(graph)
        .build();
    when(context.query()).thenReturn(query);
    planner = new DefaultQueryPlanner(context, SINK);
    planner.plan(null).join();
    assertEquals(2, planner.serializationSources().size());
  }
  
  @Test
  public void twoMetricsOneGraphNoPushdown() throws Exception {
    List<QueryNodeConfig> graph = Lists.newArrayList(
        QuerySourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setFilterId("f1")
            .setId("m1")
            .build(),
        QuerySourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.sys")
                .build())
            .setFilterId("f1")
            .setId("m2")
            .build(),
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("downsample")
            .addSource("m1")
            .addSource("m2")
            .build(),
        GroupByConfig.newBuilder()
            .setAggregator("sum")
            .addTagKey("host")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("gb")
            .addSource("downsample")
            .build());
    
    SemanticQuery query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .setExecutionGraph(graph)
        .addSerdesConfig(serdesConfigs(Lists.newArrayList("gb")))
        .build();
    
    when(context.query()).thenReturn(query);

    DefaultQueryPlanner planner = 
        new DefaultQueryPlanner(context, SINK);
    planner.plan(null).join();
    
    // validate
    assertEquals(2, planner.sources().size());
    assertTrue(planner.sources().contains(STORE_NODES.get(0)));
    assertTrue(planner.sources().contains(STORE_NODES.get(1)));
    assertEquals(5, planner.graph().vertexSet().size());

    assertEquals(2, planner.serializationSources().size());
    
    DepthFirstIterator<QueryNode, DefaultEdge> iterator = 
        new DepthFirstIterator<QueryNode, DefaultEdge>(planner.graph());
    QueryNode node = iterator.next();
    assertSame(SINK, node);
    
    node = iterator.next();
    assertTrue(node instanceof GroupBy);
    
    node = iterator.next();
    assertTrue(node instanceof Downsample);
    assertEquals(1514764800, ((DownsampleConfig) node.config()).startTime().epoch());
    assertEquals(1514768400, ((DownsampleConfig) node.config()).endTime().epoch());
    
    // TODO - watch this bit for ordering
    node = iterator.next();
    assertSame(STORE_NODES.get(1), node);
    
    node = iterator.next();
    assertSame(STORE_NODES.get(0), node);
    
    // no filter
    query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .setExecutionGraph(graph)
        .build();
    when(context.query()).thenReturn(query);
    planner = new DefaultQueryPlanner(context, SINK);
    planner.plan(null).join();
    assertEquals(2, planner.serializationSources().size());
  }

  @Test
  public void twoMetricsOneGraphPushdownCommon() throws Exception {
    when(STORE_FACTORY.supportsPushdown(DownsampleConfig.class))
      .thenReturn(true);
    
    List<QueryNodeConfig> graph = Lists.newArrayList(
        QuerySourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setFilterId("f1")
            .setId("m1")
            .build(),
       QuerySourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.sys")
                .build())
            .setFilterId("f1")
            .setId("m2")
            .build(),
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("downsample")
            .addSource("m1")
            .addSource("m2")
            .build(),
        GroupByConfig.newBuilder()
            .setAggregator("sum")
            .addTagKey("host")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("gb")
            .addSource("downsample")
            .build());
    
    SemanticQuery query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .setExecutionGraph(graph)
        .addSerdesConfig(serdesConfigs(Lists.newArrayList("gb")))
        .build();
    
    when(context.query()).thenReturn(query);

    DefaultQueryPlanner planner = 
        new DefaultQueryPlanner(context, SINK);
    planner.plan(null).join();
    
    // validate
    assertEquals(2, planner.sources().size());
    assertTrue(planner.sources().contains(STORE_NODES.get(0)));
    assertTrue(planner.sources().contains(STORE_NODES.get(1)));
    assertEquals(4, planner.graph().vertexSet().size());

    assertEquals(2, planner.serializationSources().size());
    
    DepthFirstIterator<QueryNode, DefaultEdge> iterator = 
        new DepthFirstIterator<QueryNode, DefaultEdge>(planner.graph());
    QueryNode node = iterator.next();
    assertSame(SINK, node);
    
    node = iterator.next();
    assertTrue(node instanceof GroupBy);

    // TODO - watch this bit for ordering
    node = iterator.next();
    assertSame(STORE_NODES.get(1), node);
    QuerySourceConfig source_config = (QuerySourceConfig) STORE_NODES.get(1).config();
    assertEquals(1, source_config.getPushDownNodes().size());
    assertTrue(source_config.getPushDownNodes().get(0) instanceof DownsampleConfig);
    assertEquals("downsample", source_config.getPushDownNodes().get(0).getId());
    
    node = iterator.next();
    assertSame(STORE_NODES.get(0), node);
    source_config = (QuerySourceConfig) STORE_NODES.get(0).config();
    assertEquals(1, source_config.getPushDownNodes().size());
    assertTrue(source_config.getPushDownNodes().get(0) instanceof DownsampleConfig);
    assertEquals("downsample", source_config.getPushDownNodes().get(0).getId());
    
    // no filter
    query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .setExecutionGraph(graph)
        .build();
    when(context.query()).thenReturn(query);
    planner = new DefaultQueryPlanner(context, SINK);
    planner.plan(null).join();
    assertEquals(2, planner.serializationSources().size());
  }
  
  @Test
  public void twoMetricsOneGraphPushdownNotCommon() throws Exception {
    when(STORE_FACTORY.supportsPushdown(DownsampleConfig.class))
      .thenReturn(true);
    
    List<QueryNodeConfig> graph = Lists.newArrayList(
        QuerySourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setFilterId("f1")
            .setId("m1")
            .build(),
        QuerySourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.sys")
                .build())
            .setFilterId("f1")
            .setId("m2")
            .build(),
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds1")
            .addSource("m1")
            .build(),
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("2m")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds2")
            .addSource("m2")
            .build(),
        GroupByConfig.newBuilder()
            .setAggregator("sum")
            .addTagKey("host")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("gb")
            .addSource("ds1")
            .addSource("ds2")
            .build());
    
    SemanticQuery query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .setExecutionGraph(graph)
        .addSerdesConfig(serdesConfigs(Lists.newArrayList("gb")))
        .build();
    
    when(context.query()).thenReturn(query);

    DefaultQueryPlanner planner = 
        new DefaultQueryPlanner(context, SINK);
    planner.plan(null).join();
    
    // validate
    assertEquals(2, planner.sources().size());
    assertTrue(planner.sources().contains(STORE_NODES.get(0)));
    assertTrue(planner.sources().contains(STORE_NODES.get(1)));
    assertEquals(4, planner.graph().vertexSet().size());

    assertEquals(2, planner.serializationSources().size());
    
    DepthFirstIterator<QueryNode, DefaultEdge> iterator = 
        new DepthFirstIterator<QueryNode, DefaultEdge>(planner.graph());
    QueryNode node = iterator.next();
    assertSame(SINK, node);
    
    node = iterator.next();
    assertTrue(node instanceof GroupBy);

    // TODO - watch this bit for ordering
    node = iterator.next();
    assertSame(STORE_NODES.get(1), node);
    QuerySourceConfig source_config = (QuerySourceConfig) STORE_NODES.get(1).config();
    assertEquals(1, source_config.getPushDownNodes().size());
    assertTrue(source_config.getPushDownNodes().get(0) instanceof DownsampleConfig);
    assertEquals("ds2", source_config.getPushDownNodes().get(0).getId());
    
    node = iterator.next();
    assertSame(STORE_NODES.get(0), node);
    source_config = (QuerySourceConfig) STORE_NODES.get(0).config();
    assertEquals(1, source_config.getPushDownNodes().size());
    assertTrue(source_config.getPushDownNodes().get(0) instanceof DownsampleConfig);
    assertEquals("ds1", source_config.getPushDownNodes().get(0).getId());
    
    // no filter
    query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .setExecutionGraph(graph)
        .build();
    when(context.query()).thenReturn(query);
    planner = new DefaultQueryPlanner(context, SINK);
    planner.plan(null).join();
    assertEquals(2, planner.serializationSources().size());
  }

  @Test
  public void twoMetricsOneGraphPushdownAll() throws Exception {
    when(STORE_FACTORY.supportsPushdown(DownsampleConfig.class))
      .thenReturn(true);
    
    List<QueryNodeConfig> graph = Lists.newArrayList(
        QuerySourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setFilterId("f1")
            .setId("m1")
            .build(),
        QuerySourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.sys")
                .build())
            .setFilterId("f1")
            .setId("m2")
            .build(),
       DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("downsample")
            .addSource("m1")
            .addSource("m2")
            .build());
    
    SemanticQuery query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .setExecutionGraph(graph)
        .addSerdesConfig(serdesConfigs(Lists.newArrayList("downsample")))
        .build();
    
    when(context.query()).thenReturn(query);

    DefaultQueryPlanner planner = 
        new DefaultQueryPlanner(context, SINK);
    planner.plan(null).join();
    
    // validate
    assertEquals(2, planner.sources().size());
    assertTrue(planner.sources().contains(STORE_NODES.get(0)));
    assertTrue(planner.sources().contains(STORE_NODES.get(1)));
    assertEquals(3, planner.graph().vertexSet().size());

    assertEquals(2, planner.serializationSources().size());
    
    DepthFirstIterator<QueryNode, DefaultEdge> iterator = 
        new DepthFirstIterator<QueryNode, DefaultEdge>(planner.graph());
    QueryNode node = iterator.next();
    assertSame(SINK, node);
    
    // TODO - watch this bit for ordering
    node = iterator.next();
    assertSame(STORE_NODES.get(1), node);
    QuerySourceConfig source_config = (QuerySourceConfig) STORE_NODES.get(1).config();
    assertEquals(1, source_config.getPushDownNodes().size());
    assertTrue(source_config.getPushDownNodes().get(0) instanceof DownsampleConfig);
    assertEquals("downsample", source_config.getPushDownNodes().get(0).getId());
    
    node = iterator.next();
    assertSame(STORE_NODES.get(0), node);
    source_config = (QuerySourceConfig) STORE_NODES.get(0).config();
    assertEquals(1, source_config.getPushDownNodes().size());
    assertTrue(source_config.getPushDownNodes().get(0) instanceof DownsampleConfig);
    assertEquals("downsample", source_config.getPushDownNodes().get(0).getId());
    
    // no filter
    query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .setExecutionGraph(graph)
        .build();
    when(context.query()).thenReturn(query);
    planner = new DefaultQueryPlanner(context, SINK);
    planner.plan(null).join();
    assertEquals(2, planner.serializationSources().size());
  }
  
  @Test
  public void twoMetricsTwoGraphs() throws Exception {
    List<QueryNodeConfig> graph = Lists.newArrayList(
        QuerySourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setFilterId("f1")
            .setId("m1")
            .build(),
        QuerySourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.sys")
                .build())
            .setFilterId("f1")
            .setId("m2")
            .build(),
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds1")
            .addSource("m1")
            .build(),
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds2")
            .addSource("m2")
            .build(),
        GroupByConfig.newBuilder()
            .setAggregator("sum")
            .addTagKey("host")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("gb1")
            .addSource("ds1")
            .build(),
        GroupByConfig.newBuilder()
            .setAggregator("sum")
            .addTagKey("host")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("gb2")
            .addSource("ds2")
            .build());
    
    SemanticQuery query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .setExecutionGraph(graph)
        .addSerdesConfig(serdesConfigs(Lists.newArrayList("gb1", "gb2")))
        .build();
    
    when(context.query()).thenReturn(query);

    DefaultQueryPlanner planner = 
        new DefaultQueryPlanner(context, SINK);
    planner.plan(null).join();
    
    // validate
    assertEquals(2, planner.sources().size());
    assertTrue(planner.sources().contains(STORE_NODES.get(0)));
    assertTrue(planner.sources().contains(STORE_NODES.get(1)));
    assertEquals(7, planner.graph().vertexSet().size());

    assertEquals(2, planner.serializationSources().size());
    
    DepthFirstIterator<QueryNode, DefaultEdge> iterator = 
        new DepthFirstIterator<QueryNode, DefaultEdge>(planner.graph());
    QueryNode node = iterator.next();
    assertSame(SINK, node);
    
    node = iterator.next();
    assertTrue(node instanceof GroupBy);
    
    node = iterator.next();
    assertTrue(node instanceof Downsample);
    assertEquals(1514764800, ((DownsampleConfig) node.config()).startTime().epoch());
    assertEquals(1514768400, ((DownsampleConfig) node.config()).endTime().epoch());
    
    // TODO - watch this bit for ordering
    node = iterator.next();
    assertSame(STORE_NODES.get(1), node);
    
    node = iterator.next();
    assertTrue(node instanceof GroupBy);
    
    node = iterator.next();
    assertTrue(node instanceof Downsample);
    assertEquals(1514764800, ((DownsampleConfig) node.config()).startTime().epoch());
    assertEquals(1514768400, ((DownsampleConfig) node.config()).endTime().epoch());
    
    node = iterator.next();
    assertSame(STORE_NODES.get(0), node);
    
    // no filter
    query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .setExecutionGraph(graph)
        .build();
    when(context.query()).thenReturn(query);
    planner = new DefaultQueryPlanner(context, SINK);
    planner.plan(null).join();
    assertEquals(2, planner.serializationSources().size());
  }
  
  @Test
  public void twoMetricsFilterGBandRaw() throws Exception {
    List<QueryNodeConfig> graph = Lists.newArrayList(
        QuerySourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setFilterId("f1")
            .setId("m1")
            .build(),
        QuerySourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.sys")
                .build())
            .setFilterId("f1")
            .setId("m2")
            .build(),
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("downsample")
            .addSource("m1")
            .addSource("m2")
            .build(),
        GroupByConfig.newBuilder()
            .setAggregator("sum")
            .addTagKey("host")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("gb")
            .addSource("downsample")
            .build());
    
    SemanticQuery query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .setExecutionGraph(graph)
        .addSerdesConfig(serdesConfigs(Lists.newArrayList("gb", "m1", "m2")))
        .build();
    
    when(context.query()).thenReturn(query);

    DefaultQueryPlanner planner = 
        new DefaultQueryPlanner(context, SINK);
    planner.plan(null).join();
    
    // validate
    assertEquals(2, planner.sources().size());
    assertTrue(planner.sources().contains(STORE_NODES.get(0)));
    assertTrue(planner.sources().contains(STORE_NODES.get(1)));
    assertEquals(5, planner.graph().vertexSet().size());
    
    assertTrue(planner.graph().containsEdge(SINK, planner.nodesMap().get("gb")));
    assertTrue(planner.graph().containsEdge(SINK, planner.nodesMap().get("m1")));
    assertTrue(planner.graph().containsEdge(SINK, planner.nodesMap().get("m2")));
    assertTrue(planner.graph().containsEdge(planner.nodesMap().get("downsample"), 
        planner.nodesMap().get("m2")));
    assertTrue(planner.graph().containsEdge(planner.nodesMap().get("gb"), 
        planner.nodesMap().get("downsample")));

    assertEquals(4, planner.serializationSources().size());
    
    DepthFirstIterator<QueryNode, DefaultEdge> iterator = 
        new DepthFirstIterator<QueryNode, DefaultEdge>(planner.graph());
    QueryNode node = iterator.next();
    assertSame(SINK, node);
    
    node = iterator.next();
    assertTrue(node instanceof GroupBy);
    
    node = iterator.next();
    assertTrue(node instanceof Downsample);
    assertEquals(1514764800, ((DownsampleConfig) node.config()).startTime().epoch());
    assertEquals(1514768400, ((DownsampleConfig) node.config()).endTime().epoch());
    
    // TODO - watch this bit for ordering
    node = iterator.next();
    assertSame(STORE_NODES.get(1), node);
    
    node = iterator.next();
    assertSame(STORE_NODES.get(0), node);
    
    // no filter
    query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .setExecutionGraph(graph)
        .build();
    when(context.query()).thenReturn(query);
    planner = new DefaultQueryPlanner(context, SINK);
    planner.plan(null).join();
    assertEquals(2, planner.serializationSources().size());
  }
  
  @Test
  public void twoMetricsExpression() throws Exception {
    List<QueryNodeConfig> graph = Lists.newArrayList(
        QuerySourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setFilterId("f1")
            .setId("m1")
            .build(),
        QuerySourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.sys")
                .build())
            .setFilterId("f1")
            .setId("m2")
            .build(),
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("downsample")
            .addSource("m1")
            .addSource("m2")
            .build(),
        GroupByConfig.newBuilder()
            .setAggregator("sum")
            .addTagKey("host")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("groupby")
            .addSource("downsample")
            .build(),
       ExpressionConfig.newBuilder()
            .setExpression("sys.cpu.user + sys.cpu.sys")
            .setAs("sys.tot")
            .setJoinConfig((JoinConfig) JoinConfig.newBuilder()
                .setJoinType(JoinType.NATURAL)
                .build())
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("expression")
            .addSource("groupby")
            .build());
    
    SemanticQuery query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .setExecutionGraph(graph)
        .addSerdesConfig(serdesConfigs(Lists.newArrayList("expression")))
        .build();
    
    when(context.query()).thenReturn(query);

    DefaultQueryPlanner planner = 
        new DefaultQueryPlanner(context, SINK);
    planner.plan(null).join();
    
    // validate
    assertEquals(2, planner.sources().size());
    assertTrue(planner.sources().contains(STORE_NODES.get(0)));
    assertTrue(planner.sources().contains(STORE_NODES.get(1)));
    assertEquals(6, planner.graph().vertexSet().size());

    assertEquals(1, planner.serializationSources().size());
    
    DepthFirstIterator<QueryNode, DefaultEdge> iterator = 
        new DepthFirstIterator<QueryNode, DefaultEdge>(planner.graph());
    QueryNode node = iterator.next();
    assertSame(SINK, node);
    
    node = iterator.next();
    assertTrue(node instanceof BinaryExpressionNode);
    
    node = iterator.next();
    assertTrue(node instanceof GroupBy);
    
    node = iterator.next();
    assertTrue(node instanceof Downsample);
    assertEquals(1514764800, ((DownsampleConfig) node.config()).startTime().epoch());
    assertEquals(1514768400, ((DownsampleConfig) node.config()).endTime().epoch());
    
    // TODO - watch this bit for ordering
    node = iterator.next();
    assertSame(STORE_NODES.get(1), node);
    
    node = iterator.next();
    assertSame(STORE_NODES.get(0), node);
    
    // no filter
    query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .setExecutionGraph(graph)
        .build();
    when(context.query()).thenReturn(query);
    planner = new DefaultQueryPlanner(context, SINK);
    planner.plan(null).join();
    assertEquals(1, planner.serializationSources().size());
  }
  
  @Test
  public void twoMetricsExpressionWithFilter() throws Exception {
    List<QueryNodeConfig> graph = Lists.newArrayList(
        QuerySourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setFilterId("f1")
            .setId("m1")
            .build(),
        QuerySourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.sys")
                .build())
            .setFilterId("f1")
            .setId("m2")
            .build(),
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("downsample")
            .addSource("m1")
            .addSource("m2")
            .build(),
        GroupByConfig.newBuilder()
            .setAggregator("sum")
            .addTagKey("host")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("groupby")
            .addSource("downsample")
            .build(),
        ExpressionConfig.newBuilder()
            .setExpression("sys.cpu.user + sys.cpu.sys")
            .setAs("sys.tot")
            .setJoinConfig((JoinConfig) JoinConfig.newBuilder()
                .setJoinType(JoinType.NATURAL)
                .build())
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("expression")
            .addSource("groupby")
            .build());
    
    SemanticQuery query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .setExecutionGraph(graph)
        .addSerdesConfig(serdesConfigs(Lists.newArrayList("expression", "m1", "m2")))
        .build();
    
    when(context.query()).thenReturn(query);

    DefaultQueryPlanner planner = 
        new DefaultQueryPlanner(context, SINK);
    planner.plan(null).join();
    
    // validate
    assertEquals(2, planner.sources().size());
    assertTrue(planner.sources().contains(STORE_NODES.get(0)));
    assertTrue(planner.sources().contains(STORE_NODES.get(1)));
    assertEquals(6, planner.graph().vertexSet().size());

    assertEquals(3, planner.serializationSources().size());
    
    DepthFirstIterator<QueryNode, DefaultEdge> iterator = 
        new DepthFirstIterator<QueryNode, DefaultEdge>(planner.graph());
    QueryNode node = iterator.next();
    assertSame(SINK, node);
    
    node = iterator.next();
    assertTrue(node instanceof BinaryExpressionNode);
    
    node = iterator.next();
    assertTrue(node instanceof GroupBy);
    
    node = iterator.next();
    assertTrue(node instanceof Downsample);
    assertEquals(1514764800, ((DownsampleConfig) node.config()).startTime().epoch());
    assertEquals(1514768400, ((DownsampleConfig) node.config()).endTime().epoch());
    
    // TODO - watch this bit for ordering
    node = iterator.next();
    assertSame(STORE_NODES.get(1), node);
    
    node = iterator.next();
    assertSame(STORE_NODES.get(0), node);
    
    // no filter
    query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .setExecutionGraph(graph)
        .build();
    when(context.query()).thenReturn(query);
    planner = new DefaultQueryPlanner(context, SINK);
    planner.plan(null).join();
    assertEquals(1, planner.serializationSources().size());
  }
  
  @Test
  public void cycleFound() throws Exception {
    List<QueryNodeConfig> graph = Lists.newArrayList(
        QuerySourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setFilterId("f1")
            .setId("m1")
            .build(),
       DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("downsample")
            .addSource("groupby")
            .build(),
       GroupByConfig.newBuilder()
            .setAggregator("sum")
            .addTagKey("host")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("gb")
            .addSource("downsample")
            .build());
    
    SemanticQuery query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .setExecutionGraph(graph)
        .addSerdesConfig(serdesConfigs(Lists.newArrayList("gb")))
        .build();
    
    when(context.query()).thenReturn(query);
    
    DefaultQueryPlanner planner = 
        new DefaultQueryPlanner(context, SINK);
    try {
      planner.plan(null).join();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    assertNull(planner.graph());
  }
  
  @Test
  public void duplicateNodeIds() throws Exception {
    List<QueryNodeConfig> graph = Lists.newArrayList(
        QuerySourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setFilterId("f1")
            .setId("m1")
            .build(),
        QuerySourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.sys")
                .build())
            .setFilterId("f1")
            .setId("m1")
            .build(),
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("downsample")
            .addSource("m2")
            .build(),
        GroupByConfig.newBuilder()
            .setAggregator("sum")
            .addTagKey("host")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("gb")
            .addSource("downsample")
            .build());
    
    SemanticQuery query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .setExecutionGraph(graph)
        .addSerdesConfig(serdesConfigs(Lists.newArrayList("gb")))
        .build();
    
    when(context.query()).thenReturn(query);
    
    DefaultQueryPlanner planner = 
        new DefaultQueryPlanner(context, SINK);
    try {
      planner.plan(null).join();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    assertNull(planner.graph());
  }
  
  @Test
  public void unsatisfiedFilter() throws Exception {
    List<QueryNodeConfig> graph = Lists.newArrayList(
        QuerySourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setFilterId("f1")
            .setId("m1")
            .build(),
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("downsample")
            .addSource("m1")
            .build(),
        GroupByConfig.newBuilder()
            .setAggregator("sum")
            .addTagKey("host")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("gb")
            .addSource("downsample")
            .build());
    
    SemanticQuery query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .setExecutionGraph(graph)
        .addSerdesConfig(serdesConfigs(Lists.newArrayList("nosuchnode")))
        .build();
    
    when(context.query()).thenReturn(query);
    
    DefaultQueryPlanner planner = 
        new DefaultQueryPlanner(context, SINK);
    try {
      planner.plan(null).join();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    assertNull(planner.graph());
  }
  
  private SerdesOptions serdesConfigs(final List<String> filter) {
    final SerdesOptions config = mock(SerdesOptions.class);
    when(config.getFilter()).thenReturn(filter);
    return config;
  }
}
