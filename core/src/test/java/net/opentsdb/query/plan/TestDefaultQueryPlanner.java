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
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
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
import net.opentsdb.query.execution.graph.ExecutionGraph;
import net.opentsdb.query.execution.graph.ExecutionGraphNode;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.joins.JoinConfig.JoinType;
import net.opentsdb.query.joins.JoinConfig;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.processor.downsample.Downsample;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.query.processor.expressions.ExpressionConfig;
import net.opentsdb.query.processor.groupby.GroupBy;
import net.opentsdb.query.processor.groupby.GroupByConfig;
import net.opentsdb.storage.ReadableTimeSeriesDataStore;
import net.opentsdb.storage.TimeSeriesDataStoreFactory;

public class TestDefaultQueryPlanner {

  private static MockTSDB TSDB;
  private static TimeSeriesDataStoreFactory STORE_FACTORY;
  private static ReadableTimeSeriesDataStore STORE;
  private static NumericInterpolatorConfig NUMERIC_CONFIG;
  private static QueryNode SINK;
  
  private QueryPipelineContext context;
  private static List<TimeSeriesDataSource> STORE_NODES;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    TSDB = new MockTSDB();
    STORE_FACTORY = mock(TimeSeriesDataStoreFactory.class);
    STORE = mock(ReadableTimeSeriesDataStore.class);
    //STORE_NODE = mock(TimeSeriesDataSource.class);
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
    ExecutionGraph graph = ExecutionGraph.newBuilder()
        .setId("g1")
        .addNode(ExecutionGraphNode.newBuilder()
            .setId("m1")
            .setType("DataSource")
            .setConfig(QuerySourceConfig.newBuilder()
                .setMetric(MetricLiteralFilter.newBuilder()
                    .setMetric("sys.cpu.user")
                    .build())
                .setFilterId("f1")
                .setId("m1")
                .build()))
        .addNode(ExecutionGraphNode.newBuilder()
            .setId("downsample")
            .addSource("m1")
            .setConfig(DownsampleConfig.newBuilder()
                .setAggregator("sum")
                .setInterval("1m")
                .addInterpolatorConfig(NUMERIC_CONFIG)
                .setId("downsample")
                .build())
            .build())
        .addNode(ExecutionGraphNode.newBuilder()
            .setId("groupby")
            .addSource("downsample")
            .setConfig(GroupByConfig.newBuilder()
                .setAggregator("sum")
                .addTagKey("host")
                .addInterpolatorConfig(NUMERIC_CONFIG)
                .setId("gb")
                .build()))
        .build();
    
    SemanticQuery query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .setExecutionGraph(graph)
        .build();
    
    when(context.query()).thenReturn(query);
    
    DefaultQueryPlanner planner = 
        new DefaultQueryPlanner(context, Lists.newArrayList(SINK));
    planner.plan(null);
    
    // validate
    assertSame(STORE_NODES.get(0), planner.sources().get(0));
    assertEquals(4, planner.graph().vertexSet().size());
    
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
    
    assertEquals(1, planner.roots().size());
    assertTrue(planner.roots().get(0) instanceof GroupBy);
  }
  
  @Test
  public void oneMetricOneGraphPushdown() throws Exception {
    when(STORE_FACTORY.supportsPushdown(DownsampleConfig.class))
      .thenReturn(true);
    
    ExecutionGraph graph = ExecutionGraph.newBuilder()
        .setId("g1")
        .addNode(ExecutionGraphNode.newBuilder()
            .setId("m1")
            .setType("DataSource")
            .setConfig(QuerySourceConfig.newBuilder()
                .setMetric(MetricLiteralFilter.newBuilder()
                    .setMetric("sys.cpu.user")
                    .build())
                .setFilterId("f1")
                .setId("m1")
                .build()))
        .addNode(ExecutionGraphNode.newBuilder()
            .setId("downsample")
            .addSource("m1")
            .setConfig(DownsampleConfig.newBuilder()
                .setAggregator("sum")
                .setInterval("1m")
                .addInterpolatorConfig(NUMERIC_CONFIG)
                .setId("downsample")
                .build())
            .build())
        .addNode(ExecutionGraphNode.newBuilder()
            .setId("groupby")
            .addSource("downsample")
            .setConfig(GroupByConfig.newBuilder()
                .setAggregator("sum")
                .addTagKey("host")
                .addInterpolatorConfig(NUMERIC_CONFIG)
                .setId("gb")
                .build()))
        .build();
    
    SemanticQuery query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .setExecutionGraph(graph)
        .build();
    
    when(context.query()).thenReturn(query);
    
    DefaultQueryPlanner planner = 
        new DefaultQueryPlanner(context, Lists.newArrayList(SINK));
    planner.plan(null);
    
    // validate
    assertSame(STORE_NODES.get(0), planner.sources().get(0));
    assertEquals(3, planner.graph().vertexSet().size());
    
    DepthFirstIterator<QueryNode, DefaultEdge> iterator = 
        new DepthFirstIterator<QueryNode, DefaultEdge>(planner.graph());
    QueryNode node = iterator.next();
    assertSame(SINK, node);
    
    node = iterator.next();
    assertTrue(node instanceof GroupBy);

    node = iterator.next();
    assertSame(STORE_NODES.get(0), node);
    QuerySourceConfig source_config = (QuerySourceConfig) STORE_NODES.get(0).config();
    assertEquals(1, source_config.pushDownNodes().size());
    assertTrue(source_config.pushDownNodes().get(0).getConfig() instanceof DownsampleConfig);
    assertEquals("downsample", source_config.pushDownNodes().get(0).getId());
    
    assertEquals(1, planner.roots().size());
    assertTrue(planner.roots().get(0) instanceof GroupBy);
  }
  
  @Test
  public void oneMetricOneGraphPushdownAll() throws Exception {
    when(STORE_FACTORY.supportsPushdown(DownsampleConfig.class))
      .thenReturn(true);
    
    ExecutionGraph graph = ExecutionGraph.newBuilder()
        .setId("g1")
        .addNode(ExecutionGraphNode.newBuilder()
            .setId("m1")
            .setType("DataSource")
            .setConfig(QuerySourceConfig.newBuilder()
                .setMetric(MetricLiteralFilter.newBuilder()
                    .setMetric("sys.cpu.user")
                    .build())
                .setFilterId("f1")
                .setId("m1")
                .build()))
        .addNode(ExecutionGraphNode.newBuilder()
            .setId("downsample")
            .addSource("m1")
            .setConfig(DownsampleConfig.newBuilder()
                .setAggregator("sum")
                .setInterval("1m")
                .addInterpolatorConfig(NUMERIC_CONFIG)
                .setId("downsample")
                .build())
            .build())
        .build();
    
    SemanticQuery query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .setExecutionGraph(graph)
        .build();
    
    when(context.query()).thenReturn(query);
    
    DefaultQueryPlanner planner = 
        new DefaultQueryPlanner(context, Lists.newArrayList(SINK));
    planner.plan(null);
    
    // validate
    assertSame(STORE_NODES.get(0), planner.sources().get(0));
    assertEquals(2, planner.graph().vertexSet().size());
    
    DepthFirstIterator<QueryNode, DefaultEdge> iterator = 
        new DepthFirstIterator<QueryNode, DefaultEdge>(planner.graph());
    QueryNode node = iterator.next();
    assertSame(SINK, node);
    
    node = iterator.next();
    assertSame(STORE_NODES.get(0), node);
    QuerySourceConfig source_config = (QuerySourceConfig) STORE_NODES.get(0).config();
    assertEquals(1, source_config.pushDownNodes().size());
    assertTrue(source_config.pushDownNodes().get(0).getConfig() instanceof DownsampleConfig);
    assertEquals("downsample", source_config.pushDownNodes().get(0).getId());
    
    assertEquals(1, planner.roots().size());
    assertTrue(planner.roots().get(0) instanceof TimeSeriesDataSource);
  }

  @Test
  public void twoMetricsOneGraphNoPushdown() throws Exception {
    ExecutionGraph graph = ExecutionGraph.newBuilder()
        .setId("g1")
        .addNode(ExecutionGraphNode.newBuilder()
            .setId("m1")
            .setType("DataSource")
            .setConfig(QuerySourceConfig.newBuilder()
                .setMetric(MetricLiteralFilter.newBuilder()
                    .setMetric("sys.cpu.user")
                    .build())
                .setFilterId("f1")
                .setId("m1")
                .build()))
        .addNode(ExecutionGraphNode.newBuilder()
            .setId("m2")
            .setType("DataSource")
            .setConfig(QuerySourceConfig.newBuilder()
                .setMetric(MetricLiteralFilter.newBuilder()
                    .setMetric("sys.cpu.sys")
                    .build())
                .setFilterId("f1")
                .setId("m2")
                .build()))
        .addNode(ExecutionGraphNode.newBuilder()
            .setId("downsample")
            .addSource("m1")
            .addSource("m2")
            .setConfig(DownsampleConfig.newBuilder()
                .setAggregator("sum")
                .setInterval("1m")
                .addInterpolatorConfig(NUMERIC_CONFIG)
                .setId("downsample")
                .build())
            .build())
        .addNode(ExecutionGraphNode.newBuilder()
            .setId("groupby")
            .addSource("downsample")
            .setConfig(GroupByConfig.newBuilder()
                .setAggregator("sum")
                .addTagKey("host")
                .addInterpolatorConfig(NUMERIC_CONFIG)
                .setId("gb")
                .build()))
        .build();
    
    SemanticQuery query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .setExecutionGraph(graph)
        .build();
    
    when(context.query()).thenReturn(query);

    DefaultQueryPlanner planner = 
        new DefaultQueryPlanner(context, Lists.newArrayList(SINK));
    planner.plan(null);
    
    // validate
    assertEquals(2, planner.sources().size());
    assertTrue(planner.sources().contains(STORE_NODES.get(0)));
    assertTrue(planner.sources().contains(STORE_NODES.get(1)));
    assertEquals(5, planner.graph().vertexSet().size());
    
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
    
    assertEquals(1, planner.roots().size());
    assertTrue(planner.roots().get(0) instanceof GroupBy);
  }

  @Test
  public void twoMetricsOneGraphPushdownCommon() throws Exception {
    when(STORE_FACTORY.supportsPushdown(DownsampleConfig.class))
      .thenReturn(true);
    
    ExecutionGraph graph = ExecutionGraph.newBuilder()
        .setId("g1")
        .addNode(ExecutionGraphNode.newBuilder()
            .setId("m1")
            .setType("DataSource")
            .setConfig(QuerySourceConfig.newBuilder()
                .setMetric(MetricLiteralFilter.newBuilder()
                    .setMetric("sys.cpu.user")
                    .build())
                .setFilterId("f1")
                .setId("m1")
                .build()))
        .addNode(ExecutionGraphNode.newBuilder()
            .setId("m2")
            .setType("DataSource")
            .setConfig(QuerySourceConfig.newBuilder()
                .setMetric(MetricLiteralFilter.newBuilder()
                    .setMetric("sys.cpu.sys")
                    .build())
                .setFilterId("f1")
                .setId("m2")
                .build()))
        .addNode(ExecutionGraphNode.newBuilder()
            .setId("downsample")
            .addSource("m1")
            .addSource("m2")
            .setConfig(DownsampleConfig.newBuilder()
                .setAggregator("sum")
                .setInterval("1m")
                .addInterpolatorConfig(NUMERIC_CONFIG)
                .setId("downsample")
                .build())
            .build())
        .addNode(ExecutionGraphNode.newBuilder()
            .setId("groupby")
            .addSource("downsample")
            .setConfig(GroupByConfig.newBuilder()
                .setAggregator("sum")
                .addTagKey("host")
                .addInterpolatorConfig(NUMERIC_CONFIG)
                .setId("gb")
                .build()))
        .build();
    
    SemanticQuery query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .setExecutionGraph(graph)
        .build();
    
    when(context.query()).thenReturn(query);

    DefaultQueryPlanner planner = 
        new DefaultQueryPlanner(context, Lists.newArrayList(SINK));
    planner.plan(null);
    
    // validate
    assertEquals(2, planner.sources().size());
    assertTrue(planner.sources().contains(STORE_NODES.get(0)));
    assertTrue(planner.sources().contains(STORE_NODES.get(1)));
    assertEquals(4, planner.graph().vertexSet().size());
    
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
    assertEquals(1, source_config.pushDownNodes().size());
    assertTrue(source_config.pushDownNodes().get(0).getConfig() instanceof DownsampleConfig);
    assertEquals("downsample", source_config.pushDownNodes().get(0).getId());
    
    node = iterator.next();
    assertSame(STORE_NODES.get(0), node);
    source_config = (QuerySourceConfig) STORE_NODES.get(0).config();
    assertEquals(1, source_config.pushDownNodes().size());
    assertTrue(source_config.pushDownNodes().get(0).getConfig() instanceof DownsampleConfig);
    assertEquals("downsample", source_config.pushDownNodes().get(0).getId());
    
    assertEquals(1, planner.roots().size());
    assertTrue(planner.roots().get(0) instanceof GroupBy);
  }
  
  @Test
  public void twoMetricsOneGraphPushdownNotCommon() throws Exception {
    when(STORE_FACTORY.supportsPushdown(DownsampleConfig.class))
      .thenReturn(true);
    
    ExecutionGraph graph = ExecutionGraph.newBuilder()
        .setId("g1")
        .addNode(ExecutionGraphNode.newBuilder()
            .setId("m1")
            .setType("DataSource")
            .setConfig(QuerySourceConfig.newBuilder()
                .setMetric(MetricLiteralFilter.newBuilder()
                    .setMetric("sys.cpu.user")
                    .build())
                .setFilterId("f1")
                .setId("m1")
                .build()))
        .addNode(ExecutionGraphNode.newBuilder()
            .setId("m2")
            .setType("DataSource")
            .setConfig(QuerySourceConfig.newBuilder()
                .setMetric(MetricLiteralFilter.newBuilder()
                    .setMetric("sys.cpu.sys")
                    .build())
                .setFilterId("f1")
                .setId("m2")
                .build()))
        .addNode(ExecutionGraphNode.newBuilder()
            .setId("downsample")
            .addSource("m1")
            .setConfig(DownsampleConfig.newBuilder()
                .setAggregator("sum")
                .setInterval("1m")
                .addInterpolatorConfig(NUMERIC_CONFIG)
                .setId("downsample")
                .build())
            .build())
        .addNode(ExecutionGraphNode.newBuilder()
            .setId("ds2")
            .setType("downsample")
            .addSource("m2")
            .setConfig(DownsampleConfig.newBuilder()
                .setAggregator("sum")
                .setInterval("2m")
                .addInterpolatorConfig(NUMERIC_CONFIG)
                .setId("downsample")
                .build())
            .build())
        .addNode(ExecutionGraphNode.newBuilder()
            .setId("groupby")
            .addSource("downsample")
            .addSource("ds2")
            .setConfig(GroupByConfig.newBuilder()
                .setAggregator("sum")
                .addTagKey("host")
                .addInterpolatorConfig(NUMERIC_CONFIG)
                .setId("gb")
                .build()))
        .build();
    
    SemanticQuery query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .setExecutionGraph(graph)
        .build();
    
    when(context.query()).thenReturn(query);

    DefaultQueryPlanner planner = 
        new DefaultQueryPlanner(context, Lists.newArrayList(SINK));
    planner.plan(null);
    
    // validate
    assertEquals(2, planner.sources().size());
    assertTrue(planner.sources().contains(STORE_NODES.get(0)));
    assertTrue(planner.sources().contains(STORE_NODES.get(1)));
    assertEquals(4, planner.graph().vertexSet().size());
    
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
    assertEquals(1, source_config.pushDownNodes().size());
    assertTrue(source_config.pushDownNodes().get(0).getConfig() instanceof DownsampleConfig);
    assertEquals("ds2", source_config.pushDownNodes().get(0).getId());
    
    node = iterator.next();
    assertSame(STORE_NODES.get(0), node);
    source_config = (QuerySourceConfig) STORE_NODES.get(0).config();
    assertEquals(1, source_config.pushDownNodes().size());
    assertTrue(source_config.pushDownNodes().get(0).getConfig() instanceof DownsampleConfig);
    assertEquals("downsample", source_config.pushDownNodes().get(0).getId());
    
    assertEquals(1, planner.roots().size());
    assertTrue(planner.roots().get(0) instanceof GroupBy);
  }

  @Test
  public void twoMetricsOneGraphPushdownAll() throws Exception {
    when(STORE_FACTORY.supportsPushdown(DownsampleConfig.class))
      .thenReturn(true);
    
    ExecutionGraph graph = ExecutionGraph.newBuilder()
        .setId("g1")
        .addNode(ExecutionGraphNode.newBuilder()
            .setId("m1")
            .setType("DataSource")
            .setConfig(QuerySourceConfig.newBuilder()
                .setMetric(MetricLiteralFilter.newBuilder()
                    .setMetric("sys.cpu.user")
                    .build())
                .setFilterId("f1")
                .setId("m1")
                .build()))
        .addNode(ExecutionGraphNode.newBuilder()
            .setId("m2")
            .setType("DataSource")
            .setConfig(QuerySourceConfig.newBuilder()
                .setMetric(MetricLiteralFilter.newBuilder()
                    .setMetric("sys.cpu.sys")
                    .build())
                .setFilterId("f1")
                .setId("m2")
                .build()))
        .addNode(ExecutionGraphNode.newBuilder()
            .setId("downsample")
            .addSource("m1")
            .addSource("m2")
            .setConfig(DownsampleConfig.newBuilder()
                .setAggregator("sum")
                .setInterval("1m")
                .addInterpolatorConfig(NUMERIC_CONFIG)
                .setId("downsample")
                .build())
            .build())
        .build();
    
    SemanticQuery query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .setExecutionGraph(graph)
        .build();
    
    when(context.query()).thenReturn(query);

    DefaultQueryPlanner planner = 
        new DefaultQueryPlanner(context, Lists.newArrayList(SINK));
    planner.plan(null);
    
    // validate
    assertEquals(2, planner.sources().size());
    assertTrue(planner.sources().contains(STORE_NODES.get(0)));
    assertTrue(planner.sources().contains(STORE_NODES.get(1)));
    assertEquals(3, planner.graph().vertexSet().size());
    
    DepthFirstIterator<QueryNode, DefaultEdge> iterator = 
        new DepthFirstIterator<QueryNode, DefaultEdge>(planner.graph());
    QueryNode node = iterator.next();
    assertSame(SINK, node);
    
    // TODO - watch this bit for ordering
    node = iterator.next();
    assertSame(STORE_NODES.get(1), node);
    QuerySourceConfig source_config = (QuerySourceConfig) STORE_NODES.get(1).config();
    assertEquals(1, source_config.pushDownNodes().size());
    assertTrue(source_config.pushDownNodes().get(0).getConfig() instanceof DownsampleConfig);
    assertEquals("downsample", source_config.pushDownNodes().get(0).getId());
    
    node = iterator.next();
    assertSame(STORE_NODES.get(0), node);
    source_config = (QuerySourceConfig) STORE_NODES.get(0).config();
    assertEquals(1, source_config.pushDownNodes().size());
    assertTrue(source_config.pushDownNodes().get(0).getConfig() instanceof DownsampleConfig);
    assertEquals("downsample", source_config.pushDownNodes().get(0).getId());
    
    assertEquals(2, planner.roots().size());
    assertTrue(planner.roots().get(0) instanceof TimeSeriesDataSource);
    assertTrue(planner.roots().get(1) instanceof TimeSeriesDataSource);
  }
  
  @Test
  public void cycleFound() throws Exception {
    ExecutionGraph graph = ExecutionGraph.newBuilder()
        .setId("g1")
        .addNode(ExecutionGraphNode.newBuilder()
            .setId("m1")
            .setType("DataSource")
            .addSource("groupby")
            .setConfig(QuerySourceConfig.newBuilder()
                .setMetric(MetricLiteralFilter.newBuilder()
                    .setMetric("sys.cpu.user")
                    .build())
                .setFilterId("f1")
                .setId("m1")
                .build()))
        .addNode(ExecutionGraphNode.newBuilder()
            .setId("downsample")
            .addSource("m1")
            .setConfig(DownsampleConfig.newBuilder()
                .setAggregator("sum")
                .setInterval("1m")
                .addInterpolatorConfig(NUMERIC_CONFIG)
                .setId("downsample")
                .build())
            .build())
        .addNode(ExecutionGraphNode.newBuilder()
            .setId("groupby")
            .addSource("downsample")
            .setConfig(GroupByConfig.newBuilder()
                .setAggregator("sum")
                .addTagKey("host")
                .addInterpolatorConfig(NUMERIC_CONFIG)
                .setId("gb")
                .build()))
        .build();
    
    SemanticQuery query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .setExecutionGraph(graph)
        .build();
    
    when(context.query()).thenReturn(query);
    
    DefaultQueryPlanner planner = 
        new DefaultQueryPlanner(context, Lists.newArrayList(SINK));
    try {
      planner.plan(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    assertNull(planner.graph());
  }
  
  @Test
  public void duplicateNodeIds() throws Exception {
    ExecutionGraph graph = ExecutionGraph.newBuilder()
        .setId("g1")
        .addNode(ExecutionGraphNode.newBuilder()
            .setId("m1")
            .setType("DataSource")
            .setConfig(QuerySourceConfig.newBuilder()
                .setMetric(MetricLiteralFilter.newBuilder()
                    .setMetric("sys.cpu.user")
                    .build())
                .setFilterId("f1")
                .setId("m1")
                .build()))
        .addNode(ExecutionGraphNode.newBuilder()
            .setId("m1")
            .setType("DataSource")
            .setConfig(QuerySourceConfig.newBuilder()
                .setMetric(MetricLiteralFilter.newBuilder()
                    .setMetric("sys.cpu.sys")
                    .build())
                .setFilterId("f1")
                .setId("m1")
                .build()))
        .addNode(ExecutionGraphNode.newBuilder()
            .setId("downsample")
            .addSource("m1")
            .setConfig(DownsampleConfig.newBuilder()
                .setAggregator("sum")
                .setInterval("1m")
                .addInterpolatorConfig(NUMERIC_CONFIG)
                .setId("downsample")
                .build())
            .build())
        .addNode(ExecutionGraphNode.newBuilder()
            .setId("groupby")
            .addSource("downsample")
            .setConfig(GroupByConfig.newBuilder()
                .setAggregator("sum")
                .addTagKey("host")
                .addInterpolatorConfig(NUMERIC_CONFIG)
                .setId("gb")
                .build()))
        .build();
    
    SemanticQuery query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .setExecutionGraph(graph)
        .build();
    
    when(context.query()).thenReturn(query);
    
    DefaultQueryPlanner planner = 
        new DefaultQueryPlanner(context, Lists.newArrayList(SINK));
    try {
      planner.plan(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    assertNull(planner.graph());
  }
  
}
