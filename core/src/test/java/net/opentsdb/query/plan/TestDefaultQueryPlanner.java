// This file is part of OpenTSDB.
// Copyright (C) 2018-2019  The OpenTSDB Authors.
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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;
import net.opentsdb.common.Const;
import net.opentsdb.configuration.Configuration;
import net.opentsdb.core.DefaultRegistry;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.MockTSDB;
import net.opentsdb.core.TSDBPlugin;
import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.data.TimeSeriesDataSourceFactory;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.query.BaseTimeSeriesDataSourceConfig;
import net.opentsdb.query.DefaultTimeSeriesDataSourceConfig;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.QueryMode;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.joins.JoinConfig;
import net.opentsdb.query.joins.JoinConfig.JoinType;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.processor.downsample.Downsample;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.query.processor.expressions.BinaryExpressionNode;
import net.opentsdb.query.processor.expressions.ExpressionConfig;
import net.opentsdb.query.processor.groupby.GroupBy;
import net.opentsdb.query.processor.groupby.GroupByConfig;
import net.opentsdb.query.processor.merge.MergerConfig;
import net.opentsdb.query.processor.summarizer.SummarizerConfig;
import net.opentsdb.query.serdes.SerdesOptions;
import net.opentsdb.utils.JSON;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestDefaultQueryPlanner {

  private static MockTSDB TSDB;
  private static TimeSeriesDataSourceFactory STORE_FACTORY;
  private static NumericInterpolatorConfig NUMERIC_CONFIG;
  private static QueryNode SINK;
  private static List<TimeSeriesDataSource> STORE_NODES;
  private static TimeSeriesDataSourceFactory S1;
  private static TimeSeriesDataSourceFactory S2;
  
  private QueryPipelineContext context;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    TSDB = new MockTSDB();
    STORE_FACTORY = mock(TimeSeriesDataSourceFactory.class);
    SINK = mock(QueryNode.class);
    STORE_NODES = Lists.newArrayList();
    S1 = mock(TimeSeriesDataSourceFactory.class);
    S2 = mock(TimeSeriesDataSourceFactory.class);
    
    TSDB.registry = new DefaultRegistry(TSDB);
    ((DefaultRegistry) TSDB.registry).initialize(true);
    ((DefaultRegistry) TSDB.registry).registerPlugin(
        TimeSeriesDataSourceFactory.class, null, (TSDBPlugin) STORE_FACTORY);
    ((DefaultRegistry) TSDB.registry).registerPlugin(
        TimeSeriesDataSourceFactory.class, "s1", (TSDBPlugin) S1);
    ((DefaultRegistry) TSDB.registry).registerPlugin(
        TimeSeriesDataSourceFactory.class, "s2", (TSDBPlugin) S2);
    
    when(S1.newNode(any(QueryPipelineContext.class), 
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
    when(S2.newNode(any(QueryPipelineContext.class), 
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
    when(S1.idType()).thenAnswer(new Answer<TypeToken<? extends TimeSeriesId>>() {
      @Override
      public TypeToken<? extends TimeSeriesId> answer(
          InvocationOnMock invocation) throws Throwable {
        return Const.TS_BYTE_ID;
      }
    });
    when(S2.idType()).thenAnswer(new Answer<TypeToken<? extends TimeSeriesId>>() {
      @Override
      public TypeToken<? extends TimeSeriesId> answer(
          InvocationOnMock invocation) throws Throwable {
        return Const.TS_BYTE_ID;
      }
    });
    when(S1.id()).thenReturn("s1");
    when(S2.id()).thenReturn("s2");
    
    when(STORE_FACTORY.parseConfig(any(ObjectMapper.class), any(TSDB.class), any(JsonNode.class)))
    .thenAnswer(new Answer<QueryNodeConfig>() {
      @Override
      public QueryNodeConfig answer(InvocationOnMock invocation)
          throws Throwable {
        DefaultTimeSeriesDataSourceConfig.Builder builder = DefaultTimeSeriesDataSourceConfig.newBuilder();
        
        DefaultTimeSeriesDataSourceConfig.parseConfig
            ((ObjectMapper) invocation.getArguments()[0], 
                invocation.getArgumentAt(1, TSDB.class), 
                (JsonNode) invocation.getArguments()[2],
                (BaseTimeSeriesDataSourceConfig.Builder) builder);
        return builder.build();
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
    when(context.queryContext()).thenReturn(mock(QueryContext.class));
    
    STORE_NODES.clear();
    when(STORE_FACTORY.supportsPushdown(any(Class.class)))
      .thenReturn(false);
  }
  
  @Test
  public void oneMetricAlone() throws Exception {
    List<QueryNodeConfig> graph = Lists.newArrayList(
        DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setFilterId("f1")
            .setId("m1")
            .build());
    
    SemanticQuery query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .setExecutionGraph(graph)
        .build();
    
    when(context.query()).thenReturn(query);
    
    DefaultQueryPlanner planner = 
        new DefaultQueryPlanner(context, SINK);
    planner.plan(null).join();
    
    // validate
    assertSame(STORE_NODES.get(0), planner.sources().get(0));
    assertEquals(2, planner.graph().nodes().size());
    assertTrue(planner.graph().hasEdgeConnecting(SINK, planner.nodes_map.get("m1")));
    assertTrue(planner.graph().hasEdgeConnecting(SINK, planner.nodes_map.get("m1")));
    
    assertEquals(1, planner.serializationSources().size());
    
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
  public void oneMetricOneGraphNoPushdown() throws Exception {
    List<QueryNodeConfig> graph = Lists.newArrayList(
        DefaultTimeSeriesDataSourceConfig.newBuilder()
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
    assertEquals(4, planner.graph().nodes().size());
    assertTrue(planner.graph().hasEdgeConnecting(SINK, planner.nodes_map.get("gb")));
    assertFalse(planner.graph().hasEdgeConnecting(SINK, planner.nodes_map.get("ds")));
    assertFalse(planner.graph().hasEdgeConnecting(SINK, planner.nodes_map.get("m1")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodes_map.get("ds"), 
        planner.nodes_map.get("m1")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodes_map.get("gb"), 
        planner.nodes_map.get("ds")));
    
    assertEquals(1, planner.serializationSources().size());
    
//    DepthFirstIterator<QueryNode, DefaultEdge> iterator = 
//        new DepthFirstIterator<QueryNode, DefaultEdge>(planner.graph());
//    QueryNode node = iterator.next();
//    assertSame(SINK, node);
//    
//    node = iterator.next();
//    assertTrue(node instanceof GroupBy);
//    
//    node = iterator.next();
//    assertTrue(node instanceof Downsample);
//    assertEquals(1514764800, ((DownsampleConfig) node.config()).startTime().epoch());
//    assertEquals(1514768400, ((DownsampleConfig) node.config()).endTime().epoch());
//    
//    node = iterator.next();
//    assertSame(STORE_NODES.get(0), node);
//    
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
        DefaultTimeSeriesDataSourceConfig.newBuilder()
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
    assertEquals(3, planner.graph().nodes().size());
    assertTrue(planner.graph().hasEdgeConnecting(SINK, planner.nodes_map.get("gb")));
    assertFalse(planner.graph().hasEdgeConnecting(SINK, planner.nodes_map.get("ds")));
    
    assertEquals(1, planner.serializationSources().size());
    
    QueryNode node = planner.nodes_map.get("ds");
    assertSame(STORE_NODES.get(0), node);
    DefaultTimeSeriesDataSourceConfig source_config = (DefaultTimeSeriesDataSourceConfig) STORE_NODES.get(0).config();
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
        DefaultTimeSeriesDataSourceConfig.newBuilder()
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
    assertEquals(2, planner.graph().nodes().size());
    assertTrue(planner.graph().hasEdgeConnecting(SINK, planner.nodes_map.get("downsample")));
    
    assertEquals(1, planner.serializationSources().size());
    
    QueryNode node = planner.nodes_map.get("downsample");
    assertSame(STORE_NODES.get(0), node);
    DefaultTimeSeriesDataSourceConfig source_config = (DefaultTimeSeriesDataSourceConfig) STORE_NODES.get(0).config();
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
  public void oneMetricOneGraphTwoPushDownsAndSummarizerPassThrough() throws Exception {
    when(STORE_FACTORY.supportsPushdown(DownsampleConfig.class))
      .thenReturn(true);
    when(STORE_FACTORY.supportsPushdown(GroupByConfig.class))
      .thenReturn(true);
    
    List<QueryNodeConfig> graph = Lists.newArrayList(
        DefaultTimeSeriesDataSourceConfig.newBuilder()
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
            .build(),
        SummarizerConfig.newBuilder()
            .setSummaries(Lists.newArrayList("avg", "max", "count"))
            .setId("sum")
            .addSource("gb")
            .build());
    
    SemanticQuery query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .setExecutionGraph(graph)
        .addSerdesConfig(serdesConfigs(Lists.newArrayList("gb")))
        .addSerdesConfig(serdesConfigs(Lists.newArrayList("sum")))
        .build();
    
    when(STORE_FACTORY.supportsPushdown(any(Class.class))).thenReturn(true);
    
    when(context.query()).thenReturn(query);
    
    DefaultQueryPlanner planner = 
        new DefaultQueryPlanner(context, SINK);
    planner.plan(null).join();
    
    // validate
    assertSame(STORE_NODES.get(0), planner.sources().get(0));
    assertEquals(3, planner.graph().nodes().size());
    assertTrue(planner.graph().hasEdgeConnecting(SINK, planner.nodes_map.get("sum")));
    assertNull(planner.nodeForId("ds"));
    assertFalse(planner.graph().hasEdgeConnecting(SINK, planner.nodes_map.get("gb")));
    assertTrue(planner.graph().hasEdgeConnecting(SINK, planner.nodes_map.get("sum")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodes_map.get("sum"), 
        planner.nodes_map.get("gb")));
    
    assertEquals(2, planner.serializationSources().size());
  }
  
  @Test
  public void oneMetricOneGraphTwoPushDownsAndSummarizerNoPassThrough() throws Exception {
    when(STORE_FACTORY.supportsPushdown(DownsampleConfig.class))
      .thenReturn(true);
    when(STORE_FACTORY.supportsPushdown(GroupByConfig.class))
      .thenReturn(true);
    
    List<QueryNodeConfig> graph = Lists.newArrayList(
        DefaultTimeSeriesDataSourceConfig.newBuilder()
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
            .build(),
        SummarizerConfig.newBuilder()
            .setSummaries(Lists.newArrayList("avg", "max", "count"))
            .setId("sum")
            .addSource("gb")
            .build());
    
    SemanticQuery query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .setExecutionGraph(graph)
        .addSerdesConfig(serdesConfigs(Lists.newArrayList("sum")))
        .build();
    
    when(STORE_FACTORY.supportsPushdown(any(Class.class))).thenReturn(true);
    
    when(context.query()).thenReturn(query);
    
    DefaultQueryPlanner planner = 
        new DefaultQueryPlanner(context, SINK);
    planner.plan(null).join();
    
    // validate
    assertSame(STORE_NODES.get(0), planner.sources().get(0));
    assertEquals(3, planner.graph().nodes().size());
    assertTrue(planner.graph().hasEdgeConnecting(SINK, planner.nodes_map.get("sum")));
    assertNull(planner.nodeForId("ds"));
    assertFalse(planner.graph().hasEdgeConnecting(SINK, planner.nodes_map.get("gb")));
    assertTrue(planner.graph().hasEdgeConnecting(SINK, planner.nodes_map.get("sum")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodes_map.get("sum"), 
        planner.nodes_map.get("gb")));
    
    assertEquals(1, planner.serializationSources().size());
  }
  
  @Test
  public void oneMetricOneGraphNoPushdownAndSummarizerPassThrough() throws Exception {
    List<QueryNodeConfig> graph = Lists.newArrayList(
        DefaultTimeSeriesDataSourceConfig.newBuilder()
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
            .build(),
        SummarizerConfig.newBuilder()
            .setSummaries(Lists.newArrayList("avg", "max", "count"))
            .setId("sum")
            .addSource("gb")
            .build());
    
    SemanticQuery query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .setExecutionGraph(graph)
        .addSerdesConfig(serdesConfigs(Lists.newArrayList("gb")))
        .addSerdesConfig(serdesConfigs(Lists.newArrayList("sum")))
        .build();
    
    when(STORE_FACTORY.supportsPushdown(any(Class.class))).thenReturn(false);
    
    when(context.query()).thenReturn(query);
    
    DefaultQueryPlanner planner = 
        new DefaultQueryPlanner(context, SINK);
    planner.plan(null).join();
    
    // validate
    assertSame(STORE_NODES.get(0), planner.sources().get(0));
    assertEquals(5, planner.graph().nodes().size());
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodes_map.get("gb"), 
        planner.nodes_map.get("ds")));
    assertFalse(planner.graph().hasEdgeConnecting(SINK, planner.nodes_map.get("gb")));
    assertTrue(planner.graph().hasEdgeConnecting(SINK, planner.nodes_map.get("sum")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodes_map.get("ds"), 
        planner.nodes_map.get("m1")));
    
    assertEquals(2, planner.serializationSources().size());
  }
  
  @Test
  public void oneMetricOneGraphNoPushdownAndSummarizerNoPassThrough() throws Exception {
    List<QueryNodeConfig> graph = Lists.newArrayList(
        DefaultTimeSeriesDataSourceConfig.newBuilder()
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
            .build(),
        SummarizerConfig.newBuilder()
            .setSummaries(Lists.newArrayList("avg", "max", "count"))
            .setId("sum")
            .addSource("gb")
            .build());
    
    SemanticQuery query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .setExecutionGraph(graph)
        .addSerdesConfig(serdesConfigs(Lists.newArrayList("sum")))
        .build();
    
    when(STORE_FACTORY.supportsPushdown(any(Class.class))).thenReturn(false);
    
    when(context.query()).thenReturn(query);
    
    DefaultQueryPlanner planner = 
        new DefaultQueryPlanner(context, SINK);
    planner.plan(null).join();
    
    // validate
    assertSame(STORE_NODES.get(0), planner.sources().get(0));
    assertEquals(5, planner.graph().nodes().size());
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodes_map.get("gb"), 
        planner.nodes_map.get("ds")));
    assertFalse(planner.graph().hasEdgeConnecting(SINK, planner.nodes_map.get("gb")));
    assertTrue(planner.graph().hasEdgeConnecting(SINK, planner.nodes_map.get("sum")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodes_map.get("ds"), 
        planner.nodes_map.get("m1")));
    
    assertEquals(1, planner.serializationSources().size());
  }
  
  @Test
  public void twoMetricsAlone() throws Exception {
    List<QueryNodeConfig> graph = Lists.newArrayList(
        DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setFilterId("f1")
            .setId("m1")
            .build(),
        DefaultTimeSeriesDataSourceConfig.newBuilder()
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
    assertEquals(3, planner.graph().nodes().size());

    assertEquals(2, planner.serializationSources().size());
    
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
        DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setFilterId("f1")
            .setId("m1")
            .build(),
        DefaultTimeSeriesDataSourceConfig.newBuilder()
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
    assertEquals(5, planner.graph().nodes().size());

    assertEquals(2, planner.serializationSources().size());
    
    QueryNode node = planner.nodes_map.get("downsample");
    assertTrue(node instanceof Downsample);
    assertEquals(1514764800, ((DownsampleConfig) node.config()).startTime().epoch());
    assertEquals(1514768400, ((DownsampleConfig) node.config()).endTime().epoch());
    
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
  public void oneMetricPushDown() throws Exception {
    when(STORE_FACTORY.supportsPushdown(DownsampleConfig.class))
      .thenReturn(true);
    
    List<QueryNodeConfig> graph = Lists.newArrayList(
        DefaultTimeSeriesDataSourceConfig.newBuilder()
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
        .build();
    
    when(context.query()).thenReturn(query);

    DefaultQueryPlanner planner = 
        new DefaultQueryPlanner(context, SINK);
    planner.plan(null).join();
    
    // validate
    assertEquals(1, planner.sources().size());
    assertTrue(planner.sources().contains(STORE_NODES.get(0)));
    assertEquals(2, planner.graph().nodes().size());

    assertEquals(1, planner.serializationSources().size());
    
    QueryNode node = planner.nodes_map.get("downsample");
    assertSame(STORE_NODES.get(0), node);
    DefaultTimeSeriesDataSourceConfig source_config =
        (DefaultTimeSeriesDataSourceConfig) STORE_NODES.get(0).config();
    assertEquals(1, source_config.getPushDownNodes().size());
    assertTrue(source_config.getPushDownNodes().get(0) instanceof DownsampleConfig);
    assertEquals("downsample", source_config.getPushDownNodes().get(0).getId());
    
    assertTrue(planner.graph().hasEdgeConnecting(SINK, planner.nodeForId("downsample")));
  }
// TODO - restore the multiple metrics through one node functionality. For now, 
// pushdowns have to go through their own branch.
//  @Test
//  public void twoMetricsOneGraphPushdownCommon() throws Exception {
//    when(STORE_FACTORY.supportsPushdown(DownsampleConfig.class))
//      .thenReturn(true);
//    
//    List<QueryNodeConfig> graph = Lists.newArrayList(
//        DefaultTimeSeriesDataSourceConfig.newBuilder()
//            .setMetric(MetricLiteralFilter.newBuilder()
//                .setMetric("sys.cpu.user")
//                .build())
//            .setFilterId("f1")
//            .setId("m1")
//            .build(),
//       DefaultTimeSeriesDataSourceConfig.newBuilder()
//            .setMetric(MetricLiteralFilter.newBuilder()
//                .setMetric("sys.cpu.sys")
//                .build())
//            .setFilterId("f1")
//            .setId("m2")
//            .build(),
//        DownsampleConfig.newBuilder()
//            .setAggregator("sum")
//            .setInterval("1m")
//            .addInterpolatorConfig(NUMERIC_CONFIG)
//            .setId("downsample")
//            .addSource("m1")
//            .addSource("m2")
//            .build(),
//        GroupByConfig.newBuilder()
//            .setAggregator("sum")
//            .addTagKey("host")
//            .addInterpolatorConfig(NUMERIC_CONFIG)
//            .setId("gb")
//            .addSource("downsample")
//            .build());
//    
//    SemanticQuery query = SemanticQuery.newBuilder()
//        .setMode(QueryMode.SINGLE)
//        .setStart("1514764800")
//        .setEnd("1514768400")
//        .setExecutionGraph(graph)
//        .addSerdesConfig(serdesConfigs(Lists.newArrayList("gb")))
//        .build();
//    
//    when(context.query()).thenReturn(query);
//
//    DefaultQueryPlanner planner = 
//        new DefaultQueryPlanner(context, SINK);
//    planner.plan(null).join();
//    
//    // validate
//    assertEquals(2, planner.sources().size());
//    assertTrue(planner.sources().contains(STORE_NODES.get(0)));
//    assertTrue(planner.sources().contains(STORE_NODES.get(1)));
//    assertEquals(4, planner.graph().nodes().size());
//
//    assertEquals(2, planner.serializationSources().size());
//    
//    QueryNode node = planner.nodes_map.get("m1");
//    assertSame(STORE_NODES.get(0), node);
//    DefaultTimeSeriesDataSourceConfig source_config = (DefaultTimeSeriesDataSourceConfig) STORE_NODES.get(1).config();
//    assertEquals(1, source_config.getPushDownNodes().size());
//    assertTrue(source_config.getPushDownNodes().get(0) instanceof DownsampleConfig);
//    assertEquals("downsample", source_config.getPushDownNodes().get(0).getId());
//    
//    node = planner.nodes_map.get("m2");
//    assertSame(STORE_NODES.get(1), node);
//    source_config = (DefaultTimeSeriesDataSourceConfig) STORE_NODES.get(0).config();
//    assertEquals(1, source_config.getPushDownNodes().size());
//    assertTrue(source_config.getPushDownNodes().get(0) instanceof DownsampleConfig);
//    assertEquals("downsample", source_config.getPushDownNodes().get(0).getId());
//    
//    assertTrue(planner.graph().hasEdgeConnecting(SINK, planner.nodeForId("gb")));
//    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("gb"), 
//        planner.nodeForId("m1")));
//    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("gb"), 
//        planner.nodeForId("m2")));
//    
//    // no filter
//    query = SemanticQuery.newBuilder()
//        .setMode(QueryMode.SINGLE)
//        .setStart("1514764800")
//        .setEnd("1514768400")
//        .setExecutionGraph(graph)
//        .build();
//    when(context.query()).thenReturn(query);
//    planner = new DefaultQueryPlanner(context, SINK);
//    planner.plan(null).join();
//    assertEquals(2, planner.serializationSources().size());
//  }
//  
//  @Test
//  public void twoMetricsOneGraphPushdownNotCommon() throws Exception {
//    when(STORE_FACTORY.supportsPushdown(DownsampleConfig.class))
//      .thenReturn(true);
//    
//    List<QueryNodeConfig> graph = Lists.newArrayList(
//        DefaultTimeSeriesDataSourceConfig.newBuilder()
//            .setMetric(MetricLiteralFilter.newBuilder()
//                .setMetric("sys.cpu.user")
//                .build())
//            .setFilterId("f1")
//            .setId("m1")
//            .build(),
//        DefaultTimeSeriesDataSourceConfig.newBuilder()
//            .setMetric(MetricLiteralFilter.newBuilder()
//                .setMetric("sys.cpu.sys")
//                .build())
//            .setFilterId("f1")
//            .setId("m2")
//            .build(),
//        DownsampleConfig.newBuilder()
//            .setAggregator("sum")
//            .setInterval("1m")
//            .addInterpolatorConfig(NUMERIC_CONFIG)
//            .setId("ds1")
//            .addSource("m1")
//            .build(),
//        DownsampleConfig.newBuilder()
//            .setAggregator("sum")
//            .setInterval("2m")
//            .addInterpolatorConfig(NUMERIC_CONFIG)
//            .setId("ds2")
//            .addSource("m2")
//            .build(),
//        GroupByConfig.newBuilder()
//            .setAggregator("sum")
//            .addTagKey("host")
//            .addInterpolatorConfig(NUMERIC_CONFIG)
//            .setId("gb")
//            .addSource("ds1")
//            .addSource("ds2")
//            .build());
//    
//    SemanticQuery query = SemanticQuery.newBuilder()
//        .setMode(QueryMode.SINGLE)
//        .setStart("1514764800")
//        .setEnd("1514768400")
//        .setExecutionGraph(graph)
//        .addSerdesConfig(serdesConfigs(Lists.newArrayList("gb")))
//        .build();
//    
//    when(context.query()).thenReturn(query);
//
//    DefaultQueryPlanner planner = 
//        new DefaultQueryPlanner(context, SINK);
//    planner.plan(null).join();
//    
//    // validate
//    assertEquals(2, planner.sources().size());
//    assertTrue(planner.sources().contains(STORE_NODES.get(0)));
//    assertTrue(planner.sources().contains(STORE_NODES.get(1)));
//    assertEquals(4, planner.graph().nodes().size());
//
//    assertEquals(2, planner.serializationSources().size());
//    
//    QueryNode node = planner.nodes_map.get("m1");
//    assertSame(STORE_NODES.get(0), node);
//    DefaultTimeSeriesDataSourceConfig source_config = (DefaultTimeSeriesDataSourceConfig) STORE_NODES.get(1).config();
//    assertEquals(1, source_config.getPushDownNodes().size());
//    assertTrue(source_config.getPushDownNodes().get(0) instanceof DownsampleConfig);
//    assertEquals("ds2", source_config.getPushDownNodes().get(0).getId());
//    
//    node = planner.nodes_map.get("m2");
//    assertSame(STORE_NODES.get(1), node);
//    source_config = (DefaultTimeSeriesDataSourceConfig) STORE_NODES.get(0).config();
//    assertEquals(1, source_config.getPushDownNodes().size());
//    assertTrue(source_config.getPushDownNodes().get(0) instanceof DownsampleConfig);
//    assertEquals("ds1", source_config.getPushDownNodes().get(0).getId());
//    
//    // no filter
//    query = SemanticQuery.newBuilder()
//        .setMode(QueryMode.SINGLE)
//        .setStart("1514764800")
//        .setEnd("1514768400")
//        .setExecutionGraph(graph)
//        .build();
//    when(context.query()).thenReturn(query);
//    planner = new DefaultQueryPlanner(context, SINK);
//    planner.plan(null).join();
//    assertEquals(2, planner.serializationSources().size());
//  }
//
//  @Test
//  public void twoMetricsOneGraphPushdownAll() throws Exception {
//    when(STORE_FACTORY.supportsPushdown(DownsampleConfig.class))
//      .thenReturn(true);
//    
//    List<QueryNodeConfig> graph = Lists.newArrayList(
//        DefaultTimeSeriesDataSourceConfig.newBuilder()
//            .setMetric(MetricLiteralFilter.newBuilder()
//                .setMetric("sys.cpu.user")
//                .build())
//            .setFilterId("f1")
//            .setId("m1")
//            .build(),
//        DefaultTimeSeriesDataSourceConfig.newBuilder()
//            .setMetric(MetricLiteralFilter.newBuilder()
//                .setMetric("sys.cpu.sys")
//                .build())
//            .setFilterId("f1")
//            .setId("m2")
//            .build(),
//       DownsampleConfig.newBuilder()
//            .setAggregator("sum")
//            .setInterval("1m")
//            .addInterpolatorConfig(NUMERIC_CONFIG)
//            .setId("downsample")
//            .addSource("m1")
//            .addSource("m2")
//            .build());
//    
//    SemanticQuery query = SemanticQuery.newBuilder()
//        .setMode(QueryMode.SINGLE)
//        .setStart("1514764800")
//        .setEnd("1514768400")
//        .setExecutionGraph(graph)
//        .addSerdesConfig(serdesConfigs(Lists.newArrayList("downsample")))
//        .build();
//    
//    when(context.query()).thenReturn(query);
//
//    DefaultQueryPlanner planner = 
//        new DefaultQueryPlanner(context, SINK);
//    planner.plan(null).join();
//    
//    // validate
//    assertEquals(2, planner.sources().size());
//    assertTrue(planner.sources().contains(STORE_NODES.get(0)));
//    assertTrue(planner.sources().contains(STORE_NODES.get(1)));
//    assertEquals(3, planner.graph().nodes().size());
//
//    assertEquals(2, planner.serializationSources().size());
//    
//    QueryNode node = planner.nodes_map.get("m1");
//    assertSame(STORE_NODES.get(0), node);
//    DefaultTimeSeriesDataSourceConfig source_config = (DefaultTimeSeriesDataSourceConfig) STORE_NODES.get(1).config();
//    assertEquals(1, source_config.getPushDownNodes().size());
//    assertTrue(source_config.getPushDownNodes().get(0) instanceof DownsampleConfig);
//    assertEquals("downsample", source_config.getPushDownNodes().get(0).getId());
//    
//    node = planner.nodes_map.get("m2");
//    assertSame(STORE_NODES.get(1), node);
//    source_config = (DefaultTimeSeriesDataSourceConfig) STORE_NODES.get(0).config();
//    assertEquals(1, source_config.getPushDownNodes().size());
//    assertTrue(source_config.getPushDownNodes().get(0) instanceof DownsampleConfig);
//    assertEquals("downsample", source_config.getPushDownNodes().get(0).getId());
//    
//    // no filter
//    query = SemanticQuery.newBuilder()
//        .setMode(QueryMode.SINGLE)
//        .setStart("1514764800")
//        .setEnd("1514768400")
//        .setExecutionGraph(graph)
//        .build();
//    when(context.query()).thenReturn(query);
//    planner = new DefaultQueryPlanner(context, SINK);
//    planner.plan(null).join();
//    assertEquals(2, planner.serializationSources().size());
//  }
  
  @Test
  public void twoMetricsTwoGraphs() throws Exception {
    List<QueryNodeConfig> graph = Lists.newArrayList(
        DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setFilterId("f1")
            .setId("m1")
            .build(),
        DefaultTimeSeriesDataSourceConfig.newBuilder()
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
    assertEquals(7, planner.graph().nodes().size());

    assertEquals(2, planner.serializationSources().size());
    
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
        DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setFilterId("f1")
            .setId("m1")
            .build(),
        DefaultTimeSeriesDataSourceConfig.newBuilder()
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
    assertEquals(5, planner.graph().nodes().size());
    
    assertTrue(planner.graph().hasEdgeConnecting(SINK, planner.nodes_map.get("gb")));
    assertTrue(planner.graph().hasEdgeConnecting(SINK, planner.nodes_map.get("m1")));
    assertTrue(planner.graph().hasEdgeConnecting(SINK, planner.nodes_map.get("m2")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodes_map.get("downsample"), 
        planner.nodes_map.get("m2")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodes_map.get("gb"), 
        planner.nodes_map.get("downsample")));

    assertEquals(4, planner.serializationSources().size());
   
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
        DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setFilterId("f1")
            .setId("m1")
            .build(),
        DefaultTimeSeriesDataSourceConfig.newBuilder()
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
    assertEquals(6, planner.graph().nodes().size());

    assertEquals(1, planner.serializationSources().size());
    
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
        DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setFilterId("f1")
            .setId("m1")
            .build(),
        DefaultTimeSeriesDataSourceConfig.newBuilder()
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
    assertEquals(6, planner.graph().nodes().size());

    assertEquals(3, planner.serializationSources().size());
    
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
  public void twoMetricsBranchExpression() throws Exception {
    List<QueryNodeConfig> graph = Lists.newArrayList(
        DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setFilterId("f1")
            .setId("m1")
            .build(),
        DefaultTimeSeriesDataSourceConfig.newBuilder()
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
            .setId("downsample_m1")
            .addSource("m1")
            .build(),
        GroupByConfig.newBuilder()
            .setAggregator("sum")
            .addTagKey("host")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("groupby_m1")
            .addSource("downsample_m1")
            .build(),
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("downsample_m2")
            .addSource("m2")
            .build(),
        GroupByConfig.newBuilder()
            .setAggregator("sum")
            .addTagKey("host")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("groupby_m2")
            .addSource("downsample_m2")
            .build(),
       ExpressionConfig.newBuilder()
            .setExpression("sys.cpu.user + sys.cpu.sys")
            .setAs("sys.tot")
            .setJoinConfig((JoinConfig) JoinConfig.newBuilder()
                .setJoinType(JoinType.NATURAL)
                .build())
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("expression")
            .addSource("groupby_m1")
            .addSource("groupby_m2")
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
    assertEquals(8, planner.graph().nodes().size());

    assertEquals(1, planner.serializationSources().size());
    
    QueryNode node = planner.nodes_map.get("expression");
    Set<QueryNode> nodes = planner.graph().successors(node);
    assertTrue(node instanceof BinaryExpressionNode);
    assertEquals(2, nodes.size());
    node = nodes.iterator().next();
    assertTrue(node instanceof GroupBy);
    
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
  public void twoMetricsBranchExpressionWithscalar() throws Exception {
    List<QueryNodeConfig> graph = Lists.newArrayList(
        DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setFilterId("f1")
            .setId("m1")
            .build(),
        DefaultTimeSeriesDataSourceConfig.newBuilder()
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
            .setId("downsample_m1")
            .addSource("m1")
            .build(),
        GroupByConfig.newBuilder()
            .setAggregator("sum")
            .addTagKey("host")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("groupby_m1")
            .addSource("downsample_m1")
            .build(),
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("downsample_m2")
            .addSource("m2")
            .build(),
        GroupByConfig.newBuilder()
            .setAggregator("sum")
            .addTagKey("host")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("groupby_m2")
            .addSource("downsample_m2")
            .build(),
       ExpressionConfig.newBuilder()
            .setExpression("(sys.cpu.user + sys.cpu.sys) * 2")
            .setAs("sys.tot")
            .setJoinConfig((JoinConfig) JoinConfig.newBuilder()
                .setJoinType(JoinType.NATURAL)
                .build())
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("expression")
            .addSource("groupby_m1")
            .addSource("groupby_m2")
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
    
    assertEquals(2, planner.sources().size());
    assertTrue(planner.sources().contains(STORE_NODES.get(0)));
    assertTrue(planner.sources().contains(STORE_NODES.get(1)));
    assertEquals(9, planner.graph().nodes().size());

    assertEquals(1, planner.serializationSources().size());
    
    QueryNode node = planner.nodes_map.get("expression");
    Set<QueryNode> nodes = planner.graph().successors(node);
    assertTrue(node instanceof BinaryExpressionNode);
    assertEquals(1, nodes.size());
    node = nodes.iterator().next();
    assertTrue(node instanceof BinaryExpressionNode);
    nodes = planner.graph().successors(node);
    assertEquals(2, nodes.size());
    
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
  public void idConvertTwoByteSources() throws Exception {
    List<QueryNodeConfig> graph = Lists.newArrayList(
        DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setFilterId("f1")
            .setSourceId("s1")
            .setId("m1")
            .build(),
        DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.sys")
                .build())
            .setFilterId("f1")
            .setSourceId("s2")
            .setId("m2")
            .build(),
        MergerConfig.newBuilder()
            .setAggregator("sum")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .addSource("m1")
            .addSource("m2")
            .setDataSource("m1")
            .setId("Merger")
            .build());
    
    SemanticQuery query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .setExecutionGraph(graph)
        .build();
    
    when(context.query()).thenReturn(query);

    DefaultQueryPlanner planner = 
        new DefaultQueryPlanner(context, SINK);
    planner.plan(null).join();
    
    // validate
    assertEquals(2, planner.sources().size());
    assertTrue(planner.sources().contains(STORE_NODES.get(0)));
    assertTrue(planner.sources().contains(STORE_NODES.get(1)));
    assertEquals(5, planner.graph().nodes().size());
    assertEquals(4, planner.graph().edges().size());
    assertTrue(planner.graph().hasEdgeConnecting(
        SINK, planner.nodeForId("Merger")));
    assertTrue(planner.graph().hasEdgeConnecting(
        planner.nodeForId("Merger"),
        planner.nodeForId("Merger_IdConverter")));
    assertTrue(planner.graph().hasEdgeConnecting(
        planner.nodeForId("Merger_IdConverter"),
        planner.nodeForId("m1")));
    assertTrue(planner.graph().hasEdgeConnecting(
        planner.nodeForId("Merger_IdConverter"),
        planner.nodeForId("m2")));
    
    assertEquals(1, planner.serializationSources().size());
  }
  
  @Test
  public void idConvertTwoByteSourcesPush() throws Exception {
    mockPush();
    List<QueryNodeConfig> graph = Lists.newArrayList(
        DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setFilterId("f1")
            .setSourceId("s1")
            .setId("m1")
            .build(),
        DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.sys")
                .build())
            .setFilterId("f1")
            .setSourceId("s2")
            .setId("m2")
            .build(),
        MergerConfig.newBuilder()
            .setAggregator("sum")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .addSource("m1")
            .addSource("m2")
            .setDataSource("m1")
            .setId("Merger")
            .build());
    
    SemanticQuery query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .setExecutionGraph(graph)
        .build();
    
    when(context.query()).thenReturn(query);

    DefaultQueryPlanner planner = 
        new DefaultQueryPlanner(context, SINK);
    planner.plan(null).join();
    
    // validate
    assertEquals(2, planner.sources().size());
    assertTrue(planner.sources().contains(STORE_NODES.get(0)));
    assertTrue(planner.sources().contains(STORE_NODES.get(1)));
    assertEquals(5, planner.graph().nodes().size());
    assertEquals(4, planner.graph().edges().size());
    assertTrue(planner.graph().hasEdgeConnecting(
        SINK, planner.nodeForId("IDConverter")));
    assertTrue(planner.graph().hasEdgeConnecting(
        planner.nodeForId("IDConverter"), 
        planner.nodeForId("Merger")));
    assertTrue(planner.graph().hasEdgeConnecting(
        planner.nodeForId("Merger"),
        planner.nodeForId("m1")));
    assertTrue(planner.graph().hasEdgeConnecting(
        planner.nodeForId("Merger"),
        planner.nodeForId("m2")));
    
    assertEquals(1, planner.serializationSources().size());
  }
  
  @Test
  public void idConvertOneByteSources() throws Exception {
    List<QueryNodeConfig> graph = Lists.newArrayList(
        DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setFilterId("f1")
            .setSourceId("s1")
            .setId("m1")
            .build(),
        DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.sys")
                .build())
            .setFilterId("f1")
            .setSourceId("s1")
            .setId("m2")
            .build(),
        MergerConfig.newBuilder()
            .setAggregator("sum")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .addSource("m1")
            .addSource("m2")
            .setDataSource("m1")
            .setId("Merger")
            .build());
    
    SemanticQuery query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .setExecutionGraph(graph)
        .build();
    
    when(context.query()).thenReturn(query);

    DefaultQueryPlanner planner = 
        new DefaultQueryPlanner(context, SINK);
    planner.plan(null).join();
    
    // validate
    assertEquals(2, planner.sources().size());
    assertTrue(planner.sources().contains(STORE_NODES.get(0)));
    assertTrue(planner.sources().contains(STORE_NODES.get(1)));
    assertEquals(4, planner.graph().nodes().size());
    assertEquals(3, planner.graph().edges().size());
    assertTrue(planner.graph().hasEdgeConnecting(
        SINK, planner.nodeForId("Merger")));
    assertTrue(planner.graph().hasEdgeConnecting(
        planner.nodeForId("Merger"), 
        planner.nodeForId("m1")));
    assertTrue(planner.graph().hasEdgeConnecting(
        planner.nodeForId("Merger"), 
        planner.nodeForId("m2")));
    
    assertEquals(1, planner.serializationSources().size());
  }
  
  @Test
  public void idConvertOneByteSourcesPush() throws Exception {
    mockPush();
    
    List<QueryNodeConfig> graph = Lists.newArrayList(
        DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setFilterId("f1")
            .setSourceId("s1")
            .setId("m1")
            .build(),
        DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.sys")
                .build())
            .setFilterId("f1")
            .setSourceId("s1")
            .setId("m2")
            .build(),
        MergerConfig.newBuilder()
            .setAggregator("sum")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .addSource("m1")
            .addSource("m2")
            .setDataSource("m1")
            .setId("Merger")
            .build());
    
    SemanticQuery query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .setExecutionGraph(graph)
        .build();
    
    when(context.query()).thenReturn(query);
   

    DefaultQueryPlanner planner = 
        new DefaultQueryPlanner(context, SINK);
    planner.plan(null).join();
    
    // validate
    assertEquals(2, planner.sources().size());
    assertTrue(planner.sources().contains(STORE_NODES.get(0)));
    assertTrue(planner.sources().contains(STORE_NODES.get(1)));
    assertEquals(5, planner.graph().nodes().size());
    assertEquals(4, planner.graph().edges().size());
    assertTrue(planner.graph().hasEdgeConnecting(
        SINK, planner.nodeForId("IDConverter")));
    assertTrue(planner.graph().hasEdgeConnecting(
        planner.nodeForId("IDConverter"), planner.nodeForId("Merger")));
    assertTrue(planner.graph().hasEdgeConnecting(
        planner.nodeForId("Merger"), 
        planner.nodeForId("m1")));
    assertTrue(planner.graph().hasEdgeConnecting(
        planner.nodeForId("Merger"), 
        planner.nodeForId("m2")));
    
    assertEquals(1, planner.serializationSources().size());
  }
  
  @Test
  public void idConvertOneByteOneStringSources() throws Exception {
    List<QueryNodeConfig> graph = Lists.newArrayList(
        DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setFilterId("f1")
            .setSourceId("s1")
            .setId("m1")
            .build(),
        DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.sys")
                .build())
            .setFilterId("f1")
            .setId("m2")
            .build(),
        MergerConfig.newBuilder()
            .setAggregator("sum")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .addSource("m1")
            .addSource("m2")
            .setDataSource("m1")
            .setId("Merger")
            .build());
    
    SemanticQuery query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .setExecutionGraph(graph)
        .build();
    
    when(context.query()).thenReturn(query);

    DefaultQueryPlanner planner = 
        new DefaultQueryPlanner(context, SINK);
    planner.plan(null).join();
        
    // validate
    assertEquals(2, planner.sources().size());
    assertTrue(planner.sources().contains(STORE_NODES.get(0)));
    assertTrue(planner.sources().contains(STORE_NODES.get(1)));
    assertEquals(5, planner.graph().nodes().size());
    assertEquals(4, planner.graph().edges().size());
    assertTrue(planner.graph().hasEdgeConnecting(
        SINK, planner.nodeForId("Merger")));
    assertTrue(planner.graph().hasEdgeConnecting(
        planner.nodeForId("Merger"), 
        planner.nodeForId("Merger_IdConverter")));
    assertTrue(planner.graph().hasEdgeConnecting(
        planner.nodeForId("Merger_IdConverter"),
        planner.nodeForId("m1")));
    assertTrue(planner.graph().hasEdgeConnecting(
        planner.nodeForId("Merger_IdConverter"),
        planner.nodeForId("m2")));
    
    assertEquals(1, planner.serializationSources().size());
  }
  
  @Test
  public void idConvertOneByteOneStringSourcesPush() throws Exception {
    mockPush();
    List<QueryNodeConfig> graph = Lists.newArrayList(
        DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setFilterId("f1")
            .setSourceId("s1")
            .setId("m1")
            .build(),
        DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.sys")
                .build())
            .setFilterId("f1")
            .setId("m2")
            .build(),
        MergerConfig.newBuilder()
            .setAggregator("sum")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .addSource("m1")
            .addSource("m2")
            .setDataSource("m1")
            .setId("Merger")
            .build());
    
    SemanticQuery query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .setExecutionGraph(graph)
        .build();
    
    when(context.query()).thenReturn(query);

    DefaultQueryPlanner planner = 
        new DefaultQueryPlanner(context, SINK);
    planner.plan(null).join();
    
    // validate
    assertEquals(2, planner.sources().size());
    assertTrue(planner.sources().contains(STORE_NODES.get(0)));
    assertTrue(planner.sources().contains(STORE_NODES.get(1)));
    assertEquals(5, planner.graph().nodes().size());
    assertEquals(4, planner.graph().edges().size());
    assertTrue(planner.graph().hasEdgeConnecting(
        SINK, planner.nodeForId("IDConverter")));
    assertTrue(planner.graph().hasEdgeConnecting(
        planner.nodeForId("IDConverter"), 
        planner.nodeForId("Merger")));
    assertTrue(planner.graph().hasEdgeConnecting(
        planner.nodeForId("Merger"),
        planner.nodeForId("m1")));
    assertTrue(planner.graph().hasEdgeConnecting(
        planner.nodeForId("Merger"),
        planner.nodeForId("m2")));
    
    assertEquals(1, planner.serializationSources().size());
  }
  
  @Test
  public void idConvertMultiLevelMerge() throws Exception {
    List<QueryNodeConfig> graph = Lists.newArrayList(
        DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setFilterId("f1")
            .setSourceId("s1")
            .setId("m1")
            .build(),
        DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.sys")
                .build())
            .setFilterId("f1")
            .setSourceId("s2")
            .setId("m2")
            .build(),
            DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.sys")
                .build())
            .setFilterId("f1")
            .setId("m3")
            .build(),
        MergerConfig.newBuilder()
            .setAggregator("sum")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .addSource("m1")
            .addSource("m2")
            .setDataSource("m1")
            .setId("Merger1")
            .build(),
        MergerConfig.newBuilder()
            .setAggregator("sum")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .addSource("Merger1")
            .addSource("m3")
            .setDataSource("m3")
            .setId("Merger2")
            .build());
    
    SemanticQuery query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .setExecutionGraph(graph)
        .build();
    
    when(context.query()).thenReturn(query);

    DefaultQueryPlanner planner = 
        new DefaultQueryPlanner(context, SINK);
    planner.plan(null).join();
    
    // validate
    assertEquals(3, planner.sources().size());
    assertTrue(planner.sources().contains(STORE_NODES.get(0)));
    assertTrue(planner.sources().contains(STORE_NODES.get(1)));
    assertEquals(8, planner.graph().nodes().size());
    assertEquals(7, planner.graph().edges().size());
    assertTrue(planner.graph().hasEdgeConnecting(
        SINK, planner.nodeForId("Merger2")));
    assertTrue(planner.graph().hasEdgeConnecting(
        planner.nodeForId("Merger1"), 
        planner.nodeForId("Merger1_IdConverter")));
    assertTrue(planner.graph().hasEdgeConnecting(
        planner.nodeForId("Merger2_IdConverter"),
        planner.nodeForId("m3")));
    assertTrue(planner.graph().hasEdgeConnecting(
        planner.nodeForId("Merger2"),
        planner.nodeForId("Merger2_IdConverter")));
    assertTrue(planner.graph().hasEdgeConnecting(
        planner.nodeForId("Merger1_IdConverter"),
        planner.nodeForId("m1")));
    assertTrue(planner.graph().hasEdgeConnecting(
        planner.nodeForId("Merger1_IdConverter"),
        planner.nodeForId("m2")));
    assertTrue(planner.graph().hasEdgeConnecting(
        planner.nodeForId("Merger2_IdConverter"),
        planner.nodeForId("Merger1")));
    
    assertEquals(1, planner.serializationSources().size());
  }
  
  @Test
  public void idConvertMultiLevelMergePush() throws Exception {
    mockPush();
    List<QueryNodeConfig> graph = Lists.newArrayList(
        DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setFilterId("f1")
            .setSourceId("s1")
            .setId("m1")
            .build(),
        DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.sys")
                .build())
            .setFilterId("f1")
            .setSourceId("s2")
            .setId("m2")
            .build(),
            DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.sys")
                .build())
            .setFilterId("f1")
            .setId("m3")
            .build(),
        MergerConfig.newBuilder()
            .setAggregator("sum")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .addSource("m1")
            .addSource("m2")
            .setDataSource("m1")
            .setId("Merger1")
            .build(),
        MergerConfig.newBuilder()
            .setAggregator("sum")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .addSource("Merger1")
            .addSource("m3")
            .setDataSource("m1")
            .setId("Merger2")
            .build());
    
    SemanticQuery query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .setExecutionGraph(graph)
        .build();
    
    when(context.query()).thenReturn(query);

    DefaultQueryPlanner planner = 
        new DefaultQueryPlanner(context, SINK);
    planner.plan(null).join();
    
    // validate
    assertEquals(3, planner.sources().size());
    assertTrue(planner.sources().contains(STORE_NODES.get(0)));
    assertTrue(planner.sources().contains(STORE_NODES.get(1)));
    assertEquals(7, planner.graph().nodes().size());
    assertEquals(6, planner.graph().edges().size());
    assertTrue(planner.graph().hasEdgeConnecting(
        SINK, planner.nodeForId("IDConverter")));
    assertTrue(planner.graph().hasEdgeConnecting(
        planner.nodeForId("IDConverter"), 
        planner.nodeForId("Merger2")));
    assertTrue(planner.graph().hasEdgeConnecting(
        planner.nodeForId("Merger2"),
        planner.nodeForId("m3")));
    assertTrue(planner.graph().hasEdgeConnecting(
        planner.nodeForId("Merger2"),
        planner.nodeForId("Merger1")));
    assertTrue(planner.graph().hasEdgeConnecting(
        planner.nodeForId("Merger1"),
        planner.nodeForId("m1")));
    assertTrue(planner.graph().hasEdgeConnecting(
        planner.nodeForId("Merger1"),
        planner.nodeForId("m2")));
    
    assertEquals(1, planner.serializationSources().size());
  }
  
  @Test
  public void cycleFound() throws Exception {
    List<QueryNodeConfig> graph = Lists.newArrayList(
        DefaultTimeSeriesDataSourceConfig.newBuilder()
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
            .addSource("gb")
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
        DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setFilterId("f1")
            .setId("m1")
            .build(),
        DefaultTimeSeriesDataSourceConfig.newBuilder()
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
      fail("Expected QueryExecutionException");
    } catch (QueryExecutionException e) { }
    assertNull(planner.graph());
  }
  
  @Test
  public void unsatisfiedFilter() throws Exception {
    List<QueryNodeConfig> graph = Lists.newArrayList(
        DefaultTimeSeriesDataSourceConfig.newBuilder()
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
      fail("Expected QueryExecutionException");
    } catch (QueryExecutionException e) { }
    assertNull(planner.graph());
  }
  
  @Test
  public void replace() throws Exception {
    when(context.query()).thenReturn(mock(SemanticQuery.class));
    QueryNodeConfig u1 = mock(QueryNodeConfig.class);
    QueryNodeConfig n = mock(QueryNodeConfig.class);
    QueryNodeConfig d1 = mock(QueryNodeConfig.class);
    QueryNodeConfig r = mock(QueryNodeConfig.class);
    
    // middle
    DefaultQueryPlanner planner = 
        new DefaultQueryPlanner(context, SINK);
    planner.addEdge(u1, n);
    planner.addEdge(n, d1);
    assertTrue(planner.configGraph().hasEdgeConnecting(u1, n));
    assertTrue(planner.configGraph().hasEdgeConnecting(n, d1));
    planner.replace(n, r);
    assertTrue(planner.configGraph().hasEdgeConnecting(u1, r));
    assertTrue(planner.configGraph().hasEdgeConnecting(r, d1));
    
    // sink
    planner = new DefaultQueryPlanner(context, SINK);
    planner.addEdge(n, d1);
    assertTrue(planner.configGraph().hasEdgeConnecting(n, d1));
    planner.replace(n, r);
    assertTrue(planner.configGraph().hasEdgeConnecting(r, d1));
    
    // root
    planner = new DefaultQueryPlanner(context, SINK);
    planner.addEdge(u1, n);
    assertTrue(planner.configGraph().hasEdgeConnecting(u1, n));
    planner.replace(n, r);
    assertTrue(planner.configGraph().hasEdgeConnecting(u1, r));
    
    // non-source with source
    r = mock(TimeSeriesDataSourceConfig.class);
    planner = new DefaultQueryPlanner(context, SINK);
    planner.addEdge(n, d1);
    assertTrue(planner.source_nodes.isEmpty());
    planner.replace(n, r);
    assertTrue(planner.source_nodes.contains(r));
    
    // source with source
    n = mock(TimeSeriesDataSourceConfig.class);
    planner = new DefaultQueryPlanner(context, SINK);
    planner.addEdge(n, d1);
    assertTrue(planner.source_nodes.contains(n));
    planner.replace(n, r);
    assertTrue(planner.source_nodes.contains(r));
    assertFalse(planner.source_nodes.contains(n));
    
    // source with non-source
    r = mock(QueryNodeConfig.class);
    planner = new DefaultQueryPlanner(context, SINK);
    planner.addEdge(n, d1);
    assertTrue(planner.source_nodes.contains(n));
    planner.replace(n, r);
    assertFalse(planner.source_nodes.contains(r));
    assertFalse(planner.source_nodes.contains(n));
  }

  @Test
  public void addEdge() throws Exception {
    when(context.query()).thenReturn(mock(SemanticQuery.class));
    QueryNodeConfig u1 = mock(QueryNodeConfig.class);
    QueryNodeConfig n = mock(QueryNodeConfig.class);
    QueryNodeConfig d1 = mock(QueryNodeConfig.class);
    
    DefaultQueryPlanner planner = 
        new DefaultQueryPlanner(context, SINK);
    planner.addEdge(u1, n);
    planner.addEdge(n, d1);
    assertTrue(planner.configGraph().hasEdgeConnecting(u1, n));
    assertTrue(planner.configGraph().hasEdgeConnecting(n, d1));
    
    // cycle
    try {
      planner.addEdge(d1, u1);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // from source
    u1 = mock(TimeSeriesDataSourceConfig.class);
    d1 = mock(TimeSeriesDataSourceConfig.class);
    planner = new DefaultQueryPlanner(context, SINK);
    assertTrue(planner.source_nodes.isEmpty());
    planner.addEdge(u1, n);
    assertEquals(1, planner.source_nodes.size());
    assertTrue(planner.source_nodes.contains(u1));
    
    // to source
    planner.addEdge(n, d1);
    assertEquals(2, planner.source_nodes.size());
    assertTrue(planner.source_nodes.contains(u1));
    assertTrue(planner.source_nodes.contains(d1));
  }
  
  @Test
  public void removeEdge() throws Exception {
    when(context.query()).thenReturn(mock(SemanticQuery.class));
    QueryNodeConfig u1 = mock(QueryNodeConfig.class);
    QueryNodeConfig n = mock(QueryNodeConfig.class);
    QueryNodeConfig d1 = mock(QueryNodeConfig.class);
    
    DefaultQueryPlanner planner = 
        new DefaultQueryPlanner(context, SINK);
    planner.addEdge(u1, n);
    planner.addEdge(n, d1);
    assertTrue(planner.configGraph().hasEdgeConnecting(u1, n));
    assertTrue(planner.configGraph().hasEdgeConnecting(n, d1));
    
    planner.removeEdge(n, d1);
    assertTrue(planner.configGraph().hasEdgeConnecting(u1, n));
    assertFalse(planner.configGraph().hasEdgeConnecting(n, d1));
    assertTrue(planner.configGraph().nodes().contains(u1));
    assertTrue(planner.configGraph().nodes().contains(n));
    assertFalse(planner.configGraph().nodes().contains(d1));
    
    planner.removeEdge(u1, n);
    assertFalse(planner.configGraph().hasEdgeConnecting(u1, n));
    assertFalse(planner.configGraph().hasEdgeConnecting(n, d1));
    assertFalse(planner.configGraph().nodes().contains(u1));
    assertFalse(planner.configGraph().nodes().contains(n));
    assertFalse(planner.configGraph().nodes().contains(d1));
  }
  
  @Test
  public void removeNode() throws Exception {
    when(context.query()).thenReturn(mock(SemanticQuery.class));
    QueryNodeConfig u1 = mock(QueryNodeConfig.class);
    QueryNodeConfig n = mock(QueryNodeConfig.class);
    QueryNodeConfig d1 = mock(QueryNodeConfig.class);
    
    DefaultQueryPlanner planner = 
        new DefaultQueryPlanner(context, SINK);
    planner.addEdge(u1, n);
    planner.addEdge(n, d1);
    assertTrue(planner.configGraph().hasEdgeConnecting(u1, n));
    assertTrue(planner.configGraph().hasEdgeConnecting(n, d1));
    
    planner.removeNode(d1);
    assertTrue(planner.configGraph().hasEdgeConnecting(u1, n));
    assertFalse(planner.configGraph().hasEdgeConnecting(n, d1));
    assertTrue(planner.configGraph().nodes().contains(u1));
    assertTrue(planner.configGraph().nodes().contains(n));
    assertFalse(planner.configGraph().nodes().contains(d1));
  }
  
  @Test
  public void q1_smoothing_expressions_shift() throws Exception {
    // A complex real-world query, used to fix hashes
    String json = "{\"start\": \"1569862761754\",\"end\": \"1569866361754\",\"filters\": "
        + "[{\"id\": \"f1\",\"filter\": {\"filters\": [{\"filter\": \"normal\",\"type\": "
        + "\"TagValueLiteralOr\",\"tagKey\": \"type\"}],\"op\": \"AND\",\"type\": \"Chain\"}}],"
        + "\"mode\": \"SINGLE\",\"traceEnabled\": false,\"debugEnabled\": false,"
        + "\"warnEnabled\": false,\"timezone\": null,\"cacheMode\": \"NORMAL\","
        + "\"executionGraph\": [{\"id\": \"q1_m1\",\"type\": \"TimeSeriesDataSource\","
        + "\"metric\": {\"metric\": \"ssp.exch.bdr.BidResponses\",\"type\": \"MetricLiteral\"},"
        + "\"filterId\": \"f1\",\"dataSourceId\": \"q1_m1\"}, {\"id\": "
        + "\"q1_m1_downsample\",\"type\": \"Downsample\",\"sources\": [\"q1_m1\"],"
        + "\"interval\": \"1m\",\"timezone\": \"UTC\",\"aggregator\": "
        + "\"avg\",\"fill\": true,\"runAll\": false,\"originalInterval\": "
        + "\"auto\",\"infectiousNan\": false,\"interpolatorConfigs\": "
        + "[{\"fillPolicy\": \"nan\",\"realFillPolicy\": \"NONE\",\"dataType\": "
        + "\"numeric\"}]}, {\"id\": \"q1_m1_groupby\",\"type\": \"GroupBy\",\"sources\": "
        + "[\"q1_m1_downsample\"],\"aggregator\": \"sum\",\"infectiousNan\": false,"
        + "\"fullMerge\": false,\"mergeIds\": false,\"tagKeys\": [],"
        + "\"interpolatorConfigs\": [{\"fillPolicy\": \"nan\",\"realFillPolicy\": "
        + "\"NONE\",\"dataType\": \"numeric\"}]}, {\"id\": \"q1_m1-smooth\",\"type\":"
        + " \"MovingAverage\",\"sources\": [\"q1_m1_groupby\"],\"samples\": 5,"
        + "\"alpha\": 0.0,\"median\": false,\"weighted\": false,\"exponential\": true,"
        + "\"infectiousNan\": false,\"averageInitial\": true}, {\"id\": \"q1_m2\","
        + "\"type\": \"TimeSeriesDataSource\",\"metric\": {\"metric\": "
        + "\"app.server.requests\",\"type\": \"MetricLiteral\"},\"filterId\": "
        + "\"f1\",\"dataSourceId\": \"q1_m2\"}, {\"id\": \"q1_m2_downsample\","
        + "\"type\": \"Downsample\",\"sources\": [\"q1_m2\"],\"interval\": \"1m\","
        + "\"timezone\": \"UTC\",\"aggregator\": \"avg\",\"fill\": true,\"runAll\": false,"
        + "\"originalInterval\": \"auto\",\"infectiousNan\": false,"
        + "\"interpolatorConfigs\": [{\"fillPolicy\": \"nan\",\"realFillPolicy\": "
        + "\"NONE\",\"dataType\": \"numeric\"}]}, {\"id\": \"q1_m2_groupby\",\"type\": "
        + "\"GroupBy\",\"sources\": [\"q1_m2_downsample\"],\"aggregator\": \"sum\","
        + "\"infectiousNan\": false,\"fullMerge\": false,\"mergeIds\": false,\"tagKeys\": "
        + "[],\"interpolatorConfigs\": [{\"fillPolicy\": \"nan\",\"realFillPolicy\": "
        + "\"NONE\",\"dataType\": \"numeric\"}]}, {\"id\": \"q1_m2-smooth\",\"type\": "
        + "\"MovingAverage\",\"sources\": [\"q1_m2_groupby\"],\"samples\": 5,\"alpha\": 0.0,"
        + "\"median\": false,\"weighted\": false,\"exponential\": true,\"infectiousNan\": "
        + "false,\"averageInitial\": true}, {\"id\": \"q2_m1\",\"type\": "
        + "\"TimeSeriesDataSource\",\"metric\": {\"metric\": \"ssp.exch.bdr.BidResponses\","
        + "\"type\": \"MetricLiteral\"},\"filterId\": \"f1\",\"timeShiftInterval\": "
        + "\"1d\",\"dataSourceId\": \"q2_m1\"}, {\"id\": \"q2_m1_downsample\",\"type\": "
        + "\"Downsample\",\"sources\": [\"q2_m1\"],\"interval\": \"1m\",\"timezone\":"
        + " \"UTC\",\"aggregator\": \"avg\",\"fill\": true,\"runAll\": false,"
        + "\"originalInterval\": \"auto\",\"infectiousNan\": false,\"interpolatorConfigs\": "
        + "[{\"fillPolicy\": \"nan\",\"realFillPolicy\": \"NONE\",\"dataType\": "
        + "\"numeric\"}]}, {\"id\": \"q2_m1_groupby\",\"type\": \"GroupBy\",\"sources\": "
        + "[\"q2_m1_downsample\"],\"aggregator\": \"sum\",\"infectiousNan\": false,"
        + "\"fullMerge\": false,\"mergeIds\": false,\"tagKeys\": [],\"interpolatorConfigs\": "
        + "[{\"fillPolicy\": \"nan\",\"realFillPolicy\": \"NONE\",\"dataType\": "
        + "\"numeric\"}]}, {\"id\": \"q2_m1-smooth\",\"type\": \"MovingAverage\","
        + "\"sources\": [\"q2_m1_groupby\"],\"samples\": 5,\"alpha\": 0.0,\"median\": "
        + "false,\"weighted\": false,\"exponential\": true,\"infectiousNan\": false,"
        + "\"averageInitial\": true}, {\"id\": \"q2_m2\",\"type\": "
        + "\"TimeSeriesDataSource\",\"metric\": {\"metric\": \"app.server.requests\","
        + "\"type\": \"MetricLiteral\"},\"filterId\": \"f1\",\"timeShiftInterval\": "
        + "\"1d\",\"dataSourceId\": \"q2_m2\"}, {\"id\": \"q2_m2_downsample\",\"type\": "
        + "\"Downsample\",\"sources\": [\"q2_m2\"],\"interval\": \"1m\",\"timezone\": "
        + "\"UTC\",\"aggregator\": \"avg\",\"fill\": true,\"runAll\": false,"
        + "\"originalInterval\": \"auto\",\"infectiousNan\": false,\"interpolatorConfigs\": "
        + "[{\"fillPolicy\": \"nan\",\"realFillPolicy\": \"NONE\",\"dataType\": "
        + "\"numeric\"}]}, {\"id\": \"q2_m2_groupby\",\"type\": \"GroupBy\",\"sources\": "
        + "[\"q2_m2_downsample\"],\"aggregator\": \"sum\",\"infectiousNan\": false,"
        + "\"fullMerge\": false,\"mergeIds\": false,\"tagKeys\": [],\"interpolatorConfigs\": "
        + "[{\"fillPolicy\": \"nan\",\"realFillPolicy\": \"NONE\",\"dataType\": \"numeric\"}]}, "
        + "{\"id\": \"q2_m2-smooth\",\"type\": \"MovingAverage\",\"sources\": "
        + "[\"q2_m2_groupby\"],\"samples\": 5,\"alpha\": 0.0,\"median\": false,"
        + "\"weighted\": false,\"exponential\": true,\"infectiousNan\": false,"
        + "\"averageInitial\": true}, {\"id\": \"q3_m1\",\"type\": \"TimeSeriesDataSource\","
        + "\"metric\": {\"metric\": \"ssp.exch.bdr.BidResponses\",\"type\": "
        + "\"MetricLiteral\"},\"filterId\": \"f1\",\"timeShiftInterval\": \"1w\","
        + "\"dataSourceId\": \"q3_m1\"}, {\"id\": \"q3_m1_downsample\",\"type\": "
        + "\"Downsample\",\"sources\": [\"q3_m1\"],\"interval\": \"1m\",\"timezone\": "
        + "\"UTC\",\"aggregator\": \"avg\",\"fill\": true,\"runAll\": false,"
        + "\"originalInterval\": \"auto\",\"infectiousNan\": false,\"interpolatorConfigs\": "
        + "[{\"fillPolicy\": \"nan\",\"realFillPolicy\": \"NONE\",\"dataType\": "
        + "\"numeric\"}]}, {\"id\": \"q3_m1_groupby\",\"type\": \"GroupBy\",\"sources\": "
        + "[\"q3_m1_downsample\"],\"aggregator\": \"sum\",\"infectiousNan\": "
        + "false,\"fullMerge\": false,\"mergeIds\": false,\"tagKeys\": [],"
        + "\"interpolatorConfigs\": [{\"fillPolicy\": \"nan\",\"realFillPolicy\": "
        + "\"NONE\",\"dataType\": \"numeric\"}]}, {\"id\": \"q3_m1-smooth\",\"type\": "
        + "\"MovingAverage\",\"sources\": [\"q3_m1_groupby\"],\"samples\": 5,"
        + "\"alpha\": 0.0,\"median\": false,\"weighted\": false,\"exponential\": true,"
        + "\"infectiousNan\": false,\"averageInitial\": true}, {\"id\": \"q3_m2\","
        + "\"type\": \"TimeSeriesDataSource\",\"metric\": {\"metric\": \"app.server.requests\","
        + "\"type\": \"MetricLiteral\"},\"filterId\": \"f1\",\"timeShiftInterval\": "
        + "\"1w\",\"dataSourceId\": \"q3_m2\"}, {\"id\": \"q3_m2_downsample\",\"type\": "
        + "\"Downsample\",\"sources\": [\"q3_m2\"],\"interval\": \"1m\",\"timezone\": "
        + "\"UTC\",\"aggregator\": \"avg\",\"fill\": true,\"runAll\": false,"
        + "\"originalInterval\": \"auto\",\"infectiousNan\": false,\"interpolatorConfigs\": "
        + "[{\"fillPolicy\": \"nan\",\"realFillPolicy\": \"NONE\",\"dataType\":"
        + " \"numeric\"}]}, {\"id\": \"q3_m2_groupby\",\"type\": \"GroupBy\",\"sources\": "
        + "[\"q3_m2_downsample\"],\"aggregator\": \"sum\",\"infectiousNan\": false,"
        + "\"fullMerge\": false,\"mergeIds\": false,\"tagKeys\": [],\"interpolatorConfigs\": "
        + "[{\"fillPolicy\": \"nan\",\"realFillPolicy\": \"NONE\",\"dataType\": "
        + "\"numeric\"}]}, {\"id\": \"q3_m2-smooth\",\"type\": \"MovingAverage\","
        + "\"sources\": [\"q3_m2_groupby\"],\"samples\": 5,\"alpha\": 0.0,\"median\": false,"
        + "\"weighted\": false,\"exponential\": true,\"infectiousNan\": false,"
        + "\"averageInitial\": true}, {\"id\": \"q1_e1\",\"type\": \"Expression\","
        + "\"sources\": [\"q1_m1-smooth\", \"q1_m2-smooth\"],\"expression\": "
        + "\" q1_m1  /  q1_m2  * 100\",\"as\": \"q1_e1\",\"infectiousNan\": true,"
        + "\"substituteMissing\": true,\"join\": {\"id\": \"Join\",\"type\": \"Join\","
        + "\"sources\": [],\"joins\": {},\"joinType\": \"NATURAL_OUTER\","
        + "\"explicitTags\": false},\"interpolatorConfigs\": [{\"fillPolicy\": \"nan\","
        + "\"realFillPolicy\": \"NONE\",\"dataType\": \"numeric\"}]}, {\"id\": \"q2_e1\","
        + "\"type\": \"Expression\",\"sources\": [\"q2_m1-smooth\", \"q2_m2-smooth\"],"
        + "\"expression\": \" q2_m1  /  q2_m2  * 100\",\"as\": \"q2_e1\",\"infectiousNan\": true,"
        + "\"substituteMissing\": true,\"join\": {\"id\": \"Join\",\"type\": \"Join\","
        + "\"sources\": [],\"joins\": {},\"joinType\": \"NATURAL_OUTER\","
        + "\"explicitTags\": false},\"interpolatorConfigs\": [{\"fillPolicy\": \"nan\","
        + "\"realFillPolicy\": \"NONE\",\"dataType\": \"numeric\"}]}, {\"id\": "
        + "\"q3_e1\",\"type\": \"Expression\",\"sources\": [\"q3_m1-smooth\", \"q3_m2-smooth\"],"
        + "\"expression\": \" q3_m1  /  q3_m2  * 100\",\"as\": \"q3_e1\","
        + "\"infectiousNan\": true,\"substituteMissing\": true,\"join\": {\"id\": "
        + "\"Join\",\"type\": \"Join\",\"sources\": [],\"joins\": {},\"joinType\": "
        + "\"NATURAL_OUTER\",\"explicitTags\": false},\"interpolatorConfigs\": ["
        + "{\"fillPolicy\": \"nan\",\"realFillPolicy\": \"NONE\",\"dataType\": \"numeric\"}]}],"
        + "\"serdesConfigs\": [{\"id\": \"serdes\",\"type\": \"JsonV3QuerySerdes\","
        + "\"filter\": [\"q2_m1-smooth\", \"q2_m2-smooth\", \"q3_e1\", \"q2_e1\", "
        + "\"q3_m1-smooth\", \"q3_m2-smooth\", \"q1_e1\", \"q1_m2-smooth\", \"q1_m1-smooth\"],"
        + "\"msResolution\": false,\"showQuery\": false,\"showStats\": false,"
        + "\"showSummary\": false,\"showTsuids\": false,\"parallelThreshold\": 0}],"
        + "\"logLevel\": \"ERROR\"}";

    SemanticQuery query = SemanticQuery.parse(TSDB, JSON.getMapper().readTree(json)).build();
    when(context.query()).thenReturn(query);
    
    DefaultQueryPlanner planner = 
        new DefaultQueryPlanner(context, SINK);
    planner.plan(null).join();
 
    // validate
    assertEquals(6, planner.sources().size());
    assertEquals(6, STORE_NODES.size());
    assertTrue(planner.sources().contains(STORE_NODES.get(0)));
    assertTrue(planner.sources().contains(STORE_NODES.get(1)));
    assertTrue(planner.sources().contains(STORE_NODES.get(2)));
    assertEquals(31, planner.graph().nodes().size());

    assertEquals(9, planner.serializationSources().size());
    assertTrue(planner.graph().hasEdgeConnecting(SINK, 
        planner.nodeForId("q1_e1")));
    assertTrue(planner.graph().hasEdgeConnecting(SINK, 
        planner.nodeForId("q2_e1")));
    assertTrue(planner.graph().hasEdgeConnecting(SINK, 
        planner.nodeForId("q3_e1")));
    assertTrue(planner.graph().hasEdgeConnecting(SINK, 
        planner.nodeForId("q1_m1-smooth")));
    assertTrue(planner.graph().hasEdgeConnecting(SINK, 
        planner.nodeForId("q2_m1-smooth")));
    assertTrue(planner.graph().hasEdgeConnecting(SINK, 
        planner.nodeForId("q3_m1-smooth")));
    assertTrue(planner.graph().hasEdgeConnecting(SINK, 
        planner.nodeForId("q1_m2-smooth")));
    assertTrue(planner.graph().hasEdgeConnecting(SINK, 
        planner.nodeForId("q2_m2-smooth")));
    assertTrue(planner.graph().hasEdgeConnecting(SINK, 
        planner.nodeForId("q3_m2-smooth")));
  }
  
  private SerdesOptions serdesConfigs(final List<String> filter) {
    final SerdesOptions config = mock(SerdesOptions.class);
    when(config.getFilter()).thenReturn(filter);
    return config;
  }

  void mockPush() {
    net.opentsdb.core.TSDB mock_tsdb = mock(net.opentsdb.core.TSDB.class);
    Configuration config = mock(Configuration.class);
    when(config.hasProperty("tsd.storage.enable_push")).thenReturn(true);
    when(config.getBoolean("tsd.storage.enable_push")).thenReturn(true);
    when(mock_tsdb.getConfig()).thenReturn(config);
    when(mock_tsdb.getRegistry()).thenReturn(TSDB.registry);
    when(context.tsdb()).thenReturn(mock_tsdb);
  }
}
