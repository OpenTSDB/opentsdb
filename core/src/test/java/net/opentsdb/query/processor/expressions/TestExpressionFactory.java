//This file is part of OpenTSDB.
//Copyright (C) 2018-2020  The OpenTSDB Authors.
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.
package net.opentsdb.query.processor.expressions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import net.opentsdb.query.BaseTimeSeriesDataSourceConfig;
import net.opentsdb.query.DefaultQueryResultId;
import net.opentsdb.query.DefaultTimeSeriesDataSourceConfig;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryMode;
import net.opentsdb.query.QueryNode;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.common.Const;
import net.opentsdb.core.DefaultRegistry;
import net.opentsdb.core.MockTSDB;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSDBPlugin;
import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.data.TimeSeriesDataSourceFactory;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.joins.JoinConfig;
import net.opentsdb.query.joins.JoinConfig.JoinType;
import net.opentsdb.query.plan.DefaultQueryPlanner;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.query.processor.expressions.ExpressionParseNode.ExpressionOp;
import net.opentsdb.query.processor.expressions.ExpressionParseNode.OperandType;
import net.opentsdb.query.processor.expressions.ExpressionParser.NumericLiteral;
import net.opentsdb.query.processor.merge.MergerConfig;

public class TestExpressionFactory {

  private static MockTSDB TSDB;
  private static TimeSeriesDataSourceFactory STORE_FACTORY;
  protected static NumericInterpolatorConfig NUMERIC_CONFIG;
  protected static JoinConfig JOIN_CONFIG;
  protected static QueryNode SINK;
  private static List<TimeSeriesDataSource> STORE_NODES;
  private static TimeSeriesDataSourceFactory S1;
  private static TimeSeriesDataSourceFactory S2;
  
  private QueryPipelineContext context;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    TSDB = new MockTSDB();
    STORE_FACTORY = mock(TimeSeriesDataSourceFactory.class);
    NUMERIC_CONFIG = 
        (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
      .setFillPolicy(FillPolicy.NOT_A_NUMBER)
      .setRealFillPolicy(FillWithRealPolicy.NONE)
      .setDataType(NumericType.TYPE.toString())
      .build();
    
    JOIN_CONFIG = (JoinConfig) JoinConfig.newBuilder()
        .setJoinType(JoinType.NATURAL)
        .build();
    
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
  public void ctor() throws Exception {
    ExpressionFactory factory = new ExpressionFactory();
    assertEquals(0, factory.types().size());
  }
  
  @Test
  public void setupGraph1MetricDirect() throws Exception {
    List<QueryNodeConfig> graph = Lists.newArrayList(
        DefaultTimeSeriesDataSourceConfig.newBuilder()
          .setMetric(MetricLiteralFilter.newBuilder()
              .setMetric("sys.cpu.user")
              .build())
          .setFilterId("f1")
          .setId("m1")
          .build(),
        ExpressionConfig.newBuilder()
          .setExpression("m1 + 42")
          .setJoinConfig((JoinConfig) JoinConfig.newBuilder()
              .setJoinType(JoinType.NATURAL)
              .build())
          .addInterpolatorConfig(NUMERIC_CONFIG)
          .setId("expression")
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
    
    assertSame(STORE_NODES.get(0), planner.sources().get(0));
    assertEquals(3, planner.graph().nodes().size());
    assertEquals(1, planner.serializationSources().size());
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("expression"), 
        planner.nodeForId("m1")));
    assertTrue(planner.graph().hasEdgeConnecting(SINK, planner.nodeForId("expression")));

    QueryNodeConfig b1 = planner.configNodeForId("expression");
    assertEquals("BinaryExpression", b1.getType());
    
    ExpressionParseNode p1 = (ExpressionParseNode) b1;
    assertEquals("expression", p1.getId());
    assertEquals("sys.cpu.user", p1.getLeft());
    assertEquals(new DefaultQueryResultId("m1", "m1"), p1.getLeftId());
    assertEquals(OperandType.VARIABLE, p1.getLeftType());
    assertEquals(42, ((NumericLiteral) p1.getRight()).longValue());
    assertEquals(OperandType.LITERAL_NUMERIC, p1.getRightType());
    assertNull(p1.getRightId());
    assertEquals(ExpressionOp.ADD, p1.getOperator());
    assertEquals(1, p1.resultIds().size());
    assertEquals(new DefaultQueryResultId("expression", "expression"), 
        p1.resultIds().get(0));
  }
  
  @Test
  public void setupGraph1MetricThroughNode() throws Exception {
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
        ExpressionConfig.newBuilder()
          .setExpression("m1 + 42")
          .setJoinConfig((JoinConfig) JoinConfig.newBuilder()
              .setJoinType(JoinType.NATURAL)
              .build())
          .addInterpolatorConfig(NUMERIC_CONFIG)
          .setId("expression")
          .addSource("downsample")
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

    assertSame(STORE_NODES.get(0), planner.sources().get(0));
    assertEquals(4, planner.graph().nodes().size());
    assertEquals(1, planner.serializationSources().size());
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("expression"), 
        planner.nodeForId("downsample")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("downsample"), 
        planner.nodeForId("m1")));
    assertTrue(planner.graph().hasEdgeConnecting(SINK, planner.nodeForId("expression")));
    
    QueryNodeConfig b1 = planner.configNodeForId("expression");
    assertEquals("BinaryExpression", b1.getType());
    
    ExpressionParseNode p1 = (ExpressionParseNode) b1;
    assertEquals("expression", p1.getId());
    assertEquals("sys.cpu.user", p1.getLeft());
    assertEquals(new DefaultQueryResultId("downsample", "m1"), p1.getLeftId());
    assertEquals(OperandType.VARIABLE, p1.getLeftType());
    assertEquals(42, ((NumericLiteral) p1.getRight()).longValue());
    assertEquals(OperandType.LITERAL_NUMERIC, p1.getRightType());
    assertNull(p1.getRightId());
    assertEquals(ExpressionOp.ADD, p1.getOperator());
    assertEquals(1, p1.resultIds().size());
    assertEquals(new DefaultQueryResultId("expression", "expression"), 
        p1.resultIds().get(0));
  }
  
  @Test
  public void setupGraph2MetricsThroughNode() throws Exception {
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
        ExpressionConfig.newBuilder()
          .setExpression("m1 + m2")
          .setJoinConfig(JoinConfig.newBuilder()
              .setJoinType(JoinType.NATURAL)
              .build())
          .addInterpolatorConfig(NUMERIC_CONFIG)
          .addSource("downsample")
          .setId("expression")
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
    
    assertSame(STORE_NODES.get(0), planner.sources().get(0));
    assertEquals(5, planner.graph().nodes().size());
    assertEquals(1, planner.serializationSources().size());
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("expression"), 
        planner.nodeForId("downsample")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("downsample"), 
        planner.nodeForId("m1")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("downsample"), 
        planner.nodeForId("m2")));
    assertTrue(planner.graph().hasEdgeConnecting(SINK, planner.nodeForId("expression")));
    
    QueryNodeConfig b1 = planner.configNodeForId("expression");
    assertEquals("BinaryExpression", b1.getType());
    
    ExpressionParseNode p1 = (ExpressionParseNode) b1;
    assertEquals("expression", p1.getId());
    assertEquals("sys.cpu.user", p1.getLeft());
    assertEquals(new DefaultQueryResultId("downsample", "m1"), p1.getLeftId());
    assertEquals(OperandType.VARIABLE, p1.getLeftType());
    assertEquals("sys.cpu.sys", p1.getRight());
    assertEquals(new DefaultQueryResultId("downsample", "m2"), p1.getRightId());
    assertEquals(OperandType.VARIABLE, p1.getRightType());
    assertEquals(ExpressionOp.ADD, p1.getOperator());
    assertEquals(1, p1.resultIds().size());
    assertEquals(new DefaultQueryResultId("expression", "expression"), 
        p1.resultIds().get(0));
  }
  
  @Test
  public void setupGraph2Metrics1ThroughNode() throws Exception {
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
          .build(),
        ExpressionConfig.newBuilder()
          .setExpression("m1 + m2")
          .setJoinConfig((JoinConfig) JoinConfig.newBuilder()
              .setJoinType(JoinType.NATURAL)
              .build())
          .addInterpolatorConfig(NUMERIC_CONFIG)
          .addSource("downsample")
          .addSource("m2")
          .setId("expression")
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
    
    assertSame(STORE_NODES.get(0), planner.sources().get(0));
    assertEquals(5, planner.graph().nodes().size());
    assertEquals(1, planner.serializationSources().size());
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("expression"), 
        planner.nodeForId("downsample")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("downsample"), 
        planner.nodeForId("m1")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("expression"), 
        planner.nodeForId("m2")));
    assertTrue(planner.graph().hasEdgeConnecting(SINK, planner.nodeForId("expression")));
    
    QueryNodeConfig b1 = planner.configNodeForId("expression");
    assertEquals("BinaryExpression", b1.getType());
    
    ExpressionParseNode p1 = (ExpressionParseNode) b1;
    assertEquals("expression", p1.getId());
    assertEquals("sys.cpu.user", p1.getLeft());
    assertEquals(new DefaultQueryResultId("downsample", "m1"), p1.getLeftId());
    assertEquals(OperandType.VARIABLE, p1.getLeftType());
    assertEquals("sys.cpu.sys", p1.getRight());
    assertEquals(new DefaultQueryResultId("m2", "m2"), p1.getRightId());
    assertEquals(OperandType.VARIABLE, p1.getRightType());
    assertEquals(ExpressionOp.ADD, p1.getOperator());
    assertEquals(1, p1.resultIds().size());
    assertEquals(new DefaultQueryResultId("expression", "expression"), 
        p1.resultIds().get(0));
  }
  
  @Test
  public void setupGraph3MetricsThroughNode() throws Exception {
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
        DefaultTimeSeriesDataSourceConfig.newBuilder()
          .setMetric(MetricLiteralFilter.newBuilder()
              .setMetric("sys.cpu.idle")
              .build())
          .setFilterId("f1")
          .setId("m3")
          .build(),
        DownsampleConfig.newBuilder()
          .setAggregator("sum")
          .setInterval("1m")
          .addInterpolatorConfig(NUMERIC_CONFIG)
          .setId("downsample")
          .setSources(Lists.newArrayList("m1", "m2", "m3"))
          .build(),
        ExpressionConfig.newBuilder()
          .setExpression("m1 + m2 + m3")
          .setJoinConfig((JoinConfig) JoinConfig.newBuilder()
              .setJoinType(JoinType.NATURAL)
              .build())
          .addInterpolatorConfig(NUMERIC_CONFIG)
          .setId("expression")
          .addSource("downsample")
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
    
    assertSame(STORE_NODES.get(0), planner.sources().get(0));
    assertEquals(7, planner.graph().nodes().size());
    assertEquals(1, planner.serializationSources().size());
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("expression"), 
        planner.nodeForId("downsample")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("downsample"), 
        planner.nodeForId("m1")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("downsample"), 
        planner.nodeForId("m2")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("downsample"), 
        planner.nodeForId("m3")));
    assertTrue(planner.graph().hasEdgeConnecting(SINK, planner.nodeForId("expression")));
    
    QueryNodeConfig b1 = planner.configNodeForId("expression");
    assertEquals("BinaryExpression", b1.getType());
    ExpressionParseNode p1 = (ExpressionParseNode) b1;
    assertEquals("expression", p1.getId());
    assertEquals("expression_SubExp#0", p1.getLeft());
    assertEquals(new DefaultQueryResultId(
        "expression_SubExp#0", "expression_SubExp#0"), p1.getLeftId());
    assertEquals(OperandType.SUB_EXP, p1.getLeftType());
    assertEquals("sys.cpu.idle", p1.getRight());
    assertEquals(new DefaultQueryResultId("downsample", "m3"), p1.getRightId());
    assertEquals(OperandType.VARIABLE, p1.getRightType());
    assertEquals(ExpressionOp.ADD, p1.getOperator());
    assertEquals(new DefaultQueryResultId("expression", "expression"), 
        p1.resultIds().get(0));
    
    QueryNodeConfig b2 = planner.configNodeForId("expression_SubExp#0");
    assertEquals("BinaryExpression", b2.getType());
    
    ExpressionParseNode p2 = (ExpressionParseNode) b2;
    assertEquals("expression_SubExp#0", p2.getId());
    assertEquals("sys.cpu.user", p2.getLeft());
    assertEquals(new DefaultQueryResultId("downsample", "m1"), p2.getLeftId());
    assertEquals(OperandType.VARIABLE, p2.getLeftType());
    assertEquals("sys.cpu.sys", p2.getRight());
    assertEquals(new DefaultQueryResultId("downsample", "m2"), p2.getRightId());
    assertEquals(OperandType.VARIABLE, p2.getRightType());
    assertEquals(ExpressionOp.ADD, p2.getOperator());
    assertEquals(new DefaultQueryResultId("expression_SubExp#0", "expression_SubExp#0"), 
        p2.resultIds().get(0));
  }
  
  @Test
  public void setupGraph3MetricsComplexThroughNode() throws Exception {
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
        DefaultTimeSeriesDataSourceConfig.newBuilder()
          .setMetric(MetricLiteralFilter.newBuilder()
              .setMetric("sys.cpu.idle")
              .build())
          .setFilterId("f1")
          .setId("m3")
          .build(),
        DownsampleConfig.newBuilder()
          .setAggregator("sum")
          .setInterval("1m")
          .addInterpolatorConfig(NUMERIC_CONFIG)
          .setId("downsample")
          .setSources(Lists.newArrayList("m1", "m3"))
          .build(),
        ExpressionConfig.newBuilder()
          .setExpression("(m1 * 1024) + (m2 * 1024) + (m3 * 1024)")
          .setJoinConfig((JoinConfig) JoinConfig.newBuilder()
              .setJoinType(JoinType.NATURAL)
              .build())
          .addInterpolatorConfig(NUMERIC_CONFIG)
          .addSource("downsample")
          .addSource("m2")
          .setId("expression")
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
    
    assertSame(STORE_NODES.get(0), planner.sources().get(0));
    assertEquals(10, planner.graph().nodes().size());
    assertEquals(1, planner.serializationSources().size());
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("expression_SubExp#0"), 
        planner.nodeForId("downsample")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("downsample"), 
        planner.nodeForId("m1")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("expression_SubExp#1"), 
        planner.nodeForId("m2")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("downsample"), 
        planner.nodeForId("m3")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("expression_SubExp#2"), 
        planner.nodeForId("expression_SubExp#0")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("expression_SubExp#2"), 
        planner.nodeForId("expression_SubExp#1")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("expression_SubExp#3"), 
        planner.nodeForId("downsample")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("expression"), 
        planner.nodeForId("expression_SubExp#2")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("expression"), 
        planner.nodeForId("expression_SubExp#3")));
    assertTrue(planner.graph().hasEdgeConnecting(SINK, planner.nodeForId("expression")));
    
    ExpressionParseNode node = (ExpressionParseNode) planner.configNodeForId("expression_SubExp#0");
    assertEquals("BinaryExpression", node.getType());
    assertEquals("expression_SubExp#0", node.getId());
    assertEquals("sys.cpu.user", node.getLeft());
    assertEquals(new DefaultQueryResultId("downsample", "m1"), node.getLeftId());
    assertEquals(OperandType.VARIABLE, node.getLeftType());
    assertEquals(1024, ((NumericLiteral) node.getRight()).longValue());
    assertNull(node.getRightId());
    assertEquals(OperandType.LITERAL_NUMERIC, node.getRightType());
    assertEquals(ExpressionOp.MULTIPLY, node.getOperator());
    assertEquals(1, node.resultIds().size());
    assertEquals(new DefaultQueryResultId("expression_SubExp#0", "expression_SubExp#0"), 
        node.resultIds().get(0));
    
    node = (ExpressionParseNode) planner.configNodeForId("expression_SubExp#1");
    assertEquals("BinaryExpression", node.getType());
    assertEquals("expression_SubExp#1", node.getId());
    assertEquals("sys.cpu.sys", node.getLeft());
    assertEquals(new DefaultQueryResultId("m2", "m2"), node.getLeftId());
    assertEquals(OperandType.VARIABLE, node.getLeftType());
    assertEquals(1024, ((NumericLiteral) node.getRight()).longValue());
    assertNull(node.getRightId());
    assertEquals(OperandType.LITERAL_NUMERIC, node.getRightType());
    assertEquals(ExpressionOp.MULTIPLY, node.getOperator());
    assertEquals(1, node.resultIds().size());
    assertEquals(new DefaultQueryResultId("expression_SubExp#1", "expression_SubExp#1"), 
        node.resultIds().get(0));
    
    node = (ExpressionParseNode) planner.configNodeForId("expression_SubExp#3");
    assertEquals("BinaryExpression", node.getType());
    assertEquals("expression_SubExp#3", node.getId());
    assertEquals("sys.cpu.idle", node.getLeft());
    assertEquals(new DefaultQueryResultId("downsample", "m3"), node.getLeftId());
    assertEquals(OperandType.VARIABLE, node.getLeftType());
    assertEquals(1024, ((NumericLiteral) node.getRight()).longValue());
    assertNull(node.getRightId());
    assertEquals(OperandType.LITERAL_NUMERIC, node.getRightType());
    assertEquals(ExpressionOp.MULTIPLY, node.getOperator());
    assertEquals(1, node.resultIds().size());
    assertEquals(new DefaultQueryResultId("expression_SubExp#3", "expression_SubExp#3"), 
        node.resultIds().get(0));
    
    node = (ExpressionParseNode) planner.configNodeForId("expression");
    assertEquals("BinaryExpression", node.getType());
    assertEquals("expression", node.getId());
    assertEquals("expression_SubExp#2", node.getLeft());
    assertEquals(new DefaultQueryResultId("expression_SubExp#2", "expression_SubExp#2"), 
        node.getLeftId());
    assertEquals(OperandType.SUB_EXP, node.getLeftType());
    assertEquals("expression_SubExp#3", node.getRight());
    assertEquals(new DefaultQueryResultId("expression_SubExp#3", "expression_SubExp#3"), 
        node.getRightId());
    assertEquals(OperandType.SUB_EXP, node.getRightType());
    assertEquals(ExpressionOp.ADD, node.getOperator());
    assertEquals(1, node.resultIds().size());
    assertEquals(new DefaultQueryResultId("expression", "expression"), 
        node.resultIds().get(0));
  }
  
  @Test
  public void setupGraphThroughJoinNodeMetricName() throws Exception {
    List<QueryNodeConfig> graph = Lists.newArrayList(
        DefaultTimeSeriesDataSourceConfig.newBuilder()
          .setMetric(MetricLiteralFilter.newBuilder()
              .setMetric("sys.cpu.user")
              .build())
          .setFilterId("f1")
          .setId("ha_m1")
          .build(),
        MergerConfig.newBuilder()
          .setAggregator("sum")
          .addInterpolatorConfig(NUMERIC_CONFIG)
          .setDataSource("m1")
          .setId("m1")
          .addSource("ha_m1")
          .build(),
        DownsampleConfig.newBuilder()
          .setAggregator("sum")
          .setInterval("1m")
          .addInterpolatorConfig(NUMERIC_CONFIG)
          .setId("downsample")
          .addSource("m1")
          .build(),
        ExpressionConfig.newBuilder()
          .setExpression("(sys.cpu.user * 1024)")
          .setJoinConfig((JoinConfig) JoinConfig.newBuilder()
              .setJoinType(JoinType.NATURAL)
              .build())
          .addInterpolatorConfig(NUMERIC_CONFIG)
          .setId("expression")
          .addSource("downsample")
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
    
    assertSame(STORE_NODES.get(0), planner.sources().get(0));
    assertEquals(5, planner.graph().nodes().size());
    assertEquals(1, planner.serializationSources().size());
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("expression"), 
        planner.nodeForId("downsample")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("downsample"), 
        planner.nodeForId("m1")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("m1"), 
        planner.nodeForId("ha_m1")));
    assertTrue(planner.graph().hasEdgeConnecting(SINK, planner.nodeForId("expression")));
    
    QueryNodeConfig b1 = planner.configNodeForId("expression");
    assertEquals("BinaryExpression", b1.getType());
    
    ExpressionParseNode p1 = (ExpressionParseNode) b1;
    assertEquals("expression", p1.getId());
    assertEquals("sys.cpu.user", p1.getLeft());
    assertEquals(new DefaultQueryResultId("downsample", "m1"), p1.getLeftId());
    assertEquals(OperandType.VARIABLE, p1.getLeftType());
    assertEquals(1024, ((NumericLiteral) p1.getRight()).longValue());
    assertNull(p1.getRightId());
    assertEquals(OperandType.LITERAL_NUMERIC, p1.getRightType());
    assertEquals(ExpressionOp.MULTIPLY, p1.getOperator());
    assertEquals(1, p1.resultIds().size());
    assertEquals(new DefaultQueryResultId("expression", "expression"), 
        p1.resultIds().get(0));
  }
  
  @Test
  public void setupGraphNestedExpression() throws Exception {
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
              .setMetric("sys.cpu.busy")
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
        ExpressionConfig.newBuilder()
          .setExpression("m1 + m2 / 100")
          .setJoinConfig((JoinConfig) JoinConfig.newBuilder()
              .setJoinType(JoinType.NATURAL)
              .build())
          .addInterpolatorConfig(NUMERIC_CONFIG)
          .setId("expression")
          .addSource("ds1")
          .addSource("ds2")
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
    
    assertSame(STORE_NODES.get(0), planner.sources().get(0));
    assertEquals(7, planner.graph().nodes().size());
    assertEquals(1, planner.serializationSources().size());
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("expression"), 
        planner.nodeForId("expression_SubExp#0")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("expression"), 
        planner.nodeForId("ds1")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("ds1"), 
        planner.nodeForId("m1")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("ds2"), 
        planner.nodeForId("m2")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("expression_SubExp#0"), 
        planner.nodeForId("ds2")));
    assertTrue(planner.graph().hasEdgeConnecting(SINK, planner.nodeForId("expression")));
    
    QueryNodeConfig b1 = planner.configNodeForId("expression");
    assertEquals("BinaryExpression", b1.getType());
    
    ExpressionParseNode p1 = (ExpressionParseNode) b1;
    assertEquals("expression", p1.getId());
    assertEquals("sys.cpu.user", p1.getLeft());
    assertEquals(new DefaultQueryResultId("ds1", "m1"), p1.getLeftId());
    assertEquals(OperandType.VARIABLE, p1.getLeftType());
    assertEquals("expression_SubExp#0", p1.getRight());
    assertEquals(new DefaultQueryResultId("expression_SubExp#0", "expression_SubExp#0"), 
        p1.getRightId());
    assertEquals(OperandType.SUB_EXP, p1.getRightType());
    assertEquals(ExpressionOp.ADD, p1.getOperator());
    assertEquals(1, p1.resultIds().size());
    assertEquals(new DefaultQueryResultId("expression", "expression"), 
        p1.resultIds().get(0));
  }
  
  @Test
  public void setupGraph1MetricSetsInfectiousNan() throws Exception {
    List<QueryNodeConfig> graph = Lists.newArrayList(
        DefaultTimeSeriesDataSourceConfig.newBuilder()
          .setMetric(MetricLiteralFilter.newBuilder()
              .setMetric("sys.cpu.user")
              .build())
          .setFilterId("f1")
          .setId("m1")
          .build(),
        ExpressionConfig.newBuilder()
          .setExpression("m1 + 42")
          .setJoinConfig((JoinConfig) JoinConfig.newBuilder()
              .setJoinType(JoinType.NATURAL)
              .build())
          .addInterpolatorConfig(NUMERIC_CONFIG)
          .setId("expression")
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
    
    QueryNodeConfig b1 = planner.configNodeForId("expression");
    assertEquals("BinaryExpression", b1.getType());
    
    ExpressionParseNode p1 = (ExpressionParseNode) b1;
    assertEquals("expression", p1.getId());
    assertEquals("sys.cpu.user", p1.getLeft());
    assertEquals(new DefaultQueryResultId("m1", "m1"), p1.getLeftId());
    assertEquals(OperandType.VARIABLE, p1.getLeftType());
    assertEquals(42, ((NumericLiteral) p1.getRight()).longValue());
    assertEquals(OperandType.LITERAL_NUMERIC, p1.getRightType());
    assertNull(p1.getRightId());
    assertEquals(ExpressionOp.ADD, p1.getOperator());
    // TODO - !!!!!!!! Figure this bit out
    //assertTrue(p1.getExpressionConfig().getInfectiousNan());
  }
  
  @Test
  public void setupGraph1MetricAlreadyInfectious() throws Exception {
    List<QueryNodeConfig> graph = Lists.newArrayList(
        DefaultTimeSeriesDataSourceConfig.newBuilder()
          .setMetric(MetricLiteralFilter.newBuilder()
              .setMetric("sys.cpu.user")
              .build())
          .setFilterId("f1")
          .setId("m1")
          .build(),
        ExpressionConfig.newBuilder()
          .setExpression("m1 + 42")
          .setJoinConfig((JoinConfig) JoinConfig.newBuilder()
              .setJoinType(JoinType.NATURAL)
              .build())
          .setInfectiousNan(true)
          .addInterpolatorConfig(NUMERIC_CONFIG)
          .setId("expression")
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
    
    QueryNodeConfig b1 = planner.configNodeForId("expression");
    assertEquals("BinaryExpression", b1.getType());
    
    ExpressionParseNode p1 = (ExpressionParseNode) b1;
    assertEquals("expression", p1.getId());
    assertEquals("sys.cpu.user", p1.getLeft());
    assertEquals(new DefaultQueryResultId("m1", "m1"), p1.getLeftId());
    assertEquals(OperandType.VARIABLE, p1.getLeftType());
    assertEquals(42, ((NumericLiteral) p1.getRight()).longValue());
    assertEquals(OperandType.LITERAL_NUMERIC, p1.getRightType());
    assertNull(p1.getRightId());
    assertEquals(ExpressionOp.ADD, p1.getOperator());
  }
  // TODO - this requires more work to function correctly. We need the expression
  // node to take ANY metric from `m1` instead of a single metric.
//  @Test
//  public void setupGraphThroughJoinNodeId() throws Exception {
//    ExpressionFactory factory = new ExpressionFactory();
//    MutableGraph<QueryNodeConfig> graph = GraphBuilder.directed()
//        .allowsSelfLoops(false).build();
//    
//    QueryNodeConfig m1 = DefaultTimeSeriesDataSourceConfig.newBuilder()
//        .setMetric(MetricLiteralFilter.newBuilder()
//            .setMetric("sys.cpu.user")
//            .build())
//        .setFilterId("f1")
//        .setId("ha_m1")
//        .build();
//    QueryNodeConfig merger = MergerConfig.newBuilder()
//        .setAggregator("sum")
//        .addInterpolatorConfig(NUMERIC_CONFIG)
//        .setId("m1")
//        .build();
//    QueryNodeConfig ds = DownsampleConfig.newBuilder()
//        .setAggregator("sum")
//        .setInterval("1m")
//        .addInterpolatorConfig(NUMERIC_CONFIG)
//        .setId("downsample")
//        .build();
//    QueryNodeConfig exp = ExpressionConfig.newBuilder()
//        .setExpression("(m1 * 1024)")
//        .setJoinConfig((JoinConfig) JoinConfig.newBuilder()
//            .setJoinType(JoinType.NATURAL)
//            .build())
//        .addInterpolatorConfig(NUMERIC_CONFIG)
//        .setId("expression")
//        .build();
//    
//    QueryPlanner plan = mock(QueryPlanner.class);
//    when(plan.configGraph()).thenReturn(graph);
//    
//    graph.putEdge(merger, m1);
//    graph.putEdge(ds, merger);
//    graph.putEdge(exp, ds);
//    graph.putEdge(SINK, exp);
//    
//    factory.setupGraph(mock(QueryPipelineContext.class), exp, plan);
//    assertEquals(5, graph.nodes().size());
//    assertTrue(graph.nodes().contains(m1));
//    assertTrue(graph.nodes().contains(merger));
//    assertTrue(graph.nodes().contains(ds));
//    assertFalse(graph.nodes().contains(exp));
//    assertTrue(graph.nodes().contains(SINK));
//    
//    assertTrue(graph.hasEdgeConnecting(merger, m1));
//    
//    List<QueryNodeConfig> expressions = Lists.newArrayList(graph.predecessors(ds));
//    assertEquals(1, expressions.size());
//    ExpressionParseNode p1 = (ExpressionParseNode) expressions.get(0);
//    assertTrue(graph.hasEdgeConnecting(p1, ds));
//    assertEquals("expression", p1.getId());
//    assertEquals("sys.cpu.user", p1.getLeft());
//    assertEquals("m1", p1.getLeftId());
//    assertEquals(OperandType.VARIABLE, p1.getLeftType());
//    assertEquals(1024, ((NumericLiteral) p1.getRight()).longValue());
//    assertNull(p1.getRightId());
//    assertEquals(OperandType.LITERAL_NUMERIC, p1.getRightType());
//    assertEquals(ExpressionOp.MULTIPLY, p1.getOperator());
//    
//    assertTrue(graph.hasEdgeConnecting(SINK, p1));
//    assertEquals(1, graph.predecessors(p1).size());
//  }
}
