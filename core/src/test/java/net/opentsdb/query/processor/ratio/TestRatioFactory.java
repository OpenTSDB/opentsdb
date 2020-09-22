// This file is part of OpenTSDB.
// Copyright (C) 2020  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.ratio;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

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
import net.opentsdb.query.BaseTimeSeriesDataSourceConfig;
import net.opentsdb.query.DefaultQueryResultId;
import net.opentsdb.query.DefaultTimeSeriesDataSourceConfig;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryMode;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.joins.JoinConfig;
import net.opentsdb.query.joins.JoinConfig.JoinType;
import net.opentsdb.query.plan.DefaultQueryPlanner;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.processor.expressions.ExpressionParseNode;
import net.opentsdb.query.processor.expressions.ExpressionParseNode.ExpressionOp;
import net.opentsdb.query.processor.expressions.ExpressionParseNode.OperandType;
import net.opentsdb.query.processor.expressions.ExpressionParser.NumericLiteral;
import net.opentsdb.query.processor.groupby.GroupByConfig;

public class TestRatioFactory {

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
    RatioFactory factory = new RatioFactory();
    assertNull(factory.initialize(TSDB, null).join());
    assertEquals(0, factory.types().size());
  }
  
  @Test
  public void setupGraph1Source() throws Exception {
    List<QueryNodeConfig> graph = Lists.newArrayList(
        DefaultTimeSeriesDataSourceConfig.newBuilder()
          .setMetric(MetricLiteralFilter.newBuilder()
              .setMetric("sys.cpu.user")
              .build())
          .setFilterId("f1")
          .setId("m1")
          .build(),
        RatioConfig.newBuilder()
          .setAs("myratio")
          .addDataSource("m1")
          .addSource("m1")
          .addInterpolatorConfig(NUMERIC_CONFIG)
          .setId("ratio")
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
    assertEquals(4, planner.configGraph().nodes().size());
    assertEquals(1, planner.serializationSources().size());
    assertTrue(planner.graph().hasEdgeConnecting(SINK, 
        planner.nodeForId("ratio_exp_m1")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("ratio_exp_m1"), 
        planner.nodeForId("ratio_gb_m1")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("ratio_gb_m1"), 
        planner.nodeForId("m1")));
    
    ExpressionParseNode exp = (ExpressionParseNode) planner.configNodeForId("ratio_exp_m1");
    assertEquals(ExpressionOp.DIVIDE, exp.getOperator());
    assertEquals("sys.cpu.user", exp.getLeft());
    assertEquals(new DefaultQueryResultId("m1", "m1"), exp.getLeftId());
    assertEquals(OperandType.VARIABLE, exp.getLeftType());
    assertEquals("sys.cpu.user", exp.getRight());
    assertEquals(new DefaultQueryResultId("ratio_gb_m1", "m1"), exp.getRightId());
    assertEquals(OperandType.VARIABLE, exp.getRightType());
    
    GroupByConfig gb = (GroupByConfig) planner.configNodeForId("ratio_gb_m1");
    assertTrue(gb.getTagKeys().isEmpty());
    assertEquals("sum", gb.getAggregator());
  }
  
  @Test
  public void setupGraph1SourceAsPercent() throws Exception {
    List<QueryNodeConfig> graph = Lists.newArrayList(
        DefaultTimeSeriesDataSourceConfig.newBuilder()
          .setMetric(MetricLiteralFilter.newBuilder()
              .setMetric("sys.cpu.user")
              .build())
          .setFilterId("f1")
          .setId("m1")
          .build(),
        RatioConfig.newBuilder()
          .setAs("myratio")
          .addDataSource("m1")
          .setAsPercent(true)
          .addSource("m1")
          .addInterpolatorConfig(NUMERIC_CONFIG)
          .setId("ratio")
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
    assertEquals(5, planner.configGraph().nodes().size());
    assertEquals(1, planner.serializationSources().size());
    assertTrue(planner.graph().hasEdgeConnecting(SINK, 
        planner.nodeForId("ratio_exp_m1")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("ratio_exp_m1"), 
        planner.nodeForId("ratio_subexp_m1")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("ratio_subexp_m1"), 
        planner.nodeForId("ratio_gb_m1")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("ratio_gb_m1"), 
        planner.nodeForId("m1")));
    
    ExpressionParseNode exp = (ExpressionParseNode) planner.configNodeForId("ratio_subexp_m1");
    assertEquals(ExpressionOp.DIVIDE, exp.getOperator());
    assertEquals("sys.cpu.user", exp.getLeft());
    assertEquals(new DefaultQueryResultId("m1", "m1"), exp.getLeftId());
    assertEquals(OperandType.VARIABLE, exp.getLeftType());
    assertEquals("sys.cpu.user", exp.getRight());
    assertEquals(new DefaultQueryResultId("ratio_gb_m1", "m1"), exp.getRightId());
    assertEquals(OperandType.VARIABLE, exp.getRightType());
    
    exp = (ExpressionParseNode) planner.configNodeForId("ratio_exp_m1");
    assertEquals(ExpressionOp.MULTIPLY, exp.getOperator());
    assertEquals("myratio", exp.getLeft());
    assertEquals(new DefaultQueryResultId("ratio_subexp_m1", "m1"), exp.getLeftId());
    assertEquals(OperandType.SUB_EXP, exp.getLeftType());
    assertEquals(new NumericLiteral(100), exp.getRight());
    assertNull(exp.getRightId());
    assertEquals(OperandType.LITERAL_NUMERIC, exp.getRightType());
    
    GroupByConfig gb = (GroupByConfig) planner.configNodeForId("ratio_gb_m1");
    assertTrue(gb.getTagKeys().isEmpty());
    assertEquals("sum", gb.getAggregator());
  }

  @Test
  public void setupGraph2Sources() throws Exception {
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
          .setSourceId("s2")
          .setId("m2")
          .build(),
        RatioConfig.newBuilder()
          .setAs("myratio")
          .addDataSource("m1")
          .addDataSource("m2")
          .addSource("m1")
          .addSource("m2")
          .addInterpolatorConfig(NUMERIC_CONFIG)
          .setId("ratio")
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
    assertEquals(7, planner.configGraph().nodes().size());
    assertEquals(2, planner.serializationSources().size());
    assertTrue(planner.graph().hasEdgeConnecting(SINK, 
        planner.nodeForId("ratio_exp_m1")));
    assertTrue(planner.graph().hasEdgeConnecting(SINK, 
        planner.nodeForId("ratio_exp_m2")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("ratio_exp_m1"), 
        planner.nodeForId("ratio_gb_m1")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("ratio_exp_m2"), 
        planner.nodeForId("ratio_gb_m2")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("ratio_gb_m1"), 
        planner.nodeForId("m1")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("ratio_gb_m2"), 
        planner.nodeForId("m2")));
    
    ExpressionParseNode exp = (ExpressionParseNode) planner.configNodeForId("ratio_exp_m1");
    assertEquals(ExpressionOp.DIVIDE, exp.getOperator());
    assertEquals("sys.cpu.user", exp.getLeft());
    assertEquals(new DefaultQueryResultId("m1", "m1"), exp.getLeftId());
    assertEquals(OperandType.VARIABLE, exp.getLeftType());
    assertEquals("sys.cpu.user", exp.getRight());
    assertEquals(new DefaultQueryResultId("ratio_gb_m1", "m1"), exp.getRightId());
    assertEquals(OperandType.VARIABLE, exp.getRightType());
    
    exp = (ExpressionParseNode) planner.configNodeForId("ratio_exp_m2");
    assertEquals(ExpressionOp.DIVIDE, exp.getOperator());
    assertEquals("sys.cpu.busy", exp.getLeft());
    assertEquals(new DefaultQueryResultId("m2", "m2"), exp.getLeftId());
    assertEquals(OperandType.VARIABLE, exp.getLeftType());
    assertEquals("sys.cpu.busy", exp.getRight());
    assertEquals(new DefaultQueryResultId("ratio_gb_m2", "m2"), exp.getRightId());
    assertEquals(OperandType.VARIABLE, exp.getRightType());
    
    GroupByConfig gb = (GroupByConfig) planner.configNodeForId("ratio_gb_m1");
    assertTrue(gb.getTagKeys().isEmpty());
    assertEquals("sum", gb.getAggregator());
    
    gb = (GroupByConfig) planner.configNodeForId("ratio_gb_m2");
    assertTrue(gb.getTagKeys().isEmpty());
    assertEquals("sum", gb.getAggregator());
  }
  
  @Test
  public void missingSource() throws Exception {
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
          .setSourceId("s2")
          .setId("m2")
          .build(),
        RatioConfig.newBuilder()
          .setAs("myratio")
          .addDataSource("m1")
          .addDataSource("m2")
          .addSource("m1")
          //.addSource("m2") // doh!
          .addInterpolatorConfig(NUMERIC_CONFIG)
          .setId("ratio")
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
    try {
      planner.plan(null).join();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
}
