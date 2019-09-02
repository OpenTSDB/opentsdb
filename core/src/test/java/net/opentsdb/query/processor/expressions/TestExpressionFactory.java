//This file is part of OpenTSDB.
//Copyright (C) 2018  The OpenTSDB Authors.
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import net.opentsdb.query.DefaultTimeSeriesDataSourceConfig;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.graph.GraphBuilder;
import com.google.common.graph.MutableGraph;

import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.joins.JoinConfig;
import net.opentsdb.query.joins.JoinConfig.JoinType;
import net.opentsdb.query.plan.QueryPlanner;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.query.processor.expressions.ExpressionParseNode.ExpressionOp;
import net.opentsdb.query.processor.expressions.ExpressionParseNode.OperandType;
import net.opentsdb.query.processor.expressions.ExpressionParser.NumericLiteral;
import net.opentsdb.query.processor.merge.MergerConfig;

public class TestExpressionFactory {

  protected static NumericInterpolatorConfig NUMERIC_CONFIG;
  protected static JoinConfig JOIN_CONFIG;
  protected static QueryNodeConfig SINK;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    NUMERIC_CONFIG = 
        (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
      .setFillPolicy(FillPolicy.NOT_A_NUMBER)
      .setRealFillPolicy(FillWithRealPolicy.NONE)
      .setDataType(NumericType.TYPE.toString())
      .build();
    
    JOIN_CONFIG = (JoinConfig) JoinConfig.newBuilder()
        .setJoinType(JoinType.NATURAL)
        .build();
    
    SINK = mock(QueryNodeConfig.class);
  }
  
  @Test
  public void ctor() throws Exception {
    ExpressionFactory factory = new ExpressionFactory();
    assertEquals(0, factory.types().size());
  }
  
  @Test
  public void setupGraph1MetricDirect() throws Exception {
    ExpressionFactory factory = new ExpressionFactory();
    MutableGraph<QueryNodeConfig> graph = GraphBuilder.directed()
        .allowsSelfLoops(false).build();
    
    List<QueryNodeConfig> query = Lists.newArrayList(
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
    
    QueryPlanner plan = mock(QueryPlanner.class);
    when(plan.configGraph()).thenReturn(graph);
    
    graph.putEdge(query.get(1), query.get(0));
    graph.putEdge(SINK, query.get(1));
    
    factory.setupGraph(mock(QueryPipelineContext.class), (ExpressionConfig) query.get(1), plan);
    assertEquals(3, graph.nodes().size());
    assertTrue(graph.nodes().contains(query.get(0)));
    assertFalse(graph.nodes().contains(query.get(1)));
    assertTrue(graph.nodes().contains(SINK));
    
    QueryNodeConfig b1 = graph.predecessors(query.get(0)).iterator().next();
    assertEquals("BinaryExpression", b1.getType());
    assertTrue(graph.hasEdgeConnecting(b1, query.get(0)));
    assertTrue(graph.hasEdgeConnecting(SINK, b1));
    
    ExpressionParseNode p1 = (ExpressionParseNode) b1;
    assertEquals("expression", p1.getId());
    assertEquals("sys.cpu.user", p1.getLeft());
    assertEquals("m1", p1.getLeftId());
    assertEquals(OperandType.VARIABLE, p1.getLeftType());
    assertEquals(42, ((NumericLiteral) p1.getRight()).longValue());
    assertEquals(OperandType.LITERAL_NUMERIC, p1.getRightType());
    assertNull(p1.getRightId());
    assertEquals(ExpressionOp.ADD, p1.getOperator());
  }
  
  @Test
  public void setupGraph1MetricThroughNode() throws Exception {
    ExpressionFactory factory = new ExpressionFactory();
    MutableGraph<QueryNodeConfig> graph = GraphBuilder.directed()
        .allowsSelfLoops(false).build();
    
    QueryNodeConfig m1 = DefaultTimeSeriesDataSourceConfig.newBuilder()
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric("sys.cpu.user")
            .build())
        .setFilterId("f1")
        .setId("m1")
        .build();
    QueryNodeConfig ds = DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setInterval("1m")
        .addInterpolatorConfig(NUMERIC_CONFIG)
        .setId("downsample")
        .build();
    ExpressionConfig.Builder builder = ExpressionConfig.newBuilder();
    builder.setExpression("m1 + 42")
        .setJoinConfig((JoinConfig) JoinConfig.newBuilder()
            .setJoinType(JoinType.NATURAL)
            .build())
        .addInterpolatorConfig(NUMERIC_CONFIG);
    builder.setId("expression");
    ExpressionConfig exp = builder.build();
    QueryPlanner plan = mock(QueryPlanner.class);
    when(plan.configGraph()).thenReturn(graph);
    
    graph.putEdge(ds, m1);
    graph.putEdge(exp, ds);
    graph.putEdge(SINK, exp);
    
    factory.setupGraph(mock(QueryPipelineContext.class), exp, plan);
    assertEquals(4, graph.nodes().size());
    assertTrue(graph.nodes().contains(m1));
    assertTrue(graph.nodes().contains(ds));
    assertFalse(graph.nodes().contains(exp));
    assertTrue(graph.nodes().contains(SINK));
    
    QueryNodeConfig b1 = graph.predecessors(ds).iterator().next();
    assertEquals("BinaryExpression", b1.getType());
    assertTrue(graph.hasEdgeConnecting(ds, m1));
    assertTrue(graph.hasEdgeConnecting(b1, ds));
    assertTrue(graph.hasEdgeConnecting(SINK, b1));
    
    ExpressionParseNode p1 = (ExpressionParseNode) b1;
    assertEquals("expression", p1.getId());
    assertEquals("sys.cpu.user", p1.getLeft());
    assertEquals("m1", p1.getLeftId());
    assertEquals(OperandType.VARIABLE, p1.getLeftType());
    assertEquals(42, ((NumericLiteral) p1.getRight()).longValue());
    assertEquals(OperandType.LITERAL_NUMERIC, p1.getRightType());
    assertNull(p1.getRightId());
    assertEquals(ExpressionOp.ADD, p1.getOperator());
  }
  
  @Test
  public void setupGraph2MetricsThroughNode() throws Exception {
    ExpressionFactory factory = new ExpressionFactory();
    MutableGraph<QueryNodeConfig> graph = GraphBuilder.directed()
        .allowsSelfLoops(false).build();
    
    QueryNodeConfig m1 = DefaultTimeSeriesDataSourceConfig.newBuilder()
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric("sys.cpu.user")
            .build())
        .setFilterId("f1")
        .setId("m1")
        .build();
    QueryNodeConfig m2 = DefaultTimeSeriesDataSourceConfig.newBuilder()
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric("sys.cpu.sys")
            .build())
        .setFilterId("f1")
        .setId("m2")
        .build();
    QueryNodeConfig ds = DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setInterval("1m")
        .addInterpolatorConfig(NUMERIC_CONFIG)
        .setId("downsample")
        .build();
    ExpressionConfig.Builder builder = ExpressionConfig.newBuilder();
    builder.setExpression("m1 + m2")
        .setJoinConfig(JoinConfig.newBuilder()
            .setJoinType(JoinType.NATURAL)
            .build())
        .addInterpolatorConfig(NUMERIC_CONFIG);
    builder.setId("expression");
    ExpressionConfig exp = builder.build();
    QueryPlanner plan = mock(QueryPlanner.class);
    when(plan.configGraph()).thenReturn(graph);
    
    graph.putEdge(ds, m1);
    graph.putEdge(ds, m2);
    graph.putEdge(exp, ds);
    graph.putEdge(SINK, exp);
    
    factory.setupGraph(mock(QueryPipelineContext.class), exp, plan);
    assertEquals(5, graph.nodes().size());
    assertTrue(graph.nodes().contains(m1));
    assertTrue(graph.nodes().contains(m2));
    assertTrue(graph.nodes().contains(ds));
    assertFalse(graph.nodes().contains(exp));
    assertTrue(graph.nodes().contains(SINK));
    
    QueryNodeConfig b1 = graph.predecessors(ds).iterator().next();
    assertEquals("BinaryExpression", b1.getType());
    assertTrue(graph.hasEdgeConnecting(ds, m1));
    assertTrue(graph.hasEdgeConnecting(ds, m2));
    assertTrue(graph.hasEdgeConnecting(b1, ds));
    assertTrue(graph.hasEdgeConnecting(SINK, b1));
    
    ExpressionParseNode p1 = (ExpressionParseNode) b1;
    assertEquals("expression", p1.getId());
    assertEquals("sys.cpu.user", p1.getLeft());
    assertEquals("m1", p1.getLeftId());
    assertEquals(OperandType.VARIABLE, p1.getLeftType());
    assertEquals("sys.cpu.sys", p1.getRight());
    assertEquals("m2", p1.getRightId());
    assertEquals(OperandType.VARIABLE, p1.getRightType());
    assertEquals(ExpressionOp.ADD, p1.getOperator());
  }
  
  @Test
  public void setupGraph2Metrics1ThroughNode() throws Exception {
    ExpressionFactory factory = new ExpressionFactory();
    MutableGraph<QueryNodeConfig> graph = GraphBuilder.directed()
        .allowsSelfLoops(false).build();
    
    QueryNodeConfig m1 = DefaultTimeSeriesDataSourceConfig.newBuilder()
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric("sys.cpu.user")
            .build())
        .setFilterId("f1")
        .setId("m1")
        .build();
    QueryNodeConfig m2 = DefaultTimeSeriesDataSourceConfig.newBuilder()
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric("sys.cpu.sys")
            .build())
        .setFilterId("f1")
        .setId("m2")
        .build();
    QueryNodeConfig ds = DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setInterval("1m")
        .addInterpolatorConfig(NUMERIC_CONFIG)
        .setId("downsample")
        .build();
    ExpressionConfig.Builder builder = ExpressionConfig.newBuilder();
    builder.setExpression("m1 + m2")
        .setJoinConfig((JoinConfig) JoinConfig.newBuilder()
            .setJoinType(JoinType.NATURAL)
            .build())
        .addInterpolatorConfig(NUMERIC_CONFIG);
    builder.setId("expression");
    ExpressionConfig exp = builder.build();
    QueryPlanner plan = mock(QueryPlanner.class);
    when(plan.configGraph()).thenReturn(graph);
    
    graph.putEdge(ds, m1);
    graph.putEdge(exp, m2);
    graph.putEdge(exp, ds);
    graph.putEdge(SINK, exp);
    
    factory.setupGraph(mock(QueryPipelineContext.class), exp, plan);
    assertEquals(5, graph.nodes().size());
    assertTrue(graph.nodes().contains(m1));
    assertTrue(graph.nodes().contains(m2));
    assertTrue(graph.nodes().contains(ds));
    assertFalse(graph.nodes().contains(exp));
    assertTrue(graph.nodes().contains(SINK));
    
    QueryNodeConfig b1 = graph.predecessors(ds).iterator().next();
    assertEquals("BinaryExpression", b1.getType());
    assertTrue(graph.hasEdgeConnecting(ds, m1));
    assertTrue(graph.hasEdgeConnecting(b1, m2));
    assertTrue(graph.hasEdgeConnecting(b1, ds));
    assertTrue(graph.hasEdgeConnecting(SINK, b1));
    
    ExpressionParseNode p1 = (ExpressionParseNode) b1;
    assertEquals("expression", p1.getId());
    assertEquals("sys.cpu.user", p1.getLeft());
    assertEquals("m1", p1.getLeftId());
    assertEquals(OperandType.VARIABLE, p1.getLeftType());
    assertEquals("sys.cpu.sys", p1.getRight());
    assertEquals("m2", p1.getRightId());
    assertEquals(OperandType.VARIABLE, p1.getRightType());
    assertEquals(ExpressionOp.ADD, p1.getOperator());
  }
  
  @Test
  public void setupGraph3MetricsThroughNode() throws Exception {
    ExpressionFactory factory = new ExpressionFactory();
    MutableGraph<QueryNodeConfig> graph = GraphBuilder.directed()
        .allowsSelfLoops(false).build();
    
    QueryNodeConfig m1 = DefaultTimeSeriesDataSourceConfig.newBuilder()
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric("sys.cpu.user")
            .build())
        .setFilterId("f1")
        .setId("m1")
        .build();
    QueryNodeConfig m2 = DefaultTimeSeriesDataSourceConfig.newBuilder()
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric("sys.cpu.sys")
            .build())
        .setFilterId("f1")
        .setId("m2")
        .build();
    QueryNodeConfig m3 = DefaultTimeSeriesDataSourceConfig.newBuilder()
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric("sys.cpu.idle")
            .build())
        .setFilterId("f1")
        .setId("m3")
        .build();
    QueryNodeConfig ds = DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setInterval("1m")
        .addInterpolatorConfig(NUMERIC_CONFIG)
        .setId("downsample")
        .build();
    ExpressionConfig.Builder builder = ExpressionConfig.newBuilder();
    builder.setExpression("m1 + m2 + m3")
        .setJoinConfig((JoinConfig) JoinConfig.newBuilder()
            .setJoinType(JoinType.NATURAL)
            .build())
        .addInterpolatorConfig(NUMERIC_CONFIG);
    builder.setId("expression");
    ExpressionConfig exp = builder.build();

    QueryPlanner plan = mock(QueryPlanner.class);
    when(plan.configGraph()).thenReturn(graph);
    
    graph.putEdge(ds, m1);
    graph.putEdge(ds, m2);
    graph.putEdge(ds, m3);
    graph.putEdge(exp, ds);
    graph.putEdge(SINK, exp);
    
    factory.setupGraph(mock(QueryPipelineContext.class), exp, plan);
    assertEquals(7, graph.nodes().size());
    assertTrue(graph.nodes().contains(m1));
    assertTrue(graph.nodes().contains(m2));
    assertTrue(graph.nodes().contains(m3));
    assertTrue(graph.nodes().contains(ds));
    assertFalse(graph.nodes().contains(exp));
    assertTrue(graph.nodes().contains(SINK));
    
    assertTrue(graph.hasEdgeConnecting(ds, m1));
    assertTrue(graph.hasEdgeConnecting(ds, m2));
    
    List<QueryNodeConfig> expressions = Lists.newArrayList(graph.predecessors(ds));
    assertEquals(2, expressions.size());
    for (final QueryNodeConfig binary : expressions) {
      assertTrue(graph.hasEdgeConnecting(binary, ds));
      assertEquals("BinaryExpression", binary.getType());
      
      if (binary.getId().equals("expression_SubExp#0")) {
        ExpressionParseNode p1 = (ExpressionParseNode) binary;
        assertEquals("expression_SubExp#0", p1.getId());
        assertEquals("sys.cpu.user", p1.getLeft());
        assertEquals("m1", p1.getLeftId());
        assertEquals(OperandType.VARIABLE, p1.getLeftType());
        assertEquals("sys.cpu.sys", p1.getRight());
        assertEquals("m2", p1.getRightId());
        assertEquals(OperandType.VARIABLE, p1.getRightType());
        assertEquals(ExpressionOp.ADD, p1.getOperator());
        
        assertFalse(graph.hasEdgeConnecting(SINK, binary));
      } else {
        assertEquals("expression", binary.getId());
        ExpressionParseNode p1 = (ExpressionParseNode) binary;
        assertEquals("expression", p1.getId());
        assertEquals("expression_SubExp#0", p1.getLeft());
        assertEquals("expression_SubExp#0", p1.getLeftId());
        assertEquals(OperandType.SUB_EXP, p1.getLeftType());
        assertEquals("sys.cpu.idle", p1.getRight());
        assertEquals("m3", p1.getRightId());
        assertEquals(OperandType.VARIABLE, p1.getRightType());
        assertEquals(ExpressionOp.ADD, p1.getOperator());
        
        assertTrue(graph.hasEdgeConnecting(SINK, binary));
      }
    }
    
  }
  
  @Test
  public void setupGraph3MetricsComplexThroughNode() throws Exception {
    ExpressionFactory factory = new ExpressionFactory();
    MutableGraph<QueryNodeConfig> graph = GraphBuilder.directed()
        .allowsSelfLoops(false).build();
    
    QueryNodeConfig m1 = DefaultTimeSeriesDataSourceConfig.newBuilder()
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric("sys.cpu.user")
            .build())
        .setFilterId("f1")
        .setId("m1")
        .build();
    QueryNodeConfig m2 = DefaultTimeSeriesDataSourceConfig.newBuilder()
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric("sys.cpu.sys")
            .build())
        .setFilterId("f1")
        .setId("m2")
        .build();
    QueryNodeConfig m3 = DefaultTimeSeriesDataSourceConfig.newBuilder()
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric("sys.cpu.idle")
            .build())
        .setFilterId("f1")
        .setId("m3")
        .build();
    QueryNodeConfig ds = DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setInterval("1m")
        .addInterpolatorConfig(NUMERIC_CONFIG)
        .setId("downsample")
        .build();
    ExpressionConfig.Builder builder = ExpressionConfig.newBuilder();
    builder.setExpression("(m1 * 1024) + (m2 * 1024) + (m3 * 1024)")
        .setJoinConfig((JoinConfig) JoinConfig.newBuilder()
            .setJoinType(JoinType.NATURAL)
            .build())
        .addInterpolatorConfig(NUMERIC_CONFIG);
        builder.setId("expression");
    ExpressionConfig exp = builder.build();

    QueryPlanner plan = mock(QueryPlanner.class);
    when(plan.configGraph()).thenReturn(graph);
    
    graph.putEdge(ds, m1);
    graph.putEdge(ds, m2);
    graph.putEdge(ds, m3);
    graph.putEdge(exp, ds);
    graph.putEdge(SINK, exp);
    
    factory.setupGraph(mock(QueryPipelineContext.class), exp, plan);
    assertEquals(10, graph.nodes().size());
    assertTrue(graph.nodes().contains(m1));
    assertTrue(graph.nodes().contains(m2));
    assertTrue(graph.nodes().contains(m3));
    assertTrue(graph.nodes().contains(ds));
    assertFalse(graph.nodes().contains(exp));
    assertTrue(graph.nodes().contains(SINK));
    
    assertTrue(graph.hasEdgeConnecting(ds, m1));
    assertTrue(graph.hasEdgeConnecting(ds, m2));
    
    List<QueryNodeConfig> expressions = Lists.newArrayList(graph.predecessors(ds));
    assertEquals(3, expressions.size());
    for (final QueryNodeConfig binary : expressions) {
      assertTrue(graph.hasEdgeConnecting(binary, ds));
      assertEquals("BinaryExpression", binary.getType());
      if (binary.getId().equals("expression_SubExp#0")) {
        ExpressionParseNode p1 = (ExpressionParseNode) binary;
        assertEquals("expression_SubExp#0", p1.getId());
        assertEquals("sys.cpu.user", p1.getLeft());
        assertEquals("m1", p1.getLeftId());
        assertEquals(OperandType.VARIABLE, p1.getLeftType());
        assertEquals(1024, ((NumericLiteral) p1.getRight()).longValue());
        assertNull(p1.getRightId());
        assertEquals(OperandType.LITERAL_NUMERIC, p1.getRightType());
        assertEquals(ExpressionOp.MULTIPLY, p1.getOperator());
        
        assertFalse(graph.hasEdgeConnecting(SINK, binary));
        assertEquals(1, graph.predecessors(binary).size());
        assertEquals("expression_SubExp#2", 
            graph.predecessors(binary).iterator().next().getId());
      } else if (binary.getId().equals("expression_SubExp#1")) {
        ExpressionParseNode p1 = (ExpressionParseNode) binary;
        assertEquals("expression_SubExp#1", p1.getId());
        assertEquals("sys.cpu.sys", p1.getLeft());
        assertEquals("m2", p1.getLeftId());
        assertEquals(OperandType.VARIABLE, p1.getLeftType());
        assertEquals(1024, ((NumericLiteral) p1.getRight()).longValue());
        assertNull(p1.getRightId());
        assertEquals(OperandType.LITERAL_NUMERIC, p1.getRightType());
        assertEquals(ExpressionOp.MULTIPLY, p1.getOperator());
        
        assertFalse(graph.hasEdgeConnecting(SINK, binary));
        assertEquals(1, graph.predecessors(binary).size());
        QueryNodeConfig b2 = graph.predecessors(binary).iterator().next();
        assertEquals("expression_SubExp#2", b2.getId());
        
        // validate sub2 here
        p1 = (ExpressionParseNode) b2;
        assertEquals("expression_SubExp#2", p1.getId());
        assertEquals("expression_SubExp#0", p1.getLeft());
        assertEquals("expression_SubExp#0", p1.getLeftId());
        assertEquals(OperandType.SUB_EXP, p1.getLeftType());
        assertEquals("expression_SubExp#1", p1.getRight());
        assertEquals("expression_SubExp#1", p1.getRightId());
        assertEquals(OperandType.SUB_EXP, p1.getRightType());
        assertEquals(ExpressionOp.ADD, p1.getOperator());
        
        assertFalse(graph.hasEdgeConnecting(SINK, b2));
        
      } else if (binary.getId().equals("expression_SubExp#3")) {
        ExpressionParseNode p1 = (ExpressionParseNode) binary;
        assertEquals("expression_SubExp#3", p1.getId());
        assertEquals("sys.cpu.idle", p1.getLeft());
        assertEquals("m3", p1.getLeftId());
        assertEquals(OperandType.VARIABLE, p1.getLeftType());
        assertEquals(1024, ((NumericLiteral) p1.getRight()).longValue());
        assertNull(p1.getRightId());
        assertEquals(OperandType.LITERAL_NUMERIC, p1.getRightType());
        assertEquals(ExpressionOp.MULTIPLY, p1.getOperator());
        
        assertFalse(graph.hasEdgeConnecting(SINK, binary));
        assertEquals(1, graph.predecessors(binary).size());
        
        // validate the expression here
        QueryNodeConfig b2 = graph.predecessors(binary).iterator().next();
        assertEquals("expression", b2.getId());
        
        // validate parent here
        p1 = (ExpressionParseNode) b2;
        assertEquals("expression", p1.getId());
        assertEquals("expression_SubExp#2", p1.getLeft());
        assertEquals("expression_SubExp#2", p1.getLeftId());
        assertEquals(OperandType.SUB_EXP, p1.getLeftType());
        assertEquals("expression_SubExp#3", p1.getRight());
        assertEquals("expression_SubExp#3", p1.getRightId());
        assertEquals(OperandType.SUB_EXP, p1.getRightType());
        assertEquals(ExpressionOp.ADD, p1.getOperator());
        
        assertTrue(graph.hasEdgeConnecting(SINK, b2));
      }
    }
    
  }
  
  @Test
  public void setupGraphThroughJoinNodeMetricName() throws Exception {
    ExpressionFactory factory = new ExpressionFactory();
    MutableGraph<QueryNodeConfig> graph = GraphBuilder.directed()
        .allowsSelfLoops(false).build();
    
    QueryNodeConfig m1 = DefaultTimeSeriesDataSourceConfig.newBuilder()
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric("sys.cpu.user")
            .build())
        .setFilterId("f1")
        .setId("ha_m1")
        .build();
    QueryNodeConfig merger = MergerConfig.newBuilder()
        .setAggregator("sum")
        .addInterpolatorConfig(NUMERIC_CONFIG)
        .setDataSource("m1")
        .setId("m1")
        .build();
    QueryNodeConfig ds = DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setInterval("1m")
        .addInterpolatorConfig(NUMERIC_CONFIG)
        .setId("downsample")
        .build();
    ExpressionConfig.Builder builder = ExpressionConfig.newBuilder();
    builder.setExpression("(sys.cpu.user * 1024)")
        .setJoinConfig((JoinConfig) JoinConfig.newBuilder()
            .setJoinType(JoinType.NATURAL)
            .build())
        .addInterpolatorConfig(NUMERIC_CONFIG);
    builder.setId("expression");
    ExpressionConfig exp = builder.build();
    
    QueryPlanner plan = mock(QueryPlanner.class);
    when(plan.configGraph()).thenReturn(graph);
    
    graph.putEdge(merger, m1);
    graph.putEdge(ds, merger);
    graph.putEdge(exp, ds);
    graph.putEdge(SINK, exp);
    
    factory.setupGraph(mock(QueryPipelineContext.class), exp, plan);
    assertEquals(5, graph.nodes().size());
    assertTrue(graph.nodes().contains(m1));
    assertTrue(graph.nodes().contains(merger));
    assertTrue(graph.nodes().contains(ds));
    assertFalse(graph.nodes().contains(exp));
    assertTrue(graph.nodes().contains(SINK));
    
    assertTrue(graph.hasEdgeConnecting(merger, m1));
    
    List<QueryNodeConfig> expressions = Lists.newArrayList(graph.predecessors(ds));
    assertEquals(1, expressions.size());
    ExpressionParseNode p1 = (ExpressionParseNode) expressions.get(0);
    assertTrue(graph.hasEdgeConnecting(p1, ds));
    assertEquals("expression", p1.getId());
    assertEquals("sys.cpu.user", p1.getLeft());
    assertEquals("m1", p1.getLeftId());
    assertEquals(OperandType.VARIABLE, p1.getLeftType());
    assertEquals(1024, ((NumericLiteral) p1.getRight()).longValue());
    assertNull(p1.getRightId());
    assertEquals(OperandType.LITERAL_NUMERIC, p1.getRightType());
    assertEquals(ExpressionOp.MULTIPLY, p1.getOperator());
    
    assertTrue(graph.hasEdgeConnecting(SINK, p1));
    assertEquals(1, graph.predecessors(p1).size());
  }
  
  @Test
  public void setupGraphNestedExpression() throws Exception {
    ExpressionFactory factory = new ExpressionFactory();
    MutableGraph<QueryNodeConfig> graph = GraphBuilder.directed()
        .allowsSelfLoops(false).build();
    
    QueryNodeConfig m1 = DefaultTimeSeriesDataSourceConfig.newBuilder()
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric("sys.cpu.user")
            .build())
        .setFilterId("f1")
        .setId("m1")
        .build();
    QueryNodeConfig m2 = DefaultTimeSeriesDataSourceConfig.newBuilder()
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric("sys.cpu.busy")
            .build())
        .setFilterId("f1")
        .setId("m2")
        .build();
    QueryNodeConfig ds1 = DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setInterval("1m")
        .addInterpolatorConfig(NUMERIC_CONFIG)
        .setId("ds1")
        .build();
    QueryNodeConfig ds2 = DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setInterval("1m")
        .addInterpolatorConfig(NUMERIC_CONFIG)
        .setId("ds2")
        .build();
    ExpressionConfig.Builder builder = ExpressionConfig.newBuilder();
    builder.setExpression("m1 + m2 / 100")
        .setJoinConfig((JoinConfig) JoinConfig.newBuilder()
            .setJoinType(JoinType.NATURAL)
            .build())
        .addInterpolatorConfig(NUMERIC_CONFIG);
    builder.setId("expression");
    ExpressionConfig exp = builder.build();

    builder = ExpressionConfig.newBuilder();
    builder.setExpression("m1 / expression")
        .setJoinConfig((JoinConfig) JoinConfig.newBuilder()
            .setJoinType(JoinType.NATURAL)
            .build())
        .addInterpolatorConfig(NUMERIC_CONFIG);
    builder.setId("expression");
    QueryNodeConfig exp2 = builder.build();
    
    QueryPlanner plan = mock(QueryPlanner.class);
    when(plan.configGraph()).thenReturn(graph);
    
    graph.putEdge(ds1, m1);
    graph.putEdge(ds2, m2);
    graph.putEdge(exp, ds1);
    graph.putEdge(exp, ds2);
    graph.putEdge(exp2, exp);
    graph.putEdge(exp2, ds1);
    graph.putEdge(SINK, exp);
    
    factory.setupGraph(mock(QueryPipelineContext.class), exp, plan);
    assertEquals(8, graph.nodes().size());
    assertTrue(graph.nodes().contains(m1));
    assertTrue(graph.nodes().contains(ds1));
    assertFalse(graph.nodes().contains(exp));
    assertTrue(graph.nodes().contains(SINK));
    
    QueryNodeConfig b1 = graph.predecessors(ds1).iterator().next();
    assertEquals("Expression", b1.getType());
    assertTrue(graph.hasEdgeConnecting(ds1, m1));
    assertTrue(graph.hasEdgeConnecting(b1, ds1));
    assertFalse(graph.hasEdgeConnecting(SINK, b1));
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
