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
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import org.jgrapht.experimental.dag.DirectedAcyclicGraph;
import org.jgrapht.graph.DefaultEdge;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;

import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QuerySourceConfig;
import net.opentsdb.query.TimeSeriesQuery;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
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
    DirectedAcyclicGraph<QueryNodeConfig, DefaultEdge> graph =
        new DirectedAcyclicGraph<QueryNodeConfig, DefaultEdge>(DefaultEdge.class);
    
    List<QueryNodeConfig> query = Lists.newArrayList(
        QuerySourceConfig.newBuilder()
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
    
    graph.addVertex(SINK);
    graph.addVertex(query.get(0));
    graph.addVertex(query.get(1));
    graph.addDagEdge(query.get(1), query.get(0));
    graph.addDagEdge(SINK, query.get(1));
    
    factory.setupGraph(mock(TimeSeriesQuery.class), query.get(1), plan);
    assertEquals(3, graph.vertexSet().size());
    assertTrue(graph.containsVertex(query.get(0)));
    assertFalse(graph.containsVertex(query.get(1)));
    assertTrue(graph.containsVertex(SINK));
    
    QueryNodeConfig b1 = graph.getEdgeSource(
        graph.incomingEdgesOf(query.get(0)).iterator().next());
    assertEquals("BinaryExpression", b1.getType());
    assertTrue(graph.containsEdge(b1, query.get(0)));
    assertTrue(graph.containsEdge(SINK, b1));
    
    ExpressionParseNode p1 = (ExpressionParseNode) b1;
    assertEquals("expression", p1.getId());
    assertEquals("sys.cpu.user", p1.getLeft());
    assertEquals(OperandType.VARIABLE, p1.getLeftType());
    assertEquals(42, ((NumericLiteral) p1.getRight()).longValue());
    assertEquals(OperandType.LITERAL_NUMERIC, p1.getRightType());
    assertEquals(ExpressionOp.ADD, p1.getOperator());
  }
  
  @Test
  public void setupGraph1MetricThroughNode() throws Exception {
    ExpressionFactory factory = new ExpressionFactory();
    DirectedAcyclicGraph<QueryNodeConfig, DefaultEdge> graph =
        new DirectedAcyclicGraph<QueryNodeConfig, DefaultEdge>(DefaultEdge.class);
    
    QueryNodeConfig m1 = QuerySourceConfig.newBuilder()
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
    QueryNodeConfig exp = ExpressionConfig.newBuilder()
        .setExpression("m1 + 42")
        .setJoinConfig((JoinConfig) JoinConfig.newBuilder()
            .setJoinType(JoinType.NATURAL)
            .build())
        .addInterpolatorConfig(NUMERIC_CONFIG)
        .setId("expression")
        .build();
    
    QueryPlanner plan = mock(QueryPlanner.class);
    when(plan.configGraph()).thenReturn(graph);
    
    graph.addVertex(SINK);
    graph.addVertex(m1);
    graph.addVertex(ds);
    graph.addVertex(exp);
    graph.addDagEdge(ds, m1);
    graph.addDagEdge(exp, ds);
    graph.addDagEdge(SINK, exp);
    
    factory.setupGraph(mock(TimeSeriesQuery.class), exp, plan);
    assertEquals(4, graph.vertexSet().size());
    assertTrue(graph.containsVertex(m1));
    assertTrue(graph.containsVertex(ds));
    assertFalse(graph.containsVertex(exp));
    assertTrue(graph.containsVertex(SINK));
    
    QueryNodeConfig b1 = graph.getEdgeSource(
        graph.incomingEdgesOf(ds).iterator().next());
    assertEquals("BinaryExpression", b1.getType());
    assertTrue(graph.containsEdge(ds, m1));
    assertTrue(graph.containsEdge(b1, ds));
    assertTrue(graph.containsEdge(SINK, b1));
    
    ExpressionParseNode p1 = (ExpressionParseNode) b1;
    assertEquals("expression", p1.getId());
    assertEquals("sys.cpu.user", p1.getLeft());
    assertEquals(OperandType.VARIABLE, p1.getLeftType());
    assertEquals(42, ((NumericLiteral) p1.getRight()).longValue());
    assertEquals(OperandType.LITERAL_NUMERIC, p1.getRightType());
    assertEquals(ExpressionOp.ADD, p1.getOperator());
  }
  
  @Test
  public void setupGraph2MetricsThroughNode() throws Exception {
    ExpressionFactory factory = new ExpressionFactory();
    
    DirectedAcyclicGraph<QueryNodeConfig, DefaultEdge> graph =
        new DirectedAcyclicGraph<QueryNodeConfig, DefaultEdge>(DefaultEdge.class);
    
    QueryNodeConfig m1 = QuerySourceConfig.newBuilder()
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric("sys.cpu.user")
            .build())
        .setFilterId("f1")
        .setId("m1")
        .build();
    QueryNodeConfig m2 = QuerySourceConfig.newBuilder()
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
    QueryNodeConfig exp = ExpressionConfig.newBuilder()
        .setExpression("m1 + m2")
        .setJoinConfig((JoinConfig) JoinConfig.newBuilder()
            .setJoinType(JoinType.NATURAL)
            .build())
        .addInterpolatorConfig(NUMERIC_CONFIG)
        .setId("expression")
        .build();
    
    QueryPlanner plan = mock(QueryPlanner.class);
    when(plan.configGraph()).thenReturn(graph);
    
    graph.addVertex(SINK);
    graph.addVertex(m1);
    graph.addVertex(m2);
    graph.addVertex(ds);
    graph.addVertex(exp);
    graph.addDagEdge(ds, m1);
    graph.addDagEdge(ds, m2);
    graph.addDagEdge(exp, ds);
    graph.addDagEdge(SINK, exp);
    
    factory.setupGraph(mock(TimeSeriesQuery.class), exp, plan);
    assertEquals(5, graph.vertexSet().size());
    assertTrue(graph.containsVertex(m1));
    assertTrue(graph.containsVertex(m2));
    assertTrue(graph.containsVertex(ds));
    assertFalse(graph.containsVertex(exp));
    assertTrue(graph.containsVertex(SINK));
    
    QueryNodeConfig b1 = graph.getEdgeSource(
        graph.incomingEdgesOf(ds).iterator().next());
    assertEquals("BinaryExpression", b1.getType());
    assertTrue(graph.containsEdge(ds, m1));
    assertTrue(graph.containsEdge(ds, m2));
    assertTrue(graph.containsEdge(b1, ds));
    assertTrue(graph.containsEdge(SINK, b1));
    
    ExpressionParseNode p1 = (ExpressionParseNode) b1;
    assertEquals("expression", p1.getId());
    assertEquals("sys.cpu.user", p1.getLeft());
    assertEquals(OperandType.VARIABLE, p1.getLeftType());
    assertEquals("sys.cpu.sys", p1.getRight());
    assertEquals(OperandType.VARIABLE, p1.getRightType());
    assertEquals(ExpressionOp.ADD, p1.getOperator());
  }
  
  @Test
  public void setupGraph2Metrics1ThroughNode() throws Exception {
    ExpressionFactory factory = new ExpressionFactory();
    
    DirectedAcyclicGraph<QueryNodeConfig, DefaultEdge> graph =
        new DirectedAcyclicGraph<QueryNodeConfig, DefaultEdge>(DefaultEdge.class);
    
    QueryNodeConfig m1 = QuerySourceConfig.newBuilder()
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric("sys.cpu.user")
            .build())
        .setFilterId("f1")
        .setId("m1")
        .build();
    QueryNodeConfig m2 = QuerySourceConfig.newBuilder()
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
    QueryNodeConfig exp = ExpressionConfig.newBuilder()
        .setExpression("m1 + m2")
        .setJoinConfig((JoinConfig) JoinConfig.newBuilder()
            .setJoinType(JoinType.NATURAL)
            .build())
        .addInterpolatorConfig(NUMERIC_CONFIG)
        .setId("expression")
        .build();
    
    QueryPlanner plan = mock(QueryPlanner.class);
    when(plan.configGraph()).thenReturn(graph);
    
    graph.addVertex(SINK);
    graph.addVertex(m1);
    graph.addVertex(m2);
    graph.addVertex(ds);
    graph.addVertex(exp);
    graph.addDagEdge(ds, m1);
    graph.addDagEdge(exp, m2);
    graph.addDagEdge(exp, ds);
    graph.addDagEdge(SINK, exp);
    
    factory.setupGraph(mock(TimeSeriesQuery.class), exp, plan);
    assertEquals(5, graph.vertexSet().size());
    assertTrue(graph.containsVertex(m1));
    assertTrue(graph.containsVertex(m2));
    assertTrue(graph.containsVertex(ds));
    assertFalse(graph.containsVertex(exp));
    assertTrue(graph.containsVertex(SINK));
    
    QueryNodeConfig b1 = graph.getEdgeSource(
        graph.incomingEdgesOf(ds).iterator().next());
    assertEquals("BinaryExpression", b1.getType());
    assertTrue(graph.containsEdge(ds, m1));
    assertTrue(graph.containsEdge(b1, m2));
    assertTrue(graph.containsEdge(b1, ds));
    assertTrue(graph.containsEdge(SINK, b1));
    
    ExpressionParseNode p1 = (ExpressionParseNode) b1;
    assertEquals("expression", p1.getId());
    assertEquals("sys.cpu.user", p1.getLeft());
    assertEquals(OperandType.VARIABLE, p1.getLeftType());
    assertEquals("sys.cpu.sys", p1.getRight());
    assertEquals(OperandType.VARIABLE, p1.getRightType());
    assertEquals(ExpressionOp.ADD, p1.getOperator());
  }
  
  @Test
  public void setupGraph3MetricsThroughNode() throws Exception {
    ExpressionFactory factory = new ExpressionFactory();
    DirectedAcyclicGraph<QueryNodeConfig, DefaultEdge> graph =
        new DirectedAcyclicGraph<QueryNodeConfig, DefaultEdge>(DefaultEdge.class);
    
    QueryNodeConfig m1 = QuerySourceConfig.newBuilder()
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric("sys.cpu.user")
            .build())
        .setFilterId("f1")
        .setId("m1")
        .build();
    QueryNodeConfig m2 = QuerySourceConfig.newBuilder()
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric("sys.cpu.sys")
            .build())
        .setFilterId("f1")
        .setId("m2")
        .build();
    QueryNodeConfig m3 = QuerySourceConfig.newBuilder()
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
    QueryNodeConfig exp =ExpressionConfig.newBuilder()
        .setExpression("m1 + m2 + m3")
        .setJoinConfig((JoinConfig) JoinConfig.newBuilder()
            .setJoinType(JoinType.NATURAL)
            .build())
        .addInterpolatorConfig(NUMERIC_CONFIG)
        .setId("expression")
        .build();
    
    QueryPlanner plan = mock(QueryPlanner.class);
    when(plan.configGraph()).thenReturn(graph);
    
    graph.addVertex(SINK);
    graph.addVertex(m1);
    graph.addVertex(m2);
    graph.addVertex(m3);
    graph.addVertex(ds);
    graph.addVertex(exp);
    graph.addDagEdge(ds, m1);
    graph.addDagEdge(ds, m2);
    graph.addDagEdge(ds, m3);
    graph.addDagEdge(exp, ds);
    graph.addDagEdge(SINK, exp);
    
    factory.setupGraph(mock(TimeSeriesQuery.class), exp, plan);
    assertEquals(7, graph.vertexSet().size());
    assertTrue(graph.containsVertex(m1));
    assertTrue(graph.containsVertex(m2));
    assertTrue(graph.containsVertex(m3));
    assertTrue(graph.containsVertex(ds));
    assertFalse(graph.containsVertex(exp));
    assertTrue(graph.containsVertex(SINK));
    
    assertTrue(graph.containsEdge(ds, m1));
    assertTrue(graph.containsEdge(ds, m2));
    
    List<QueryNodeConfig> expressions = Lists.newArrayListWithCapacity(2);
    for (final DefaultEdge edge : graph.incomingEdgesOf(ds)) {
      expressions.add(graph.getEdgeSource(edge));
    }
    assertEquals(2, expressions.size());
    for (final QueryNodeConfig binary : expressions) {
      assertTrue(graph.containsEdge(binary, ds));
      assertEquals("BinaryExpression", binary.getType());
      
      if (binary.getId().equals("expression_SubExp#0")) {
        ExpressionParseNode p1 = (ExpressionParseNode) binary;
        assertEquals("expression_SubExp#0", p1.getId());
        assertEquals("sys.cpu.user", p1.getLeft());
        assertEquals(OperandType.VARIABLE, p1.getLeftType());
        assertEquals("sys.cpu.sys", p1.getRight());
        assertEquals(OperandType.VARIABLE, p1.getRightType());
        assertEquals(ExpressionOp.ADD, p1.getOperator());
        
        assertFalse(graph.containsEdge(SINK, binary));
      } else {
        assertEquals("expression", binary.getId());
        ExpressionParseNode p1 = (ExpressionParseNode) binary;
        assertEquals("expression", p1.getId());
        assertEquals("expression_SubExp#0", p1.getLeft());
        assertEquals(OperandType.SUB_EXP, p1.getLeftType());
        assertEquals("sys.cpu.idle", p1.getRight());
        assertEquals(OperandType.VARIABLE, p1.getRightType());
        assertEquals(ExpressionOp.ADD, p1.getOperator());
        
        assertTrue(graph.containsEdge(SINK, binary));
      }
    }
    
  }
  
  @Test
  public void setupGraph3MetricsComplexThroughNode() throws Exception {
    ExpressionFactory factory = new ExpressionFactory();
    DirectedAcyclicGraph<QueryNodeConfig, DefaultEdge> graph =
        new DirectedAcyclicGraph<QueryNodeConfig, DefaultEdge>(DefaultEdge.class);
    
    QueryNodeConfig m1 = QuerySourceConfig.newBuilder()
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric("sys.cpu.user")
            .build())
        .setFilterId("f1")
        .setId("m1")
        .build();
    QueryNodeConfig m2 = QuerySourceConfig.newBuilder()
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric("sys.cpu.sys")
            .build())
        .setFilterId("f1")
        .setId("m2")
        .build();
    QueryNodeConfig m3 = QuerySourceConfig.newBuilder()
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
    QueryNodeConfig exp = ExpressionConfig.newBuilder()
        .setExpression("(m1 * 1024) + (m2 * 1024) + (m3 * 1024)")
        .setJoinConfig((JoinConfig) JoinConfig.newBuilder()
            .setJoinType(JoinType.NATURAL)
            .build())
        .addInterpolatorConfig(NUMERIC_CONFIG)
        .setId("expression")
        .build();
    
    QueryPlanner plan = mock(QueryPlanner.class);
    when(plan.configGraph()).thenReturn(graph);
    
    graph.addVertex(SINK);
    graph.addVertex(m1);
    graph.addVertex(m2);
    graph.addVertex(m3);
    graph.addVertex(ds);
    graph.addVertex(exp);
    graph.addDagEdge(ds, m1);
    graph.addDagEdge(ds, m2);
    graph.addDagEdge(ds, m3);
    graph.addDagEdge(exp, ds);
    graph.addDagEdge(SINK, exp);
    
    factory.setupGraph(mock(TimeSeriesQuery.class), exp, plan);
    assertEquals(10, graph.vertexSet().size());
    assertTrue(graph.containsVertex(m1));
    assertTrue(graph.containsVertex(m2));
    assertTrue(graph.containsVertex(m3));
    assertTrue(graph.containsVertex(ds));
    assertFalse(graph.containsVertex(exp));
    assertTrue(graph.containsVertex(SINK));
    
    assertTrue(graph.containsEdge(ds, m1));
    assertTrue(graph.containsEdge(ds, m2));
    
    List<QueryNodeConfig> expressions = Lists.newArrayListWithCapacity(2);
    for (final DefaultEdge edge : graph.incomingEdgesOf(ds)) {
      expressions.add(graph.getEdgeSource(edge));
    }
    assertEquals(3, expressions.size());
    for (final QueryNodeConfig binary : expressions) {
      assertTrue(graph.containsEdge(binary, ds));
      assertEquals("BinaryExpression", binary.getType());
      if (binary.getId().equals("expression_SubExp#0")) {
        ExpressionParseNode p1 = (ExpressionParseNode) binary;
        assertEquals("expression_SubExp#0", p1.getId());
        assertEquals("sys.cpu.user", p1.getLeft());
        assertEquals(OperandType.VARIABLE, p1.getLeftType());
        assertEquals(1024, ((NumericLiteral) p1.getRight()).longValue());
        assertEquals(OperandType.LITERAL_NUMERIC, p1.getRightType());
        assertEquals(ExpressionOp.MULTIPLY, p1.getOperator());
        
        assertFalse(graph.containsEdge(SINK, binary));
        assertEquals(1, graph.incomingEdgesOf(binary).size());
        assertEquals("expression_SubExp#2", 
            graph.getEdgeSource(graph.incomingEdgesOf(binary).iterator().next()).getId());
      } else if (binary.getId().equals("expression_SubExp#1")) {
        ExpressionParseNode p1 = (ExpressionParseNode) binary;
        assertEquals("expression_SubExp#1", p1.getId());
        assertEquals("sys.cpu.sys", p1.getLeft());
        assertEquals(OperandType.VARIABLE, p1.getLeftType());
        assertEquals(1024, ((NumericLiteral) p1.getRight()).longValue());
        assertEquals(OperandType.LITERAL_NUMERIC, p1.getRightType());
        assertEquals(ExpressionOp.MULTIPLY, p1.getOperator());
        
        assertFalse(graph.containsEdge(SINK, binary));
        assertEquals(1, graph.incomingEdgesOf(binary).size());
        QueryNodeConfig b2 = graph.getEdgeSource(
            graph.incomingEdgesOf(binary).iterator().next());
        assertEquals("expression_SubExp#2", b2.getId());
        
        // validate sub2 here
        p1 = (ExpressionParseNode) b2;
        assertEquals("expression_SubExp#2", p1.getId());
        assertEquals("expression_SubExp#0", p1.getLeft());
        assertEquals(OperandType.SUB_EXP, p1.getLeftType());
        assertEquals("expression_SubExp#1", p1.getRight());
        assertEquals(OperandType.SUB_EXP, p1.getRightType());
        assertEquals(ExpressionOp.ADD, p1.getOperator());
        
        assertFalse(graph.containsEdge(SINK, b2));
        
      } else if (binary.getId().equals("expression_SubExp#3")) {
        ExpressionParseNode p1 = (ExpressionParseNode) binary;
        assertEquals("expression_SubExp#3", p1.getId());
        assertEquals("sys.cpu.idle", p1.getLeft());
        assertEquals(OperandType.VARIABLE, p1.getLeftType());
        assertEquals(1024, ((NumericLiteral) p1.getRight()).longValue());
        assertEquals(OperandType.LITERAL_NUMERIC, p1.getRightType());
        assertEquals(ExpressionOp.MULTIPLY, p1.getOperator());
        
        assertFalse(graph.containsEdge(SINK, binary));
        assertEquals(1, graph.incomingEdgesOf(binary).size());
        
        // validate the expression here
        QueryNodeConfig b2 = graph.getEdgeSource(
            graph.incomingEdgesOf(binary).iterator().next());
        assertEquals("expression", b2.getId());
        
        // validate parent here
        p1 = (ExpressionParseNode) b2;
        assertEquals("expression", p1.getId());
        assertEquals("expression_SubExp#2", p1.getLeft());
        assertEquals(OperandType.SUB_EXP, p1.getLeftType());
        assertEquals("expression_SubExp#3", p1.getRight());
        assertEquals(OperandType.SUB_EXP, p1.getRightType());
        assertEquals(ExpressionOp.ADD, p1.getOperator());
        
        assertTrue(graph.containsEdge(SINK, b2));
      }
    }
    
  }
}
