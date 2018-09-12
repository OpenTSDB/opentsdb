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
import net.opentsdb.query.execution.graph.ExecutionGraphNode;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.joins.JoinConfig;
import net.opentsdb.query.joins.JoinConfig.JoinType;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.query.processor.expressions.ExpressionParseNode.ExpressionOp;
import net.opentsdb.query.processor.expressions.ExpressionParseNode.OperandType;
import net.opentsdb.query.processor.expressions.ExpressionParser.NumericLiteral;

public class TestExpressionFactory {

  protected static NumericInterpolatorConfig NUMERIC_CONFIG;
  protected static JoinConfig JOIN_CONFIG;
  protected static ExecutionGraphNode SINK;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    NUMERIC_CONFIG = 
        (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
      .setFillPolicy(FillPolicy.NOT_A_NUMBER)
      .setRealFillPolicy(FillWithRealPolicy.NONE)
      .setDataType(NumericType.TYPE.toString())
      .build();
    
    JOIN_CONFIG = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.NATURAL)
        .build();
    
    SINK = mock(ExecutionGraphNode.class);
  }
  
  @Test
  public void ctor() throws Exception {
    ExpressionFactory factory = new ExpressionFactory();
    assertEquals(0, factory.types().size());
  }
  
  @Test
  public void setupGraph1MetricDirect() throws Exception {
    ExpressionFactory factory = new ExpressionFactory();
    DirectedAcyclicGraph<ExecutionGraphNode, DefaultEdge> graph =
        new DirectedAcyclicGraph<ExecutionGraphNode, DefaultEdge>(DefaultEdge.class);
    
    ExecutionGraphNode m1 = ExecutionGraphNode.newBuilder()
        .setId("m1")
        .setType("DataSource")
        .setConfig(QuerySourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setFilterId("f1")
            .setId("m1")
            .build())
        .build();
    ExecutionGraphNode exp = ExecutionGraphNode.newBuilder()
        .setId("expression")
        .addSource("m1")
        .setConfig(ExpressionConfig.newBuilder()
            .setExpression("m1 + 42")
            .setJoinConfig((JoinConfig) JoinConfig.newBuilder()
                .setType(JoinType.NATURAL)
                .build())
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("expression")
            .build())
        .build();
    
    graph.addVertex(SINK);
    graph.addVertex(m1);
    graph.addVertex(exp);
    graph.addDagEdge(exp, m1);
    graph.addDagEdge(SINK, exp);
    
    factory.setupGraph(mock(TimeSeriesQuery.class), exp, graph);
    assertEquals(3, graph.vertexSet().size());
    assertTrue(graph.containsVertex(m1));
    assertFalse(graph.containsVertex(exp));
    assertTrue(graph.containsVertex(SINK));
    
    ExecutionGraphNode b1 = graph.getEdgeSource(
        graph.incomingEdgesOf(m1).iterator().next());
    assertEquals("BinaryExpression", b1.getType());
    assertTrue(graph.containsEdge(b1, m1));
    assertTrue(graph.containsEdge(SINK, b1));
    
    ExpressionParseNode p1 = (ExpressionParseNode) b1.getConfig();
    assertEquals("expression", p1.getId());
    assertEquals("sys.cpu.user", p1.left());
    assertEquals(OperandType.VARIABLE, p1.leftType());
    assertEquals(42, ((NumericLiteral) p1.right()).longValue());
    assertEquals(OperandType.LITERAL_NUMERIC, p1.rightType());
    assertEquals(ExpressionOp.ADD, p1.operator());
  }
  
  @Test
  public void setupGraph1MetricThroughNode() throws Exception {
    ExpressionFactory factory = new ExpressionFactory();
    DirectedAcyclicGraph<ExecutionGraphNode, DefaultEdge> graph =
        new DirectedAcyclicGraph<ExecutionGraphNode, DefaultEdge>(DefaultEdge.class);
    
    ExecutionGraphNode m1 = ExecutionGraphNode.newBuilder()
        .setId("m1")
        .setType("DataSource")
        .setConfig(QuerySourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setFilterId("f1")
            .setId("m1")
            .build())
        .build();
    ExecutionGraphNode ds = ExecutionGraphNode.newBuilder()
        .setId("downsample")
        .addSource("m1")
        .setConfig(DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("downsample")
            .build())
        .build();
    ExecutionGraphNode exp = ExecutionGraphNode.newBuilder()
        .setId("expression")
        .addSource("downsample")
        .setConfig(ExpressionConfig.newBuilder()
            .setExpression("m1 + 42")
            .setJoinConfig((JoinConfig) JoinConfig.newBuilder()
                .setType(JoinType.NATURAL)
                .build())
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("expression")
            .build())
        .build();
    
    graph.addVertex(SINK);
    graph.addVertex(m1);
    graph.addVertex(ds);
    graph.addVertex(exp);
    graph.addDagEdge(ds, m1);
    graph.addDagEdge(exp, ds);
    graph.addDagEdge(SINK, exp);
    
    factory.setupGraph(mock(TimeSeriesQuery.class), exp, graph);
    assertEquals(4, graph.vertexSet().size());
    assertTrue(graph.containsVertex(m1));
    assertTrue(graph.containsVertex(ds));
    assertFalse(graph.containsVertex(exp));
    assertTrue(graph.containsVertex(SINK));
    
    ExecutionGraphNode b1 = graph.getEdgeSource(
        graph.incomingEdgesOf(ds).iterator().next());
    assertEquals("BinaryExpression", b1.getType());
    assertTrue(graph.containsEdge(ds, m1));
    assertTrue(graph.containsEdge(b1, ds));
    assertTrue(graph.containsEdge(SINK, b1));
    
    ExpressionParseNode p1 = (ExpressionParseNode) b1.getConfig();
    assertEquals("expression", p1.getId());
    assertEquals("sys.cpu.user", p1.left());
    assertEquals(OperandType.VARIABLE, p1.leftType());
    assertEquals(42, ((NumericLiteral) p1.right()).longValue());
    assertEquals(OperandType.LITERAL_NUMERIC, p1.rightType());
    assertEquals(ExpressionOp.ADD, p1.operator());
  }
  
  @Test
  public void setupGraph2MetricsThroughNode() throws Exception {
    ExpressionFactory factory = new ExpressionFactory();
    
    DirectedAcyclicGraph<ExecutionGraphNode, DefaultEdge> graph =
        new DirectedAcyclicGraph<ExecutionGraphNode, DefaultEdge>(DefaultEdge.class);
    
    ExecutionGraphNode m1 = ExecutionGraphNode.newBuilder()
        .setId("m1")
        .setType("DataSource")
        .setConfig(QuerySourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setFilterId("f1")
            .setId("m1")
            .build())
        .build();
    ExecutionGraphNode m2 = ExecutionGraphNode.newBuilder()
        .setId("m2")
        .setType("DataSource")
        .setConfig(QuerySourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.sys")
                .build())
            .setFilterId("f1")
            .setId("m2")
            .build())
        .build();
    ExecutionGraphNode ds = ExecutionGraphNode.newBuilder()
        .setId("downsample")
        .addSource("m1")
        .addSource("m2")
        .setConfig(DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("downsample")
            .build())
        .build();
    ExecutionGraphNode exp = ExecutionGraphNode.newBuilder()
        .setId("expression")
        .addSource("downsample")
        .setConfig(ExpressionConfig.newBuilder()
            .setExpression("m1 + m2")
            .setJoinConfig((JoinConfig) JoinConfig.newBuilder()
                .setType(JoinType.NATURAL)
                .build())
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("expression")
            .build())
        .build();
    
    graph.addVertex(SINK);
    graph.addVertex(m1);
    graph.addVertex(m2);
    graph.addVertex(ds);
    graph.addVertex(exp);
    graph.addDagEdge(ds, m1);
    graph.addDagEdge(ds, m2);
    graph.addDagEdge(exp, ds);
    graph.addDagEdge(SINK, exp);
    
    factory.setupGraph(mock(TimeSeriesQuery.class), exp, graph);
    assertEquals(5, graph.vertexSet().size());
    assertTrue(graph.containsVertex(m1));
    assertTrue(graph.containsVertex(m2));
    assertTrue(graph.containsVertex(ds));
    assertFalse(graph.containsVertex(exp));
    assertTrue(graph.containsVertex(SINK));
    
    ExecutionGraphNode b1 = graph.getEdgeSource(
        graph.incomingEdgesOf(ds).iterator().next());
    assertEquals("BinaryExpression", b1.getType());
    assertTrue(graph.containsEdge(ds, m1));
    assertTrue(graph.containsEdge(ds, m2));
    assertTrue(graph.containsEdge(b1, ds));
    assertTrue(graph.containsEdge(SINK, b1));
    
    ExpressionParseNode p1 = (ExpressionParseNode) b1.getConfig();
    assertEquals("expression", p1.getId());
    assertEquals("sys.cpu.user", p1.left());
    assertEquals(OperandType.VARIABLE, p1.leftType());
    assertEquals("sys.cpu.sys", p1.right());
    assertEquals(OperandType.VARIABLE, p1.rightType());
    assertEquals(ExpressionOp.ADD, p1.operator());
  }
  
  @Test
  public void setupGraph2Metrics1ThroughNode() throws Exception {
    ExpressionFactory factory = new ExpressionFactory();
    
    DirectedAcyclicGraph<ExecutionGraphNode, DefaultEdge> graph =
        new DirectedAcyclicGraph<ExecutionGraphNode, DefaultEdge>(DefaultEdge.class);
    
    ExecutionGraphNode m1 = ExecutionGraphNode.newBuilder()
        .setId("m1")
        .setType("DataSource")
        .setConfig(QuerySourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setFilterId("f1")
            .setId("m1")
            .build())
        .build();
    ExecutionGraphNode m2 = ExecutionGraphNode.newBuilder()
        .setId("m2")
        .setType("DataSource")
        .setConfig(QuerySourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.sys")
                .build())
            .setFilterId("f1")
            .setId("m2")
            .build())
        .build();
    ExecutionGraphNode ds = ExecutionGraphNode.newBuilder()
        .setId("downsample")
        .addSource("m1")
        .setConfig(DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("downsample")
            .build())
        .build();
    ExecutionGraphNode exp = ExecutionGraphNode.newBuilder()
        .setId("expression")
        .addSource("downsample")
        .addSource("m2")
        .setConfig(ExpressionConfig.newBuilder()
            .setExpression("m1 + m2")
            .setJoinConfig((JoinConfig) JoinConfig.newBuilder()
                .setType(JoinType.NATURAL)
                .build())
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("expression")
            .build())
        .build();
    
    graph.addVertex(SINK);
    graph.addVertex(m1);
    graph.addVertex(m2);
    graph.addVertex(ds);
    graph.addVertex(exp);
    graph.addDagEdge(ds, m1);
    graph.addDagEdge(exp, m2);
    graph.addDagEdge(exp, ds);
    graph.addDagEdge(SINK, exp);
    
    factory.setupGraph(mock(TimeSeriesQuery.class), exp, graph);
    assertEquals(5, graph.vertexSet().size());
    assertTrue(graph.containsVertex(m1));
    assertTrue(graph.containsVertex(m2));
    assertTrue(graph.containsVertex(ds));
    assertFalse(graph.containsVertex(exp));
    assertTrue(graph.containsVertex(SINK));
    
    ExecutionGraphNode b1 = graph.getEdgeSource(
        graph.incomingEdgesOf(ds).iterator().next());
    assertEquals("BinaryExpression", b1.getType());
    assertTrue(graph.containsEdge(ds, m1));
    assertTrue(graph.containsEdge(b1, m2));
    assertTrue(graph.containsEdge(b1, ds));
    assertTrue(graph.containsEdge(SINK, b1));
    
    ExpressionParseNode p1 = (ExpressionParseNode) b1.getConfig();
    assertEquals("expression", p1.getId());
    assertEquals("sys.cpu.user", p1.left());
    assertEquals(OperandType.VARIABLE, p1.leftType());
    assertEquals("sys.cpu.sys", p1.right());
    assertEquals(OperandType.VARIABLE, p1.rightType());
    assertEquals(ExpressionOp.ADD, p1.operator());
  }
  
  @Test
  public void setupGraph3MetricsThroughNode() throws Exception {
    ExpressionFactory factory = new ExpressionFactory();
    DirectedAcyclicGraph<ExecutionGraphNode, DefaultEdge> graph =
        new DirectedAcyclicGraph<ExecutionGraphNode, DefaultEdge>(DefaultEdge.class);
    
    ExecutionGraphNode m1 = ExecutionGraphNode.newBuilder()
        .setId("m1")
        .setType("DataSource")
        .setConfig(QuerySourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setFilterId("f1")
            .setId("m1")
            .build())
        .build();
    ExecutionGraphNode m2 = ExecutionGraphNode.newBuilder()
        .setId("m2")
        .setType("DataSource")
        .setConfig(QuerySourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.sys")
                .build())
            .setFilterId("f1")
            .setId("m2")
            .build())
        .build();
    ExecutionGraphNode m3 = ExecutionGraphNode.newBuilder()
        .setId("m3")
        .setType("DataSource")
        .setConfig(QuerySourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.idle")
                .build())
            .setFilterId("f1")
            .setId("m3")
            .build())
        .build();
    ExecutionGraphNode ds = ExecutionGraphNode.newBuilder()
        .setId("downsample")
        .addSource("m1")
        .addSource("m2")
        .addSource("m3")
        .setConfig(DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("downsample")
            .build())
        .build();
    ExecutionGraphNode exp = ExecutionGraphNode.newBuilder()
        .setId("expression")
        .addSource("downsample")
        .setConfig(ExpressionConfig.newBuilder()
            .setExpression("m1 + m2 + m3")
            .setJoinConfig((JoinConfig) JoinConfig.newBuilder()
                .setType(JoinType.NATURAL)
                .build())
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("expression")
            .build())
        .build();
    
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
    
    factory.setupGraph(mock(TimeSeriesQuery.class), exp, graph);
    assertEquals(7, graph.vertexSet().size());
    assertTrue(graph.containsVertex(m1));
    assertTrue(graph.containsVertex(m2));
    assertTrue(graph.containsVertex(m3));
    assertTrue(graph.containsVertex(ds));
    assertFalse(graph.containsVertex(exp));
    assertTrue(graph.containsVertex(SINK));
    
    assertTrue(graph.containsEdge(ds, m1));
    assertTrue(graph.containsEdge(ds, m2));
    
    List<ExecutionGraphNode> expressions = Lists.newArrayListWithCapacity(2);
    for (final DefaultEdge edge : graph.incomingEdgesOf(ds)) {
      expressions.add(graph.getEdgeSource(edge));
    }
    assertEquals(2, expressions.size());
    for (final ExecutionGraphNode binary : expressions) {
      assertTrue(graph.containsEdge(binary, ds));
      assertEquals("BinaryExpression", binary.getType());
      
      if (binary.getId().equals("expression_SubExp#0")) {
        ExpressionParseNode p1 = (ExpressionParseNode) binary.getConfig();
        assertEquals("expression_SubExp#0", p1.getId());
        assertEquals("sys.cpu.user", p1.left());
        assertEquals(OperandType.VARIABLE, p1.leftType());
        assertEquals("sys.cpu.sys", p1.right());
        assertEquals(OperandType.VARIABLE, p1.rightType());
        assertEquals(ExpressionOp.ADD, p1.operator());
        
        assertFalse(graph.containsEdge(SINK, binary));
      } else {
        assertEquals("expression", binary.getId());
        ExpressionParseNode p1 = (ExpressionParseNode) binary.getConfig();
        assertEquals("expression", p1.getId());
        assertEquals("expression_SubExp#0", p1.left());
        assertEquals(OperandType.SUB_EXP, p1.leftType());
        assertEquals("sys.cpu.idle", p1.right());
        assertEquals(OperandType.VARIABLE, p1.rightType());
        assertEquals(ExpressionOp.ADD, p1.operator());
        
        assertTrue(graph.containsEdge(SINK, binary));
      }
    }
    
  }
  
  @Test
  public void setupGraph3MetricsComplexThroughNode() throws Exception {
    ExpressionFactory factory = new ExpressionFactory();
    DirectedAcyclicGraph<ExecutionGraphNode, DefaultEdge> graph =
        new DirectedAcyclicGraph<ExecutionGraphNode, DefaultEdge>(DefaultEdge.class);
    
    ExecutionGraphNode m1 = ExecutionGraphNode.newBuilder()
        .setId("m1")
        .setType("DataSource")
        .setConfig(QuerySourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setFilterId("f1")
            .setId("m1")
            .build())
        .build();
    ExecutionGraphNode m2 = ExecutionGraphNode.newBuilder()
        .setId("m2")
        .setType("DataSource")
        .setConfig(QuerySourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.sys")
                .build())
            .setFilterId("f1")
            .setId("m2")
            .build())
        .build();
    ExecutionGraphNode m3 = ExecutionGraphNode.newBuilder()
        .setId("m3")
        .setType("DataSource")
        .setConfig(QuerySourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.idle")
                .build())
            .setFilterId("f1")
            .setId("m3")
            .build())
        .build();
    ExecutionGraphNode ds = ExecutionGraphNode.newBuilder()
        .setId("downsample")
        .addSource("m1")
        .addSource("m2")
        .addSource("m3")
        .setConfig(DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("downsample")
            .build())
        .build();
    ExecutionGraphNode exp = ExecutionGraphNode.newBuilder()
        .setId("expression")
        .addSource("downsample")
        .setConfig(ExpressionConfig.newBuilder()
            .setExpression("(m1 * 1024) + (m2 * 1024) + (m3 * 1024)")
            .setJoinConfig((JoinConfig) JoinConfig.newBuilder()
                .setType(JoinType.NATURAL)
                .build())
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("expression")
            .build())
        .build();
    
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
    
    factory.setupGraph(mock(TimeSeriesQuery.class), exp, graph);
    assertEquals(10, graph.vertexSet().size());
    assertTrue(graph.containsVertex(m1));
    assertTrue(graph.containsVertex(m2));
    assertTrue(graph.containsVertex(m3));
    assertTrue(graph.containsVertex(ds));
    assertFalse(graph.containsVertex(exp));
    assertTrue(graph.containsVertex(SINK));
    
    assertTrue(graph.containsEdge(ds, m1));
    assertTrue(graph.containsEdge(ds, m2));
    
    List<ExecutionGraphNode> expressions = Lists.newArrayListWithCapacity(2);
    for (final DefaultEdge edge : graph.incomingEdgesOf(ds)) {
      expressions.add(graph.getEdgeSource(edge));
    }
    assertEquals(3, expressions.size());
    for (final ExecutionGraphNode binary : expressions) {
      assertTrue(graph.containsEdge(binary, ds));
      assertEquals("BinaryExpression", binary.getType());
      if (binary.getId().equals("expression_SubExp#0")) {
        ExpressionParseNode p1 = (ExpressionParseNode) binary.getConfig();
        assertEquals("expression_SubExp#0", p1.getId());
        assertEquals("sys.cpu.user", p1.left());
        assertEquals(OperandType.VARIABLE, p1.leftType());
        assertEquals(1024, ((NumericLiteral) p1.right()).longValue());
        assertEquals(OperandType.LITERAL_NUMERIC, p1.rightType());
        assertEquals(ExpressionOp.MULTIPLY, p1.operator());
        
        assertFalse(graph.containsEdge(SINK, binary));
        assertEquals(1, graph.incomingEdgesOf(binary).size());
        assertEquals("expression_SubExp#2", 
            graph.getEdgeSource(graph.incomingEdgesOf(binary).iterator().next()).getId());
      } else if (binary.getId().equals("expression_SubExp#1")) {
        ExpressionParseNode p1 = (ExpressionParseNode) binary.getConfig();
        assertEquals("expression_SubExp#1", p1.getId());
        assertEquals("sys.cpu.sys", p1.left());
        assertEquals(OperandType.VARIABLE, p1.leftType());
        assertEquals(1024, ((NumericLiteral) p1.right()).longValue());
        assertEquals(OperandType.LITERAL_NUMERIC, p1.rightType());
        assertEquals(ExpressionOp.MULTIPLY, p1.operator());
        
        assertFalse(graph.containsEdge(SINK, binary));
        assertEquals(1, graph.incomingEdgesOf(binary).size());
        ExecutionGraphNode b2 = graph.getEdgeSource(
            graph.incomingEdgesOf(binary).iterator().next());
        assertEquals("expression_SubExp#2", b2.getId());
        
        // validate sub2 here
        p1 = (ExpressionParseNode) b2.getConfig();
        assertEquals("expression_SubExp#2", p1.getId());
        assertEquals("expression_SubExp#0", p1.left());
        assertEquals(OperandType.SUB_EXP, p1.leftType());
        assertEquals("expression_SubExp#1", p1.right());
        assertEquals(OperandType.SUB_EXP, p1.rightType());
        assertEquals(ExpressionOp.ADD, p1.operator());
        
        assertFalse(graph.containsEdge(SINK, b2));
        
      } else if (binary.getId().equals("expression_SubExp#3")) {
        ExpressionParseNode p1 = (ExpressionParseNode) binary.getConfig();
        assertEquals("expression_SubExp#3", p1.getId());
        assertEquals("sys.cpu.idle", p1.left());
        assertEquals(OperandType.VARIABLE, p1.leftType());
        assertEquals(1024, ((NumericLiteral) p1.right()).longValue());
        assertEquals(OperandType.LITERAL_NUMERIC, p1.rightType());
        assertEquals(ExpressionOp.MULTIPLY, p1.operator());
        
        assertFalse(graph.containsEdge(SINK, binary));
        assertEquals(1, graph.incomingEdgesOf(binary).size());
        
        // validate the expression here
        ExecutionGraphNode b2 = graph.getEdgeSource(
            graph.incomingEdgesOf(binary).iterator().next());
        assertEquals("expression", b2.getId());
        
        // validate parent here
        p1 = (ExpressionParseNode) b2.getConfig();
        assertEquals("expression", p1.getId());
        assertEquals("expression_SubExp#2", p1.left());
        assertEquals(OperandType.SUB_EXP, p1.leftType());
        assertEquals("expression_SubExp#3", p1.right());
        assertEquals(OperandType.SUB_EXP, p1.rightType());
        assertEquals(ExpressionOp.ADD, p1.operator());
        
        assertTrue(graph.containsEdge(SINK, b2));
      }
    }
    
  }
}
