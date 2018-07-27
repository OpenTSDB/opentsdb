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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;

import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QuerySourceConfig;
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
import net.opentsdb.query.processor.groupby.GroupByConfig;

public class TestExpressionFactory {

  private static QueryPipelineContext CONTEXT;
  protected static NumericInterpolatorConfig NUMERIC_CONFIG;
  protected static JoinConfig JOIN_CONFIG;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    CONTEXT =  mock(QueryPipelineContext.class);
    
    NUMERIC_CONFIG = 
        (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
      .setFillPolicy(FillPolicy.NOT_A_NUMBER)
      .setRealFillPolicy(FillWithRealPolicy.NONE)
      .setType(NumericType.TYPE.toString())
      .build();
    
    JOIN_CONFIG = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.NATURAL)
        .build();
  }
  
  @Test
  public void ctor() throws Exception {
    ExpressionFactory factory = new ExpressionFactory();
    assertEquals(2, factory.types().size());
    assertTrue(factory.types().contains(NumericType.TYPE));
    assertTrue(factory.types().contains(NumericSummaryType.TYPE));
  }
  
  @Test
  public void newNodesSimpleBinary() throws Exception {
    ExpressionFactory factory = new ExpressionFactory();
    
    ExpressionConfig config = 
        (ExpressionConfig) ExpressionConfig.newBuilder()
        .setExpression("a + b")
        .setJoinConfig(JOIN_CONFIG)
        .addInterpolatorConfig(NUMERIC_CONFIG)
        .setId("e1")
        .build();
    
    List<ExecutionGraphNode> nodes = Lists.newArrayList();
    nodes.add(ExecutionGraphNode.newBuilder()
        .setId("e1")
        .setType("expression")
        .setConfig(config)
        .setSources(Lists.newArrayList("a", "b"))
        .build());
    nodes.add(ExecutionGraphNode.newBuilder()
        .setType("DataSource")
        .setId("a")
        .setConfig(QuerySourceConfig.newBuilder()
            .setStart("1h-ago")
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("metric.a")
                .build())
            .build())
        .build());
    nodes.add(ExecutionGraphNode.newBuilder()
        .setType("DataSource")
        .setId("b")
        .setConfig(QuerySourceConfig.newBuilder()
            .setStart("1h-ago")
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("metric.b")
                .build())
            .build())
        .build());
    
    Collection<QueryNode> new_nodes = factory.newNodes(
        CONTEXT, "e1", config, nodes);
    assertEquals(1, new_nodes.size());
    
    Iterator<QueryNode> iterator = new_nodes.iterator();
    BinaryExpressionNode exp_node = (BinaryExpressionNode) iterator.next();
    assertEquals("e1", exp_node.id());
    assertSame(config, exp_node.config());
    
    assertEquals(4, nodes.size());
    assertTrue(nodes.get(3).getSources().contains("a"));
    assertTrue(nodes.get(3).getSources().contains("b"));
    ExpressionParseNode parse_node = (ExpressionParseNode) nodes.get(3).getConfig();
    assertEquals("metric.a", parse_node.left());
    assertEquals(OperandType.VARIABLE, parse_node.leftType());
    assertEquals("metric.b", parse_node.right());
    assertEquals(OperandType.VARIABLE, parse_node.rightType());
    assertEquals(ExpressionOp.ADD, parse_node.operator());
    
    // literal
    nodes.remove(3);
    config = (ExpressionConfig) ExpressionConfig.newBuilder()
        .setExpression("a + 42")
        .setJoinConfig(JOIN_CONFIG)
        .addInterpolatorConfig(NUMERIC_CONFIG)
        .setId("e1")
        .build();
    
    new_nodes = factory.newNodes(
        CONTEXT, "e1", config, nodes);
    assertEquals(1, new_nodes.size());
    
    iterator = new_nodes.iterator();
    exp_node = (BinaryExpressionNode) iterator.next();
    assertEquals("e1", exp_node.id());
    assertSame(config, exp_node.config());
    
    assertEquals(4, nodes.size());
    assertTrue(nodes.get(3).getSources().contains("a"));
    parse_node = (ExpressionParseNode) nodes.get(3).getConfig();
    assertEquals("metric.a", parse_node.left());
    assertEquals(OperandType.VARIABLE, parse_node.leftType());
    assertTrue(parse_node.right() instanceof NumericLiteral);
    assertEquals(OperandType.LITERAL_NUMERIC, parse_node.rightType());
    assertEquals(ExpressionOp.ADD, parse_node.operator());
  }
  
  @Test
  public void newNodesTwoNewNodes() throws Exception {
    ExpressionFactory factory = new ExpressionFactory();
    
    ExpressionConfig config = 
        (ExpressionConfig) ExpressionConfig.newBuilder()
        .setExpression("a + b + c")
        .setJoinConfig(JOIN_CONFIG)
        .addInterpolatorConfig(NUMERIC_CONFIG)
        .setId("e1")
        .build();
    
    List<ExecutionGraphNode> nodes = Lists.newArrayList();
    nodes.add(ExecutionGraphNode.newBuilder()
        .setId("e1")
        .setType("expression")
        .setConfig(config)
        .setSources(Lists.newArrayList("a", "b", "c"))
        .build());
    nodes.add(ExecutionGraphNode.newBuilder()
        .setType("DataSource")
        .setId("a")
        .setConfig(QuerySourceConfig.newBuilder()
            .setStart("1h-ago")
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("metric.a")
                .build())
            .build())
        .build());
    nodes.add(ExecutionGraphNode.newBuilder()
        .setId("b")
        .setType("DataSource")
        .setId("b")
        .setConfig(QuerySourceConfig.newBuilder()
            .setStart("1h-ago")
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("metric.b")
                .build())
            .build())
        .build());
    nodes.add(ExecutionGraphNode.newBuilder()
        .setId("c")
        .setType("DataSource")
        .setId("c")
        .setConfig(QuerySourceConfig.newBuilder()
            .setStart("1h-ago")
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("metric.c")
                .build())
            .build())
        .build());
    
    Collection<QueryNode> new_nodes = factory.newNodes(
        CONTEXT, "e1", config, nodes);
    assertEquals(2, new_nodes.size());
    
    Iterator<QueryNode> iterator = new_nodes.iterator();
    BinaryExpressionNode exp_node = (BinaryExpressionNode) iterator.next();
    assertEquals("e1_SubExp#0", exp_node.id());
    assertSame(config, exp_node.config());
    
    exp_node = (BinaryExpressionNode) iterator.next();
    assertEquals("e1", exp_node.id());
    assertSame(config, exp_node.config());
    
    assertEquals(6, nodes.size());
    assertEquals(2, nodes.get(4).getSources().size());
    assertTrue(nodes.get(4).getSources().contains("a"));
    assertTrue(nodes.get(4).getSources().contains("b"));
    
    ExpressionParseNode parse_node = (ExpressionParseNode) nodes.get(4).getConfig();
    assertEquals("metric.a", parse_node.left());
    assertEquals(OperandType.VARIABLE, parse_node.leftType());
    assertEquals("metric.b", parse_node.right());
    assertEquals(OperandType.VARIABLE, parse_node.rightType());
    assertEquals(ExpressionOp.ADD, parse_node.operator());
    
    assertEquals(2, nodes.get(5).getSources().size());
    assertTrue(nodes.get(5).getSources().contains("e1_SubExp#0"));
    assertTrue(nodes.get(5).getSources().contains("c"));
    parse_node = (ExpressionParseNode) nodes.get(5).getConfig();
    assertEquals("e1_SubExp#0", parse_node.left());
    assertEquals(OperandType.SUB_EXP, parse_node.leftType());
    assertEquals("metric.c", parse_node.right());
    assertEquals(OperandType.VARIABLE, parse_node.rightType());
    assertEquals(ExpressionOp.ADD, parse_node.operator());
  }

  @Test
  public void validateLiteralMetrics() throws Exception {
    ExpressionConfig c = (ExpressionConfig) ExpressionConfig.newBuilder()
        .setExpression("metric.a + metric.b")
        .setJoinConfig(JOIN_CONFIG)
        .setAs("e1")
        .addInterpolatorConfig(NUMERIC_CONFIG)
        .setId("e1")
        .build();
    
    List<ExecutionGraphNode> graph = Lists.newArrayList();
    graph.add(ExecutionGraphNode.newBuilder()
        .setType("Expression")
        .setId("e1")
        .setConfig(c)
        .addSource("a")
        .addSource("b")
        .build());
    
    graph.add(ExecutionGraphNode.newBuilder()
        .setType("DataSource")
        .setId("a")
        .setConfig(QuerySourceConfig.newBuilder()
            .setStart("1h-ago")
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("metric.a")
                .build())
            .build())
        .build());
    
    graph.add(ExecutionGraphNode.newBuilder()
        .setType("DataSource")
        .setId("b")
        .setConfig(QuerySourceConfig.newBuilder()
            .setStart("1h-ago")
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("metric.b")
                .build())
            .build())
        .build());
    
    ExpressionFactory factory = new ExpressionFactory();
    Collection<QueryNode> new_nodes = factory.newNodes(
        CONTEXT, "e1", c, graph);
    assertEquals(1, new_nodes.size());
    
    BinaryExpressionNode node = (BinaryExpressionNode) new_nodes.iterator().next();
    assertEquals("metric.a", node.expressionConfig().left());
    assertEquals("metric.b", node.expressionConfig().right());
  }
  
  @Test
  public void validateLiteralMetricThroughNodes() throws Exception {
    ExpressionConfig c = (ExpressionConfig) ExpressionConfig.newBuilder()
        .setExpression("metric.a + metric.b")
        .setJoinConfig(JOIN_CONFIG)
        .setAs("e1")
        .addInterpolatorConfig(NUMERIC_CONFIG)
        .setId("e1")
        .build();
    
    List<ExecutionGraphNode> graph = Lists.newArrayList();
    graph.add(ExecutionGraphNode.newBuilder()
        .setType("Expression")
        .setId("e1")
        .setConfig(c)
        .addSource("gb")
        .build());
    
    graph.add(ExecutionGraphNode.newBuilder()
        .setType("GroupBy")
        .setId("gb")
        .setConfig(GroupByConfig.newBuilder()
            .setAggregator("sum")
            .addTagKey("host")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .build())
        .addSource("a")
        .addSource("b")
        .build());
    
    graph.add(ExecutionGraphNode.newBuilder()
        .setType("DataSource")
        .setId("a")
        .setConfig(QuerySourceConfig.newBuilder()
            .setStart("1h-ago")
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("metric.a")
                .build())
            .build())
        .build());
    
    graph.add(ExecutionGraphNode.newBuilder()
        .setType("DataSource")
        .setId("b")
        .setConfig(QuerySourceConfig.newBuilder()
            .setStart("1h-ago")
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("metric.b")
                .build())
            .build())
        .build());
    
    ExpressionFactory factory = new ExpressionFactory();
    Collection<QueryNode> new_nodes = factory.newNodes(
        CONTEXT, "e1", c, graph);
    assertEquals(1, new_nodes.size());
    
    BinaryExpressionNode node = (BinaryExpressionNode) new_nodes.iterator().next();
    assertEquals("metric.a", node.expressionConfig().left());
    assertEquals("metric.b", node.expressionConfig().right());
  }
  
  @Test
  public void validateIdThroughNodes() throws Exception {
    ExpressionConfig c = (ExpressionConfig) ExpressionConfig.newBuilder()
        .setExpression("a + b")
        .setJoinConfig(JOIN_CONFIG)
        .setAs("e1")
        .addInterpolatorConfig(NUMERIC_CONFIG)
        .setId("e1")
        .build();
    
    List<ExecutionGraphNode> graph = Lists.newArrayList();
    graph.add(ExecutionGraphNode.newBuilder()
        .setType("Expression")
        .setId("e1")
        .setConfig(c)
        .addSource("gb")
        .build());
    
    graph.add(ExecutionGraphNode.newBuilder()
        .setType("GroupBy")
        .setId("gb")
        .setConfig(GroupByConfig.newBuilder()
            .setAggregator("sum")
            .addTagKey("host")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .build())
        .addSource("ds")
        .build());
    
    graph.add(ExecutionGraphNode.newBuilder()
        .setType("Downsample")
        .setId("ds")
        .setConfig(DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .build())
        .addSource("a")
        .addSource("b")
        .build());
    
    graph.add(ExecutionGraphNode.newBuilder()
        .setType("DataSource")
        .setId("a")
        .setConfig(QuerySourceConfig.newBuilder()
            .setStart("1h-ago")
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("metric.a")
                .build())
            .build())
        .build());
    
    graph.add(ExecutionGraphNode.newBuilder()
        .setType("DataSource")
        .setId("b")
        .setConfig(QuerySourceConfig.newBuilder()
            .setStart("1h-ago")
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("metric.b")
                .build())
            .build())
        .build());
    
    ExpressionFactory factory = new ExpressionFactory();
    Collection<QueryNode> new_nodes = factory.newNodes(
        CONTEXT, "e1", c, graph);
    assertEquals(1, new_nodes.size());
    
    BinaryExpressionNode node = (BinaryExpressionNode) new_nodes.iterator().next();
    assertEquals("metric.a", node.expressionConfig().left());
    assertEquals("metric.b", node.expressionConfig().right());
  }

  @Test
  public void validateTernaryIdsThroughNodes() throws Exception {
    ExpressionConfig c = (ExpressionConfig) ExpressionConfig.newBuilder()
        .setExpression("a + b + c")
        .setJoinConfig(JOIN_CONFIG)
        .setAs("e1")
        .addInterpolatorConfig(NUMERIC_CONFIG)
        .setId("e1")
        .build();
    
    List<ExecutionGraphNode> graph = Lists.newArrayList();
    graph.add(ExecutionGraphNode.newBuilder()
        .setType("Expression")
        .setId("e1")
        .setConfig(c)
        .addSource("gb")
        .build());
    
    graph.add(ExecutionGraphNode.newBuilder()
        .setType("GroupBy")
        .setId("gb")
        .setConfig(GroupByConfig.newBuilder()
            .setAggregator("sum")
            .addTagKey("host")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .build())
        .addSource("ds")
        .build());
    
    graph.add(ExecutionGraphNode.newBuilder()
        .setType("Downsample")
        .setId("ds")
        .setConfig(DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .build())
        .addSource("a")
        .addSource("b")
        .addSource("c")
        .build());
    
    graph.add(ExecutionGraphNode.newBuilder()
        .setType("DataSource")
        .setId("a")
        .setConfig(QuerySourceConfig.newBuilder()
            .setStart("1h-ago")
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("metric.a")
                .build())
            .build())
        .build());
    
    graph.add(ExecutionGraphNode.newBuilder()
        .setType("DataSource")
        .setId("b")
        .setConfig(QuerySourceConfig.newBuilder()
            .setStart("1h-ago")
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("metric.b")
                .build())
            .build())
        .build());
    
    graph.add(ExecutionGraphNode.newBuilder()
        .setType("DataSource")
        .setId("c")
        .setConfig(QuerySourceConfig.newBuilder()
            .setStart("1h-ago")
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("metric.c")
                .build())
            .build())
        .build());
    
    ExpressionFactory factory = new ExpressionFactory();
    Collection<QueryNode> new_nodes = factory.newNodes(
        CONTEXT, "e1", c, graph);
    assertEquals(2, new_nodes.size());
    
    Iterator<QueryNode> iterator = new_nodes.iterator();
    BinaryExpressionNode node = (BinaryExpressionNode) iterator.next();
    assertEquals("metric.a", node.expressionConfig().left());
    assertEquals("metric.b", node.expressionConfig().right());
    
    node = (BinaryExpressionNode) iterator.next();
    assertEquals("e1_SubExp#0", node.expressionConfig().left());
    assertEquals("metric.c", node.expressionConfig().right());
  }
  
  @Test
  public void validateIdOfExpression() throws Exception {
    ExpressionConfig c = (ExpressionConfig) ExpressionConfig.newBuilder()
        .setExpression("a + b")
        .setJoinConfig(JOIN_CONFIG)
        .setAs("my.new.metric")
        .addInterpolatorConfig(NUMERIC_CONFIG)
        .setId("e1")
        .build();
    
    ExpressionConfig c2 = (ExpressionConfig) ExpressionConfig.newBuilder()
        .setExpression("e1 * c")
        .setJoinConfig(JOIN_CONFIG)
        .setAs("e")
        .addInterpolatorConfig(NUMERIC_CONFIG)
        .setId("e2")
        .build();
    
    List<ExecutionGraphNode> graph = Lists.newArrayList();
    graph.add(ExecutionGraphNode.newBuilder()
        .setType("Expression")
        .setId("e1")
        .setConfig(c)
        .addSource("gb")
        .build());
    
    graph.add(ExecutionGraphNode.newBuilder()
        .setType("Expression")
        .setId("e2")
        .setConfig(c2)
        .addSource("gb")
        .addSource("e1")
        .build());
    
    graph.add(ExecutionGraphNode.newBuilder()
        .setType("GroupBy")
        .setId("gb")
        .setConfig(GroupByConfig.newBuilder()
            .setAggregator("sum")
            .addTagKey("host")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .build())
        .addSource("ds")
        .build());
    
    graph.add(ExecutionGraphNode.newBuilder()
        .setType("Downsample")
        .setId("ds")
        .setConfig(DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .build())
        .addSource("a")
        .addSource("b")
        .addSource("c")
        .build());
    
    graph.add(ExecutionGraphNode.newBuilder()
        .setType("DataSource")
        .setId("a")
        .setConfig(QuerySourceConfig.newBuilder()
            .setStart("1h-ago")
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("metric.a")
                .build())
            .build())
        .build());
    
    graph.add(ExecutionGraphNode.newBuilder()
        .setType("DataSource")
        .setId("b")
        .setConfig(QuerySourceConfig.newBuilder()
            .setStart("1h-ago")
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("metric.b")
                .build())
            .build())
        .build());
    
    graph.add(ExecutionGraphNode.newBuilder()
        .setType("DataSource")
        .setId("c")
        .setConfig(QuerySourceConfig.newBuilder()
            .setStart("1h-ago")
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("metric.c")
                .build())
            .build())
        .build());
    
    ExpressionFactory factory = new ExpressionFactory();
    Collection<QueryNode> new_nodes = factory.newNodes(
        CONTEXT, "e1", c, graph);
    assertEquals(1, new_nodes.size());
    
    BinaryExpressionNode node = (BinaryExpressionNode) new_nodes.iterator().next();
    assertEquals("metric.a", node.expressionConfig().left());
    assertEquals("metric.b", node.expressionConfig().right());
    
    // simulate removal
    graph.remove(0);
    
    new_nodes = factory.newNodes(CONTEXT, "e2", c2, graph);
    assertEquals(1, new_nodes.size());
    node = (BinaryExpressionNode) new_nodes.iterator().next();
    assertEquals("my.new.metric", node.expressionConfig().left());
    assertEquals("metric.c", node.expressionConfig().right());
  }
}
