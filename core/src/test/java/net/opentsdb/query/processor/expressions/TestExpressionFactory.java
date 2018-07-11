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
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.execution.graph.ExecutionGraphNode;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.joins.JoinConfig;
import net.opentsdb.query.joins.JoinConfig.JoinType;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.processor.expressions.ExpressionParseNode.ExpressionOp;
import net.opentsdb.query.processor.expressions.ExpressionParseNode.OperandType;
import net.opentsdb.query.processor.expressions.ExpressionParser.NumericLiteral;

public class TestExpressionFactory {

  private static QueryPipelineContext CONTEXT;
  protected static NumericInterpolatorConfig NUMERIC_CONFIG;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    CONTEXT =  mock(QueryPipelineContext.class);
    NUMERIC_CONFIG = 
        (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
      .setFillPolicy(FillPolicy.NOT_A_NUMBER)
      .setRealFillPolicy(FillWithRealPolicy.NONE)
      .setType(NumericType.TYPE.toString())
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
        .setJoinConfig((JoinConfig) JoinConfig.newBuilder()
            .setType(JoinType.NATURAL)
            .build())
        .addInterpolatorConfig(NUMERIC_CONFIG)
        .setId("e1")
        .build();
    
    List<ExecutionGraphNode> nodes = Lists.newArrayList();
    nodes.add(ExecutionGraphNode.newBuilder()
        .setId("a")
        .build());
    nodes.add(ExecutionGraphNode.newBuilder()
        .setId("b")
        .build());
    
    Collection<QueryNode> new_nodes = factory.newNodes(
        CONTEXT, "e1", config, nodes);
    assertEquals(1, new_nodes.size());
    
    Iterator<QueryNode> iterator = new_nodes.iterator();
    BinaryExpressionNode exp_node = (BinaryExpressionNode) iterator.next();
    assertEquals("e1", exp_node.id());
    assertSame(config, exp_node.config());
    
    assertEquals(3, nodes.size());
    assertTrue(nodes.get(2).getSources().contains("a"));
    assertTrue(nodes.get(2).getSources().contains("b"));
    ExpressionParseNode parse_node = (ExpressionParseNode) nodes.get(2).getConfig();
    assertEquals("a", parse_node.left());
    assertEquals(OperandType.VARIABLE, parse_node.leftType());
    assertEquals("b", parse_node.right());
    assertEquals(OperandType.VARIABLE, parse_node.rightType());
    assertEquals(ExpressionOp.ADD, parse_node.operator());
    
    // literal
    nodes.remove(2);
    config = 
        (ExpressionConfig) ExpressionConfig.newBuilder()
        .setExpression("a + 42")
        .setJoinConfig((JoinConfig) JoinConfig.newBuilder()
            .setType(JoinType.NATURAL)
            .build())
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
    
    assertEquals(3, nodes.size());
    assertTrue(nodes.get(2).getSources().contains("a"));
    parse_node = (ExpressionParseNode) nodes.get(2).getConfig();
    assertEquals("a", parse_node.left());
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
        .setJoinConfig((JoinConfig) JoinConfig.newBuilder()
            .setType(JoinType.NATURAL)
            .build())
        .addInterpolatorConfig(NUMERIC_CONFIG)
        .setId("e1")
        .build();
    
    List<ExecutionGraphNode> nodes = Lists.newArrayList();
    nodes.add(ExecutionGraphNode.newBuilder()
        .setId("a")
        .build());
    nodes.add(ExecutionGraphNode.newBuilder()
        .setId("b")
        .build());
    nodes.add(ExecutionGraphNode.newBuilder()
        .setId("c")
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
    
    assertEquals(5, nodes.size());
    assertEquals(2, nodes.get(3).getSources().size());
    assertTrue(nodes.get(3).getSources().contains("a"));
    assertTrue(nodes.get(3).getSources().contains("b"));
    
    ExpressionParseNode parse_node = (ExpressionParseNode) nodes.get(3).getConfig();
    assertEquals("a", parse_node.left());
    assertEquals(OperandType.VARIABLE, parse_node.leftType());
    assertEquals("b", parse_node.right());
    assertEquals(OperandType.VARIABLE, parse_node.rightType());
    assertEquals(ExpressionOp.ADD, parse_node.operator());
    
    assertEquals(2, nodes.get(4).getSources().size());
    assertTrue(nodes.get(4).getSources().contains("e1_SubExp#0"));
    assertTrue(nodes.get(4).getSources().contains("c"));
    parse_node = (ExpressionParseNode) nodes.get(4).getConfig();
    assertEquals("e1_SubExp#0", parse_node.left());
    assertEquals(OperandType.SUB_EXP, parse_node.leftType());
    assertEquals("c", parse_node.right());
    assertEquals(OperandType.VARIABLE, parse_node.rightType());
    assertEquals(ExpressionOp.ADD, parse_node.operator());
  }
}
