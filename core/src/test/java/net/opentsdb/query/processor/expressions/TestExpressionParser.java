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
package net.opentsdb.query.processor.expressions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;
import java.util.Set;

import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.junit.BeforeClass;
import org.junit.Test;

import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.joins.JoinConfig;
import net.opentsdb.query.joins.JoinConfig.JoinType;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.processor.expressions.ExpressionParseNode.ExpressionOp;
import net.opentsdb.query.processor.expressions.ExpressionParseNode.OperandType;
import net.opentsdb.query.processor.expressions.ExpressionParser.NumericLiteral;

public class TestExpressionParser {

  protected static NumericInterpolatorConfig NUMERIC_CONFIG;
  protected static JoinConfig JOIN_CONFIG;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    NUMERIC_CONFIG = 
        (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
      .setFillPolicy(FillPolicy.NOT_A_NUMBER)
      .setRealFillPolicy(FillWithRealPolicy.NONE)
      .setDataType(NumericType.TYPE.toString())
      .build();
    
    JOIN_CONFIG = (JoinConfig) JoinConfig.newBuilder()
        .setJoinType(JoinType.INNER)
        .addJoins("host", "host")
        .setId("join")
        .build();
  }
  
  @Test
  public void parseBinaryOperators() throws Exception {
    ExpressionParser parser = new ExpressionParser(config("a.metric + b.metric"));
    List<ExpressionParseNode> nodes = parser.parse();
    assertEquals(1, nodes.size());
    assertEquals("e1", nodes.get(0).getId());
    assertEquals(OperandType.VARIABLE, nodes.get(0).getLeftType());
    assertEquals("a.metric", nodes.get(0).getLeft());
    assertEquals(OperandType.VARIABLE, nodes.get(0).getRightType());
    assertEquals("b.metric", nodes.get(0).getRight());
    assertEquals(ExpressionOp.ADD, nodes.get(0).getOperator());
    
    parser = new ExpressionParser(config("a.metric - b.metric"));
    nodes = parser.parse();
    assertEquals(1, nodes.size());
    assertEquals("e1", nodes.get(0).getId());
    assertEquals("e1", nodes.get(0).getAs());
    assertEquals(OperandType.VARIABLE, nodes.get(0).getLeftType());
    assertEquals("a.metric", nodes.get(0).getLeft());
    assertEquals(OperandType.VARIABLE, nodes.get(0).getRightType());
    assertEquals("b.metric", nodes.get(0).getRight());
    assertEquals(ExpressionOp.SUBTRACT, nodes.get(0).getOperator());
    
    parser = new ExpressionParser(config("a.metric * b.metric"));
    nodes = parser.parse();
    assertEquals(1, nodes.size());
    assertEquals("e1", nodes.get(0).getId());
    assertEquals("e1", nodes.get(0).getAs());
    assertEquals(OperandType.VARIABLE, nodes.get(0).getLeftType());
    assertEquals("a.metric", nodes.get(0).getLeft());
    assertEquals(OperandType.VARIABLE, nodes.get(0).getRightType());
    assertEquals("b.metric", nodes.get(0).getRight());
    assertEquals(ExpressionOp.MULTIPLY, nodes.get(0).getOperator());
    
    parser = new ExpressionParser(config("a.metric / b.metric"));
    nodes = parser.parse();
    assertEquals(1, nodes.size());
    assertEquals("e1", nodes.get(0).getId());
    assertEquals("e1", nodes.get(0).getAs());
    assertEquals(OperandType.VARIABLE, nodes.get(0).getLeftType());
    assertEquals("a.metric", nodes.get(0).getLeft());
    assertEquals(OperandType.VARIABLE, nodes.get(0).getRightType());
    assertEquals("b.metric", nodes.get(0).getRight());
    assertEquals(ExpressionOp.DIVIDE, nodes.get(0).getOperator());
    
    parser = new ExpressionParser(config("a.metric % b.metric"));
    nodes = parser.parse();
    assertEquals(1, nodes.size());
    assertEquals("e1", nodes.get(0).getId());
    assertEquals("e1", nodes.get(0).getAs());
    assertEquals(OperandType.VARIABLE, nodes.get(0).getLeftType());
    assertEquals("a.metric", nodes.get(0).getLeft());
    assertEquals(OperandType.VARIABLE, nodes.get(0).getRightType());
    assertEquals("b.metric", nodes.get(0).getRight());
    assertEquals(ExpressionOp.MOD, nodes.get(0).getOperator());
    
    parser = new ExpressionParser(config("a.metric == b.metric"));
    nodes = parser.parse();
    assertEquals(1, nodes.size());
    assertEquals("e1", nodes.get(0).getId());
    assertEquals("e1", nodes.get(0).getAs());
    assertEquals(OperandType.VARIABLE, nodes.get(0).getLeftType());
    assertEquals("a.metric", nodes.get(0).getLeft());
    assertEquals(OperandType.VARIABLE, nodes.get(0).getRightType());
    assertEquals("b.metric", nodes.get(0).getRight());
    assertEquals(ExpressionOp.EQ, nodes.get(0).getOperator());
    
    parser = new ExpressionParser(config("a.metric != b.metric"));
    nodes = parser.parse();
    assertEquals(1, nodes.size());
    assertEquals("e1", nodes.get(0).getId());
    assertEquals("e1", nodes.get(0).getAs());
    assertEquals(OperandType.VARIABLE, nodes.get(0).getLeftType());
    assertEquals("a.metric", nodes.get(0).getLeft());
    assertEquals(OperandType.VARIABLE, nodes.get(0).getRightType());
    assertEquals("b.metric", nodes.get(0).getRight());
    assertEquals(ExpressionOp.NE, nodes.get(0).getOperator());
    
    parser = new ExpressionParser(config("a.metric > b.metric"));
    nodes = parser.parse();
    assertEquals(1, nodes.size());
    assertEquals("e1", nodes.get(0).getId());
    assertEquals("e1", nodes.get(0).getAs());
    assertEquals(OperandType.VARIABLE, nodes.get(0).getLeftType());
    assertEquals("a.metric", nodes.get(0).getLeft());
    assertEquals(OperandType.VARIABLE, nodes.get(0).getRightType());
    assertEquals("b.metric", nodes.get(0).getRight());
    assertEquals(ExpressionOp.GT, nodes.get(0).getOperator());
    
    parser = new ExpressionParser(config("a.metric < b.metric"));
    nodes = parser.parse();
    assertEquals(1, nodes.size());
    assertEquals("e1", nodes.get(0).getId());
    assertEquals("e1", nodes.get(0).getAs());
    assertEquals(OperandType.VARIABLE, nodes.get(0).getLeftType());
    assertEquals("a.metric", nodes.get(0).getLeft());
    assertEquals(OperandType.VARIABLE, nodes.get(0).getRightType());
    assertEquals("b.metric", nodes.get(0).getRight());
    assertEquals(ExpressionOp.LT, nodes.get(0).getOperator());
    
    parser = new ExpressionParser(config("a.metric >= b.metric"));
    nodes = parser.parse();
    assertEquals(1, nodes.size());
    assertEquals("e1", nodes.get(0).getId());
    assertEquals("e1", nodes.get(0).getAs());
    assertEquals(OperandType.VARIABLE, nodes.get(0).getLeftType());
    assertEquals("a.metric", nodes.get(0).getLeft());
    assertEquals(OperandType.VARIABLE, nodes.get(0).getRightType());
    assertEquals("b.metric", nodes.get(0).getRight());
    assertEquals(ExpressionOp.GE, nodes.get(0).getOperator());
    
    parser = new ExpressionParser(config("a.metric <= b.metric"));
    nodes = parser.parse();
    assertEquals(1, nodes.size());
    assertEquals("e1", nodes.get(0).getId());
    assertEquals("e1", nodes.get(0).getAs());
    assertEquals(OperandType.VARIABLE, nodes.get(0).getLeftType());
    assertEquals("a.metric", nodes.get(0).getLeft());
    assertEquals(OperandType.VARIABLE, nodes.get(0).getRightType());
    assertEquals("b.metric", nodes.get(0).getRight());
    assertEquals(ExpressionOp.LE, nodes.get(0).getOperator());
  }

  @Test
  public void parseBinaryTwoBranches() throws Exception {
    ExpressionParser parser = 
        new ExpressionParser(config("a.metric + b.metric + c.metric"));
    List<ExpressionParseNode> nodes = parser.parse();
    assertEquals(2, nodes.size());
    
    assertEquals("e1_SubExp#0", nodes.get(0).getId());
    assertEquals("e1_SubExp#0", nodes.get(0).getAs());
    assertEquals(OperandType.VARIABLE, nodes.get(0).getLeftType());
    assertEquals("a.metric", nodes.get(0).getLeft());
    assertEquals(OperandType.VARIABLE, nodes.get(0).getRightType());
    assertEquals("b.metric", nodes.get(0).getRight());
    assertEquals(ExpressionOp.ADD, nodes.get(0).getOperator());
    
    assertEquals("e1", nodes.get(1).getId());
    assertEquals("e1", nodes.get(1).getAs());
    assertEquals(OperandType.SUB_EXP, nodes.get(1).getLeftType());
    assertEquals("e1_SubExp#0", nodes.get(1).getLeft());
    assertEquals(OperandType.VARIABLE, nodes.get(1).getRightType());
    assertEquals("c.metric", nodes.get(1).getRight());
    assertEquals(ExpressionOp.ADD, nodes.get(1).getOperator());
    
    // change order of precedence
    parser = new ExpressionParser(config("a.metric + (b.metric + c.metric)"));
    nodes = parser.parse();
    assertEquals(2, nodes.size());
    
    assertEquals("e1_SubExp#0", nodes.get(0).getId());
    assertEquals("e1_SubExp#0", nodes.get(0).getAs());
    assertEquals(OperandType.VARIABLE, nodes.get(0).getLeftType());
    assertEquals("b.metric", nodes.get(0).getLeft());
    assertEquals(OperandType.VARIABLE, nodes.get(0).getRightType());
    assertEquals("c.metric", nodes.get(0).getRight());
    assertEquals(ExpressionOp.ADD, nodes.get(0).getOperator());
    
    assertEquals("e1", nodes.get(1).getId());
    assertEquals("e1", nodes.get(1).getAs());
    assertEquals(OperandType.VARIABLE, nodes.get(1).getLeftType());
    assertEquals("a.metric", nodes.get(1).getLeft());
    assertEquals(OperandType.SUB_EXP, nodes.get(1).getRightType());
    assertEquals("e1_SubExp#0", nodes.get(1).getRight());
    assertEquals(ExpressionOp.ADD, nodes.get(1).getOperator());
    
    // numeric squashing, test all operators
    parser = new ExpressionParser(config("a.metric + (42 + 2)"));
    nodes = parser.parse();
    assertEquals(1, nodes.size());
    assertEquals(44L, ((NumericLiteral) nodes.get(0).getRight()).longValue());
    
    parser = new ExpressionParser(config("a.metric + (42 - 2)"));
    nodes = parser.parse();
    assertEquals(1, nodes.size());
    assertEquals(40L, ((NumericLiteral) nodes.get(0).getRight()).longValue());
    
    parser = new ExpressionParser(config("a.metric + (42 * 2)"));
    nodes = parser.parse();
    assertEquals(1, nodes.size());
    assertEquals(84, ((NumericLiteral) nodes.get(0).getRight()).longValue());
    
    parser = new ExpressionParser(config("a.metric + (42 / 2)"));
    nodes = parser.parse();
    assertEquals(1, nodes.size());
    assertEquals(21, ((NumericLiteral) nodes.get(0).getRight()).longValue());
    
    // to double
    parser = new ExpressionParser(config("a.metric + (42 / 5)"));
    nodes = parser.parse();
    assertEquals(1, nodes.size());
    assertEquals(8.4, ((NumericLiteral) nodes.get(0).getRight()).doubleValue(), 0.001);
    
    parser = new ExpressionParser(config("a.metric + (42 % 2)"));
    nodes = parser.parse();
    assertEquals(1, nodes.size());
    assertEquals(0, ((NumericLiteral) nodes.get(0).getRight()).longValue());
    
    // doubles
    parser = new ExpressionParser(config("a.metric + (42.5 + 2)"));
    nodes = parser.parse();
    assertEquals(1, nodes.size());
    assertEquals(44.5, ((NumericLiteral) nodes.get(0).getRight()).doubleValue(), 0.001);
    
    parser = new ExpressionParser(config("a.metric + (42.5 - 2)"));
    nodes = parser.parse();
    assertEquals(1, nodes.size());
    assertEquals(40.5, ((NumericLiteral) nodes.get(0).getRight()).doubleValue(), 0.001);
    
    parser = new ExpressionParser(config("a.metric + (42.5 * 2)"));
    nodes = parser.parse();
    assertEquals(1, nodes.size());
    assertEquals(85, ((NumericLiteral) nodes.get(0).getRight()).doubleValue(), 0.001);
    
    parser = new ExpressionParser(config("a.metric + (42.5 / 2)"));
    nodes = parser.parse();
    assertEquals(1, nodes.size());
    assertEquals(21.25, ((NumericLiteral) nodes.get(0).getRight()).doubleValue(), 0.001);
    
    parser = new ExpressionParser(config("a.metric + (42.5 % 2)"));
    nodes = parser.parse();
    assertEquals(1, nodes.size());
    assertEquals(0.0, ((NumericLiteral) nodes.get(0).getRight()).doubleValue(), 0.001);
  }
  
  @Test
  public void parseBinaryRelational() throws Exception {
    ExpressionParser parser = new ExpressionParser(
        config("a.metric == 42"));
    List<ExpressionParseNode> nodes = parser.parse();
    assertEquals(1, nodes.size());
    assertEquals("e1", nodes.get(0).getId());
    assertEquals(OperandType.VARIABLE, nodes.get(0).getLeftType());
    assertEquals("a.metric", nodes.get(0).getLeft());
    assertEquals(OperandType.LITERAL_NUMERIC, nodes.get(0).getRightType());
    assertEquals(42, ((NumericLiteral) nodes.get(0).getRight()).longValue());
    assertEquals(ExpressionOp.EQ, nodes.get(0).getOperator());
    
    parser = new ExpressionParser(config("a.metric != 42"));
    nodes = parser.parse();
    assertEquals(1, nodes.size());
    assertEquals(42, ((NumericLiteral) nodes.get(0).getRight()).longValue());
    assertEquals(ExpressionOp.NE, nodes.get(0).getOperator());
    
    parser = new ExpressionParser(config("a.metric > 42"));
    nodes = parser.parse();
    assertEquals(1, nodes.size());
    assertEquals(42, ((NumericLiteral) nodes.get(0).getRight()).longValue());
    assertEquals(ExpressionOp.GT, nodes.get(0).getOperator());
    
    parser = new ExpressionParser(config("a.metric < 42"));
    nodes = parser.parse();
    assertEquals(1, nodes.size());
    assertEquals(42, ((NumericLiteral) nodes.get(0).getRight()).longValue());
    assertEquals(ExpressionOp.LT, nodes.get(0).getOperator());
    
    parser = new ExpressionParser(config("a.metric >= 42"));
    nodes = parser.parse();
    assertEquals(1, nodes.size());
    assertEquals(42, ((NumericLiteral) nodes.get(0).getRight()).longValue());
    assertEquals(ExpressionOp.GE, nodes.get(0).getOperator());
    
    parser = new ExpressionParser(config("a.metric <= 42"));
    nodes = parser.parse();
    assertEquals(1, nodes.size());
    assertEquals(42, ((NumericLiteral) nodes.get(0).getRight()).longValue());
    assertEquals(ExpressionOp.LE, nodes.get(0).getOperator());
    
    // check negative numbers
    parser = new ExpressionParser(config("a.metric <= -42"));
    nodes = parser.parse();
    assertEquals(1, nodes.size());
    assertEquals(-42, ((NumericLiteral) nodes.get(0).getRight()).longValue());
    assertEquals(ExpressionOp.LE, nodes.get(0).getOperator());
    
    parser = new ExpressionParser(config("a.metric <= -42.75"));
    nodes = parser.parse();
    assertEquals(1, nodes.size());
    assertEquals(-42.75, ((NumericLiteral) nodes.get(0).getRight()).doubleValue(), 0.001);
    assertEquals(ExpressionOp.LE, nodes.get(0).getOperator());
  }
  
  @Test
  public void parseLogicalRelational() throws Exception {
    ExpressionParser parser = new ExpressionParser(
        config("a.metric > 0 && b.metric > 0"));
    List<ExpressionParseNode> nodes = parser.parse();
    assertEquals(3, nodes.size());
    assertEquals("e1_SubExp#0", nodes.get(0).getId());
    assertEquals(OperandType.VARIABLE, nodes.get(0).getLeftType());
    assertEquals("a.metric", nodes.get(0).getLeft());
    assertEquals(OperandType.LITERAL_NUMERIC, nodes.get(0).getRightType());
    assertEquals(0, ((NumericLiteral) nodes.get(0).getRight()).longValue());
    assertEquals(ExpressionOp.GT, nodes.get(0).getOperator());
    
    assertEquals("e1_SubExp#1", nodes.get(1).getId());
    assertEquals(OperandType.VARIABLE, nodes.get(1).getLeftType());
    assertEquals("b.metric", nodes.get(1).getLeft());
    assertEquals(OperandType.LITERAL_NUMERIC, nodes.get(1).getRightType());
    assertEquals(0, ((NumericLiteral) nodes.get(1).getRight()).longValue());
    assertEquals(ExpressionOp.GT, nodes.get(1).getOperator());
    
    assertEquals("e1", nodes.get(2).getId());
    assertEquals(OperandType.SUB_EXP, nodes.get(2).getLeftType());
    assertEquals("e1_SubExp#0", nodes.get(2).getLeft());
    assertEquals(OperandType.SUB_EXP, nodes.get(2).getRightType());
    assertEquals("e1_SubExp#1", nodes.get(2).getRight());
    assertEquals(ExpressionOp.AND, nodes.get(2).getOperator());

    // TODO - fix this parsing
//    parser = new ExpressionParser("a.metric > 0 AND b.metric > 0", "e1");
//    nodes = parser.parse();
//    assertEquals(3, nodes.size());
//    assertEquals("e1", nodes.get(2).getId());
//    assertEquals(OperandType.SUB_EXP, nodes.get(2).leftType());
//    assertEquals("e1_SubExp#0", nodes.get(2).left());
//    assertEquals(OperandType.SUB_EXP, nodes.get(2).rightType());
//    assertEquals("e1_SubExp#1", nodes.get(2).right());
//    assertEquals(ExpressionOp.OR, nodes.get(2).operator());
    
    parser = new ExpressionParser(config("a.metric > 0 || b.metric > 0"));
    nodes = parser.parse();
    assertEquals(3, nodes.size());
    assertEquals("e1_SubExp#0", nodes.get(0).getId());
    assertEquals(OperandType.VARIABLE, nodes.get(0).getLeftType());
    assertEquals("a.metric", nodes.get(0).getLeft());
    assertEquals(OperandType.LITERAL_NUMERIC, nodes.get(0).getRightType());
    assertEquals(0, ((NumericLiteral) nodes.get(0).getRight()).longValue());
    assertEquals(ExpressionOp.GT, nodes.get(0).getOperator());
    
    assertEquals("e1_SubExp#1", nodes.get(1).getId());
    assertEquals(OperandType.VARIABLE, nodes.get(1).getLeftType());
    assertEquals("b.metric", nodes.get(1).getLeft());
    assertEquals(OperandType.LITERAL_NUMERIC, nodes.get(1).getRightType());
    assertEquals(0, ((NumericLiteral) nodes.get(1).getRight()).longValue());
    assertEquals(ExpressionOp.GT, nodes.get(1).getOperator());
    
    assertEquals("e1", nodes.get(2).getId());
    assertEquals(OperandType.SUB_EXP, nodes.get(2).getLeftType());
    assertEquals("e1_SubExp#0", nodes.get(2).getLeft());
    assertEquals(OperandType.SUB_EXP, nodes.get(2).getRightType());
    assertEquals("e1_SubExp#1", nodes.get(2).getRight());
    assertEquals(ExpressionOp.OR, nodes.get(2).getOperator());

    // TODO - fix this parsing
//    parser = new ExpressionParser("a.metric > 0 OR b.metric > 0", "e1");
//    nodes = parser.parse();
//    assertEquals(3, nodes.size());
//    assertEquals("e1", nodes.get(2).getId());
//    assertEquals(OperandType.SUB_EXP, nodes.get(2).leftType());
//    assertEquals("e1_SubExp#0", nodes.get(2).left());
//    assertEquals(OperandType.SUB_EXP, nodes.get(2).rightType());
//    assertEquals("e1_SubExp#1", nodes.get(2).right());
//    assertEquals(ExpressionOp.OR, nodes.get(2).operator());
  }
  
  @Test
  public void parseNot() throws Exception {
    // explicit
    ExpressionParser parser = new ExpressionParser(
        config("a.metric > 0 && !(b.metric > 0)"));
    List<ExpressionParseNode> nodes = parser.parse();
    assertEquals(3, nodes.size());
    assertEquals("e1_SubExp#0", nodes.get(0).getId());
    assertEquals(OperandType.VARIABLE, nodes.get(0).getLeftType());
    assertEquals("a.metric", nodes.get(0).getLeft());
    assertEquals(OperandType.LITERAL_NUMERIC, nodes.get(0).getRightType());
    assertEquals(0, ((NumericLiteral) nodes.get(0).getRight()).longValue());
    assertEquals(ExpressionOp.GT, nodes.get(0).getOperator());
    
    assertEquals("e1_SubExp#1", nodes.get(1).getId());
    assertEquals(OperandType.VARIABLE, nodes.get(1).getLeftType());
    assertEquals("b.metric", nodes.get(1).getLeft());
    assertEquals(OperandType.LITERAL_NUMERIC, nodes.get(1).getRightType());
    assertEquals(0, ((NumericLiteral) nodes.get(1).getRight()).longValue());
    assertEquals(ExpressionOp.GT, nodes.get(1).getOperator());
    assertTrue(nodes.get(1).getNot());
    
    assertEquals("e1", nodes.get(2).getId());
    assertEquals(OperandType.SUB_EXP, nodes.get(2).getLeftType());
    assertEquals("e1_SubExp#0", nodes.get(2).getLeft());
    assertEquals(OperandType.SUB_EXP, nodes.get(2).getRightType());
    assertEquals("e1_SubExp#1", nodes.get(2).getRight());
    assertEquals(ExpressionOp.AND, nodes.get(2).getOperator());
    
    // implicit
    parser = new ExpressionParser(config("a.metric > 0 && !b.metric > 0"));
    nodes = parser.parse();
    assertEquals(3, nodes.size());
    assertEquals("e1_SubExp#0", nodes.get(0).getId());
    assertEquals(OperandType.VARIABLE, nodes.get(0).getLeftType());
    assertEquals("a.metric", nodes.get(0).getLeft());
    assertEquals(OperandType.LITERAL_NUMERIC, nodes.get(0).getRightType());
    assertEquals(0, ((NumericLiteral) nodes.get(0).getRight()).longValue());
    assertEquals(ExpressionOp.GT, nodes.get(0).getOperator());
    
    assertEquals("e1_SubExp#1", nodes.get(1).getId());
    assertEquals(OperandType.VARIABLE, nodes.get(1).getLeftType());
    assertEquals("b.metric", nodes.get(1).getLeft());
    assertEquals(OperandType.LITERAL_NUMERIC, nodes.get(1).getRightType());
    assertEquals(0, ((NumericLiteral) nodes.get(1).getRight()).longValue());
    assertEquals(ExpressionOp.GT, nodes.get(1).getOperator());
    assertTrue(nodes.get(1).getNot());
    
    assertEquals("e1", nodes.get(2).getId());
    assertEquals(OperandType.SUB_EXP, nodes.get(2).getLeftType());
    assertEquals("e1_SubExp#0", nodes.get(2).getLeft());
    assertEquals(OperandType.SUB_EXP, nodes.get(2).getRightType());
    assertEquals("e1_SubExp#1", nodes.get(2).getRight());
    assertEquals(ExpressionOp.AND, nodes.get(2).getOperator());
  }
  
  @Test
  public void parseFailures() throws Exception {
    // numeric OP numeric not allowed
    try {
      new ExpressionParser(config("42 * 1")).parse();
      fail("Expected ParseCancellationException");
    } catch (ParseCancellationException e) { }
    
    // nor single variables
    try {
      new ExpressionParser(config("a")).parse();
      fail("Expected ParseCancellationException");
    } catch (ParseCancellationException e) { }
    
    // logical on raw metrics, nope.
    try {
      new ExpressionParser(config("a && b")).parse();
      fail("Expected ParseCancellationException");
    } catch (ParseCancellationException e) { }
    
    // reltional on two numerics?
    try {
      new ExpressionParser(config("a.metric + (42 > 2)")).parse();
      fail("Expected ParseCancellationException");
    } catch (ParseCancellationException e) { }
    
    try {
      new ExpressionParser(config("-a.metric * 1")).parse();
      fail("Expected ParseCancellationException");
    } catch (ParseCancellationException e) { }
  }

  @Test
  public void parseVariables() throws Exception {
    Set<String> variables = ExpressionParser.parseVariables("metric.a + metric.b");
    assertEquals(2, variables.size());
    assertTrue(variables.contains("metric.a"));
    assertTrue(variables.contains("metric.b"));
    
    variables = ExpressionParser.parseVariables("metric.a + metric.b + foo.c");
    assertEquals(3, variables.size());
    assertTrue(variables.contains("metric.a"));
    assertTrue(variables.contains("metric.b"));
    assertTrue(variables.contains("foo.c"));
    
    variables = ExpressionParser.parseVariables("(a + b + c) / (a + b)");
    assertEquals(3, variables.size());
    assertTrue(variables.contains("a"));
    assertTrue(variables.contains("b"));
    assertTrue(variables.contains("c"));
    
    variables = ExpressionParser.parseVariables("(a + 1 + c) / (a * 42)");
    assertEquals(2, variables.size());
    assertTrue(variables.contains("a"));
    assertTrue(variables.contains("c"));
    
    try {
      ExpressionParser.parseVariables("a");
      fail("Expected ParseCancellationException");
    } catch (ParseCancellationException e) { }
    
  }
  
  ExpressionConfig config(final String exp) {
    return (ExpressionConfig) ExpressionConfig.newBuilder()
      .setExpression(exp)
      .setJoinConfig(JOIN_CONFIG)
      .setAs("my.new.metric")
      .addInterpolatorConfig(NUMERIC_CONFIG)
      .setId("e1")
      .build();
  }
}
