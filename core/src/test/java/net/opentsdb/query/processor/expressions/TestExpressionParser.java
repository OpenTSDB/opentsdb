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

import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.junit.Test;

import net.opentsdb.query.processor.expressions.ExpressionParseNode.ExpressionOp;
import net.opentsdb.query.processor.expressions.ExpressionParseNode.OperandType;
import net.opentsdb.query.processor.expressions.ExpressionParser.NumericLiteral;

public class TestExpressionParser {

  @Test
  public void parseBinaryOperators() throws Exception {
    ExpressionParser parser = new ExpressionParser("a.metric + b.metric", "e1");
    List<ExpressionParseNode> nodes = parser.parse();
    assertEquals(1, nodes.size());
    assertEquals("e1", nodes.get(0).getId());
    assertEquals(OperandType.VARIABLE, nodes.get(0).leftType());
    assertEquals("a.metric", nodes.get(0).left());
    assertEquals(OperandType.VARIABLE, nodes.get(0).rightType());
    assertEquals("b.metric", nodes.get(0).right());
    assertEquals(ExpressionOp.ADD, nodes.get(0).operator());
    
    parser = new ExpressionParser("a.metric - b.metric", "e1");
    nodes = parser.parse();
    assertEquals(1, nodes.size());
    assertEquals("e1", nodes.get(0).getId());
    assertEquals(OperandType.VARIABLE, nodes.get(0).leftType());
    assertEquals("a.metric", nodes.get(0).left());
    assertEquals(OperandType.VARIABLE, nodes.get(0).rightType());
    assertEquals("b.metric", nodes.get(0).right());
    assertEquals(ExpressionOp.SUBTRACT, nodes.get(0).operator());
    
    parser = new ExpressionParser("a.metric * b.metric", "e1");
    nodes = parser.parse();
    assertEquals(1, nodes.size());
    assertEquals("e1", nodes.get(0).getId());
    assertEquals(OperandType.VARIABLE, nodes.get(0).leftType());
    assertEquals("a.metric", nodes.get(0).left());
    assertEquals(OperandType.VARIABLE, nodes.get(0).rightType());
    assertEquals("b.metric", nodes.get(0).right());
    assertEquals(ExpressionOp.MULTIPLY, nodes.get(0).operator());
    
    parser = new ExpressionParser("a.metric / b.metric", "e1");
    nodes = parser.parse();
    assertEquals(1, nodes.size());
    assertEquals("e1", nodes.get(0).getId());
    assertEquals(OperandType.VARIABLE, nodes.get(0).leftType());
    assertEquals("a.metric", nodes.get(0).left());
    assertEquals(OperandType.VARIABLE, nodes.get(0).rightType());
    assertEquals("b.metric", nodes.get(0).right());
    assertEquals(ExpressionOp.DIVIDE, nodes.get(0).operator());
    
    parser = new ExpressionParser("a.metric % b.metric", "e1");
    nodes = parser.parse();
    assertEquals(1, nodes.size());
    assertEquals("e1", nodes.get(0).getId());
    assertEquals(OperandType.VARIABLE, nodes.get(0).leftType());
    assertEquals("a.metric", nodes.get(0).left());
    assertEquals(OperandType.VARIABLE, nodes.get(0).rightType());
    assertEquals("b.metric", nodes.get(0).right());
    assertEquals(ExpressionOp.MOD, nodes.get(0).operator());
    
    parser = new ExpressionParser("a.metric == b.metric", "e1");
    nodes = parser.parse();
    assertEquals(1, nodes.size());
    assertEquals("e1", nodes.get(0).getId());
    assertEquals(OperandType.VARIABLE, nodes.get(0).leftType());
    assertEquals("a.metric", nodes.get(0).left());
    assertEquals(OperandType.VARIABLE, nodes.get(0).rightType());
    assertEquals("b.metric", nodes.get(0).right());
    assertEquals(ExpressionOp.EQ, nodes.get(0).operator());
    
    parser = new ExpressionParser("a.metric != b.metric", "e1");
    nodes = parser.parse();
    assertEquals(1, nodes.size());
    assertEquals("e1", nodes.get(0).getId());
    assertEquals(OperandType.VARIABLE, nodes.get(0).leftType());
    assertEquals("a.metric", nodes.get(0).left());
    assertEquals(OperandType.VARIABLE, nodes.get(0).rightType());
    assertEquals("b.metric", nodes.get(0).right());
    assertEquals(ExpressionOp.NE, nodes.get(0).operator());
    
    parser = new ExpressionParser("a.metric > b.metric", "e1");
    nodes = parser.parse();
    assertEquals(1, nodes.size());
    assertEquals("e1", nodes.get(0).getId());
    assertEquals(OperandType.VARIABLE, nodes.get(0).leftType());
    assertEquals("a.metric", nodes.get(0).left());
    assertEquals(OperandType.VARIABLE, nodes.get(0).rightType());
    assertEquals("b.metric", nodes.get(0).right());
    assertEquals(ExpressionOp.GT, nodes.get(0).operator());
    
    parser = new ExpressionParser("a.metric < b.metric", "e1");
    nodes = parser.parse();
    assertEquals(1, nodes.size());
    assertEquals("e1", nodes.get(0).getId());
    assertEquals(OperandType.VARIABLE, nodes.get(0).leftType());
    assertEquals("a.metric", nodes.get(0).left());
    assertEquals(OperandType.VARIABLE, nodes.get(0).rightType());
    assertEquals("b.metric", nodes.get(0).right());
    assertEquals(ExpressionOp.LT, nodes.get(0).operator());
    
    parser = new ExpressionParser("a.metric >= b.metric", "e1");
    nodes = parser.parse();
    assertEquals(1, nodes.size());
    assertEquals("e1", nodes.get(0).getId());
    assertEquals(OperandType.VARIABLE, nodes.get(0).leftType());
    assertEquals("a.metric", nodes.get(0).left());
    assertEquals(OperandType.VARIABLE, nodes.get(0).rightType());
    assertEquals("b.metric", nodes.get(0).right());
    assertEquals(ExpressionOp.GE, nodes.get(0).operator());
    
    parser = new ExpressionParser("a.metric <= b.metric", "e1");
    nodes = parser.parse();
    assertEquals(1, nodes.size());
    assertEquals("e1", nodes.get(0).getId());
    assertEquals(OperandType.VARIABLE, nodes.get(0).leftType());
    assertEquals("a.metric", nodes.get(0).left());
    assertEquals(OperandType.VARIABLE, nodes.get(0).rightType());
    assertEquals("b.metric", nodes.get(0).right());
    assertEquals(ExpressionOp.LE, nodes.get(0).operator());
  }

  @Test
  public void parseBinaryTwoBranches() throws Exception {
    ExpressionParser parser = new ExpressionParser(
        "a.metric + b.metric + c.metric", "e1");
    List<ExpressionParseNode> nodes = parser.parse();
    assertEquals(2, nodes.size());
    
    assertEquals("e1_SubExp#0", nodes.get(0).getId());
    assertEquals(OperandType.VARIABLE, nodes.get(0).leftType());
    assertEquals("a.metric", nodes.get(0).left());
    assertEquals(OperandType.VARIABLE, nodes.get(0).rightType());
    assertEquals("b.metric", nodes.get(0).right());
    assertEquals(ExpressionOp.ADD, nodes.get(0).operator());
    
    assertEquals("e1", nodes.get(1).getId());
    assertEquals(OperandType.SUB_EXP, nodes.get(1).leftType());
    assertEquals("e1_SubExp#0", nodes.get(1).left());
    assertEquals(OperandType.VARIABLE, nodes.get(1).rightType());
    assertEquals("c.metric", nodes.get(1).right());
    assertEquals(ExpressionOp.ADD, nodes.get(1).operator());
    
    // change order of precedence
    parser = new ExpressionParser("a.metric + (b.metric + c.metric)", "e1");
    nodes = parser.parse();
    assertEquals(2, nodes.size());
    
    assertEquals("e1_SubExp#0", nodes.get(0).getId());
    assertEquals(OperandType.VARIABLE, nodes.get(0).leftType());
    assertEquals("b.metric", nodes.get(0).left());
    assertEquals(OperandType.VARIABLE, nodes.get(0).rightType());
    assertEquals("c.metric", nodes.get(0).right());
    assertEquals(ExpressionOp.ADD, nodes.get(0).operator());
    
    assertEquals("e1", nodes.get(1).getId());
    assertEquals(OperandType.VARIABLE, nodes.get(1).leftType());
    assertEquals("a.metric", nodes.get(1).left());
    assertEquals(OperandType.SUB_EXP, nodes.get(1).rightType());
    assertEquals("e1_SubExp#0", nodes.get(1).right());
    assertEquals(ExpressionOp.ADD, nodes.get(1).operator());
    
    // numeric squashing, test all operators
    parser = new ExpressionParser("a.metric + (42 + 2)", "e1");
    nodes = parser.parse();
    assertEquals(1, nodes.size());
    assertEquals(44L, ((NumericLiteral) nodes.get(0).right()).longValue());
    
    parser = new ExpressionParser("a.metric + (42 - 2)", "e1");
    nodes = parser.parse();
    assertEquals(1, nodes.size());
    assertEquals(40L, ((NumericLiteral) nodes.get(0).right()).longValue());
    
    parser = new ExpressionParser("a.metric + (42 * 2)", "e1");
    nodes = parser.parse();
    assertEquals(1, nodes.size());
    assertEquals(84, ((NumericLiteral) nodes.get(0).right()).longValue());
    
    parser = new ExpressionParser("a.metric + (42 / 2)", "e1");
    nodes = parser.parse();
    assertEquals(1, nodes.size());
    assertEquals(21, ((NumericLiteral) nodes.get(0).right()).longValue());
    
    // to double
    parser = new ExpressionParser("a.metric + (42 / 5)", "e1");
    nodes = parser.parse();
    assertEquals(1, nodes.size());
    assertEquals(8.4, ((NumericLiteral) nodes.get(0).right()).doubleValue(), 0.001);
    
    parser = new ExpressionParser("a.metric + (42 % 2)", "e1");
    nodes = parser.parse();
    assertEquals(1, nodes.size());
    assertEquals(0, ((NumericLiteral) nodes.get(0).right()).longValue());
    
    // doubles
    parser = new ExpressionParser("a.metric + (42.5 + 2)", "e1");
    nodes = parser.parse();
    assertEquals(1, nodes.size());
    assertEquals(44.5, ((NumericLiteral) nodes.get(0).right()).doubleValue(), 0.001);
    
    parser = new ExpressionParser("a.metric + (42.5 - 2)", "e1");
    nodes = parser.parse();
    assertEquals(1, nodes.size());
    assertEquals(40.5, ((NumericLiteral) nodes.get(0).right()).doubleValue(), 0.001);
    
    parser = new ExpressionParser("a.metric + (42.5 * 2)", "e1");
    nodes = parser.parse();
    assertEquals(1, nodes.size());
    assertEquals(85, ((NumericLiteral) nodes.get(0).right()).doubleValue(), 0.001);
    
    parser = new ExpressionParser("a.metric + (42.5 / 2)", "e1");
    nodes = parser.parse();
    assertEquals(1, nodes.size());
    assertEquals(21.25, ((NumericLiteral) nodes.get(0).right()).doubleValue(), 0.001);
    
    parser = new ExpressionParser("a.metric + (42.5 % 2)", "e1");
    nodes = parser.parse();
    assertEquals(1, nodes.size());
    assertEquals(0.0, ((NumericLiteral) nodes.get(0).right()).doubleValue(), 0.001);
  }
  
  @Test
  public void parseBinaryRelational() throws Exception {
    ExpressionParser parser = new ExpressionParser(
        "a.metric == 42", "e1");
    List<ExpressionParseNode> nodes = parser.parse();
    assertEquals(1, nodes.size());
    assertEquals("e1", nodes.get(0).getId());
    assertEquals(OperandType.VARIABLE, nodes.get(0).leftType());
    assertEquals("a.metric", nodes.get(0).left());
    assertEquals(OperandType.LITERAL_NUMERIC, nodes.get(0).rightType());
    assertEquals(42, ((NumericLiteral) nodes.get(0).right()).longValue());
    assertEquals(ExpressionOp.EQ, nodes.get(0).operator());
    
    parser = new ExpressionParser("a.metric != 42", "e1");
    nodes = parser.parse();
    assertEquals(1, nodes.size());
    assertEquals(42, ((NumericLiteral) nodes.get(0).right()).longValue());
    assertEquals(ExpressionOp.NE, nodes.get(0).operator());
    
    parser = new ExpressionParser("a.metric > 42", "e1");
    nodes = parser.parse();
    assertEquals(1, nodes.size());
    assertEquals(42, ((NumericLiteral) nodes.get(0).right()).longValue());
    assertEquals(ExpressionOp.GT, nodes.get(0).operator());
    
    parser = new ExpressionParser("a.metric < 42", "e1");
    nodes = parser.parse();
    assertEquals(1, nodes.size());
    assertEquals(42, ((NumericLiteral) nodes.get(0).right()).longValue());
    assertEquals(ExpressionOp.LT, nodes.get(0).operator());
    
    parser = new ExpressionParser("a.metric >= 42", "e1");
    nodes = parser.parse();
    assertEquals(1, nodes.size());
    assertEquals(42, ((NumericLiteral) nodes.get(0).right()).longValue());
    assertEquals(ExpressionOp.GE, nodes.get(0).operator());
    
    parser = new ExpressionParser("a.metric <= 42", "e1");
    nodes = parser.parse();
    assertEquals(1, nodes.size());
    assertEquals(42, ((NumericLiteral) nodes.get(0).right()).longValue());
    assertEquals(ExpressionOp.LE, nodes.get(0).operator());
    
    // check negative numbers
    parser = new ExpressionParser("a.metric <= -42", "e1");
    nodes = parser.parse();
    assertEquals(1, nodes.size());
    assertEquals(-42, ((NumericLiteral) nodes.get(0).right()).longValue());
    assertEquals(ExpressionOp.LE, nodes.get(0).operator());
    
    parser = new ExpressionParser("a.metric <= -42.75", "e1");
    nodes = parser.parse();
    assertEquals(1, nodes.size());
    assertEquals(-42.75, ((NumericLiteral) nodes.get(0).right()).doubleValue(), 0.001);
    assertEquals(ExpressionOp.LE, nodes.get(0).operator());
  }
  
  @Test
  public void parseLogicalRelational() throws Exception {
    ExpressionParser parser = new ExpressionParser(
        "a.metric > 0 && b.metric > 0", "e1");
    List<ExpressionParseNode> nodes = parser.parse();
    assertEquals(3, nodes.size());
    assertEquals("e1_SubExp#0", nodes.get(0).getId());
    assertEquals(OperandType.VARIABLE, nodes.get(0).leftType());
    assertEquals("a.metric", nodes.get(0).left());
    assertEquals(OperandType.LITERAL_NUMERIC, nodes.get(0).rightType());
    assertEquals(0, ((NumericLiteral) nodes.get(0).right()).longValue());
    assertEquals(ExpressionOp.GT, nodes.get(0).operator());
    
    assertEquals("e1_SubExp#1", nodes.get(1).getId());
    assertEquals(OperandType.VARIABLE, nodes.get(1).leftType());
    assertEquals("b.metric", nodes.get(1).left());
    assertEquals(OperandType.LITERAL_NUMERIC, nodes.get(1).rightType());
    assertEquals(0, ((NumericLiteral) nodes.get(1).right()).longValue());
    assertEquals(ExpressionOp.GT, nodes.get(1).operator());
    
    assertEquals("e1", nodes.get(2).getId());
    assertEquals(OperandType.SUB_EXP, nodes.get(2).leftType());
    assertEquals("e1_SubExp#0", nodes.get(2).left());
    assertEquals(OperandType.SUB_EXP, nodes.get(2).rightType());
    assertEquals("e1_SubExp#1", nodes.get(2).right());
    assertEquals(ExpressionOp.AND, nodes.get(2).operator());

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
    
    parser = new ExpressionParser("a.metric > 0 || b.metric > 0", "e1");
    nodes = parser.parse();
    assertEquals(3, nodes.size());
    assertEquals("e1_SubExp#0", nodes.get(0).getId());
    assertEquals(OperandType.VARIABLE, nodes.get(0).leftType());
    assertEquals("a.metric", nodes.get(0).left());
    assertEquals(OperandType.LITERAL_NUMERIC, nodes.get(0).rightType());
    assertEquals(0, ((NumericLiteral) nodes.get(0).right()).longValue());
    assertEquals(ExpressionOp.GT, nodes.get(0).operator());
    
    assertEquals("e1_SubExp#1", nodes.get(1).getId());
    assertEquals(OperandType.VARIABLE, nodes.get(1).leftType());
    assertEquals("b.metric", nodes.get(1).left());
    assertEquals(OperandType.LITERAL_NUMERIC, nodes.get(1).rightType());
    assertEquals(0, ((NumericLiteral) nodes.get(1).right()).longValue());
    assertEquals(ExpressionOp.GT, nodes.get(1).operator());
    
    assertEquals("e1", nodes.get(2).getId());
    assertEquals(OperandType.SUB_EXP, nodes.get(2).leftType());
    assertEquals("e1_SubExp#0", nodes.get(2).left());
    assertEquals(OperandType.SUB_EXP, nodes.get(2).rightType());
    assertEquals("e1_SubExp#1", nodes.get(2).right());
    assertEquals(ExpressionOp.OR, nodes.get(2).operator());

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
        "a.metric > 0 && !(b.metric > 0)", "e1");
    List<ExpressionParseNode> nodes = parser.parse();
    assertEquals(3, nodes.size());
    assertEquals("e1_SubExp#0", nodes.get(0).getId());
    assertEquals(OperandType.VARIABLE, nodes.get(0).leftType());
    assertEquals("a.metric", nodes.get(0).left());
    assertEquals(OperandType.LITERAL_NUMERIC, nodes.get(0).rightType());
    assertEquals(0, ((NumericLiteral) nodes.get(0).right()).longValue());
    assertEquals(ExpressionOp.GT, nodes.get(0).operator());
    
    assertEquals("e1_SubExp#1", nodes.get(1).getId());
    assertEquals(OperandType.VARIABLE, nodes.get(1).leftType());
    assertEquals("b.metric", nodes.get(1).left());
    assertEquals(OperandType.LITERAL_NUMERIC, nodes.get(1).rightType());
    assertEquals(0, ((NumericLiteral) nodes.get(1).right()).longValue());
    assertEquals(ExpressionOp.GT, nodes.get(1).operator());
    assertTrue(nodes.get(1).not());
    
    assertEquals("e1", nodes.get(2).getId());
    assertEquals(OperandType.SUB_EXP, nodes.get(2).leftType());
    assertEquals("e1_SubExp#0", nodes.get(2).left());
    assertEquals(OperandType.SUB_EXP, nodes.get(2).rightType());
    assertEquals("e1_SubExp#1", nodes.get(2).right());
    assertEquals(ExpressionOp.AND, nodes.get(2).operator());
    
    // implicit
    parser = new ExpressionParser(
        "a.metric > 0 && !b.metric > 0", "e1");
    nodes = parser.parse();
    assertEquals(3, nodes.size());
    assertEquals("e1_SubExp#0", nodes.get(0).getId());
    assertEquals(OperandType.VARIABLE, nodes.get(0).leftType());
    assertEquals("a.metric", nodes.get(0).left());
    assertEquals(OperandType.LITERAL_NUMERIC, nodes.get(0).rightType());
    assertEquals(0, ((NumericLiteral) nodes.get(0).right()).longValue());
    assertEquals(ExpressionOp.GT, nodes.get(0).operator());
    
    assertEquals("e1_SubExp#1", nodes.get(1).getId());
    assertEquals(OperandType.VARIABLE, nodes.get(1).leftType());
    assertEquals("b.metric", nodes.get(1).left());
    assertEquals(OperandType.LITERAL_NUMERIC, nodes.get(1).rightType());
    assertEquals(0, ((NumericLiteral) nodes.get(1).right()).longValue());
    assertEquals(ExpressionOp.GT, nodes.get(1).operator());
    assertTrue(nodes.get(1).not());
    
    assertEquals("e1", nodes.get(2).getId());
    assertEquals(OperandType.SUB_EXP, nodes.get(2).leftType());
    assertEquals("e1_SubExp#0", nodes.get(2).left());
    assertEquals(OperandType.SUB_EXP, nodes.get(2).rightType());
    assertEquals("e1_SubExp#1", nodes.get(2).right());
    assertEquals(ExpressionOp.AND, nodes.get(2).operator());
  }
  
  @Test
  public void parseFailures() throws Exception {
    // numeric OP numeric not allowed
    try {
      new ExpressionParser("42 * 1", "e1").parse();
      fail("Expected ParseCancellationException");
    } catch (ParseCancellationException e) { }
    
    // nor single variables
    try {
      new ExpressionParser("a", "e1").parse();
      fail("Expected ParseCancellationException");
    } catch (ParseCancellationException e) { }
    
    // logical on raw metrics, nope.
    try {
      new ExpressionParser("a && b", "e1").parse();
      fail("Expected ParseCancellationException");
    } catch (ParseCancellationException e) { }
    
    // reltional on two numerics?
    try {
      new ExpressionParser("a.metric + (42 > 2)", "e1").parse();
      fail("Expected ParseCancellationException");
    } catch (ParseCancellationException e) { }
    
    try {
      new ExpressionParser("-a.metric * 1", "e1").parse();
      fail("Expected ParseCancellationException");
    } catch (ParseCancellationException e) { }
  }
}
