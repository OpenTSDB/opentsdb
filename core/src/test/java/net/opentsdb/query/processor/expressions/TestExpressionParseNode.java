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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import org.junit.Test;

import net.opentsdb.query.processor.expressions.ExpressionParseNode.ExpressionOp;
import net.opentsdb.query.processor.expressions.ExpressionParseNode.OperandType;

public class TestExpressionParseNode {

  @Test
  public void expressionOp() throws Exception {
    assertEquals(ExpressionOp.OR, ExpressionOp.parse(" || "));
    assertEquals(ExpressionOp.OR, ExpressionOp.parse(" OR "));
    try {
      ExpressionOp.parse(" or ");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    try {
      ExpressionOp.parse("|");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    assertEquals(ExpressionOp.AND, ExpressionOp.parse(" &&"));
    assertEquals(ExpressionOp.AND, ExpressionOp.parse(" AND "));
    try {
      ExpressionOp.parse(" and ");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    try {
      ExpressionOp.parse("&");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    assertEquals(ExpressionOp.EQ, ExpressionOp.parse("=="));
    assertEquals(ExpressionOp.NE, ExpressionOp.parse("!="));
    assertEquals(ExpressionOp.LT, ExpressionOp.parse("<"));
    assertEquals(ExpressionOp.GT, ExpressionOp.parse(">"));
    assertEquals(ExpressionOp.LE, ExpressionOp.parse("<="));
    assertEquals(ExpressionOp.GE, ExpressionOp.parse(">="));
    assertEquals(ExpressionOp.ADD, ExpressionOp.parse("+"));
    assertEquals(ExpressionOp.SUBTRACT, ExpressionOp.parse("-"));
    assertEquals(ExpressionOp.MULTIPLY, ExpressionOp.parse("*"));
    assertEquals(ExpressionOp.DIVIDE, ExpressionOp.parse("/"));
    assertEquals(ExpressionOp.MOD, ExpressionOp.parse("%"));
    
    try {
      ExpressionOp.parse("!");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    try {
      ExpressionOp.parse("=");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    try {
      ExpressionOp.parse("^");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    try {
      ExpressionOp.parse(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    try {
      ExpressionOp.parse("");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    try {
      ExpressionOp.parse(" ");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void builder() throws Exception {
    ExpressionParseNode node = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight("42")
        .setRightType(OperandType.LITERAL_NUMERIC)
        .setExpressionOp(ExpressionOp.MOD)
        .build();
    assertEquals("a", node.left());
    assertEquals(OperandType.VARIABLE, node.leftType());
    assertEquals("42", node.right());
    assertEquals(OperandType.LITERAL_NUMERIC, node.rightType());
    assertEquals(ExpressionOp.MOD, node.operator());
    assertFalse(node.negate());
    assertFalse(node.not());
  }
}
