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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;

import net.opentsdb.core.MockTSDB;
import net.opentsdb.core.MockTSDBDefault;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.joins.JoinConfig;
import net.opentsdb.query.joins.JoinConfig.JoinType;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.processor.expressions.ExpressionParseNode.ExpressionOp;
import net.opentsdb.query.processor.expressions.ExpressionParseNode.OperandType;
import net.opentsdb.query.processor.expressions.ExpressionParser.NumericLiteral;
import net.opentsdb.utils.JSON;

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
        .setExpressionConfig(mock(ExpressionConfig.class))
        .setId("expression")
        .build();
    assertEquals("a", node.getLeft());
    assertEquals(OperandType.VARIABLE, node.getLeftType());
    assertEquals("42", node.getRight());
    assertEquals(OperandType.LITERAL_NUMERIC, node.getRightType());
    assertEquals(ExpressionOp.MOD, node.getOperator());
    assertFalse(node.getNegate());
    assertFalse(node.getNot());
  }

  @Test
  public void serdes() throws Exception {
    NumericInterpolatorConfig numeric_config = 
        (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
      .setFillPolicy(FillPolicy.NOT_A_NUMBER)
      .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
      .setDataType(NumericType.TYPE.toString())
      .build();
    
    ExpressionConfig config = (ExpressionConfig) 
        ExpressionConfig.newBuilder()
          .setExpression("a + 42")
          .setJoinConfig((JoinConfig) JoinConfig.newBuilder()
              .addJoins("host", "host")
              .setJoinType(JoinType.INNER)
              .setId("jc")
              .build())
          .addVariableInterpolator("a", numeric_config)
          .setAs("some.metric.name")
          .addInterpolatorConfig(numeric_config)
          .setId("e1")
          .build();
    
    ExpressionParseNode node = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setLeftId("m1")
        .setRight("42")
        .setRightType(OperandType.LITERAL_NUMERIC)
        .setExpressionOp(ExpressionOp.MOD)
        .setExpressionConfig(config)
        .setAs("foo")
        .setNot(true)
        .setId("expression")
        .build();
    
    final String json = JSON.serializeToString(node);
    System.out.println(json);
    assertTrue(json.contains("\"left\":\"a\""));
    assertTrue(json.contains("\"right\":\"42\""));
    assertTrue(json.contains("\"negate\":false"));
    assertTrue(json.contains("\"not\":true"));
    assertTrue(json.contains("\"leftType\":\"VARIABLE\""));
    assertTrue(json.contains("\"rightType\":\"LITERAL_NUMERIC\""));
    assertTrue(json.contains("\"operator\":\"MOD\""));
    assertTrue(json.contains("\"expressionConfig\":{"));
    assertTrue(json.contains("\"expression\":\"a + 42\""));
    assertTrue(json.contains("\"leftId\":\"m1\""));
    assertFalse(json.contains("\"rightId\":"));
    assertTrue(json.contains("\"as\":\"foo\""));
    
    MockTSDB tsdb = MockTSDBDefault.getMockTSDB();
    JsonNode jn = JSON.getMapper().readTree(json);
    node = (ExpressionParseNode) new BinaryExpressionNodeFactory()
          .parseConfig(JSON.getMapper(), tsdb, jn);
    
    assertEquals("a", node.getLeft());
    assertEquals(OperandType.VARIABLE, node.getLeftType());
    assertEquals("m1", node.getLeftId());
    assertEquals(42, ((NumericLiteral) node.getRight()).longValue());
    assertEquals(OperandType.LITERAL_NUMERIC, node.getRightType());
    assertNull(node.getRightId());
    assertEquals(ExpressionOp.MOD, node.getOperator());
    assertFalse(node.getNegate());
    assertTrue(node.getNot());
    assertEquals("a + 42", node.getExpressionConfig().getExpression());
    assertEquals("foo", node.getAs());
  }
}
