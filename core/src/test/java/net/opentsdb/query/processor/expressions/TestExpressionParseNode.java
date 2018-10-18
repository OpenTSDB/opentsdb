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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.reflect.TypeToken;

import net.opentsdb.core.MockTSDB;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.DefaultInterpolatorFactory;
import net.opentsdb.query.interpolation.QueryInterpolatorFactory;
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
  public void deserialize() throws Exception {
    DefaultInterpolatorFactory factory = new DefaultInterpolatorFactory();
    MockTSDB tsdb = new MockTSDB();
    factory.initialize(tsdb, null).join();
    when(tsdb.getRegistry().getPlugin(eq(QueryInterpolatorFactory.class), anyString()))
      .thenReturn(factory);
    when(tsdb.getRegistry().getType(anyString())).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return NumericType.TYPE;
      }
    });
    
    final String json = "{\"id\":\"exp\",\"left\":\"a\",\"right\":\"42\","
        + "\"negate\":false,\"not\":true,\"leftType\":\"VARIABLE\","
        + "\"rightType\":\"LITERAL_NUMERIC\",\"operator\":\"MOD\","
        + "\"expressionConfig\":{\"id\":\"e1\",\"expression\":\"a + 42\","
        + "\"as\":\"some.metric.name\",\"join\":{\"id\":\"jc\","
        + "\"joinType\":\"INNER\",\"joins\":{\"host\":\"host\"},"
        + "\"explicitTags\":false},\"variableInterpolators\":{\"a\":["
        + "{\"fillPolicy\":\"nan\",\"realFillPolicy\":\"PREFER_NEXT\","
        + "\"dataType\":\"net.opentsdb.data.types.numeric.NumericType\"}]},"
        + "\"infectiousNan\":false,\"interpolatorConfigs\":"
        + "[{\"fillPolicy\":\"nan\",\"realFillPolicy\":\"PREFER_NEXT\","
        + "\"dataType\":\"net.opentsdb.data.types.numeric.NumericType\"}]}}\n";
    
    JsonNode jn = JSON.getMapper().readTree(json);
    ExpressionParseNode node = (ExpressionParseNode) 
        new BinaryExpressionNodeFactory()
          .parseConfig(JSON.getMapper(), tsdb, jn);
    
    assertEquals("a", node.getLeft());
    assertEquals(OperandType.VARIABLE, node.getLeftType());
    assertEquals(42, ((NumericLiteral) node.getRight()).longValue());
    assertEquals(OperandType.LITERAL_NUMERIC, node.getRightType());
    assertEquals(ExpressionOp.MOD, node.getOperator());
    assertFalse(node.getNegate());
    assertTrue(node.getNot());
    assertEquals("a + 42", node.getExpressionConfig().getExpression());
  }
  
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
  public void serialize() throws Exception {
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
        .setRight("42")
        .setRightType(OperandType.LITERAL_NUMERIC)
        .setExpressionOp(ExpressionOp.MOD)
        .setExpressionConfig(config)
        .setId("expression")
        .build();
    
    final String json = JSON.serializeToString(node);
    assertTrue(json.contains("\"left\":\"a\""));
    assertTrue(json.contains("\"right\":\"42\""));
    assertTrue(json.contains("\"negate\":false,\"not\""));
    assertTrue(json.contains("\"not\":false"));
    assertTrue(json.contains("\"leftType\":\"VARIABLE\""));
    assertTrue(json.contains("\"rightType\":\"LITERAL_NUMERIC\""));
    assertTrue(json.contains("\"operator\":\"MOD\""));
    assertTrue(json.contains("\"expressionConfig\":{"));
    assertTrue(json.contains("\"expression\":\"a + 42\""));
  }
}
