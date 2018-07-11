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
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.eq;
import static org.mockito.AdditionalMatchers.aryEq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import net.opentsdb.common.Const;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.joins.JoinConfig;
import net.opentsdb.query.joins.JoinConfig.JoinType;
import net.opentsdb.query.joins.Joiner;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.processor.expressions.ExpressionParseNode.ExpressionOp;
import net.opentsdb.query.processor.expressions.ExpressionParseNode.OperandType;
import net.opentsdb.utils.Pair;

public class TestExpressionResult {
  private static final byte[] LEFT = new byte[] { 0, 0, 1 };
  private static final byte[] RIGHT = new byte[] { 0, 0, 2 };
  
  private BinaryExpressionNode node;
  private Joiner joiner;
  private NumericInterpolatorConfig numeric_config;
  private ExpressionConfig config;
  private JoinConfig join_config;
  private ExpressionParseNode expression_config;
  
  @Before
  public void before() throws Exception {
    node = mock(BinaryExpressionNode.class);
    joiner = mock(Joiner.class);
    
    numeric_config = 
        (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
      .setFillPolicy(FillPolicy.NOT_A_NUMBER)
      .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
      .setType(NumericType.TYPE.toString())
      .build();
    
    join_config = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.INNER)
        .addJoins("host", "host")
        .setId("join")
        .build();
    
    config = (ExpressionConfig) ExpressionConfig.newBuilder()
        .setExpression("a + b + c")
        .setJoinConfig(join_config)
        .addInterpolatorConfig(numeric_config)
        .setId("e1")
        .build();
    
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight("b")
        .setRightType(OperandType.VARIABLE)
        .setExpressionOp(ExpressionOp.ADD)
        .build();
    
    when(node.config()).thenReturn(config);
    when(node.expressionConfig()).thenReturn(expression_config);
    when(node.joiner()).thenReturn(joiner);
  }
  
  @Test
  public void ctor() throws Exception {
    ExpressionResult result = new ExpressionResult(node);
    assertEquals(0, result.results.size());
  }
  
  @Test
  public void joinString() throws Exception {
    List<Pair<TimeSeries, TimeSeries>> joins = 
        Lists.newArrayList(new Pair<TimeSeries, TimeSeries>(mock(TimeSeries.class), mock(TimeSeries.class)));
    when(joiner.join(any(List.class), any(byte[].class), any(byte[].class), anyBoolean()))
      .thenReturn(joins);
    when(joiner.join(any(List.class), any(byte[].class), anyBoolean(), anyBoolean()))
      .thenReturn(joins);
    
    ExpressionResult result = new ExpressionResult(node);
    result.join();
    assertEquals(1, result.time_series.size());
    verify(joiner, times(1)).join(
        eq(result.results), 
        aryEq("a".getBytes(Const.UTF8_CHARSET)), 
        aryEq("b".getBytes(Const.UTF8_CHARSET)), 
        eq(false));
    
    // one subexp
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("sub1")
        .setLeftType(OperandType.SUB_EXP)
        .setRight("b")
        .setRightType(OperandType.VARIABLE)
        .setExpressionOp(ExpressionOp.ADD)
        .build();
    when(node.expressionConfig()).thenReturn(expression_config);
    
    result = new ExpressionResult(node);
    result.join();
    assertEquals(1, result.time_series.size());
    verify(joiner, times(1)).join(
        eq(result.results), 
        aryEq("sub1".getBytes(Const.UTF8_CHARSET)), 
        aryEq("b".getBytes(Const.UTF8_CHARSET)), 
        eq(true));
    
    // other subexp
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight("sub1")
        .setRightType(OperandType.SUB_EXP)
        .setExpressionOp(ExpressionOp.ADD)
        .build();
    when(node.expressionConfig()).thenReturn(expression_config);
    
    result = new ExpressionResult(node);
    result.join();
    assertEquals(1, result.time_series.size());
    verify(joiner, times(1)).join(
        eq(result.results), 
        aryEq("a".getBytes(Const.UTF8_CHARSET)), 
        aryEq("sub1".getBytes(Const.UTF8_CHARSET)), 
        eq(true));
    
    // both subexp
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("sub1")
        .setLeftType(OperandType.SUB_EXP)
        .setRight("sub2")
        .setRightType(OperandType.SUB_EXP)
        .setExpressionOp(ExpressionOp.ADD)
        .build();
    when(node.expressionConfig()).thenReturn(expression_config);
    
    result = new ExpressionResult(node);
    result.join();
    assertEquals(1, result.time_series.size());
    verify(joiner, times(1)).join(
        eq(result.results), 
        aryEq("sub1".getBytes(Const.UTF8_CHARSET)), 
        aryEq("sub2".getBytes(Const.UTF8_CHARSET)), 
        eq(true));
    
    // left metric
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight("42")
        .setRightType(OperandType.LITERAL_NUMERIC)
        .setExpressionOp(ExpressionOp.ADD)
        .build();
    when(node.expressionConfig()).thenReturn(expression_config);
    
    result = new ExpressionResult(node);
    result.join();
    assertEquals(1, result.time_series.size());
    verify(joiner, times(1)).join(
        eq(result.results), 
        aryEq("a".getBytes(Const.UTF8_CHARSET)), 
        eq(true),
        eq(false));
    
    // left sub
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("sub1")
        .setLeftType(OperandType.SUB_EXP)
        .setRight("42")
        .setRightType(OperandType.LITERAL_NUMERIC)
        .setExpressionOp(ExpressionOp.ADD)
        .build();
    when(node.expressionConfig()).thenReturn(expression_config);
    
    result = new ExpressionResult(node);
    result.join();
    assertEquals(1, result.time_series.size());
    verify(joiner, times(1)).join(
        eq(result.results), 
        aryEq("sub1".getBytes(Const.UTF8_CHARSET)), 
        eq(true),
        eq(true));
    
    // right metric
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("42")
        .setLeftType(OperandType.LITERAL_NUMERIC)
        .setRight("b")
        .setRightType(OperandType.VARIABLE)
        .setExpressionOp(ExpressionOp.ADD)
        .build();
    when(node.expressionConfig()).thenReturn(expression_config);
    
    result = new ExpressionResult(node);
    result.join();
    assertEquals(1, result.time_series.size());
    verify(joiner, times(1)).join(
        eq(result.results), 
        aryEq("b".getBytes(Const.UTF8_CHARSET)), 
        eq(false),
        eq(false));
    
    // right sub
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("42")
        .setLeftType(OperandType.LITERAL_NUMERIC)
        .setRight("sub2")
        .setRightType(OperandType.SUB_EXP)
        .setExpressionOp(ExpressionOp.ADD)
        .build();
    when(node.expressionConfig()).thenReturn(expression_config);
    
    result = new ExpressionResult(node);
    result.join();
    assertEquals(1, result.time_series.size());
    verify(joiner, times(1)).join(
        eq(result.results), 
        aryEq("sub2".getBytes(Const.UTF8_CHARSET)), 
        eq(false),
        eq(true));
  }
  
  @Test
  public void joinBytes() throws Exception {
    List<Pair<TimeSeries, TimeSeries>> joins = 
        Lists.newArrayList(new Pair<TimeSeries, TimeSeries>(mock(TimeSeries.class), mock(TimeSeries.class)));
    when(joiner.join(any(List.class), any(byte[].class), any(byte[].class), anyBoolean()))
      .thenReturn(joins);
    when(joiner.join(any(List.class), any(byte[].class), anyBoolean(), anyBoolean()))
      .thenReturn(joins);
    
    ExpressionResult result = new ExpressionResult(node);
    when(node.leftMetric()).thenReturn(LEFT);
    when(node.rightMetric()).thenReturn(RIGHT);
    
    result.join();
    assertEquals(1, result.time_series.size());
    verify(joiner, times(1)).join(
        eq(result.results), 
        aryEq(LEFT), 
        aryEq(RIGHT), 
        eq(false));
    
    // one subexp
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("sub1")
        .setLeftType(OperandType.SUB_EXP)
        .setRight("b")
        .setRightType(OperandType.VARIABLE)
        .setExpressionOp(ExpressionOp.ADD)
        .build();
    when(node.expressionConfig()).thenReturn(expression_config);
    
    result = new ExpressionResult(node);
    result.join();
    assertEquals(1, result.time_series.size());
    verify(joiner, times(1)).join(
        eq(result.results), 
        aryEq(LEFT), 
        aryEq(RIGHT), 
        eq(true));
    
    // other subexp
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight("sub1")
        .setRightType(OperandType.SUB_EXP)
        .setExpressionOp(ExpressionOp.ADD)
        .build();
    when(node.expressionConfig()).thenReturn(expression_config);
    
    result = new ExpressionResult(node);
    result.join();
    assertEquals(1, result.time_series.size());
    verify(joiner, times(2)).join(
        eq(result.results), 
        aryEq(LEFT), 
        aryEq(RIGHT), 
        eq(true));
    
    // both subexp
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("sub1")
        .setLeftType(OperandType.SUB_EXP)
        .setRight("sub2")
        .setRightType(OperandType.SUB_EXP)
        .setExpressionOp(ExpressionOp.ADD)
        .build();
    when(node.expressionConfig()).thenReturn(expression_config);
    
    result = new ExpressionResult(node);
    result.join();
    assertEquals(1, result.time_series.size());
    verify(joiner, times(3)).join(
        eq(result.results), 
        aryEq(LEFT), 
        aryEq(RIGHT), 
        eq(true));
    
    // left metric
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight("42")
        .setRightType(OperandType.LITERAL_NUMERIC)
        .setExpressionOp(ExpressionOp.ADD)
        .build();
    when(node.expressionConfig()).thenReturn(expression_config);
    
    result = new ExpressionResult(node);
    result.join();
    assertEquals(1, result.time_series.size());
    verify(joiner, times(1)).join(
        eq(result.results), 
        aryEq(LEFT), 
        eq(true),
        eq(false));
    
    // left sub
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("sub1")
        .setLeftType(OperandType.SUB_EXP)
        .setRight("42")
        .setRightType(OperandType.LITERAL_NUMERIC)
        .setExpressionOp(ExpressionOp.ADD)
        .build();
    when(node.expressionConfig()).thenReturn(expression_config);
    
    result = new ExpressionResult(node);
    result.join();
    assertEquals(1, result.time_series.size());
    verify(joiner, times(1)).join(
        eq(result.results), 
        aryEq(LEFT), 
        eq(true),
        eq(true));
    
    // right metric
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("42")
        .setLeftType(OperandType.LITERAL_NUMERIC)
        .setRight("b")
        .setRightType(OperandType.VARIABLE)
        .setExpressionOp(ExpressionOp.ADD)
        .build();
    when(node.expressionConfig()).thenReturn(expression_config);
    
    result = new ExpressionResult(node);
    result.join();
    assertEquals(1, result.time_series.size());
    verify(joiner, times(1)).join(
        eq(result.results), 
        aryEq(RIGHT), 
        eq(false),
        eq(false));
    
    // right sub
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("42")
        .setLeftType(OperandType.LITERAL_NUMERIC)
        .setRight("sub2")
        .setRightType(OperandType.SUB_EXP)
        .setExpressionOp(ExpressionOp.ADD)
        .build();
    when(node.expressionConfig()).thenReturn(expression_config);
    
    result = new ExpressionResult(node);
    result.join();
    assertEquals(1, result.time_series.size());
    verify(joiner, times(1)).join(
        eq(result.results), 
        aryEq(RIGHT), 
        eq(false),
        eq(true));
  }
}
