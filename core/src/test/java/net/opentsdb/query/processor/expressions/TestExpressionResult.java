//This file is part of OpenTSDB.
//Copyright (C) 2018-2020  The OpenTSDB Authors.
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
import static org.mockito.Matchers.eq;
import static org.mockito.AdditionalMatchers.aryEq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import net.opentsdb.common.Const;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.DefaultQueryResultId;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QueryResultId;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.joins.JoinConfig;
import net.opentsdb.query.joins.JoinConfig.JoinType;
import net.opentsdb.query.joins.Joiner;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.processor.expressions.ExpressionParseNode.ExpressionOp;
import net.opentsdb.query.processor.expressions.ExpressionParseNode.OperandType;

public class TestExpressionResult {
  private static final byte[] LEFT = new byte[] { 0, 0, 1 };
  private static final byte[] RIGHT = new byte[] { 0, 0, 2 };
  private static final byte[] CONDITION = new byte[] { 0, 0, 3 };
  private static final byte[] SUB1 = new byte[] { 0, 0, 4 };
  private static final byte[] SUB2 = new byte[] { 0, 0, 5 };
  private static final byte[] SUB3 = new byte[] { 0, 0, 6 };
  
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
      .setDataType(NumericType.TYPE.toString())
      .build();
    
    join_config = (JoinConfig) JoinConfig.newBuilder()
        .setJoinType(JoinType.INNER)
        .addJoins("host", "host")
        .setId("join")
        .setId("expression")
        .build();
    
    config = (ExpressionConfig) ExpressionConfig.newBuilder()
        .setExpression("a + b + c")
        .setJoinConfig(join_config)
        .addInterpolatorConfig(numeric_config)
        .setId("e1")
        .setId("expression")
        .build();
    
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight("b")
        .setRightType(OperandType.VARIABLE)
        .setExpressionOp(ExpressionOp.ADD)
        .setExpressionConfig(config)
        .setId("expression")
        .build();
    
    when(node.config()).thenReturn(expression_config);
    when(node.joiner()).thenReturn(joiner);
  }
  
  @Test
  public void ctor() throws Exception {
    ExpressionResult result = new ExpressionResult(node);
  }
  
  @Test
  public void joinString() throws Exception {
    Collection<TimeSeries[]> joins = 
        Lists.<TimeSeries[]>newArrayList(
            new TimeSeries[] { mock(TimeSeries.class), mock(TimeSeries.class) });
    when(joiner.join(any(Collection.class), 
                     any(ExpressionParseNode.class), 
                     any(byte[].class), 
                     any(byte[].class), 
                     any(byte[].class)))
      .thenReturn(joins);
    
    setupNode(false);
    ExpressionResult result = new ExpressionResult(node);
    result.join();
    assertEquals(1, result.time_series.size());
    verify(joiner, times(1)).join(
        (Collection<QueryResult>) any(Collection.class), 
        eq(expression_config),
        aryEq("a".getBytes(Const.UTF8_CHARSET)), 
        aryEq("b".getBytes(Const.UTF8_CHARSET)), 
        eq(null));
    
    // one subexp
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("sub1")
        .setLeftType(OperandType.SUB_EXP)
        .setRight("b")
        .setRightType(OperandType.VARIABLE)
        .setExpressionOp(ExpressionOp.ADD)
        .setExpressionConfig(config)
        .setId("expression")
        .build();
    
    when(node.config()).thenReturn(expression_config);
    setupNode(false);
    result = new ExpressionResult(node);
    result.join();
    assertEquals(1, result.time_series.size());
    verify(joiner, times(1)).join(
        (Collection<QueryResult>) any(Collection.class),
        eq(expression_config),
        aryEq("sub1".getBytes(Const.UTF8_CHARSET)), 
        aryEq("b".getBytes(Const.UTF8_CHARSET)),
        eq(null));
    
    // other subexp
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight("sub1")
        .setRightType(OperandType.SUB_EXP)
        .setExpressionOp(ExpressionOp.ADD)
        .setExpressionConfig(config)
        .setId("expression")
        .build();
    when(node.config()).thenReturn(expression_config);
    setupNode(false);
    result = new ExpressionResult(node);
    result.join();
    assertEquals(1, result.time_series.size());
    verify(joiner, times(1)).join(
        (Collection<QueryResult>) any(Collection.class), 
        eq(expression_config),
        aryEq("a".getBytes(Const.UTF8_CHARSET)), 
        aryEq("sub1".getBytes(Const.UTF8_CHARSET)), 
        eq(null));
    
    // both subexp
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("sub1")
        .setLeftType(OperandType.SUB_EXP)
        .setRight("sub2")
        .setRightType(OperandType.SUB_EXP)
        .setExpressionOp(ExpressionOp.ADD)
        .setExpressionConfig(config)
        .setId("expression")
        .build();
    when(node.config()).thenReturn(expression_config);
    setupNode(false);
    result = new ExpressionResult(node);
    result.join();
    assertEquals(1, result.time_series.size());
    verify(joiner, times(1)).join(
        (Collection<QueryResult>) any(Collection.class), 
        eq(expression_config),
        aryEq("sub1".getBytes(Const.UTF8_CHARSET)), 
        aryEq("sub2".getBytes(Const.UTF8_CHARSET)), 
        eq(null));
    
    // left metric
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight("42")
        .setRightType(OperandType.LITERAL_NUMERIC)
        .setExpressionOp(ExpressionOp.ADD)
        .setExpressionConfig(config)
        .setId("expression")
        .build();
    when(node.config()).thenReturn(expression_config);
    setupNode(false);
    result = new ExpressionResult(node);
    result.join();
    assertEquals(1, result.time_series.size());
    verify(joiner, times(1)).join(
        (Collection<QueryResult>) any(Collection.class),
        eq(expression_config),
        aryEq("a".getBytes(Const.UTF8_CHARSET)), 
        eq(null),
        eq(null));
    
    // left sub
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("sub1")
        .setLeftType(OperandType.SUB_EXP)
        .setRight("42")
        .setRightType(OperandType.LITERAL_NUMERIC)
        .setExpressionOp(ExpressionOp.ADD)
        .setExpressionConfig(config)
        .setId("expression")
        .build();
    when(node.config()).thenReturn(expression_config);
    setupNode(false);
    result = new ExpressionResult(node);
    result.join();
    assertEquals(1, result.time_series.size());
    verify(joiner, times(1)).join(
        (Collection<QueryResult>) any(Collection.class), 
        eq(expression_config),
        aryEq("sub1".getBytes(Const.UTF8_CHARSET)), 
        eq(null),
        eq(null));
    
    // right metric
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("42")
        .setLeftType(OperandType.LITERAL_NUMERIC)
        .setRight("b")
        .setRightType(OperandType.VARIABLE)
        .setExpressionOp(ExpressionOp.ADD)
        .setExpressionConfig(config)
        .setId("expression")
        .build();
    when(node.config()).thenReturn(expression_config);
    setupNode(false);
    result = new ExpressionResult(node);
    result.join();
    assertEquals(1, result.time_series.size());
    verify(joiner, times(1)).join(
        (Collection<QueryResult>) any(Collection.class), 
        eq(expression_config),
        eq(null),
        aryEq("b".getBytes(Const.UTF8_CHARSET)), 
        eq(null));
    
    // right sub
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("42")
        .setLeftType(OperandType.LITERAL_NUMERIC)
        .setRight("sub2")
        .setRightType(OperandType.SUB_EXP)
        .setExpressionOp(ExpressionOp.ADD)
        .setExpressionConfig(config)
        .setId("expression")
        .build();
    when(node.config()).thenReturn(expression_config);
    setupNode(false);
    result = new ExpressionResult(node);
    result.join();
    assertEquals(1, result.time_series.size());
    verify(joiner, times(1)).join(
        (Collection<QueryResult>) any(Collection.class), 
        eq(expression_config),
        eq(null),
        aryEq("sub2".getBytes(Const.UTF8_CHARSET)), 
        eq(null));
  }
  
  @Test
  public void joinStringTernary() throws Exception {
    Collection<TimeSeries[]> joins = Lists.<TimeSeries[]>newArrayList(
        new TimeSeries[] { mock(TimeSeries.class), 
                           mock(TimeSeries.class), 
                           mock(TimeSeries.class) });
    when(joiner.join(any(Collection.class), 
                     any(ExpressionParseNode.class), 
                     any(byte[].class), 
                     any(byte[].class), 
                     any(byte[].class)))
      .thenReturn(joins);
    
    expression_config = (TernaryParseNode) TernaryParseNode.newBuilder()
        .setCondition("c")
        .setConditionType(OperandType.VARIABLE)
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight("b")
        .setRightType(OperandType.VARIABLE)
        .setExpressionConfig(config)
        .setId("expression")
        .build();
    
    setupTernaryNode(false);
    ExpressionResult result = new ExpressionResult(node);
    result.join();
    assertEquals(1, result.time_series.size());
    verify(joiner, times(1)).join(
        (Collection<QueryResult>) any(Collection.class), 
        eq(expression_config),
        aryEq("a".getBytes(Const.UTF8_CHARSET)), 
        aryEq("b".getBytes(Const.UTF8_CHARSET)), 
        aryEq("c".getBytes(Const.UTF8_CHARSET)));
    
    // one subexp
    expression_config = (TernaryParseNode) TernaryParseNode.newBuilder()
        .setCondition("sub1")
        .setConditionType(OperandType.SUB_EXP)
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight("b")
        .setRightType(OperandType.VARIABLE)
        .setExpressionConfig(config)
        .setId("expression")
        .build();
    
    when(node.config()).thenReturn(expression_config);
    setupTernaryNode(false);
    result = new ExpressionResult(node);
    result.join();
    assertEquals(1, result.time_series.size());
    verify(joiner, times(1)).join(
        (Collection<QueryResult>) any(Collection.class),
        eq(expression_config),
        aryEq("a".getBytes(Const.UTF8_CHARSET)), 
        aryEq("b".getBytes(Const.UTF8_CHARSET)), 
        aryEq("sub1".getBytes(Const.UTF8_CHARSET)));
    
    expression_config = (TernaryParseNode) TernaryParseNode.newBuilder()
        .setCondition("sub2")
        .setConditionType(OperandType.SUB_EXP)
        .setLeft("sub0")
        .setLeftType(OperandType.SUB_EXP)
        .setRight("sub1")
        .setRightType(OperandType.SUB_EXP)
        .setExpressionConfig(config)
        .setId("expression")
        .build();
    
    when(node.config()).thenReturn(expression_config);
    setupTernaryNode(false);
    result = new ExpressionResult(node);
    result.join();
    assertEquals(1, result.time_series.size());
    verify(joiner, times(1)).join(
        (Collection<QueryResult>) any(Collection.class),
        eq(expression_config),
        aryEq("sub0".getBytes(Const.UTF8_CHARSET)), 
        aryEq("sub1".getBytes(Const.UTF8_CHARSET)), 
        aryEq("sub2".getBytes(Const.UTF8_CHARSET)));
    
    expression_config = (TernaryParseNode) TernaryParseNode.newBuilder()
        .setCondition("sub2")
        .setConditionType(OperandType.SUB_EXP)
        .setLeft("sub0")
        .setLeftType(OperandType.SUB_EXP)
        .setRight("42")
        .setRightType(OperandType.LITERAL_NUMERIC)
        .setExpressionConfig(config)
        .setId("expression")
        .build();
    
    when(node.config()).thenReturn(expression_config);
    setupTernaryNode(false);
    result = new ExpressionResult(node);
    result.join();
    assertEquals(1, result.time_series.size());
    verify(joiner, times(1)).join(
        (Collection<QueryResult>) any(Collection.class),
        eq(expression_config),
        aryEq("sub0".getBytes(Const.UTF8_CHARSET)), 
        eq(null), 
        aryEq("sub2".getBytes(Const.UTF8_CHARSET)));
    
    expression_config = (TernaryParseNode) TernaryParseNode.newBuilder()
        .setCondition("sub2")
        .setConditionType(OperandType.SUB_EXP)
        .setLeft("42")
        .setLeftType(OperandType.LITERAL_NUMERIC)
        .setRight("sub1")
        .setRightType(OperandType.SUB_EXP)
        .setExpressionConfig(config)
        .setId("expression")
        .build();
    
    when(node.config()).thenReturn(expression_config);
    setupTernaryNode(false);
    result = new ExpressionResult(node);
    result.join();
    assertEquals(1, result.time_series.size());
    verify(joiner, times(1)).join(
        (Collection<QueryResult>) any(Collection.class),
        eq(expression_config),
        eq(null), 
        aryEq("sub1".getBytes(Const.UTF8_CHARSET)), 
        aryEq("sub2".getBytes(Const.UTF8_CHARSET)));
    
    expression_config = (TernaryParseNode) TernaryParseNode.newBuilder()
        .setCondition("sub2")
        .setConditionType(OperandType.SUB_EXP)
        .setLeft("42")
        .setLeftType(OperandType.LITERAL_NUMERIC)
        .setRight("24")
        .setRightType(OperandType.LITERAL_NUMERIC)
        .setExpressionConfig(config)
        .setId("expression")
        .build();
    
    when(node.config()).thenReturn(expression_config);
    setupTernaryNode(false);
    result = new ExpressionResult(node);
    result.join();
    assertEquals(1, result.time_series.size());
    verify(joiner, times(1)).join(
        (Collection<QueryResult>) any(Collection.class),
        eq(expression_config),
        eq(null), 
        eq(null), 
        aryEq("sub2".getBytes(Const.UTF8_CHARSET)));
  }
  
  @Test
  public void joinBytes() throws Exception {
    Collection<TimeSeries[]> joins = 
        Lists.<TimeSeries[]>newArrayList(
            new TimeSeries[] { mock(TimeSeries.class), mock(TimeSeries.class) });
    when(joiner.join(any(Collection.class), 
                     any(ExpressionParseNode.class), 
                     any(byte[].class), 
                     any(byte[].class), 
                     any(byte[].class)))
        .thenReturn(joins);
    setupNode(true);
    ExpressionResult result = new ExpressionResult(node);
    
    result.join();
    assertEquals(1, result.time_series.size());
    verify(joiner, times(1)).join(
        (Collection<QueryResult>) any(Collection.class), 
        eq(expression_config),
        aryEq(LEFT), 
        aryEq(RIGHT), 
        eq(null));
    
    // one subexp
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("sub1")
        .setLeftType(OperandType.SUB_EXP)
        .setRight("b")
        .setRightType(OperandType.VARIABLE)
        .setExpressionOp(ExpressionOp.ADD)
        .setExpressionConfig(config)
        .setId("expression")
        .build();
    when(node.config()).thenReturn(expression_config);
    setupNode(true);
    result = new ExpressionResult(node);
    result.join();
    assertEquals(1, result.time_series.size());
    verify(joiner, times(1)).join(
        (Collection<QueryResult>) any(Collection.class), 
        eq(expression_config),
        aryEq(LEFT), 
        aryEq(RIGHT), 
        eq(null));
    
    // other subexp
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight("sub2")
        .setRightType(OperandType.SUB_EXP)
        .setExpressionOp(ExpressionOp.ADD)
        .setExpressionConfig(config)
        .setId("expression")
        .build();
    when(node.config()).thenReturn(expression_config);
    setupNode(true);
    result = new ExpressionResult(node);
    result.join();
    assertEquals(1, result.time_series.size());
    verify(joiner, times(1)).join(
        (Collection<QueryResult>) any(Collection.class), 
        eq(expression_config),
        aryEq(LEFT), 
        aryEq(SUB2), 
        eq(null));
    
    // both subexp
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("sub1")
        .setLeftType(OperandType.SUB_EXP)
        .setRight("sub2")
        .setRightType(OperandType.SUB_EXP)
        .setExpressionOp(ExpressionOp.ADD)
        .setExpressionConfig(config)
        .setId("expression")
        .build();
    when(node.config()).thenReturn(expression_config);
    setupNode(true);
    result = new ExpressionResult(node);
    result.join();
    assertEquals(1, result.time_series.size());
    verify(joiner, times(1)).join(
        (Collection<QueryResult>) any(Collection.class), 
        eq(expression_config),
        aryEq(SUB1), 
        aryEq(SUB2), 
        eq(null));
    
    // left metric
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight("42")
        .setRightType(OperandType.LITERAL_NUMERIC)
        .setExpressionOp(ExpressionOp.ADD)
        .setExpressionConfig(config)
        .setId("expression")
        .build();
    when(node.config()).thenReturn(expression_config);
    setupNode(true);
    result = new ExpressionResult(node);
    result.join();
    assertEquals(1, result.time_series.size());
    verify(joiner, times(1)).join(
        (Collection<QueryResult>) any(Collection.class), 
        eq(expression_config),
        aryEq(LEFT), 
        eq(null),
        eq(null));
    
    // left sub
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("sub1")
        .setLeftType(OperandType.SUB_EXP)
        .setRight("42")
        .setRightType(OperandType.LITERAL_NUMERIC)
        .setExpressionOp(ExpressionOp.ADD)
        .setExpressionConfig(config)
        .setId("expression")
        .build();
    when(node.config()).thenReturn(expression_config);
    setupNode(true);
    result = new ExpressionResult(node);
    result.join();
    assertEquals(1, result.time_series.size());
    verify(joiner, times(1)).join(
        (Collection<QueryResult>) any(Collection.class), 
        eq(expression_config),
        aryEq(SUB1), 
        eq(null),
        eq(null));
    
    // right metric
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("42")
        .setLeftType(OperandType.LITERAL_NUMERIC)
        .setRight("b")
        .setRightType(OperandType.VARIABLE)
        .setExpressionOp(ExpressionOp.ADD)
        .setExpressionConfig(config)
        .setId("expression")
        .build();
    when(node.config()).thenReturn(expression_config);
    setupNode(true);
    result = new ExpressionResult(node);
    result.join();
    assertEquals(1, result.time_series.size());
    verify(joiner, times(1)).join(
        (Collection<QueryResult>) any(Collection.class), 
        eq(expression_config),
        eq(null),
        aryEq(RIGHT), 
        eq(null));
    
    // right sub
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("42")
        .setLeftType(OperandType.LITERAL_NUMERIC)
        .setRight("sub2")
        .setRightType(OperandType.SUB_EXP)
        .setExpressionOp(ExpressionOp.ADD)
        .setExpressionConfig(config)
        .setId("expression")
        .build();
    when(node.config()).thenReturn(expression_config);
    setupNode(true);
    result = new ExpressionResult(node);
    result.join();
    assertEquals(1, result.time_series.size());
    verify(joiner, times(1)).join(
        (Collection<QueryResult>) any(Collection.class), 
        eq(expression_config),
        eq(null),
        aryEq(SUB2), 
        eq(null));
  }
  
  @Test
  public void joinBytesTernary() throws Exception {
    Collection<TimeSeries[]> joins = 
        Lists.<TimeSeries[]>newArrayList(
            new TimeSeries[] { mock(TimeSeries.class), mock(TimeSeries.class) });
    when(joiner.join(any(Collection.class), 
                     any(ExpressionParseNode.class), 
                     any(byte[].class), 
                     any(byte[].class), 
                     any(byte[].class)))
        .thenReturn(joins);
    
    expression_config = (TernaryParseNode) TernaryParseNode.newBuilder()
        .setCondition("c")
        .setConditionType(OperandType.VARIABLE)
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight("b")
        .setRightType(OperandType.VARIABLE)
        .setExpressionConfig(config)
        .setId("expression")
        .build();
    setupTernaryNode(true);
    ExpressionResult result = new ExpressionResult(node);
    
    result.join();
    assertEquals(1, result.time_series.size());
    verify(joiner, times(1)).join(
        (Collection<QueryResult>) any(Collection.class), 
        eq(expression_config),
        aryEq(LEFT), 
        aryEq(RIGHT), 
        aryEq(CONDITION));
    
    expression_config = (TernaryParseNode) TernaryParseNode.newBuilder()
        .setCondition("sub3")
        .setConditionType(OperandType.SUB_EXP)
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight("b")
        .setRightType(OperandType.VARIABLE)
        .setExpressionConfig(config)
        .setId("expression")
        .build();
    setupTernaryNode(true);
    result = new ExpressionResult(node);
    
    result.join();
    assertEquals(1, result.time_series.size());
    verify(joiner, times(1)).join(
        (Collection<QueryResult>) any(Collection.class), 
        eq(expression_config),
        aryEq(LEFT), 
        aryEq(RIGHT), 
        aryEq(SUB3));
    
    expression_config = (TernaryParseNode) TernaryParseNode.newBuilder()
        .setCondition("sub3")
        .setConditionType(OperandType.SUB_EXP)
        .setLeft("sub1")
        .setLeftType(OperandType.SUB_EXP)
        .setRight("sub2")
        .setRightType(OperandType.SUB_EXP)
        .setExpressionConfig(config)
        .setId("expression")
        .build();
    setupTernaryNode(true);
    result = new ExpressionResult(node);
    
    result.join();
    assertEquals(1, result.time_series.size());
    verify(joiner, times(1)).join(
        (Collection<QueryResult>) any(Collection.class), 
        eq(expression_config),
        aryEq(SUB1), 
        aryEq(SUB2), 
        aryEq(SUB3));
    
    expression_config = (TernaryParseNode) TernaryParseNode.newBuilder()
        .setCondition("sub3")
        .setConditionType(OperandType.SUB_EXP)
        .setLeft("42")
        .setLeftType(OperandType.LITERAL_NUMERIC)
        .setRight("sub2")
        .setRightType(OperandType.SUB_EXP)
        .setExpressionConfig(config)
        .setId("expression")
        .build();
    setupTernaryNode(true);
    result = new ExpressionResult(node);
    
    result.join();
    assertEquals(1, result.time_series.size());
    verify(joiner, times(1)).join(
        (Collection<QueryResult>) any(Collection.class), 
        eq(expression_config),
        eq(null), 
        aryEq(SUB2), 
        aryEq(SUB3));
    
    expression_config = (TernaryParseNode) TernaryParseNode.newBuilder()
        .setCondition("sub3")
        .setConditionType(OperandType.SUB_EXP)
        .setLeft("sub1")
        .setLeftType(OperandType.SUB_EXP)
        .setRight("42")
        .setRightType(OperandType.LITERAL_NUMERIC)
        .setExpressionConfig(config)
        .setId("expression")
        .build();
    setupTernaryNode(true);
    result = new ExpressionResult(node);
    
    result.join();
    assertEquals(1, result.time_series.size());
    verify(joiner, times(1)).join(
        (Collection<QueryResult>) any(Collection.class), 
        eq(expression_config),
        aryEq(SUB1), 
        eq(null), 
        aryEq(SUB3));
    
    expression_config = (TernaryParseNode) TernaryParseNode.newBuilder()
        .setCondition("sub3")
        .setConditionType(OperandType.SUB_EXP)
        .setLeft("24")
        .setLeftType(OperandType.LITERAL_NUMERIC)
        .setRight("42")
        .setRightType(OperandType.LITERAL_NUMERIC)
        .setExpressionConfig(config)
        .setId("expression")
        .build();
    setupTernaryNode(true);
    result = new ExpressionResult(node);
    
    result.join();
    assertEquals(1, result.time_series.size());
    verify(joiner, times(1)).join(
        (Collection<QueryResult>) any(Collection.class), 
        eq(expression_config),
        eq(null), 
        eq(null), 
        aryEq(SUB3));
  }
  
  void setupNode(final boolean byte_mode) {
    when(node.config()).thenReturn(expression_config);
    when(node.joiner()).thenReturn(joiner);
    if (byte_mode) {
      if (expression_config.getLeft().equals("a")) {
        when(node.leftMetric()).thenReturn(LEFT);
      } else if (expression_config.getLeft().equals("sub1")) {
        when(node.leftMetric()).thenReturn(SUB1);
      }
      
      if (expression_config.getRight().equals("b")) {
        when(node.rightMetric()).thenReturn(RIGHT);
      } else if (expression_config.getRight().equals("sub2")) {
        when(node.rightMetric()).thenReturn(SUB2);
      }
    } else {
      when(node.leftMetric()).thenReturn(((String) expression_config.getLeft()).getBytes());
      when(node.rightMetric()).thenReturn(((String) expression_config.getRight()).getBytes());
    }
    
    Map<QueryResultId, QueryResult> results = Maps.newHashMap();
    if (expression_config.getLeftType() != null && 
       (expression_config.getLeftType() == OperandType.VARIABLE ||
        expression_config.getLeftType() == OperandType.SUB_EXP)) {
      QueryResult result = mock(QueryResult.class);
      results.put(new DefaultQueryResultId(((String) expression_config.getLeft()), 
          ((String) expression_config.getLeft())), 
          result);
    }
    
    if (expression_config.getRightType() != null && 
       (expression_config.getRightType() == OperandType.VARIABLE ||
        expression_config.getRightType() == OperandType.SUB_EXP)) {
      QueryResult result = mock(QueryResult.class);
      results.put(new DefaultQueryResultId(((String) expression_config.getRight()), 
          ((String) expression_config.getRight())), 
          result);
    }
  }
  
  void setupTernaryNode(final boolean byte_mode) {
    node = mock(TernaryNode.class);
    when(node.config()).thenReturn(expression_config);
    when(node.joiner()).thenReturn(joiner);
    TernaryParseNode config = (TernaryParseNode) expression_config;
    if (byte_mode) {
      if (expression_config.getLeft().equals("a")) {
        when(node.leftMetric()).thenReturn(LEFT);
      } else if (expression_config.getLeft().equals("sub1")) {
        when(node.leftMetric()).thenReturn(SUB1);
      }
      
      if (expression_config.getRight().equals("b")) {
        when(node.rightMetric()).thenReturn(RIGHT);
      } else if (expression_config.getRight().equals("sub2")) {
        when(node.rightMetric()).thenReturn(SUB2);
      }
      
      if (config.getCondition().equals("c")) {
        when(((TernaryNode) node).conditionMetric()).thenReturn(CONDITION);
      } else if (config.getCondition().equals("sub3")) {
        when(((TernaryNode) node).conditionMetric()).thenReturn(SUB3);
      }
    } else {
      when(node.leftMetric()).thenReturn(((String) expression_config.getLeft()).getBytes());
      when(node.rightMetric()).thenReturn(((String) expression_config.getRight()).getBytes());
      when(((TernaryNode) node).conditionMetric()).thenReturn(
          ((String) ((TernaryParseNode) expression_config).getCondition()).getBytes());
    }
    
    Map<QueryResultId, QueryResult> results = Maps.newHashMap();
    if (expression_config.getLeftType() != null && 
       (expression_config.getLeftType() == OperandType.VARIABLE ||
        expression_config.getLeftType() == OperandType.SUB_EXP)) {
      QueryResult result = mock(QueryResult.class);
      results.put(new DefaultQueryResultId(((String) expression_config.getLeft()), 
          ((String) expression_config.getLeft())), 
          result);
    }
    
    if (expression_config.getRightType() != null && 
       (expression_config.getRightType() == OperandType.VARIABLE ||
        expression_config.getRightType() == OperandType.SUB_EXP)) {
      QueryResult result = mock(QueryResult.class);
      results.put(new DefaultQueryResultId(((String) expression_config.getRight()), 
          ((String) expression_config.getRight())), 
          result);
    }
    if (config.getCondition() != null &&
        (config.getConditionType() == OperandType.VARIABLE ||
         config.getConditionType() == OperandType.SUB_EXP)) {
      QueryResult result = mock(QueryResult.class);
      results.put(new DefaultQueryResultId(((String) config.getCondition()), 
          ((String) config.getCondition())), 
          result);
    }
  }
}