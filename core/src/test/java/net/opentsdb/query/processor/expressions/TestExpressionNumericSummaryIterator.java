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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.junit.Test;
import org.powermock.reflect.Whitebox;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.types.numeric.NumericSummaryInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.processor.expressions.ExpressionParseNode.ExpressionOp;
import net.opentsdb.query.processor.expressions.ExpressionParseNode.OperandType;
import net.opentsdb.query.processor.expressions.ExpressionParser.NumericLiteral;
import net.opentsdb.rollup.RollupConfig;

public class TestExpressionNumericSummaryIterator 
    extends BaseNumericSummaryTest {
  
  /**
   * Note that all of the arithmetic and logical tests are in the the
   * other Numeric classes. Since, for now, summaries are treated in the
   * same way as numerics, we just need to make sure individual summaries
   * are pulled out and processed. 
   */
  
  @Test
  public void ctor() throws Exception {
    RollupConfig rollup_config = mock(RollupConfig.class);
    when(rollup_config.getAggregationIds()).thenReturn(
        (Map) ImmutableMap.builder()
          .put("sum", 0)
          .put("count", 2)
          .put("avg", 5)
          .build());
    when(RESULT.rollupConfig()).thenReturn(rollup_config);
    
    setupData(new long[] { 1, 5, 2 }, new long[] { 1, 2, 2 }, 
        new long[] { 4, 10, 8 }, new long[] { 1, 2, 2 }, false);
    
    // others test regular interpolator configs, here test fallback 
    // to numeric type config.
    ExpressionConfig config = (ExpressionConfig) ExpressionConfig.newBuilder()
      .setExpression("a + b + c")
      .setJoinConfig(JOIN_CONFIG)
      .addInterpolatorConfig(NUMERIC_CONFIG)
      //.addInterpolatorConfig(NUMERIC_SUMMARY_CONFIG) // doh!
      .setId("e1")
      .build();
    when(node.config()).thenReturn(config);
    
    ExpressionNumericSummaryIterator iterator = 
        new ExpressionNumericSummaryIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .build());
    NumericSummaryInterpolatorConfig nsic = 
        (NumericSummaryInterpolatorConfig) iterator.left_interpolator
          .fillPolicy().config();
    assertEquals(3, nsic.expectedSummaries().size());
    assertTrue(nsic.expectedSummaries().contains(0));
    assertTrue(nsic.expectedSummaries().contains(2));
    assertTrue(nsic.expectedSummaries().contains(5));
    // nulls for both
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft(null)
        .setLeftType(OperandType.NULL)
        .setRight(null)
        .setRightType(OperandType.NULL)
        .setExpressionOp(ExpressionOp.SUBTRACT)
        .build();
    when(node.expressionConfig()).thenReturn(expression_config);
    
    try {
      new ExpressionNumericSummaryIterator(node, RESULT, 
          (Map) ImmutableMap.builder()
            .build());
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
  }
  
  @Test
  public void longLong() throws Exception {
    setupData(new long[] { 1, 5, 2 }, new long[] { 1, 2, 2 }, 
              new long[] { 4, 10, 8 }, new long[] { 1, 2, 2 }, false);

    ExpressionNumericSummaryIterator iterator = 
        new ExpressionNumericSummaryIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .build());
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(1000, value.timestamp().msEpoch());
    assertEquals(5, value.value().value(0).longValue());
    assertEquals(2, value.value().value(2).longValue());
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(3000, value.timestamp().msEpoch());
    assertEquals(15, value.value().value(0).longValue());
    assertEquals(4, value.value().value(2).longValue());
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(5000, value.timestamp().msEpoch());
    assertEquals(10, value.value().value(0).longValue());
    assertEquals(4, value.value().value(2).longValue());
    assertFalse(iterator.hasNext());
    
    // subtract
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight(null)
        .setRightType(OperandType.NULL)
        .setExpressionOp(ExpressionOp.SUBTRACT)
        .build();
    when(node.expressionConfig()).thenReturn(expression_config);
    
    iterator = new ExpressionNumericSummaryIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .build());
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(1000, value.timestamp().msEpoch());
    assertEquals(-3, value.value().value(0).longValue());
    assertEquals(0, value.value().value(2).longValue());
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(3000, value.timestamp().msEpoch());
    assertEquals(-5, value.value().value(0).longValue());
    assertEquals(0, value.value().value(2).longValue());
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(5000, value.timestamp().msEpoch());
    assertEquals(-6, value.value().value(0).longValue());
    assertEquals(0, value.value().value(2).longValue());
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void longLongNegate() throws Exception {
    setupData(new long[] { 1, 5, 2 }, new long[] { 1, 2, 2 }, 
              new long[] { 4, 10, 8 }, new long[] { 1, 2, 2 }, false);
    
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight("b")
        .setRightType(OperandType.VARIABLE)
        .setExpressionOp(ExpressionOp.ADD)
        .setNegate(true)
        .build();
    when(node.expressionConfig()).thenReturn(expression_config);
    
    ExpressionNumericSummaryIterator iterator = 
        new ExpressionNumericSummaryIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .build());
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(1000, value.timestamp().msEpoch());
    assertEquals(-5, value.value().value(0).longValue());
    assertEquals(-2, value.value().value(2).longValue());
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(3000, value.timestamp().msEpoch());
    assertEquals(-15, value.value().value(0).longValue());
    assertEquals(-4, value.value().value(2).longValue());
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(5000, value.timestamp().msEpoch());
    assertEquals(-10, value.value().value(0).longValue());
    assertEquals(-4, value.value().value(2).longValue());
    assertFalse(iterator.hasNext());
    
    // subtract
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight(null)
        .setRightType(OperandType.NULL)
        .setExpressionOp(ExpressionOp.SUBTRACT)
        .setNegate(true)
        .build();
    when(node.expressionConfig()).thenReturn(expression_config);
    
    iterator = new ExpressionNumericSummaryIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .build());
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(1000, value.timestamp().msEpoch());
    assertEquals(3, value.value().value(0).longValue());
    assertEquals(0, value.value().value(2).longValue());
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(3000, value.timestamp().msEpoch());
    assertEquals(5, value.value().value(0).longValue());
    assertEquals(0, value.value().value(2).longValue());
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(5000, value.timestamp().msEpoch());
    assertEquals(6, value.value().value(0).longValue());
    assertEquals(0, value.value().value(2).longValue());
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void fillNaNNonInfectious() throws Exception {
    setupData(new double[] { 1.1, -1, 2.66 }, new long[] { 1, -1, 2 }, 
              new double[] { 4.5, 10.75, 8.9 }, new long[] { 1, 2, 2 }, false);
    
    ExpressionNumericSummaryIterator iterator = 
        new ExpressionNumericSummaryIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .build());
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(1000, value.timestamp().msEpoch());
    assertEquals(5.6, value.value().value(0).doubleValue(), 0.001);
    assertEquals(2, value.value().value(2).longValue());
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(3000, value.timestamp().msEpoch());
    assertEquals(10.75, value.value().value(0).doubleValue(), 0.001);
    assertEquals(2, value.value().value(2).doubleValue(), 0.001);
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(5000, value.timestamp().msEpoch());
    assertEquals(11.56, value.value().value(0).doubleValue(), 0.001);
    assertEquals(4, value.value().value(2).longValue());
    assertFalse(iterator.hasNext());
    
    // subtract
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight(null)
        .setRightType(OperandType.NULL)
        .setExpressionOp(ExpressionOp.SUBTRACT)
        .build();
    when(node.expressionConfig()).thenReturn(expression_config);
    
    iterator = new ExpressionNumericSummaryIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .build());
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(1000, value.timestamp().msEpoch());
    assertEquals(-3.4, value.value().value(0).doubleValue(), 0.001);
    assertEquals(0, value.value().value(2).longValue());
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(3000, value.timestamp().msEpoch());
    assertEquals(10.75, value.value().value(0).doubleValue(), 0.001);
    assertEquals(2, value.value().value(2).doubleValue(), 0.001);
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(5000, value.timestamp().msEpoch());
    assertEquals(-6.24, value.value().value(0).doubleValue(), 0.001);
    assertEquals(0, value.value().value(2).longValue());
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void fillNaNInfectious() throws Exception {
    setupData(new double[] { 1.1, -1, 2.66 }, new long[] { 1, -1, 2 }, 
              new double[] { 4.5, 10.75, 8.9 }, new long[] { 1, 2, 2 }, false);
    
    ExpressionNumericSummaryIterator iterator = 
        new ExpressionNumericSummaryIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .build());
    Whitebox.setInternalState(iterator, "infectious_nan", true);
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(1000, value.timestamp().msEpoch());
    assertEquals(5.6, value.value().value(0).doubleValue(), 0.001);
    assertEquals(2, value.value().value(2).longValue());
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(3000, value.timestamp().msEpoch());
    assertTrue(Double.isNaN(value.value().value(0).doubleValue()));
    assertTrue(Double.isNaN(value.value().value(2).doubleValue()));
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(5000, value.timestamp().msEpoch());
    assertEquals(11.56, value.value().value(0).doubleValue(), 0.001);
    assertEquals(4, value.value().value(2).longValue());
    assertFalse(iterator.hasNext());
    
    // subtract
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight(null)
        .setRightType(OperandType.NULL)
        .setExpressionOp(ExpressionOp.SUBTRACT)
        .build();
    when(node.expressionConfig()).thenReturn(expression_config);
    
    iterator = new ExpressionNumericSummaryIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .build());
    Whitebox.setInternalState(iterator, "infectious_nan", true);
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(1000, value.timestamp().msEpoch());
    assertEquals(-3.4, value.value().value(0).doubleValue(), 0.001);
    assertEquals(0, value.value().value(2).longValue());
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(3000, value.timestamp().msEpoch());
    assertTrue(Double.isNaN(value.value().value(0).doubleValue()));
    assertTrue(Double.isNaN(value.value().value(2).doubleValue()));
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(5000, value.timestamp().msEpoch());
    assertEquals(-6.24, value.value().value(0).doubleValue(), 0.001);
    assertEquals(0, value.value().value(2).longValue());
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void fillNull() throws Exception {
    NumericSummaryInterpolatorConfig numeric_config = 
        (NumericSummaryInterpolatorConfig) NumericSummaryInterpolatorConfig.newBuilder()
      .setDefaultFillPolicy(FillPolicy.NONE)
      .setDefaultRealFillPolicy(FillWithRealPolicy.NONE)
      .setExpectedSummaries(Lists.newArrayList(0, 2))
      .setType(NumericSummaryType.TYPE.toString())
      .build();
    
    ExpressionConfig exp_config = (ExpressionConfig) ExpressionConfig.newBuilder()
      .setExpression("a + b + c")
      .setJoinConfig(JOIN_CONFIG)
      .addInterpolatorConfig(numeric_config)
      .setId("e1")
      .build();
    when(node.config()).thenReturn(exp_config);
    
    setupData(new double[] { 1.1, -1, 2.66 }, new long[] { 1, -1, 2 }, 
              new double[] { 4.5, 10.75, 8.9 }, new long[] { 1, 2, 2 }, false);
    
    ExpressionNumericSummaryIterator iterator = 
        new ExpressionNumericSummaryIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .build());
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(1000, value.timestamp().msEpoch());
    assertEquals(5.6, value.value().value(0).doubleValue(), 0.001);
    assertEquals(2, value.value().value(2).longValue());
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(3000, value.timestamp().msEpoch());
    assertNull(value.value());
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(5000, value.timestamp().msEpoch());
    assertEquals(11.56, value.value().value(0).doubleValue(), 0.001);
    assertFalse(iterator.hasNext());
    assertEquals(4, value.value().value(2).longValue());
    
    // subtract
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight(null)
        .setRightType(OperandType.NULL)
        .setExpressionOp(ExpressionOp.SUBTRACT)
        .build();
    when(node.expressionConfig()).thenReturn(expression_config);
    
    iterator = new ExpressionNumericSummaryIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .build());
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(1000, value.timestamp().msEpoch());
    assertEquals(-3.4, value.value().value(0).doubleValue(), 0.001);
    assertEquals(0, value.value().value(2).longValue());
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(3000, value.timestamp().msEpoch());
    assertNull(value.value());
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(5000, value.timestamp().msEpoch());
    assertEquals(-6.24, value.value().value(0).doubleValue(), 0.001);
    assertEquals(0, value.value().value(2).longValue());
    assertFalse(iterator.hasNext());
  }

  @Test
  public void numericLiteralLeft() throws Exception {
    NumericLiteral literal = mock(NumericLiteral.class);
    when(literal.isInteger()).thenReturn(true);
    when(literal.longValue()).thenReturn(42L);
    
    setupData(new long[] { }, new long[] { }, 
              new long[] { 4, 10, 8 }, new long[] { 1, 2, 2 }, false);
    
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft(literal)
        .setLeftType(OperandType.LITERAL_NUMERIC)
        .setRight("b")
        .setRightType(OperandType.VARIABLE)
        .setExpressionOp(ExpressionOp.ADD)
        .build();
    when(node.expressionConfig()).thenReturn(expression_config);
    
    ExpressionNumericSummaryIterator iterator = 
        new ExpressionNumericSummaryIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .build());
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(1000, value.timestamp().msEpoch());
    assertEquals(46, value.value().value(0).longValue());
    assertEquals(43, value.value().value(2).longValue());
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(3000, value.timestamp().msEpoch());
    assertEquals(52, value.value().value(0).longValue());
    assertEquals(44, value.value().value(2).longValue());
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(5000, value.timestamp().msEpoch());
    assertEquals(50, value.value().value(0).longValue());
    assertEquals(44, value.value().value(2).longValue());
    
    // subtract
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft(literal)
        .setLeftType(OperandType.LITERAL_NUMERIC)
        .setRight("b")
        .setRightType(OperandType.VARIABLE)
        .setExpressionOp(ExpressionOp.SUBTRACT)
        .build();
    when(node.expressionConfig()).thenReturn(expression_config);
    
    iterator = new ExpressionNumericSummaryIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .build());
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(1000, value.timestamp().msEpoch());
    assertEquals(38, value.value().value(0).longValue());
    assertEquals(41, value.value().value(2).longValue());
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(3000, value.timestamp().msEpoch());
    assertEquals(32, value.value().value(0).longValue());
    assertEquals(40, value.value().value(2).longValue());
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(5000, value.timestamp().msEpoch());
    assertEquals(34, value.value().value(0).longValue());
    assertEquals(40, value.value().value(2).longValue());
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void numericLiteralRight() throws Exception {
    NumericLiteral literal = mock(NumericLiteral.class);
    when(literal.isInteger()).thenReturn(true);
    when(literal.longValue()).thenReturn(42L);
    
    setupData(new long[] { 4, 10, 8 }, new long[] { 1, 2, 2 }, 
              new long[] { }, new long[] { }, false);
    
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight(literal)
        .setRightType(OperandType.LITERAL_NUMERIC)
        .setExpressionOp(ExpressionOp.ADD)
        .build();
    when(node.expressionConfig()).thenReturn(expression_config);
    
    ExpressionNumericSummaryIterator iterator = 
        new ExpressionNumericSummaryIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .build());
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(1000, value.timestamp().msEpoch());
    assertEquals(46, value.value().value(0).longValue());
    assertEquals(43, value.value().value(2).longValue());
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(3000, value.timestamp().msEpoch());
    assertEquals(52, value.value().value(0).longValue());
    assertEquals(44, value.value().value(2).longValue());
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(5000, value.timestamp().msEpoch());
    assertEquals(50, value.value().value(0).longValue());
    assertEquals(44, value.value().value(2).longValue());
    
    // subtract
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight(literal)
        .setRightType(OperandType.LITERAL_NUMERIC)
        .setExpressionOp(ExpressionOp.SUBTRACT)
        .build();
    when(node.expressionConfig()).thenReturn(expression_config);
    
    iterator = new ExpressionNumericSummaryIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .build());
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(1000, value.timestamp().msEpoch());
    assertEquals(-38, value.value().value(0).longValue());
    assertEquals(-41, value.value().value(2).longValue());
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(3000, value.timestamp().msEpoch());
    assertEquals(-32, value.value().value(0).longValue());
    assertEquals(-40, value.value().value(2).longValue());
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(5000, value.timestamp().msEpoch());
    assertEquals(-34, value.value().value(0).longValue());
    assertEquals(-40, value.value().value(2).longValue());
    assertFalse(iterator.hasNext());
  }

  @Test
  public void booleanLeft() throws Exception {
    setupData(new long[] { }, new long[] { }, 
              new long[] { 4, 10, 8 }, new long[] { 1, 2, 2 }, false);
    
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft(true)
        .setLeftType(OperandType.LITERAL_BOOL)
        .setRight("b")
        .setRightType(OperandType.VARIABLE)
        .setExpressionOp(ExpressionOp.ADD)
        .build();
    when(node.expressionConfig()).thenReturn(expression_config);
    
    ExpressionNumericSummaryIterator iterator = 
        new ExpressionNumericSummaryIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .build());
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(1000, value.timestamp().msEpoch());
    assertEquals(5, value.value().value(0).longValue());
    assertEquals(2, value.value().value(2).longValue());
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(3000, value.timestamp().msEpoch());
    assertEquals(11, value.value().value(0).longValue());
    assertEquals(3, value.value().value(2).longValue());
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(5000, value.timestamp().msEpoch());
    assertEquals(9, value.value().value(0).longValue());
    assertEquals(3, value.value().value(2).longValue());
    
    // subtract
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeft(true)
        .setLeftType(OperandType.LITERAL_BOOL)
        .setRight("b")
        .setRightType(OperandType.VARIABLE)
        .setExpressionOp(ExpressionOp.SUBTRACT)
        .build();
    when(node.expressionConfig()).thenReturn(expression_config);
    
    iterator = new ExpressionNumericSummaryIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
            .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .build());
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(1000, value.timestamp().msEpoch());
    assertEquals(-3, value.value().value(0).longValue());
    assertEquals(0, value.value().value(2).longValue());
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(3000, value.timestamp().msEpoch());
    assertEquals(-9, value.value().value(0).longValue());
    assertEquals(-1, value.value().value(2).longValue());
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(5000, value.timestamp().msEpoch());
    assertEquals(-7, value.value().value(0).longValue());
    assertEquals(-1, value.value().value(2).longValue());
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void booleanRight() throws Exception {
    setupData(new long[] { 4, 10, 8 }, new long[] { 1, 2, 2 }, 
              new long[] { }, new long[] { }, false);
    
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight(true)
        .setRightType(OperandType.LITERAL_BOOL)
        .setExpressionOp(ExpressionOp.ADD)
        .build();
    when(node.expressionConfig()).thenReturn(expression_config);
    
    ExpressionNumericSummaryIterator iterator = 
        new ExpressionNumericSummaryIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .build());
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(1000, value.timestamp().msEpoch());
    assertEquals(5, value.value().value(0).longValue());
    assertEquals(2, value.value().value(2).longValue());
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(3000, value.timestamp().msEpoch());
    assertEquals(11, value.value().value(0).longValue());
    assertEquals(3, value.value().value(2).longValue());
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(5000, value.timestamp().msEpoch());
    assertEquals(9, value.value().value(0).longValue());
    assertEquals(3, value.value().value(2).longValue());
    
    // subtract
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight(true)
        .setRightType(OperandType.LITERAL_BOOL)
        .setExpressionOp(ExpressionOp.SUBTRACT)
        .build();
    when(node.expressionConfig()).thenReturn(expression_config);
    
    iterator = new ExpressionNumericSummaryIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
            .put(ExpressionTimeSeries.LEFT_KEY, left)
              .build());
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(1000, value.timestamp().msEpoch());
    assertEquals(3, value.value().value(0).longValue());
    assertEquals(0, value.value().value(2).longValue());
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(3000, value.timestamp().msEpoch());
    assertEquals(9, value.value().value(0).longValue());
    assertEquals(1, value.value().value(2).longValue());
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(5000, value.timestamp().msEpoch());
    assertEquals(7, value.value().value(0).longValue());
    assertEquals(1, value.value().value(2).longValue());
    assertFalse(iterator.hasNext());
  }

  @Test
  public void nullLeft() throws Exception {
    setupData(new long[] { }, new long[] { }, 
              new long[] { 4, 10, 8 }, new long[] { 1, 2, 2 }, false);
    
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft(null)
        .setLeftType(OperandType.NULL)
        .setRight("b")
        .setRightType(OperandType.VARIABLE)
        .setExpressionOp(ExpressionOp.ADD)
        .build();
    when(node.expressionConfig()).thenReturn(expression_config);
    
    ExpressionNumericSummaryIterator iterator = 
        new ExpressionNumericSummaryIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .build());
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(1000, value.timestamp().msEpoch());
    assertNull(value.value());
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(3000, value.timestamp().msEpoch());
    assertNull(value.value());
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(5000, value.timestamp().msEpoch());
    assertNull(value.value());
  }
  
  @Test
  public void nullRight() throws Exception {
    setupData(new long[] { 4, 10, 8 }, new long[] { 1, 2, 2 }, 
              new long[] { }, new long[] { }, false);
    
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight(null)
        .setRightType(OperandType.NULL)
        .setExpressionOp(ExpressionOp.ADD)
        .build();
    when(node.expressionConfig()).thenReturn(expression_config);
    
    ExpressionNumericSummaryIterator iterator = 
        new ExpressionNumericSummaryIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .build());
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(1000, value.timestamp().msEpoch());
    assertNull(value.value());
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(3000, value.timestamp().msEpoch());
    assertNull(value.value());
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(5000, value.timestamp().msEpoch());
    assertNull(value.value());
  }
}
