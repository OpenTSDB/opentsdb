//This file is part of OpenTSDB.
//Copyright (C) 2018-2021  The OpenTSDB Authors.
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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.types.numeric.NumericArrayTimeSeries;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.query.processor.expressions.ExpressionParseNode.ExpressionOp;
import net.opentsdb.query.processor.expressions.ExpressionParseNode.OperandType;
import net.opentsdb.query.processor.expressions.ExpressionParser.NumericLiteral;

public class TestExpressionNumericArrayIteratorDivide extends BaseNumericTest {

  private TimeSeries left;
  private TimeSeries right;
  
  @Before
  public void beforeLocal() {
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight("b")
        .setRightType(OperandType.VARIABLE)
        .setExpressionOp(ExpressionOp.DIVIDE)
        .setExpressionConfig(config)
        .setId("expression")
        .build();
    when(node.config()).thenReturn(expression_config);
  }
  
  @Test
  public void longLong() throws Exception {
    left = new NumericArrayTimeSeries(LEFT_ID, 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) left).add(1);
    ((NumericArrayTimeSeries) left).add(4);
    ((NumericArrayTimeSeries) left).add(2);
    
    right = new NumericArrayTimeSeries(RIGHT_ID, 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) right).add(4);
    ((NumericArrayTimeSeries) right).add(2);
    ((NumericArrayTimeSeries) right).add(8);
    
    ExpressionNumericArrayIterator iterator = 
        new ExpressionNumericArrayIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .build());
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new double[] { .25, 2, .25 },
        value.value().doubleArray(), 0.001);
    assertEquals(60, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(3, value.value().end());
    assertFalse(iterator.hasNext());

    // divide w/ same operand
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
            .setLeft("a")
            .setLeftType(OperandType.VARIABLE)
            .setRight("a")
            .setRightType(OperandType.VARIABLE)
            .setExpressionOp(ExpressionOp.DIVIDE)
            .setExpressionConfig(config)
            .setId("expression")
            .build();
    when(node.config()).thenReturn(expression_config);

    iterator = new ExpressionNumericArrayIterator(node, RESULT,
            (Map) ImmutableMap.builder()
                    .put(ExpressionTimeSeries.LEFT_KEY, left)
                    .build());
    assertTrue(iterator.hasNext());
    value =  (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new double[] { 1.0, 1.0, 1.0 },
            value.value().doubleArray(), 0.001);
    assertEquals(60, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(3, value.value().end());
    assertFalse(iterator.hasNext());
  }

  @Test
  public void longLongWithRate() throws Exception {
    left = new NumericArrayTimeSeries(LEFT_ID,
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) left).add(1);
    ((NumericArrayTimeSeries) left).add(4);
    ((NumericArrayTimeSeries) left).add(2);

    right = new NumericArrayTimeSeries(RIGHT_ID, //rate has one dp less than regular
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) right).add(4);
    ((NumericArrayTimeSeries) right).add(2);

    ExpressionNumericArrayIterator iterator =
        new ExpressionNumericArrayIterator(node, RESULT,
            (Map) ImmutableMap.builder()
                .put(ExpressionTimeSeries.LEFT_KEY, left)
                .put(ExpressionTimeSeries.RIGHT_KEY, right)
                .build());
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericArrayType> value =
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new double[] { 1, 1 },
        value.value().doubleArray(), 0.001);
    assertEquals(60, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(2, value.value().end());
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void longLongNegate() throws Exception {
    left = new NumericArrayTimeSeries(LEFT_ID, 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) left).add(1);
    ((NumericArrayTimeSeries) left).add(4);
    ((NumericArrayTimeSeries) left).add(2);
    
    right = new NumericArrayTimeSeries(RIGHT_ID, 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) right).add(4);
    ((NumericArrayTimeSeries) right).add(2);
    ((NumericArrayTimeSeries) right).add(8);
    
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight("b")
        .setRightType(OperandType.VARIABLE)
        .setExpressionOp(ExpressionOp.DIVIDE)
        .setExpressionConfig(config)
        .setNegate(true)
        .setId("expression")
        .build();
    when(node.config()).thenReturn(expression_config);
    
    ExpressionNumericArrayIterator iterator = 
        new ExpressionNumericArrayIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .build());
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new double[] { -.25, -2, -.25 },
        value.value().doubleArray(), 0.001);
    assertEquals(60, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(3, value.value().end());
    assertFalse(iterator.hasNext());
  }

  @Test
  public void longDouble() throws Exception {
    left = new NumericArrayTimeSeries(LEFT_ID, 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) left).add(1);
    ((NumericArrayTimeSeries) left).add(5);
    ((NumericArrayTimeSeries) left).add(2);
    
    right = new NumericArrayTimeSeries(RIGHT_ID, 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) right).add(4.5);
    ((NumericArrayTimeSeries) right).add(10.75);
    ((NumericArrayTimeSeries) right).add(8.9);
    
    ExpressionNumericArrayIterator iterator = 
        new ExpressionNumericArrayIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .build());
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new double[] { .2225, 0.465116279069767, 0.224719101123596 },
        value.value().doubleArray(), 0.001);
    assertEquals(60, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(3, value.value().end());
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void longDoubleNegate() throws Exception {
    left = new NumericArrayTimeSeries(LEFT_ID, 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) left).add(1);
    ((NumericArrayTimeSeries) left).add(5);
    ((NumericArrayTimeSeries) left).add(2);
    
    right = new NumericArrayTimeSeries(RIGHT_ID, 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) right).add(4.5);
    ((NumericArrayTimeSeries) right).add(10.75);
    ((NumericArrayTimeSeries) right).add(8.9);
    
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight("b")
        .setRightType(OperandType.VARIABLE)
        .setExpressionOp(ExpressionOp.DIVIDE)
        .setExpressionConfig(config)
        .setNegate(true)
        .setId("expression")
        .build();
    when(node.config()).thenReturn(expression_config);
    
    ExpressionNumericArrayIterator iterator = 
        new ExpressionNumericArrayIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .build());
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new double[] { -.2225, -0.465116279069767, -0.224719101123596 },
        value.value().doubleArray(), 0.001);
    assertEquals(60, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(3, value.value().end());
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void doubleDouble() throws Exception {
    left = new NumericArrayTimeSeries(LEFT_ID, 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) left).add(1.1);
    ((NumericArrayTimeSeries) left).add(5.33);
    ((NumericArrayTimeSeries) left).add(2.66);
    
    right = new NumericArrayTimeSeries(RIGHT_ID, 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) right).add(4.5);
    ((NumericArrayTimeSeries) right).add(10.75);
    ((NumericArrayTimeSeries) right).add(8.9);
    
    ExpressionNumericArrayIterator iterator = 
        new ExpressionNumericArrayIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .build());
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new double[] { 0.244444444444444, 0.495813953488372, 0.298876404494382 },
        value.value().doubleArray(), 0.001);
    assertEquals(60, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(3, value.value().end());
    assertFalse(iterator.hasNext());
  }

  @Test
  public void doubleDoubleWithRate() throws Exception {
    left = new NumericArrayTimeSeries(LEFT_ID,
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) left).add(1.1);
    ((NumericArrayTimeSeries) left).add(5.33);
    ((NumericArrayTimeSeries) left).add(2.66);

    right = new NumericArrayTimeSeries(RIGHT_ID,
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) right).add(4.5);
    ((NumericArrayTimeSeries) right).add(10.75);

    ExpressionNumericArrayIterator iterator =
        new ExpressionNumericArrayIterator(node, RESULT,
            (Map) ImmutableMap.builder()
                .put(ExpressionTimeSeries.LEFT_KEY, left)
                .put(ExpressionTimeSeries.RIGHT_KEY, right)
                .build());
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericArrayType> value =
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new double[] { 1.1844444444,  0.2474418605},
        value.value().doubleArray(), 0.001);
    assertEquals(60, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(2, value.value().end());
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void divideByZero() throws Exception {
    left = new NumericArrayTimeSeries(LEFT_ID, 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) left).add(1.1);
    ((NumericArrayTimeSeries) left).add(0);
    ((NumericArrayTimeSeries) left).add(2.66);
    
    right = new NumericArrayTimeSeries(RIGHT_ID, 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) right).add(4.5);
    ((NumericArrayTimeSeries) right).add(10.75);
    ((NumericArrayTimeSeries) right).add(0);
    
    ExpressionNumericArrayIterator iterator = 
        new ExpressionNumericArrayIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .build());
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new double[] { 0.244444444444444, 0, 0 },
        value.value().doubleArray(), 0.001);
    assertEquals(60, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(3, value.value().end());
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void fillNaN() throws Exception {
    left = new NumericArrayTimeSeries(LEFT_ID, 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) left).add(1.1);
    ((NumericArrayTimeSeries) left).add(Double.NaN);
    ((NumericArrayTimeSeries) left).add(2.66);
    
    right = new NumericArrayTimeSeries(RIGHT_ID, 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) right).add(4.5);
    ((NumericArrayTimeSeries) right).add(10.75);
    ((NumericArrayTimeSeries) right).add(Double.NaN);
    
    ExpressionNumericArrayIterator iterator = 
        new ExpressionNumericArrayIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .build());
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new double[] { 0.244444444444444, 0, 0 },
        value.value().doubleArray(), 0.001);
    assertEquals(60, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(3, value.value().end());
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void fillNaNInfectious() throws Exception {
    ExpressionConfig e = ExpressionConfig.newBuilder()
        .setExpression("a / b")
        .setJoinConfig(JOIN_CONFIG)
        .setInfectiousNan(true)
        .addInterpolatorConfig(NUMERIC_CONFIG)
        .setId("e1")
        .build();
    when(node.expressionConfig()).thenReturn(e);
    
    left = new NumericArrayTimeSeries(LEFT_ID, 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) left).add(1.1);
    ((NumericArrayTimeSeries) left).add(Double.NaN);
    ((NumericArrayTimeSeries) left).add(2.66);
    
    right = new NumericArrayTimeSeries(RIGHT_ID, 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) right).add(4.5);
    ((NumericArrayTimeSeries) right).add(10.75);
    ((NumericArrayTimeSeries) right).add(Double.NaN);
    
    ExpressionNumericArrayIterator iterator = 
        new ExpressionNumericArrayIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .build());
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new double[] { 0.244444444444444, Double.NaN, Double.NaN },
        value.value().doubleArray(), 0.001);
    assertEquals(60, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(3, value.value().end());
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void fillNaNInfectiousNegate() throws Exception {
    ExpressionConfig e = ExpressionConfig.newBuilder()
        .setExpression("a / b")
        .setJoinConfig(JOIN_CONFIG)
        .setInfectiousNan(true)
        .addInterpolatorConfig(NUMERIC_CONFIG)
        .setId("e1")
        .build();
    when(node.expressionConfig()).thenReturn(e);
    
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight("b")
        .setRightType(OperandType.VARIABLE)
        .setExpressionOp(ExpressionOp.DIVIDE)
        .setExpressionConfig(config)
        .setNegate(true)
        .setId("expression")
        .build();
    when(node.config()).thenReturn(expression_config);
    
    left = new NumericArrayTimeSeries(LEFT_ID, 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) left).add(1.1);
    ((NumericArrayTimeSeries) left).add(Double.NaN);
    ((NumericArrayTimeSeries) left).add(2.66);
    
    right = new NumericArrayTimeSeries(RIGHT_ID, 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) right).add(4.5);
    ((NumericArrayTimeSeries) right).add(10.75);
    ((NumericArrayTimeSeries) right).add(Double.NaN);
    
    ExpressionNumericArrayIterator iterator = 
        new ExpressionNumericArrayIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .build());
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new double[] { -0.244444444444444, Double.NaN, Double.NaN },
        value.value().doubleArray(), 0.001);
    assertEquals(60, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(3, value.value().end());
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void numericLiteralLeft() throws Exception {
    NumericLiteral literal = mock(NumericLiteral.class);
    when(literal.isInteger()).thenReturn(true);
    when(literal.longValue()).thenReturn(42L);
    
    right = new NumericArrayTimeSeries(RIGHT_ID, 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) right).add(4);
    ((NumericArrayTimeSeries) right).add(10);
    ((NumericArrayTimeSeries) right).add(8);
    
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft(literal)
        .setLeftType(OperandType.LITERAL_NUMERIC)
        .setRight("b")
        .setRightType(OperandType.VARIABLE)
        .setExpressionOp(ExpressionOp.DIVIDE)
        .setExpressionConfig(config)
        .setId("expression")
        .build();
    when(node.config()).thenReturn(expression_config);
    
    ExpressionNumericArrayIterator iterator = 
        new ExpressionNumericArrayIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .build());
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new double[] { 10.5, 4.2, 5.25 },
        value.value().doubleArray(), 0.001);
    assertEquals(60, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(3, value.value().end());
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void numericLiteralLeftAndNan() throws Exception {
    NumericLiteral literal = mock(NumericLiteral.class);
    when(literal.isInteger()).thenReturn(true);
    when(literal.longValue()).thenReturn(42L);
    
    right = new NumericArrayTimeSeries(RIGHT_ID, 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) right).add(4);
    ((NumericArrayTimeSeries) right).add(Double.NaN);
    ((NumericArrayTimeSeries) right).add(8);
    
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft(literal)
        .setLeftType(OperandType.LITERAL_NUMERIC)
        .setRight("b")
        .setRightType(OperandType.VARIABLE)
        .setExpressionOp(ExpressionOp.DIVIDE)
        .setExpressionConfig(config)
        .setId("expression")
        .build();
    when(node.config()).thenReturn(expression_config);
    
    ExpressionNumericArrayIterator iterator = 
        new ExpressionNumericArrayIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .build());
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new double[] { 10.5, 0, 5.25 },
        value.value().doubleArray(), 0.001);
    assertEquals(60, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(3, value.value().end());
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void numericLiteralLeftAndNanInfectious() throws Exception {
    ExpressionConfig e = ExpressionConfig.newBuilder()
        .setExpression("a / b")
        .setJoinConfig(JOIN_CONFIG)
        .setInfectiousNan(true)
        .addInterpolatorConfig(NUMERIC_CONFIG)
        .setId("e1")
        .build();
    when(node.expressionConfig()).thenReturn(e);
    
    NumericLiteral literal = mock(NumericLiteral.class);
    when(literal.isInteger()).thenReturn(true);
    when(literal.longValue()).thenReturn(42L);
    
    right = new NumericArrayTimeSeries(RIGHT_ID, 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) right).add(4);
    ((NumericArrayTimeSeries) right).add(Double.NaN);
    ((NumericArrayTimeSeries) right).add(8);
    
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft(literal)
        .setLeftType(OperandType.LITERAL_NUMERIC)
        .setRight("b")
        .setRightType(OperandType.VARIABLE)
        .setExpressionOp(ExpressionOp.DIVIDE)
        .setExpressionConfig(config)
        .setId("expression")
        .build();
    when(node.config()).thenReturn(expression_config);
    
    ExpressionNumericArrayIterator iterator = 
        new ExpressionNumericArrayIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .build());
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new double[] { 10.5, Double.NaN, 5.25 },
        value.value().doubleArray(), 0.001);
    assertEquals(60, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(3, value.value().end());
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void numericLiteralRight() throws Exception {
    NumericLiteral literal = mock(NumericLiteral.class);
    when(literal.isInteger()).thenReturn(true);
    when(literal.longValue()).thenReturn(42L);
    
    left = new NumericArrayTimeSeries(LEFT_ID, 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) left).add(4);
    ((NumericArrayTimeSeries) left).add(10);
    ((NumericArrayTimeSeries) left).add(8);
    
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight(literal)
        .setRightType(OperandType.LITERAL_NUMERIC)
        .setExpressionOp(ExpressionOp.DIVIDE)
        .setExpressionConfig(config)
        .setId("expression")
        .build();
    when(node.config()).thenReturn(expression_config);
    
    ExpressionNumericArrayIterator iterator = 
        new ExpressionNumericArrayIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .build());
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new double[] { 0.095238095238095, 0.238095238095238, 0.19047619047619 },
        value.value().doubleArray(), 0.001);
    assertEquals(60, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(3, value.value().end());
    assertFalse(iterator.hasNext());
    
    // divide by zero integer
    when(literal.longValue()).thenReturn(0L);
    
    iterator = new ExpressionNumericArrayIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .build());
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new double[] { 0, 0, 0 },
        value.value().doubleArray(), 0.001);
    assertEquals(60, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(3, value.value().end());
    assertFalse(iterator.hasNext());
    
    // divide by zero float
    when(literal.isInteger()).thenReturn(false);
    when(literal.doubleValue()).thenReturn(0.0);
    
    iterator = new ExpressionNumericArrayIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .build());
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new double[] { 0, 0, 0 },
        value.value().doubleArray(), 0.001);
    assertEquals(60, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(3, value.value().end());
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void numericLiteralRightAndNan() throws Exception {
    NumericLiteral literal = mock(NumericLiteral.class);
    when(literal.isInteger()).thenReturn(true);
    when(literal.longValue()).thenReturn(42L);
    
    left = new NumericArrayTimeSeries(LEFT_ID, 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) left).add(4);
    ((NumericArrayTimeSeries) left).add(Double.NaN);
    ((NumericArrayTimeSeries) left).add(8);
    
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight(literal)
        .setRightType(OperandType.LITERAL_NUMERIC)
        .setExpressionOp(ExpressionOp.DIVIDE)
        .setExpressionConfig(config)
        .setId("expression")
        .build();
    when(node.config()).thenReturn(expression_config);
    
    ExpressionNumericArrayIterator iterator = 
        new ExpressionNumericArrayIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .build());
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new double[] { 0.095238095238095, 0, 0.19047619047619 },
        value.value().doubleArray(), 0.001);
    assertEquals(60, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(3, value.value().end());
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void numericLiteralRightAndNanInfectious() throws Exception {
    ExpressionConfig e = ExpressionConfig.newBuilder()
        .setExpression("a / b")
        .setJoinConfig(JOIN_CONFIG)
        .setInfectiousNan(true)
        .addInterpolatorConfig(NUMERIC_CONFIG)
        .setId("e1")
        .build();
    when(node.expressionConfig()).thenReturn(e);
    
    NumericLiteral literal = mock(NumericLiteral.class);
    when(literal.isInteger()).thenReturn(true);
    when(literal.longValue()).thenReturn(42L);
    
    left = new NumericArrayTimeSeries(LEFT_ID, 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) left).add(4);
    ((NumericArrayTimeSeries) left).add(Double.NaN);
    ((NumericArrayTimeSeries) left).add(8);
    
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight(literal)
        .setRightType(OperandType.LITERAL_NUMERIC)
        .setExpressionOp(ExpressionOp.DIVIDE)
        .setExpressionConfig(config)
        .setId("expression")
        .build();
    when(node.config()).thenReturn(expression_config);
    
    ExpressionNumericArrayIterator iterator = 
        new ExpressionNumericArrayIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .build());
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new double[] { 0.095238095238095, Double.NaN, 0.19047619047619 },
        value.value().doubleArray(), 0.001);
    assertEquals(60, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(3, value.value().end());
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void booleanLeft() throws Exception {
    right = new NumericArrayTimeSeries(RIGHT_ID, 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) right).add(4);
    ((NumericArrayTimeSeries) right).add(10);
    ((NumericArrayTimeSeries) right).add(8);
    
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft(true)
        .setLeftType(OperandType.LITERAL_BOOL)
        .setRight("b")
        .setRightType(OperandType.VARIABLE)
        .setExpressionOp(ExpressionOp.DIVIDE)
        .setExpressionConfig(config)
        .setId("expression")
        .build();
    when(node.config()).thenReturn(expression_config);
    
    ExpressionNumericArrayIterator iterator = 
        new ExpressionNumericArrayIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .build());
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new double[] { .25, .1, .125 },
        value.value().doubleArray(), 0.001);
    assertEquals(60, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(3, value.value().end());
    assertFalse(iterator.hasNext());
    
    // with false == 0
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft(false)
        .setLeftType(OperandType.LITERAL_BOOL)
        .setRight("b")
        .setRightType(OperandType.VARIABLE)
        .setExpressionOp(ExpressionOp.DIVIDE)
        .setExpressionConfig(config)
        .setId("expression")
        .build();
    when(node.config()).thenReturn(expression_config);
    
    iterator = new ExpressionNumericArrayIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .build());
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new double[] { 0, 0, 0 },
        value.value().doubleArray(), 0.001);
    assertEquals(60, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(3, value.value().end());
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void booleanRight() throws Exception {
    left = new NumericArrayTimeSeries(LEFT_ID, 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) left).add(4);
    ((NumericArrayTimeSeries) left).add(10);
    ((NumericArrayTimeSeries) left).add(8);
    
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight(true)
        .setRightType(OperandType.LITERAL_BOOL)
        .setExpressionOp(ExpressionOp.DIVIDE)
        .setExpressionConfig(config)
        .setId("expression")
        .build();
    when(node.config()).thenReturn(expression_config);
    
    ExpressionNumericArrayIterator iterator = 
        new ExpressionNumericArrayIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .build());
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new double[] { 4, 10, 8 },
        value.value().doubleArray(), 0.001);
    assertEquals(60, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(3, value.value().end());
    assertFalse(iterator.hasNext());
    
    // with false == 0
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight(false)
        .setRightType(OperandType.LITERAL_BOOL)
        .setExpressionOp(ExpressionOp.DIVIDE)
        .setExpressionConfig(config)
        .setId("expression")
        .build();
    when(node.config()).thenReturn(expression_config);
    
    iterator = new ExpressionNumericArrayIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .build());
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new double[] { 0, 0, 0 },
        value.value().doubleArray(), 0.001);
    assertEquals(60, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(3, value.value().end());
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void booleanRightNegate() throws Exception {
    left = new NumericArrayTimeSeries(LEFT_ID, 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) left).add(4);
    ((NumericArrayTimeSeries) left).add(10);
    ((NumericArrayTimeSeries) left).add(8);
    
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight(true)
        .setRightType(OperandType.LITERAL_BOOL)
        .setExpressionOp(ExpressionOp.DIVIDE)
        .setExpressionConfig(config)
        .setNegate(true)
        .setId("expression")
        .build();
    when(node.config()).thenReturn(expression_config);
    
    ExpressionNumericArrayIterator iterator = 
        new ExpressionNumericArrayIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .build());
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new double[] { -4, -10, -8 },
        value.value().doubleArray(), 0.001);
    assertEquals(60, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(3, value.value().end());
    assertFalse(iterator.hasNext());
    
    // with false == 0
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight(false)
        .setRightType(OperandType.LITERAL_BOOL)
        .setExpressionOp(ExpressionOp.DIVIDE)
        .setExpressionConfig(config)
        .setNegate(true)
        .setId("expression")
        .build();
    when(node.config()).thenReturn(expression_config);
    
    iterator = new ExpressionNumericArrayIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .build());
    assertTrue(iterator.hasNext());
    value = (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new double[] { 0, 0, 0 },
        value.value().doubleArray(), 0.001);
    assertEquals(60, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(3, value.value().end());
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nullLeft() throws Exception {
    right = new NumericArrayTimeSeries(RIGHT_ID, 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) right).add(4);
    ((NumericArrayTimeSeries) right).add(10);
    ((NumericArrayTimeSeries) right).add(8);
    
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft(null)
        .setLeftType(OperandType.NULL)
        .setRight("b")
        .setRightType(OperandType.VARIABLE)
        .setExpressionOp(ExpressionOp.DIVIDE)
        .setExpressionConfig(config)
        .setId("expression")
        .build();
    when(node.config()).thenReturn(expression_config);
    
    ExpressionNumericArrayIterator iterator = 
        new ExpressionNumericArrayIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .build());
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new double[] { 0, 0, 0 },
        value.value().doubleArray(), 0.001);
    assertEquals(60, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(3, value.value().end());
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nullLeftNegate() throws Exception {
    right = new NumericArrayTimeSeries(RIGHT_ID, 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) right).add(4);
    ((NumericArrayTimeSeries) right).add(10);
    ((NumericArrayTimeSeries) right).add(8);
    
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft(null)
        .setLeftType(OperandType.NULL)
        .setRight("b")
        .setRightType(OperandType.VARIABLE)
        .setExpressionOp(ExpressionOp.DIVIDE)
        .setExpressionConfig(config)
        .setNegate(true)
        .setId("expression")
        .build();
    when(node.config()).thenReturn(expression_config);
    
    ExpressionNumericArrayIterator iterator = 
        new ExpressionNumericArrayIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .build());
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new double[] { 0, 0, 0 },
        value.value().doubleArray(), 0.001);
    assertEquals(60, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(3, value.value().end());
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nullLeftInfectious() throws Exception {
    ExpressionConfig e = ExpressionConfig.newBuilder()
        .setExpression("a / b")
        .setJoinConfig(JOIN_CONFIG)
        .setInfectiousNan(true)
        .addInterpolatorConfig(NUMERIC_CONFIG)
        .setId("e1")
        .build();
    when(node.expressionConfig()).thenReturn(e);
    
    right = new NumericArrayTimeSeries(RIGHT_ID, 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) right).add(4);
    ((NumericArrayTimeSeries) right).add(10);
    ((NumericArrayTimeSeries) right).add(8);
    
    ExpressionNumericArrayIterator iterator = 
        new ExpressionNumericArrayIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .build());
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nullRight() throws Exception {
    left = new NumericArrayTimeSeries(LEFT_ID, 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) left).add(4);
    ((NumericArrayTimeSeries) left).add(10);
    ((NumericArrayTimeSeries) left).add(8);
    
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight(null)
        .setRightType(OperandType.NULL)
        .setExpressionOp(ExpressionOp.DIVIDE)
        .setExpressionConfig(config)
        .setId("expression")
        .build();
    when(node.config()).thenReturn(expression_config);
    
    ExpressionNumericArrayIterator iterator = 
        new ExpressionNumericArrayIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .build());
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new double[] { 0, 0, 0 },
        value.value().doubleArray(), 0.001);
    assertEquals(60, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(3, value.value().end());
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nullRightNegate() throws Exception {
    left = new NumericArrayTimeSeries(LEFT_ID, 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) left).add(4);
    ((NumericArrayTimeSeries) left).add(10);
    ((NumericArrayTimeSeries) left).add(8);
    
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight(null)
        .setRightType(OperandType.NULL)
        .setExpressionOp(ExpressionOp.DIVIDE)
        .setExpressionConfig(config)
        .setNegate(true)
        .setId("expression")
        .build();
    when(node.config()).thenReturn(expression_config);
    
    ExpressionNumericArrayIterator iterator = 
        new ExpressionNumericArrayIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .build());
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new double[] { 0, 0, 0 },
        value.value().doubleArray(), 0.001);
    assertEquals(60, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(3, value.value().end());
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nullRightInfectious() throws Exception {
    ExpressionConfig cfg = ExpressionConfig.newBuilder()
        .setExpression("a / b")
        .setJoinConfig(JOIN_CONFIG)
        .addInterpolatorConfig(NUMERIC_CONFIG)
        .setInfectiousNan(true)
        .setId("e1")
        .build();
    when(node.config()).thenReturn(expression_config);
    
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight(null)
        .setRightType(OperandType.NULL)
        .setExpressionOp(ExpressionOp.DIVIDE)
        .setExpressionConfig(config)
        .setId("expression")
        .build();
    when(node.expressionConfig()).thenReturn(cfg);
    
    left = new NumericArrayTimeSeries(LEFT_ID, 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) left).add(4);
    ((NumericArrayTimeSeries) left).add(10);
    ((NumericArrayTimeSeries) left).add(8);
    
    ExpressionNumericArrayIterator iterator = 
        new ExpressionNumericArrayIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .build());
    assertFalse(iterator.hasNext());
  }

  @Test
  public void testSelfDivide() throws Exception {
    left = new NumericArrayTimeSeries(LEFT_ID,
            new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) left).add(1);
    ((NumericArrayTimeSeries) left).add(5);
    ((NumericArrayTimeSeries) left).add(2);

    right = new NumericArrayTimeSeries(RIGHT_ID,
            new SecondTimeStamp(60));

    ExpressionNumericArrayIterator iterator =
            new ExpressionNumericArrayIterator(node, RESULT,
                    (Map) ImmutableMap.builder()
                            .put(ExpressionTimeSeries.LEFT_KEY, left)
                            .put(ExpressionTimeSeries.RIGHT_KEY, left)
                            .build());
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericArrayType> value =
            (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new double[] { 1, 1, 1 },
            value.value().doubleArray(), 0.001);
    assertEquals(60, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(3, value.value().end());
    assertFalse(iterator.hasNext());
  }
}
