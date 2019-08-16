//This file is part of OpenTSDB.
//Copyright (C) 2018  The OpenTSDB Authors.
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may getNot use this file except in compliance with the License.
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

import org.junit.Test;
import org.powermock.reflect.Whitebox;

import com.google.common.collect.ImmutableMap;

import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.types.numeric.NumericArrayTimeSeries;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.query.processor.expressions.ExpressionParseNode.ExpressionOp;
import net.opentsdb.query.processor.expressions.ExpressionParseNode.OperandType;
import net.opentsdb.query.processor.expressions.ExpressionParser.NumericLiteral;

public class TestExpressionNumericArrayIteratorAdditive extends BaseNumericTest {

  private TimeSeries left;
  private TimeSeries right;
  
  @Test
  public void longLong() throws Exception {
    left = new NumericArrayTimeSeries(LEFT_ID, 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) left).add(1);
    ((NumericArrayTimeSeries) left).add(5);
    ((NumericArrayTimeSeries) left).add(2);
    
    right = new NumericArrayTimeSeries(RIGHT_ID, 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) right).add(4);
    ((NumericArrayTimeSeries) right).add(10);
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
    assertArrayEquals(new long[] { 5, 15, 10 },
        value.value().longArray());
    assertEquals(60, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(3, value.value().end());
    assertFalse(iterator.hasNext());
    
    // subtract
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight(null)
        .setRightType(OperandType.NULL)
        .setExpressionOp(ExpressionOp.SUBTRACT)
        .setExpressionConfig(CONFIG)
        .setId("expression")
        .build();
    when(node.config()).thenReturn(expression_config);
    
    iterator = new ExpressionNumericArrayIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .build());
    assertTrue(iterator.hasNext());
    value =  (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new long[] { -3, -5, -6 },
        value.value().longArray());
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
    ((NumericArrayTimeSeries) left).add(5);
    ((NumericArrayTimeSeries) left).add(2);

    right = new NumericArrayTimeSeries(RIGHT_ID, // rate has one dp less than regular
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) right).add(4);
    ((NumericArrayTimeSeries) right).add(10);

    ExpressionNumericArrayIterator iterator =
        new ExpressionNumericArrayIterator(node, RESULT,
            (Map) ImmutableMap.builder()
                .put(ExpressionTimeSeries.LEFT_KEY, left)
                .put(ExpressionTimeSeries.RIGHT_KEY, right)
                .build());
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericArrayType> value =
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new long[] { 9, 12 },
        value.value().longArray());
    assertEquals(60, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(2, value.value().end());
    assertFalse(iterator.hasNext());

    // subtract
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight(null)
        .setRightType(OperandType.NULL)
        .setExpressionOp(ExpressionOp.SUBTRACT)
        .setExpressionConfig(CONFIG)
        .setId("expression")
        .build();
    when(node.config()).thenReturn(expression_config);

    iterator = new ExpressionNumericArrayIterator(node, RESULT,
        (Map) ImmutableMap.builder()
            .put(ExpressionTimeSeries.LEFT_KEY, left)
            .put(ExpressionTimeSeries.RIGHT_KEY, right)
            .build());
    assertTrue(iterator.hasNext());
    value =  (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new long[] { 1, -8 },
        value.value().longArray());
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
    ((NumericArrayTimeSeries) left).add(5);
    ((NumericArrayTimeSeries) left).add(2);
    
    right = new NumericArrayTimeSeries(RIGHT_ID, 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) right).add(4);
    ((NumericArrayTimeSeries) right).add(10);
    ((NumericArrayTimeSeries) right).add(8);
    
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight("b")
        .setRightType(OperandType.VARIABLE)
        .setExpressionOp(ExpressionOp.ADD)
        .setExpressionConfig(CONFIG)
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
    assertArrayEquals(new long[] { -5, -15, -10 },
        value.value().longArray());
    assertEquals(60, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(3, value.value().end());
    assertFalse(iterator.hasNext());
    
    // subtract
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight(null)
        .setRightType(OperandType.NULL)
        .setExpressionOp(ExpressionOp.SUBTRACT)
        .setExpressionConfig(CONFIG)
        .setNegate(true)
        .setId("expression")
        .build();
    when(node.config()).thenReturn(expression_config);
    
    iterator = new ExpressionNumericArrayIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .build());
    assertTrue(iterator.hasNext());
    value =  (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new long[] { 3, 5, 6 },
        value.value().longArray());
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
    assertArrayEquals(new double[] { 5.5, 15.75, 10.9 },
        value.value().doubleArray(), 0.001);
    assertEquals(60, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(3, value.value().end());
    assertFalse(iterator.hasNext());
    
    // subtract
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight(null)
        .setRightType(OperandType.NULL)
        .setExpressionOp(ExpressionOp.SUBTRACT)
        .setExpressionConfig(CONFIG)
        .setId("expression")
        .build();
    when(node.config()).thenReturn(expression_config);
    
    iterator = new ExpressionNumericArrayIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .build());
    assertTrue(iterator.hasNext());
    value =  (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new double[] { -3.5, -5.75, -6.9 },
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
        .setExpressionOp(ExpressionOp.ADD)
        .setExpressionConfig(CONFIG)
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
    assertArrayEquals(new double[] { -5.5, -15.75, -10.9 },
        value.value().doubleArray(), 0.001);
    assertEquals(60, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(3, value.value().end());
    assertFalse(iterator.hasNext());
    
    // subtract
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight(null)
        .setRightType(OperandType.NULL)
        .setExpressionOp(ExpressionOp.SUBTRACT)
        .setExpressionConfig(CONFIG)
        .setNegate(true)
        .setId("expression")
        .build();
    when(node.config()).thenReturn(expression_config);
    
    iterator = new ExpressionNumericArrayIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .build());
    assertTrue(iterator.hasNext());
    value =  (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new double[] { 3.5, 5.75, 6.9 },
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
    assertArrayEquals(new double[] { 5.6, 16.08, 11.56 },
        value.value().doubleArray(), 0.001);
    assertEquals(60, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(3, value.value().end());
    assertFalse(iterator.hasNext());
    
    // subtract
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight(null)
        .setRightType(OperandType.NULL)
        .setExpressionOp(ExpressionOp.SUBTRACT)
        .setExpressionConfig(CONFIG)
        .setId("expression")
        .build();
    when(node.config()).thenReturn(expression_config);
    
    iterator = new ExpressionNumericArrayIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .build());
    assertTrue(iterator.hasNext());
    value =  (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new double[] { -3.4, -5.42, -6.24 },
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
    assertArrayEquals(new double[] { 9.83, 13.41},
        value.value().doubleArray(), 0.001);
    assertEquals(60, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(2, value.value().end());
    assertFalse(iterator.hasNext());

    // subtract
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight(null)
        .setRightType(OperandType.NULL)
        .setExpressionOp(ExpressionOp.SUBTRACT)
        .setExpressionConfig(CONFIG)
        .setId("expression")
        .build();
    when(node.config()).thenReturn(expression_config);

    iterator = new ExpressionNumericArrayIterator(node, RESULT,
        (Map) ImmutableMap.builder()
            .put(ExpressionTimeSeries.LEFT_KEY, left)
            .put(ExpressionTimeSeries.RIGHT_KEY, right)
            .build());
    assertTrue(iterator.hasNext());
    value =  (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new double[] { 0.83, -8.09},
        value.value().doubleArray(), 0.001);
    assertEquals(60, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(2, value.value().end());
    assertFalse(iterator.hasNext());
  }

  @Test
  public void fillNaNNonInfectious() throws Exception {
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
    assertArrayEquals(new double[] { 5.6, 10.75, 2.66 },
        value.value().doubleArray(), 0.001);
    assertEquals(60, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(3, value.value().end());
    assertFalse(iterator.hasNext());
    
    // subtract
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight(null)
        .setRightType(OperandType.NULL)
        .setExpressionOp(ExpressionOp.SUBTRACT)
        .setExpressionConfig(CONFIG)
        .setId("expression")
        .build();
    when(node.config()).thenReturn(expression_config);
    
    iterator = new ExpressionNumericArrayIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .build());
    assertTrue(iterator.hasNext());
    value =  (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new double[] { -3.4, 10.75, 2.66 },
        value.value().doubleArray(), 0.001);
    assertEquals(60, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(3, value.value().end());
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void fillNaNInfectious() throws Exception {
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
    Whitebox.setInternalState(iterator, "infectious_nan", true);
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new double[] { 5.6, Double.NaN, Double.NaN },
        value.value().doubleArray(), 0.001);
    assertEquals(60, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(3, value.value().end());
    assertFalse(iterator.hasNext());
    
    // subtract
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight(null)
        .setRightType(OperandType.NULL)
        .setExpressionOp(ExpressionOp.SUBTRACT)
        .setExpressionConfig(CONFIG)
        .setId("expression")
        .build();
    when(node.config()).thenReturn(expression_config);
    
    iterator = new ExpressionNumericArrayIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .build());
    Whitebox.setInternalState(iterator, "infectious_nan", true);
    assertTrue(iterator.hasNext());
    value =  (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new double[] { -3.4, Double.NaN, Double.NaN },
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
        .setExpressionOp(ExpressionOp.ADD)
        .setExpressionConfig(CONFIG)
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
    assertArrayEquals(new long[] { 46, 52, 50 },
        value.value().longArray());
    assertEquals(60, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(3, value.value().end());
    assertFalse(iterator.hasNext());
    
    // subtract
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft(literal)
        .setLeftType(OperandType.LITERAL_NUMERIC)
        .setRight("b")
        .setRightType(OperandType.VARIABLE)
        .setExpressionOp(ExpressionOp.SUBTRACT)
        .setExpressionConfig(CONFIG)
        .setId("expression")
        .build();
    when(node.config()).thenReturn(expression_config);
    
    iterator = new ExpressionNumericArrayIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .build());
    assertTrue(iterator.hasNext());
    value =  (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new long[] { 38, 32, 34 },
        value.value().longArray());
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
    ((NumericArrayTimeSeries) left).add(1);
    ((NumericArrayTimeSeries) left).add(5);
    ((NumericArrayTimeSeries) left).add(2);
        
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight(literal)
        .setRightType(OperandType.LITERAL_NUMERIC)
        .setExpressionOp(ExpressionOp.ADD)
        .setExpressionConfig(CONFIG)
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
    assertArrayEquals(new long[] { 43, 47, 44 },
        value.value().longArray());
    assertEquals(60, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(3, value.value().end());
    assertFalse(iterator.hasNext());
    
    // subtract
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight(literal)
        .setRightType(OperandType.LITERAL_NUMERIC)
        .setExpressionOp(ExpressionOp.SUBTRACT)
        .setExpressionConfig(CONFIG)
        .setId("expression")
        .build();
    when(node.config()).thenReturn(expression_config);
    
    iterator = new ExpressionNumericArrayIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .build());
    assertTrue(iterator.hasNext());
    value =  (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new long[] { -41, -37, -40 },
        value.value().longArray());
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
        .setExpressionOp(ExpressionOp.ADD)
        .setExpressionConfig(CONFIG)
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
    assertArrayEquals(new long[] { 5, 11, 9 },
        value.value().longArray());
    assertEquals(60, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(3, value.value().end());
    assertFalse(iterator.hasNext());
    
    // subtract
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeft(true)
        .setLeftType(OperandType.LITERAL_BOOL)
        .setRight("b")
        .setRightType(OperandType.VARIABLE)
        .setExpressionOp(ExpressionOp.SUBTRACT)
        .setExpressionConfig(CONFIG)
        .setId("expression")
        .build();
    when(node.config()).thenReturn(expression_config);
    
    iterator = new ExpressionNumericArrayIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .build());
    assertTrue(iterator.hasNext());
    value =  (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new long[] { -3, -9, -7 },
        value.value().longArray());
    assertEquals(60, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(3, value.value().end());
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void booleanRight() throws Exception {
    left = new NumericArrayTimeSeries(LEFT_ID, 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) left).add(1);
    ((NumericArrayTimeSeries) left).add(5);
    ((NumericArrayTimeSeries) left).add(2);
        
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight(true)
        .setRightType(OperandType.LITERAL_BOOL)
        .setExpressionOp(ExpressionOp.ADD)
        .setExpressionConfig(CONFIG)
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
    assertArrayEquals(new long[] { 2, 6, 3 },
        value.value().longArray());
    assertEquals(60, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(3, value.value().end());
    assertFalse(iterator.hasNext());
    
    // subtract
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight(true)
        .setRightType(OperandType.LITERAL_BOOL)
        .setExpressionOp(ExpressionOp.SUBTRACT)
        .setExpressionConfig(CONFIG)
        .setId("expression")
        .build();
    when(node.config()).thenReturn(expression_config);
    
    iterator = new ExpressionNumericArrayIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .build());
    assertTrue(iterator.hasNext());
    value =  (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new long[] { 0, 4, 1 },
        value.value().longArray());
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
        .setExpressionOp(ExpressionOp.ADD)
        .setExpressionConfig(CONFIG)
        .setId("expression")
        .build();
    when(node.config()).thenReturn(expression_config);
    
    ExpressionNumericArrayIterator iterator = 
        new ExpressionNumericArrayIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .build());
    assertFalse(iterator.hasNext());
    
    // subtract
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft(null)
        .setLeftType(OperandType.NULL)
        .setRight("b")
        .setRightType(OperandType.VARIABLE)
        .setExpressionOp(ExpressionOp.SUBTRACT)
        .setExpressionConfig(CONFIG)
        .setId("expression")
        .build();
    when(node.config()).thenReturn(expression_config);
    
    iterator = new ExpressionNumericArrayIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .build());
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nullLeftSubstitute() throws Exception {
    right = new NumericArrayTimeSeries(RIGHT_ID, 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) right).add(4);
    ((NumericArrayTimeSeries) right).add(10);
    ((NumericArrayTimeSeries) right).add(8);
    
    ExpressionConfig cfg = ExpressionConfig.newBuilder()
        .setExpression("a + b")
        .setJoinConfig(JOIN_CONFIG)
        .addInterpolatorConfig(NUMERIC_CONFIG)
        .setSubstituteMissing(true)
        .setId("e1")
        .build();
    
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft(null)
        .setLeftType(OperandType.NULL)
        .setRight("b")
        .setRightType(OperandType.VARIABLE)
        .setExpressionOp(ExpressionOp.ADD)
        .setExpressionConfig(cfg)
        .setId("expression")
        .build();
    when(node.config()).thenReturn(expression_config);
    when(node.expressionConfig()).thenReturn(cfg);
    
    ExpressionNumericArrayIterator iterator = 
        new ExpressionNumericArrayIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .build());
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new long[] { 4, 10, 8 },
        value.value().longArray());
    assertEquals(60, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(3, value.value().end());
    assertFalse(iterator.hasNext());
    
    // subtract
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft(null)
        .setLeftType(OperandType.NULL)
        .setRight("b")
        .setRightType(OperandType.VARIABLE)
        .setExpressionOp(ExpressionOp.SUBTRACT)
        .setExpressionConfig(CONFIG)
        .setId("expression")
        .build();
    when(node.config()).thenReturn(expression_config);
    
    iterator = new ExpressionNumericArrayIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .build());
    assertTrue(iterator.hasNext());
    value =  (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new long[] { -4, -10, -8 },
        value.value().longArray());
    assertEquals(60, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(3, value.value().end());
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nullRight() throws Exception {
    left = new NumericArrayTimeSeries(LEFT_ID, 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) left).add(1);
    ((NumericArrayTimeSeries) left).add(5);
    ((NumericArrayTimeSeries) left).add(2);
        
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight(null)
        .setRightType(OperandType.NULL)
        .setExpressionOp(ExpressionOp.ADD)
        .setExpressionConfig(CONFIG)
        .setId("expression")
        .build();
    when(node.config()).thenReturn(expression_config);
    
    ExpressionNumericArrayIterator iterator = 
        new ExpressionNumericArrayIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .build());
    assertFalse(iterator.hasNext());
    
    // subtract
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight(null)
        .setRightType(OperandType.NULL)
        .setExpressionOp(ExpressionOp.SUBTRACT)
        .setExpressionConfig(CONFIG)
        .setId("expression")
        .build();
    when(node.config()).thenReturn(expression_config);
    
    iterator = new ExpressionNumericArrayIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .build());
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nullRightSubstitute() throws Exception {
    left = new NumericArrayTimeSeries(LEFT_ID, 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) left).add(1);
    ((NumericArrayTimeSeries) left).add(5);
    ((NumericArrayTimeSeries) left).add(2);
    
    ExpressionConfig cfg = ExpressionConfig.newBuilder()
        .setExpression("a + b")
        .setJoinConfig(JOIN_CONFIG)
        .addInterpolatorConfig(NUMERIC_CONFIG)
        .setSubstituteMissing(true)
        .setId("e1")
        .build();
    
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight(null)
        .setRightType(OperandType.NULL)
        .setExpressionOp(ExpressionOp.ADD)
        .setExpressionConfig(cfg)
        .setId("expression")
        .build();
    when(node.config()).thenReturn(expression_config);
    when(node.expressionConfig()).thenReturn(cfg);
    
    ExpressionNumericArrayIterator iterator = 
        new ExpressionNumericArrayIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .build());
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new long[] { 1, 5, 2 },
        value.value().longArray());
    assertEquals(60, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(3, value.value().end());
    assertFalse(iterator.hasNext());
    
    // subtract
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight(null)
        .setRightType(OperandType.NULL)
        .setExpressionOp(ExpressionOp.SUBTRACT)
        .setExpressionConfig(CONFIG)
        .setId("expression")
        .build();
    when(node.config()).thenReturn(expression_config);
    
    iterator = new ExpressionNumericArrayIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .build());
    assertTrue(iterator.hasNext());
    value =  (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new long[] { 1, 5, 2 },
        value.value().longArray());
    assertEquals(60, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(3, value.value().end());
    assertFalse(iterator.hasNext());
  }
}
