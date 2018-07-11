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
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Optional;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.types.numeric.MutableNumericType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.exceptions.QueryDownstreamException;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.processor.expressions.ExpressionParseNode.ExpressionOp;
import net.opentsdb.query.processor.expressions.ExpressionParseNode.OperandType;
import net.opentsdb.query.processor.expressions.ExpressionParser.NumericLiteral;

public class TestBaseExpressionNumericIterator extends BaseNumericTest {
  
  @SuppressWarnings("unchecked")
  @Test
  public void ctors() throws Exception {
    TimeSeries left = mock(TimeSeries.class);
    TimeSeries right = mock(TimeSeries.class);
    
    when(left.iterator(any(TypeToken.class))).thenReturn(Optional.empty());
    when(right.iterator(any(TypeToken.class))).thenReturn(Optional.empty());
    
    MockIterator iterator = 
        new MockIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .build());
    assertNull(iterator.left_literal);
    assertNull(iterator.right_literal);
    assertFalse(iterator.hasNext());
    
    // right is a literal
    NumericLiteral literal = mock(NumericLiteral.class);
    when(literal.isInteger()).thenReturn(true);
    when(literal.longValue()).thenReturn(42L);
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight(literal)
        .setRightType(OperandType.LITERAL_NUMERIC)
        .setExpressionOp(ExpressionOp.ADD)
        .build();
    when(node.expressionConfig()).thenReturn(expression_config);
    
    iterator = 
        new MockIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              //.put(ExpressionTimeSeries.RIGHT_KEY, right)
              .build());
    assertNull(iterator.left_literal);
    assertTrue(iterator.right_literal.isInteger());
    assertEquals(42, iterator.right_literal.longValue());
    assertFalse(iterator.hasNext());
    
    // left is a literal
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft(literal)
        .setLeftType(OperandType.LITERAL_NUMERIC)
        .setRight("b")
        .setRightType(OperandType.VARIABLE)
        .setExpressionOp(ExpressionOp.ADD)
        .build();
    when(node.expressionConfig()).thenReturn(expression_config);
    
    iterator = 
        new MockIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              //.put(ExpressionTimeSeries.LEFT_KEY, left)
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .build());
    assertTrue(iterator.left_literal.isInteger());
    assertEquals(42, iterator.left_literal.longValue());
    assertNull(iterator.right_literal);
    assertFalse(iterator.hasNext());
    
    // null in expression
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft(null)
        .setLeftType(OperandType.NULL)
        .setRight("b")
        .setRightType(OperandType.VARIABLE)
        .setExpressionOp(ExpressionOp.ADD)
        .build();
    when(node.expressionConfig()).thenReturn(expression_config);
    iterator = 
        new MockIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              //.put(ExpressionTimeSeries.LEFT_KEY, left)
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .build());
    assertNull(iterator.left_literal);
    assertNull(iterator.right_literal);
    assertFalse(iterator.hasNext());
  }

  @Test
  public void isTrue() throws Exception {
    assertFalse(BaseExpressionNumericIterator.isTrue(null));
    
    MutableNumericType v = new MutableNumericType();
    v.set(0);
    assertFalse(BaseExpressionNumericIterator.isTrue(v));
    
    v.set(1);
    assertTrue(BaseExpressionNumericIterator.isTrue(v));
    
    v.set(-42);
    assertFalse(BaseExpressionNumericIterator.isTrue(v));
    
    v.set(0.0);
    assertFalse(BaseExpressionNumericIterator.isTrue(v));
    
    v.set(0.000000001);
    assertTrue(BaseExpressionNumericIterator.isTrue(v));
    
    v.set(-0.001);
    assertFalse(BaseExpressionNumericIterator.isTrue(v));
    
    v.set(Double.NaN);
    assertFalse(BaseExpressionNumericIterator.isTrue(v));
    
    v.set(Double.POSITIVE_INFINITY);
    assertFalse(BaseExpressionNumericIterator.isTrue(v));
    
    v.set(Double.NEGATIVE_INFINITY);
    assertFalse(BaseExpressionNumericIterator.isTrue(v));
  }
  
  @Test
  public void buildLiteral() throws Exception {
    NumericType literal = MockIterator.buildLiteral(true, OperandType.LITERAL_BOOL);
    assertTrue(literal.isInteger());
    assertEquals(1, literal.longValue());
    
    literal = MockIterator.buildLiteral(false, OperandType.LITERAL_BOOL);
    assertTrue(literal.isInteger());
    assertEquals(0, literal.longValue());
    
    literal = MockIterator.buildLiteral(null, OperandType.NULL);
    assertNull(literal);
    
    NumericLiteral source = mock(NumericLiteral.class);
    when(source.isInteger()).thenReturn(true);
    when(source.longValue()).thenReturn(42L);
    
    literal = MockIterator.buildLiteral(source, OperandType.LITERAL_NUMERIC);
    assertTrue(literal.isInteger());
    assertEquals(42, literal.longValue());
    
    when(source.isInteger()).thenReturn(false);
    when(source.doubleValue()).thenReturn(42.75);
    
    literal = MockIterator.buildLiteral(source, OperandType.LITERAL_NUMERIC);
    assertFalse(literal.isInteger());
    assertEquals(42.75, literal.doubleValue(), 0.001);
    
    try {
      MockIterator.buildLiteral("a", OperandType.VARIABLE);
      fail("Expected QueryDownstreamException");
    } catch (QueryDownstreamException e) { }
    
    try {
      MockIterator.buildLiteral("a", OperandType.SUB_EXP);
      fail("Expected QueryDownstreamException");
    } catch (QueryDownstreamException e) { }
  }
  
  class MockIterator extends BaseExpressionNumericIterator<NumericType> {

    MockIterator(final QueryNode node, 
                 final QueryResult RESULT,
                 final Map<String, TimeSeries> sources) {
      super(node, RESULT, sources);
    }

    @Override
    public TimeSeriesValue<? extends TimeSeriesDataType> next() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public TimeStamp timestamp() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public NumericType value() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public TypeToken<NumericType> type() {
      // TODO Auto-generated method stub
      return null;
    }
    
  }
}
