// This file is part of OpenTSDB.
// Copyright (C) 2020  The OpenTSDB Authors.
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.types.numeric.NumericMillisecondShard;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.processor.expressions.ExpressionParseNode.ExpressionOp;
import net.opentsdb.query.processor.expressions.ExpressionParseNode.OperandType;

public class TestTernaryNumericIterator extends BaseNumericTest {

  private TimeSeries left;
  private TimeSeries right;
  private TimeSeries condition;
  
  // TODO - more tests!
  
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
  public void longAligned() throws Exception {
    left = new NumericMillisecondShard(LEFT_ID, 
        new MillisecondTimeStamp(1000), new MillisecondTimeStamp(7000));
    ((NumericMillisecondShard) left).add(1000, 1);
    ((NumericMillisecondShard) left).add(3000, 4);
    ((NumericMillisecondShard) left).add(5000, 2);
    ((NumericMillisecondShard) left).add(7000, 6);
    
    right = new NumericMillisecondShard(RIGHT_ID, 
        new MillisecondTimeStamp(1000), new MillisecondTimeStamp(7000));
    ((NumericMillisecondShard) right).add(1000, 4);
    ((NumericMillisecondShard) right).add(3000, 2);
    ((NumericMillisecondShard) right).add(5000, 8);
    ((NumericMillisecondShard) right).add(7000, 3);
    
    condition = new NumericMillisecondShard(mock(TimeSeriesId.class), 
        new MillisecondTimeStamp(1000), new MillisecondTimeStamp(7000));
    ((NumericMillisecondShard) condition).add(1000, 1);
    ((NumericMillisecondShard) condition).add(3000, 0);
    ((NumericMillisecondShard) condition).add(5000, 0);
    ((NumericMillisecondShard) condition).add(7000, 1);
    
    TernaryNumericIterator iterator = 
        new TernaryNumericIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .put(ExpressionTimeSeries.CONDITION_KEY, condition)
              .build());
    assertTrue(iterator.hasNext());

    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1000, value.timestamp().msEpoch());
    assertEquals(1, value.value().longValue());
    
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(3000, value.timestamp().msEpoch());
    assertEquals(2, value.value().longValue());
    
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(5000, value.timestamp().msEpoch());
    assertEquals(8, value.value().longValue());
    
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(7000, value.timestamp().msEpoch());
    assertEquals(6, value.value().longValue());
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void doubleAligned() throws Exception {
    left = new NumericMillisecondShard(LEFT_ID, 
        new MillisecondTimeStamp(1000), new MillisecondTimeStamp(7000));
    ((NumericMillisecondShard) left).add(1000, 1);
    ((NumericMillisecondShard) left).add(3000, 4);
    ((NumericMillisecondShard) left).add(5000, 2);
    ((NumericMillisecondShard) left).add(7000, 6);
    
    right = new NumericMillisecondShard(RIGHT_ID, 
        new MillisecondTimeStamp(1000), new MillisecondTimeStamp(7000));
    ((NumericMillisecondShard) right).add(1000, 4);
    ((NumericMillisecondShard) right).add(3000, 2);
    ((NumericMillisecondShard) right).add(5000, 8);
    ((NumericMillisecondShard) right).add(7000, 3);
    
    condition = new NumericMillisecondShard(mock(TimeSeriesId.class), 
        new MillisecondTimeStamp(1000), new MillisecondTimeStamp(7000));
    ((NumericMillisecondShard) condition).add(1000, 1D);
    ((NumericMillisecondShard) condition).add(3000, 0D);
    ((NumericMillisecondShard) condition).add(5000, 0D);
    ((NumericMillisecondShard) condition).add(7000, 1D);
    
    TernaryNumericIterator iterator = 
        new TernaryNumericIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .put(ExpressionTimeSeries.CONDITION_KEY, condition)
              .build());
    assertTrue(iterator.hasNext());

    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1000, value.timestamp().msEpoch());
    assertEquals(1, value.value().longValue());
    
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(3000, value.timestamp().msEpoch());
    assertEquals(2, value.value().longValue());
    
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(5000, value.timestamp().msEpoch());
    assertEquals(8, value.value().longValue());
    
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(7000, value.timestamp().msEpoch());
    assertEquals(6, value.value().longValue());
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void NullLeftAligned() throws Exception {
    right = new NumericMillisecondShard(RIGHT_ID, 
        new MillisecondTimeStamp(1000), new MillisecondTimeStamp(7000));
    ((NumericMillisecondShard) right).add(1000, 4);
    ((NumericMillisecondShard) right).add(3000, 2);
    ((NumericMillisecondShard) right).add(5000, 8);
    ((NumericMillisecondShard) right).add(7000, 3);
    
    condition = new NumericMillisecondShard(mock(TimeSeriesId.class), 
        new MillisecondTimeStamp(1000), new MillisecondTimeStamp(7000));
    ((NumericMillisecondShard) condition).add(1000, 1);
    ((NumericMillisecondShard) condition).add(3000, 0);
    ((NumericMillisecondShard) condition).add(5000, 0);
    ((NumericMillisecondShard) condition).add(7000, 1);
    
    TernaryNumericIterator iterator = 
        new TernaryNumericIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .put(ExpressionTimeSeries.CONDITION_KEY, condition)
              .build());
    assertTrue(iterator.hasNext());

    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1000, value.timestamp().msEpoch());
    assertEquals(0, value.value().longValue());
    
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(3000, value.timestamp().msEpoch());
    assertEquals(2, value.value().longValue());
    
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(5000, value.timestamp().msEpoch());
    assertEquals(8, value.value().longValue());
    
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(7000, value.timestamp().msEpoch());
    assertEquals(0, value.value().longValue());
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nullRightAligned() throws Exception {
    left = new NumericMillisecondShard(LEFT_ID, 
        new MillisecondTimeStamp(1000), new MillisecondTimeStamp(7000));
    ((NumericMillisecondShard) left).add(1000, 1);
    ((NumericMillisecondShard) left).add(3000, 4);
    ((NumericMillisecondShard) left).add(5000, 2);
    ((NumericMillisecondShard) left).add(7000, 6);
    
    condition = new NumericMillisecondShard(mock(TimeSeriesId.class), 
        new MillisecondTimeStamp(1000), new MillisecondTimeStamp(7000));
    ((NumericMillisecondShard) condition).add(1000, 1);
    ((NumericMillisecondShard) condition).add(3000, 0);
    ((NumericMillisecondShard) condition).add(5000, 0);
    ((NumericMillisecondShard) condition).add(7000, 1);
    
    TernaryNumericIterator iterator = 
        new TernaryNumericIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .put(ExpressionTimeSeries.CONDITION_KEY, condition)
              .build());
    assertTrue(iterator.hasNext());

    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1000, value.timestamp().msEpoch());
    assertEquals(1, value.value().longValue());
    
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(3000, value.timestamp().msEpoch());
    assertEquals(0, value.value().longValue());
    
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(5000, value.timestamp().msEpoch());
    assertEquals(0, value.value().longValue());
    
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(7000, value.timestamp().msEpoch());
    assertEquals(6, value.value().longValue());
    assertFalse(iterator.hasNext());
  }
  
  // TODO - more tests.
}