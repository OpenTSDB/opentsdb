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
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.MockTimeSeries;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.types.numeric.MutableNumericSummaryValue;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.query.interpolation.types.numeric.NumericSummaryInterpolatorConfig;
import net.opentsdb.query.processor.expressions.ExpressionParseNode.ExpressionOp;
import net.opentsdb.query.processor.expressions.ExpressionParseNode.OperandType;
import net.opentsdb.rollup.RollupConfig;

public class TestTernaryNumericSummaryIterator extends BaseNumericSummaryTest {

  protected MockTimeSeries condition;
  
  // TODO - more tests.
  
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
    
    setupData(new double[] { 1, 5, 2 }, new long[] { 1, 2, 2 }, 
              new double[] { 4, 10, 8 }, new long[] { 1, 2, 2 }, false,
              new double[] { 0, 1, 1 }, new long[] { 1, 0, 0 });
    
    // others test regular interpolator configs, here test fallback 
    // to numeric type config.
    ExpressionConfig config = (ExpressionConfig) ExpressionConfig.newBuilder()
      .setExpression("a ? b : c")
      .setJoinConfig(JOIN_CONFIG)
      .addInterpolatorConfig(NUMERIC_CONFIG)
      //.addInterpolatorConfig(NUMERIC_SUMMARY_CONFIG) // doh!
      .setId("e1")
      .build();
    when(node.expressionConfig()).thenReturn(config);
    
    TernaryNumericSummaryIterator iterator = 
        new TernaryNumericSummaryIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .put(ExpressionTimeSeries.CONDITION_KEY, condition)
              .build());
    NumericSummaryInterpolatorConfig nsic = 
        (NumericSummaryInterpolatorConfig) iterator.left_interpolator
          .fillPolicy().config();
    assertEquals(3, nsic.getExpectedSummaries().size());
    assertTrue(nsic.getExpectedSummaries().contains(0));
    assertTrue(nsic.getExpectedSummaries().contains(2));
    assertTrue(nsic.getExpectedSummaries().contains(5));
    // nulls for both
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft(null)
        .setLeftType(OperandType.NULL)
        .setRight(null)
        .setRightType(OperandType.NULL)
        .setExpressionOp(ExpressionOp.SUBTRACT)
        .setExpressionConfig(CONFIG)
        .setId("expression")
        .build();
    when(node.config()).thenReturn(expression_config);
    
    try {
      new ExpressionNumericSummaryIterator(node, RESULT, 
          (Map) ImmutableMap.builder()
            .build());
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
  }
  
  @Test
  public void longLong() throws Exception {
    setupData(new double[] { 1, 5, 2 }, new long[] { 3, 2, 2 }, 
              new double[] { 4, 10, 8 }, new long[] { 1, 5, 1 }, false,
              new double[] { 0, 1, 1 }, new long[] { 1, 0, 0 });

    TernaryNumericSummaryIterator iterator = 
        new TernaryNumericSummaryIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .put(ExpressionTimeSeries.CONDITION_KEY, condition)
              .build());
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(1000, value.timestamp().msEpoch());
    assertEquals(4, value.value().value(0).doubleValue(), 0.001);
    assertEquals(3, value.value().value(2).longValue());
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(3000, value.timestamp().msEpoch());
    assertEquals(5, value.value().value(0).doubleValue(), 0.001);
    assertEquals(5, value.value().value(2).longValue());
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(5000, value.timestamp().msEpoch());
    assertEquals(2, value.value().value(0).doubleValue(), 0.001);
    assertEquals(1, value.value().value(2).longValue());
    assertFalse(iterator.hasNext());
  }

  // TODO - more UTS!!
  
  protected void setupData(double[] left_sums, long[] left_counts, 
                           double[] right_sums, long[] right_counts, boolean nans,
                           double[] tern_sums, long[] tern_counts) {
    super.setupData(left_sums, left_counts, right_sums, right_counts, nans);
    condition = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("c")
        .build());
    
    long timestamp = 1000;
    for (int i = 0; i < left_sums.length; i++) {
      MutableNumericSummaryValue v = new MutableNumericSummaryValue();
      v.resetTimestamp(new MillisecondTimeStamp(timestamp));
      if (tern_sums[i] >= 0) {
        v.resetValue(0, tern_sums[i]);
      } else if (nans) {
        v.resetValue(0, Double.NaN);
      }
      if (tern_counts[i] >= 0) {
        v.resetValue(2, tern_counts[i]);
      } else if (nans) {
        v.resetValue(2, Double.NaN);
      }
      condition.addValue(v);
      timestamp += 2000;
    }
  }
}