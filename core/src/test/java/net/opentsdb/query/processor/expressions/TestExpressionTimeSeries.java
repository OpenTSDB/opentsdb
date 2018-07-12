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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Iterator;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.collect.Lists;

import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.types.annotation.AnnotationType;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.joins.JoinConfig;
import net.opentsdb.query.joins.Joiner;
import net.opentsdb.query.joins.JoinConfig.JoinType;
import net.opentsdb.query.pojo.FillPolicy;

public class TestExpressionTimeSeries {

  private BinaryExpressionNode node;
  private ExpressionConfig config;
  private QueryResult result;
  private Joiner joiner;
  private TimeSeries left;
  private TimeSeries right;
  private TimeSeriesId left_id;
  private TimeSeriesId right_id;
  private TimeSeriesId joined_id;
  private ExpressionFactory factory;
  
  @Before
  public void before() throws Exception {
    node = mock(BinaryExpressionNode.class);
    result = mock(QueryResult.class);
    joiner = mock(Joiner.class);
    left = mock(TimeSeries.class);
    right = mock(TimeSeries.class);
    factory = mock(ExpressionFactory.class);
    
    when(node.id()).thenReturn("e1");
    when(node.joiner()).thenReturn(joiner);
    when(node.factory()).thenReturn(factory);
    
    left_id = BaseTimeSeriesStringId.newBuilder()
        .setMetric("sys.cpu.user")
        .addTags("host", "web01")
        .build();
    right_id = BaseTimeSeriesStringId.newBuilder()
        .setMetric("sys.cpu.system")
        .addTags("host", "web01")
        .build();
    joined_id = BaseTimeSeriesStringId.newBuilder()
        .setMetric("e1")
        .addTags("host", "web01")
        .build();
    when(left.id()).thenReturn(left_id);
    when(right.id()).thenReturn(right_id);
    when(joiner.joinIds(any(TimeSeries.class), any(TimeSeries.class), 
        anyString())).thenReturn(joined_id);
    
    when(left.types()).thenReturn(Lists.newArrayList(NumericType.TYPE));
    when(right.types()).thenReturn(Lists.newArrayList(
        NumericType.TYPE, NumericSummaryType.TYPE));
    
    when(factory.newIterator(eq(NumericType.TYPE), eq(node), eq(result), any(Map.class)))
      .thenAnswer(new Answer<Iterator>() {
        @Override
        public Iterator answer(InvocationOnMock invocation) throws Throwable {
          return mock(Iterator.class);
        }
      });
    
    JoinConfig jc = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.INNER)
        .addJoins("host", "host")
        .setId("join")
        .build();
    
    NumericInterpolatorConfig numeric_config = 
        (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
      .setFillPolicy(FillPolicy.NOT_A_NUMBER)
      .setRealFillPolicy(FillWithRealPolicy.NONE)
      .setType(NumericType.TYPE.toString())
      .build();
    
    config = (ExpressionConfig) ExpressionConfig.newBuilder()
        .setExpression("metric.a + metric.b")
        .setJoinConfig(jc)
        .setAs("e1")
        .addInterpolatorConfig(numeric_config)
        .setId("e1")
        .build();
    when(node.config()).thenReturn(config);
  }
  
  @Test
  public void ctor() {
    ExpressionTimeSeries series = new ExpressionTimeSeries(
        node, result, left, right);
    assertSame(joined_id, series.id());
    assertEquals(2, series.types().size());
    assertTrue(series.types().contains(NumericType.TYPE));
    assertTrue(series.types().contains(NumericSummaryType.TYPE));
    
    series = new ExpressionTimeSeries(node, result, left, null);
    assertSame(joined_id, series.id());
    assertEquals(1, series.types().size());
    assertTrue(series.types().contains(NumericType.TYPE));
    
    try {
      new ExpressionTimeSeries(node, result, null, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void iterator() throws Exception {
    ExpressionTimeSeries series = new ExpressionTimeSeries(
        node, result, left, right);
    assertTrue(series.iterator(NumericType.TYPE).isPresent());
    assertFalse(series.iterator(AnnotationType.TYPE).isPresent());
    
    series = new ExpressionTimeSeries(node, result, null, right);
    assertTrue(series.iterator(NumericType.TYPE).isPresent());
    assertFalse(series.iterator(AnnotationType.TYPE).isPresent());
    
    series = new ExpressionTimeSeries(node, result, left, null);
    assertTrue(series.iterator(NumericType.TYPE).isPresent());
    assertFalse(series.iterator(AnnotationType.TYPE).isPresent());
  }
  
  @Test
  public void iterators() throws Exception {
    ExpressionTimeSeries series = new ExpressionTimeSeries(
        node, result, left, right);
    assertEquals(2, series.iterators().size());
    
    series = new ExpressionTimeSeries(node, result, null, right);
    assertEquals(2, series.iterators().size());
    
    series = new ExpressionTimeSeries(node, result, left, null);
    assertEquals(1, series.iterators().size());
  }
}
