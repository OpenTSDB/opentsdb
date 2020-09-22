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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

import net.opentsdb.data.TypedTimeSeriesIterator;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.types.annotation.AnnotationType;
import net.opentsdb.data.types.numeric.NumericArrayType;
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
  private ExpressionParseNode config;
  private ExpressionConfig exp_config;
  private QueryResult result;
  private Joiner joiner;
  private TimeSeries left;
  private TimeSeries right;
  private TimeSeries condition;
  private TimeSeriesId left_id;
  private TimeSeriesId right_id;
  private TimeSeriesId condition_id;
  private TimeSeriesId joined_id;
  private BinaryExpressionNodeFactory factory;
  
  @Before
  public void before() throws Exception {
    node = mock(BinaryExpressionNode.class);
    result = mock(QueryResult.class);
    joiner = mock(Joiner.class);
    left = mock(TimeSeries.class);
    right = mock(TimeSeries.class);
    condition = mock(TimeSeries.class);
    factory = mock(BinaryExpressionNodeFactory.class);
    
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
    condition_id = BaseTimeSeriesStringId.newBuilder()
        .setMetric("subexp#0")
        .addTags("host", "web01")
        .build();
    joined_id = BaseTimeSeriesStringId.newBuilder()
        .setMetric("e1")
        .addTags("host", "web01")
        .build();
    when(left.id()).thenReturn(left_id);
    when(right.id()).thenReturn(right_id);
    when(joiner.joinIds(any(TimeSeries.class), any(TimeSeries.class), 
        anyString(), any(JoinType.class))).thenReturn(joined_id);
    when(joiner.joinIds(eq(condition), eq(null), 
        anyString(), any(JoinType.class))).thenReturn(condition_id);
    
    when(left.types()).thenReturn(NumericType.SINGLE_LIST);
    when(right.types()).thenReturn(Lists.newArrayList(
        NumericType.TYPE, NumericSummaryType.TYPE));
    when(condition.types()).thenReturn(NumericType.SINGLE_LIST);
    
    when(factory.newTypedIterator(eq(NumericType.TYPE), 
        eq(node), eq(result), any(Map.class)))
        .thenReturn(mock(ExpressionNumericIterator.class));
    when(factory.newTypedIterator(eq(NumericArrayType.TYPE), 
        eq(node), eq(result), any(Map.class)))
        .thenReturn(mock(ExpressionNumericArrayIterator.class));
    
    JoinConfig jc = (JoinConfig) JoinConfig.newBuilder()
        .setJoinType(JoinType.INNER)
        .addJoins("host", "host")
        .setId("join")
        .build();
    
    NumericInterpolatorConfig numeric_config = 
        (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
      .setFillPolicy(FillPolicy.NOT_A_NUMBER)
      .setRealFillPolicy(FillWithRealPolicy.NONE)
      .setDataType(NumericType.TYPE.toString())
      .build();
    
    config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setExpressionConfig(mock(ExpressionConfig.class))
        .setAs("e1")
        .setId("e1")
        .build();
    when(node.config()).thenReturn(config);
    
    exp_config = ExpressionConfig.newBuilder()
        .setAs("e1")
        .setExpression("a + b")
        .setJoinConfig(jc)
        .addInterpolatorConfig(numeric_config)
        .setId("e1")
        .build();
    when(node.expressionConfig()).thenReturn(exp_config);
  }
  
  @Test
  public void ctor() {
    ExpressionTimeSeries series = new ExpressionTimeSeries(
        node, result, left, right, condition);
    assertSame(joined_id, series.id());
    assertEquals(1, series.types().size());
    assertTrue(series.types().contains(NumericType.TYPE));
    
    series = new ExpressionTimeSeries(node, result, left, null, condition);
    assertSame(joined_id, series.id());
    assertEquals(1, series.types().size());
    assertTrue(series.types().contains(NumericType.TYPE));
    
    
    series = new ExpressionTimeSeries(node, result, null, null, condition);
    assertSame(condition_id, series.id());
    assertEquals(1, series.types().size());
    assertTrue(series.types().contains(NumericType.TYPE));
    
    try {
      new ExpressionTimeSeries(node, result, null, null, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // no overlapping types
    // TODO
    /*when(right.types()).thenReturn(Lists.newArrayList(
        NumericArrayType.TYPE));
    series = new ExpressionTimeSeries(node, result, left, right, condition);
    assertSame(joined_id, series.id());
    assertEquals(0, series.types().size());*/
  }
  
  @Test
  public void iterator() throws Exception {
    ExpressionTimeSeries series = new ExpressionTimeSeries(
        node, result, left, right, condition);
    Optional<TypedTimeSeriesIterator<?>> op = series.iterator(NumericType.TYPE);
    assertTrue(op.isPresent());
    assertTrue(op.get() instanceof ExpressionNumericIterator);
    assertFalse(series.iterator(AnnotationType.TYPE).isPresent());
    
    series = new ExpressionTimeSeries(node, result, null, right, condition);
    op = series.iterator(NumericType.TYPE);
    assertTrue(op.isPresent());
    assertTrue(op.get() instanceof ExpressionNumericIterator);
    assertFalse(series.iterator(AnnotationType.TYPE).isPresent());
    
    series = new ExpressionTimeSeries(node, result, left, null, condition);
    op = series.iterator(NumericType.TYPE);
    assertTrue(op.isPresent());
    assertTrue(op.get() instanceof ExpressionNumericIterator);
    assertFalse(series.iterator(AnnotationType.TYPE).isPresent());
    
    when(left.types()).thenReturn(Lists.newArrayList(
        NumericArrayType.TYPE));
    when(right.types()).thenReturn(Lists.newArrayList(
        NumericArrayType.TYPE));
    series = new ExpressionTimeSeries(node, result, left, right, condition);
    op = series.iterator(NumericArrayType.TYPE);
    assertTrue(op.isPresent());
    assertTrue(op.get() instanceof ExpressionNumericArrayIterator);
    assertFalse(series.iterator(NumericType.TYPE).isPresent());
  }
  
  @Test
  public void iterators() throws Exception {
    ExpressionTimeSeries series = new ExpressionTimeSeries(
        node, result, left, right, condition);
    Collection<TypedTimeSeriesIterator<?>> its = series.iterators();
    assertEquals(1, its.size());
    assertTrue(its.iterator().next() instanceof ExpressionNumericIterator);
    
    series = new ExpressionTimeSeries(node, result, null, right, condition);
    its = series.iterators();
    assertEquals(2, its.size());
    Iterator<TypedTimeSeriesIterator<?>> it = its.iterator();
    assertTrue(it.next() instanceof ExpressionNumericIterator);
    assertNull(it.next());
    
    series = new ExpressionTimeSeries(node, result, left, null, condition);
    its = series.iterators();
    assertEquals(1, its.size());
    assertTrue(its.iterator().next() instanceof ExpressionNumericIterator);
  }

  @Test
  public void ternaryIterator() throws Exception {
    node = mock(TernaryNode.class);
    when(node.joiner()).thenReturn(joiner);
    when(node.config()).thenReturn(config);
    when(node.expressionConfig()).thenReturn(exp_config);
    
    TernaryNodeFactory ternary_factory = mock(TernaryNodeFactory.class);
    when(node.factory()).thenReturn(ternary_factory);
    when(ternary_factory.newTypedIterator(eq(NumericType.TYPE), 
        eq((TernaryNode) node), eq(result), any(Map.class)))
        .thenReturn(mock(TernaryNumericIterator.class));
    
    ExpressionTimeSeries series = new ExpressionTimeSeries(
        node, result, left, right, condition);
    Optional<TypedTimeSeriesIterator<?>> op = series.iterator(NumericType.TYPE);
    assertTrue(op.isPresent());
    assertTrue(op.get() instanceof TernaryNumericIterator);
  }
  
  @Test
  public void ternaryIterators() throws Exception {
    node = mock(TernaryNode.class);
    when(node.joiner()).thenReturn(joiner);
    when(node.config()).thenReturn(config);
    when(node.expressionConfig()).thenReturn(exp_config);
    
    TernaryNodeFactory ternary_factory = mock(TernaryNodeFactory.class);
    when(node.factory()).thenReturn(ternary_factory);
    when(ternary_factory.newTypedIterator(eq(NumericType.TYPE), 
        eq((TernaryNode) node), eq(result), any(Map.class)))
        .thenReturn(mock(TernaryNumericIterator.class));
    
    ExpressionTimeSeries series = new ExpressionTimeSeries(
        node, result, left, right, condition);
    Collection<TypedTimeSeriesIterator<?>> its = series.iterators();
    assertEquals(1, its.size());
    assertTrue(its.iterator().next() instanceof TernaryNumericIterator);
    
    series = new ExpressionTimeSeries(node, result, null, right, condition);
    its = series.iterators();
    assertEquals(2, its.size());
    Iterator<TypedTimeSeriesIterator<?>> it = its.iterator();
    assertTrue(it.next() instanceof TernaryNumericIterator);
    assertNull(it.next());
    
    series = new ExpressionTimeSeries(node, result, left, null, condition);
    its = series.iterators();
    assertEquals(1, its.size());
    assertTrue(its.iterator().next() instanceof TernaryNumericIterator);
  }
}
