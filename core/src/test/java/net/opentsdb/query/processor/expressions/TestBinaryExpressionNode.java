// This file is part of OpenTSDB.
// Copyright (C) 2018-2020  The OpenTSDB Authors.
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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.common.Const;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesDataSourceFactory;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.DefaultQueryResultId;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.joins.JoinConfig;
import net.opentsdb.query.joins.JoinConfig.JoinType;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.processor.expressions.BinaryExpressionNode.FailedQueryResult;
import net.opentsdb.query.processor.expressions.ExpressionParseNode.ExpressionOp;
import net.opentsdb.query.processor.expressions.ExpressionParseNode.OperandType;
import net.opentsdb.stats.Span;
import net.opentsdb.utils.UnitTestException;

public class TestBinaryExpressionNode {

  private QueryNodeFactory factory;
  private QueryPipelineContext context;
  private QueryNode upstream;
  private NumericInterpolatorConfig numeric_config;
  private ExpressionConfig config;
  private JoinConfig join_config;
  private ExpressionParseNode expression_config;
  
  @Before
  public void before() throws Exception {
    factory = mock(QueryNodeFactory.class);
    context = mock(QueryPipelineContext.class);
    upstream = mock(QueryNode.class);
    when(context.upstream(any(QueryNode.class)))
      .thenReturn(Lists.newArrayList(upstream));
    
    numeric_config = 
        (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
      .setFillPolicy(FillPolicy.NOT_A_NUMBER)
      .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
      .setDataType(NumericType.TYPE.toString())
      .build();
    
    join_config = JoinConfig.newBuilder()
        .setJoinType(JoinType.INNER)
        .addJoins("host", "host")
        .setId("join")
        .setId("expression")
        .build();
    
    config = ExpressionConfig.newBuilder()
        .setExpression("a + b + c")
        .setJoinConfig(join_config)
        .addInterpolatorConfig(numeric_config)
        .setId("e1")
        .setId("expression")
        .build();
    
    expression_config = ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftId(new DefaultQueryResultId("a", "a"))
        .setLeftType(OperandType.VARIABLE)
        .setRight("b")
        .setRightId(new DefaultQueryResultId("b", "b"))
        .setRightType(OperandType.VARIABLE)
        .setExpressionOp(ExpressionOp.ADD)
        .setExpressionConfig(config)
        .setId("expression")
        .build();
  }
  
  @Test
  public void ctor() throws Exception {
    BinaryExpressionNode node = new BinaryExpressionNode(
        factory, context, expression_config);
    assertSame(config, node.config);
    assertSame(expression_config, node.config());
    assertNotNull(node.result);
    assertNotNull(node.joiner());
    
    // sub-exp
    expression_config = ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftId(new DefaultQueryResultId("a", "a"))
        .setLeftType(OperandType.VARIABLE)
        .setRight("SubExp#1")
        .setRightId(new DefaultQueryResultId("SubExp#1", "b"))
        .setRightType(OperandType.SUB_EXP)
        .setExpressionOp(ExpressionOp.ADD)
        .setExpressionConfig(config)
        .setId("expression")
        .build();
    node = new BinaryExpressionNode(factory, context, expression_config);
    assertSame(config, node.config);
    assertSame(expression_config, node.expression_config);
    assertNotNull(node.result);
    assertNotNull(node.joiner());
    assertEquals(new DefaultQueryResultId("a", "a"), node.left_source);
    assertEquals(new DefaultQueryResultId("SubExp#1", "b"), node.right_source);
    
    // one needed
    expression_config = ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftId(new DefaultQueryResultId("a", "a"))
        .setLeftType(OperandType.VARIABLE)
        .setRight("42")
        .setRightType(OperandType.LITERAL_NUMERIC)
        .setExpressionOp(ExpressionOp.ADD)
        .setExpressionConfig(config)
        .setId("expression")
        .build();
    node = new BinaryExpressionNode(factory, context, expression_config);
    assertSame(config, node.config);
    assertSame(expression_config, node.expression_config);
    assertNotNull(node.result);
    assertNotNull(node.joiner());
    assertEquals(new DefaultQueryResultId("a", "a"), node.left_source);
    assertNull(node.right_source);
    
    try {
      new BinaryExpressionNode(factory, null, expression_config);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new BinaryExpressionNode(factory, context, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void onComplete() throws Exception {
    BinaryExpressionNode node = new BinaryExpressionNode(
        factory, context, expression_config);
    node.initialize(null);
    
    node.onComplete(null, 42, 42);
    verify(upstream, times(1)).onComplete(node, 42, 42);
    
    doThrow(new IllegalArgumentException("Boo!")).when(upstream)
      .onComplete(any(QueryNode.class), anyLong(), anyLong());
    node.onComplete(null, 42, 42);
    verify(upstream, times(2)).onComplete(node, 42, 42);
  }
  
  @Test
  public void onError() throws Exception {
    BinaryExpressionNode node = new BinaryExpressionNode(
        factory, context, expression_config);
    node.initialize(null);
    
    final UnitTestException ex = new UnitTestException();
    
    node.onError(ex);
    verify(upstream, times(1)).onError(ex);
    
    doThrow(new IllegalArgumentException("Boo!")).when(upstream)
      .onError(any(Throwable.class));
    node.onError(ex);
    verify(upstream, times(2)).onError(ex);
  }

  @Test
  public void onNextStringDouble() throws Exception {
    BinaryExpressionNode node = new BinaryExpressionNode(
        factory, context, expression_config);
    node.initialize(null);
    
    QueryResult r1 = mock(QueryResult.class);
    when(r1.dataSource()).thenReturn(new DefaultQueryResultId("a", "a"));
    QueryNode n1 = mock(QueryNode.class);
    QueryNodeConfig c1 = mock(QueryNodeConfig.class);
    when(c1.getId()).thenReturn("a");
    when(n1.config()).thenReturn(c1);
    when(r1.source()).thenReturn(n1);
    QueryResult r2 = mock(QueryResult.class);
    when(r2.dataSource()).thenReturn(new DefaultQueryResultId("b", "b"));
    QueryNodeConfig c2 = mock(QueryNodeConfig.class);
    when(c2.getId()).thenReturn("b");
    QueryNode n2 = mock(QueryNode.class);
    when(n2.config()).thenReturn(c2);
    when(r2.source()).thenReturn(n2);
    
    node.onNext(r1);
    assertSame(r1, node.results.getKey());
    assertNull(((ExpressionResult) node.result).results);
    verify(upstream, never()).onNext(any(QueryResult.class));
    
    node.onNext(r2);
    assertSame(r2, node.results.getValue());
    assertSame(node.results, ((ExpressionResult) node.result).results);
    verify(upstream, times(1)).onNext(any(QueryResult.class));
  }
  
  @Test
  public void onNextStringSingle() throws Exception {
    expression_config = ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftId(new DefaultQueryResultId("a", "a"))
        .setLeftType(OperandType.VARIABLE)
        .setRight("42")
        .setRightType(OperandType.LITERAL_NUMERIC)
        .setExpressionOp(ExpressionOp.ADD)
        .setExpressionConfig(config)
        .setId("expression")
        .build();
    
    BinaryExpressionNode node = new BinaryExpressionNode(
        factory, context, expression_config);
    node.initialize(null);
    
    QueryResult r1 = mock(QueryResult.class);
    when(r1.dataSource()).thenReturn(new DefaultQueryResultId("a", "a"));
    QueryNode n1 = mock(QueryNode.class);
    QueryNodeConfig c1 = mock(QueryNodeConfig.class);
    when(c1.getId()).thenReturn("a");
    when(n1.config()).thenReturn(c1);
    when(r1.source()).thenReturn(n1);
    
    node.onNext(r1);
    assertSame(r1, node.results.getKey());
    assertSame(node.results, ((ExpressionResult) node.result).results);
    verify(upstream, times(1)).onNext(any(QueryResult.class));
  }

  @Test
  public void onNextByteDouble() throws Exception {
    BinaryExpressionNode node = new BinaryExpressionNode(
        factory, context, expression_config);
    node.initialize(null);
    
    QueryResult r1 = mock(QueryResult.class);
    when(r1.dataSource()).thenReturn(new DefaultQueryResultId("a", "a"));
    QueryNodeConfig c1 = mock(QueryNodeConfig.class);
    when(c1.getId()).thenReturn("a");
    QueryNode n1 = mock(QueryNode.class);
    when(n1.config()).thenReturn(c1);
    when(r1.source()).thenReturn(n1);
    QueryResult r2 = mock(QueryResult.class);
    when(r2.dataSource()).thenReturn(new DefaultQueryResultId("b", "b"));
    QueryNodeConfig c2 = mock(QueryNodeConfig.class);
    when(c2.getId()).thenReturn("b");
    QueryNode n2 = mock(QueryNode.class);
    when(n2.config()).thenReturn(c2);
    when(r2.source()).thenReturn(n2);
    when(r1.idType()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return Const.TS_BYTE_ID;
      }
    });
    when(r2.idType()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return Const.TS_BYTE_ID;
      }
    });
    TimeSeries ts = mock(TimeSeries.class);
    TimeSeriesByteId id = mock(TimeSeriesByteId.class);
    TimeSeriesDataSourceFactory store = mock(TimeSeriesDataSourceFactory.class);
    when(ts.id()).thenReturn(id);
    when(id.dataStore()).thenReturn(store);
    when(id.type()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return Const.TS_BYTE_ID;
      }
    });
    when(r1.timeSeries()).thenReturn(Lists.newArrayList(ts))
      .thenReturn(Lists.newArrayList(ts))
      .thenReturn(Lists.newArrayList(ts))
      .thenReturn(Lists.newArrayList(ts))
      .thenReturn(Collections.emptyList()); // avoid having to not-mock out the id.
    
    Deferred<List<byte[]>> metrics = new Deferred<List<byte[]>>();
    Deferred<List<byte[]>> tags = new Deferred<List<byte[]>>();
    when(store.encodeJoinMetrics(any(List.class), any(Span.class)))
      .thenReturn(metrics);
    when(store.encodeJoinKeys(any(List.class), any(Span.class)))
      .thenReturn(tags);
    
    node.onNext(r1);
    assertSame(r1, node.results.getKey());
    assertNull(((ExpressionResult) node.result).results);
    verify(upstream, never()).onNext(any(QueryResult.class));
    verify(upstream, never()).onError(any(Throwable.class));
    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(store, times(1)).encodeJoinMetrics(any(List.class), any(Span.class));
    verify(store, never()).encodeJoinKeys(any(List.class), any(Span.class));
    assertNull(node.left_metric);
    assertNull(node.right_metric);
    assertNull(node.joiner.encodedJoins());
    
    // callback.
    metrics.callback(Lists.newArrayList(new byte[] { 0, 0, 1 }, new byte[] { 0, 0, 2 }));
    verify(upstream, never()).onNext(any(QueryResult.class));
    verify(upstream, never()).onError(any(Throwable.class));
    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(store, times(1)).encodeJoinMetrics(any(List.class), any(Span.class));
    verify(store, times(1)).encodeJoinKeys(any(List.class), any(Span.class));
    assertArrayEquals(new byte[] { 0, 0, 1 }, node.left_metric);
    assertArrayEquals(new byte[] { 0, 0, 2 }, node.right_metric);
    assertNull(node.joiner.encodedJoins());
    
    // callback tags now
    tags.callback(Lists.newArrayList(new byte[] { 0, 0, 3 }, new byte[] { 0, 0, 3 }));
    verify(upstream, never()).onNext(any(QueryResult.class));
    verify(upstream, never()).onError(any(Throwable.class));
    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(store, times(1)).encodeJoinMetrics(any(List.class), any(Span.class));
    verify(store, times(1)).encodeJoinKeys(any(List.class), any(Span.class));
    assertEquals(1, node.joiner.encodedJoins().size());
    assertArrayEquals(new byte[] { 0, 0, 3 }, node.joiner.encodedJoins().get(new byte[] { 0, 0, 3 }));
    assertNull(((ExpressionResult) node.result).results);
    
    node.onNext(r2);
    assertSame(r2, node.results.getValue());
    assertSame(node.results, ((ExpressionResult) node.result).results);
    verify(upstream, times(1)).onNext(any(QueryResult.class));
  }
  
  @Test
  public void onNextByteDoubleOneSubExp() throws Exception {
    expression_config = ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftId(new DefaultQueryResultId("a", "a"))
        .setLeftType(OperandType.VARIABLE)
        .setRight("sub")
        .setRightId(new DefaultQueryResultId("sub", "m1"))
        .setRightType(OperandType.SUB_EXP)
        .setExpressionOp(ExpressionOp.ADD)
        .setExpressionConfig(config)
        .setId("expression")
        .build();
    
    BinaryExpressionNode node = new BinaryExpressionNode(
        factory, context, expression_config);
    node.initialize(null);
    
    QueryNode n2 = mock(QueryNode.class);
    QueryNodeConfig c2 = mock(QueryNodeConfig.class);
    when(n2.config()).thenReturn(c2);
    when(c2.getId()).thenReturn("sub");
    
    QueryResult r1 = mock(QueryResult.class);
    when(r1.dataSource()).thenReturn(new DefaultQueryResultId("a", "a"));
    QueryNode n1 = mock(QueryNode.class);
    QueryNodeConfig c1 = mock(QueryNodeConfig.class);
    when(c1.getId()).thenReturn("a");
    when(n1.config()).thenReturn(c1);
    when(r1.source()).thenReturn(n1);
    QueryResult r2 = mock(QueryResult.class);
    when(r2.dataSource()).thenReturn(new DefaultQueryResultId("sub", "m1"));
    when(r2.source()).thenReturn(n2);
    when(r1.idType()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return Const.TS_BYTE_ID;
      }
    });
    when(r2.idType()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return Const.TS_BYTE_ID;
      }
    });
    TimeSeries ts = mock(TimeSeries.class);
    TimeSeriesByteId id = mock(TimeSeriesByteId.class);
    TimeSeriesDataSourceFactory store = mock(TimeSeriesDataSourceFactory.class);
    when(ts.id()).thenReturn(id);
    when(id.dataStore()).thenReturn(store);
    when(id.type()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return Const.TS_BYTE_ID;
      }
    });
    when(r1.timeSeries()).thenReturn(Lists.newArrayList(ts))
      .thenReturn(Lists.newArrayList(ts))
      .thenReturn(Lists.newArrayList(ts))
      .thenReturn(Lists.newArrayList(ts))
      .thenReturn(Collections.emptyList()); // avoid having to not-mock out the id.
    
    Deferred<List<byte[]>> metrics = new Deferred<List<byte[]>>();
    Deferred<List<byte[]>> tags = new Deferred<List<byte[]>>();
    when(store.encodeJoinMetrics(any(List.class), any(Span.class)))
      .thenReturn(metrics);
    when(store.encodeJoinKeys(any(List.class), any(Span.class)))
      .thenReturn(tags);
    
    node.onNext(r1);
    assertSame(r1, node.results.getKey());
    assertNull(((ExpressionResult) node.result).results);
    verify(upstream, never()).onNext(any(QueryResult.class));
    verify(upstream, never()).onError(any(Throwable.class));
    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(store, times(1)).encodeJoinMetrics(any(List.class), any(Span.class));
    verify(store, never()).encodeJoinKeys(any(List.class), any(Span.class));
    assertNull(node.left_metric);
    assertArrayEquals("sub".getBytes(Const.UTF8_CHARSET), node.right_metric);
    assertNull(node.joiner.encodedJoins());
    
    // callback.
    metrics.callback(Lists.newArrayList(new byte[] { 0, 0, 1 }, null));
    verify(upstream, never()).onNext(any(QueryResult.class));
    verify(upstream, never()).onError(any(Throwable.class));
    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(store, times(1)).encodeJoinMetrics(any(List.class), any(Span.class));
    verify(store, times(1)).encodeJoinKeys(any(List.class), any(Span.class));
    assertArrayEquals(new byte[] { 0, 0, 1 }, node.left_metric);
    assertArrayEquals("sub".getBytes(Const.UTF8_CHARSET), node.right_metric);
    assertNull(node.joiner.encodedJoins());
    
    // callback tags now
    tags.callback(Lists.newArrayList(new byte[] { 0, 0, 3 }, new byte[] { 0, 0, 3 }));
    verify(upstream, never()).onNext(any(QueryResult.class));
    verify(upstream, never()).onError(any(Throwable.class));
    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(store, times(1)).encodeJoinMetrics(any(List.class), any(Span.class));
    verify(store, times(1)).encodeJoinKeys(any(List.class), any(Span.class));
    assertEquals(1, node.joiner.encodedJoins().size());
    assertArrayEquals(new byte[] { 0, 0, 3 }, node.joiner.encodedJoins().get(new byte[] { 0, 0, 3 }));
    assertNull(((ExpressionResult) node.result).results);
    
    node.onNext(r2);
    assertSame(r2, node.results.getValue());
    assertSame(node.results, ((ExpressionResult) node.result).results);
    verify(upstream, times(1)).onNext(any(QueryResult.class));
  }
  
  @Test
  public void onNextByteDoubleTwoSubExp() throws Exception {
    expression_config = ExpressionParseNode.newBuilder()
        .setLeft("sub1")
        .setLeftId(new DefaultQueryResultId("sub1", "m1"))
        .setLeftType(OperandType.SUB_EXP)
        .setRight("sub2")
        .setRightId(new DefaultQueryResultId("sub2", "m1"))
        .setRightType(OperandType.SUB_EXP)
        .setExpressionOp(ExpressionOp.ADD)
        .setExpressionConfig(config)
        .setId("expression")
        .build();
    
    BinaryExpressionNode node = new BinaryExpressionNode(
        factory, context, expression_config);
    node.initialize(null);
    
    QueryNode n1 = mock(QueryNode.class);
    QueryNodeConfig c1 = mock(QueryNodeConfig.class);
    when(n1.config()).thenReturn(c1);
    when(c1.getId()).thenReturn("sub1");
    
    QueryNode n2 = mock(QueryNode.class);
    QueryNodeConfig c2 = mock(QueryNodeConfig.class);
    when(n2.config()).thenReturn(c2);
    when(c2.getId()).thenReturn("sub2");
    
    QueryResult r1 = mock(QueryResult.class);
    when(r1.dataSource()).thenReturn(new DefaultQueryResultId("sub1", "m1"));
    when(r1.source()).thenReturn(n1);
    QueryResult r2 = mock(QueryResult.class);
    when(r2.dataSource()).thenReturn(new DefaultQueryResultId("sub2", "m1"));
    when(r2.source()).thenReturn(n2);
    when(r1.idType()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return Const.TS_BYTE_ID;
      }
    });
    when(r2.idType()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return Const.TS_BYTE_ID;
      }
    });
    TimeSeries ts = mock(TimeSeries.class);
    TimeSeriesByteId id = mock(TimeSeriesByteId.class);
    TimeSeriesDataSourceFactory store = mock(TimeSeriesDataSourceFactory.class);
    when(ts.id()).thenReturn(id);
    when(id.dataStore()).thenReturn(store);
    when(id.type()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return Const.TS_BYTE_ID;
      }
    });
    when(r1.timeSeries()).thenReturn(Lists.newArrayList(ts))
      .thenReturn(Collections.emptyList()); // avoid having to not-mock out the id.
    
    Deferred<List<byte[]>> tags = new Deferred<List<byte[]>>();
    when(store.encodeJoinKeys(any(List.class), any(Span.class)))
      .thenReturn(tags);
    
    node.onNext(r1);
    assertSame(r1, node.results.getKey());
    assertNull(((ExpressionResult) node.result).results);
    verify(upstream, never()).onNext(any(QueryResult.class));
    verify(upstream, never()).onError(any(Throwable.class));
    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(store, never()).encodeJoinMetrics(any(List.class), any(Span.class));
    verify(store, times(1)).encodeJoinKeys(any(List.class), any(Span.class));
    assertNull(node.left_metric);
    assertNull(node.right_metric);
    assertNull(node.joiner.encodedJoins());
    
    // callback tags now
    tags.callback(Lists.newArrayList(new byte[] { 0, 0, 3 }, new byte[] { 0, 0, 3 }));
    verify(upstream, never()).onNext(any(QueryResult.class));
    verify(upstream, never()).onError(any(Throwable.class));
    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(store, never()).encodeJoinMetrics(any(List.class), any(Span.class));
    verify(store, times(1)).encodeJoinKeys(any(List.class), any(Span.class));
    assertEquals(1, node.joiner.encodedJoins().size());
    assertArrayEquals(new byte[] { 0, 0, 3 }, node.joiner.encodedJoins().get(new byte[] { 0, 0, 3 }));
    assertNull(((ExpressionResult) node.result).results);
    
    node.onNext(r2);
    assertSame(r2, node.results.getValue());
    assertSame(node.results, ((ExpressionResult) node.result).results);
    verify(upstream, times(1)).onNext(any(QueryResult.class));
  }
  
  @Test
  public void onNextByteSingleLeft() throws Exception {
    expression_config = ExpressionParseNode.newBuilder()
        .setLeft("42")
        .setLeftType(OperandType.LITERAL_NUMERIC)
        .setRight("b")
        .setRightId(new DefaultQueryResultId("b", "b"))
        .setRightType(OperandType.VARIABLE)
        .setExpressionOp(ExpressionOp.ADD)
        .setExpressionConfig(config)
        .setId("expression")
        .build();
    
    BinaryExpressionNode node = new BinaryExpressionNode(
        factory, context, expression_config);
    node.initialize(null);
    
    QueryResult r1 = mock(QueryResult.class);
    when(r1.dataSource()).thenReturn(new DefaultQueryResultId("b", "b"));
    QueryNode n1 = mock(QueryNode.class);
    QueryNodeConfig c1 = mock(QueryNodeConfig.class);
    when(c1.getId()).thenReturn("a");
    when(n1.config()).thenReturn(c1);
    when(r1.source()).thenReturn(n1);
    when(r1.idType()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return Const.TS_BYTE_ID;
      }
    });
    TimeSeries ts = mock(TimeSeries.class);
    TimeSeriesByteId id = mock(TimeSeriesByteId.class);
    TimeSeriesDataSourceFactory store = mock(TimeSeriesDataSourceFactory.class);
    when(ts.id()).thenReturn(id);
    when(id.dataStore()).thenReturn(store);
    when(id.type()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return Const.TS_BYTE_ID;
      }
    });
    when(r1.timeSeries()).thenReturn(Lists.newArrayList(ts))
      .thenReturn(Lists.newArrayList(ts))
      .thenReturn(Lists.newArrayList(ts))
      .thenReturn(Lists.newArrayList(ts))
      .thenReturn(Collections.emptyList()); // avoid having to not-mock out the id.
    
    Deferred<List<byte[]>> metrics = new Deferred<List<byte[]>>();
    Deferred<List<byte[]>> tags = new Deferred<List<byte[]>>();
    when(store.encodeJoinMetrics(any(List.class), any(Span.class)))
      .thenReturn(metrics);
    when(store.encodeJoinKeys(any(List.class), any(Span.class)))
      .thenReturn(tags);
    
    node.onNext(r1);
    assertSame(r1, node.results.getValue());
    assertNull(((ExpressionResult) node.result).results);
    verify(upstream, never()).onNext(any(QueryResult.class));
    verify(upstream, never()).onError(any(Throwable.class));
    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(store, times(1)).encodeJoinMetrics(any(List.class), any(Span.class));
    verify(store, never()).encodeJoinKeys(any(List.class), any(Span.class));
    assertNull(node.left_metric);
    assertNull(node.right_metric);
    assertNull(node.joiner.encodedJoins());
    
    // callback.
    metrics.callback(Lists.newArrayList(new byte[] { 0, 0, 1 }, null));
    verify(upstream, never()).onNext(any(QueryResult.class));
    verify(upstream, never()).onError(any(Throwable.class));
    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(store, times(1)).encodeJoinMetrics(any(List.class), any(Span.class));
    verify(store, times(1)).encodeJoinKeys(any(List.class), any(Span.class));
    assertNull(node.left_metric);
    assertArrayEquals(new byte[] { 0, 0, 1 }, node.right_metric);
    assertNull(node.joiner.encodedJoins());
    
    // callback tags now
    tags.callback(Lists.newArrayList(new byte[] { 0, 0, 3 }, new byte[] { 0, 0, 3 }));
    verify(upstream, times(1)).onNext(any(QueryResult.class));
    verify(upstream, never()).onError(any(Throwable.class));
    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(store, times(1)).encodeJoinMetrics(any(List.class), any(Span.class));
    verify(store, times(1)).encodeJoinKeys(any(List.class), any(Span.class));
    assertEquals(1, node.joiner.encodedJoins().size());
    assertArrayEquals(new byte[] { 0, 0, 3 }, node.joiner.encodedJoins().get(new byte[] { 0, 0, 3 }));
    assertSame(node.results, ((ExpressionResult) node.result).results);
  }
  
  @Test
  public void onNextByteSingleRight() throws Exception {
    expression_config = ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftId(new DefaultQueryResultId("a", "a"))
        .setLeftType(OperandType.VARIABLE)
        .setRight("42")
        .setRightType(OperandType.LITERAL_NUMERIC)
        .setExpressionOp(ExpressionOp.ADD)
        .setExpressionConfig(config)
        .setId("expression")
        .build();
    
    BinaryExpressionNode node = new BinaryExpressionNode(
        factory, context, expression_config);
    node.initialize(null);
    
    QueryResult r1 = mock(QueryResult.class);
    when(r1.dataSource()).thenReturn(new DefaultQueryResultId("a", "a"));
    QueryNode n1 = mock(QueryNode.class);
    QueryNodeConfig c1 = mock(QueryNodeConfig.class);
    when(c1.getId()).thenReturn("a");
    when(n1.config()).thenReturn(c1);
    when(r1.source()).thenReturn(n1);
    when(r1.idType()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return Const.TS_BYTE_ID;
      }
    });
    TimeSeries ts = mock(TimeSeries.class);
    TimeSeriesByteId id = mock(TimeSeriesByteId.class);
    TimeSeriesDataSourceFactory store = mock(TimeSeriesDataSourceFactory.class);
    when(ts.id()).thenReturn(id);
    when(id.dataStore()).thenReturn(store);
    when(id.type()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return Const.TS_BYTE_ID;
      }
    });
    when(r1.timeSeries()).thenReturn(Lists.newArrayList(ts))
      .thenReturn(Lists.newArrayList(ts))
      .thenReturn(Lists.newArrayList(ts))
      .thenReturn(Lists.newArrayList(ts))
      .thenReturn(Collections.emptyList()); // avoid having to not-mock out the id.
    
    Deferred<List<byte[]>> metrics = new Deferred<List<byte[]>>();
    Deferred<List<byte[]>> tags = new Deferred<List<byte[]>>();
    when(store.encodeJoinMetrics(any(List.class), any(Span.class)))
      .thenReturn(metrics);
    when(store.encodeJoinKeys(any(List.class), any(Span.class)))
      .thenReturn(tags);
    
    node.onNext(r1);
    assertSame(r1, node.results.getKey());
    assertNull(((ExpressionResult) node.result).results);
    verify(upstream, never()).onNext(any(QueryResult.class));
    verify(upstream, never()).onError(any(Throwable.class));
    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(store, times(1)).encodeJoinMetrics(any(List.class), any(Span.class));
    verify(store, never()).encodeJoinKeys(any(List.class), any(Span.class));
    assertNull(node.left_metric);
    assertNull(node.right_metric);
    assertNull(node.joiner.encodedJoins());
    
    // callback.
    metrics.callback(Lists.newArrayList(new byte[] { 0, 0, 2 }));
    verify(upstream, never()).onNext(any(QueryResult.class));
    verify(upstream, never()).onError(any(Throwable.class));
    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(store, times(1)).encodeJoinMetrics(any(List.class), any(Span.class));
    verify(store, times(1)).encodeJoinKeys(any(List.class), any(Span.class));
    assertArrayEquals(new byte[] { 0, 0, 2 }, node.left_metric);
    assertNull(node.right_metric);
    assertNull(node.joiner.encodedJoins());
    
    // callback tags now
    tags.callback(Lists.newArrayList(new byte[] { 0, 0, 3 }, new byte[] { 0, 0, 3 }));
    verify(upstream, times(1)).onNext(any(QueryResult.class));
    verify(upstream, never()).onError(any(Throwable.class));
    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(store, times(1)).encodeJoinMetrics(any(List.class), any(Span.class));
    verify(store, times(1)).encodeJoinKeys(any(List.class), any(Span.class));
    assertEquals(1, node.joiner.encodedJoins().size());
    assertArrayEquals(new byte[] { 0, 0, 3 }, node.joiner.encodedJoins().get(new byte[] { 0, 0, 3 }));
    assertSame(node.results, ((ExpressionResult) node.result).results);
  }

  @Test
  public void onNextByteSameOperand() throws Exception {
    expression_config = ExpressionParseNode.newBuilder()
            .setLeft("a")
            .setLeftId(new DefaultQueryResultId("a", "a"))
            .setLeftType(OperandType.VARIABLE)
            .setRight("a")
            .setRightId(new DefaultQueryResultId("a", "a"))
            .setRightType(OperandType.VARIABLE)
            .setExpressionOp(ExpressionOp.ADD)
            .setExpressionConfig(config)
            .setId("expression")
            .build();

    BinaryExpressionNode node = new BinaryExpressionNode(
            factory, context, expression_config);
    node.initialize(null);

    QueryResult r1 = mock(QueryResult.class);
    when(r1.dataSource()).thenReturn(new DefaultQueryResultId("a", "a"));
    QueryNode n1 = mock(QueryNode.class);
    QueryNodeConfig c1 = mock(QueryNodeConfig.class);
    when(c1.getId()).thenReturn("a");
    when(n1.config()).thenReturn(c1);
    when(r1.source()).thenReturn(n1);
    when(r1.idType()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return Const.TS_BYTE_ID;
      }
    });
    TimeSeries ts = mock(TimeSeries.class);
    TimeSeriesByteId id = mock(TimeSeriesByteId.class);
    TimeSeriesDataSourceFactory store = mock(TimeSeriesDataSourceFactory.class);
    when(ts.id()).thenReturn(id);
    when(id.dataStore()).thenReturn(store);
    when(id.type()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return Const.TS_BYTE_ID;
      }
    });
    when(r1.timeSeries()).thenReturn(Lists.newArrayList(ts))
            .thenReturn(Lists.newArrayList(ts))
            .thenReturn(Lists.newArrayList(ts))
            .thenReturn(Lists.newArrayList(ts))
            .thenReturn(Collections.emptyList()); // avoid having to not-mock out the id.

    Deferred<List<byte[]>> metrics = new Deferred<List<byte[]>>();
    Deferred<List<byte[]>> tags = new Deferred<List<byte[]>>();
    when(store.encodeJoinMetrics(any(List.class), any(Span.class)))
            .thenReturn(metrics);
    when(store.encodeJoinKeys(any(List.class), any(Span.class)))
            .thenReturn(tags);

    node.onNext(r1);
    assertSame(r1, node.results.getKey());
    assertNull(((ExpressionResult) node.result).results);
    verify(upstream, never()).onNext(any(QueryResult.class));
    verify(upstream, never()).onError(any(Throwable.class));
    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(store, times(1)).encodeJoinMetrics(any(List.class), any(Span.class));
    verify(store, never()).encodeJoinKeys(any(List.class), any(Span.class));
    assertNull(node.left_metric);
    assertNull(node.right_metric);
    assertNull(node.joiner.encodedJoins());

    // callback.
    metrics.callback(Lists.newArrayList(new byte[] { 0, 0, 2 }));
    verify(upstream, never()).onNext(any(QueryResult.class));
    verify(upstream, never()).onError(any(Throwable.class));
    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(store, times(1)).encodeJoinMetrics(any(List.class), any(Span.class));
    verify(store, times(1)).encodeJoinKeys(any(List.class), any(Span.class));
    assertArrayEquals(new byte[] { 0, 0, 2 }, node.left_metric);
    assertNotNull(node.right_metric);
    assertNull(node.joiner.encodedJoins());

    // callback tags now
    tags.callback(Lists.newArrayList(new byte[] { 0, 0, 3 }, new byte[] { 0, 0, 3 }));
    verify(upstream, times(1)).onNext(any(QueryResult.class));
    verify(upstream, never()).onError(any(Throwable.class));
    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(store, times(1)).encodeJoinMetrics(any(List.class), any(Span.class));
    verify(store, times(1)).encodeJoinKeys(any(List.class), any(Span.class));
    assertEquals(1, node.joiner.encodedJoins().size());
    assertArrayEquals(new byte[] { 0, 0, 3 }, node.joiner.encodedJoins().get(new byte[] { 0, 0, 3 }));
    assertSame(node.results, ((ExpressionResult) node.result).results);
  }
  
  @Test
  public void onNextByteMetricException() throws Exception {
    BinaryExpressionNode node = new BinaryExpressionNode(
        factory, context, expression_config);
    node.initialize(null);
    
    QueryResult r1 = mock(QueryResult.class);
    when(r1.dataSource()).thenReturn(new DefaultQueryResultId("a", "a"));
    QueryNode n1 = mock(QueryNode.class);
    QueryNodeConfig c1 = mock(QueryNodeConfig.class);
    when(c1.getId()).thenReturn("a");
    when(n1.config()).thenReturn(c1);
    when(r1.source()).thenReturn(n1);
    QueryResult r2 = mock(QueryResult.class);
    when(r2.dataSource()).thenReturn(new DefaultQueryResultId("b", "b"));
    QueryNode n2 = mock(QueryNode.class);
    QueryNodeConfig c2 = mock(QueryNodeConfig.class);
    when(c2.getId()).thenReturn("b");
    when(n2.config()).thenReturn(c1);
    when(r2.source()).thenReturn(n1);
    when(r1.idType()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return Const.TS_BYTE_ID;
      }
    });
    when(r2.idType()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return Const.TS_BYTE_ID;
      }
    });
    TimeSeries ts = mock(TimeSeries.class);
    TimeSeriesByteId id = mock(TimeSeriesByteId.class);
    TimeSeriesDataSourceFactory store = mock(TimeSeriesDataSourceFactory.class);
    when(ts.id()).thenReturn(id);
    when(id.dataStore()).thenReturn(store);
    when(id.type()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return Const.TS_BYTE_ID;
      }
    });
    when(r1.timeSeries()).thenReturn(Lists.newArrayList(ts))
      .thenReturn(Lists.newArrayList(ts))
      .thenReturn(Lists.newArrayList(ts))
      .thenReturn(Collections.emptyList()); // avoid having to not-mock out the id.
    
    Deferred<List<byte[]>> metrics = new Deferred<List<byte[]>>();
    Deferred<List<byte[]>> tags = new Deferred<List<byte[]>>();
    when(store.encodeJoinMetrics(any(List.class), any(Span.class)))
      .thenReturn(metrics);
    when(store.encodeJoinKeys(any(List.class), any(Span.class)))
      .thenReturn(tags);
    
    node.onNext(r1);
    assertSame(r1, node.results.getKey());
    assertNull(((ExpressionResult) node.result).results);
    verify(upstream, never()).onNext(any(QueryResult.class));
    verify(upstream, never()).onError(any(Throwable.class));
    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(store, times(1)).encodeJoinMetrics(any(List.class), any(Span.class));
    verify(store, never()).encodeJoinKeys(any(List.class), any(Span.class));
    assertNull(node.left_metric);
    assertNull(node.right_metric);
    assertNull(node.joiner.encodedJoins());
    
    // callback.
    metrics.callback(new UnitTestException());
    verify(upstream, never()).onNext(any(QueryResult.class));
    verify(upstream, times(1)).onError(any(Throwable.class));
    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(store, times(1)).encodeJoinMetrics(any(List.class), any(Span.class));
    verify(store, never()).encodeJoinKeys(any(List.class), any(Span.class));
    assertNull(node.left_metric);
    assertNull(node.right_metric);
    assertNull(node.joiner.encodedJoins());
    assertNull(((ExpressionResult) node.result).results);
  }
  
  @Test
  public void onNextByteTagException() throws Exception {
    BinaryExpressionNode node = new BinaryExpressionNode(
        factory, context, expression_config);
    node.initialize(null);
    
    QueryResult r1 = mock(QueryResult.class);
    when(r1.dataSource()).thenReturn(new DefaultQueryResultId("a", "a"));
    QueryNode n1 = mock(QueryNode.class);
    QueryNodeConfig c1 = mock(QueryNodeConfig.class);
    when(c1.getId()).thenReturn("a");
    when(n1.config()).thenReturn(c1);
    when(r1.source()).thenReturn(n1);
    QueryResult r2 = mock(QueryResult.class);
    when(r2.dataSource()).thenReturn(new DefaultQueryResultId("b", "b"));
    QueryNode n2 = mock(QueryNode.class);
    QueryNodeConfig c2 = mock(QueryNodeConfig.class);
    when(c2.getId()).thenReturn("a");
    when(n2.config()).thenReturn(c2);
    when(r2.source()).thenReturn(n2);
    when(r1.idType()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return Const.TS_BYTE_ID;
      }
    });
    when(r2.idType()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return Const.TS_BYTE_ID;
      }
    });
    TimeSeries ts = mock(TimeSeries.class);
    TimeSeriesByteId id = mock(TimeSeriesByteId.class);
    TimeSeriesDataSourceFactory store = mock(TimeSeriesDataSourceFactory.class);
    when(ts.id()).thenReturn(id);
    when(id.dataStore()).thenReturn(store);
    when(id.type()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return Const.TS_BYTE_ID;
      }
    });
    when(r1.timeSeries()).thenReturn(Lists.newArrayList(ts))
      .thenReturn(Lists.newArrayList(ts))
      .thenReturn(Lists.newArrayList(ts))
      .thenReturn(Lists.newArrayList(ts))
      .thenReturn(Collections.emptyList()); // avoid having to not-mock out the id.
    
    Deferred<List<byte[]>> metrics = new Deferred<List<byte[]>>();
    Deferred<List<byte[]>> tags = new Deferred<List<byte[]>>();
    when(store.encodeJoinMetrics(any(List.class), any(Span.class)))
      .thenReturn(metrics);
    when(store.encodeJoinKeys(any(List.class), any(Span.class)))
      .thenReturn(tags);
    
    node.onNext(r1);
    assertSame(r1, node.results.getKey());
    assertNull(((ExpressionResult) node.result).results);
    verify(upstream, never()).onNext(any(QueryResult.class));
    verify(upstream, never()).onError(any(Throwable.class));
    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(store, times(1)).encodeJoinMetrics(any(List.class), any(Span.class));
    verify(store, never()).encodeJoinKeys(any(List.class), any(Span.class));
    assertNull(node.left_metric);
    assertNull(node.right_metric);
    assertNull(node.joiner.encodedJoins());
    
    // callback.
    metrics.callback(Lists.newArrayList(new byte[] { 0, 0, 1 }, new byte[] { 0, 0, 2 }));
    verify(upstream, never()).onNext(any(QueryResult.class));
    verify(upstream, never()).onError(any(Throwable.class));
    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(store, times(1)).encodeJoinMetrics(any(List.class), any(Span.class));
    verify(store, times(1)).encodeJoinKeys(any(List.class), any(Span.class));
    assertArrayEquals(new byte[] { 0, 0, 1 }, node.left_metric);
    assertArrayEquals(new byte[] { 0, 0, 2 }, node.right_metric);
    assertNull(node.joiner.encodedJoins());
    
    // callback tags now
    tags.callback(new UnitTestException());
    verify(upstream, never()).onNext(any(QueryResult.class));
    verify(upstream, times(1)).onError(any(Throwable.class));
    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(store, times(1)).encodeJoinMetrics(any(List.class), any(Span.class));
    verify(store, times(1)).encodeJoinKeys(any(List.class), any(Span.class));
    assertNull(node.joiner.encodedJoins());
    assertNull(((ExpressionResult) node.result).results);
  }
 
  @Test
  public void onNextByteNullTag() throws Exception {
    BinaryExpressionNode node = new BinaryExpressionNode(
        factory, context, expression_config);
    node.initialize(null);
    
    QueryResult r1 = mock(QueryResult.class);
    when(r1.dataSource()).thenReturn(new DefaultQueryResultId("a", "a"));
    QueryNode n1 = mock(QueryNode.class);
    QueryNodeConfig c1 = mock(QueryNodeConfig.class);
    when(c1.getId()).thenReturn("a");
    when(n1.config()).thenReturn(c1);
    when(r1.source()).thenReturn(n1);
    QueryResult r2 = mock(QueryResult.class);
    when(r2.dataSource()).thenReturn(new DefaultQueryResultId("b", "b"));
    QueryNode n2 = mock(QueryNode.class);
    QueryNodeConfig c2 = mock(QueryNodeConfig.class);
    when(c2.getId()).thenReturn("a");
    when(n2.config()).thenReturn(c2);
    when(r2.source()).thenReturn(n2);
    when(r1.idType()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return Const.TS_BYTE_ID;
      }
    });
    when(r2.idType()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return Const.TS_BYTE_ID;
      }
    });
    TimeSeries ts = mock(TimeSeries.class);
    TimeSeriesByteId id = mock(TimeSeriesByteId.class);
    TimeSeriesDataSourceFactory store = mock(TimeSeriesDataSourceFactory.class);
    when(ts.id()).thenReturn(id);
    when(id.dataStore()).thenReturn(store);
    when(id.type()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return Const.TS_BYTE_ID;
      }
    });
    when(r1.timeSeries()).thenReturn(Lists.newArrayList(ts))
      .thenReturn(Lists.newArrayList(ts))
      .thenReturn(Lists.newArrayList(ts))
      .thenReturn(Lists.newArrayList(ts))
      .thenReturn(Collections.emptyList()); // avoid having to not-mock out the id.
    
    Deferred<List<byte[]>> metrics = new Deferred<List<byte[]>>();
    Deferred<List<byte[]>> tags = new Deferred<List<byte[]>>();
    when(store.encodeJoinMetrics(any(List.class), any(Span.class)))
      .thenReturn(metrics);
    when(store.encodeJoinKeys(any(List.class), any(Span.class)))
      .thenReturn(tags);
    
    node.onNext(r1);
    assertSame(r1, node.results.getKey());
    assertNull(((ExpressionResult) node.result).results);
    verify(upstream, never()).onNext(any(QueryResult.class));
    verify(upstream, never()).onError(any(Throwable.class));
    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(store, times(1)).encodeJoinMetrics(any(List.class), any(Span.class));
    verify(store, never()).encodeJoinKeys(any(List.class), any(Span.class));
    assertNull(node.left_metric);
    assertNull(node.right_metric);
    assertNull(node.joiner.encodedJoins());
    
    // callback.
    metrics.callback(Lists.newArrayList(new byte[] { 0, 0, 1 }, new byte[] { 0, 0, 2 }));
    verify(upstream, never()).onNext(any(QueryResult.class));
    verify(upstream, never()).onError(any(Throwable.class));
    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(store, times(1)).encodeJoinMetrics(any(List.class), any(Span.class));
    verify(store, times(1)).encodeJoinKeys(any(List.class), any(Span.class));
    assertArrayEquals(new byte[] { 0, 0, 1 }, node.left_metric);
    assertArrayEquals(new byte[] { 0, 0, 2 }, node.right_metric);
    assertNull(node.joiner.encodedJoins());
    
    // callback tags now
    tags.callback(Lists.newArrayList(new byte[] { 0, 0, 1 }, null));
    verify(upstream, never()).onNext(any(QueryResult.class));
    verify(upstream, times(1)).onError(any(Throwable.class));
    verify(upstream, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(store, times(1)).encodeJoinMetrics(any(List.class), any(Span.class));
    verify(store, times(1)).encodeJoinKeys(any(List.class), any(Span.class));
    assertNull(node.joiner.encodedJoins());
    assertNull(((ExpressionResult) node.result).results);
  }

  @Test
  public void onNextError() throws Exception {
    BinaryExpressionNode node = new BinaryExpressionNode(
        factory, context, expression_config);
    node.initialize(null);
    
    QueryResult r1 = mock(QueryResult.class);
    when(r1.dataSource()).thenReturn(new DefaultQueryResultId("a", "a"));
    QueryNode n1 = mock(QueryNode.class);
    QueryNodeConfig c1 = mock(QueryNodeConfig.class);
    when(c1.getId()).thenReturn("a");
    when(n1.config()).thenReturn(c1);
    when(r1.source()).thenReturn(n1);
    when(r1.error()).thenReturn("Boo!");
    
    node.onNext(r1);
    verify(upstream, times(1)).onNext(any(FailedQueryResult.class));
  }
  
}
