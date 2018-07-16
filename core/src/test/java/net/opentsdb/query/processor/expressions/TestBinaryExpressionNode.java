// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
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
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.joins.JoinConfig;
import net.opentsdb.query.joins.JoinConfig.JoinType;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.processor.expressions.ExpressionParseNode.ExpressionOp;
import net.opentsdb.query.processor.expressions.ExpressionParseNode.OperandType;
import net.opentsdb.stats.Span;
import net.opentsdb.storage.ReadableTimeSeriesDataStore;
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
      .setType(NumericType.TYPE.toString())
      .build();
    
    join_config = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.INNER)
        .addJoins("host", "host")
        .setId("join")
        .build();
    
    config = (ExpressionConfig) ExpressionConfig.newBuilder()
        .setExpression("a + b + c")
        .setJoinConfig(join_config)
        .addInterpolatorConfig(numeric_config)
        .setId("e1")
        .build();
    
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight("b")
        .setRightType(OperandType.VARIABLE)
        .setExpressionOp(ExpressionOp.ADD)
        .build();
  }
  
  @Test
  public void ctor() throws Exception {
    BinaryExpressionNode node = new BinaryExpressionNode(
        factory, context, "a+b", config, expression_config);
    assertSame(config, node.config);
    assertSame(expression_config, node.expressionConfig());
    assertTrue(node.need_two_sources);
    assertNotNull(node.result);
    assertNotNull(node.joiner());
    
    // sub-exp
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight("SubExp#1")
        .setRightType(OperandType.SUB_EXP)
        .setExpressionOp(ExpressionOp.ADD)
        .build();
    node = new BinaryExpressionNode(
        factory, context, "a+b", config, expression_config);
    assertSame(config, node.config);
    assertSame(expression_config, node.expressionConfig());
    assertTrue(node.need_two_sources);
    assertNotNull(node.result);
    assertNotNull(node.joiner());
    
    // one needed
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight("42#1")
        .setRightType(OperandType.LITERAL_NUMERIC)
        .setExpressionOp(ExpressionOp.ADD)
        .build();
    node = new BinaryExpressionNode(
        factory, context, "a+b", config, expression_config);
    assertSame(config, node.config);
    assertSame(expression_config, node.expressionConfig());
    assertFalse(node.need_two_sources);
    assertNotNull(node.result);
    assertNotNull(node.joiner());
    
    try {
      new BinaryExpressionNode(
          factory, null, "a+b", config, expression_config);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new BinaryExpressionNode(
          factory, context, "a+b", null, expression_config);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new BinaryExpressionNode(
          factory, context, "a+b", config, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void onComplete() throws Exception {
    BinaryExpressionNode node = new BinaryExpressionNode(
        factory, context, "a+b", config, expression_config);
    node.initialize(null);
    
    node.onComplete(mock(QueryNode.class), 42, 42);
    verify(upstream, times(1)).onComplete(node, 42, 42);
    
    doThrow(new IllegalArgumentException("Boo!")).when(upstream)
      .onComplete(any(QueryNode.class), anyLong(), anyLong());
    node.onComplete(mock(QueryNode.class), 42, 42);
    verify(upstream, times(2)).onComplete(node, 42, 42);
  }
  
  @Test
  public void onError() throws Exception {
    BinaryExpressionNode node = new BinaryExpressionNode(
        factory, context, "a+b", config, expression_config);
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
        factory, context, "a+b", config, expression_config);
    node.initialize(null);
    
    QueryResult r1 = mock(QueryResult.class);
    QueryResult r2 = mock(QueryResult.class);
    
    node.onNext(r1);
    assertEquals(1, ((ExpressionResult) node.result).results.size());
    verify(upstream, never()).onNext(any(QueryResult.class));
    
    node.onNext(r2);
    assertEquals(2, ((ExpressionResult) node.result).results.size());
    verify(upstream, times(1)).onNext(any(QueryResult.class));
  }
  
  @Test
  public void onNextStringSingle() throws Exception {
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight("42")
        .setRightType(OperandType.LITERAL_NUMERIC)
        .setExpressionOp(ExpressionOp.ADD)
        .build();
    
    BinaryExpressionNode node = new BinaryExpressionNode(
        factory, context, "a+b", config, expression_config);
    node.initialize(null);
    
    QueryResult r1 = mock(QueryResult.class);
    
    node.onNext(r1);
    assertEquals(1, ((ExpressionResult) node.result).results.size());
    verify(upstream, times(1)).onNext(any(QueryResult.class));
  }

  @Test
  public void onNextByteDouble() throws Exception {
    BinaryExpressionNode node = new BinaryExpressionNode(
        factory, context, "a+b", config, expression_config);
    node.initialize(null);
    
    QueryResult r1 = mock(QueryResult.class);
    QueryResult r2 = mock(QueryResult.class);
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
    ReadableTimeSeriesDataStore store = mock(ReadableTimeSeriesDataStore.class);
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
      .thenReturn(Collections.emptyList()); // avoid having to not-mock out the id.
    
    Deferred<List<byte[]>> metrics = new Deferred<List<byte[]>>();
    Deferred<List<byte[]>> tags = new Deferred<List<byte[]>>();
    when(store.encodeJoinMetrics(any(List.class), any(Span.class)))
      .thenReturn(metrics);
    when(store.encodeJoinKeys(any(List.class), any(Span.class)))
      .thenReturn(tags);
    
    node.onNext(r1);
    assertEquals(0, ((ExpressionResult) node.result).results.size());
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
    assertEquals(1, ((ExpressionResult) node.result).results.size());
    
    node.onNext(r2);
    assertEquals(2, ((ExpressionResult) node.result).results.size());
    verify(upstream, times(1)).onNext(any(QueryResult.class));
  }
  
  @Test
  public void onNextByteDoubleOneSubExp() throws Exception {
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight("sub")
        .setRightType(OperandType.SUB_EXP)
        .setExpressionOp(ExpressionOp.ADD)
        .build();
    
    BinaryExpressionNode node = new BinaryExpressionNode(
        factory, context, "a+sub", config, expression_config);
    node.initialize(null);
    
    QueryResult r1 = mock(QueryResult.class);
    QueryResult r2 = mock(QueryResult.class);
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
    ReadableTimeSeriesDataStore store = mock(ReadableTimeSeriesDataStore.class);
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
      .thenReturn(Collections.emptyList()); // avoid having to not-mock out the id.
    
    Deferred<List<byte[]>> metrics = new Deferred<List<byte[]>>();
    Deferred<List<byte[]>> tags = new Deferred<List<byte[]>>();
    when(store.encodeJoinMetrics(any(List.class), any(Span.class)))
      .thenReturn(metrics);
    when(store.encodeJoinKeys(any(List.class), any(Span.class)))
      .thenReturn(tags);
    
    node.onNext(r1);
    assertEquals(0, ((ExpressionResult) node.result).results.size());
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
    assertEquals(1, ((ExpressionResult) node.result).results.size());
    
    node.onNext(r2);
    assertEquals(2, ((ExpressionResult) node.result).results.size());
    verify(upstream, times(1)).onNext(any(QueryResult.class));
  }
  
  @Test
  public void onNextByteDoubleTwoSubExp() throws Exception {
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("sub1")
        .setLeftType(OperandType.SUB_EXP)
        .setRight("sub2")
        .setRightType(OperandType.SUB_EXP)
        .setExpressionOp(ExpressionOp.ADD)
        .build();
    
    BinaryExpressionNode node = new BinaryExpressionNode(
        factory, context, "a+sub", config, expression_config);
    node.initialize(null);
    
    QueryResult r1 = mock(QueryResult.class);
    QueryResult r2 = mock(QueryResult.class);
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
    ReadableTimeSeriesDataStore store = mock(ReadableTimeSeriesDataStore.class);
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
    assertEquals(0, ((ExpressionResult) node.result).results.size());
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
    assertEquals(1, ((ExpressionResult) node.result).results.size());
    
    node.onNext(r2);
    assertEquals(2, ((ExpressionResult) node.result).results.size());
    verify(upstream, times(1)).onNext(any(QueryResult.class));
  }
  
  @Test
  public void onNextByteSingleLeft() throws Exception {
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("42")
        .setLeftType(OperandType.LITERAL_NUMERIC)
        .setRight("b")
        .setRightType(OperandType.VARIABLE)
        .setExpressionOp(ExpressionOp.ADD)
        .build();
    
    BinaryExpressionNode node = new BinaryExpressionNode(
        factory, context, "a+b", config, expression_config);
    node.initialize(null);
    
    QueryResult r1 = mock(QueryResult.class);
    when(r1.idType()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return Const.TS_BYTE_ID;
      }
    });
    TimeSeries ts = mock(TimeSeries.class);
    TimeSeriesByteId id = mock(TimeSeriesByteId.class);
    ReadableTimeSeriesDataStore store = mock(ReadableTimeSeriesDataStore.class);
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
      .thenReturn(Collections.emptyList()); // avoid having to not-mock out the id.
    
    Deferred<List<byte[]>> metrics = new Deferred<List<byte[]>>();
    Deferred<List<byte[]>> tags = new Deferred<List<byte[]>>();
    when(store.encodeJoinMetrics(any(List.class), any(Span.class)))
      .thenReturn(metrics);
    when(store.encodeJoinKeys(any(List.class), any(Span.class)))
      .thenReturn(tags);
    
    node.onNext(r1);
    assertEquals(0, ((ExpressionResult) node.result).results.size());
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
    assertEquals(1, ((ExpressionResult) node.result).results.size());
  }
  
  @Test
  public void onNextByteSingleRight() throws Exception {
    expression_config = (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setRight("42")
        .setRightType(OperandType.LITERAL_NUMERIC)
        .setExpressionOp(ExpressionOp.ADD)
        .build();
    
    BinaryExpressionNode node = new BinaryExpressionNode(
        factory, context, "a+b", config, expression_config);
    node.initialize(null);
    
    QueryResult r1 = mock(QueryResult.class);
    when(r1.idType()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return Const.TS_BYTE_ID;
      }
    });
    TimeSeries ts = mock(TimeSeries.class);
    TimeSeriesByteId id = mock(TimeSeriesByteId.class);
    ReadableTimeSeriesDataStore store = mock(ReadableTimeSeriesDataStore.class);
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
      .thenReturn(Collections.emptyList()); // avoid having to not-mock out the id.
    
    Deferred<List<byte[]>> metrics = new Deferred<List<byte[]>>();
    Deferred<List<byte[]>> tags = new Deferred<List<byte[]>>();
    when(store.encodeJoinMetrics(any(List.class), any(Span.class)))
      .thenReturn(metrics);
    when(store.encodeJoinKeys(any(List.class), any(Span.class)))
      .thenReturn(tags);
    
    node.onNext(r1);
    assertEquals(0, ((ExpressionResult) node.result).results.size());
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
    assertEquals(1, ((ExpressionResult) node.result).results.size());
  }
  
  @Test
  public void onNextByteMetricException() throws Exception {
    BinaryExpressionNode node = new BinaryExpressionNode(
        factory, context, "a+b", config, expression_config);
    node.initialize(null);
    
    QueryResult r1 = mock(QueryResult.class);
    QueryResult r2 = mock(QueryResult.class);
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
    ReadableTimeSeriesDataStore store = mock(ReadableTimeSeriesDataStore.class);
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
      .thenReturn(Collections.emptyList()); // avoid having to not-mock out the id.
    
    Deferred<List<byte[]>> metrics = new Deferred<List<byte[]>>();
    Deferred<List<byte[]>> tags = new Deferred<List<byte[]>>();
    when(store.encodeJoinMetrics(any(List.class), any(Span.class)))
      .thenReturn(metrics);
    when(store.encodeJoinKeys(any(List.class), any(Span.class)))
      .thenReturn(tags);
    
    node.onNext(r1);
    assertEquals(0, ((ExpressionResult) node.result).results.size());
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
  }
  
  @Test
  public void onNextByteTagException() throws Exception {
    BinaryExpressionNode node = new BinaryExpressionNode(
        factory, context, "a+b", config, expression_config);
    node.initialize(null);
    
    QueryResult r1 = mock(QueryResult.class);
    QueryResult r2 = mock(QueryResult.class);
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
    ReadableTimeSeriesDataStore store = mock(ReadableTimeSeriesDataStore.class);
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
      .thenReturn(Collections.emptyList()); // avoid having to not-mock out the id.
    
    Deferred<List<byte[]>> metrics = new Deferred<List<byte[]>>();
    Deferred<List<byte[]>> tags = new Deferred<List<byte[]>>();
    when(store.encodeJoinMetrics(any(List.class), any(Span.class)))
      .thenReturn(metrics);
    when(store.encodeJoinKeys(any(List.class), any(Span.class)))
      .thenReturn(tags);
    
    node.onNext(r1);
    assertEquals(0, ((ExpressionResult) node.result).results.size());
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
    assertEquals(0, ((ExpressionResult) node.result).results.size());
  }
 
  @Test
  public void onNextByteNullTag() throws Exception {
    BinaryExpressionNode node = new BinaryExpressionNode(
        factory, context, "a+b", config, expression_config);
    node.initialize(null);
    
    QueryResult r1 = mock(QueryResult.class);
    QueryResult r2 = mock(QueryResult.class);
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
    ReadableTimeSeriesDataStore store = mock(ReadableTimeSeriesDataStore.class);
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
      .thenReturn(Collections.emptyList()); // avoid having to not-mock out the id.
    
    Deferred<List<byte[]>> metrics = new Deferred<List<byte[]>>();
    Deferred<List<byte[]>> tags = new Deferred<List<byte[]>>();
    when(store.encodeJoinMetrics(any(List.class), any(Span.class)))
      .thenReturn(metrics);
    when(store.encodeJoinKeys(any(List.class), any(Span.class)))
      .thenReturn(tags);
    
    node.onNext(r1);
    assertEquals(0, ((ExpressionResult) node.result).results.size());
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
    assertEquals(0, ((ExpressionResult) node.result).results.size());
  }
}
