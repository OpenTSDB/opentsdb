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
package net.opentsdb.query;

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

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import net.opentsdb.exceptions.QueryUpstreamException;
import net.opentsdb.utils.UnitTestException;

public class TestAbstractQueryNode {

  private QueryNodeFactory factory;
  private QueryPipelineContext context;
  private List<QueryNode> upstream;
  private List<QueryNode> downstream;
  
  @Before
  public void before() throws Exception {
    factory = mock(QueryNodeFactory.class);
    context = mock(QueryPipelineContext.class);
    
    upstream = Lists.newArrayList(
        mock(QueryNode.class),
        mock(QueryNode.class));
    downstream = Lists.newArrayList(
        mock(QueryNode.class),
        mock(QueryNode.class));
    
    when(context.upstream(any(QueryNode.class))).thenReturn(upstream);
    when(context.downstream(any(QueryNode.class))).thenReturn(downstream);
  }
  
  @Test
  public void initialize() throws Exception {
    final TestAQ node = new TestAQ(factory, context);
    assertNull(node.upstream);
    assertNull(node.downstream);
    
    node.initialize();
    assertSame(upstream, node.upstream);
    assertSame(downstream, node.downstream);
  }
  
  @Test
  public void sendUpstream() throws Exception {
    final TestAQ node = new TestAQ(factory, context);
    node.initialize();
    
    try {
      node.sendUpstream((QueryResult) null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    final QueryResult result = mock(QueryResult.class);
    node.sendUpstream(result);
    verify(upstream.get(0), times(1)).onNext(result);
    verify(upstream.get(1), times(1)).onNext(result);
  }
  
  @Test
  public void sendUpstreamExceptionSecond() throws Exception {
    final TestAQ node = new TestAQ(factory, context);
    node.initialize();
    doThrow(new UnitTestException())
      .when(upstream.get(1)).onNext(any(QueryResult.class));
    
    final QueryResult result = mock(QueryResult.class);
    try {
      node.sendUpstream(result);
      fail("Expected QueryUpstreamException");
    } catch (QueryUpstreamException e) { }
    verify(upstream.get(0), times(1)).onNext(result);
    verify(upstream.get(1), times(1)).onNext(result);
  }
  
  @Test
  public void sendUpstreamExceptionFirst() throws Exception {
    final TestAQ node = new TestAQ(factory, context);
    node.initialize();
    doThrow(new UnitTestException())
      .when(upstream.get(0)).onNext(any(QueryResult.class));
    
    final QueryResult result = mock(QueryResult.class);
    try {
      node.sendUpstream(result);
      fail("Expected QueryUpstreamException");
    } catch (QueryUpstreamException e) { }
    verify(upstream.get(0), times(1)).onNext(result);
    verify(upstream.get(1), never()).onNext(result);
  }
  
  @Test
  public void sendUpstreamThrowable() throws Exception {
    final TestAQ node = new TestAQ(factory, context);
    node.initialize();
    
    try {
      node.sendUpstream((Throwable) null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    final UnitTestException ex = new UnitTestException();
    node.sendUpstream(ex);
    verify(upstream.get(0), times(1)).onError(ex);
    verify(upstream.get(1), times(1)).onError(ex);
  }
  
  @Test
  public void sendUpstreamThrowableExceptionSecond() throws Exception {
    final TestAQ node = new TestAQ(factory, context);
    node.initialize();
    doThrow(new UnitTestException())
      .when(upstream.get(1)).onError(any(Throwable.class));
   
    final UnitTestException ex = new UnitTestException();
    node.sendUpstream(ex);
    verify(upstream.get(0), times(1)).onError(ex);
    verify(upstream.get(1), times(1)).onError(ex);
  }
  
  @Test
  public void sendUpstreamThrowableExceptionFirst() throws Exception {
    final TestAQ node = new TestAQ(factory, context);
    node.initialize();
    doThrow(new UnitTestException())
      .when(upstream.get(0)).onError(any(Throwable.class));
   
    final UnitTestException ex = new UnitTestException();
    node.sendUpstream(ex);
    verify(upstream.get(0), times(1)).onError(ex);
    verify(upstream.get(1), times(1)).onError(ex);
  }
  
  @Test
  public void completeUpstream() throws Exception {
    final TestAQ node = new TestAQ(factory, context);
    node.initialize();
    
    node.completeUpstream(42, 42);
    verify(upstream.get(0), times(1)).onComplete(node, 42, 42);
    verify(upstream.get(1), times(1)).onComplete(node, 42, 42);
  }
  
  @Test
  public void completeUpstreamExceptionSecond() throws Exception {
    final TestAQ node = new TestAQ(factory, context);
    node.initialize();
    doThrow(new UnitTestException())
      .when(upstream.get(1)).onComplete(any(QueryNode.class), anyLong(), anyLong());
    
    node.completeUpstream(42, 42);
    verify(upstream.get(0), times(1)).onComplete(node, 42, 42);
    verify(upstream.get(1), times(1)).onComplete(node, 42, 42);
  }
  
  @Test
  public void completeUpstreamExceptionFirst() throws Exception {
    final TestAQ node = new TestAQ(factory, context);
    node.initialize();
    doThrow(new UnitTestException())
      .when(upstream.get(0)).onComplete(any(QueryNode.class), anyLong(), anyLong());
    
    node.completeUpstream(42, 42);
    verify(upstream.get(0), times(1)).onComplete(node, 42, 42);
    verify(upstream.get(1), times(1)).onComplete(node, 42, 42);
  }
  
  class TestAQ extends AbstractQueryNode {

    public TestAQ(QueryNodeFactory factory, QueryPipelineContext context) {
      super(factory, context);
    }

    @Override
    public QueryNodeConfig config() { return null; }

    @Override
    public String id() { return "TestAQ"; }

    @Override
    public void close() { }

    @Override
    public void onComplete(QueryNode downstream, long final_sequence,
        long total_sequences) { }

    @Override
    public void onNext(QueryResult next) { }

    @Override
    public void onError(Throwable t) { }
    
  }
}
