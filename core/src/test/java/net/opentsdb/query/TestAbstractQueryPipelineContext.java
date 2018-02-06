// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collection;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import net.opentsdb.core.DefaultTSDB;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataSource;

public class TestAbstractQueryPipelineContext {

  private DefaultTSDB tsdb;
  private TimeSeriesQuery query;
  private QueryContext context;
  private QuerySink sink1;
  private QuerySink sink2;
  
  @Before
  public void before() throws Exception {
    tsdb = mock(DefaultTSDB.class);
    query = mock(TimeSeriesQuery.class);
    context = mock(QueryContext.class);
    sink1 = mock(QuerySink.class);
    sink2 = mock(QuerySink.class);
  }
  
  @Test
  public void ctor() throws Exception {
    AbstractQueryPipelineContext ctx = new TestContext(tsdb, query, context, 
        Lists.newArrayList(sink1));
    assertSame(context, ctx.queryContext());
    assertSame(ctx, ctx.pipelineContext());
    assertSame(query, ctx.query());
    assertSame(sink1, ctx.sinks().iterator().next());
    assertTrue(ctx.roots().isEmpty());
    assertNull(ctx.config());
    
    try {
      new TestContext(null, query, context, Lists.newArrayList(sink1));
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new TestContext(tsdb, null, context, Lists.newArrayList(sink1));
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new TestContext(tsdb, query, null, Lists.newArrayList(sink1));
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new TestContext(tsdb, query, context, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new TestContext(tsdb, query, context, Lists.newArrayList());
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void upstream() throws Exception {
    final TestContext ctx = new TestContext(tsdb, query, context, 
        Lists.newArrayList(sink1));
    ctx.initialize();
    assertTrue(ctx.upstream(ctx).isEmpty());
    
    Collection<QueryNode> us = ctx.upstream(ctx.n3);
    assertEquals(2, us.size());
    assertTrue(us.contains(ctx.n1));
    assertTrue(us.contains(ctx.n2));
    
    assertSame(ctx.n4, ctx.upstream(ctx.s1).iterator().next());
    assertSame(ctx.n5, ctx.upstream(ctx.s2).iterator().next());
    
    try {
      ctx.upstream(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      ctx.upstream(mock(QueryNode.class));
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void downstream() throws Exception {
    final TestContext ctx = new TestContext(tsdb, query, context, 
        Lists.newArrayList(sink1));
    ctx.initialize();
    assertSame(ctx.n3, ctx.downstream(ctx.n1).iterator().next());
    assertSame(ctx.n3, ctx.downstream(ctx.n2).iterator().next());
    
    Collection<QueryNode> ds = ctx.downstream(ctx.n3);
    assertEquals(2, ds.size());
    assertTrue(ds.contains(ctx.n4));
    assertTrue(ds.contains(ctx.n5));
    
    assertTrue(ctx.downstream(ctx.s1).isEmpty());
    assertTrue(ctx.downstream(ctx.s2).isEmpty());
    
    try {
      ctx.downstream(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      ctx.downstream(mock(QueryNode.class));
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void close() throws Exception {
    final TestContext ctx = new TestContext(tsdb, query, context, 
        Lists.newArrayList(sink1));
    ctx.initialize();
    ctx.close();
    verify(ctx.n1, times(1)).close();
    verify(ctx.n2, times(1)).close();
    verify(ctx.n3, times(1)).close();
    verify(ctx.n4, times(1)).close();
    verify(ctx.n5, times(1)).close();
    verify(ctx.s1, times(1)).close();
    verify(ctx.s2, times(1)).close();
  }
  
  @Test
  public void fetchNext() throws Exception {
    TestContext ctx = new TestContext(tsdb, query, context, 
        Lists.newArrayList(sink1));
    ctx.initialize();
    
    verify(ctx.s1, never()).fetchNext();
    verify(ctx.s2, never()).fetchNext();
    
    ctx.fetchNext();
    verify(ctx.s1, never()).fetchNext();
    verify(ctx.s2, times(1)).fetchNext();
    
    ctx.fetchNext();
    verify(ctx.s1, times(1)).fetchNext();
    verify(ctx.s2, times(1)).fetchNext();
    
    ctx.fetchNext();
    verify(ctx.s1, times(1)).fetchNext();
    verify(ctx.s2, times(2)).fetchNext();
    
    doThrow(new IllegalStateException("Boo!")).when(ctx.s1).fetchNext();
    ctx.fetchNext();
    verify(ctx.s1, times(2)).fetchNext();
    verify(ctx.s2, times(2)).fetchNext();
    verify(sink1, times(1)).onError(any(Throwable.class));
    
    // Test the single mutli-source case.
    when(context.mode()).thenReturn(QueryMode.SINGLE);
    ctx = new TestContext(tsdb, query, context, Lists.newArrayList(sink1));
    ctx.initialize();
    
    verify(ctx.s1, never()).fetchNext();
    verify(ctx.s2, never()).fetchNext();
    
    ctx.fetchNext();
    verify(ctx.s1, times(1)).fetchNext();
    verify(ctx.s2, times(1)).fetchNext();
  }
  
  @Test
  public void onComplete() throws Exception {
    final TestContext ctx = new TestContext(tsdb, query, context, 
        Lists.newArrayList(sink1, sink2));
    ctx.initialize();
    
    doThrow(new IllegalStateException("Boo!")).when(sink1).onComplete();
    
    QueryResult result = mock(QueryResult.class);
    when(result.source()).thenReturn(ctx.n1);
    ctx.onNext(result);
    ctx.onComplete(ctx.n1, 0, 1);
    verify(sink1, never()).onComplete();
    verify(sink2, never()).onComplete();
    
    result = mock(QueryResult.class);
    when(result.source()).thenReturn(ctx.n2);
    ctx.onNext(result);
    ctx.onComplete(ctx.n2, 0, 1);
    verify(sink1, times(1)).onComplete();
    verify(sink2, times(1)).onComplete();
  }
  
  @Test
  public void onNext() throws Exception {
    TestContext ctx = new TestContext(tsdb, query, context, 
        Lists.newArrayList(sink1, sink2));
    ctx.initialize();
    
    doThrow(new IllegalStateException("Boo!")).when(sink1).onNext(any());
    
    QueryResult result = mock(QueryResult.class);
    when(result.source()).thenReturn(ctx.n1);
    ctx.onNext(result);
    ctx.onComplete(ctx.n1, 0, 1);
    verify(sink1, times(1)).onNext(result);
    verify(sink2, times(1)).onNext(result);
    verify(sink1, never()).onComplete();
    verify(sink2, never()).onComplete();
    
    result = mock(QueryResult.class);
    when(result.source()).thenReturn(ctx.n2);
    ctx.onNext(result);
    ctx.onComplete(ctx.n2, 0, 1);
    verify(sink1, times(1)).onNext(result);
    verify(sink2, times(1)).onNext(result);
    verify(sink1, times(1)).onComplete();
    verify(sink2, times(1)).onComplete();
    
    // Test the single multi-source case
    when(context.mode()).thenReturn(QueryMode.SINGLE);
    ctx = new TestContext(tsdb, query, context, Lists.newArrayList(sink1, sink2));
    ctx.initialize();
    
    final TimeSeries t1 = mock(TimeSeries.class);
    final TimeSeries t2 = mock(TimeSeries.class);
    
    assertNull(ctx.single_results);
    result = mock(QueryResult.class);
    when(result.timeSeries()).thenReturn(Lists.newArrayList(t1));
    when(result.source()).thenReturn(ctx.n1);
    ctx.onNext(result);
    ctx.onComplete(ctx.n1, 0, 1);
    assertNotNull(ctx.single_results);
    verify(sink1, never()).onNext(result);
    verify(sink2, never()).onNext(result);
    verify(sink1, never()).onNext(ctx.single_results);
    verify(sink2, never()).onNext(ctx.single_results);
    verify(sink1, times(1)).onComplete();
    verify(sink2, times(1)).onComplete();
    
    result = mock(QueryResult.class);
    when(result.timeSeries()).thenReturn(Lists.newArrayList(t1));
    when(result.source()).thenReturn(ctx.n2);
    ctx.onNext(result);
    ctx.onComplete(ctx.n2, 0, 1);
    assertNotNull(ctx.single_results);
    verify(sink1, times(2)).onComplete();
    verify(sink2, times(2)).onComplete();
    verify(sink1, never()).onNext(result);
    verify(sink2, never()).onNext(result);
    verify(sink1, times(1)).onNext(ctx.single_results);
    verify(sink2, times(1)).onNext(ctx.single_results);
  }
  
  @Test
  public void onError() throws Exception {
    final TestContext ctx = new TestContext(tsdb, query, context, 
        Lists.newArrayList(sink1, sink2));
    ctx.initialize();
    
    doThrow(new IllegalStateException("Boo!")).when(sink1).onError(any());
    
    ctx.onError(new IllegalArgumentException("Bad Query!"));
    verify(sink1, times(1)).onError(any());
    verify(sink2, times(1)).onError(any());
  }
  
  @Test
  public void addVertex() throws Exception {
    final TestContext ctx = new TestContext(tsdb, query, context, 
        Lists.newArrayList(sink1, sink2));
    ctx.initialize();
    
    // same is ok
    ctx.addVertex(ctx.n1);
    
    try {
      ctx.addVertex(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void addDagEdge() throws Exception {
    final TestContext ctx = new TestContext(tsdb, query, context, 
        Lists.newArrayList(sink1, sink2));
    ctx.initialize();
    
    // add existing is ok
    ctx.addDagEdge(ctx.n4, ctx.s1);
    
    try {
      ctx.addDagEdge(null, ctx.s1);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      ctx.addDagEdge(ctx.n4, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // no cycles allowed
    try {
      ctx.addDagEdge(ctx.s1, ctx.n1);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // or dag to self
    try {
      ctx.addDagEdge(ctx.n1, ctx.n1);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void initializeGraph() {
    TestContext ctx = new TestContext(tsdb, query, context, 
        Lists.newArrayList(sink1, sink2));
    // empty is not allowed
    try {
      ctx.initializeGraph();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    ctx.initialize();
  }
  
  class TestContext extends AbstractQueryPipelineContext {
    final QueryNode n1 = mock(QueryNode.class);
    final QueryNode n2 = mock(QueryNode.class);
    final QueryNode n3 = mock(QueryNode.class);
    final QueryNode n4 = mock(QueryNode.class);
    final QueryNode n5 = mock(QueryNode.class);
    final TimeSeriesDataSource s1 = mock(TimeSeriesDataSource.class);
    final TimeSeriesDataSource s2 = mock(TimeSeriesDataSource.class);
    
    public TestContext(DefaultTSDB tsdb, TimeSeriesQuery query, QueryContext context,
        Collection<QuerySink> sinks) {
      super(tsdb, query, context, sinks);
    }

    @Override
    public void initialize() {
      // sets up the following DAG:
      // n1 --> n3 --> n4 --> s1
      //        ^  |
      // n2 ----|  |-> n5 --> s2
      when(n1.id()).thenReturn("n1");
      when(n2.id()).thenReturn("n2");
      when(n3.id()).thenReturn("n3");
      when(n4.id()).thenReturn("n4");
      when(n5.id()).thenReturn("n5");
      when(s1.id()).thenReturn("s1");
      when(s2.id()).thenReturn("s2");
      
      addVertex(n1);
      addVertex(n2);
      addVertex(n3);
      addVertex(n4);
      addVertex(n5);
      addVertex(s1);
      addVertex(s2);
    
      addDagEdge(n4, s1);
      addDagEdge(n5, s2);
      addDagEdge(n3, n4);
      addDagEdge(n3, n5);
      addDagEdge(n1, n3);
      addDagEdge(n2, n3);
      addDagEdge(this, n1);
      addDagEdge(this, n2);
      
      doThrow(new IllegalStateException("Boo!")).when(n4).close();
      initializeGraph();
    }

    @Override
    public String id() {
      return "TestContext";
    }

  }
}
