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

import java.time.temporal.ChronoUnit;
import java.util.Collection;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import net.opentsdb.core.DefaultTSDB;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.query.AbstractQueryPipelineContext.CumulativeQueryResult;
import net.opentsdb.stats.Span;

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
    ctx.init1();
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
    ctx.init1();
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
  public void downstreamSources() throws Exception {
    final TestContext ctx = new TestContext(tsdb, query, context, 
        Lists.newArrayList(sink1));
    ctx.init1();
    Collection<TimeSeriesDataSource> ds = ctx.downstreamSources(ctx.n1);
    assertEquals(2, ds.size());
    assertTrue(ds.contains(ctx.s1));
    assertTrue(ds.contains(ctx.s2));
    
    ds = ctx.downstreamSources(ctx.n2);
    assertEquals(2, ds.size());
    assertTrue(ds.contains(ctx.s1));
    assertTrue(ds.contains(ctx.s2));
    
    ds = ctx.downstreamSources(ctx.n3);
    assertEquals(2, ds.size());
    assertTrue(ds.contains(ctx.s1));
    assertTrue(ds.contains(ctx.s2));
    
    ds = ctx.downstreamSources(ctx.n4);
    assertEquals(1, ds.size());
    assertTrue(ds.contains(ctx.s1));
    
    ds = ctx.downstreamSources(ctx.n5);
    assertEquals(1, ds.size());
    assertTrue(ds.contains(ctx.s2));

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
    ctx.init1();
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
    ctx.init1();
    assertTrue(ctx.sources.contains(ctx.s1));
    assertTrue(ctx.sources.contains(ctx.s2));
    // reset the sources so it's deterministic.
    ctx.sources.clear();
    ctx.sources.add(ctx.s1);
    ctx.sources.add(ctx.s2);
    
    verify(ctx.s1, never()).fetchNext(null);
    verify(ctx.s2, never()).fetchNext(null);
    
    ctx.fetchNext(null);
    verify(ctx.s1, times(1)).fetchNext(null);
    verify(ctx.s2, never()).fetchNext(null);
    
    ctx.fetchNext(null);
    verify(ctx.s1, times(1)).fetchNext(null);
    verify(ctx.s2, times(1)).fetchNext(null);
    
    ctx.fetchNext(null);
    verify(ctx.s1, times(2)).fetchNext(null);
    verify(ctx.s2, times(1)).fetchNext(null);
    
    doThrow(new IllegalStateException("Boo!")).when(ctx.s2).fetchNext(null);
    ctx.fetchNext(null);
    verify(ctx.s1, times(2)).fetchNext(null);
    verify(ctx.s2, times(2)).fetchNext(null);
    verify(sink1, times(1)).onError(any(Throwable.class));
    
    // Test the single mutli-source case.
    when(context.mode()).thenReturn(QueryMode.SINGLE);
    ctx = new TestContext(tsdb, query, context, Lists.newArrayList(sink1));
    ctx.init1();
    assertTrue(ctx.sources.contains(ctx.s1));
    assertTrue(ctx.sources.contains(ctx.s2));
    // reset the sources so it's deterministic.
    ctx.sources.clear();
    ctx.sources.add(ctx.s1);
    ctx.sources.add(ctx.s2);
    
    verify(ctx.s1, never()).fetchNext(null);
    verify(ctx.s2, never()).fetchNext(null);
    
    ctx.fetchNext(null);
    verify(ctx.s1, times(1)).fetchNext(null);
    verify(ctx.s2, times(1)).fetchNext(null);
  }
  
  @Test
  public void onComplete() throws Exception {
    final TestContext ctx = new TestContext(tsdb, query, context, 
        Lists.newArrayList(sink1, sink2));
    ctx.init1();
    
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
    ctx.init1();
    
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
    ctx.init1();
    
    final TimeSeries t1 = mock(TimeSeries.class);
    final TimeSeries t2 = mock(TimeSeries.class);
    
    assertNull(ctx.single_results);
    result = mock(QueryResult.class);
    when(result.resolution()).thenReturn(ChronoUnit.MILLIS);
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
    when(result.resolution()).thenReturn(ChronoUnit.MILLIS);
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
    ctx.init1();
    
    doThrow(new IllegalStateException("Boo!")).when(sink1).onError(any());
    
    ctx.onError(new IllegalArgumentException("Bad Query!"));
    verify(sink1, times(1)).onError(any());
    verify(sink2, times(1)).onError(any());
  }
  
  @Test
  public void addVertex() throws Exception {
    final TestContext ctx = new TestContext(tsdb, query, context, 
        Lists.newArrayList(sink1, sink2));
    ctx.init1();
    
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
    ctx.init1();
    
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
  public void chainedSources() throws Exception {
    TestContext ctx = new TestContext(tsdb, query, context, 
        Lists.newArrayList(sink1));
    
    ctx.init2();
    assertEquals(1, ctx.sources.size());
    assertSame(ctx.s2, ctx.sources.get(0));
  }
  
  @Test
  public void initializeGraph() throws Exception {
    TestContext ctx = new TestContext(tsdb, query, context, 
        Lists.newArrayList(sink1, sink2));
    // empty is not allowed
    try {
      ctx.initializeGraph(null);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    ctx.init1();
  }
  
  @Test
  public void cumulativeQueryResult_addResults() throws Exception {
    TestContext ctx = new TestContext(tsdb, query, context, 
        Lists.newArrayList(sink1, sink2));
    ctx.init1();
    
    final TimeSeries t1 = mock(TimeSeries.class);
    final TimeSeries t2 = mock(TimeSeries.class);
    final TimeSeries t3 = mock(TimeSeries.class);
    
    QueryResult result = mock(QueryResult.class);
    when(result.resolution()).thenReturn(ChronoUnit.MILLIS);
    when(result.timeSeries()).thenReturn(Lists.newArrayList(t1));
    when(result.source()).thenReturn(ctx.n1);
    
    CumulativeQueryResult cqr = ctx.new CumulativeQueryResult(result);
    assertEquals(1, cqr.timeSeries().size());
    assertSame(t1, cqr.timeSeries().iterator().next());
    assertEquals(ChronoUnit.MILLIS, cqr.resolution());
    assertNull(cqr.timeSpecification());
    assertEquals(0, cqr.sequenceId());
    
    // another result
    result = mock(QueryResult.class);
    when(result.resolution()).thenReturn(ChronoUnit.NANOS);
    when(result.timeSeries()).thenReturn(Lists.newArrayList(t2));
    when(result.source()).thenReturn(ctx.n2);
    when(result.sequenceId()).thenReturn(4L);
    
    cqr.addResults(result);
    assertEquals(2, cqr.timeSeries().size());
    assertTrue(cqr.timeSeries().contains(t1));
    assertTrue(cqr.timeSeries().contains(t2));
    assertEquals(ChronoUnit.NANOS, cqr.resolution());
    assertNull(cqr.timeSpecification());
    assertEquals(4, cqr.sequenceId());
    
    // interspersed result
    result = mock(QueryResult.class);
    when(result.resolution()).thenReturn(ChronoUnit.SECONDS);
    when(result.timeSeries()).thenReturn(Lists.newArrayList(t3));
    when(result.source()).thenReturn(ctx.n2);
    when(result.sequenceId()).thenReturn(1L);
    
    cqr.addResults(result);
    assertEquals(3, cqr.timeSeries().size());
    assertTrue(cqr.timeSeries().contains(t1));
    assertTrue(cqr.timeSeries().contains(t2));
    assertTrue(cqr.timeSeries().contains(t3));
    assertEquals(ChronoUnit.NANOS, cqr.resolution());
    assertNull(cqr.timeSpecification());
    assertEquals(4, cqr.sequenceId());
    
    result = mock(QueryResult.class);
    when(result.timeSpecification()).thenReturn(mock(TimeSpecification.class));
    when(result.resolution()).thenReturn(ChronoUnit.SECONDS);
    when(result.timeSeries()).thenReturn(Lists.newArrayList(t3));
    when(result.source()).thenReturn(ctx.n2);
    when(result.sequenceId()).thenReturn(1L);
    try {
      cqr.addResults(result);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
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
      
      when(n1.id()).thenReturn("n1");
      when(n2.id()).thenReturn("n2");
      when(n3.id()).thenReturn("n3");
      when(n4.id()).thenReturn("n4");
      when(n5.id()).thenReturn("n5");
      when(s1.id()).thenReturn("s1");
      when(s2.id()).thenReturn("s2");
    }

    @Override
    public void initialize(final Span span) {
      initializeGraph(null);
    }
    
    public void init1() {
      // sets up the following DAG:
      // n1 --> n3 --> n4 --> s1
      //        ^  |
      // n2 ----|  |-> n5 --> s2
      
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
      initialize(null);
    }

    public void init2() {
      // sets up the following DAG:
      // n1 --> s2 --> s1
      //        ^
      // n2 ----|
      
      addVertex(s1);
      addVertex(s2);
      addVertex(n1);
      addVertex(n2);
      
      addDagEdge(s2, s1);
      addDagEdge(n1, s2);
      addDagEdge(n2, s2);
      addDagEdge(this, n1);
      addDagEdge(this, n2);
      initialize(null);
    }
    
    @Override
    public String id() {
      return "TestContext";
    }

  }
}
