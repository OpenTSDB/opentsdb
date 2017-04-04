// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.query.context;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.jgrapht.experimental.dag.DirectedAcyclicGraph.CycleFoundException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import net.opentsdb.query.execution.QueryExecutor;
import net.opentsdb.query.execution.QueryExecutorFactory;

public class TestQueryExecutorContext {

  private QueryContext query_context;
  
  @Before
  public void before() throws Exception {
    query_context = mock(QueryContext.class);
  }
  
  @Test
  public void ctor() throws Exception {
    final QueryExecutorContext context = new QueryExecutorContext("Default");
    assertTrue(context.executor_graph.vertexSet().isEmpty());
    assertNull(context.sink_factory);
    assertEquals("Default", context.id());
    
    try {
      new QueryExecutorContext(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new QueryExecutorContext("");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void registerFactory() throws Exception {
    final QueryExecutorFactory<?> factory = mock(QueryExecutorFactory.class);
    final QueryExecutorFactory<?> downstream_a = mock(QueryExecutorFactory.class);
    final QueryExecutorFactory<?> downstream_b = mock(QueryExecutorFactory.class);
    
    QueryExecutorContext context = new QueryExecutorContext("Default");
    context.registerFactory(factory);
    assertTrue(context.executor_graph.containsVertex(factory));
    assertSame(factory, context.sink_factory);

    context.registerFactory(factory, downstream_a);
    assertTrue(context.executor_graph.containsVertex(factory));
    assertTrue(context.executor_graph.containsVertex(downstream_a));
    assertTrue(context.executor_graph.containsEdge(factory, downstream_a));
    assertSame(factory, context.sink_factory);
    
    context.registerFactory(downstream_a, downstream_b);
    assertTrue(context.executor_graph.containsVertex(factory));
    assertTrue(context.executor_graph.containsVertex(downstream_a));
    assertTrue(context.executor_graph.containsVertex(downstream_b));
    assertTrue(context.executor_graph.containsEdge(factory, downstream_a));
    assertTrue(context.executor_graph.containsEdge(downstream_a, downstream_b));
    assertSame(factory, context.sink_factory);
    
    // reverse order
    context = new QueryExecutorContext("Default");
    context.registerFactory(downstream_b);
    assertTrue(context.executor_graph.containsVertex(downstream_b));
    assertSame(downstream_b, context.sink_factory);
    
    context.registerFactory(downstream_a, downstream_b);
    assertTrue(context.executor_graph.containsVertex(downstream_a));
    assertTrue(context.executor_graph.containsVertex(downstream_b));
    assertTrue(context.executor_graph.containsEdge(downstream_a, downstream_b));
    assertSame(downstream_a, context.sink_factory);
    
    context.registerFactory(factory, downstream_a);
    assertTrue(context.executor_graph.containsVertex(factory));
    assertTrue(context.executor_graph.containsVertex(downstream_a));
    assertTrue(context.executor_graph.containsVertex(downstream_b));
    assertTrue(context.executor_graph.containsEdge(factory, downstream_a));
    assertTrue(context.executor_graph.containsEdge(downstream_a, downstream_b));
    assertSame(factory, context.sink_factory);
    
    // register all then link fails since we can't have two sinks.
    context = new QueryExecutorContext("Default");
    context.registerFactory(factory);
    try {
      context.registerFactory(downstream_a);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // cycle
    context.registerFactory(factory, downstream_a);
    context.registerFactory(downstream_a, downstream_b);
    try {
      context.registerFactory(downstream_b, factory);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertTrue(e.getCause() instanceof CycleFoundException);
    }
    
    try {
      context.registerFactory(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void newSinkExecutor() throws Exception {
    final QueryExecutor<?> executor = mock(QueryExecutor.class);
    final QueryExecutorFactory<?> factory = mock(QueryExecutorFactory.class);
    when(factory.newExecutor(query_context)).thenAnswer(
        new Answer<QueryExecutor<?>>() {
      @Override
      public QueryExecutor<?> answer(InvocationOnMock invocation)
          throws Throwable {
        return executor;
      }
    });
    final QueryExecutorFactory<?> downstream_a = mock(QueryExecutorFactory.class);
    QueryExecutorContext context = new QueryExecutorContext("Default");
    context.registerFactory(factory, downstream_a);
    
    assertSame(executor, context.newSinkExecutor(query_context));
    verify(factory, times(1)).newExecutor(query_context);
    verify(downstream_a, never()).newExecutor(query_context);
    
    // bad factory
    when(factory.newExecutor(query_context)).thenReturn(null);
    try {
      context.newSinkExecutor(query_context);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    
    // exception in factory
    final Exception ex = new IllegalArgumentException("Boo!");
    when(factory.newExecutor(query_context)).thenThrow(ex);
    try {
      context.newSinkExecutor(query_context);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { 
      assertSame(e, ex);
    }
    
    // empty context graph
    context = new QueryExecutorContext("Default");
    try {
      context.newSinkExecutor(query_context);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
  }
  
  @Test
  public void newDownstreamExecutor() throws Exception {
    final QueryExecutor<?> executor = mock(QueryExecutor.class);
    final QueryExecutorFactory<?> factory = mock(QueryExecutorFactory.class);
    final QueryExecutorFactory<?> downstream_a = mock(QueryExecutorFactory.class);
    when(downstream_a.newExecutor(query_context)).thenAnswer(
        new Answer<QueryExecutor<?>>() {
      @Override
      public QueryExecutor<?> answer(InvocationOnMock invocation)
          throws Throwable {
        return executor;
      }
    });
    QueryExecutorContext context = new QueryExecutorContext("Default");
    context.registerFactory(factory, downstream_a);
    assertSame(executor, context.newDownstreamExecutor(query_context, factory));
    verify(factory, never()).newExecutor(query_context);
    verify(downstream_a, times(1)).newExecutor(query_context);
    
    // no downstream
    try {
      context.newDownstreamExecutor(query_context, downstream_a);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    
    // no such factory
    try {
      context.newDownstreamExecutor(query_context, 
          mock(QueryExecutorFactory.class));
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    
    // bad factory
    when(downstream_a.newExecutor(query_context)).thenReturn(null);
    try {
      context.newDownstreamExecutor(query_context, factory);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    
    // exception in factory
    final Exception ex = new IllegalArgumentException("Boo!");
    when(downstream_a.newExecutor(query_context)).thenThrow(ex);
    try {
      context.newDownstreamExecutor(query_context, factory);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { 
      assertSame(e, ex);
    }
    
    try {
      context.newDownstreamExecutor(query_context, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
}
