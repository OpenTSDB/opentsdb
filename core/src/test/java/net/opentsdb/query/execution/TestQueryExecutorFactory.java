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
package net.opentsdb.query.execution;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Constructor;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.common.reflect.TypeToken;

import io.opentracing.Span;
import net.opentsdb.query.context.QueryContext;
import net.opentsdb.query.execution.graph.ExecutionGraphNode;
import net.opentsdb.query.pojo.TimeSeriesQuery;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ Constructor.class, QueryExecutorFactory.class })
public class TestQueryExecutorFactory {
  
  private ExecutionGraphNode node;
  
  @Before
  public void before() throws Exception {
    node = mock(ExecutionGraphNode.class);
    when(node.getExecutorId()).thenReturn("TextExec");
  }
  
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void ctor() throws Exception {
    Constructor<?> ctor = TestExec.class.getDeclaredConstructor(
        ExecutionGraphNode.class);
    
    QueryExecutorFactory<Long> factory = 
        new DefaultQueryExecutorFactory(ctor, Long.class, "ex1");
    QueryExecutor<Long> executor = factory.newExecutor(node);
    assertSame(node, executor.node);
    assertEquals("ex1", factory.id());
    assertEquals(TypeToken.of(Long.class), factory.type());

    try {
      new DefaultQueryExecutorFactory(null, Long.class, "ex1");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new DefaultQueryExecutorFactory(ctor, Long.class, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new DefaultQueryExecutorFactory(ctor, Long.class, "");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    ctor = TestExec.class.getDeclaredConstructor(ExecutionGraphNode.class, 
        long.class);
    assertNotNull(ctor);
    try {
      new DefaultQueryExecutorFactory(null, Long.class, "ex1");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void newExcecutor() throws Exception {
    Constructor<?> ctor = PowerMockito.spy(TestExec.class.getDeclaredConstructor(
        ExecutionGraphNode.class));
    
    @SuppressWarnings("rawtypes")
    QueryExecutorFactory<Long> factory = 
        new DefaultQueryExecutorFactory(ctor, Long.class, "ex1");
    final QueryExecutor<Long> executor = factory.newExecutor(node);
    assertSame(node, executor.node);
    
    when(ctor.newInstance(node)).thenThrow(new RuntimeException("Boo!"));
    try {
      factory.newExecutor(node);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    ctor = TestExec.class.getDeclaredConstructor(
        ExecutionGraphNode.class);
    
    try {
      factory.newExecutor(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    when(node.getExecutorId()).thenReturn(null);
    try {
      factory.newExecutor(node);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    when(node.getExecutorId()).thenReturn("");
    try {
      factory.newExecutor(node);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  private static class TestExec<T> extends QueryExecutor<T> {
    
    public TestExec(final ExecutionGraphNode node) {
      super(node);
    }
    
    public TestExec(final ExecutionGraphNode node, final long badctor_bad) {
      super(node);
    }

    @Override
    public QueryExecution<T> executeQuery(final QueryContext context, 
        final TimeSeriesQuery query,
        final Span upstream_span) { return null; }
  }
  
}
