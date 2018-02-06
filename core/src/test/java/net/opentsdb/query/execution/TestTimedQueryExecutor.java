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
package net.opentsdb.query.execution;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.TimeoutException;

import io.netty.util.TimerTask;
import io.opentracing.Span;
import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.query.execution.TestQueryExecutor.MockDownstream;
import net.opentsdb.query.execution.TimedQueryExecutor.Config;
import net.opentsdb.query.execution.graph.ExecutionGraphNode;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.utils.JSON;

public class TestTimedQueryExecutor extends BaseExecutorTest {
  private QueryExecutor<Long> executor;
  private MockDownstream downstream;
  private Config config;
  
  @SuppressWarnings("unchecked")
  @Before
  public void beforeLocal() throws Exception {
    executor = mock(QueryExecutor.class);
    config = (Config) TimedQueryExecutor.Config.newBuilder()
      .setTimeout(1000)
      .setExecutorId("UT Executor")
      .setExecutorType("TimedQueryExecutor")
      .build();
    query = TimeSeriesQuery.newBuilder()
        .build();
    downstream = new MockDownstream(query);
    
    when(context.getTimer()).thenReturn(timer);
    when(timer.newTimeout(any(TimerTask.class), anyLong(), 
        eq(TimeUnit.MILLISECONDS))).thenReturn(timeout);
    when(executor.executeQuery(eq(context), eq(query), any(Span.class)))
      .thenReturn(downstream);
    when(executor.close()).thenReturn(Deferred.fromResult(null));
    
    node = ExecutionGraphNode.newBuilder()
        .setExecutorId("UTExecutor")
        .setExecutorType("TimedQueryExecutor")
        .setDefaultConfig(config)
        .build();
    node.setExecutionGraph(graph);
    
    when(graph.getDownstreamExecutor(anyString()))
      .thenAnswer(new Answer<QueryExecutor<?>>() {
        @Override
        public QueryExecutor<?> answer(InvocationOnMock invocation)
            throws Throwable {
          return executor;
        }
      });
  }
  
  @Test
  public void ctor() throws Exception {
    TimedQueryExecutor<Long> tqe = new TimedQueryExecutor<Long>(node);
    assertTrue(tqe.outstandingRequests().isEmpty());
    assertEquals(1, tqe.downstreamExecutors().size());
    assertSame(executor, tqe.downstreamExecutors().get(0));

    try {
      node = ExecutionGraphNode.newBuilder()
          .setExecutorId("UTExecutor")
          .setExecutorType("TimedQueryExecutor")
          .build();
      node.setExecutionGraph(graph);
      new TimedQueryExecutor<Long>(node);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      node = ExecutionGraphNode.newBuilder()
          .setExecutorId("UTExecutor")
          .setExecutorType("TimedQueryExecutor")
          .setDefaultConfig(TimedQueryExecutor.Config.newBuilder()
            .setTimeout(0)
            .setExecutorId("UT Executor")
            .setExecutorType("TimedQueryExecutor"))
          .build();
      node.setExecutionGraph(graph);
      new TimedQueryExecutor<Long>(node);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      node = ExecutionGraphNode.newBuilder()
          .setExecutorId("UTExecutor")
          .setExecutorType("TimedQueryExecutor")
          .setDefaultConfig(config)
          .build();
      node.setExecutionGraph(graph);
      when(graph.getDownstreamExecutor(anyString()))
        .thenAnswer(new Answer<QueryExecutor<?>>() {
          @Override
          public QueryExecutor<?> answer(InvocationOnMock invocation)
              throws Throwable {
            return null;
          }
        });
      new TimedQueryExecutor<Long>(node);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
  }
  
  @Test
  public void execute() throws Exception {
    final TimedQueryExecutor<Long> tqe = new TimedQueryExecutor<Long>(node);
    
    final QueryExecution<Long> exec = tqe.executeQuery(context, query, span);
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    
    verify(timer, times(1))
      .newTimeout((TimerTask) exec, 1000, TimeUnit.MILLISECONDS);
    verify(timeout, never()).cancel();
    verify(executor, times(1)).executeQuery(context, query, span);
    assertFalse(downstream.cancelled);
    assertFalse(downstream.completed());
    assertTrue(tqe.outstandingRequests().contains(exec));
    
    downstream.callback(42L);
    assertEquals(42L, (long) exec.deferred().join());
    assertFalse(tqe.outstandingRequests().contains(exec));
    verify(timeout, times(1)).cancel();
    assertTrue(downstream.completed());
    assertFalse(downstream.cancelled);
  }
  
  @Test
  public void executeAlreadyCompleted() throws Exception {
    final TimedQueryExecutor<Long> tqe = new TimedQueryExecutor<Long>(node);
    tqe.completed.set(true);
    
    final QueryExecution<Long> exec = tqe.executeQuery(context, query, span);
    try {
      exec.deferred().join();
      fail("Expected QueryExecutionException");
    } catch (QueryExecutionException e) { }
    verify(timer, never())
      .newTimeout(any(TimerTask.class), eq(1000), eq(TimeUnit.MILLISECONDS));
    verify(timeout, never()).cancel();
    verify(executor, never()).executeQuery(context, query, span);
    assertNotSame(downstream, exec);
    assertFalse(tqe.outstandingRequests().contains(exec));
  }
  
  @Test
  public void executeThrownException() throws Exception {
    final TimedQueryExecutor<Long> tqe = new TimedQueryExecutor<Long>(node);
    when(executor.executeQuery(eq(context), eq(query), any(Span.class)))
      .thenThrow(new IllegalStateException("Boo!"));
    
    final QueryExecution<Long> exec = tqe.executeQuery(context, query, span);
    try {
      exec.deferred().join();
      fail("Expected QueryExecutionException");
    } catch (QueryExecutionException e) { }
    verify(timer, never())
      .newTimeout(any(TimerTask.class), eq(1000), eq(TimeUnit.MILLISECONDS));
    verify(timeout, never()).cancel();
    verify(executor, times(1)).executeQuery(context, query, span);
    assertFalse(tqe.outstandingRequests().contains(exec));
  }
  
  @Test
  public void executeNullTimer() throws Exception {
    final TimedQueryExecutor<Long> tqe = new TimedQueryExecutor<Long>(node);
    when(context.getTimer()).thenReturn(null);
    
    final QueryExecution<Long> exec = tqe.executeQuery(context, query, span);
    try {
      exec.deferred().join();
      fail("Expected QueryExecutionException");
    } catch (QueryExecutionException e) { }
    verify(timer, never())
      .newTimeout(any(TimerTask.class), eq(1000), eq(TimeUnit.MILLISECONDS));
    verify(timeout, never()).cancel();
    verify(executor, times(1)).executeQuery(context, query, span);
    assertFalse(tqe.outstandingRequests().contains(exec));
  }
  
  @Test
  public void executeDownstreamException() throws Exception {
    final TimedQueryExecutor<Long> tqe = new TimedQueryExecutor<Long>(node);
    
    final QueryExecution<Long> exec = tqe.executeQuery(context, query, span);
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    
    verify(timer, times(1))
      .newTimeout((TimerTask) exec, 1000, TimeUnit.MILLISECONDS);
    verify(timeout, never()).cancel();
    verify(executor, times(1)).executeQuery(context, query, span);
    assertFalse(downstream.cancelled);
    assertFalse(downstream.completed());
    assertFalse(exec.completed());
    assertTrue(tqe.outstandingRequests().contains(exec));
    
    downstream.callback(new IllegalStateException("Boo!"));
    try {
      exec.deferred().join();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    assertFalse(tqe.outstandingRequests().contains(exec));
    verify(timeout, times(1)).cancel();
    assertTrue(downstream.completed());
    assertFalse(downstream.cancelled);
    assertTrue(exec.completed());
  }
  
  @Test
  public void executeTimeout() throws Exception {
    final TimedQueryExecutor<Long> tqe = new TimedQueryExecutor<Long>(node);
    
    final QueryExecution<Long> exec = tqe.executeQuery(context, query, span);
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    
    verify(timer, times(1))
      .newTimeout((TimerTask) exec, 1000, TimeUnit.MILLISECONDS);
    verify(timeout, never()).cancel();
    verify(executor, times(1)).executeQuery(context, query, span);
    assertFalse(downstream.cancelled);
    assertFalse(downstream.completed());
    assertTrue(tqe.outstandingRequests().contains(exec));
    
    ((TimerTask) exec).run(null);
    try {
      exec.deferred().join();
      fail("Expected QueryExecutionException");
    } catch (QueryExecutionException e) { }
    assertFalse(tqe.outstandingRequests().contains(exec));
    verify(timeout, never()).cancel();
    assertTrue(downstream.completed());
    assertTrue(downstream.cancelled);
    assertTrue(exec.completed());
    
    // double is ok
    ((TimerTask) exec).run(null);
  }
  
  @Test
  public void executeTimeoutAfterSuccess() throws Exception {
    final TimedQueryExecutor<Long> tqe = new TimedQueryExecutor<Long>(node);
    
    final QueryExecution<Long> exec = tqe.executeQuery(context, query, span);
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    
    verify(timer, times(1))
      .newTimeout((TimerTask) exec, 1000, TimeUnit.MILLISECONDS);
    verify(timeout, never()).cancel();
    verify(executor, times(1)).executeQuery(context, query, span);
    assertFalse(downstream.cancelled);
    assertFalse(downstream.completed());
    assertTrue(tqe.outstandingRequests().contains(exec));
    
    downstream.callback(42L);
    assertEquals(42L, (long) exec.deferred().join());
    assertFalse(tqe.outstandingRequests().contains(exec));
    verify(timeout, times(1)).cancel();
    assertTrue(downstream.completed());
    assertFalse(downstream.cancelled);
    
    ((TimerTask) exec).run(null);
  }
  
  @Test
  public void executeCancel() throws Exception {
    final TimedQueryExecutor<Long> tqe = new TimedQueryExecutor<Long>(node);
    
    final QueryExecution<Long> exec = tqe.executeQuery(context, query, span);
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    
    verify(timer, times(1))
      .newTimeout((TimerTask) exec, 1000, TimeUnit.MILLISECONDS);
    verify(timeout, never()).cancel();
    verify(executor, times(1)).executeQuery(context, query, span);
    assertFalse(downstream.cancelled);
    assertFalse(downstream.completed());
    assertTrue(tqe.outstandingRequests().contains(exec));
    
    exec.cancel();
    try {
      exec.deferred().join();
      fail("Expected QueryExecutionException");
    } catch (QueryExecutionException e) { }
    assertFalse(tqe.outstandingRequests().contains(exec));
    verify(timeout, times(1)).cancel();
    assertTrue(downstream.completed());
    assertTrue(downstream.cancelled);
  }
  
  @Test
  public void close() throws Exception {
    final TimedQueryExecutor<Long> tqe = new TimedQueryExecutor<Long>(node);
    
    final QueryExecution<Long> exec = tqe.executeQuery(context, query, span);
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    assertTrue(tqe.outstandingRequests().contains(exec));
    
    tqe.close().join();
    verify(timeout, times(1)).cancel();
    verify(executor, times(1)).close();
    assertTrue(downstream.completed());
    assertTrue(downstream.cancelled);
  }
  
  @Test
  public void builder() throws Exception {
    String json = JSON.serializeToString(config);
    assertTrue(json.contains("\"executorType\":\"TimedQueryExecutor\""));
    assertTrue(json.contains("\"timeout\":1000"));
    assertTrue(json.contains("\"executorId\":\"UT Executor\""));
    
    json = "{\"executorType\":\"TimedQueryExecutor\",\"timeout\":1000,"
        + "\"executorId\":\"UT Executor\"}";
    config = JSON.parseToObject(json, Config.class);
    assertEquals("TimedQueryExecutor", config.executorType());
    assertEquals("UT Executor", config.getExecutorId());
    assertEquals(1000, config.getTimeout());
  }
  
  @Test
  public void hashCodeEqualsCompareTo() throws Exception {
    final Config c1 = (Config) Config.newBuilder()
        .setTimeout(60000)
        .setExecutorId("MyID")
        .setExecutorType("TimedQueryExecutor")
        .build();
    
    Config c2 = (Config) Config.newBuilder()
        .setTimeout(60000)
        .setExecutorId("MyID")
        .setExecutorType("TimedQueryExecutor")
        .build();
    assertEquals(c1.hashCode(), c2.hashCode());
    assertEquals(c1, c2);
    assertEquals(0, c1.compareTo(c2));
    
    c2 = (Config) Config.newBuilder()
        .setTimeout(30000)  // <-- diff
        .setExecutorId("MyID")
        .setExecutorType("TimedQueryExecutor")
        .build();
    assertNotEquals(c1.hashCode(), c2.hashCode());
    assertNotEquals(c1, c2);
    assertEquals(1, c1.compareTo(c2));
    
    c2 = (Config) Config.newBuilder()
        .setTimeout(60000)
        .setExecutorId("MyID2")  // <-- diff
        .setExecutorType("TimedQueryExecutor")
        .build();
    assertNotEquals(c1.hashCode(), c2.hashCode());
    assertNotEquals(c1, c2);
    assertEquals(-1, c1.compareTo(c2));
    
    c2 = (Config) Config.newBuilder()
        .setTimeout(60000)
        .setExecutorId("MyID")
        .setExecutorType("TimedQueryExecutor2")  // <-- diff
        .build();
    assertNotEquals(c1.hashCode(), c2.hashCode());
    assertNotEquals(c1, c2);
    assertEquals(-1, c1.compareTo(c2));
  }
}
