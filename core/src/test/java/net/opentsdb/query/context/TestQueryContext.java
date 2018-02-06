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
package net.opentsdb.query.context;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.collect.Lists;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.DeferredGroupException;

import io.netty.util.Timer;
import io.opentracing.Tracer;
import net.opentsdb.core.DefaultTSDB;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.iterators.IteratorStatus;
import net.opentsdb.data.iterators.TimeSeriesIterator;
import net.opentsdb.query.execution.QueryExecutor;
import net.opentsdb.query.execution.QueryExecutorConfig;
import net.opentsdb.query.execution.graph.ExecutionGraph;
import net.opentsdb.query.processor.TimeSeriesProcessor;

public class TestQueryContext {
  private DefaultTSDB tsdb;
  private ExecutionGraph executor_graph;
  private int order_counter = 0;
  
  @Before
  public void before() throws Exception {
    tsdb = mock(DefaultTSDB.class);
    executor_graph = mock(ExecutionGraph.class);
  }
  
  @Test
  public void ctors() throws Exception {
    final MockContext context = new MockContext(tsdb, executor_graph);
    assertTrue(context.context_graph.containsVertex(context));
    assertNotNull(context.iterator_graph);
    assertNotNull(context.processor_graph);
    assertEquals(Long.MAX_VALUE, context.syncTimestamp().msEpoch());
    assertEquals(Long.MAX_VALUE, context.nextTimestamp().msEpoch());
    assertEquals(IteratorStatus.END_OF_DATA, context.currentStatus());
    assertEquals(IteratorStatus.END_OF_DATA, context.nextStatus());
    assertNull(context.getParent());
    assertNull(context.children);
    assertTrue(context.iterator_sinks.isEmpty());
    assertTrue(context.processor_sinks.isEmpty());
    assertNull(context.getTracer());
    
    MockContext context2 = new MockContext(context);
    assertTrue(context2.context_graph.containsVertex(context));
    assertTrue(context2.context_graph.containsVertex(context2));
    assertTrue(context2.context_graph.containsEdge(context, context2));
    assertSame(context2.context_graph, context.context_graph);
    assertNotNull(context2.iterator_graph);
    assertNotNull(context2.processor_graph);
    assertEquals(Long.MAX_VALUE, context2.syncTimestamp().msEpoch());
    assertEquals(Long.MAX_VALUE, context2.nextTimestamp().msEpoch());
    assertEquals(IteratorStatus.END_OF_DATA, context2.currentStatus());
    assertEquals(IteratorStatus.END_OF_DATA, context2.nextStatus());
    assertSame(context, context2.getParent());
    assertNull(context2.children);
    assertTrue(context2.iterator_sinks.isEmpty());
    assertTrue(context2.processor_sinks.isEmpty());
    assertNull(context2.getTracer());
    
    final Tracer tracer = mock(Tracer.class);
    context2 = new MockContext(tsdb, executor_graph, tracer);
    assertTrue(context2.context_graph.containsVertex(context2));
    assertNotNull(context2.iterator_graph);
    assertNotNull(context2.processor_graph);
    assertEquals(Long.MAX_VALUE, context2.syncTimestamp().msEpoch());
    assertEquals(Long.MAX_VALUE, context2.nextTimestamp().msEpoch());
    assertEquals(IteratorStatus.END_OF_DATA, context2.currentStatus());
    assertEquals(IteratorStatus.END_OF_DATA, context2.nextStatus());
    assertNull(context2.getParent());
    assertNull(context2.children);
    assertTrue(context2.iterator_sinks.isEmpty());
    assertTrue(context2.processor_sinks.isEmpty());
    assertSame(tracer, context2.getTracer());
    
    try {
      new MockContext(null, executor_graph);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new MockContext(tsdb, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new MockContext((QueryContext) null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void initializeSimpleChain() throws Exception {
    MockProcessor p_1 = new MockProcessor(1, null);
    MockProcessor p_2 = new MockProcessor(2, null);
    MockProcessor p_3 = new MockProcessor(3, null);

    // graph: 1 -> 2 -> 3
    
    MockContext context = new MockContext(tsdb, executor_graph);
    context.register(p_1, p_2);
    context.register(p_2, p_3);
    
    Deferred<Object> deferred = context.initialize();
    assertNull(deferred.join());
    assertEquals(0, p_3.init_order);
    assertEquals(-1, p_3.callback_order);
    assertEquals(2, p_2.init_order);
    assertEquals(1, p_2.callback_order);
    assertEquals(4, p_1.init_order);
    assertEquals(3, p_1.callback_order);

    // exception at start
    final IllegalStateException ex = new IllegalStateException("Boo!");
    p_1 = new MockProcessor(1, null);
    p_2 = new MockProcessor(2, null);
    p_3 = new MockProcessor(3, ex);

    order_counter = 0;
    context = new MockContext(tsdb, executor_graph);
    context.register(p_1, p_2);
    context.register(p_2, p_3);
    
    deferred = context.initialize();
    try {
      deferred.join();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertSame(ex, e);
    }
    assertEquals(0, p_3.init_order);
    assertEquals(-1, p_3.callback_order);
    assertEquals(-1, p_2.init_order);
    assertEquals(-1, p_2.callback_order);
    assertEquals(-1, p_1.init_order);
    assertEquals(-1, p_1.callback_order);
    assertEquals(IteratorStatus.EXCEPTION, context.currentStatus());
    assertEquals(IteratorStatus.EXCEPTION, context.nextStatus());
    
    // exception in middle
    p_1 = new MockProcessor(1, null);
    p_2 = new MockProcessor(2, ex);
    p_3 = new MockProcessor(3, null);

    order_counter = 0;
    context = new MockContext(tsdb, executor_graph);
    context.register(p_1, p_2);
    context.register(p_2, p_3);
    
    deferred = context.initialize();
    try {
      deferred.join();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertSame(ex, e);
    }
    assertEquals(0, p_3.init_order);
    assertEquals(-1, p_3.callback_order);
    assertEquals(2, p_2.init_order);
    assertEquals(1, p_2.callback_order);
    assertEquals(-1, p_1.init_order);
    assertEquals(-1, p_1.callback_order);
    assertEquals(IteratorStatus.EXCEPTION, context.currentStatus());
    assertEquals(IteratorStatus.EXCEPTION, context.nextStatus());
    
    // exception at sink
    p_1 = new MockProcessor(1, ex);
    p_2 = new MockProcessor(2, null);
    p_3 = new MockProcessor(3, null);

    order_counter = 0;
    context = new MockContext(tsdb, executor_graph);
    context.register(p_1, p_2);
    context.register(p_2, p_3);
    
    deferred = context.initialize();
    try {
      deferred.join();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertSame(ex, e);
    }
    assertEquals(0, p_3.init_order);
    assertEquals(-1, p_3.callback_order);
    assertEquals(2, p_2.init_order);
    assertEquals(1, p_2.callback_order);
    assertEquals(4, p_1.init_order);
    assertEquals(3, p_1.callback_order);
    assertEquals(IteratorStatus.EXCEPTION, context.currentStatus());
    assertEquals(IteratorStatus.EXCEPTION, context.nextStatus());
    
    // exception at sink
    p_1 = new MockProcessor(1, ex);
    p_2 = new MockProcessor(2, null);
    p_3 = new MockProcessor(3, null);
    p_1.throw_exception = 1;

    order_counter = 0;
    context = new MockContext(tsdb, executor_graph);
    context.register(p_1, p_2);
    context.register(p_2, p_3);
    
    deferred = context.initialize();
    try {
      deferred.join();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertSame(ex, e);
    }
    assertEquals(0, p_3.init_order);
    assertEquals(-1, p_3.callback_order);
    assertEquals(2, p_2.init_order);
    assertEquals(1, p_2.callback_order);
    assertEquals(4, p_1.init_order);
    assertEquals(3, p_1.callback_order);
    assertEquals(IteratorStatus.EXCEPTION, context.currentStatus());
    assertEquals(IteratorStatus.EXCEPTION, context.nextStatus());
    
    // exception at sink
    p_1 = new MockProcessor(1, ex);
    p_2 = new MockProcessor(2, null);
    p_3 = new MockProcessor(3, null);
    p_1.throw_exception = 2;

    order_counter = 0;
    context = new MockContext(tsdb, executor_graph);
    context.register(p_1, p_2);
    context.register(p_2, p_3);
    
    deferred = context.initialize();
    try {
      deferred.join();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertSame(ex, e);
    }
    assertEquals(0, p_3.init_order);
    assertEquals(-1, p_3.callback_order);
    assertEquals(2, p_2.init_order);
    assertEquals(1, p_2.callback_order);
    assertEquals(-1, p_1.init_order);
    assertEquals(3, p_1.callback_order);
    assertEquals(IteratorStatus.EXCEPTION, context.currentStatus());
    assertEquals(IteratorStatus.EXCEPTION, context.nextStatus());
    
    // throw exception in init
    p_1 = new MockProcessor(1, null);
    p_2 = new MockProcessor(2, null);
    p_3 = new MockProcessor(3, ex);
    p_3.throw_exception = 1;

    order_counter = 0;
    context = new MockContext(tsdb, executor_graph);
    context.register(p_1, p_2);
    context.register(p_2, p_3);
    
    deferred = context.initialize();
    try {
      deferred.join();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertSame(ex, e);
    }
    assertEquals(0, p_3.init_order);
    assertEquals(-1, p_3.callback_order);
    assertEquals(-1, p_2.init_order);
    assertEquals(-1, p_2.callback_order);
    assertEquals(-1, p_1.init_order);
    assertEquals(-1, p_1.callback_order);
    assertEquals(IteratorStatus.EXCEPTION, context.currentStatus());
    assertEquals(IteratorStatus.EXCEPTION, context.nextStatus());
    
    // throw exception in cb
    p_1 = new MockProcessor(1, null);
    p_2 = new MockProcessor(2, ex);
    p_3 = new MockProcessor(3, null);
    p_2.throw_exception = 2;

    order_counter = 0;
    context = new MockContext(tsdb, executor_graph);
    context.register(p_1, p_2);
    context.register(p_2, p_3);
    
    deferred = context.initialize();
    try {
      deferred.join();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertSame(ex, e);
    }
    assertEquals(0, p_3.init_order);
    assertEquals(-1, p_3.callback_order);
    assertEquals(-1, p_2.init_order);
    assertEquals(1, p_2.callback_order);
    assertEquals(-1, p_1.init_order);
    assertEquals(-1, p_1.callback_order);
    assertEquals(IteratorStatus.EXCEPTION, context.currentStatus());
    assertEquals(IteratorStatus.EXCEPTION, context.nextStatus());
  }
  
  @Test
  public void initializeParallelChains() throws Exception {
    MockProcessor p_1 = new MockProcessor(1, null);
    MockProcessor p_2 = new MockProcessor(2, null);
    MockProcessor p_3 = new MockProcessor(3, null);
    MockProcessor p_4 = new MockProcessor(4, null);
    MockProcessor p_5 = new MockProcessor(5, null);
    
    // graph: 1 -> 2 -> 3
    //        4 -> 5
    
    MockContext context = new MockContext(tsdb, executor_graph);
    context.register(p_1, p_2);
    context.register(p_2, p_3);
    context.register(p_4, p_5);
    
    Deferred<Object> deferred = context.initialize();
    assertNull(deferred.join());
    assertTrue(p_3.init_order >= 0);
    assertEquals(-1, p_3.callback_order);
    assertTrue(p_2.init_order > p_3.init_order);
    assertEquals(p_2.init_order - 1, p_2.callback_order);
    assertTrue(p_1.init_order > p_2.init_order);
    assertEquals(p_1.init_order - 1, p_1.callback_order);
    assertTrue(p_5.init_order >= 0);
    assertEquals(-1, p_5.callback_order);
    assertTrue(p_4.init_order > p_5.init_order);
    assertEquals(p_4.init_order - 1, p_4.callback_order);
    
    // exception in first graph
    final IllegalStateException ex = new IllegalStateException("Boo!");
    order_counter = 0;
    p_1 = new MockProcessor(1, null);
    p_2 = new MockProcessor(2, ex);
    p_3 = new MockProcessor(3, null);
    p_4 = new MockProcessor(4, null);
    p_5 = new MockProcessor(5, null);
    
    context = new MockContext(tsdb, executor_graph);
    context.register(p_1, p_2);
    context.register(p_2, p_3);
    context.register(p_4, p_5);
    
    deferred = context.initialize();
    try {
      deferred.join();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertSame(ex, e);
    }
    assertTrue(p_3.init_order >= 0);
    assertEquals(-1, p_3.callback_order);
    assertTrue(p_2.init_order > p_3.init_order);
    assertEquals(p_2.init_order - 1, p_2.callback_order);
    assertEquals(-1, p_1.init_order);
    assertEquals(-1, p_1.callback_order);
    assertEquals(IteratorStatus.EXCEPTION, context.currentStatus());
    assertEquals(IteratorStatus.EXCEPTION, context.nextStatus());
    // separate graph still inits.
    assertTrue(p_5.init_order >= 0);
    assertEquals(-1, p_5.callback_order);
    assertTrue(p_4.init_order > p_5.init_order);
    assertEquals(p_4.init_order - 1, p_4.callback_order);
  }
  
  @Test
  public void initializeDualSources() throws Exception {
    MockProcessor p_1 = new MockProcessor(1, null);
    MockProcessor p_2 = new MockProcessor(2, null);
    MockProcessor p_3 = new MockProcessor(3, null);
    MockProcessor p_4 = new MockProcessor(4, null);
    
    // graph: 1 -> 2 -> 3
    //             |--> 4
    
    MockContext context = new MockContext(tsdb, executor_graph);
    context.register(p_1, p_2);
    context.register(p_2, p_3);
    context.register(p_2, p_4);
    
    Deferred<Object> deferred = context.initialize();
    assertNull(deferred.join());
    // order is indeterminate.
    assertTrue(p_4.init_order >= 0);
    assertEquals(-1, p_4.callback_order);
    assertTrue(p_3.init_order >= 0);
    assertEquals(-1, p_3.callback_order);
    assertTrue(p_2.init_order > p_3.init_order);
    assertEquals(p_2.init_order - 1, p_2.callback_order);
    assertTrue(p_1.init_order > p_2.init_order);
    assertEquals(p_1.init_order - 1, p_1.callback_order);

    // kaboom!
    final IllegalStateException ex = new IllegalStateException("Boo!");
    order_counter = 0;
    p_1 = new MockProcessor(1, null);
    p_2 = new MockProcessor(2, null);
    p_3 = new MockProcessor(3, ex);
    p_4 = new MockProcessor(4, null);

    context = new MockContext(tsdb, executor_graph);
    context.register(p_1, p_2);
    context.register(p_2, p_3);
    context.register(p_2, p_4);
    
    deferred = context.initialize();
    try {
      deferred.join();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertSame(ex, e);
    }
    // order is indeterminate.
    assertTrue(p_4.init_order >= 0);
    assertEquals(-1, p_4.callback_order);
    assertTrue(p_3.init_order >= 0);
    assertEquals(-1, p_3.callback_order);
    assertEquals(-1, p_2.init_order);
    assertEquals(-1, p_2.callback_order);
    assertEquals(-1, p_1.init_order);
    assertEquals(-1, p_1.callback_order);
    assertEquals(IteratorStatus.EXCEPTION, context.currentStatus());
    assertEquals(IteratorStatus.EXCEPTION, context.nextStatus());
  }
  
  @Test
  public void initializeDualSourcesDualSink() throws Exception {
    MockProcessor p_1 = new MockProcessor(1, null);
    MockProcessor p_2 = new MockProcessor(2, null);
    MockProcessor p_3 = new MockProcessor(3, null);
    MockProcessor p_4 = new MockProcessor(4, null);
    MockProcessor p_5 = new MockProcessor(5, null);
    
    // graph: 1 -> 2 -> 3
    //        5 ->^|--> 4
    
    MockContext context = new MockContext(tsdb, executor_graph);
    context.register(p_1, p_2);
    context.register(p_2, p_3);
    context.register(p_2, p_4);
    context.register(p_5, p_2);
    
    Deferred<Object> deferred = context.initialize();
    assertNull(deferred.join());
    // order is indeterminate.
    assertTrue(p_4.init_order >= 0);
    assertEquals(-1, p_4.callback_order);
    assertTrue(p_3.init_order >= 0);
    assertEquals(-1, p_3.callback_order);
    assertTrue(p_2.init_order > p_3.init_order);
    assertEquals(p_2.init_order - 1, p_2.callback_order);
    assertTrue(p_1.init_order > p_2.init_order);
    assertEquals(p_1.init_order - 1, p_1.callback_order);
    assertTrue(p_5.init_order > p_2.init_order);
    assertEquals(p_5.init_order - 1, p_5.callback_order);
    
    // kaboom!
    final IllegalStateException ex = new IllegalStateException("Boo!");
    order_counter = 0;
    p_1 = new MockProcessor(1, null);
    p_2 = new MockProcessor(2, null);
    p_3 = new MockProcessor(3, null);
    p_4 = new MockProcessor(4, ex);
    p_5 = new MockProcessor(5, null);

    context = new MockContext(tsdb, executor_graph);
    context.register(p_1, p_2);
    context.register(p_2, p_3);
    context.register(p_2, p_4);
    context.register(p_5, p_2);
    
    deferred = context.initialize();
    try {
      deferred.join();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertSame(ex, e);
    }
    // order is indeterminate.
    assertTrue(p_4.init_order >= 0);
    assertEquals(-1, p_4.callback_order);
    assertTrue(p_3.init_order >= 0);
    assertEquals(-1, p_3.callback_order);
    assertEquals(-1, p_2.init_order);
    assertEquals(-1, p_2.callback_order);
    assertEquals(-1, p_1.init_order);
    assertEquals(-1, p_1.callback_order);
    assertEquals(-1, p_5.init_order);
    assertEquals(-1, p_5.callback_order);
    assertEquals(IteratorStatus.EXCEPTION, context.currentStatus());
    assertEquals(IteratorStatus.EXCEPTION, context.nextStatus());
  }
  
  @Test
  public void initializeDualSourcesSplit() throws Exception {
    MockProcessor p_1 = new MockProcessor(1, null);
    MockProcessor p_2 = new MockProcessor(2, null);
    MockProcessor p_3 = new MockProcessor(3, null);
    MockProcessor p_4 = new MockProcessor(4, null);
    MockProcessor p_5 = new MockProcessor(5, null);
    
    // graph: 1 -> 2 -> 3
    //        5    |--> 4
    //        |--------^
    
    MockContext context = new MockContext(tsdb, executor_graph);
    context.register(p_1, p_2);
    context.register(p_2, p_3);
    context.register(p_2, p_4);
    context.register(p_5, p_4);
    
    Deferred<Object> deferred = context.initialize();
    assertNull(deferred.join());
    // order is indeterminate.
    assertTrue(p_4.init_order == 0 || p_4.init_order == 1);
    assertEquals(-1, p_4.callback_order);
    assertTrue(p_3.init_order == 0 || p_3.init_order == 1 || p_3.init_order == 3);
    assertEquals(-1, p_3.callback_order);
    assertTrue(p_2.init_order > p_3.init_order);
    assertEquals(p_2.init_order - 1, p_2.callback_order);
    assertTrue(p_1.init_order > p_2.init_order);
    assertEquals(p_1.init_order - 1, p_1.callback_order);
    assertTrue(p_5.init_order > p_4.init_order);
    assertEquals(p_5.init_order - 1, p_5.callback_order);
    
    // kaboom!
    final IllegalStateException ex = new IllegalStateException("Boo!");
    order_counter = 0;
    p_1 = new MockProcessor(1, null);
    p_2 = new MockProcessor(2, null);
    p_3 = new MockProcessor(3, null);
    p_4 = new MockProcessor(4, ex);
    p_5 = new MockProcessor(5, null);

    context = new MockContext(tsdb, executor_graph);
    context.register(p_1, p_2);
    context.register(p_2, p_3);
    context.register(p_2, p_4);
    context.register(p_5, p_2);
    
    deferred = context.initialize();
    try {
      deferred.join();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertSame(ex, e);
    }
    // order is indeterminate.
    assertTrue(p_4.init_order == 0 || p_4.init_order == 1);
    assertEquals(-1, p_4.callback_order);
    assertTrue(p_3.init_order == 0 || p_3.init_order == 1);
    assertEquals(-1, p_3.callback_order);
    assertEquals(-1, p_2.init_order);
    assertEquals(-1, p_2.callback_order);
    assertEquals(-1, p_1.init_order);
    assertEquals(-1, p_1.callback_order);
    assertEquals(-1, p_5.init_order);
    assertEquals(-1, p_5.callback_order);
    assertEquals(IteratorStatus.EXCEPTION, context.currentStatus());
    assertEquals(IteratorStatus.EXCEPTION, context.nextStatus());
  }
  
  @Test
  public void updateContext() throws Exception {
    final MockContext context = new MockContext(tsdb, executor_graph);
    assertEquals(Long.MAX_VALUE, context.syncTimestamp().msEpoch());
    assertEquals(Long.MAX_VALUE, context.nextTimestamp().msEpoch());
    assertEquals(IteratorStatus.END_OF_DATA, context.currentStatus());
    assertEquals(IteratorStatus.END_OF_DATA, context.nextStatus());
    
    final TimeStamp ts = new MillisecondTimeStamp(4000);
    context.updateContext(IteratorStatus.HAS_DATA, ts);
    assertEquals(Long.MAX_VALUE, context.syncTimestamp().msEpoch());
    assertEquals(4000, context.nextTimestamp().msEpoch());
    assertEquals(IteratorStatus.END_OF_DATA, context.currentStatus());
    assertEquals(IteratorStatus.HAS_DATA, context.nextStatus());
    
    // higher ts is ignored
    ts.updateMsEpoch(5000);
    context.updateContext(IteratorStatus.HAS_DATA, ts);
    assertEquals(Long.MAX_VALUE, context.syncTimestamp().msEpoch());
    assertEquals(4000, context.nextTimestamp().msEpoch());
    assertEquals(IteratorStatus.END_OF_DATA, context.currentStatus());
    assertEquals(IteratorStatus.HAS_DATA, context.nextStatus());
    
    ts.updateMsEpoch(3000);
    context.updateContext(IteratorStatus.HAS_DATA, ts);
    assertEquals(Long.MAX_VALUE, context.syncTimestamp().msEpoch());
    assertEquals(3000, context.nextTimestamp().msEpoch());
    assertEquals(IteratorStatus.END_OF_DATA, context.currentStatus());
    assertEquals(IteratorStatus.HAS_DATA, context.nextStatus());
    
    // same ts
    context.updateContext(IteratorStatus.HAS_DATA, ts);
    assertEquals(Long.MAX_VALUE, context.syncTimestamp().msEpoch());
    assertEquals(3000, context.nextTimestamp().msEpoch());
    assertEquals(IteratorStatus.END_OF_DATA, context.currentStatus());
    assertEquals(IteratorStatus.HAS_DATA, context.nextStatus());
    
    // null TS
    context.updateContext(IteratorStatus.END_OF_CHUNK, null);
    assertEquals(Long.MAX_VALUE, context.syncTimestamp().msEpoch());
    assertEquals(3000, context.nextTimestamp().msEpoch());
    assertEquals(IteratorStatus.END_OF_DATA, context.currentStatus());
    assertEquals(IteratorStatus.HAS_DATA, context.nextStatus());
    
    // exception
    context.updateContext(IteratorStatus.EXCEPTION, ts);
    assertEquals(Long.MAX_VALUE, context.syncTimestamp().msEpoch());
    assertEquals(3000, context.nextTimestamp().msEpoch());
    assertEquals(IteratorStatus.END_OF_DATA, context.currentStatus());
    assertEquals(IteratorStatus.EXCEPTION, context.nextStatus());
    
    try {
      context.updateContext(null, ts);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void advance() throws Exception {
    final MockContext context = new MockContext(tsdb, executor_graph);
    assertEquals(Long.MAX_VALUE, context.syncTimestamp().msEpoch());
    assertEquals(Long.MAX_VALUE, context.nextTimestamp().msEpoch());
    assertEquals(IteratorStatus.END_OF_DATA, context.currentStatus());
    assertEquals(IteratorStatus.END_OF_DATA, context.nextStatus());
    
    // no-op
    assertEquals(IteratorStatus.END_OF_DATA, context.advance());
    assertEquals(Long.MAX_VALUE, context.syncTimestamp().msEpoch());
    assertEquals(Long.MAX_VALUE, context.nextTimestamp().msEpoch());
    assertEquals(IteratorStatus.END_OF_DATA, context.currentStatus());
    assertEquals(IteratorStatus.END_OF_DATA, context.nextStatus());
    
    // set a timestamp
    final TimeStamp ts = new MillisecondTimeStamp(4000);
    context.updateContext(IteratorStatus.HAS_DATA, ts);
    assertEquals(Long.MAX_VALUE, context.syncTimestamp().msEpoch());
    assertEquals(4000, context.nextTimestamp().msEpoch());
    assertEquals(IteratorStatus.END_OF_DATA, context.currentStatus());
    assertEquals(IteratorStatus.HAS_DATA, context.nextStatus());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    assertEquals(4000, context.syncTimestamp().msEpoch());
    assertEquals(Long.MAX_VALUE, context.nextTimestamp().msEpoch());
    assertEquals(IteratorStatus.HAS_DATA, context.currentStatus());
    assertEquals(IteratorStatus.END_OF_DATA, context.nextStatus());
  }

  @Test
  public void registerIterator() throws Exception {
    final TimeSeriesIterator<?> it_1 = mock(TimeSeriesIterator.class);
    final TimeSeriesIterator<?> it_2 = mock(TimeSeriesIterator.class);
    final TimeSeriesIterator<?> it_3 = mock(TimeSeriesIterator.class);
    
    final MockContext context = new MockContext(tsdb, executor_graph);
    assertTrue(context.iterator_sinks.isEmpty());
    assertTrue(context.iterator_graph.vertexSet().isEmpty());
    
    context.register(it_1);
    assertTrue(context.iterator_graph.containsVertex(it_1));
    assertEquals(1, context.iterator_graph.vertexSet().size());
    assertEquals(1, context.iterator_sinks.size());
    assertTrue(context.iterator_sinks.contains(it_1));
    
    context.register(it_1, null);
    assertTrue(context.iterator_graph.containsVertex(it_1));
    assertEquals(1, context.iterator_graph.vertexSet().size());
    assertEquals(1, context.iterator_sinks.size());
    assertTrue(context.iterator_sinks.contains(it_1));
    
    context.register(it_1, it_2);
    assertTrue(context.iterator_graph.containsVertex(it_1));
    assertTrue(context.iterator_graph.containsVertex(it_2));
    assertEquals(2, context.iterator_graph.vertexSet().size());
    assertTrue(context.iterator_graph.containsEdge(it_1, it_2));
    // sink stays the same
    assertEquals(1, context.iterator_sinks.size());
    assertTrue(context.iterator_sinks.contains(it_1));
    
    // bump 1 out of the sink set
    context.register(it_3, it_1);
    assertTrue(context.iterator_graph.containsVertex(it_1));
    assertTrue(context.iterator_graph.containsVertex(it_2));
    assertTrue(context.iterator_graph.containsVertex(it_3));
    assertEquals(3, context.iterator_graph.vertexSet().size());
    assertTrue(context.iterator_graph.containsEdge(it_1, it_2));
    assertTrue(context.iterator_graph.containsEdge(it_3, it_1));
    // sink stays the same
    assertEquals(1, context.iterator_sinks.size());
    assertTrue(context.iterator_sinks.contains(it_3));
    
    // cycle
    try {
      context.register(it_1, it_3);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    
    try {
      context.register((TimeSeriesIterator<?>) null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      context.register((TimeSeriesIterator<?>) null, it_2);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void registerProcessor() throws Exception {
    final TimeSeriesProcessor p_1 = mock(TimeSeriesProcessor.class);
    final TimeSeriesProcessor p_2 = mock(TimeSeriesProcessor.class);
    final TimeSeriesProcessor p_3 = mock(TimeSeriesProcessor.class);
    
    final MockContext context = new MockContext(tsdb, executor_graph);
    assertTrue(context.processor_sinks.isEmpty());
    assertTrue(context.processor_graph.vertexSet().isEmpty());
    
    context.register(p_1);
    assertTrue(context.processor_graph.containsVertex(p_1));
    assertEquals(1, context.processor_graph.vertexSet().size());
    assertEquals(1, context.processor_sinks.size());
    assertTrue(context.processor_sinks.contains(p_1));
    
    context.register(p_1, null);
    assertTrue(context.processor_graph.containsVertex(p_1));
    assertEquals(1, context.processor_graph.vertexSet().size());
    assertEquals(1, context.processor_sinks.size());
    assertTrue(context.processor_sinks.contains(p_1));
    
    context.register(p_1, p_2);
    assertTrue(context.processor_graph.containsVertex(p_1));
    assertTrue(context.processor_graph.containsVertex(p_2));
    assertEquals(2, context.processor_graph.vertexSet().size());
    assertTrue(context.processor_graph.containsEdge(p_1, p_2));
    // sink stays the same
    assertEquals(1, context.processor_sinks.size());
    assertTrue(context.processor_sinks.contains(p_1));
    
    // bump 1 out of the sink set
    context.register(p_3, p_1);
    assertTrue(context.processor_graph.containsVertex(p_1));
    assertTrue(context.processor_graph.containsVertex(p_2));
    assertTrue(context.processor_graph.containsVertex(p_3));
    assertEquals(3, context.processor_graph.vertexSet().size());
    assertTrue(context.processor_graph.containsEdge(p_1, p_2));
    assertTrue(context.processor_graph.containsEdge(p_3, p_1));
    // sink stays the same
    assertEquals(1, context.processor_sinks.size());
    assertTrue(context.processor_sinks.contains(p_3));
    
    // cycle
    try {
      context.register(p_1, p_3);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    
    try {
      context.register((TimeSeriesProcessor) null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      context.register((TimeSeriesProcessor) null, p_2);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void splitContext() throws Exception {
    final TimeSeriesProcessor p_1 = mock(TimeSeriesProcessor.class);
    final TimeSeriesProcessor p_2 = mock(TimeSeriesProcessor.class);
    final TimeSeriesProcessor p_3 = mock(TimeSeriesProcessor.class);
    final TimeSeriesProcessor p_4 = mock(TimeSeriesProcessor.class);
    
    MockContext context = new MockContext(tsdb, executor_graph);
    MockContext split_context = new MockContext(context);
    
    context.register(p_4, p_3);
    context.register(p_3, p_2);
    context.register(p_2, p_1);
    
    assertTrue(context.processor_graph.containsVertex(p_1));
    assertTrue(context.processor_graph.containsVertex(p_2));
    assertTrue(context.processor_graph.containsVertex(p_3));
    assertTrue(context.processor_graph.containsVertex(p_4));
    assertEquals(4, context.processor_graph.vertexSet().size());
    assertTrue(context.processor_graph.containsEdge(p_4, p_3));
    assertTrue(context.processor_graph.containsEdge(p_3, p_2));
    assertTrue(context.processor_graph.containsEdge(p_2, p_1));
    assertEquals(1, context.processor_sinks.size());
    assertTrue(context.processor_sinks.contains(p_4));
    assertTrue(split_context.processor_graph.vertexSet().isEmpty());
    
    context.splitContext(split_context, p_3);
    assertTrue(context.processor_graph.containsVertex(p_1));
    assertTrue(context.processor_graph.containsVertex(p_2));
    assertTrue(context.processor_graph.containsVertex(p_3));
    assertTrue(context.processor_graph.containsVertex(p_4));
    assertEquals(4, context.processor_graph.vertexSet().size());
    assertTrue(context.processor_graph.containsEdge(p_4, p_3));
    assertTrue(context.processor_graph.containsEdge(p_3, p_2));
    assertTrue(context.processor_graph.containsEdge(p_2, p_1));
    assertEquals(1, context.processor_sinks.size());
    assertTrue(context.processor_sinks.contains(p_4));
    
    assertTrue(context.context_graph.containsVertex(context));
    assertTrue(context.context_graph.containsVertex(split_context));
    assertTrue(context.context_graph.containsEdge(context, split_context));
    
    assertEquals(3, split_context.processor_graph.vertexSet().size());
    assertTrue(split_context.processor_graph.containsVertex(p_1));
    assertTrue(split_context.processor_graph.containsVertex(p_2));
    assertTrue(split_context.processor_graph.containsVertex(p_3));
    assertTrue(split_context.processor_graph.containsEdge(p_3, p_2));
    assertTrue(split_context.processor_graph.containsEdge(p_2, p_1));
    assertEquals(1, split_context.processor_sinks.size());
    assertTrue(split_context.processor_sinks.contains(p_3));
    
    assertTrue(split_context.context_graph.containsVertex(context));
    assertTrue(split_context.context_graph.containsVertex(split_context));
    assertTrue(split_context.context_graph.containsEdge(context, split_context));
    
    try {
      split_context.splitContext(context, p_2);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    
    // reset and test bottom
    context = new MockContext(tsdb, executor_graph);
    split_context = new MockContext(tsdb, executor_graph);
    
    context.register(p_4, p_3);
    context.register(p_3, p_2);
    context.register(p_2, p_1);
    
    context.splitContext(split_context, p_1);
    assertEquals(1, split_context.processor_graph.vertexSet().size());
    assertTrue(split_context.processor_graph.containsVertex(p_1));
    assertEquals(1, split_context.processor_sinks.size());
    assertTrue(split_context.processor_sinks.contains(p_1));
    
    // reset and test top
    context = new MockContext(tsdb, executor_graph);
    split_context = new MockContext(tsdb, executor_graph);
    
    context.register(p_4, p_3);
    context.register(p_3, p_2);
    context.register(p_2, p_1);
    
    context.splitContext(split_context, p_4);
    assertEquals(4, split_context.processor_graph.vertexSet().size());
    assertTrue(split_context.processor_graph.containsVertex(p_1));
    assertTrue(split_context.processor_graph.containsVertex(p_2));
    assertTrue(split_context.processor_graph.containsVertex(p_3));
    assertTrue(split_context.processor_graph.containsVertex(p_4));
    assertTrue(split_context.processor_graph.containsEdge(p_4, p_3));
    assertTrue(split_context.processor_graph.containsEdge(p_3, p_2));
    assertTrue(split_context.processor_graph.containsEdge(p_2, p_1));
    assertEquals(1, split_context.processor_sinks.size());
    assertTrue(split_context.processor_sinks.contains(p_4));
    
    
    
    try {
      context.splitContext(split_context, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      context.splitContext(split_context, mock(TimeSeriesProcessor.class));
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void fetchNext() throws Exception {
    final TimeSeriesIterator<?> it_1 = mock(TimeSeriesIterator.class);
    final TimeSeriesIterator<?> it_2 = mock(TimeSeriesIterator.class);
    final TimeSeriesIterator<?> it_3 = mock(TimeSeriesIterator.class);
    when(it_1.fetchNext()).thenAnswer(new Answer<Deferred<Object>>() {
      @Override
      public Deferred<Object> answer(final InvocationOnMock invocation)
          throws Throwable {
        return Deferred.fromResult(null);
      }
    });
    when(it_2.fetchNext()).thenAnswer(new Answer<Deferred<Object>>() {
      @Override
      public Deferred<Object> answer(final InvocationOnMock invocation)
          throws Throwable {
        return Deferred.fromResult(null);
      }
    });
    when(it_3.fetchNext()).thenAnswer(new Answer<Deferred<Object>>() {
      @Override
      public Deferred<Object> answer(final InvocationOnMock invocation)
          throws Throwable {
        return Deferred.fromResult(null);
      }
    });
    
    final MockContext context = new MockContext(tsdb, executor_graph);
    context.next_status = IteratorStatus.END_OF_CHUNK;
    context.register(it_1, it_2);
    context.register(it_3);
    
    Deferred<Object> deferred = context.fetchNext();
    assertNull(deferred.join());
    verify(it_1, times(1)).fetchNext();
    verify(it_2, never()).fetchNext(); // not a sink
    verify(it_3, times(1)).fetchNext();
    assertEquals(IteratorStatus.END_OF_DATA, context.next_status);
    
    // make sure children are called
    final QueryContext mock_context = mock(QueryContext.class);
    when(mock_context.fetchNext()).thenAnswer(new Answer<Deferred<Object>>() {
      @Override
      public Deferred<Object> answer(final InvocationOnMock invocation)
          throws Throwable {
        return Deferred.fromResult(null);
      }
    });
    when(mock_context.nextStatus()).thenReturn(IteratorStatus.END_OF_CHUNK);
    
    context.children = Lists.newArrayListWithExpectedSize(1);
    context.children.add(mock_context);
    
    deferred = context.fetchNext();
    assertNull(deferred.join());
    verify(it_1, times(2)).fetchNext();
    verify(it_2, never()).fetchNext(); // not a sink
    verify(it_3, times(2)).fetchNext();
    verify(mock_context, times(1)).fetchNext();
    assertEquals(IteratorStatus.END_OF_CHUNK, context.nextStatus());
    
    final IllegalStateException ex = new IllegalStateException("Boo!");
    when(it_3.fetchNext()).thenAnswer(new Answer<Deferred<Object>>() {
      @Override
      public Deferred<Object> answer(final InvocationOnMock invocation)
          throws Throwable {
        return Deferred.fromError(ex);
      }
    });
    deferred = context.fetchNext();
    try {
      deferred.join();
      fail("Expected DeferredGroupException");
    } catch (DeferredGroupException e) { 
      assertSame(ex, e.getCause());
    }
    
    when(mock_context.fetchNext()).thenThrow(ex);
    deferred = context.fetchNext();
    try {
      deferred.join();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { 
      assertSame(ex, e);
    }
  }
  
  @Test
  public void close() throws Exception {
    final TimeSeriesIterator<?> it_1 = mock(TimeSeriesIterator.class);
    final TimeSeriesIterator<?> it_2 = mock(TimeSeriesIterator.class);
    final TimeSeriesIterator<?> it_3 = mock(TimeSeriesIterator.class);
    when(it_1.close()).thenAnswer(new Answer<Deferred<Object>>() {
      @Override
      public Deferred<Object> answer(final InvocationOnMock invocation)
          throws Throwable {
        return Deferred.fromResult(null);
      }
    });
    when(it_2.close()).thenAnswer(new Answer<Deferred<Object>>() {
      @Override
      public Deferred<Object> answer(final InvocationOnMock invocation)
          throws Throwable {
        return Deferred.fromResult(null);
      }
    });
    when(it_3.close()).thenAnswer(new Answer<Deferred<Object>>() {
      @Override
      public Deferred<Object> answer(final InvocationOnMock invocation)
          throws Throwable {
        return Deferred.fromResult(null);
      }
    });
    
    final MockContext context = new MockContext(tsdb, executor_graph);
    context.next_status = IteratorStatus.END_OF_CHUNK;
    context.register(it_1, it_2);
    context.register(it_3);
    final QueryExecutor<?> executor = mock(QueryExecutor.class);
    context.sink_executors.add(executor);
    when(executor.close()).thenReturn(Deferred.fromResult(null));
    
    Deferred<Object> deferred = context.close();
    assertNull(deferred.join());
    verify(it_1, times(1)).close();
    verify(it_2, never()).close(); // not a sink
    verify(it_3, times(1)).close();
    verify(executor, times(1)).close();
    assertEquals(IteratorStatus.END_OF_DATA, context.next_status);
    
    // make sure children are called
    final QueryContext mock_context = mock(QueryContext.class);
    when(mock_context.close()).thenAnswer(new Answer<Deferred<Object>>() {
      @Override
      public Deferred<Object> answer(final InvocationOnMock invocation)
          throws Throwable {
        return Deferred.fromResult(null);
      }
    });
    when(mock_context.nextStatus()).thenReturn(IteratorStatus.END_OF_CHUNK);
    
    context.children = Lists.newArrayListWithExpectedSize(1);
    context.children.add(mock_context);
    
    deferred = context.close();
    assertNull(deferred.join());
    verify(it_1, times(2)).close();
    verify(it_2, never()).close(); // not a sink
    verify(it_3, times(2)).close();
    verify(mock_context, times(1)).close();
    assertEquals(IteratorStatus.END_OF_DATA, context.nextStatus());
    
    final IllegalStateException ex = new IllegalStateException("Boo!");
    when(it_3.close()).thenAnswer(new Answer<Deferred<Object>>() {
      @Override
      public Deferred<Object> answer(final InvocationOnMock invocation)
          throws Throwable {
        return Deferred.fromError(ex);
      }
    });
    deferred = context.close();
    try {
      deferred.join();
      fail("Expected DeferredGroupException");
    } catch (DeferredGroupException e) { 
      assertSame(ex, e.getCause());
    }
    
    when(mock_context.close()).thenThrow(ex);
    deferred = context.close();
    try {
      deferred.join();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { 
      assertSame(ex, e);
    }
  }

  @Test
  public void configOverride() throws Exception {
    final QueryExecutorConfig config_a = mock(QueryExecutorConfig.class);
    final QueryExecutorConfig config_b = mock(QueryExecutorConfig.class);
    when(config_a.getExecutorId()).thenReturn("config_a");
    when(config_b.getExecutorId()).thenReturn("config_b");
    
    final MockContext context = new MockContext(tsdb, executor_graph);
    context.addConfigOverride(config_a);
    assertSame(config_a, context.getConfigOverride("config_a"));
    context.addConfigOverride(config_b);
    assertSame(config_b, context.getConfigOverride("config_b"));
    
    final QueryExecutorConfig config_c = mock(QueryExecutorConfig.class);
    when(config_c.getExecutorId()).thenReturn("config_a");
    context.addConfigOverride(config_c);
    assertSame(config_c, context.getConfigOverride("config_a"));
    
    try {
      context.addConfigOverride(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    when(config_a.getExecutorId()).thenReturn(null);
    try {
      context.addConfigOverride(config_a);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    when(config_a.getExecutorId()).thenReturn("");
    try {
      context.addConfigOverride(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      context.getConfigOverride(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      context.getConfigOverride("");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void sessionObjects() throws Exception {
    final Object obj_a = new Object();
    final Object obj_b = new Object();
   
    final MockContext context = new MockContext(tsdb, executor_graph);
    assertNull(context.getSessionObject("obj_a"));
    assertNull(context.getSessionObject("obj_b"));

    context.addSessionObject("obj_a", obj_a);
    assertSame(obj_a, context.getSessionObject("obj_a"));
    context.addSessionObject("obj_b", obj_b);
    assertSame(obj_b, context.getSessionObject("obj_b"));
    
    final Object obj_c = new Object();
    context.addSessionObject("obj_a", obj_c);
    assertSame(obj_c, context.getSessionObject("obj_a"));
    
    context.addSessionObject("obj_c", null);
    assertNull(context.getSessionObject("obj_c"));
    
    try {
      context.addSessionObject(null, obj_a);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      context.addSessionObject("", obj_a);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      context.getSessionObject(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      context.getSessionObject("");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  /**
   * Mock that lets us peek into protected variables.
   */
  class MockContext extends QueryContext {
    
    public MockContext(final DefaultTSDB tsdb, 
                       final ExecutionGraph executor_context) {
      super(tsdb, executor_context, null);
    }
    
    public MockContext(final DefaultTSDB tsdb, 
                       final ExecutionGraph executor_context, 
                       final Tracer tracer) {
      super(tsdb, executor_context, tracer);
    }
    
    public MockContext(final QueryContext context) {
      super(context);
    }

    @Override
    public Timer getTimer() {
      // TODO Auto-generated method stub
      return null;
    }
    
  }
  
  /**
   * Mock that overloads the init and callback methods so that we can inject
   * or throw exceptions AND track the initialization order for proper testing.
   */
  class MockProcessor extends TimeSeriesProcessor {
    int throw_exception; // 0 = nope, 1 = in init, 2 = in callback
    int callback_order = -1;
    int init_order = -1;
    final int id;
    final IllegalStateException e;
    
    public MockProcessor(final int id, final IllegalStateException e) throws Exception {
      this.id = id;
      this.e = e;
    }
    
    @Override
    public Deferred<Object> initialize() {
      init_order = order_counter++;
      if (throw_exception == 1) {
        throw e;
      }
      if (e != null) {
        init_deferred.callback(e);
      } else {
        init_deferred.callback(null);
      }
      return init_deferred;
    }

    @Override
    public Callback<Deferred<Object>, Object> initializationCallback() {
      class InitCB implements Callback<Deferred<Object>, Object> {
        @Override
        public Deferred<Object> call(final Object result_or_exception) 
            throws Exception {
          if (result_or_exception instanceof Exception) {
            init_deferred.callback((Exception) result_or_exception);
            return init_deferred;
          }
          callback_order = order_counter++;
          if (throw_exception == 2) {
            throw e;
          }
          return initialize();
        }
      }
      return new InitCB();
    }
    
    @Override
    public String toString() {
      return "[ID] " + id + " CO: " + callback_order + "  IO: " + init_order;
    }

    @Override
    public TimeSeriesProcessor getClone(QueryContext context) {
      return null;
    }
  }
  
}
