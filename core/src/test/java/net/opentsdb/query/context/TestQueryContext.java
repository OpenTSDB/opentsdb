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
import static org.junit.Assert.assertFalse;
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

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.collect.Lists;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.DeferredGroupException;

import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.iterators.IteratorStatus;
import net.opentsdb.data.iterators.TimeSeriesIterator;
import net.opentsdb.query.processor.TimeSeriesProcessor;

public class TestQueryContext {

  private int order_counter = 0;
  
  @Test
  public void ctors() throws Exception {
    final MockContext context = new MockContext();
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
  }
  
  @Test
  public void initializeSimpleChain() throws Exception {
    MockProcessor p_1 = new MockProcessor(1, null);
    MockProcessor p_2 = new MockProcessor(2, null);
    MockProcessor p_3 = new MockProcessor(3, null);

    // graph: 1 -> 2 -> 3
    
    MockContext context = new MockContext();
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
    final RuntimeException ex = new RuntimeException("Boo!");
    p_1 = new MockProcessor(1, null);
    p_2 = new MockProcessor(2, null);
    p_3 = new MockProcessor(3, ex);

    order_counter = 0;
    context = new MockContext();
    context.register(p_1, p_2);
    context.register(p_2, p_3);
    
    deferred = context.initialize();
    try {
      deferred.join();
      fail("Expected DeferredGroupException");
    } catch (DeferredGroupException e) {
      assertSame(ex, e.getCause());
    }
    assertEquals(0, p_3.init_order);
    assertEquals(-1, p_3.callback_order);
    assertEquals(-1, p_2.init_order);
    assertEquals(-1, p_2.callback_order);
    assertEquals(-1, p_1.init_order);
    assertEquals(-1, p_1.callback_order);
    
    // exception in middle
    p_1 = new MockProcessor(1, null);
    p_2 = new MockProcessor(2, ex);
    p_3 = new MockProcessor(3, null);

    order_counter = 0;
    context = new MockContext();
    context.register(p_1, p_2);
    context.register(p_2, p_3);
    
    deferred = context.initialize();
    try {
      deferred.join();
      fail("Expected DeferredGroupException");
    } catch (DeferredGroupException e) {
      assertSame(ex, e.getCause());
    }
    assertEquals(0, p_3.init_order);
    assertEquals(-1, p_3.callback_order);
    assertEquals(2, p_2.init_order);
    assertEquals(1, p_2.callback_order);
    assertEquals(-1, p_1.init_order);
    assertEquals(-1, p_1.callback_order);
    
    // exception at sink
    p_1 = new MockProcessor(1, ex);
    p_2 = new MockProcessor(2, null);
    p_3 = new MockProcessor(3, null);

    order_counter = 0;
    context = new MockContext();
    context.register(p_1, p_2);
    context.register(p_2, p_3);
    
    deferred = context.initialize();
    try {
      deferred.join();
      fail("Expected DeferredGroupException");
    } catch (DeferredGroupException e) {
      assertSame(ex, e.getCause());
    }
    assertEquals(0, p_3.init_order);
    assertEquals(-1, p_3.callback_order);
    assertEquals(2, p_2.init_order);
    assertEquals(1, p_2.callback_order);
    assertEquals(4, p_1.init_order);
    assertEquals(3, p_1.callback_order);
    
    // throw exception in init
    p_1 = new MockProcessor(1, null);
    p_2 = new MockProcessor(2, null);
    p_3 = new MockProcessor(3, ex);
    p_3.throw_exception = 1;

    order_counter = 0;
    context = new MockContext();
    context.register(p_1, p_2);
    context.register(p_2, p_3);
    
    deferred = context.initialize();
    try {
      deferred.join();
      fail("Expected RuntimeException");
    } catch (RuntimeException e) {
      assertSame(ex, e);
    }
    assertEquals(0, p_3.init_order);
    assertEquals(-1, p_3.callback_order);
    assertEquals(-1, p_2.init_order);
    assertEquals(-1, p_2.callback_order);
    assertEquals(-1, p_1.init_order);
    assertEquals(-1, p_1.callback_order);
    
    // throw exception in cb
    p_1 = new MockProcessor(1, null);
    p_2 = new MockProcessor(2, ex);
    p_3 = new MockProcessor(3, null);
    p_2.throw_exception = 2;

    order_counter = 0;
    context = new MockContext();
    context.register(p_1, p_2);
    context.register(p_2, p_3);
    
    deferred = context.initialize();
    try {
      deferred.join();
      fail("Expected DeferredGroupException");
    } catch (DeferredGroupException e) {
      assertSame(ex, e.getCause());
    }
    assertEquals(0, p_3.init_order);
    assertEquals(-1, p_3.callback_order);
    assertEquals(-1, p_2.init_order);
    assertEquals(1, p_2.callback_order);
    assertEquals(-1, p_1.init_order);
    assertEquals(-1, p_1.callback_order);
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
    
    MockContext context = new MockContext();
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
    final RuntimeException ex = new RuntimeException("Boo!");
    order_counter = 0;
    p_1 = new MockProcessor(1, null);
    p_2 = new MockProcessor(2, ex);
    p_3 = new MockProcessor(3, null);
    p_4 = new MockProcessor(4, null);
    p_5 = new MockProcessor(5, null);
    
    context = new MockContext();
    context.register(p_1, p_2);
    context.register(p_2, p_3);
    context.register(p_4, p_5);
    
    deferred = context.initialize();
    try {
      deferred.join();
      fail("Expected DeferredGroupException");
    } catch (DeferredGroupException e) {
      assertSame(ex, e.getCause());
    }
    assertTrue(p_3.init_order >= 0);
    assertEquals(-1, p_3.callback_order);
    assertTrue(p_2.init_order > p_3.init_order);
    assertEquals(p_2.init_order - 1, p_2.callback_order);
    assertEquals(-1, p_1.init_order);
    assertEquals(-1, p_1.callback_order);
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
    
    MockContext context = new MockContext();
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
    final RuntimeException ex = new RuntimeException("Boo!");
    order_counter = 0;
    p_1 = new MockProcessor(1, null);
    p_2 = new MockProcessor(2, null);
    p_3 = new MockProcessor(3, ex);
    p_4 = new MockProcessor(4, null);

    context = new MockContext();
    context.register(p_1, p_2);
    context.register(p_2, p_3);
    context.register(p_2, p_4);
    
    deferred = context.initialize();
    try {
      deferred.join();
      fail("Expected DeferredGroupException");
    } catch (DeferredGroupException e) {
      assertSame(ex, e.getCause());
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
    
    MockContext context = new MockContext();
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
    final RuntimeException ex = new RuntimeException("Boo!");
    order_counter = 0;
    p_1 = new MockProcessor(1, null);
    p_2 = new MockProcessor(2, null);
    p_3 = new MockProcessor(3, null);
    p_4 = new MockProcessor(4, ex);
    p_5 = new MockProcessor(5, null);

    context = new MockContext();
    context.register(p_1, p_2);
    context.register(p_2, p_3);
    context.register(p_2, p_4);
    context.register(p_5, p_2);
    
    deferred = context.initialize();
    try {
      deferred.join();
      fail("Expected DeferredGroupException");
    } catch (DeferredGroupException e) {
      assertSame(ex, e.getCause());
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
    
    MockContext context = new MockContext();
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
    final RuntimeException ex = new RuntimeException("Boo!");
    order_counter = 0;
    p_1 = new MockProcessor(1, null);
    p_2 = new MockProcessor(2, null);
    p_3 = new MockProcessor(3, null);
    p_4 = new MockProcessor(4, ex);
    p_5 = new MockProcessor(5, null);

    context = new MockContext();
    context.register(p_1, p_2);
    context.register(p_2, p_3);
    context.register(p_2, p_4);
    context.register(p_5, p_2);
    
    deferred = context.initialize();
    try {
      deferred.join();
      fail("Expected DeferredGroupException");
    } catch (DeferredGroupException e) {
      assertSame(ex, e.getCause());
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
  }
  
  @Test
  public void updateContext() throws Exception {
    final MockContext context = new MockContext();
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
    
    context.updateContext(IteratorStatus.EXCEPTION, ts);
    assertEquals(Long.MAX_VALUE, context.syncTimestamp().msEpoch());
    assertEquals(3000, context.nextTimestamp().msEpoch());
    assertEquals(IteratorStatus.END_OF_DATA, context.currentStatus());
    assertEquals(IteratorStatus.EXCEPTION, context.nextStatus());
    
    try {
      context.updateContext(null, ts);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      context.updateContext(IteratorStatus.END_OF_CHUNK, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void advance() throws Exception {
    final MockContext context = new MockContext();
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
    
    final MockContext context = new MockContext();
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
    
    final MockContext context = new MockContext();
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
    
    MockContext context = new MockContext();
    MockContext split_context = new MockContext();
    
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
    
    assertFalse(split_context.context_graph.containsVertex(context));
    assertTrue(split_context.context_graph.containsVertex(split_context));
    assertFalse(split_context.context_graph.containsEdge(context, split_context));
    
    // reset and test bottom
    context = new MockContext();
    split_context = new MockContext();
    
    context.register(p_4, p_3);
    context.register(p_3, p_2);
    context.register(p_2, p_1);
    
    context.splitContext(split_context, p_1);
    assertEquals(1, split_context.processor_graph.vertexSet().size());
    assertTrue(split_context.processor_graph.containsVertex(p_1));
    assertEquals(1, split_context.processor_sinks.size());
    assertTrue(split_context.processor_sinks.contains(p_1));
    
    // reset and test top
    context = new MockContext();
    split_context = new MockContext();
    
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
    
    final MockContext context = new MockContext();
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
    
    final RuntimeException ex = new RuntimeException("Boo!");
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
      fail("Expected RuntimeException");
    } catch (RuntimeException e) { 
      assertSame(ex, e);
    }
  }
  
  /**
   * Mock that lets us peek into protected variables.
   */
  class MockContext extends QueryContext {
    
    public MockContext() {
      super();
    }
    
    public MockContext(final QueryContext context) {
      super(context);
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
    final RuntimeException e;
    
    public MockProcessor(final int id, final RuntimeException e) throws Exception {
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
        public Deferred<Object> call(Object ignored) throws Exception {
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
