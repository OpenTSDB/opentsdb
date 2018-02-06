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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.collect.Lists;
import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.TimeoutException;
import io.opentracing.Span;
import net.opentsdb.data.DataMerger;
import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.query.execution.MetricShardingExecutor.Config;
import net.opentsdb.query.execution.TestQueryExecutor.MockDownstream;
import net.opentsdb.query.execution.graph.ExecutionGraphNode;
import net.opentsdb.query.pojo.Metric;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.utils.JSON;

public class TestMetricShardingExecutor extends BaseExecutorTest {
  private QueryExecutor<Long> executor;
  private List<MockDownstream> downstreams;
  private DataMerger<Long> merger;
  private Config config;
  private TimeSeriesQuery q1;
  private TimeSeriesQuery q2;
  private TimeSeriesQuery q3;
  private TimeSeriesQuery q4;
  
  @SuppressWarnings("unchecked")
  @Before
  public void beforeLocal() throws Exception {
    node = mock(ExecutionGraphNode.class);
    executor = mock(QueryExecutor.class);
    downstreams = Lists.newArrayList();
    merger = mock(DataMerger.class);
    config = (Config) Config.newBuilder()
        .setParallelExecutors(2)
        .setExecutorId("UT Executor")
        .setExecutorType("MetricShardingExecutor")
        .build();

    when(node.graph()).thenReturn(graph);
    when(node.getDefaultConfig()).thenReturn(config);
    when(registry.getDataMerger(anyString()))
      .thenAnswer(new Answer<DataMerger<?>>() {
      @Override
      public DataMerger<?> answer(InvocationOnMock invocation)
          throws Throwable {
        return merger;
      }
    });
    when(graph.getDownstreamExecutor(anyString()))
      .thenAnswer(new Answer<QueryExecutor<?>>() {
      @Override
      public QueryExecutor<?> answer(InvocationOnMock invocation)
          throws Throwable {
        return executor;
      }
    });
    when(executor.close()).thenReturn(Deferred.fromResult(null));
    when(merger.merge((List<Long>) any(List.class), eq(context), any(Span.class)))
      .thenAnswer(new Answer<Long>() {
      @Override
      public Long answer(InvocationOnMock invocation) throws Throwable {
        return 42L;
      }
    });
    when(executor.executeQuery(eq(context), any(TimeSeriesQuery.class), 
        any(Span.class))).thenAnswer(new Answer<QueryExecution<?>>() {
          @Override
          public QueryExecution<?> answer(InvocationOnMock invocation)
              throws Throwable {
            final MockDownstream downstream = new MockDownstream(
                (TimeSeriesQuery) invocation.getArguments()[1]);
            downstreams.add(downstream);
            return downstream;
          }
    });
    
    q1 = TimeSeriesQuery.newBuilder()
        .addMetric(Metric.newBuilder().setMetric("sys.cpu.user").setId("m1"))
        .build();
    q2 = TimeSeriesQuery.newBuilder()
        .addMetric(Metric.newBuilder().setMetric("sys.cpu.idle").setId("m2"))
        .build();
    q3 = TimeSeriesQuery.newBuilder()
        .addMetric(Metric.newBuilder().setMetric("sys.cpu.iowait").setId("m3"))
        .build();
    q4 = TimeSeriesQuery.newBuilder()
        .addMetric(Metric.newBuilder().setMetric("sys.cpu.softirq").setId("m4"))
        .build();
    
    query = TimeSeriesQuery.newBuilder()
        .addMetric(Metric.newBuilder().setMetric("sys.cpu.user").setId("m1"))
        .addMetric(Metric.newBuilder().setMetric("sys.cpu.idle").setId("m2"))
        .addMetric(Metric.newBuilder().setMetric("sys.cpu.iowait").setId("m3"))
        .addMetric(Metric.newBuilder().setMetric("sys.cpu.softirq").setId("m4"))
        .build();
  }
  
  @Test
  public void ctor() throws Exception {
    MetricShardingExecutor<Long> executor = 
        new MetricShardingExecutor<Long>(node);
    assertSame(merger, executor.dataMerger());
    assertTrue(executor.outstandingRequests().isEmpty());
    assertEquals(1, executor.downstreamExecutors().size());
    assertSame(this.executor, executor.downstreamExecutors().get(0));

    try {
      new MetricShardingExecutor<Long>(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    when(node.getDefaultConfig()).thenReturn(null);
    try {
      new MetricShardingExecutor<Long>(node);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    config = (Config) MetricShardingExecutor.Config.newBuilder()
        .setParallelExecutors(0)
        .setExecutorId("UT Executor")
        .setExecutorType("MetricShardingExecutor")
        .build();
    when(node.getDefaultConfig()).thenReturn(config);
    try {
      new MetricShardingExecutor<Long>(node);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    config = (Config) MetricShardingExecutor.Config.newBuilder()
        .setParallelExecutors(2)
        .setExecutorId("UT Executor")
        .setExecutorType("MetricShardingExecutor")
        .build();
    when(node.getDefaultConfig()).thenReturn(config);
    when(graph.getDownstreamExecutor(anyString()))
    .thenAnswer(new Answer<QueryExecutor<?>>() {
    @Override
    public QueryExecutor<?> answer(InvocationOnMock invocation)
        throws Throwable {
      return null;
    }
  });
    try {
      new MetricShardingExecutor<Long>(node);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void execute() throws Exception {
    final MetricShardingExecutor<Long> executor = 
        new MetricShardingExecutor<Long>(node);
    
    final QueryExecution<Long> exec = executor.executeQuery(context, query, span);
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    
    verify(this.executor, times(1)).executeQuery(context, q1, null);
    verify(this.executor, times(1)).executeQuery(context, q2, null);
    verify(this.executor, never()).executeQuery(context, q3, null);
    verify(this.executor, never()).executeQuery(context, q3, null);
    verify(this.executor, never()).executeQuery(context, q4, null);
    verify(this.executor, never()).executeQuery(context, q4, null);
    assertTrue(executor.outstandingRequests().contains(exec));
    assertFalse(exec.completed());
    assertEquals(2, downstreams.size());
    for (final MockDownstream execution : downstreams) {
      assertFalse(execution.cancelled);
      assertFalse(execution.completed());
    }

    // callback 1st, next should start
    downstreams.get(0).callback(1L);
    verify(this.executor, times(1)).executeQuery(context, q3, null);
    verify(this.executor, never()).executeQuery(context, q4, null);
    verify(this.executor, never()).executeQuery(context, q4, null);
    assertTrue(executor.outstandingRequests().contains(exec));
    assertFalse(exec.completed());
    assertEquals(3, downstreams.size());
    
    // callback 2nd, last should start
    downstreams.get(1).callback(2L);
    verify(this.executor, times(1)).executeQuery(context, q4, null);
    assertTrue(executor.outstandingRequests().contains(exec));
    assertFalse(exec.completed());
    assertEquals(4, downstreams.size());
    
    // call the rest back
    downstreams.get(2).callback(3L);
    downstreams.get(3).callback(4L);
    
    assertEquals(42L, (long) exec.deferred().join(1));
    assertFalse(executor.outstandingRequests().contains(exec));
    assertTrue(exec.completed());
    for (final MockDownstream execution : downstreams) {
      assertFalse(execution.cancelled);
      assertTrue(execution.completed());
    }
  }
  
  @Test
  public void executeOverrideParallels() throws Exception {
    final Config override = (Config) Config.newBuilder()
        .setParallelExecutors(3)
        .setExecutorId("UT Executor")
        .setExecutorType("MetricShardingExecutor")
        .build();
    when(context.getConfigOverride(anyString())).thenReturn(override);
    final MetricShardingExecutor<Long> executor = 
        new MetricShardingExecutor<Long>(node);
    
    final QueryExecution<Long> exec = executor.executeQuery(context, query, span);
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    
    verify(this.executor, times(1)).executeQuery(context, q1, null);
    verify(this.executor, times(1)).executeQuery(context, q2, null);
    verify(this.executor, times(1)).executeQuery(context, q3, null);
    verify(this.executor, never()).executeQuery(context, q4, null);
    assertTrue(executor.outstandingRequests().contains(exec));
    assertFalse(exec.completed());
    assertEquals(3, downstreams.size());
    for (final MockDownstream execution : downstreams) {
      assertFalse(execution.cancelled);
      assertFalse(execution.completed());
    }

    // callback 1st, next should start
    downstreams.get(0).callback(1L);
    verify(this.executor, times(1)).executeQuery(context, q3, null);
    verify(this.executor, times(1)).executeQuery(context, q4, null);
    assertTrue(executor.outstandingRequests().contains(exec));
    assertFalse(exec.completed());
    assertEquals(4, downstreams.size());
    
    // call the rest back
    downstreams.get(1).callback(2L);
    downstreams.get(2).callback(3L);
    downstreams.get(3).callback(4L);
    
    assertEquals(42L, (long) exec.deferred().join(1));
    assertFalse(executor.outstandingRequests().contains(exec));
    assertTrue(exec.completed());
    for (final MockDownstream execution : downstreams) {
      assertFalse(execution.cancelled);
      assertTrue(execution.completed());
    }
  }
  
  @Test (expected = IllegalStateException.class)
  public void executNoSubQueries() throws Exception {
    final MetricShardingExecutor<Long> executor = 
        new MetricShardingExecutor<Long>(node);
    executor.executeQuery(context, TimeSeriesQuery.newBuilder().build(), null);
  }
  
  @Test
  public void executeEarlyFail() throws Exception {
    final MetricShardingExecutor<Long> executor = 
        new MetricShardingExecutor<Long>(node);
    
    final QueryExecution<Long> exec = executor.executeQuery(context, query, span);
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    
    verify(this.executor, times(1)).executeQuery(context, q1, null);
    verify(this.executor, times(1)).executeQuery(context, q2, null);
    verify(this.executor, never()).executeQuery(context, q3, null);
    verify(this.executor, never()).executeQuery(context, q3, null);
    verify(this.executor, never()).executeQuery(context, q4, null);
    verify(this.executor, never()).executeQuery(context, q4, null);
    assertTrue(executor.outstandingRequests().contains(exec));
    assertFalse(exec.completed());
    assertEquals(2, downstreams.size());
    for (final MockDownstream execution : downstreams) {
      assertFalse(execution.cancelled);
      assertFalse(execution.completed());
    }

    // callback 1st, all should stop
    final IllegalStateException ex = new IllegalStateException("Boo!");
    downstreams.get(0).callback(ex);
    try {
      exec.deferred().join();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    verify(this.executor, never()).executeQuery(context, q3, null);
    verify(this.executor, never()).executeQuery(context, q4, null);
    verify(this.executor, never()).executeQuery(context, q4, null);
    assertFalse(executor.outstandingRequests().contains(exec));
    assertTrue(exec.completed());
    assertEquals(2, downstreams.size());
    
    assertTrue(downstreams.get(1).cancelled);
  }
  
  @Test
  public void executeLateFail() throws Exception {
    final MetricShardingExecutor<Long> executor = 
        new MetricShardingExecutor<Long>(node);
    
    final QueryExecution<Long> exec = executor.executeQuery(context, query, span);
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    
    verify(this.executor, times(1)).executeQuery(context, q1, null);
    verify(this.executor, times(1)).executeQuery(context, q2, null);
    verify(this.executor, never()).executeQuery(context, q3, null);
    verify(this.executor, never()).executeQuery(context, q3, null);
    verify(this.executor, never()).executeQuery(context, q4, null);
    verify(this.executor, never()).executeQuery(context, q4, null);
    assertTrue(executor.outstandingRequests().contains(exec));
    assertFalse(exec.completed());
    assertEquals(2, downstreams.size());
    for (final MockDownstream execution : downstreams) {
      assertFalse(execution.cancelled);
      assertFalse(execution.completed());
    }

    // callback 1st, next should start
    downstreams.get(0).callback(1L);
    verify(this.executor, times(1)).executeQuery(context, q3, null);
    verify(this.executor, never()).executeQuery(context, q4, null);
    verify(this.executor, never()).executeQuery(context, q4, null);
    assertTrue(executor.outstandingRequests().contains(exec));
    assertFalse(exec.completed());
    assertEquals(3, downstreams.size());
    
    // callback 2nd, last should start
    downstreams.get(1).callback(2L);
    verify(this.executor, times(1)).executeQuery(context, q4, null);
    assertTrue(executor.outstandingRequests().contains(exec));
    assertFalse(exec.completed());
    assertEquals(4, downstreams.size());
    
    // call the rest back
    downstreams.get(2).callback(3L);
    final IllegalStateException ex = new IllegalStateException("Boo!");
    downstreams.get(3).callback(ex);
    try {
      exec.deferred().join();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    assertFalse(executor.outstandingRequests().contains(exec));
    assertTrue(exec.completed());
    assertEquals(4, downstreams.size());
  }
  
  @Test
  public void executeFailureRace() throws Exception {
    final MetricShardingExecutor<Long> executor = 
        new MetricShardingExecutor<Long>(node);
    
    final QueryExecution<Long> exec = executor.executeQuery(context, query, span);
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    
    verify(this.executor, times(1)).executeQuery(context, q1, null);
    verify(this.executor, times(1)).executeQuery(context, q2, null);
    verify(this.executor, never()).executeQuery(context, q3, null);
    verify(this.executor, never()).executeQuery(context, q3, null);
    verify(this.executor, never()).executeQuery(context, q4, null);
    verify(this.executor, never()).executeQuery(context, q4, null);
    assertTrue(executor.outstandingRequests().contains(exec));
    assertFalse(exec.completed());
    assertEquals(2, downstreams.size());
    for (final MockDownstream execution : downstreams) {
      assertFalse(execution.cancelled);
      assertFalse(execution.completed());
    }

    // callback 1st, all should stop
    final IllegalStateException ex = new IllegalStateException("Boo!");
    downstreams.get(0).callback(ex);
    try {
      exec.deferred().join();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    verify(this.executor, never()).executeQuery(context, q3, null);
    verify(this.executor, never()).executeQuery(context, q4, null);
    verify(this.executor, never()).executeQuery(context, q4, null);
    assertFalse(executor.outstandingRequests().contains(exec));
    assertTrue(exec.completed());
    assertEquals(2, downstreams.size());
    
    assertFalse(downstreams.get(0).cancelled);
    assertTrue(downstreams.get(0).completed());
    assertTrue(downstreams.get(1).cancelled);
    assertTrue(downstreams.get(1).completed());
  }
  
  @Test
  public void executeCancel() throws Exception {
    final MetricShardingExecutor<Long> executor = 
        new MetricShardingExecutor<Long>(node);
    
    final QueryExecution<Long> exec = executor.executeQuery(context, query, span);
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    
    verify(this.executor, times(1)).executeQuery(context, q1, null);
    verify(this.executor, times(1)).executeQuery(context, q2, null);
    verify(this.executor, never()).executeQuery(context, q3, null);
    verify(this.executor, never()).executeQuery(context, q3, null);
    verify(this.executor, never()).executeQuery(context, q4, null);
    verify(this.executor, never()).executeQuery(context, q4, null);
    assertTrue(executor.outstandingRequests().contains(exec));
    assertFalse(exec.completed());
    assertEquals(2, downstreams.size());
    for (final MockDownstream execution : downstreams) {
      assertFalse(execution.cancelled);
      assertFalse(execution.completed());
    }

    exec.cancel();
    
    try {
      exec.deferred().join();
      fail("Expected QueryExecutionException");
    } catch (QueryExecutionException e) { }
    verify(this.executor, never()).executeQuery(context, q3, null);
    verify(this.executor, never()).executeQuery(context, q4, null);
    verify(this.executor, never()).executeQuery(context, q4, null);
    assertFalse(executor.outstandingRequests().contains(exec));
    assertTrue(exec.completed());
    assertEquals(2, downstreams.size());
    
    assertTrue(downstreams.get(0).cancelled);
    assertTrue(downstreams.get(1).cancelled);
  }
  
  @Test
  public void close() throws Exception {
    final MetricShardingExecutor<Long> executor = 
        new MetricShardingExecutor<Long>(node);
    
    final QueryExecution<Long> exec = executor.executeQuery(context, query, span);
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    
    verify(this.executor, times(1)).executeQuery(context, q1, null);
    verify(this.executor, times(1)).executeQuery(context, q2, null);
    verify(this.executor, never()).executeQuery(context, q3, null);
    verify(this.executor, never()).executeQuery(context, q3, null);
    verify(this.executor, never()).executeQuery(context, q4, null);
    verify(this.executor, never()).executeQuery(context, q4, null);
    assertTrue(executor.outstandingRequests().contains(exec));
    assertFalse(exec.completed());
    assertEquals(2, downstreams.size());
    for (final MockDownstream execution : downstreams) {
      assertFalse(execution.cancelled);
      assertFalse(execution.completed());
    }

    assertNull(executor.close().join());
    
    try {
      exec.deferred().join();
      fail("Expected QueryExecutionException");
    } catch (QueryExecutionException e) { }
    verify(this.executor, never()).executeQuery(context, q3, null);
    verify(this.executor, never()).executeQuery(context, q4, null);
    verify(this.executor, never()).executeQuery(context, q4, null);
    assertFalse(executor.outstandingRequests().contains(exec));
    assertTrue(exec.completed());
    assertEquals(2, downstreams.size());
    
    assertTrue(downstreams.get(0).cancelled);
    assertTrue(downstreams.get(1).cancelled);
    assertEquals(1, executor.downstreamExecutors().size());
    assertSame(this.executor, executor.downstreamExecutors().get(0));
    verify(this.executor, times(1)).close();
  }

  @Test
  public void builder() throws Exception {
    String json = JSON.serializeToString(config);
    assertTrue(json.contains("\"executorType\":\"MetricShardingExecutor\""));
    assertTrue(json.contains("\"parallelExecutors\":2"));
    assertTrue(json.contains("\"mergeStrategy\":\"largest\""));
    assertTrue(json.contains("\"executorId\":\"UT Executor\""));
    
    json = "{\"executorType\":\"MetricShardingExecutor\",\"parallelExecutors\":"
        + "2,\"mergeStrategy\":\"largest\",\"executorId\":\"UT Executor\"}";
    config = JSON.parseToObject(json, Config.class);
    assertEquals("MetricShardingExecutor", config.executorType());
    assertEquals("largest", config.getMergeStrategy());
    assertEquals("UT Executor", config.getExecutorId());
    assertEquals(2, config.getParallelExecutors());
  }
  
  @Test
  public void hashCodeEqualsCompareTo() throws Exception {
    final Config c1 = (Config) Config.newBuilder()
        .setParallelExecutors(2)
        .setMergeStrategy("smallest")
        .setExecutorId("UT Executor")
        .setExecutorType("MetricShardingExecutor")
        .build();
    
    Config c2 = (Config) Config.newBuilder()
        .setParallelExecutors(2)
        .setMergeStrategy("smallest")
        .setExecutorId("UT Executor")
        .setExecutorType("MetricShardingExecutor")
        .build();
    assertEquals(c1.hashCode(), c2.hashCode());
    assertEquals(c1, c2);
    assertEquals(0, c1.compareTo(c2));
    
    c2 = (Config) Config.newBuilder()
        .setParallelExecutors(5)  // <-- Diff
        .setMergeStrategy("smallest")
        .setExecutorId("UT Executor")
        .setExecutorType("MetricShardingExecutor")
        .build();
    assertNotEquals(c1.hashCode(), c2.hashCode());
    assertNotEquals(c1, c2);
    assertEquals(-1, c1.compareTo(c2));
    
    c2 = (Config) Config.newBuilder()
        .setParallelExecutors(2)
        .setMergeStrategy("rando")  // <-- Diff
        .setExecutorId("UT Executor")
        .setExecutorType("MetricShardingExecutor")
        .build();
    assertNotEquals(c1.hashCode(), c2.hashCode());
    assertNotEquals(c1, c2);
    assertEquals(1, c1.compareTo(c2));
    
    c2 = (Config) Config.newBuilder()
        .setParallelExecutors(2)
        //.setMergeStrategy("smallest")  // <-- Diff
        .setExecutorId("UT Executor")
        .setExecutorType("MetricShardingExecutor")
        .build();
    assertNotEquals(c1.hashCode(), c2.hashCode());
    assertNotEquals(c1, c2);
    assertEquals(1, c1.compareTo(c2));
    
    c2 = (Config) Config.newBuilder()
        .setParallelExecutors(2)
        .setMergeStrategy("smallest")
        .setExecutorId("UT Executor2")  // <-- Diff
        .setExecutorType("MetricShardingExecutor")
        .build();
    assertNotEquals(c1.hashCode(), c2.hashCode());
    assertNotEquals(c1, c2);
    assertEquals(-1, c1.compareTo(c2));
    
    c2 = (Config) Config.newBuilder()
        .setParallelExecutors(2)
        .setMergeStrategy("smallest")
        .setExecutorId("UT Executor")
        .setExecutorType("MetricShardingExecutor2")  // <-- Diff
        .build();
    assertNotEquals(c1.hashCode(), c2.hashCode());
    assertNotEquals(c1, c2);
    assertEquals(-1, c1.compareTo(c2));
  }
}
