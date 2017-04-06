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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.TimeoutException;

import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import io.opentracing.Span;
import net.opentsdb.data.DataMerger;
import net.opentsdb.exceptions.RemoteQueryExecutionException;
import net.opentsdb.query.context.QueryContext;
import net.opentsdb.query.context.QueryExecutorContext;
import net.opentsdb.query.context.RemoteContext;
import net.opentsdb.query.execution.MetricShardingExecutor.Config;
import net.opentsdb.query.pojo.Metric;
import net.opentsdb.query.pojo.TimeSeriesQuery;

public class TestMetricShardingExecutor {
  private QueryContext context;
  private RemoteContext remote_context;
  private QueryExecutorContext executor_context;
  private Timer timer;
  private TimeSeriesQuery query;
  private ClusterConfig cluster_a;
  private ClusterConfig cluster_b;
  private int executor_index;
  private QueryExecutor<Long> executor_a;
  private QueryExecutor<Long> executor_b;
  private List<MockDownstream> downstreams;
  private DataMerger<Long> merger;
  private Timeout timeout;
  private Span span;
  private Config<Long> config;
  
  @SuppressWarnings("unchecked")
  @Before
  public void before() throws Exception {
    context = mock(QueryContext.class);
    remote_context = mock(RemoteContext.class);
    executor_context = mock(QueryExecutorContext.class);
    timer = mock(Timer.class);
    cluster_a = mock(ClusterConfig.class);
    cluster_b = mock(ClusterConfig.class);
    executor_a = mock(QueryExecutor.class);
    executor_b = mock(QueryExecutor.class);
    downstreams = Lists.newArrayList();
    merger = mock(DataMerger.class);
    timeout = mock(Timeout.class);
    span = mock(Span.class);
    config = Config.<Long>newBuilder()
        .setType(Long.class)
        .setParallelExecutors(2)
        .build();
    
    when(context.getRemoteContext()).thenReturn(remote_context);
    when(context.getTimer()).thenReturn(timer);
    when(context.getQueryExecutorContext()).thenReturn(executor_context);
    when(timer.newTimeout(any(TimerTask.class), anyLong(), 
        eq(TimeUnit.MILLISECONDS))).thenReturn(timeout);
    when(remote_context.dataMerger(any(TypeToken.class)))
      .thenAnswer(new Answer<DataMerger<?>>() {
      @Override
      public DataMerger<?> answer(InvocationOnMock invocation)
          throws Throwable {
        return merger;
      }
    });
    when(remote_context.clusters()).thenReturn(
        Lists.newArrayList(cluster_a, cluster_b));
    when(merger.type()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return TypeToken.of(Long.class);
      }
    });
    when(executor_context.newDownstreamExecutor(context, config.getFactory()))
      .thenAnswer(new Answer<QueryExecutor<?>>() {
      @Override
      public QueryExecutor<?> answer(InvocationOnMock invocation)
          throws Throwable {
        if ((executor_index++ % 2) == 0) {
          return executor_a;
        } else {
          return executor_b;
        }
      }
    });
    when(cluster_a.remoteExecutor()).thenAnswer(new Answer<QueryExecutor<?>>() {
      @Override
      public QueryExecutor<?> answer(InvocationOnMock invocation)
          throws Throwable {
        return executor_a;
      }
    });
    when(cluster_b.remoteExecutor()).thenAnswer(new Answer<QueryExecutor<?>>() {
      @Override
      public QueryExecutor<?> answer(InvocationOnMock invocation)
          throws Throwable {
        return executor_b;
      }
    });
    when(executor_a.executeQuery(any(TimeSeriesQuery.class), any(Span.class)))
      .thenAnswer(new Answer<QueryExecution<?>>() {
        @Override
        public QueryExecution<?> answer(InvocationOnMock invocation)
            throws Throwable {
          final MockDownstream downstream = 
              new MockDownstream((TimeSeriesQuery) invocation.getArguments()[0]);
          downstreams.add(downstream);
          return downstream;
        }
      });
    when(executor_b.executeQuery(any(TimeSeriesQuery.class), any(Span.class)))
      .thenAnswer(new Answer<QueryExecution<?>>() {
        @Override
        public QueryExecution<?> answer(InvocationOnMock invocation)
            throws Throwable {
          final MockDownstream downstream = 
              new MockDownstream((TimeSeriesQuery) invocation.getArguments()[0]);
          downstreams.add(downstream);
          return downstream;
        }
      });
    when(executor_a.close()).thenReturn(Deferred.fromResult(null));
    when(executor_b.close()).thenReturn(Deferred.fromResult(null));
    when(merger.merge((List<Long>) any(List.class), eq(context), any(Span.class)))
      .thenAnswer(new Answer<Long>() {
      @Override
      public Long answer(InvocationOnMock invocation) throws Throwable {
        return 42L;
      }
    });
    
    final TimeSeriesQuery q1 = TimeSeriesQuery.newBuilder()
        .addMetric(Metric.newBuilder().setMetric("sys.cpu.user").setId("m1"))
        .build();
    final TimeSeriesQuery q2 = TimeSeriesQuery.newBuilder()
        .addMetric(Metric.newBuilder().setMetric("sys.cpu.idle").setId("m2"))
        .build();
    final TimeSeriesQuery q3 = TimeSeriesQuery.newBuilder()
        .addMetric(Metric.newBuilder().setMetric("sys.cpu.iowait").setId("m3"))
        .build();
    final TimeSeriesQuery q4 = TimeSeriesQuery.newBuilder()
        .addMetric(Metric.newBuilder().setMetric("sys.cpu.softirq").setId("m4"))
        .build();
    
    query = TimeSeriesQuery.newBuilder()
        .addMetric(Metric.newBuilder().setMetric("sys.cpu.user").setId("m1"))
        .addMetric(Metric.newBuilder().setMetric("sys.cpu.idle").setId("m2"))
        .addMetric(Metric.newBuilder().setMetric("sys.cpu.iowait").setId("m3"))
        .addMetric(Metric.newBuilder().setMetric("sys.cpu.softirq").setId("m4"))
        .build();
    query.addSubQuery(q1);
    query.addSubQuery(q2);
    query.addSubQuery(q3);
    query.addSubQuery(q4);
  }
  
  @Test
  public void ctor() throws Exception {
    MetricShardingExecutor<Long> executor = 
        new MetricShardingExecutor<Long>(context, config);
    assertSame(merger, executor.dataMerger());
    assertTrue(executor.outstandingRequests().isEmpty());
    
    executor = new MetricShardingExecutor<Long>(context, config);
    assertSame(merger, executor.dataMerger());
    assertTrue(executor.outstandingRequests().isEmpty());
    
    try {
      new MetricShardingExecutor<Long>(null, config);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new MetricShardingExecutor<Long>(context, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new MetricShardingExecutor<Long>(context, Config.<Long>newBuilder()
          .setType(Long.class)
          .setParallelExecutors(0)
          .build());
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    when(merger.type()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return TypeToken.of(String.class);
      }
    });
    try {
      new MetricShardingExecutor<Long>(context, config);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    
    when(remote_context.dataMerger(any(TypeToken.class))).thenReturn(null);
    try {
      new MetricShardingExecutor<Long>(context, config);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    
    // bad executor factory. Have to restore the merger answers
    when(remote_context.dataMerger(any(TypeToken.class)))
      .thenAnswer(new Answer<DataMerger<?>>() {
      @Override
      public DataMerger<?> answer(InvocationOnMock invocation) throws Throwable {
        return merger;
      }
    });
    when(merger.type()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return TypeToken.of(Long.class);
      }
    });
    when(executor_context.newDownstreamExecutor(context, config.getFactory()))
      .thenReturn(null);
    try {
      new MetricShardingExecutor<Long>(context, config);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    
    when(executor_context.newDownstreamExecutor(context, config.getFactory()))
      .thenThrow(new IllegalArgumentException("Boo!"));
    try {
      new MetricShardingExecutor<Long>(context, config);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void execute() throws Exception {
    final MetricShardingExecutor<Long> executor = 
        new MetricShardingExecutor<Long>(context, config);
    
    final QueryExecution<Long> exec = executor.executeQuery(query, span);
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    
    verify(executor_a, times(1)).executeQuery(query.subQueries().get(0), null);
    verify(executor_b, times(1)).executeQuery(query.subQueries().get(1), null);
    verify(executor_a, never()).executeQuery(query.subQueries().get(2), null);
    verify(executor_b, never()).executeQuery(query.subQueries().get(2), null);
    verify(executor_a, never()).executeQuery(query.subQueries().get(3), null);
    verify(executor_b, never()).executeQuery(query.subQueries().get(3), null);
    assertTrue(executor.outstandingRequests().contains(exec));
    assertFalse(exec.completed());
    assertEquals(2, downstreams.size());
    for (final MockDownstream execution : downstreams) {
      assertFalse(execution.cancelled);
      assertFalse(execution.completed());
    }

    // callback 1st, next should start
    downstreams.get(0).callback(1L);
    verify(executor_a, times(1)).executeQuery(query.subQueries().get(2), null);
    verify(executor_a, never()).executeQuery(query.subQueries().get(3), null);
    verify(executor_b, never()).executeQuery(query.subQueries().get(3), null);
    assertTrue(executor.outstandingRequests().contains(exec));
    assertFalse(exec.completed());
    assertEquals(3, downstreams.size());
    
    // callback 2nd, last should start
    downstreams.get(1).callback(2L);
    verify(executor_b, times(1)).executeQuery(query.subQueries().get(3), null);
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
  public void executeEarlyFail() throws Exception {
    final MetricShardingExecutor<Long> executor = 
        new MetricShardingExecutor<Long>(context, config);
    
    final QueryExecution<Long> exec = executor.executeQuery(query, span);
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    
    verify(executor_a, times(1)).executeQuery(query.subQueries().get(0), null);
    verify(executor_b, times(1)).executeQuery(query.subQueries().get(1), null);
    verify(executor_a, never()).executeQuery(query.subQueries().get(2), null);
    verify(executor_b, never()).executeQuery(query.subQueries().get(2), null);
    verify(executor_a, never()).executeQuery(query.subQueries().get(3), null);
    verify(executor_b, never()).executeQuery(query.subQueries().get(3), null);
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
    verify(executor_a, never()).executeQuery(query.subQueries().get(2), null);
    verify(executor_a, never()).executeQuery(query.subQueries().get(3), null);
    verify(executor_b, never()).executeQuery(query.subQueries().get(3), null);
    assertFalse(executor.outstandingRequests().contains(exec));
    assertTrue(exec.completed());
    assertEquals(2, downstreams.size());
    
    assertTrue(downstreams.get(1).cancelled);
  }
  
  @Test
  public void executeLateFail() throws Exception {
    final MetricShardingExecutor<Long> executor = 
        new MetricShardingExecutor<Long>(context, config);
    
    final QueryExecution<Long> exec = executor.executeQuery(query, span);
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    
    verify(executor_a, times(1)).executeQuery(query.subQueries().get(0), null);
    verify(executor_b, times(1)).executeQuery(query.subQueries().get(1), null);
    verify(executor_a, never()).executeQuery(query.subQueries().get(2), null);
    verify(executor_b, never()).executeQuery(query.subQueries().get(2), null);
    verify(executor_a, never()).executeQuery(query.subQueries().get(3), null);
    verify(executor_b, never()).executeQuery(query.subQueries().get(3), null);
    assertTrue(executor.outstandingRequests().contains(exec));
    assertFalse(exec.completed());
    assertEquals(2, downstreams.size());
    for (final MockDownstream execution : downstreams) {
      assertFalse(execution.cancelled);
      assertFalse(execution.completed());
    }

    // callback 1st, next should start
    downstreams.get(0).callback(1L);
    verify(executor_a, times(1)).executeQuery(query.subQueries().get(2), null);
    verify(executor_a, never()).executeQuery(query.subQueries().get(3), null);
    verify(executor_b, never()).executeQuery(query.subQueries().get(3), null);
    assertTrue(executor.outstandingRequests().contains(exec));
    assertFalse(exec.completed());
    assertEquals(3, downstreams.size());
    
    // callback 2nd, last should start
    downstreams.get(1).callback(2L);
    verify(executor_b, times(1)).executeQuery(query.subQueries().get(3), null);
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
        new MetricShardingExecutor<Long>(context, config);
    
    final QueryExecution<Long> exec = executor.executeQuery(query, span);
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    
    verify(executor_a, times(1)).executeQuery(query.subQueries().get(0), null);
    verify(executor_b, times(1)).executeQuery(query.subQueries().get(1), null);
    verify(executor_a, never()).executeQuery(query.subQueries().get(2), null);
    verify(executor_b, never()).executeQuery(query.subQueries().get(2), null);
    verify(executor_a, never()).executeQuery(query.subQueries().get(3), null);
    verify(executor_b, never()).executeQuery(query.subQueries().get(3), null);
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
    downstreams.get(1).callback(ex);
    try {
      exec.deferred().join();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    verify(executor_a, never()).executeQuery(query.subQueries().get(2), null);
    verify(executor_a, never()).executeQuery(query.subQueries().get(3), null);
    verify(executor_b, never()).executeQuery(query.subQueries().get(3), null);
    assertFalse(executor.outstandingRequests().contains(exec));
    assertTrue(exec.completed());
    assertEquals(2, downstreams.size());
    
    assertTrue(downstreams.get(1).cancelled);
    assertTrue(downstreams.get(1).completed());
  }
  
  @Test
  public void executeCancel() throws Exception {
    final MetricShardingExecutor<Long> executor = 
        new MetricShardingExecutor<Long>(context, config);
    
    final QueryExecution<Long> exec = executor.executeQuery(query, span);
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    
    verify(executor_a, times(1)).executeQuery(query.subQueries().get(0), null);
    verify(executor_b, times(1)).executeQuery(query.subQueries().get(1), null);
    verify(executor_a, never()).executeQuery(query.subQueries().get(2), null);
    verify(executor_b, never()).executeQuery(query.subQueries().get(2), null);
    verify(executor_a, never()).executeQuery(query.subQueries().get(3), null);
    verify(executor_b, never()).executeQuery(query.subQueries().get(3), null);
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
      fail("Expected RemoteQueryExecutionException");
    } catch (RemoteQueryExecutionException e) { }
    verify(executor_a, never()).executeQuery(query.subQueries().get(2), null);
    verify(executor_a, never()).executeQuery(query.subQueries().get(3), null);
    verify(executor_b, never()).executeQuery(query.subQueries().get(3), null);
    assertFalse(executor.outstandingRequests().contains(exec));
    assertTrue(exec.completed());
    assertEquals(2, downstreams.size());
    
    assertTrue(downstreams.get(0).cancelled);
    assertTrue(downstreams.get(1).cancelled);
  }
  
  @Test
  public void close() throws Exception {
    final MetricShardingExecutor<Long> executor = 
        new MetricShardingExecutor<Long>(context, config);
    
    final QueryExecution<Long> exec = executor.executeQuery(query, span);
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    
    verify(executor_a, times(1)).executeQuery(query.subQueries().get(0), null);
    verify(executor_b, times(1)).executeQuery(query.subQueries().get(1), null);
    verify(executor_a, never()).executeQuery(query.subQueries().get(2), null);
    verify(executor_b, never()).executeQuery(query.subQueries().get(2), null);
    verify(executor_a, never()).executeQuery(query.subQueries().get(3), null);
    verify(executor_b, never()).executeQuery(query.subQueries().get(3), null);
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
      fail("Expected RemoteQueryExecutionException");
    } catch (RemoteQueryExecutionException e) { }
    verify(executor_a, never()).executeQuery(query.subQueries().get(2), null);
    verify(executor_a, never()).executeQuery(query.subQueries().get(3), null);
    verify(executor_b, never()).executeQuery(query.subQueries().get(3), null);
    assertFalse(executor.outstandingRequests().contains(exec));
    assertTrue(exec.completed());
    assertEquals(2, downstreams.size());
    
    assertTrue(downstreams.get(0).cancelled);
    assertTrue(downstreams.get(1).cancelled);
  }
  
  /** Simple implementation to peek into the cancel call. */
  class MockDownstream extends QueryExecution<Long> {
    boolean cancelled;
    
    public MockDownstream(TimeSeriesQuery query) {
      super(query);
    }

    @Override
    public void cancel() {
      cancelled = true;
    }
    
  }
}
