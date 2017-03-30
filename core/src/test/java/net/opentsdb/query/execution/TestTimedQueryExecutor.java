// This file is part of OpenTSDB.
// Copyright (C) 2016  The OpenTSDB Authors.
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
import static org.junit.Assert.assertNotSame;
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

import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.TimeoutException;

import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import net.opentsdb.exceptions.RemoteQueryExecutionException;
import net.opentsdb.query.context.QueryContext;
import net.opentsdb.query.pojo.TimeSeriesQuery;

public class TestTimedQueryExecutor {
  private QueryContext context;
  private QueryExecutor<Long> executor;
  private Timer timer;
  private TimeSeriesQuery query;
  private MockDownstream downstream;
  private Timeout timeout;
  
  @SuppressWarnings("unchecked")
  @Before
  public void before() throws Exception {
    context = mock(QueryContext.class);
    executor = mock(QueryExecutor.class);
    timer = mock(Timer.class);
    query = mock(TimeSeriesQuery.class);
    downstream = new MockDownstream(query);
    timeout = mock(Timeout.class);
    
    when(context.getTimer()).thenReturn(timer);
    when(executor.executeQuery(query)).thenReturn(downstream);
    when(executor.close()).thenReturn(Deferred.fromResult(null));
    when(timer.newTimeout(any(TimerTask.class), anyLong(), eq(TimeUnit.MILLISECONDS)))
      .thenReturn(timeout);
  }
  
  @Test
  public void ctor() throws Exception {
    final TimedQueryExecutor<Long> tqe = new TimedQueryExecutor<Long>(
        context, executor, 1000);
    assertTrue(tqe.outstandingRequests().isEmpty());
    
    try {
      new TimedQueryExecutor<Long>(null, executor, 1000);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new TimedQueryExecutor<Long>(context, null, 1000);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new TimedQueryExecutor<Long>(context, executor, 0);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void execute() throws Exception {
    final TimedQueryExecutor<Long> tqe = new TimedQueryExecutor<Long>(
        context, executor, 1000);
    
    final QueryExecution<Long> exec = tqe.executeQuery(query);
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    
    verify(timer, times(1))
      .newTimeout((TimerTask) exec, 1000, TimeUnit.MILLISECONDS);
    verify(timeout, never()).cancel();
    verify(executor, times(1)).executeQuery(query);
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
    final TimedQueryExecutor<Long> tqe = new TimedQueryExecutor<Long>(
        context, executor, 1000);
    tqe.completed.set(true);
    
    final QueryExecution<Long> exec = tqe.executeQuery(query);
    try {
      exec.deferred().join();
      fail("Expected RemoteQueryExecutionException");
    } catch (RemoteQueryExecutionException e) { }
    verify(timer, never())
      .newTimeout(any(TimerTask.class), eq(1000), eq(TimeUnit.MILLISECONDS));
    verify(timeout, never()).cancel();
    verify(executor, never()).executeQuery(query);
    assertNotSame(downstream, exec);
    assertFalse(tqe.outstandingRequests().contains(exec));
  }
  
  @Test
  public void executeThrownException() throws Exception {
    final TimedQueryExecutor<Long> tqe = new TimedQueryExecutor<Long>(
        context, executor, 1000);
    when(executor.executeQuery(query)).thenThrow(new IllegalStateException("Boo!"));
    
    final QueryExecution<Long> exec = tqe.executeQuery(query);
    try {
      exec.deferred().join();
      fail("Expected RemoteQueryExecutionException");
    } catch (RemoteQueryExecutionException e) { }
    verify(timer, never())
      .newTimeout(any(TimerTask.class), eq(1000), eq(TimeUnit.MILLISECONDS));
    verify(timeout, never()).cancel();
    verify(executor, times(1)).executeQuery(query);
    assertFalse(tqe.outstandingRequests().contains(exec));
  }
  
  @Test
  public void executeNullTimer() throws Exception {
    final TimedQueryExecutor<Long> tqe = new TimedQueryExecutor<Long>(
        context, executor, 1000);
    when(context.getTimer()).thenReturn(null);
    
    final QueryExecution<Long> exec = tqe.executeQuery(query);
    try {
      exec.deferred().join();
      fail("Expected RemoteQueryExecutionException");
    } catch (RemoteQueryExecutionException e) { }
    verify(timer, never())
      .newTimeout(any(TimerTask.class), eq(1000), eq(TimeUnit.MILLISECONDS));
    verify(timeout, never()).cancel();
    verify(executor, times(1)).executeQuery(query);
    assertFalse(tqe.outstandingRequests().contains(exec));
  }
  
  @Test
  public void executeDownstreamException() throws Exception {
    final TimedQueryExecutor<Long> tqe = new TimedQueryExecutor<Long>(
        context, executor, 1000);
    
    final QueryExecution<Long> exec = tqe.executeQuery(query);
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    
    verify(timer, times(1))
      .newTimeout((TimerTask) exec, 1000, TimeUnit.MILLISECONDS);
    verify(timeout, never()).cancel();
    verify(executor, times(1)).executeQuery(query);
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
    final TimedQueryExecutor<Long> tqe = new TimedQueryExecutor<Long>(
        context, executor, 1000);
    
    final QueryExecution<Long> exec = tqe.executeQuery(query);
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    
    verify(timer, times(1))
      .newTimeout((TimerTask) exec, 1000, TimeUnit.MILLISECONDS);
    verify(timeout, never()).cancel();
    verify(executor, times(1)).executeQuery(query);
    assertFalse(downstream.cancelled);
    assertFalse(downstream.completed());
    assertTrue(tqe.outstandingRequests().contains(exec));
    
    ((TimerTask) exec).run(null);
    try {
      exec.deferred().join();
      fail("Expected RemoteQueryExecutionException");
    } catch (RemoteQueryExecutionException e) { }
    assertFalse(tqe.outstandingRequests().contains(exec));
    verify(timeout, never()).cancel();
    assertFalse(downstream.completed());
    assertTrue(downstream.cancelled);
    assertTrue(exec.completed());
    
    // double is ok
    ((TimerTask) exec).run(null);
  }
  
  @Test
  public void executeTimeoutAfterSuccess() throws Exception {
    final TimedQueryExecutor<Long> tqe = new TimedQueryExecutor<Long>(
        context, executor, 1000);
    
    final QueryExecution<Long> exec = tqe.executeQuery(query);
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    
    verify(timer, times(1))
      .newTimeout((TimerTask) exec, 1000, TimeUnit.MILLISECONDS);
    verify(timeout, never()).cancel();
    verify(executor, times(1)).executeQuery(query);
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
    final TimedQueryExecutor<Long> tqe = new TimedQueryExecutor<Long>(
        context, executor, 1000);
    
    final QueryExecution<Long> exec = tqe.executeQuery(query);
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    
    verify(timer, times(1))
      .newTimeout((TimerTask) exec, 1000, TimeUnit.MILLISECONDS);
    verify(timeout, never()).cancel();
    verify(executor, times(1)).executeQuery(query);
    assertFalse(downstream.cancelled);
    assertFalse(downstream.completed());
    assertTrue(tqe.outstandingRequests().contains(exec));
    
    exec.cancel();
    try {
      exec.deferred().join();
      fail("Expected RemoteQueryExecutionException");
    } catch (RemoteQueryExecutionException e) { }
    assertFalse(tqe.outstandingRequests().contains(exec));
    verify(timeout, times(1)).cancel();
    assertFalse(downstream.completed());
    assertTrue(downstream.cancelled);
  }
  
  @Test
  public void close() throws Exception {
    final TimedQueryExecutor<Long> tqe = new TimedQueryExecutor<Long>(
        context, executor, 1000);
    
    final QueryExecution<Long> exec = tqe.executeQuery(query);
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    assertTrue(tqe.outstandingRequests().contains(exec));
    
    tqe.close().join();
    verify(timeout, times(1)).cancel();
    assertFalse(downstream.completed());
    assertTrue(downstream.cancelled);
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
