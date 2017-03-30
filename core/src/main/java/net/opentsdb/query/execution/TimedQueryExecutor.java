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

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import net.opentsdb.exceptions.RemoteQueryExecutionException;
import net.opentsdb.query.context.QueryContext;
import net.opentsdb.query.pojo.Query;

/**
 * A {@link QueryExecutor} wrapper that uses a timer to kill a query that
 * is taking too long. On a timeout, {@link QueryExecution#cancel()} is called
 * on the downstream query.
 * 
 * @param <T> The type of data the query executor handles.
 * 
 * @since 3.0
 */
public class TimedQueryExecutor<T> extends QueryExecutor<T> {
  private static final Logger LOG = LoggerFactory.getLogger(
      TimedQueryExecutor.class);
  
  /** The downstream executor that queries are passed to. */
  private final QueryExecutor<T> executor;
  
  /** How long, in milliseconds, we wait for each query. */
  private final long timeout;
  
  /** The queries that are still waiting for a response or timeout. */
  private Set<TimedQuery> outstanding_queries;
  
  /**
   * Default ctor.
   * @param context A non-null context to pull the timer from.
   * @param executor A non-null query executor to forward queries to.
   * @param timeout A timeout in milliseconds.
   * @throws IllegalArgumentException if the context or executor were null or
   * if the timeout was less than 1 millisecond.
   */
  public TimedQueryExecutor(final QueryContext context,
                            final QueryExecutor<T> executor, 
                            final long timeout) {
    super(context);
    if (executor == null) {
      throw new IllegalArgumentException("Executor cannot be null.");
    }
    if (timeout < 1) {
      throw new IllegalArgumentException("Timeout must be greater than zero.");
    }
    this.executor = executor;
    this.timeout = timeout;
    outstanding_queries = Collections.<TimedQuery>synchronizedSet(
        Sets.<TimedQuery>newHashSetWithExpectedSize(1));
  }

  @Override
  public QueryExecution<T> executeQuery(final Query query) {
    if (completed.get()) {
      return new FailedQueryExecution<T>(query, new RemoteQueryExecutionException(
            "Timeout executor was already marked as completed: " + this, 
            query.getOrder(), 500));
    }
    try {
      final TimedQuery timed_query = new TimedQuery(query);
      timed_query.execute();
      return timed_query;
    } catch (Exception e) {
      return new FailedQueryExecution<T>(query, new RemoteQueryExecutionException(
          "Unexpected exception executing query: " + this, 
          query.getOrder(), 500, e));
    }
  }
  
  /** Class that wraps the response from the downstream query so that on
   * callback, the timer task is cancelled. Also implements the timer task that
   * will be called if query has indeed timed out. */
  private class TimedQuery extends QueryExecution<T> implements TimerTask {
    /** The timeout returned by the timer so we can cancel it. */
    protected Timeout timer_timeout;
    
    /** The downstream execution to wait on (or cancel). */
    protected QueryExecution<T> downstream;
    
    /**
     * Default ctor.
     * @param query A non-null query.
     */
    public TimedQuery(final Query query) {
      super(query);
      outstanding_queries.add(this);
    }
    
    void execute() {
      class ErrCB implements Callback<Object, Exception> {
        @Override
        public Object call(final Exception e) throws Exception {
          complete();
          callback(e);
          return null;
        }
      }

      class SuccessCB implements Callback<Object, T> {
        @Override
        public Object call(final T obj) throws Exception {
          complete();
          callback(obj);
          return null;
        }
      }
      
      // run it!
      try {
        downstream = executor.executeQuery(query);
        downstream.deferred()
          .addCallback(new SuccessCB())
          .addErrback(new ErrCB());
        timer_timeout = context.getTimer()
            .newTimeout(this, timeout, TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        cancel();
        complete();
        callback(new RemoteQueryExecutionException(
          "Unexpected exception executing query: " + this, 
              query.getOrder(), 500, e));
      }
    }

    @Override
    public void run(final Timeout timeout) throws Exception {
      try {
        if (completed.get()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Timeout executed after a successful execution.");
          }
          return;
        }
        callback(new RemoteQueryExecutionException(
            "Timed executor timed out " + this, query.getOrder(), 504));
      } catch (Exception e) {
        LOG.error("Timer task callback: ", e);
      }
      synchronized (this) {
        timer_timeout = null;
        cancel();
        complete();
      }
    }

    @Override
    public void cancel() {
      synchronized (this) {
        if (downstream != null) {
          downstream.cancel();
        }
        complete();
        if (!completed.get()) {
          try {
            callback(new RemoteQueryExecutionException(
                "Query was cancelled upstream: " + this, query.getOrder(), 500));
          } catch (Exception e) {
            LOG.warn("Exception thrown trying to callback on cancellation.", e);
          }
        }
      }
    }
    
    /** If the timeout is not null, we cancel it. Also remove this from the
     * outstanding queries set. */
    private synchronized void complete() {
      if (timer_timeout != null) {
        timer_timeout.cancel();
        timer_timeout = null;
      }
      outstanding_queries.remove(this);
    }
  }
  
  @Override
  public Deferred<Object> close() {
    if (completed.compareAndSet(false, true)) {
      for (final TimedQuery outstanding : outstanding_queries) {
        outstanding.cancel();
      }
      return executor.close();
    }
    return Deferred.fromResult(null);
  }

  @VisibleForTesting
  Set<TimedQuery> outstandingQueries() {
    return outstanding_queries;
  }
}
