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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import net.opentsdb.data.DataMerger;
import net.opentsdb.exceptions.RemoteQueryExecutionException;
import net.opentsdb.query.context.QueryContext;
import net.opentsdb.query.context.RemoteContext;
import net.opentsdb.query.execution.ClusterConfig;
import net.opentsdb.query.pojo.TimeSeriesQuery;

/**
 * An executor that uses the {@link RemoteContext#clusters()} config to send
 * the same query to multiple clusters (in dual-write situations) and merges
 * the results using a {@link DataMerger} for the given type.
 * <p>
 * For multi-cluster queries, we fire the same query off to each cluster and 
 * wait for a response from every cluster. If at least one cluster returns
 * a valid result, then that result will be returned via {@link #executeQuery(TimeSeriesQuery)}.
 * However if all clusters return an exception, then the exceptions are
 * packed into a {@link RemoteQueryExecutionException} and the highest status
 * code from exceptions (assuming each one returns a RemoteQueryExecutionException)
 * is used. (500 if they weren't RemoteQueryExecutionExceptions.)
 * 
 * <p>
 * This implementation does not provide timeout handling. Instead, make sure
 * to wrap the {@link ClusterConfig#remoteExecutor()} with a timeout executor
 * if timeout handling is desired.
 * 
 * @param <T>
 */
public class MultiClusterQueryExecutor<T> extends QueryExecutor<T> {
  private static final Logger LOG = LoggerFactory.getLogger(
      MultiClusterQueryExecutor.class);

  /** The list of outstanding executors. */ 
  private final Set<QueryToClusterSplitter> outstanding_executors;
  
  /** The data merger used to merge results. */
  private final DataMerger<T> data_merger;
  
  /** Optional timeout in ms for alternate clusters. */
  private final long timeout;
  
  /**
   * Default Ctor.
   * The ctor will populate a {@link DataMerger} from the context using the
   * provided type. If a proper merger wasn't found, the ctor throws an
   * exception.
   * 
   * TODO - For this type of class that extends a parameterized abstract, the
   * TypeToken doesn't fetch the proper type via:
   * {@code TypeToken<T> type = new TypeToken<T>(getClass()) {};} as it should.
   * Instead it just returns "T". There must be a way to walk the type tree and
   * find the proper type but for now we'll have to pass in the type in the ctor
   * so we can lookup the proper merger.
   * 
   * @param context A non-null context. 
   * @param type A non-null type, same as T that this executor works on.
   * @throws IllegalArgumentException if the cluster or type were null.
   * @throws IllegalStateException if the data merger was null or of the wrong
   * type.
   */
  public MultiClusterQueryExecutor(final QueryContext context, 
      final Class<T> type) {
    this(context, type, 0);
  }
  
  /**
   * Alternate CTor that sets a timeout that, when the first positive response
   * is received from a cluster, initiates a timer task that will merge and 
   * forward the query results upstream regardless of the state of remaining
   * clusters.
   * 
   * @param context A non-null context. 
   * @param type A non-null type, same as T that this executor works on.
   * @param timeout A timeout in milliseconds for alternate colos. 0 == no 
   * timeout and it must be positive.
   * @throws IllegalArgumentException if the cluster or type were null.
   * @throws IllegalStateException if the data merger was null or of the wrong
   * type.
   */
  @SuppressWarnings("unchecked")
  public MultiClusterQueryExecutor(final QueryContext context, 
      final Class<T> type, final long timeout) {
    super(context);
    if (type == null) {
      throw new IllegalArgumentException("Type cannot be null.");
    }
    outstanding_executors = Collections.synchronizedSet(
        Sets.<QueryToClusterSplitter>newHashSet());
    data_merger = (DataMerger<T>) context.getRemoteContext().dataMerger(
        TypeToken.of(type));
    if (data_merger == null) {
      throw new IllegalStateException("No merger could be found for type " + type);
    }
    if (!TypeToken.of(type).equals(data_merger.type())) {
      throw new IllegalStateException("Data merger of type " + data_merger.type() 
        + " did not match the executor's type: " + type);
    }
    if (timeout < 0) {
      throw new IllegalArgumentException("Timeout cannot be negative.");
    }
    this.timeout = timeout;
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override
  public QueryExecution<T> executeQuery(final TimeSeriesQuery query) {
    if (completed.get()) {
      return new FailedQueryExecution(query,
          new RemoteQueryExecutionException(
          "Executor has been cancelled", query.getOrder(), 410));
    }
    try {
      final QueryToClusterSplitter executor = new QueryToClusterSplitter(query);
      outstanding_executors.add(executor);
      return executor.executeQuery(query);
    } catch (Exception e) {
      return new FailedQueryExecution(query, new RemoteQueryExecutionException(
          "Unexpected exception executing query: " + this, 
          query.getOrder(), 500, e));
    }
  }

  @Override
  public Deferred<Object> close() {
    if (completed.compareAndSet(false, true)) {
      if (outstanding_executors.isEmpty()) {
        return Deferred.fromResult(null);
      }
      
      for (final QueryToClusterSplitter executor : outstanding_executors) {
        try {
          executor.cancel();
        } catch (Exception e) {
          LOG.warn("Exception caught when cancelling outstanding "
              + "executor: " + executor, e);
        }
      }
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Executor was already cancelled.");
      }
    }
    return Deferred.fromResult(null);
  }

  /** State class for a specific query that waits for both clusters to complete
   * and merges the response. */
  class QueryToClusterSplitter extends QueryExecution<T> implements TimerTask {
    /** The cluster config snapshot to use when sending queries. */
    private final List<ClusterConfig> clusters;
    
    /** A list of remote exceptions */
    private final Exception[] remote_exceptions;
    
    /** The list of outstanding executions so we can cancel them if needed. */
    @VisibleForTesting
    final QueryExecution<T>[] executions;
    
    /** The results populated by the group by. */
    private final T[] results;
    
    /** Flag set when starting the timer. */
    private final AtomicBoolean timer_started;
    
    /** The timeout from the Timer if timeouts are enabled. */ 
    private Timeout timer_timeout;
    
    /**
     * Default ctor.
     * @param query A non-null query.
     * @throws IllegalStateException if the remote context cluster call fails.
     */
    @SuppressWarnings("unchecked")
    public QueryToClusterSplitter(final TimeSeriesQuery query) {
      super(query);
      clusters = context.getRemoteContext().clusters();
      if (clusters == null || clusters.isEmpty()) {
        throw new IllegalStateException("Remote context returned a null or "
            + "empty list of clusters.");
      }
      remote_exceptions = new Exception[clusters.size()];
      executions = new QueryExecution[clusters.size()];
      results = (T[]) new Object[clusters.size()];
      if (timeout > 0) {
        timer_started = new AtomicBoolean();
      } else {
        timer_started = null;
      }
    }
    
    @SuppressWarnings("unchecked")
    QueryExecution<T> executeQuery(final TimeSeriesQuery query) {
      final List<Deferred<T>> deferreds = Lists.
          <Deferred<T>>newArrayListWithExpectedSize(clusters.size());
      try {
        /** Added to each execution to capture and log exceptions. One cluster
         * could throw errors while the other returns good data. */
        class ErrCB implements Callback<Object, Exception> {
          final int idx;
          public ErrCB(final int idx) {
            this.idx = idx;
          }
          @Override
          public Object call(final Exception e) throws Exception {
            remote_exceptions[idx] = e;
            return null;
          }
        }
        
        /** Triggers the timer on the first positive result. */
        class TimerStarter implements Callback<T, T> {
          final int idx;
          TimerStarter(final int idx) {
            this.idx = idx;
          }
          @Override
          public T call(final T data) throws Exception {
            if (timer_started.compareAndSet(false, true)) {
              timer_timeout = context.getTimer().newTimeout(
                  QueryToClusterSplitter.this, timeout, TimeUnit.MILLISECONDS);
              if (LOG.isDebugEnabled()) {
                LOG.debug("Started timout timer after receiving good result: " 
                    + QueryToClusterSplitter.this);
              }
            }
            results[idx] = data;
            return data;
          }
        }
        
        // execute the query on each remote and add an ErrCB to capture badness.
        for (int i = 0; i < clusters.size(); i++) {
          executions[i] = (QueryExecution<T>) clusters.get(i)
              .remoteExecutor().executeQuery(query);
          deferreds.add(executions[i].deferred()
              .addErrback(new ErrCB(i)));
          if (timeout > 0) {
            executions[i].deferred().addCallback(new TimerStarter(i));
          }
        }
        
        /** Callback that either merges good results and executes the callback or
         * merges the exceptions and executes the callback. */
        class GroupCB implements Callback<Object, ArrayList<T>> {
          @Override
          public Object call(final ArrayList<T> data) throws Exception {
            try {
              if (completed.get()) {
                LOG.error("Splitter was called back but may have been triggered "
                    + "by a timeout. Skipping.");
                deferred.callback(new RemoteQueryExecutionException(
                    "Splitter was already cancelled but we received a result.", 
                    query.getOrder(), 500));
                return null;
              }
              synchronized (this) {
                if (timer_timeout != null) {
                  try {
                    timer_timeout.cancel();
                  } catch (Exception e) {
                    LOG.warn("Unexpected exception canceling timeout", e);
                  }
                  timer_timeout = null;
                }
              }
              final T[] results = (T[]) new Object[data.size()];
              int valid = 0;
              for (int i = 0; i < data.size(); i++) {
                results[i] = data.get(i);
                if (results[i] != null) {
                  valid++;
                }
              }
              
              // we have at least one good result so return it.
              if (valid > 0) {
                callback(data_merger.merge(results));
                return null;
              }
              
              // we don't have good data so we need to bubble up an exception.
              int status = 0;
              for (int i = 0; i < remote_exceptions.length; i++) {
                if (remote_exceptions[i] != null && 
                    remote_exceptions[i] instanceof RemoteQueryExecutionException) {
                  final RemoteQueryExecutionException e = 
                      (RemoteQueryExecutionException) remote_exceptions[i];
                  if (e.getStatusCode() > status) {
                    status = e.getStatusCode();
                  }
                }
              }
              callback(new RemoteQueryExecutionException(
                  "One or more of the cluster sources had an exception", 
                  query.getOrder(), 
                  (status == 0 ? 500 : status), 
                  Lists.newArrayList(remote_exceptions)));
              return null;
            } catch (Exception e) {
              callback(new RemoteQueryExecutionException(
                  "Unexpected exception", query.getOrder(), 500, e));
              return null;
            } finally {
              outstanding_executors.remove(QueryToClusterSplitter.this);
            }
          }
        }
        
        Deferred.group(deferreds).addCallback(new GroupCB());
        return this;
      } catch (Exception e) {
        try {
          callback(new RemoteQueryExecutionException(
              "Unexpected exception executing queries downstream.", 
              query.getOrder(), 500, Lists.newArrayList(remote_exceptions)));
        } catch (Exception ex) {
          LOG.error("Callback threw an exception", e);
        }
        outstanding_executors.remove(QueryToClusterSplitter.this);
        for (final QueryExecution<T> exec : executions) {
          if (exec == null) {
            continue;
          }
          try {
            exec.cancel();
          } catch (Exception ex) {
            LOG.error("Exception thrown cancelling downstream query:" 
                + exec, ex);
          }
        }
        return this;
      }
    }

    @Override
    public void cancel() {
      synchronized (this) {
        if (timer_timeout != null) {
          try {
            timer_timeout.cancel();
          } catch (Exception e) {
            LOG.warn("Exception canceling timer task", e);
          }
          timer_timeout = null;
        }
      }
      for (final QueryExecution<T> exec : executions) {
        try {
          exec.cancel();
        } catch (Exception e) {
          LOG.warn("Exception caught while trying to cancel execution: " 
              + exec, e);
        }
        if (!completed.get()) {
          try {
            callback(new RemoteQueryExecutionException(
                "Query was cancelled upstream: " + this, query.getOrder(), 500));
          } catch (Exception e) {
            LOG.warn("Exception thrown trying to callback on cancellation.", e);
          }
        }
      }
      outstanding_executors.remove(this);
    }

    @Override
    public void run(final Timeout ignored) throws Exception {
      synchronized (this) {
        timer_timeout = null;
      }
      try {
        callback(data_merger.merge(results));
      } catch (Exception e) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Lost race condition timing out query after getting "
              + "good results from a colo", e);
        }
      }
      cancel();
    }
  }

  @VisibleForTesting
  DataMerger<T> dataMerger() {
    return data_merger;
  }
  
  @VisibleForTesting
  Set<QueryToClusterSplitter> outstandingExecutors() {
    return outstanding_executors;
  }
}
