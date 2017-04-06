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

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import io.opentracing.Span;
import net.opentsdb.data.DataMerger;
import net.opentsdb.exceptions.RemoteQueryExecutionException;
import net.opentsdb.query.context.QueryContext;
import net.opentsdb.query.plan.SplitMetricPlanner;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.utils.JSON;

/**
 * An executor that takes {@link TimeSeriesQuery}s that have 1 or more child
 * queries with a single metric each (e.g. one returned by the 
 * {@link SplitMetricPlanner}. Each sub query is sent to an executor up to
 * {@link #parallel_executors}. If there are more sub queries than 
 * {@link #parallel_executors} the executor waits until one of the outstanding
 * queries has completed before firing off another query to the next executor.
 * <p>
 * If any of the downstream executors return an exception, all sub queries are
 * cancelled and the exception is sent upstream.
 * <p>
 * Results from the various metrics are then merged into one response using the
 * appropriate {@link DataMerger}.
 * 
 * @param <T> The type of data returned by this executor.
 * 
 * @since 3.0
 */
public class MetricShardingExecutor<T> extends QueryExecutor<T> {
  private static final Logger LOG = LoggerFactory.getLogger(
      MetricShardingExecutor.class);
  
  /** How many queries to fire in parallel for the currently executing query. */
  protected final int parallel_executors;
  
  /** The data merger used to merge results. */
  private final DataMerger<T> data_merger;
  
  /** The downstream executors to rotate through. */
  protected QueryExecutor<T>[] executors;
  
  /** The current index in the executors array. */
  protected int executor_index;
  
  /**
   * Default ctor.
   * @param context A non-null context. 
   * @param config A query executor config.
   * @throws IllegalArgumentException if the config or type were null or the
   * parallel executors were set to less than 1.
   * @throws IllegalStateException if the data merger was null or the wrong type
   * or if one of the downstream executors was null.
   */
  @SuppressWarnings("unchecked")
  public MetricShardingExecutor(final QueryContext context, 
                                final QueryExecutorConfig config) {
    super(context, config);
    if (config == null) {
      throw new IllegalArgumentException("Config cannot be null.");
    }
    if (((Config<T>) config).type == null) {
      throw new IllegalArgumentException("Type cannot be null.");
    }
    if (((Config<T>) config).parallel_executors < 1) {
      throw new IllegalArgumentException("Parallel executors must be one or "
          + "greater.");
    }
    parallel_executors = ((Config<T>) config).parallel_executors;
    data_merger = (DataMerger<T>) context.getRemoteContext().dataMerger(
        TypeToken.of(((Config<T>) config).type));
    if (data_merger == null) {
      throw new IllegalStateException("No merger could be found for type " 
          + ((Config<T>) config).type);
    }
    if (!TypeToken.of(((Config<T>) config).type).equals(data_merger.type())) {
      throw new IllegalStateException("Data merger of type " + data_merger.type() 
        + " did not match the executor's type: " + ((Config<T>) config).type);
    }
    
    executors = new QueryExecutor[parallel_executors];
    for (int i = 0; i < parallel_executors; i++) {
      executors[i] = (QueryExecutor<T>) context.getQueryExecutorContext()
          .newDownstreamExecutor(context, config.getFactory());
      if (executors[i] == null) {
        throw new IllegalStateException("Downstream executor returned null.");
      }
    }
  }

  @Override
  public QueryExecution<T> executeQuery(final TimeSeriesQuery query,
                                        final Span upstream_span) {
    if (query.subQueries() == null || query.subQueries().isEmpty()) {
      throw new IllegalArgumentException("Query didn't have any sub queries.");
    }
    final QuerySplitter executor = new QuerySplitter(query);
    executor.executeQuery(upstream_span);
    return executor;
  }

  /** The execution for a specific query that handles rotating through executors. */
  private class QuerySplitter extends QueryExecution<T> {
    /** The parent query to pull sub queries out of. */
    private final TimeSeriesQuery query;
    
    /** The index of the next child query to fire off. */
    private int splits_index;
    
    /** The list of outstanding executions so we can cancel them if needed. */
    @VisibleForTesting
    private final QueryExecution<T>[] executions;
    
    /**
     * Default ctor.
     * @param query A non-null query.
     */
    @SuppressWarnings("unchecked")
    public QuerySplitter(final TimeSeriesQuery query) {
      super(query);
      outstanding_executions.add(this);
      this.query = query;
      executions = new QueryExecution[query.subQueries().size()];
    }

    @SuppressWarnings("unchecked")
    QueryExecution<T> executeQuery(final Span upstream_span) {
      if (context.getTracer() != null) {
        setSpan(context, MetricShardingExecutor.this.getClass().getSimpleName(), 
            upstream_span,
            new ImmutableMap.Builder<String, String>()
              .put("order", Integer.toString(query.getOrder()))
              .put("query", JSON.serializeToString(query))
              .put("startThread", Thread.currentThread().getName())
              .build());
      }
      for (int i = 0; i < executions.length && i < parallel_executors; i++) {
        executions[i] = (QueryExecution<T>) 
            nextExecutor().executeQuery(query.subQueries().get(i), tracer_span);
        executions[i].deferred().addCallback(new DataCB())
                                .addErrback(new ErrCB());
        ++splits_index;
      }
      if (splits_index >= executions.length) {
        groupEm();
      }
      return this;
    }
    
    /**
     * <b>WARNING:</b> Make sure to synchronize on *this* before executing to
     * avoid a race.
     */
    @SuppressWarnings("unchecked")
    private void launchNext() {
      final TimeSeriesQuery sub_query = query.subQueries().get(splits_index);
      executions[splits_index] = (QueryExecution<T>) 
          nextExecutor().executeQuery(sub_query, tracer_span);
      executions[splits_index].deferred()
                              .addCallback(new DataCB())
                              .addErrback(new ErrCB());
      ++splits_index;
      if (splits_index >= query.subQueries().size()) {
        groupEm();
      }
    }
    
    @Override
    public void cancel() {
      synchronized (this) {
        outstanding_executions.remove(this);
        // set to max to prevent anyone else running.
        splits_index = query.subQueries().size(); 
        for (final QueryExecution<T> execution : executions) {
          if (execution != null) {
            execution.cancel();
          }
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
    }
  
    /** Helper that groups the deferreds and adds the final callback. */
    private void groupEm() {
      final List<Deferred<T>> deferreds = 
          Lists.<Deferred<T>>newArrayListWithExpectedSize(executions.length);
      for (final QueryExecution<T> execution : executions) {
        deferreds.add(execution.deferred());
      }
      Deferred.group(deferreds).addCallback(new GroupCB());
    }
    
    /** The final class that will be called if all is good */
    class GroupCB implements Callback<Object, ArrayList<T>> {
      @Override
      public Object call(final ArrayList<T> data) throws Exception {
        outstanding_executions.remove(QuerySplitter.this);
        callback(data_merger.merge(data, context, tracer_span),
            new ImmutableMap.Builder<String, String>()
              .put("status", "ok")
              .put("finalThread", Thread.currentThread().getName())
              .build());
        return null;
      }
    }
    
    /** Error catcher for each execution that will cancel the remaining 
     * executions. */
    class ErrCB implements Callback<Object, Exception> {
      @Override
      public Object call(final Exception ex) throws Exception {
        if (!completed.get()) {
          callback(ex);
          cancel();
        }
        return ex;
      }
    }
    
    /** Called on success to trigger the next sub query. */
    class DataCB implements Callback<T, T> {
      @Override
      public T call(final T arg) throws Exception {
        synchronized (QuerySplitter.this) {
          if (splits_index < query.subQueries().size()) {
            launchNext();
          }
        }
        return arg;
      }
    }
    
  }
  
  /**
   * Fetches the next executor from the executors array. This is higher than the
   * {@link QueryExecution} level so that if a single parent query is streaming
   * a lot of queries at this executor we can rotate through the executors for
   * all of the queries.
   * @return A non-null query executor to use for the next query.
   */
  private synchronized QueryExecutor<?> nextExecutor() {
    final QueryExecutor<?> executor = executors[executor_index++];
    if (executor_index >= executors.length) {
      executor_index = 0;
    }
    return executor;
  }
  
  @VisibleForTesting
  DataMerger<T> dataMerger() {
    return data_merger;
  }
  
  public static class Config<T> extends QueryExecutorConfig {
    private Class<T> type;
    private int parallel_executors;
    
    private Config(final Builder<T> builder) {
      type = builder.type;
      parallel_executors = builder.parallel_executors;
    }
    
    public static <T> Builder<T> newBuilder() {
      return new Builder<T>();
    }
    
    public static class Builder<T> {
      private Class<T> type;
      private int parallel_executors;
      
      /**
       * The class of the return type handled by the executor.
       * @param type A non-null class.
       * @return The builder.
       */
      public Builder<T> setType(final Class<T> type) {
        this.type = type;
        return this;
      }
      
      public Builder<T> setParallelExecutors(final int parallel_executors) {
        this.parallel_executors = parallel_executors;
        return this;
      }
      
      public Config<T> build() {
        return new Config<T>(this);
      }
    }
  }
}
