// This file is part of OpenTSDB.
// Copyright (C) 2016  The OpenTSDB Authors.
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

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.hash.HashCode;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import io.opentracing.Span;
import net.opentsdb.core.Const;
import net.opentsdb.core.DefaultRegistry;
import net.opentsdb.data.DataMerger;
import net.opentsdb.exceptions.QueryExecutionCanceled;
import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.query.BaseQueryNodeConfig;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.context.QueryContext;
import net.opentsdb.query.execution.graph.ExecutionGraphNode;
import net.opentsdb.query.plan.SplitMetricPlanner;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.stats.TsdbTrace;
import net.opentsdb.utils.JSON;

/**
 * An executor that takes {@link TimeSeriesQuery}s that have 1 or more child
 * queries with a single metric each (e.g. one returned by the 
 * {@link SplitMetricPlanner}. Each sub query is sent to an executor up to
 * {@link #default_parallel_executors}. If there are more sub queries than 
 * {@link #default_parallel_executors} the executor waits until one of the outstanding
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
  protected final int default_parallel_executors;
  
  /** The data merger used to merge results. */
  private DataMerger<T> default_data_merger;
  
  /** The downstream executor to pass sharded queries to. */
  protected QueryExecutor<T> executor;
  
  /**
   * Default ctor.
   * @param node A non null node to pull the ID and config from. 
   * @throws IllegalArgumentException if a config param was invalid.
   */
  @SuppressWarnings("unchecked")
  public MetricShardingExecutor(final ExecutionGraphNode node) {
    super(node);
//    if (node.getConfig() == null) {
//      throw new IllegalArgumentException("Config cannot be null.");
//    }
//    if (((Config) node.getConfig()).parallel_executors < 1) {
//      throw new IllegalArgumentException("Parallel executors must be one or "
//          + "greater.");
//    }
//    default_parallel_executors = ((Config) node.getConfig()).parallel_executors;
//    default_data_merger = (DataMerger<T>) ((DefaultRegistry) node.graph().tsdb()
//        .getRegistry()).getDataMerger(
//            ((Config) node.getConfig()).merge_strategy);
//    if (default_data_merger == null) {
//      throw new IllegalArgumentException("No data merger found for: " 
//          + ((Config) node.getConfig()).merge_strategy);
//    }
//    
//    executor = (QueryExecutor<T>) node.graph()
//        .getDownstreamExecutor(node.getId());
    default_parallel_executors = 0;
    if (executor == null) {
      throw new IllegalArgumentException("Downstream executor was null: " + this);
    }
    registerDownstreamExecutor(executor);
  }

  @Override
  public QueryExecution<T> executeQuery(final QueryContext context,
                                        final TimeSeriesQuery query,
                                        final Span upstream_span) {
    final SplitMetricPlanner plan = new SplitMetricPlanner(query);
    if (plan.getPlannedQuery().subQueries() == null || 
        plan.getPlannedQuery().subQueries().isEmpty()) {
      throw new IllegalArgumentException("Query didn't have any sub after "
          + "planning: " + plan.getPlannedQuery());
    }
    final QuerySplitter executor = new QuerySplitter(context, plan.getPlannedQuery());
    executor.executeQuery(upstream_span);
    return executor;
  }

  /** The execution for a specific query that handles rotating through executors. */
  private class QuerySplitter extends QueryExecution<T> {
    final QueryContext context;
    
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
    public QuerySplitter(final QueryContext context,final TimeSeriesQuery query) {
      super(query);
      this.context = context;
      outstanding_executions.add(this);
      this.query = query;
      executions = new QueryExecution[query.subQueries().size()];
    }
    
    QueryExecution<T> executeQuery(final Span upstream_span) {
      if (context.getTracer() != null) {
        setSpan(context, MetricShardingExecutor.this.getClass().getSimpleName(), 
            upstream_span,
            TsdbTrace.addTags(
                "order", Integer.toString(query.getOrder()),
                "query", JSON.serializeToString(query),
                "startThread", Thread.currentThread().getName()));
      }
      
      final int parallels;
//      final Config override = (Config) context.getConfigOverride(
//          node.getId());
//      if (override != null && override.getParallelExecutors() > 0) {
//        parallels = override.getParallelExecutors();
//      } else {
        parallels = default_parallel_executors;
//      }
      
      // locked here so that a query that returns BEFORE we fire the proper
      // amount doesn't increment the index on us.
      synchronized (this) {
        for (int i = 0; i < executions.length && i < parallels; i++) {
          if (!completed.get()) {
            launchNext();
          } else {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Possibly canceled or executed in a single thread. "
                  + "Exiting initial launch path.");
            }
            return this;
          }
        }
      }
      return this;
    }
    
    /**
     * <b>WARNING:</b> Make sure to synchronize on *this* before executing to
     * avoid a race.
     */
    private void launchNext() {
      int idx = splits_index++;
      final TimeSeriesQuery sub_query = query.subQueries().get(idx);
      executions[idx] = (QueryExecution<T>) 
          executor.executeQuery(context, sub_query, tracer_span);
      executions[idx].deferred()
                              .addCallback(new DataCB(idx))
                              .addErrback(new ErrCB(idx));
      if (idx + 1 >= query.subQueries().size()) {
        groupEm();
      }
    }
    
    @Override
    public void cancel() {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Cancelling query.");
      }
      if (!completed.get()) {
        try {
          final Exception e = new QueryExecutionCanceled(
              "Query was cancelled upstream: " + this, 400, query.getOrder());
          callback(e, TsdbTrace.canceledTags(e));
        } catch (IllegalStateException e) {
          // already called, don't care.
        } catch (Exception e) {
          LOG.warn("Exception thrown trying to callback on cancellation: " 
              + this, e);
        }
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Canceling but already called completed.");
        }
      }
      synchronized (this) {
        outstanding_executions.remove(this);
        // set to max to prevent anyone else running.
        splits_index = query.subQueries().size(); 
        for (final QueryExecution<T> execution : executions) {
          if (execution != null) {
            execution.cancel();
          }
        }
      }
    }
  
    /** Helper that groups the deferreds and adds the final callback. */
    private void groupEm() {
      final List<Deferred<T>> deferreds = 
          Lists.<Deferred<T>>newArrayListWithExpectedSize(executions.length);
      for (final QueryExecution<T> execution : executions) {
        if (execution == null) {
          continue;
        }
        deferreds.add(execution.deferred());
      }
      Deferred.group(deferreds).addCallback(new GroupCB());
    }
    
    /** The final class that will be called if all is good */
    class GroupCB implements Callback<Object, ArrayList<T>> {
      @Override
      public Object call(final ArrayList<T> data) throws Exception {
        outstanding_executions.remove(QuerySplitter.this);
        try {
          // TODO - override
          callback(default_data_merger.merge(data, context, tracer_span),
              TsdbTrace.successfulTags());
        } catch (IllegalStateException e) {
          LOG.warn("Group callback tried to return results despite being "
              + "called: " + this);
        } catch (Exception e) {
          try {
            final QueryExecutionException ex = new QueryExecutionException(
                "Unexpected exception", 500, query.getOrder(), e);
            callback(ex, 
                TsdbTrace.exceptionTags(ex),
                TsdbTrace.exceptionAnnotation(ex));
          } catch (IllegalStateException ex) {
            // already called, it's ok.
          } catch (Exception ex) {
            LOG.warn("Failed callback: " + this, ex);
          }
        }
        return null;
      }
    }
    
    /** Error catcher for each execution that will cancel the remaining 
     * executions. */
    class ErrCB implements Callback<Object, Exception> {
      final int index;
      ErrCB(final int index) {
        this.index = index;
      }
      @Override
      public Object call(final Exception ex) throws Exception {
        if (!completed.get()) {
          try {
            callback(ex,
                TsdbTrace.exceptionTags(ex),
                TsdbTrace.exceptionAnnotation(ex));
          } catch (IllegalStateException e) {
            // already called, it's ok.
          } catch (Exception e) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Unexected exception triggering callback on "
                  + "exception: " + this, e);
            }
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug("Exception on index " + index, ex);
          }
          cancel();
        } else {
          // cancels bubble up here so don't pollute the logs.
        }
        return ex;
      }
    }
    
    /** Called on success to trigger the next sub query. */
    class DataCB implements Callback<T, T> {
      final int index;
      DataCB(final int index) {
        this.index = index;
      }
      @Override
      public T call(final T arg) throws Exception {
        if (!completed.get()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Received data on index " + index);
          }
          synchronized (QuerySplitter.this) {
            if (splits_index < query.subQueries().size()) {
              launchNext();
            }
          }
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Successful response from index " + index 
                + " but we've been canceled.");
          }
        }
        return arg;
      }
    }
    
  }
  
  @VisibleForTesting
  DataMerger<T> dataMerger() {
    return default_data_merger;
  }
  
  @JsonInclude(Include.NON_NULL)
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonDeserialize(builder = Config.Builder.class)
  public static class Config extends BaseQueryNodeConfig {
    private int parallel_executors;
    private String merge_strategy;

    /**
     * Default ctor.
     * @param builder A non-null builder.
     */
    private Config(final Builder builder) {
      super(builder);
      parallel_executors = builder.parallelExecutors;
      merge_strategy = builder.mergeStrategy;
    }
    
    /** @return The number of executions to run in parallel. */
    public int getParallelExecutors() {
      return parallel_executors;
    }
    
    /** @return The merge strategy to use for data. */
    public String getMergeStrategy() {
      return merge_strategy;
    }
    
    @Override
    public String getId() {
      // TODO Auto-generated method stub
      return null;
    }
    
    /** @return A new builder. */
    public static Builder newBuilder() {
      return new Builder();
    }
    
    /**
     * @param config A non-null builcer to pull from.
     * @return A cloned builder.
     */
    public static Builder newBuilder(final Config config) {
      return (Builder) new Builder()
          .setParallelExecutors(config.parallel_executors)
          .setMergeStrategy(config.merge_strategy)
          .setId(config.id);
    }
    
    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final Config config = (Config) o;
      return Objects.equal(id, config.id)
          && Objects.equal(parallel_executors, config.parallel_executors)
          && Objects.equal(merge_strategy, config.merge_strategy);
    }

    @Override
    public int hashCode() {
      return buildHashCode().asInt();
    }

    @Override
    public HashCode buildHashCode() {
      return Const.HASH_FUNCTION().newHasher()
        .putString(Strings.nullToEmpty(id), Const.UTF8_CHARSET)
        .putLong(parallel_executors)
        .putString(Strings.nullToEmpty(merge_strategy), Const.UTF8_CHARSET)
        .hash();
    }

    @Override
    public int compareTo(final QueryNodeConfig other) {
      if (!(other instanceof Config)) {
        return -1;
      }
      final Config config = (Config) other;
      return ComparisonChain.start()
          .compare(id, config.id, 
              Ordering.natural().nullsFirst())
          .compare(parallel_executors, ((Config) config).parallel_executors)
          .compare(merge_strategy, ((Config) config).merge_strategy, 
              Ordering.natural().nullsFirst())
          .result();
    }
    
    public static class Builder extends BaseQueryNodeConfig.Builder {
      @JsonProperty
      private int parallelExecutors;
      @JsonProperty
      private String mergeStrategy = "largest";
      
      /**
       * How many executors to run in parallel.
       * @param parallel_executors A value greater than zero.
       * @return The builder.
       */
      public Builder setParallelExecutors(final int parallel_executors) {
        this.parallelExecutors = parallel_executors;
        return this;
      }
      
      /**
       * The data merge strategy to use for merging data from different clusters.
       * @param merge_strategy A non-null merge strategy.
       * @return The builder.
       */
      public Builder setMergeStrategy(final String merge_strategy) {
        this.mergeStrategy = merge_strategy;
        return this;
      }
      
      /** @return An instantiated config if validation passes. */
      public Config build() {
        return new Config(this);
      }
    }
    
  }
}
