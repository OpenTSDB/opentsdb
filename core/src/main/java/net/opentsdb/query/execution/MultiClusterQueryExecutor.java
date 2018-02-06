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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.hash.HashCode;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import io.opentracing.Span;
import net.opentsdb.core.Const;
import net.opentsdb.core.DefaultRegistry;
import net.opentsdb.data.DataMerger;
import net.opentsdb.exceptions.QueryExecutionCanceled;
import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.query.context.QueryContext;
import net.opentsdb.query.execution.cluster.ClusterConfig;
import net.opentsdb.query.execution.graph.ExecutionGraphNode;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.stats.TsdbTrace;
import net.opentsdb.utils.JSON;

/**
 * An executor that uses the {@link ClusterConfig} to send the same query to 
 * multiple clusters (in dual-write situations) and merges the results using a 
 * {@link DataMerger} for the given type.
 * <p>
 * For multi-cluster queries, we fire the same query off to each cluster and 
 * wait for a response from every cluster. If at least one cluster returns
 * a valid result, then that result will be returned via 
 * {@link #executeQuery(QueryContext, TimeSeriesQuery, Span)}.
 * However if all clusters return an exception, then the exceptions are
 * packed into a {@link QueryExecutionException} and the highest status
 * code from exceptions (assuming each one returns a QueryExecutionException)
 * is used. (500 if they weren't QueryExecutionExceptions.)
 * 
 * <p>
 * This implementation does not provide timeout handling. Instead, make sure
 * to wrap the {@link #downstreamExecutors()} with a timeout executor
 * if timeout handling is desired.
 * 
 * @param <T>
 */
public class MultiClusterQueryExecutor<T> extends QueryExecutor<T> {
  private static final Logger LOG = LoggerFactory.getLogger(
      MultiClusterQueryExecutor.class);

  /** The data merger used to merge results. */
  private DataMerger<T> default_data_merger;
  
  /** The cluster graph used with this executor. */
  private final ClusterConfig cluster_graph;
  
  /** The list of executors keyed on cluster IDs. */
  private final Map<String, QueryExecutor<T>> executors;
  
  /** Optional timeout in ms for alternate clusters. */
  private final long default_timeout;

  /**
   * Alternate CTor that sets a timeout that, when the first positive response
   * is received from a cluster, initiates a timer task that will merge and 
   * forward the query results upstream regardless of the state of remaining
   * clusters.
   * 
   * @param node A non null node to pull the ID and config from. 
   * @throws IllegalArgumentException if a config param was invalid.
   */
  @SuppressWarnings("unchecked")
  public MultiClusterQueryExecutor(final ExecutionGraphNode node) {
    super(node);
    if (node.getDefaultConfig() == null) {
      throw new IllegalArgumentException("Config cannot be null.");
    }
    if (Strings.isNullOrEmpty(((Config) node.getDefaultConfig()).cluster_config)) {
      throw new IllegalArgumentException("Cluster config cannot be null.");
    }
    cluster_graph = ((DefaultRegistry) node.graph().tsdb().getRegistry()).getClusterConfig(
        ((Config) node.getDefaultConfig()).cluster_config);
    if (cluster_graph == null) {
      throw new IllegalArgumentException("No cluster found for: " 
          + ((Config) node.getDefaultConfig()).cluster_config);
    }
    if (((Config) node.getDefaultConfig()).timeout < 0) {
      throw new IllegalArgumentException("Timeout cannot be negative.");
    }
    if (cluster_graph.clusters().isEmpty()) {
      throw new IllegalArgumentException("The cluster graph cannot be empty.");
    }
    executors = Maps.newHashMapWithExpectedSize(cluster_graph.clusters().size());
    for (final String cluster_id : cluster_graph.clusters().keySet()) {
      final QueryExecutor<T> executor = (QueryExecutor<T>) 
          cluster_graph.getSinkExecutor(cluster_id);
      if (executor == null) {
        throw new IllegalArgumentException("Factory returned a null executor "
            + "for cluster: " + cluster_id);
      }
      executors.put(cluster_id, executor);
      registerDownstreamExecutor(executor);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Instantiated executor sink for cluster: " + cluster_id);
      }
    }
    default_timeout = ((Config) node.getDefaultConfig()).timeout;
    default_data_merger = (DataMerger<T>) ((DefaultRegistry) node.graph().tsdb()
        .getRegistry()).getDataMerger(
            ((Config) node.getDefaultConfig()).merge_strategy);
    if (default_data_merger == null) {
      throw new IllegalArgumentException("No data merger found for: " 
          + ((Config) node.getDefaultConfig()).merge_strategy);
    }
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override
  public QueryExecution<T> executeQuery(final QueryContext context,
                                        final TimeSeriesQuery query, 
                                        final Span upstream_span) {
    if (completed.get()) {
      return new FailedQueryExecution(query,
          new QueryExecutionException(
          "Executor has been cancelled", 410, query.getOrder()));
    }
    
    try {
      final QueryToClusterSplitter execution = new QueryToClusterSplitter(
          context, query);
      outstanding_executions.add(execution);
      return execution.executeQuery(upstream_span);
    } catch (Exception e) {
      return new FailedQueryExecution(query, new QueryExecutionException(
          "Unexpected exception executing query: " + this, 
          500, query.getOrder(), e));
    }
  }

  /** State class for a specific query that waits for both clusters to complete
   * and merges the response. */
  class QueryToClusterSplitter extends QueryExecution<T> implements TimerTask {
    /** The query context. */
    private final QueryContext context;
    
    /** Potential override or the default config. */
    private final QueryExecutorConfig config;
    
    /** A list of remote exceptions */
    private final Exception[] remote_exceptions;
    
    /** The list of outstanding executions so we can cancel them if needed. */
    @VisibleForTesting
    private final QueryExecution<T>[] executions;
    
    /** The results populated by the group by. */
    private final List<T> results;
    
    /** Flag set when starting the timer. */
    private final AtomicBoolean timer_started;
    
    /** The list of clusters to call into for this particular query. */
    private final List<String> clusters;
    
    /** The potentially overriden timeout in ms. */
    private final long timeout;
    
    /** The timeout from the Timer if timeouts are enabled. */ 
    private Timeout timer_timeout;
        
    /**
     * Default ctor.
     * @param query A non-null query.
     * @throws IllegalStateException if the remote context cluster call fails.
     */
    @SuppressWarnings("unchecked")
    public QueryToClusterSplitter(final QueryContext context, 
                                  final TimeSeriesQuery query) {
      super(query);
      
      final QueryExecutorConfig override = 
          context.getConfigOverride(node.getExecutorId());
      if (override != null) {
        config = override;
      } else {
        config = node.getDefaultConfig();
      }
      clusters = cluster_graph.setupQuery(context, 
          ((Config) config).cluster_override);
      
      this.context = context;
      remote_exceptions = new Exception[clusters.size()];
      executions = new QueryExecution[clusters.size()];
      results = Lists.newArrayListWithExpectedSize(clusters.size());
      for (int i = 0; i < clusters.size(); i++) {
        results.add(null);
      }
      if (((Config) config).timeout > 0 || default_timeout > 0) {
        timer_started = new AtomicBoolean();
        if (((Config) config).timeout > 0) {
          timeout = ((Config) config).timeout;
        } else {
          timeout = default_timeout;
        }
      } else {
        timer_started = null;
        timeout = 0;
      }
    }
    
    /**
     * Executes the query, looking for overrides in the context.
     * @param upstream_span An optional tracer span.
     * @return The split query execution.
     */
    QueryExecution<T> executeQuery(final Span upstream_span) {
      if (context.getTracer() != null) {
        setSpan(context, 
            MultiClusterQueryExecutor.this.getClass().getSimpleName(), 
            upstream_span,
            TsdbTrace.addTags(
                "order", Integer.toString(query.getOrder()),
                "query", JSON.serializeToString(query),
                "startThread", Thread.currentThread().getName()));
      }
      
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
            results.set(idx, data);
            return data;
          }
        }
        
        // execute the query on each remote and add an ErrCB to capture badness.
        int i = 0;
        for (final String cluster : clusters) {
          executions[i] = (QueryExecution<T>) executors.get(cluster)
              .executeQuery(context, query, tracer_span);
          if (executions[i] == null) {
            try {
              final Exception ex = new QueryExecutionException(
                  "Cluster " + cluster + " was not found in the cluster map.", 
                  400, query.getOrder());
              callback(ex,
                  TsdbTrace.exceptionTags(ex),
                  TsdbTrace.exceptionAnnotation(ex));
            } catch (IllegalArgumentException ex) {
              // lost race, no prob.
            } catch (Exception ex) {
              LOG.warn("Failed to complete callback due to unexepcted "
                  + "exception: " + this, ex);
            }
          }
          
          deferreds.add(executions[i].deferred()
              .addErrback(new ErrCB(i)));
          if (timeout > 0) {
            executions[i].deferred().addCallback(new TimerStarter(i));
          }
          i++;
        }
        
        /** Callback that either merges good results and executes the callback or
         * merges the exceptions and executes the callback. */
        class GroupCB implements Callback<Object, ArrayList<T>> {
          @Override
          public Object call(final ArrayList<T> data) throws Exception {
            try {
              if (completed.get()) {
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Splitter was already cancelled but we received "
                      + "a result: " + this);
                }
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
              int valid = 0;
              for (final T result : data) {
                if (result != null) {
                  valid++;
                }
              }
              
              // we have at least one good result so return it.
              if (valid > 0) {
                try {
                  callback(default_data_merger.merge(data, context, tracer_span),
                      TsdbTrace.successfulTags());
                } catch (IllegalArgumentException e) {
                  if (LOG.isDebugEnabled()) {
                    LOG.debug("Lost race returning good data on execution: " 
                        + this);
                  }
                } catch (Exception e) {
                  try {
                    final QueryExecutionException ex = 
                        new QueryExecutionException("Unexpected exception "
                            + "calling callback for execution: " + this, 500,
                            query.getOrder(), e);
                    callback(ex, TsdbTrace.exceptionTags(ex),
                        TsdbTrace.exceptionAnnotation(ex));
                  } catch (IllegalArgumentException ex) {
                    // lost race, no prob.
                  } catch (Exception ex) {
                    LOG.warn("Failed to complete callback due to unexepcted "
                        + "exception: " + this, ex);
                  }
                }
                return null;
              }
              
              // we don't have good data so we need to bubble up an exception.
              int status = 0;
              for (int i = 0; i < remote_exceptions.length; i++) {
                if (remote_exceptions[i] != null && 
                    remote_exceptions[i] instanceof QueryExecutionException) {
                  final QueryExecutionException e = 
                      (QueryExecutionException) remote_exceptions[i];
                  if (e.getStatusCode() > status) {
                    status = e.getStatusCode();
                  }
                }
              }
              final Exception e = new QueryExecutionException(
                  "One or more of the cluster sources had an exception", 
                  (status == 0 ? 500 : status),
                  query.getOrder(),
                  Lists.newArrayList(remote_exceptions));
              try {
                callback(e,
                    TsdbTrace.exceptionTags(e),
                    TsdbTrace.exceptionAnnotation(e));
              } catch (IllegalArgumentException ex) {
                // lost race, no prob.
              } catch (Exception ex) {
                LOG.warn("Failed to complete callback due to unexepcted "
                    + "exception: " + this, ex);
              }
              return null;
            } catch (Exception e) {
              try {
                final QueryExecutionException ex = new QueryExecutionException(
                    "Unexpected exception", 500, query.getOrder(), e);
                callback(ex, 
                    TsdbTrace.exceptionTags(e),
                    TsdbTrace.exceptionAnnotation(e));
              } catch (IllegalArgumentException ex) {
                // lost race, no prob.
              } catch (Exception ex) {
                LOG.warn("Failed to complete callback due to unexepcted "
                    + "exception: " + this, ex);
              }
              return null;
            } finally {
              outstanding_executions.remove(QueryToClusterSplitter.this);
            }
          }
        }
        
        Deferred.group(deferreds).addCallback(new GroupCB());
        return this;
      } catch (Exception e) {
        try {
          final Exception ex = new QueryExecutionException(
              "Unexpected exception executing queries downstream.", 
              500, query.getOrder(), e);
          callback(ex,
              TsdbTrace.exceptionTags(ex),
              TsdbTrace.exceptionAnnotation(ex));
        } catch (IllegalArgumentException ex) {
          // lost race, no prob.
        } catch (Exception ex) {
          LOG.warn("Failed to complete callback due to unexepcted "
              + "exception: " + this, ex);
        }
        outstanding_executions.remove(QueryToClusterSplitter.this);
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
      if (!completed.get()) {
        try {
          final Exception e = new QueryExecutionCanceled(
              "Query was cancelled upstream: " + this, 400, query.getOrder());
          callback(e, TsdbTrace.canceledTags(e));
        } catch (IllegalArgumentException ex) {
          // lost race, no prob.
        } catch (Exception ex) {
          LOG.warn("Failed to complete callback due to unexepcted "
              + "exception: " + this, ex);
        }
      }
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
      }
      outstanding_executions.remove(this);
    }

    @Override
    public void run(final Timeout ignored) throws Exception {
      synchronized (this) {
        timer_timeout = null;
      }
      try {
        callback(default_data_merger.merge(results, context, tracer_span));
      } catch (IllegalArgumentException ex) {
        // lost race, no prob.
      } catch (Exception ex) {
        LOG.warn("Failed to complete callback due to unexepcted "
            + "exception: " + this, ex);
      }
      cancel();
    }
  
    @VisibleForTesting
    QueryExecution<T>[] executions() {
      return executions;
    }
  }

  @VisibleForTesting
  DataMerger<T> dataMerger() {
    return default_data_merger;
  }

  @VisibleForTesting
  Map<String, QueryExecutor<T>> executors() {
    return executors;
  }
  
  /**
   * The config for this executor.
   */
  @JsonInclude(Include.NON_NULL)
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonDeserialize(builder = Config.Builder.class)
  public static class Config extends QueryExecutorConfig {
    private long timeout;
    private String merge_strategy;
    private String cluster_config;
    private String cluster_override;
    
    /**
     * Default ctor.
     * @param builder A non-null builder.
     */
    private Config(final Builder builder) {
      super(builder);
      timeout = builder.timeout;
      merge_strategy = builder.mergeStrategy;
      cluster_config = builder.clusterConfig;
      cluster_override = builder.clusterOverride;
    }
    
    /** @return An optional timeout in milliseconds. */
    public long getTimeout() {
      return timeout;
    }
    
    /** @return The merge strategy to use for data. */
    public String getMergeStrategy() {
      return merge_strategy;
    }
    
    /** @return The cluster config name to pull from the registry. */
    public String getClusterConfig() {
      return cluster_config;
    }
    
    /** @return An optional cluster override ID. */
    public String getClusterOverride() {
      return cluster_override;
    }
    
    /** @return A new builder. */
    public static Builder newBuilder() {
      return new Builder();
    }
    
    /**
     * @param config A non-null builder to pull from.
     * @return A cloned builder.
     */
    public static Builder newBuilder(final Config config) {
      return (Builder) new Builder()
          .setTimeout(config.timeout)
          .setMergeStrategy(config.merge_strategy)
          .setClusterConfig(config.cluster_config)
          .setClusterOverride(config.cluster_override)
          .setExecutorId(config.executor_id)
          .setExecutorType(config.executor_type);
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
      return Objects.equal(executor_id, config.executor_id)
          && Objects.equal(executor_type, config.executor_type)
          && Objects.equal(timeout, config.timeout)
          && Objects.equal(merge_strategy, config.merge_strategy)
          && Objects.equal(cluster_config, config.cluster_config)
          && Objects.equal(cluster_override, config.cluster_override);
    }

    @Override
    public int hashCode() {
      return buildHashCode().asInt();
    }

    @Override
    public HashCode buildHashCode() {
      return Const.HASH_FUNCTION().newHasher()
          .putString(Strings.nullToEmpty(executor_id), Const.UTF8_CHARSET)
          .putString(Strings.nullToEmpty(executor_type), Const.UTF8_CHARSET)
          .putLong(timeout)
          .putString(Strings.nullToEmpty(merge_strategy), Const.UTF8_CHARSET)
          .putString(Strings.nullToEmpty(cluster_config), Const.UTF8_CHARSET)
          .putString(Strings.nullToEmpty(cluster_override), Const.UTF8_CHARSET)
          .hash();
    }

    @Override
    public int compareTo(QueryExecutorConfig config) {
      return ComparisonChain.start()
          .compare(executor_id, config.executor_id, 
              Ordering.natural().nullsFirst())
          .compare(executor_type, config.executor_type, 
              Ordering.natural().nullsFirst())
          .compare(timeout, ((Config) config).timeout)
          .compare(merge_strategy, ((Config) config).merge_strategy, 
              Ordering.natural().nullsFirst())
          .compare(cluster_config, ((Config) config).cluster_config, 
              Ordering.natural().nullsFirst())
          .compare(cluster_override, ((Config) config).cluster_override, 
              Ordering.natural().nullsFirst())
          .result();
    }
    
    public static class Builder extends QueryExecutorConfig.Builder {
      @JsonProperty
      private long timeout;
      @JsonProperty
      private String mergeStrategy = "largest";
      @JsonProperty
      private String clusterConfig;
      @JsonProperty
      private String clusterOverride;
      
      /**
       * An optional timeout in milliseconds for the alternate clusters.
       * @param timeout A timeout in milliseconds or 0 to disable.
       * @return The builder.
       */
      public Builder setTimeout(final long timeout) {
        this.timeout = timeout;
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
      
      /**
       * @param clusterConfig A non-null cluster config registered with the TSD.
       * @return The builder.
       */
      public Builder setClusterConfig(final String clusterConfig) {
        this.clusterConfig = clusterConfig;
        return this;
      }
      
      /**
       * @param clusterOverride An optional cluster override associated with
       * the cluster config.
       * @return The builder.
       */
      public Builder setClusterOverride(final String clusterOverride) {
        this.clusterOverride = clusterOverride;
        return this;
      }
      
      /** @return An instantiated config. */
      public Config build() {
        return new Config(this);
      }
    }

  }
}
