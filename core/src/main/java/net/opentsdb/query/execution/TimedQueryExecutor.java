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

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;
import com.google.common.hash.HashCode;
import com.stumbleupon.async.Callback;

import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import io.opentracing.Span;
import net.opentsdb.core.Const;
import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.query.BaseQueryNodeConfig;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.context.QueryContext;
import net.opentsdb.query.execution.graph.ExecutionGraphNode;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.stats.TsdbTrace;
import net.opentsdb.utils.JSON;

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
  private final long default_timeout;
  
  /**
   * Default ctor.
   * @param node The execution graph node with the config and graph.
   * @throws IllegalArgumentException if the node was null or the default config
   * was null or if the timeout was less than 1 millisecond.
   */
  @SuppressWarnings("unchecked")
  public TimedQueryExecutor(final ExecutionGraphNode node) {
    super(node);
//    if (node.getConfig() == null) {
//      throw new IllegalArgumentException("Default config cannot be null.");
//    }
//    if (node.graph() == null) {
//      throw new IllegalStateException("Execution graph cannot be null.");
//    }
//    if (((Config) node.getConfig()).timeout < 1) {
//      throw new IllegalArgumentException("Default timeout must be greater "
//          + "than zero.");
//    }
//    default_timeout = ((Config) node.getConfig()).timeout;
//    executor = (QueryExecutor<T>) 
//        node.graph().getDownstreamExecutor(node.getId());
//    if (executor == null) {
//      throw new IllegalStateException("No downstream executor found: " + this);
//    }
    executor = null;
    default_timeout = 0;
    registerDownstreamExecutor(executor);
  }

  @Override
  public QueryExecution<T> executeQuery(final QueryContext context,
                                        final TimeSeriesQuery query,
                                        final Span upstream_span) {
    if (completed.get()) {
      return new FailedQueryExecution<T>(query, new QueryExecutionException(
            "Timeout executor was already marked as completed: " + this, 
            500, query.getOrder()));
    }
    try {
      final TimedQuery timed_query = new TimedQuery(context, query);
      timed_query.execute(upstream_span);
      return timed_query;
    } catch (Exception e) {
      return new FailedQueryExecution<T>(query, new QueryExecutionException(
          "Unexpected exception executing query: " + this, 
          500, query.getOrder(), e));
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
    
    final QueryContext context;
    /**
     * Default ctor.
     * @param query A non-null query.
     */
    public TimedQuery(final QueryContext context,final TimeSeriesQuery query) {
      super(query);
      this.context = context;
      outstanding_executions.add(this);
    }
    
    void execute(final Span upstream_span) {
      if (context.getTracer() != null) {
        setSpan(context, 
            TimedQueryExecutor.this.getClass().getSimpleName(), 
            upstream_span,
            TsdbTrace.addTags(
                "order", Integer.toString(query.getOrder()),
                "query", JSON.serializeToString(query),
                "startThread", Thread.currentThread().getName()));
      }
      
      class ErrCB implements Callback<Object, Exception> {
        @Override
        public Object call(final Exception e) throws Exception {
          try {
            callback(e, 
                TsdbTrace.exceptionTags(e),
                TsdbTrace.exceptionAnnotation(e));
          } catch (IllegalStateException ex) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Lost race condition calling back with an "
                  + "exception: " + this);
            }
          } catch (Exception ex) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Unexpected exception sending exception upstream: " 
                  + this, ex);
            }
          }
          complete();
          return null;
        }
      }

      class SuccessCB implements Callback<Object, T> {
        @Override
        public Object call(final T obj) throws Exception {
          complete();
          callback(obj, TsdbTrace.successfulTags());
          return null;
        }
      }
      
      // run it!
      try {
        final QueryExecutorConfig override = 
            context.getConfigOverride(node.getId());
        long timeout = default_timeout;
        if (override != null) {
//          if (override instanceof Config) {
//            timeout = ((Config) override).timeout;
//          }
        }
        downstream = executor.executeQuery(context, query, upstream_span);
        downstream.deferred()
          .addCallback(new SuccessCB())
          .addErrback(new ErrCB());
        timer_timeout = context.getTimer()
            .newTimeout(this, timeout, TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        try {
          final Exception ex = new QueryExecutionException(
              "Unexpected exception executing query: " + this, 
              500, query.getOrder(), e);
          callback(ex,
              TsdbTrace.exceptionTags(ex),
              TsdbTrace.exceptionAnnotation(ex));
        } catch (IllegalStateException ex) {
          // lost race, don't care.
        } catch (Exception ex) {
          LOG.warn("Unexpected exception calling back execution with an "
              + "error: " + this, ex);
        }
        cancel();
        complete();
      }
    }

    @Override
    public void run(final Timeout timeout) throws Exception {
      if (completed.get()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Timeout executed after a successful execution: " + this);
        }
        return;
      }
      final Exception e = new QueryExecutionException(
          "Timed executor timed out " + this, 408, query.getOrder());
      try {
        callback(e,
            TsdbTrace.exceptionTags(e),
            TsdbTrace.exceptionAnnotation(e));
      } catch (IllegalStateException ex) {
        // lost the race, don't care
      } catch (Exception ex) {
        LOG.warn("Unexpected exception calling back execution with a "
            + "timeout: " + this, ex);
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
            final Exception e = new QueryExecutionException(
                "Query was cancelled upstream: " + this, 500, query.getOrder()); 
            callback(e, TsdbTrace.canceledTags(e));
          } catch (IllegalStateException ex) {
            // lost the race, don't care
          } catch (Exception ex) {
            LOG.warn("Unexpected exception calling back execution with a "
                + "cancelation: " + this, ex);
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
      outstanding_executions.remove(this);
    }
  }

  /**
   * The config for this executor.
   */
  @JsonInclude(Include.NON_NULL)
  @JsonDeserialize(builder = Config.Builder.class)
  public static class Config extends BaseQueryNodeConfig {
    /** The timeout in milliseconds. */
    private long timeout;
    
    /**
     * Default ctor.
     * @param builder A non-null builder.
     */
    protected Config(final Builder builder) {
      super(builder);
      timeout = builder.timeout;
    }
    
    /** @return The timeout in milliseconds. */ 
    public long getTimeout() {
      return timeout;
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
     * A builder that clones the given config.
     * @param config A non-null config to pull from.
     * @return A new builder populated with values from the config.
     */
    public static Builder newBuilder(final Config config) {
      return (Builder) new Builder()
          .setTimeout(config.timeout)
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
          && Objects.equal(timeout, config.timeout);
    }

    @Override
    public int hashCode() {
      return buildHashCode().asInt();
    }

    @Override
    public HashCode buildHashCode() {
      return Const.HASH_FUNCTION().newHasher()
          .putString(Strings.nullToEmpty(id), Const.UTF8_CHARSET)
          .putLong(timeout)
          .hash();
    }

    @Override
    public int compareTo(QueryNodeConfig config) {
      return ComparisonChain.start()
          .compare(id, config.getId(), Ordering.natural().nullsFirst())
          .compare(timeout, ((Config) config).timeout)
          .result();
    }
    
    /** The builder for TimedQueryExecutor configs. */
    public static class Builder extends BaseQueryNodeConfig.Builder {
      @JsonProperty
      private long timeout;
      
      /**
       * The timeout in milliseconds for the executor.
       * @param timeout A timeout in milliseconds.
       * @return The builder.
       */
      public Builder setTimeout(final long timeout) {
        this.timeout = timeout;
        return this;
      }
      
      /** @return A compiled Config object. */
      public Config build() {
        return new Config(this);
      }
    }
  }
}
