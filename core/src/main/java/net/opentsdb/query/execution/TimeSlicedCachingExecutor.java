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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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

import io.opentracing.Span;
import net.opentsdb.core.Const;
import net.opentsdb.core.DefaultRegistry;
import net.opentsdb.exceptions.QueryExecutionCanceled;
import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.query.context.QueryContext;
import net.opentsdb.query.execution.cache.QueryCachePlugin;
import net.opentsdb.query.execution.cache.TimeSeriesCacheKeyGenerator;
import net.opentsdb.query.execution.graph.ExecutionGraphNode;
import net.opentsdb.query.execution.serdes.TimeSeriesSerdes;
import net.opentsdb.query.plan.QueryPlannnerFactory;
import net.opentsdb.query.plan.QueryPlanner;
import net.opentsdb.query.plan.TimeSlicedQueryPlanner;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.query.pojo.Timespan;
import net.opentsdb.stats.TsdbTrace;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.JSON;

/**
 * A caching executor for {@link TimeSeriesQuery}s that will use a 
 * {@link TimeSlicedQueryPlanner} to slice the query into blocks of time. The
 * blocks are fetched from cache and for each block that was missed, a query is
 * sent downstream for data. Successful downstream queries will then populate
 * the cache with their blocks. 
 * <p>
 * Consecutive missed blocks are merged into a single downstream query to avoid
 * sending many small queries. The results are then parsed into blocks before
 * caching.
 * <p>
 * <b>Exception Handling:</b> If calls to the cache fail or return exceptions,
 * then the downstream queries are executed to complete the original query. If
 * individual blocks returned by the cache are unable to be deserialized (e.g.
 * they were from a different version or are simply corrupted) then downstream
 * queries are issued for those blocks. And if one or more of the downstream
 * queries throw an exception then the entire query is canceled and the exception
 * returned upstream.
 * <p>
 * Once the caching plugin and serdes modules are set via the default config,
 * query time overridden settings are ignored. (TODO - should we allow?)
 * <p>
 * TODO - Right now the population of the cache is performed serially with the 
 * request so that queries are blocked until the slicing and writing to cache
 * are complete. We should handle that out of band (even though it may mean a
 * few more cache re-writes). 
 * TODO - Load the key gen from registry.
 *
 * @param <T> The type of data returned by the executor.
 * 
 * @since 3.0
 */
public class TimeSlicedCachingExecutor<T> extends QueryExecutor<T> {
  private static final Logger LOG = LoggerFactory.getLogger(
      TimeSlicedCachingExecutor.class);
  
  /** The downstream executor that queries are passed to. */
  private final QueryExecutor<T> executor;
  
  /** The cache plugin to use. */
  private final QueryCachePlugin plugin;
  
  /** The serdes class to use. */
  private final TimeSeriesSerdes serdes;
  
  /** A key generator used for reading and writing the cache data. */
  private final TimeSeriesCacheKeyGenerator key_generator;
  
  /**
   * Default ctor.
   * @param node A non-null graph node to pull the config from.
   */
  @SuppressWarnings("unchecked")
  public TimeSlicedCachingExecutor(final ExecutionGraphNode node) {
    super(node);
    if (node.getDefaultConfig() == null) {
      throw new IllegalArgumentException("Config cannot be null.");
    }
    
    plugin = (QueryCachePlugin) 
        node.graph().tsdb().getRegistry().getPlugin(
            QueryCachePlugin.class, 
            ((Config) node.getDefaultConfig()).cache_id);
    if (plugin == null) {
      throw new IllegalArgumentException("Unable to find a caching plugin "
          + "for ID: " + ((Config) node.getDefaultConfig()).cache_id);
    }
    serdes = (TimeSeriesSerdes) ((DefaultRegistry) node.graph().tsdb()
        .getRegistry()).getSerdes(
            ((Config) node.getDefaultConfig()).serdes_id);
    if (serdes == null) {
      throw new IllegalArgumentException("Unable to find a serdes implementation "
          + "for ID: " + ((Config) node.getDefaultConfig()).serdes_id);
    }
    if (Strings.isNullOrEmpty(((Config) node.getDefaultConfig()).getPlannerId())) {
      throw new IllegalArgumentException("Default planner ID must be non-empty.");
    }
    executor = (QueryExecutor<T>) node.graph()
        .getDownstreamExecutor(node.getExecutorId());
    if (executor == null) {
      throw new IllegalArgumentException("Unable to find a downstream executor.");
    }
    registerDownstreamExecutor(executor);
    key_generator = (TimeSeriesCacheKeyGenerator) 
        node.graph().tsdb().getRegistry().getPlugin(
            TimeSeriesCacheKeyGenerator.class, 
            ((Config) node.getDefaultConfig()).getKeyGeneratorId());
    if (key_generator == null) {
      throw new IllegalArgumentException("Unable to find a key generator "
          + "for ID: " + ((Config) node.getDefaultConfig()).getKeyGeneratorId());
    }
  }

  @Override
  public QueryExecution<T> executeQuery(QueryContext context,
      TimeSeriesQuery query, Span upstream_span) {
    final LocalExecution ex = new LocalExecution(context, query, upstream_span);
    ex.execute();
    return ex;
  }

  /** Local execution class. */
  class LocalExecution extends QueryExecution<T> {
    /** The default or overridden config. */
    private final Config config;
    
    /** The query context. */
    private final QueryContext context;
    
    /** The array list of results, may contain nulls. */
    private final List<T> results;
    
    /** The list of hash keys per time slice used for cache lookup and write. */ 
    final byte[][] keys;
    
    /** The cache execution to track. */
    private QueryExecution<byte[][]> cache_execution;
    
    /** The downstream executions to wait on (or cancel). */
    private List<QueryExecution<T>> downstreams;
    
    /** The query planner used to generate slices. */
    private final TimeSlicedQueryPlanner<T> planner;
    
    /**
     * Default ctor
     * @param context The query context.
     * @param query The query itself.
     * @param upstream_span An optional tracer span.
     */
    @SuppressWarnings("unchecked")
    public LocalExecution(final QueryContext context, 
                          final TimeSeriesQuery query, 
                          final Span upstream_span) {
      super(query);
      this.context = context;
      outstanding_executions.add(this);
      
      final QueryExecutorConfig override = 
          context.getConfigOverride(node.getExecutorId());
      if (override != null) {
        config = (Config) override;
      } else {
        config = (Config) node.getDefaultConfig();
      }
      
      final String plan_id = Strings.isNullOrEmpty(config.getPlannerId()) ? 
          ((Config) node.getDefaultConfig()).getPlannerId() : config.getPlannerId();
      final QueryPlannnerFactory<?> plan_factory = ((DefaultRegistry) context.getTSDB().getRegistry())
          .getQueryPlanner(plan_id);
      if (plan_factory == null) {
        throw new IllegalArgumentException("No such query plan factory: " 
            + config.getPlannerId());
      }
      final QueryPlanner<?> temp_planner = plan_factory.newPlanner(query);
      if (temp_planner == null) {
        throw new IllegalStateException("Plan factory returned a null planner: " 
            + plan_factory);
      }
      if (!(temp_planner instanceof TimeSlicedQueryPlanner)) {
        throw new IllegalStateException("Planner was of the wrong type: " 
            + temp_planner.getClass().getCanonicalName() + " while it must be "
                + "a TimeSlicedQueryPlanner.");
      }
      planner = (TimeSlicedQueryPlanner<T>) temp_planner;
      
      if (context.getTracer() != null) {
        setSpan(context, 
            TimeSlicedCachingExecutor.this.getClass().getSimpleName(), 
            upstream_span,
            TsdbTrace.addTags(
                "order", Integer.toString(query.getOrder()),
                "query", JSON.serializeToString(query),
                "startThread", Thread.currentThread().getName()));
      }
      
      results = Lists.newArrayListWithCapacity(planner.getTimeRanges().length);
      for (int i = 0; i < planner.getTimeRanges().length; i++) {
        results.add(null);
      }
      keys = key_generator.generate(query, planner.getTimeRanges());
    }
    
    /** Runs the query. */
    public void execute() {
      
      /** The cache response callback that will continue downstream if needed. */
      class CacheCB implements Callback<Object, byte[][]> {
        @Override
        public Object call(final byte[][] cache_data) throws Exception {
          int last_null = -1;
          long bytes = 0;
          final List<Deferred<Object>> deferreds = Lists.newArrayList();
          final Span cache_span;
          if (context.getTracer() != null) {
            cache_span = context.getTracer()
                .buildSpan("cacheResponse")
                .asChildOf(tracer_span)
                .start();
          } else {
            cache_span = null;
          }
          
          if (cache_data == null) {
            LOG.warn("Cache returned a null set, which shouldn't happen.");
            last_null = 0;
          } else {
            for (int i = 0; i < cache_data.length; i++) {
              if (cache_data[i] != null) {
                bytes += cache_data[i].length;
                try {
//                  results.set(i, 
//                      serdes.deserialize(null, new ByteArrayInputStream(cache_data[i])));
                } catch (Exception e) {
                  LOG.warn("Exception deserializing cache object at index: " + i, e);
                  fireDownstream(deferreds, i, i);
                  continue;
                }
                
                if (last_null >= 0) {
                  // we had at least one null so run the merged query
                  fireDownstream(deferreds, last_null, i - 1);
                  last_null = -1;
                }
              } else if (last_null < 0) {
                last_null = i;
              }
            }
          }
          
          // pick up any tailing nulls after the iteration.
          if (last_null >= 0) {
            fireDownstream(deferreds, last_null, 
                planner.getTimeRanges().length - 1);
          }
          
          if (cache_span != null) {
            // make a pretty annotation
            int hits = 0;
            final Map<String, Boolean> cache_map = Maps.newTreeMap();
            for (int i = 0; i < cache_data.length; i++) {
              cache_map.put(
                  planner.getTimeRanges()[i][0].msEpoch() + "-" + 
                  planner.getTimeRanges()[i][1].msEpoch(),
                  cache_data[i] == null ? false : true);
              if (cache_data[i] != null) {
                ++hits;
              }
            }
            cache_span.setTag("status", "OK");
            cache_span.setTag("finalThread", Thread.currentThread().getName());
            cache_span.setTag("bytes", Long.toString(bytes));
            cache_span.setTag("cacheBySlice", JSON.serializeToString(cache_map));
            cache_span.setTag("hitPercent", 
                (((double) hits / (double) cache_data.length) * 100));
            cache_span.finish();
          }
          
          // if there weren't any downstreams fired, then we can callback now!
          if (downstreams == null || downstreams.isEmpty()) {
            new CompletedCB().call(null);
          } else {
            Deferred.group(deferreds)
              .addCallback(new CompletedCB())
              .addErrback(new ErrorCB(false));
          }
          return null;
        }
      }
      
      if (config.getBypass()) {
        try {
          new CacheCB().call(new byte[planner.getTimeRanges().length][]);
        } catch (Exception e1) {
          final Exception ex = new QueryExecutionException(
            "Unexpected exception calling cache callback after "
                + "cache failure: " + this, 500, query.getOrder(), e1);
          handleException(ex);
        }
      } else {
        // start the callback chain by fetching from the cache first. If an exception
        // is thrown immediately, catch and continue downstream.
        try {
          cache_execution = plugin.fetch(context, keys, tracer_span);
          cache_execution.deferred()
            .addCallback(new CacheCB())
            .addErrback(new ErrorCB(false));
        } catch (Exception e) {
          LOG.error("Unexpected cache exception. Falling back downstream: " 
              + this, e);
          try {
            new CacheCB().call(new byte[planner.getTimeRanges().length][]);
          } catch (Exception e1) {
            final Exception ex = new QueryExecutionException(
              "Unexpected exception calling cache callback after "
                  + "cache failure: " + this, 500, query.getOrder(), e1);
            handleException(ex);
          }
        }
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
      outstanding_executions.remove(LocalExecution.this);
      if (cache_execution != null) {
        try {
          cache_execution.cancel();
        } catch (Exception e) {
          LOG.warn("Failed canceling cache execution: " + cache_execution, e);
        }
      }
      if (downstreams != null) {
        for (final QueryExecution<T> downstream : downstreams) {
          try {
            downstream.cancel();
          } catch (Exception e) {
            LOG.warn("Failed canceling downstream execution: " + downstream, e);
          }
        }
      }
    }
    
    /**
     * Helper that removes the execution from the outstanding set and calls back
     * with the exception. Also cancels outstanding downstream calls.
     * @param e The exception to return.
     */
    private void handleException(final Exception e) {
      outstanding_executions.remove(LocalExecution.this);
      try {
        callback(e, 
            TsdbTrace.exceptionTags(e),
            TsdbTrace.exceptionAnnotation(e));
        cancel();
      } catch (IllegalStateException ex) { 
        // Safe to ignore as it's already been called.
      } catch (Exception ex) {
        LOG.error("Unexpected exception when handling exception for " + this, ex);
      }
    }
  
    /**
     * Helper that sends the query downstream.
     * @param deferreds A non-null list of deferreds that we need to wait 
     * on (the execution deferreds).
     * @param start_idx The start index in the time ranges.
     * @param end_idx The end index in the time ranges.
     */
    private void fireDownstream(final List<Deferred<Object>> deferreds, 
                                final int start_idx, 
                                final int end_idx) {
      // merge the timestamps and fire a new downstream query.
      if (downstreams == null) {
        downstreams = Lists.newArrayListWithExpectedSize(1);
      }
      
      try {
        final TimeSeriesQuery.Builder builder = 
            TimeSeriesQuery.newBuilder(query);
        builder.setTime(Timespan.newBuilder(query.getTime())
            .setStart(Long.toString(planner.getTimeRanges()[start_idx][0].msEpoch()))
            .setEnd(Long.toString(planner.getTimeRanges()[end_idx][1].msEpoch())))
        .setOrder(start_idx);
        
        final QueryExecution<T> execution = 
            executor.executeQuery(context, builder.build(), tracer_span);
        downstreams.add(execution);
        deferreds.add(execution.deferred()
            .addCallback(new DownstreamCB(builder.build(), start_idx, end_idx))
            .addErrback(new ErrorCB(true)));
      } catch (Exception e) {
        final Exception ex = new QueryExecutionException(
            "Unexpected exception sending query downstream: " + this, 
              500, query.getOrder(), e);
        handleException(ex);
      }
    }
  
    /** Called once all downstream executors are done or the full cache results
     * were fetched. Merges the results and calls back successful. */
    class CompletedCB implements Callback<Object, ArrayList<Object>> {

      @Override
      public Object call(ArrayList<Object> arg) throws Exception {
        final T merged;
        try {
          merged = planner.mergeSlicedResults(results);
        } catch (Exception e) {
          final Exception ex = new QueryExecutionException(
              "Unexpected exception merging results after successful query: "
                  + LocalExecution.this, 500, query.getOrder(), e);
          handleException(ex);
          return null;
        }
        outstanding_executions.remove(LocalExecution.this);
        try {
          callback(merged, TsdbTrace.successfulTags());
        } catch (IllegalStateException e) {
          LOG.error("Callback already executed: " + LocalExecution.this, e);
        } catch (Exception e) {
          final Exception ex = new QueryExecutionException(
              "Unexpected exception merging results after successful query: "
                  + LocalExecution.this, 500, query.getOrder(), e);
          handleException(ex);
        }
        return null;
      }
    }
    
    /** The callback for a query response from downstream. */
    class DownstreamCB implements Callback<Object, T> {
      final TimeSeriesQuery query;
      final int start_index;
      final int end_index;
      
      /**
       * @param query A non-null query.
       * @param start_index A start index within the time ranges.
       * @param end_index An end index within the time ranges.
       */
      DownstreamCB(final TimeSeriesQuery query, 
                   final int start_index, 
                   final int end_index) {
        this.query = query;
        this.start_index = start_index;
        this.end_index = end_index;
      }
      
      @Override
      public Object call(final T response) throws Exception {
        // store, then build and cache
        results.set(start_index, response);
        final Span cache_span;
        if (context.getTracer() != null) {
          cache_span = context.getTracer()
              .buildSpan("cacheWriter_idx_" + start_index + "_" + end_index)
              .asChildOf(tracer_span)
              .start();
        } else {
          cache_span = null;
        }
        
        if (config.getBypass()) {
          return null;
        }
        try {
          // TODO - split this out into a separate thread. Otherwise we're delaying
          // the query while we slice and store the data.
          final List<T> slices = 
              planner.sliceResult(response, start_index, end_index);
          long bytes = 0;
          final byte[][] data = new byte[slices.size()][];
          final byte[][] cache_keys = new byte[slices.size()][];
          final long[] expirations = new long[slices.size()];
          for (int i = 0; i < slices.size(); i++) {
            final ByteArrayOutputStream output = new ByteArrayOutputStream();
            //serdes.serialize(query, null, output, slices.get(i));
            output.close();
            data[i] = output.toByteArray();
            bytes += data[i].length;
            final long expiration;
            if (start_index == end_index) {
              // then we'd only have a single result when slicing.
              expiration = key_generator.expiration(query, config.expiration);
            } else {
              final TimeSeriesQuery.Builder builder = 
                  TimeSeriesQuery.newBuilder(query);
              builder.setTime(Timespan.newBuilder(query.getTime())
                  .setStart(Long.toString(
                      planner.getTimeRanges()[start_index + i][0].msEpoch()))
                  .setEnd(Long.toString(
                      planner.getTimeRanges()[start_index + i][1].msEpoch())))
              .setOrder(start_index + i);
              expiration = key_generator.expiration(builder.build(), 
                  config.expiration);
            }
            expirations[i] = expiration;
            cache_keys[i] = keys[start_index + i];
          }
          plugin.cache(cache_keys, data, expirations, TimeUnit.MILLISECONDS);
          if (cache_span != null) {
            final List<Map<String, Object>> cache_details = 
                Lists.newArrayListWithCapacity(data.length);
            for (int i = 0; i < data.length; i++) {
              final Map<String, Object> map = Maps.newHashMap();
              cache_details.add(map);
              map.put("range", planner.getTimeRanges()[i][0] + "-" 
                  + planner.getTimeRanges()[i][1]);
              map.put("bytes", data[i].length);
              map.put("key", Bytes.pretty(cache_keys[i]));
              map.put("expiration", expirations[i]);
            }
            cache_span.setTag("cacheDetails", JSON.serializeToString(cache_details));
            cache_span.setTag("status", "OK");
            cache_span.setTag("finalThread", Thread.currentThread().getName());
            cache_span.setTag("bytes", Long.toString(bytes));
            cache_span.setTag("keys", Integer.toString(keys.length));
            cache_span.finish();
          }
        } catch (Exception e) {
          // since the write to the cache is less important that the query, 
          // log the exception and keep going.
          LOG.error("Failed to slice and cache query results: " 
              + LocalExecution.this, e);
          if (cache_span != null) {
            cache_span.setTag("status", "Error");
            cache_span.setTag("finalThread", Thread.currentThread().getName());
            cache_span.setTag("error", e != null ? e.getMessage() : "Unknown");
            cache_span.log(TsdbTrace.exceptionAnnotation(e));
            cache_span.finish();
          }
        }
        
        return null;
      }
    }
    
    /** The callback for exceptions. Used for both cache and downstream. */
    class ErrorCB implements Callback<Object, Exception> {
      final boolean is_downstream;
      
      /**
       * @param is_downstream Whether or not the error handler is dealing with
       * a downstream query (we cancel immediately) or the cache in which
       * we fire downstream.
       */
      ErrorCB(final boolean is_downstream) {
        this.is_downstream = is_downstream;
      }
      
      @Override
      public Object call(final Exception ex) throws Exception {
        if (is_downstream) {
          handleException(ex);
        } else {
          cache_execution = null;
          if (LOG.isDebugEnabled()) {
            LOG.debug("Firing query downstream after exception from cache.", ex);
          }
          // send the entire query.
          final List<Deferred<Object>> deferreds = Lists.newArrayListWithCapacity(1);
          fireDownstream(deferreds, 0, planner.getTimeRanges().length - 1);
          Deferred.group(deferreds)
            .addCallback(new CompletedCB())
            .addErrback(new ErrorCB(true));
        }
        return null;
      }
    }
    
  }
  
  /** The configuration class for this executor. */
  @JsonInclude(Include.NON_NULL)
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonDeserialize(builder = Config.Builder.class)
  public static class Config extends QueryExecutorConfig {
    private final String cache_id;
    private final String serdes_id;
    private final String planner_id;
    private final String key_generator_id;
    private final long expiration;
    private final boolean bypass;
    
    /**
     * Default ctor.
     * @param builder A non-null builder.
     */
    protected Config(final Builder builder) {
      super(builder);
      cache_id = builder.cacheId;
      serdes_id = builder.serdesId;
      planner_id = builder.plannerId;
      key_generator_id = builder.keyGeneratorId;
      expiration = builder.expiration;
      bypass = builder.bypass;
    }

    /** @return A cache ID representing a plugin in the registry. */
    public String getCacheId() {
      return cache_id;
    }
    
    /** @return A serdes ID representing a serializer in the registry. */
    public String getSerdesId() {
      return serdes_id;
    }
    
    /** @return A query planner ID used for slicing the query. */
    public String getPlannerId() {
      return planner_id;
    }
    
    /** @return An optional key generator id, null for the default. */
    public String getKeyGeneratorId() {
      return key_generator_id;
    }
    
    /** @return How long, in milliseconds, to keep the data in cache. 0 = don't
     * write, -1 means use query end-time and downsampling. */
    public long getExpiration() {
      return expiration;
    }
    
    /** @return Whether or not to bypass the cache query and write. */
    public boolean getBypass() {
      return bypass;
    }
    
    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final Config config = (Config) o;
      return Objects.equal(executor_id, config.executor_id)
          && Objects.equal(executor_type, config.executor_type)
          && Objects.equal(cache_id, config.cache_id)
          && Objects.equal(serdes_id, config.serdes_id)
          && Objects.equal(planner_id, config.planner_id)
          && Objects.equal(key_generator_id, config.key_generator_id)
          && Objects.equal(expiration, config.expiration)
          && Objects.equal(bypass, config.bypass);
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
          .putString(Strings.nullToEmpty(cache_id), Const.UTF8_CHARSET)
          .putString(Strings.nullToEmpty(serdes_id), Const.UTF8_CHARSET)
          .putString(Strings.nullToEmpty(planner_id), Const.UTF8_CHARSET)
          .putString(Strings.nullToEmpty(key_generator_id), Const.UTF8_CHARSET)
          .putLong(expiration)
          .putBoolean(bypass)
          .hash();
    }

    @Override
    public int compareTo(final QueryExecutorConfig config) {
      return ComparisonChain.start()
          .compare(executor_id, config.executor_id, 
              Ordering.natural().nullsFirst())
          .compare(executor_type, config.executor_type, 
              Ordering.natural().nullsFirst())
          .compare(cache_id, ((Config) config).cache_id, 
              Ordering.natural().nullsFirst())
          .compare(serdes_id, ((Config) config).serdes_id, 
              Ordering.natural().nullsFirst())
          .compare(planner_id, ((Config) config).planner_id, 
              Ordering.natural().nullsFirst())
          .compare(key_generator_id, ((Config) config).key_generator_id, 
              Ordering.natural().nullsFirst())
          .compare(expiration, ((Config) config).expiration)
          .compareTrueFirst(bypass, ((Config) config).bypass)
          .result();
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
          .setCacheId(config.cache_id)
          .setSerdesId(config.serdes_id)
          .setPlannerId(config.planner_id)
          .setKeyGeneratorId(config.key_generator_id)
          .setExpiration(config.expiration)
          .setBypass(config.bypass)
          .setExecutorId(config.executor_id)
          .setExecutorType(config.executor_type);
    }
    
    public static class Builder extends QueryExecutorConfig.Builder {
      @JsonProperty
      private String cacheId;
      @JsonProperty
      private String serdesId;
      @JsonProperty
      private String plannerId;
      @JsonProperty
      private String keyGeneratorId;
      @JsonProperty
      private long expiration = -1;
      @JsonProperty
      private boolean bypass;
      
      /**
       * @param cacheId A non-null and non-empty cache ID.
       * @return The builder.
       */
      public Builder setCacheId(final String cacheId) {
        this.cacheId = cacheId;
        return this;
      }
      
      /**
       * @param serdesId A non-null and non-empty serdes ID.
       * @return The builder.
       */
      public Builder setSerdesId(final String serdesId) {
        this.serdesId = serdesId;
        return this;
      }
      
      /**
       * @param plannerId A non-null and non-empty planner ID.
       * @return The builder.
       */
      public Builder setPlannerId(final String plannerId) {
        this.plannerId = plannerId;
        return this;
      }
      
      /**
       * @param keyGeneratorId An optional key generator Id. If null, the default
       * is used.
       * @return The builder.
       */
      public Builder setKeyGeneratorId(final String keyGeneratorId) {
        this.keyGeneratorId = keyGeneratorId;
        return this;
      }
      
      /**
       * @param expiration How long, in milliseconds, to keep the data in cache.
       * 0 = don't write, -1 means use query end-time and downsampling. 
       * @return The builder.
       */
      public Builder setExpiration(final long expiration) {
        this.expiration = expiration;
        return this;
      }
      
      /**
       * @param bypass Whether or not to bypass the cache query and write.
       * @return The builder.
       */
      public Builder setBypass(final boolean bypass) {
        this.bypass = bypass;
        return this;
      }
      
      @Override
      public Config build() {
        return new Config(this);
      }
      
    }
  }
  
  @VisibleForTesting
  QueryCachePlugin plugin() {
    return plugin;
  }
  
  @VisibleForTesting
  TimeSeriesSerdes serdes() {
    return serdes;
  }
  
  @VisibleForTesting
  TimeSeriesCacheKeyGenerator keyGenerator() {
    return key_generator;
  }
  
}
