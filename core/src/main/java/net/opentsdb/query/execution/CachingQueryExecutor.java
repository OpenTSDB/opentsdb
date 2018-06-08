// This file is part of OpenTSDB.
// Copyright (C) 2017-2018  The OpenTSDB Authors.
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
import com.google.common.collect.Ordering;
import com.google.common.hash.HashCode;
import com.stumbleupon.async.Callback;

import net.opentsdb.common.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.exceptions.QueryExecutionCanceled;
import net.opentsdb.query.AbstractQueryNode;
import net.opentsdb.query.BaseQueryNodeConfig;
import net.opentsdb.query.ConvertedQueryResult;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QuerySourceFactory;
import net.opentsdb.query.execution.cache.QueryCachePlugin;
import net.opentsdb.query.execution.cache.TimeSeriesCacheKeyGenerator;
import net.opentsdb.query.execution.serdes.BaseSerdesOptions;
import net.opentsdb.query.serdes.SerdesOptions;
import net.opentsdb.query.serdes.TimeSeriesSerdes;
import net.opentsdb.stats.Span;

/**
 * Executor that can either query a cache for a query and then send it 
 * downstream on a miss, or query both the cache and downstream at the same
 * time and return the results of whoever responds quicker.
 * <p>
 * On a cache exception, the downstream result will always be preferred.
 * <p>
 * Once the caching plugin and serdes modules are set via the default config,
 * query time overridden settings are ignored. (TODO - should we allow?)
 * <p>
 * TODO - handle streaming
 * TODO - merge execution graph with pipeline
 * 
 * @since 3.0
 */
public class CachingQueryExecutor implements QuerySourceFactory {
  private static final Logger LOG = LoggerFactory.getLogger(
      CachingQueryExecutor.class);
  
  /** Reference to the TSD. */
  private final TSDB tsdb;
  
  /** The default cache plugin to use. */
  private final QueryCachePlugin plugin;
  
  /** The default serdes class to use. */
  private final TimeSeriesSerdes serdes;
  
  /** A key generator used for reading and writing the cache data. */
  private final TimeSeriesCacheKeyGenerator key_generator;
  
  /**
   * <b>TEMPORARY</b> Ctor till we get the execution graph code merged
   * with the pipeline.
   * @param tsdb A non-null TSD.
   */
  public CachingQueryExecutor(final TSDB tsdb) {
    if (tsdb == null) {
      throw new IllegalArgumentException("TSDB cannot be null.");
    }
    this.tsdb = tsdb;
    plugin = tsdb.getRegistry().getDefaultPlugin(QueryCachePlugin.class);    
    serdes = tsdb.getRegistry().getDefaultPlugin(TimeSeriesSerdes.class);
    key_generator = tsdb.getRegistry().getDefaultPlugin(TimeSeriesCacheKeyGenerator.class);
    if (plugin == null) {
      throw new IllegalArgumentException("No default cache plugin loaded.");
    }
    if (serdes == null) {
      throw new IllegalArgumentException("No default serdes plugin loaded.");
    }
    if (key_generator == null) {
      throw new IllegalArgumentException("No default key generator loaded.");
    }
    
  }
  
  @Override
  public QueryNode newNode(final QueryPipelineContext context, 
                           final String id) {
    // TODO pull the default config from some place
    throw new UnsupportedOperationException("Not implemented yet");
  }
  
  @Override
  public Class<? extends QueryNodeConfig> nodeConfigClass() {
    return Config.class;
  }
  
  @Override
  public TimeSeriesDataSource newNode(final QueryPipelineContext context,
                                      final String id,
                                      final QueryNodeConfig config) {
    return new LocalExecution(context, id, config);
  }
  
  /** Local execution class. */
  class LocalExecution extends AbstractQueryNode implements 
      TimeSeriesDataSource {
    /** The default or overridden config. */
    protected final Config config;
    
    /** A lock to determine when we've received results. Required when executing
     * simultaneously. */
    protected final AtomicBoolean complete;

    /** The key for the cache entry. */
    protected final byte[] key;
    
    /** The cache execution to track. */
    protected QueryExecution<byte[]> cache_execution;
    
    /** A child tracer if we got one. */
    protected Span child;
    
    /**
     * Default ctor
     * @param context The query context.
     * @param QueryNodeConfig The config for the node.
     */
    public LocalExecution(final QueryPipelineContext context, 
                          final String id,
                          final QueryNodeConfig config) {
      super(CachingQueryExecutor.this, context, id);
      complete = new AtomicBoolean();

      key = key_generator.generate(
          (net.opentsdb.query.pojo.TimeSeriesQuery) context.query(), 
          ((Config) config).use_timestamps);
      this.config = (Config) config;
//      final QueryExecutorConfig override = 
//          context.getConfigOverride(node.getExecutorId());
//      if (override != null) {
//        config = (Config) override;
//      } else {
//        config = (Config) node.getDefaultConfig();
//      }
//      
//      if (context.getTracer() != null) {
//        setSpan(context, 
//            CachingQueryExecutor.this.getClass().getSimpleName(), 
//            upstream_span,
//            TsdbTrace.addTags(
//                "order", Integer.toString(query.getOrder()),
//                "query", JSON.serializeToString(query),
//                "startThread", Thread.currentThread().getName()));
//      }
    }
    
    @Override
    public QueryNodeConfig config() {
      return config;
    }
    
    @Override
    public String id() {
      return config.getId();
    }
    
    @Override
    public void onComplete(final QueryNode downstream, 
                           final long final_sequence,
                           final long total_sequences) {
      completeUpstream(final_sequence, total_sequences);
    }
    
    @Override
    public void onNext(final QueryResult next) {
      // Only cache if the result source didn't come from this execution.
      // If the cache implementation forgets to set the source of it's 
      // result it would be null but all other nodes should have a proper
      // source.
      if (!complete.get() &&
          next.source() != null && (
          downstream.contains(next.source()) || 
          next.source() != LocalExecution.this)) {
        try {
          tsdb.getStatsCollector().incrementCounter("query.cache.executor.miss",
              "node", "CachingQueryExecutor");
          final net.opentsdb.query.pojo.TimeSeriesQuery query = 
              (net.opentsdb.query.pojo.TimeSeriesQuery) context.query();
          
          final long expiration = key_generator.expiration(query, 
              config.getExpiration());
          if (LOG.isDebugEnabled()) {
            LOG.debug("Calculated cache expiration: " + expiration);
          }
          if (expiration > 0) {
            // only serialize string IDs so in this case we're going to 
            // convert asynchronously and call ourselves back with the
            // results.
            if (next.idType() == Const.TS_BYTE_ID) {
              // NOTE: If this implementation fails to call onError or
              // onNext, we're screwed.
              ConvertedQueryResult.convert(next, LocalExecution.this, child);
              return;
            }
            
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            class SerdesCB implements Callback<Object, Object> {
              @Override
              public Object call(Object arg) throws Exception {
                plugin.cache(key, baos.toByteArray(), expiration, 
                    TimeUnit.MILLISECONDS, null);
                return null;
              }
            }
            
            class ErrorCB implements Callback<Object, Exception> {
              @Override
              public Object call(final Exception ex) throws Exception {
                LOG.warn("Failed to serialize result: " + next, ex);
                return null;
              }
            }
            
            // TODO - normalize times.
            final SerdesOptions options = BaseSerdesOptions.newBuilder()
                .setStart(query.getTime().startTime())
                .setEnd(query.getTime().endTime())
                .build();
            serdes.serialize(context.queryContext(), options, baos, next, child)
              .addCallbacks(new SerdesCB(), new ErrorCB());
          }
        } catch (Exception e) {
          LOG.error("Failed to process results for cache", e);
        }
      } else {
        tsdb.getStatsCollector().incrementCounter("query.cache.executor.hit",
            "node", "CachingQueryExecutor");
      }
      
      if (complete.compareAndSet(false, true)) {
        sendUpstream(next);
        if (next.source() == null || next.source() == LocalExecution.this || 
            (!downstream.contains(next.source()))) {
          // only call complete if we had a cached result.
          completeUpstream(next.sequenceId(), next.sequenceId());
        }
        
        if (config.getSimultaneous()) {
          try {
            if (cache_execution != null) {
              cache_execution.cancel();
            }
          } catch (Exception e) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Error cancelling the cache execution", e);
            }
          }
        }
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Race sending results upstream.");
        }
      }
    }
    
    @Override
    public void fetchNext(final Span span) {
      if (span != null) {
        child = span.newChild(getClass().getName() + ".fetchNext()")
            .start();
      }
//      /** The callback for downstream data. */
//      class DownstreamCB implements Callback<Object, T> {
//        @Override
//        public Object call(final T results) throws Exception {
//          if (!complete.compareAndSet(false, true)) {
//            if (LOG.isDebugEnabled()) {
//              LOG.debug("Call downstream completed after the call to the cache.");
//            }
//            return null;
//          }
//          outstanding_executions.remove(LocalExecution.this);
//          callback(results,
//              TsdbTrace.successfulTags());
//          
//          final Span cache_span;
//          if (context.getTracer() != null) {
//            cache_span = context.getTracer().buildSpan(
//                CachingQueryExecutor.this.getClass().getSimpleName() 
//                  + "#cacheWrite")
//              .asChildOf(tracer_span)
//              .withTag("order", Integer.toString(query.getOrder()))
//              .withTag("query", JSON.serializeToString(query))
//              .withTag("startThread", Thread.currentThread().getName())
//              .start();
//          } else {
//            cache_span = null;
//          }
//          if (config.bypass) {
//            if (LOG.isDebugEnabled()) {
//              LOG.debug("Bypassing cache write.");
//            }
//            return null;
//          }
//          try {
//            final long expiration = key_generator.expiration(query, 
//                config.expiration);
//            if (expiration == 0) {
//              // told not to write to the cache.
//              return null;
//            }
//            
//            // TODO - run this in another thread pool. Would let us hit cache
//            // quicker if someone's firing off the same query.
//            final ByteArrayOutputStream output = new ByteArrayOutputStream();
//            //serdes.serialize(query, null, output, results);
//            output.close();
//            
//            final byte[] data = output.toByteArray();
//            plugin.cache(key, data, expiration, TimeUnit.MILLISECONDS, null);
//            if (cache_span != null) {
//              cache_span.setTag("status", "OK");
//              cache_span.setTag("finalThread", Thread.currentThread().getName());
//              cache_span.setTag("bytes", Integer.toString(data.length));
//              cache_span.finish();
//            }
//          } catch (Exception e) {
//            LOG.error("WTF?", e);
//            if (cache_span != null) {
//              cache_span.setTag("status", "Error");
//              cache_span.setTag("finalThread", Thread.currentThread().getName());
//              cache_span.setTag("error", e != null ? e.getMessage() : "Unknown");
//              cache_span.log(TsdbTrace.exceptionAnnotation(e));
//              cache_span.finish();
//            }
//          }
//          
//          if (config.simultaneous && cache_execution != null) {
//            // ^^ race here but shouldn't be a big deal.
//            try {
//              cache_execution.cancel();
//            } catch (Exception e) {
//              LOG.warn("Exception thrown cancelling cache execution: " 
//                  + cache_execution, e);
//            }
//          }
//          
//          return null;
//        }
//      }
      
      /** The callback for exceptions. Used for both cache and downstream. */
      class ErrorCB implements Callback<Object, Exception> {
        @Override
        public Object call(final Exception ex) throws Exception {
            cache_execution = null;
            if (ex instanceof QueryExecutionCanceled) {
              return null;
            }
            if (!config.simultaneous) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Firing query downstream after exception from cache.");
              }
              if (child != null) {
                child.setErrorTags(ex)
                     .finish();
              }
              fetchDownstream(child);
            }
            LOG.warn("Exception returned from cache: " + this, ex);
          return null;
        }
      }
      
      /** The cache response callback that will continue downstream if needed. */
      class CacheCB implements Callback<Object, byte[]> {
        @Override
        public Object call(final byte[] cache_data) throws Exception {
          cache_execution = null;
          
          // cache miss, so start the downstream call if we haven't already.
          if (cache_data == null && !config.simultaneous) {
            fetchDownstream(child);
          } else {
            // TODO - see if we can cancel the other call.
//            if (downstream != null) {
//              try {
//                downstream.cancel();
//              } catch (Exception e) {
//                LOG.warn("Exception thrown cancelling downstream executor: " 
//                    + downstream, e);
//              }
//            }
            if (child != null) {
              child.setSuccessTags()
                   .finish();
            }
            
            // TODO - normalize times.
            final net.opentsdb.query.pojo.TimeSeriesQuery query = 
                (net.opentsdb.query.pojo.TimeSeriesQuery) context.query();
            final SerdesOptions options = BaseSerdesOptions.newBuilder()
                .setStart(query.getTime().startTime())
                .setEnd(query.getTime().endTime())
                .build();
            serdes.deserialize(options, 
                new ByteArrayInputStream(cache_data), 
                LocalExecution.this,
                child);
          }
          return null;
        }
      }
      
      try {
        if (config.getBypass()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Bypassing cache query.");
          }
          fetchDownstream(child);
        } else if (config.simultaneous) {
            // fire both before attaching callbacks to avoid a race on canceling
            // the executors.
            cache_execution = plugin.fetch(context.queryContext(), key, child);
            fetchDownstream(child);
            cache_execution.deferred()
              .addCallback(new CacheCB())
              .addErrback(new ErrorCB());
        } else {
          cache_execution = plugin.fetch(context.queryContext(), key, child);
          cache_execution.deferred()
            .addCallback(new CacheCB())
            .addErrback(new ErrorCB());
        }
      } catch (Exception e) {
        try {
          final Exception ex = new QueryExecutionCanceled(
              "Unexpected exception initiating query: " + this, 500, 0/** TODO query.getOrder()*/);
          if (child != null) {
            child.setErrorTags(ex)
                 .finish();
          }
          sendUpstream(ex);
        } catch (IllegalArgumentException ex) {
          // lost race, no prob.
        } catch (Exception ex) {
          LOG.warn("Failed to complete callback due to unexepcted "
              + "exception: " + this, ex);
        }
      }
    }

    @Override
    public void onError(final Throwable t) {
      if (complete.compareAndSet(false, true)) {
        sendUpstream(t);        
      }
    }
    
    @Override
    public void close() {
      if (!complete.get()) {
        try {
          final Exception e = new QueryExecutionCanceled(
              "Query was cancelled upstream: " + this, 400, 0/** TODO query.getOrder()*/);
          //callback(e, TsdbTrace.canceledTags(e));
          sendUpstream(e);
        } catch (IllegalArgumentException ex) {
          // lost race, no prob.
        } catch (Exception ex) {
          LOG.warn("Failed to complete callback due to unexepcted "
              + "exception: " + this, ex);
        }
      }
      
      if (cache_execution != null) {
        try {
          cache_execution.cancel();
        } catch (Exception e) {
          LOG.warn("Failed canceling cache execution: " + cache_execution, e);
        }
      }
      if (downstream != null) {
        try {
          // TODO - close all downstream?
          //downstream.cancel();
        } catch (Exception e) {
          LOG.warn("Failed canceling downstream execution: " + downstream, e);
        }
      }
    }
  }
  
  @Override
  public String id() {
    return getClass().getSimpleName();
  }
  
  @JsonInclude(Include.NON_NULL)
  @JsonDeserialize(builder = Config.Builder.class)
  public static class Config extends BaseQueryNodeConfig {
    private final String cache_id;
    private final String serdes_id;
    private final boolean simultaneous;
    private final String key_generator_id;
    private final long expiration;
    private final boolean bypass;
    private final boolean use_timestamps;
    
    /**
     * Default ctor.
     * @param builder A non-null builder.
     */
    protected Config(final Builder builder) {
      super(builder);
      cache_id = builder.cacheId;
      serdes_id = builder.serdesId;
      simultaneous = builder.simultaneous;
      key_generator_id = builder.keyGeneratorId;
      expiration = builder.expiration;
      bypass = builder.bypass;
      use_timestamps = builder.useTimestamps;
    }
    
    /** @return A cache ID representing a plugin in the registry. */
    public String getCacheId() {
      return cache_id;
    }
    
    /** @return A serdes ID representing a serializer in the registry. */
    public String getSerdesId() {
      return serdes_id;
    }
    
    /** @return Whether or not to fire downstream and to the cache
      * at the same time (true) or wait for the cache to return before sending */
    public boolean getSimultaneous() {
      return simultaneous;
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
    
    /** @return Whether or not to bypass the cache query and write.  */
    public boolean getBypass() {
      return bypass;
    }
    
    /** @return Whether or not to use the timestamps of the query when generating
     * the query key. */
    public boolean getUseTimestamps() {
      return use_timestamps;
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
      return Objects.equal(id, config.id)
          && Objects.equal(cache_id, config.cache_id)
          && Objects.equal(serdes_id, config.serdes_id)
          && Objects.equal(simultaneous, config.simultaneous)
          && Objects.equal(key_generator_id, config.key_generator_id)
          && Objects.equal(expiration, config.expiration)
          && Objects.equal(bypass, config.bypass)
          && Objects.equal(use_timestamps, config.use_timestamps);
    }

    @Override
    public int hashCode() {
      return buildHashCode().asInt();
    }

    @Override
    public HashCode buildHashCode() {
      return Const.HASH_FUNCTION().newHasher()
          .putString(Strings.nullToEmpty(id), Const.UTF8_CHARSET)
          .putString(Strings.nullToEmpty(cache_id), Const.UTF8_CHARSET)
          .putString(Strings.nullToEmpty(serdes_id), Const.UTF8_CHARSET)
          .putBoolean(simultaneous)
          .putString(Strings.nullToEmpty(key_generator_id), Const.UTF8_CHARSET)
          .putLong(expiration)
          .putBoolean(bypass)
          .putBoolean(use_timestamps)
          .hash();
    }

    @Override
    public int compareTo(final QueryNodeConfig other) {
      if (!(other instanceof Config)) {
        return -1;
      }
      final Config config = (Config) other;
      return ComparisonChain.start()
          .compare(id, config.id, Ordering.natural().nullsFirst())
          .compare(cache_id, ((Config) config).cache_id, 
              Ordering.natural().nullsFirst())
          .compare(serdes_id, ((Config) config).serdes_id, 
              Ordering.natural().nullsFirst())
          .compareTrueFirst(simultaneous, ((Config) config).simultaneous)
          .compare(key_generator_id, ((Config) config).key_generator_id, 
              Ordering.natural().nullsFirst())
          .compare(expiration, ((Config) config).expiration)
          .compareTrueFirst(bypass, ((Config) config).bypass)
          .compareTrueFirst(use_timestamps, (((Config) config).use_timestamps))
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
          .setSimultaneous(config.simultaneous)
          .setKeyGeneratorId(config.key_generator_id)
          .setExpiration(config.expiration)
          .setUseTimestamps(config.use_timestamps)
          .setId(config.id);
    }
    
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Builder extends BaseQueryNodeConfig.Builder {
      @JsonProperty
      private String cacheId;
      @JsonProperty
      private String serdesId;
      @JsonProperty
      private boolean simultaneous;
      @JsonProperty
      private String keyGeneratorId;
      @JsonProperty
      private long expiration = -1;
      @JsonProperty
      private boolean bypass;
      @JsonProperty
      private boolean useTimestamps;
      
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
       * @param simultaneous Whether or not to fire downstream and to the cache
       * at the same time (true) or wait for the cache to return before sending
       * downstream on a miss.
       * @return The builder.
       */
      public Builder setSimultaneous(final boolean simultaneous) {
        this.simultaneous = simultaneous;
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
      
      /**
       * @param useTimestamps Whether or not to include the query timestamps
       * when generating the key.
       * @return The builder.
       */
      public Builder setUseTimestamps(final boolean useTimestamps) {
        this.useTimestamps = useTimestamps;
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
