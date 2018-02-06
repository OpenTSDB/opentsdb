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

import io.opentracing.Span;
import net.opentsdb.core.Const;
import net.opentsdb.core.DefaultRegistry;
import net.opentsdb.exceptions.QueryExecutionCanceled;
import net.opentsdb.query.context.QueryContext;
import net.opentsdb.query.execution.cache.QueryCachePlugin;
import net.opentsdb.query.execution.cache.TimeSeriesCacheKeyGenerator;
import net.opentsdb.query.execution.graph.ExecutionGraphNode;
import net.opentsdb.query.execution.serdes.TimeSeriesSerdes;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.stats.TsdbTrace;
import net.opentsdb.utils.JSON;

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
 * TODO - Load the key gen from registry.
 *
 * @param <T> The type of data handled by this executor.
 * 
 * @since 3.0
 */
public class CachingQueryExecutor<T> extends QueryExecutor<T> {
  private static final Logger LOG = LoggerFactory.getLogger(
      CachingQueryExecutor.class);
  
  /** The downstream executor that queries are passed to. */
  private final QueryExecutor<T> executor;
  
  /** The cache plugin to use. */
  private final QueryCachePlugin plugin;
  
  /** The serdes class to use. */
  private final TimeSeriesSerdes serdes;
  
  /** A key generator used for reading and writing the cache data. */
  private final TimeSeriesCacheKeyGenerator key_generator;
  
  @SuppressWarnings("unchecked")
  public CachingQueryExecutor(final ExecutionGraphNode node) {
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
  public QueryExecution<T> executeQuery(final QueryContext context,
                                        final TimeSeriesQuery query, 
                                        final Span upstream_span) {
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
    
    /** A lock to determine when we've received results. Required when executing
     * simultaneously. */
    private final AtomicBoolean complete;
    
    /** The cache execution to track. */
    private QueryExecution<byte[]> cache_execution;
    
    /** The downstream execution to wait on (or cancel). */
    private QueryExecution<T> downstream;
    
    /**
     * Default ctor
     * @param context The query context.
     * @param query The query itself.
     * @param upstream_span An optional tracer span.
     */
    public LocalExecution(final QueryContext context, 
                          final TimeSeriesQuery query, 
                          final Span upstream_span) {
      super(query);
      this.context = context;
      complete = new AtomicBoolean();
      outstanding_executions.add(this);
      
      final QueryExecutorConfig override = 
          context.getConfigOverride(node.getExecutorId());
      if (override != null) {
        config = (Config) override;
      } else {
        config = (Config) node.getDefaultConfig();
      }
      
      if (context.getTracer() != null) {
        setSpan(context, 
            CachingQueryExecutor.this.getClass().getSimpleName(), 
            upstream_span,
            TsdbTrace.addTags(
                "order", Integer.toString(query.getOrder()),
                "query", JSON.serializeToString(query),
                "startThread", Thread.currentThread().getName()));
      }
    }
    
    /** Runs the query. */
    public void execute() {
      final byte[] key = key_generator.generate(query, config.use_timestamps);
      
      /** The callback for downstream data. */
      class DownstreamCB implements Callback<Object, T> {
        @Override
        public Object call(final T results) throws Exception {
          if (!complete.compareAndSet(false, true)) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Call downstream completed after the call to the cache.");
            }
            return null;
          }
          outstanding_executions.remove(LocalExecution.this);
          callback(results,
              TsdbTrace.successfulTags());
          
          final Span cache_span;
          if (context.getTracer() != null) {
            cache_span = context.getTracer().buildSpan(
                CachingQueryExecutor.this.getClass().getSimpleName() 
                  + "#cacheWrite")
              .asChildOf(tracer_span)
              .withTag("order", Integer.toString(query.getOrder()))
              .withTag("query", JSON.serializeToString(query))
              .withTag("startThread", Thread.currentThread().getName())
              .start();
          } else {
            cache_span = null;
          }
          if (config.bypass) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Bypassing cache write.");
            }
            return null;
          }
          try {
            final long expiration = key_generator.expiration(query, 
                config.expiration);
            if (expiration == 0) {
              // told not to write to the cache.
              return null;
            }
            
            // TODO - run this in another thread pool. Would let us hit cache
            // quicker if someone's firing off the same query.
            final ByteArrayOutputStream output = new ByteArrayOutputStream();
            //serdes.serialize(query, null, output, results);
            output.close();
            
            final byte[] data = output.toByteArray();
            plugin.cache(key, data, expiration, TimeUnit.MILLISECONDS);
            if (cache_span != null) {
              cache_span.setTag("status", "OK");
              cache_span.setTag("finalThread", Thread.currentThread().getName());
              cache_span.setTag("bytes", Integer.toString(data.length));
              cache_span.finish();
            }
          } catch (Exception e) {
            LOG.error("WTF?", e);
            if (cache_span != null) {
              cache_span.setTag("status", "Error");
              cache_span.setTag("finalThread", Thread.currentThread().getName());
              cache_span.setTag("error", e != null ? e.getMessage() : "Unknown");
              cache_span.log(TsdbTrace.exceptionAnnotation(e));
              cache_span.finish();
            }
          }
          
          if (config.simultaneous && cache_execution != null) {
            // ^^ race here but shouldn't be a big deal.
            try {
              cache_execution.cancel();
            } catch (Exception e) {
              LOG.warn("Exception thrown cancelling cache execution: " 
                  + cache_execution, e);
            }
          }
          
          return null;
        }
      }
      
      /** The callback for exceptions. Used for both cache and downstream. */
      class ErrorCB implements Callback<Object, Exception> {
        final boolean is_downstream;
        
        ErrorCB(final boolean is_downstream) {
          this.is_downstream = is_downstream;
        }
        
        @Override
        public Object call(final Exception ex) throws Exception {
          if (is_downstream) {
            if (!complete.compareAndSet(false, true)) {
              LOG.warn("Downstream returned an exception but we were already "
                  + "marked complete: " + this, ex);
              return null;
            }
            outstanding_executions.remove(LocalExecution.this);
            callback(ex,
                TsdbTrace.exceptionTags(ex),
                TsdbTrace.exceptionAnnotation(ex));
            if (cache_execution != null) {
              try {
                cache_execution.cancel();
              } catch (Exception e) {
                LOG.warn("Exception canceling cache call", e);
              }
            }
          } else {
            cache_execution = null;
            if (ex instanceof QueryExecutionCanceled) {
              return null;
            }
            if (!config.simultaneous) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Firing query downstream after exception from cache.");
              }
              downstream = executor.executeQuery(context, query, tracer_span);
              downstream.deferred()
                  .addCallback(new DownstreamCB())
                  .addErrback(new ErrorCB(true));
            }
            LOG.error("Exception returned from cache: " + this, ex);
          }
          return null;
        }
      }
      
      /** The cache response callback that will continue downstream if needed. */
      class CacheCB implements Callback<Object, byte[]> {
        @Override
        public Object call(final byte[] cache_data) throws Exception {
          if (!complete.compareAndSet(false, true)) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Call to cache plugin: " + plugin + " completed after "
                  + "the call to the downstream executor.");
            }
            return null;
          }
          cache_execution = null;
          outstanding_executions.remove(LocalExecution.this);
          // cache miss, so start the downstream call if we haven't already.
          if (cache_data == null && !config.simultaneous) {
            // unset completed so the downstream can make it through.
            complete.set(false);
            downstream = executor.executeQuery(context, query, tracer_span);
            downstream.deferred()
                .addCallback(new DownstreamCB())
                .addErrback(new ErrorCB(true));
          } else {
            if (downstream != null) {
              try {
                downstream.cancel();
              } catch (Exception e) {
                LOG.warn("Exception thrown cancelling downstream executor: " 
                    + downstream, e);
              }
            }
            callback(serdes.deserialize(null, new ByteArrayInputStream(cache_data)),
                TsdbTrace.successfulTags());
          }
          return null;
        }
      }
      
      try {
        if (config.getBypass()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Bypassing cache query.");
          }
          downstream = executor.executeQuery(context, query, tracer_span);
          downstream.deferred()
            .addCallback(new DownstreamCB())
            .addErrback(new ErrorCB(true));
        } else {
          if (config.simultaneous) {
            // fire both before attaching callbacks to avoid a race on canceling
            // the executors.
            cache_execution = plugin.fetch(context, key, null);
            downstream = executor.executeQuery(context, query, tracer_span);
          } else {
            cache_execution = plugin.fetch(context, key, null);
          }
          cache_execution.deferred()
            .addCallback(new CacheCB())
            .addErrback(new ErrorCB(false));
          if (config.simultaneous) {
            downstream.deferred()
              .addCallback(new DownstreamCB())
              .addErrback(new ErrorCB(true));
          }
        }
      } catch (Exception e) {
        try {
          final Exception ex = new QueryExecutionCanceled(
              "Unexpected exception initiating query: " + this, 500, query.getOrder());
          callback(e, TsdbTrace.exceptionTags(ex),
                      TsdbTrace.exceptionAnnotation(ex));
        } catch (IllegalArgumentException ex) {
          // lost race, no prob.
        } catch (Exception ex) {
          LOG.warn("Failed to complete callback due to unexepcted "
              + "exception: " + this, ex);
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
      if (downstream != null) {
        try {
          downstream.cancel();
        } catch (Exception e) {
          LOG.warn("Failed canceling downstream execution: " + downstream, e);
        }
      }
    }
  }
  
  @JsonInclude(Include.NON_NULL)
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonDeserialize(builder = Config.Builder.class)
  public static class Config extends QueryExecutorConfig {
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
      return Objects.equal(executor_id, config.executor_id)
          && Objects.equal(executor_type, config.executor_type)
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
          .putString(Strings.nullToEmpty(executor_id), Const.UTF8_CHARSET)
          .putString(Strings.nullToEmpty(executor_type), Const.UTF8_CHARSET)
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
          .setExecutorId(config.executor_id)
          .setExecutorType(config.executor_type);
    }
    
    public static class Builder extends QueryExecutorConfig.Builder {
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
