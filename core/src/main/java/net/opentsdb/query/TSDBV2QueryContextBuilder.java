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
package net.opentsdb.query;

import java.util.Collection;
import java.util.List;

import com.google.common.collect.Lists;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.auth.AuthState;
import net.opentsdb.core.TSDB;
import net.opentsdb.query.filter.NamedFilter;
import net.opentsdb.stats.QueryStats;
import net.opentsdb.stats.Span;
import net.opentsdb.utils.Deferreds;

/**
 * The an implementation of a context builder that constructs a TSDBV2
 * query pipeline.
 * 
 * TODO - more work coming on this.
 * 
 * @since 3.0
 */
public class TSDBV2QueryContextBuilder implements QueryContextBuilder {
  /** The TSDB to pull configs and settings from. */
  private TSDB tsdb;
  
  /** The query. */
  private TimeSeriesQuery query;
  
  /** The mode of operation. */
  private QueryMode mode;
  
  /** The stats object. */
  private QueryStats stats;

  /** The sinks we'll write to. */
  private List<QuerySinkConfig> sink_configs;
  
  /** Programmatic sinks. */
  private List<QuerySink> sinks;
  
  /** The authentication state. */
  private AuthState auth_state;
  
  /** Whether or not this object was built users can't call the set methods. */
  private boolean built;
  
  /**
   * Static helper to construct the builder.
   * @param tsdb A non-null TSDB object.
   * @return The builder.
   * @throws IllegalArgumentException if the TSDB object was null.
   */
  public static QueryContextBuilder newBuilder(final TSDB tsdb) {
    if (tsdb == null) {
      throw new IllegalArgumentException("TSDB cannot be null.");
    }
    final TSDBV2QueryContextBuilder builder = new TSDBV2QueryContextBuilder();
    builder.tsdb = tsdb;
    return builder;
  }
  
  @Override
  public QueryContextBuilder setQuery(final TimeSeriesQuery query) {
    if (built) {
      throw new IllegalStateException("Builder was already built.");
    }
    this.query = query;
    return this;
  }

  @Override
  public QueryContextBuilder setMode(final QueryMode mode) {
    if (built) {
      throw new IllegalStateException("Builder was already built.");
    }
    this.mode = mode;
    return this;
  }

  @Override
  public QueryContextBuilder setStats(final QueryStats stats) {
    if (built) {
      throw new IllegalStateException("Builder was already built.");
    }
    this.stats = stats;
    return this;
  }
  
  @Override
  public QueryContextBuilder setSinks(final List<QuerySinkConfig> configs) {
    this.sink_configs = configs;
    return this;
  }
  
  @Override
  public QueryContextBuilder addSink(final QuerySinkConfig config) {
    if (sink_configs == null) {
      sink_configs = Lists.newArrayList();
    }
    sink_configs.add(config);
    return this;
  }
  
  @Override
  public QueryContextBuilder addSink(final QuerySink sink) {
    if (sinks == null) {
      sinks = Lists.newArrayList();
    }
    sinks.add(sink);
    return this;
  }
  
  @Override
  public QueryContextBuilder setAuthState(final AuthState auth_state) {
    this.auth_state = auth_state;
    return this;
  }
  
  @Override
  public QueryContext build() {
    if (query == null) {
      throw new IllegalArgumentException("Query cannot be null.");
    }
    if (sink_configs == null || sink_configs.isEmpty()) {
      throw new IllegalArgumentException("At least one sink must be provided.");
    }
    if (mode == null) {
      throw new IllegalArgumentException("Query mode cannot be null.");
    }
    built = true;
    return new LocalContext();
  }

  /**
   * Local implementation of a context. Just reads the variables from the
   * builder for now.
   */
  class LocalContext implements QueryContext {
    /** The downstream pipeline context. */
    private LocalPipeline context;
    
    /** A local span for tracing. */
    private Span local_span;
    
    public LocalContext() {
      if (stats != null && stats.trace() != null) {
        local_span = stats.trace().newSpan("Query Context Initialization")
            .asChildOf(stats.querySpan())
            .start();
      }
      context = new LocalPipeline(this);
    }
    
    @Override
    public Collection<QuerySink> sinks() {
      return context.sinks();
    }

    @Override
    public QueryMode mode() {
      return mode;
    }

    @Override
    public void fetchNext(final Span span) {
      context.fetchNext(span);
    }

    @Override
    public void close() {
      context.close();
      if (local_span != null) {
        // TODO - more stats around the context
        local_span.finish();
      }
    }

    @Override
    public QueryStats stats() {
      return stats;
    }
    
    @Override
    public List<QuerySinkConfig> sinkConfigs() {
      return sink_configs;
    }
    
    @Override
    public TimeSeriesQuery query() {
      return query;
    }
    
    @Override
    public TSDB tsdb() {
      return tsdb;
    }
  
    @Override
    public AuthState authState() {
      return auth_state;
    }
    
    @Override
    public Deferred<Void> initialize(final Span span) {
      List<Deferred<Void>> initializations = null;
      if (query.getFilters() != null && !query.getFilters().isEmpty()) {
        initializations = Lists.newArrayListWithExpectedSize(
            query.getFilters().size());
        for (final NamedFilter filter : query.getFilters()) {
          initializations.add(filter.getFilter().initialize(span));
        }
      }
      
      class FilterCB implements Callback<Deferred<Void>, Void> {
        @Override
        public Deferred<Void> call(Void arg) throws Exception {
          return context.initialize(local_span);
        }
      }
      
      if (initializations == null) {
        return context.initialize(local_span);
      }
      return Deferred.group(initializations)
          .addBoth(Deferreds.VOID_GROUP_CB)
          .addCallbackDeferring(new FilterCB());
    }
  }
  
  class LocalPipeline extends AbstractQueryPipelineContext {

    public LocalPipeline(final QueryContext context) {
      super(context);
      if (TSDBV2QueryContextBuilder.this.sinks != null && 
          !TSDBV2QueryContextBuilder.this.sinks.isEmpty()) {
        sinks.addAll(TSDBV2QueryContextBuilder.this.sinks);
      }
    }

    @Override
    public Deferred<Void> initialize(Span span) {
      final Span child;
      if (span != null) {
        child = span.newChild(getClass().getSimpleName() + ".initialize()")
                     .start();
      } else {
        child = null;
      }
      

      class SpanCB implements Callback<Void, Void> {
        @Override
        public Void call(final Void ignored) throws Exception {
          if (child != null) {
            child.setSuccessTags().finish();
          }
          return null;
        }
      }
      
      return initializeGraph(child).addCallback(new SpanCB());
    }

  }
}
