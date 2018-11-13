// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
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

public class SemanticQueryContext implements QueryContext {

  /** The TSDB to which we belong. */
  private TSDB tsdb;
  
  /** The query we're executing. */
  private SemanticQuery query;
  
  /** A stats object. */
  private QueryStats stats;
  
  /** The sinks we'll write to. */
  private List<QuerySinkConfig> sink_configs;
  
  /** The pipeline. */
  private LocalPipeline pipeline;
  
  /** The authentication state. */
  private AuthState auth_state;
  
  /** A local span for tracing. */
  private Span local_span;
  
  SemanticQueryContext(final Builder builder) {
    tsdb = builder.tsdb;
    query = builder.query;
    stats = builder.stats;
    sink_configs = builder.sink_configs;
    if (stats != null && stats.trace() != null) {
      local_span = stats.trace().newSpan("Query Context Initialization")
          .asChildOf(stats.querySpan())
          .start();
    }
    
    pipeline = new LocalPipeline(this, builder.sinks);
  }
  
  @Override
  public Collection<QuerySink> sinks() {
    return pipeline.sinks();
  }

  @Override
  public QueryMode mode() {
    return query.getMode();
  }

  @Override
  public void fetchNext(Span span) {
    pipeline.fetchNext(span);
  }

  @Override
  public void close() {
    pipeline.close();
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
        return pipeline.initialize(local_span);
      }
    }
    
    if (initializations != null) {
      return Deferred.group(initializations)
          .addBoth(Deferreds.VOID_GROUP_CB)
          .addCallbackDeferring(new FilterCB());
    } else {
      return pipeline.initialize(local_span);
    }
  }
  
  public static Builder newBuilder() {
    return new Builder();
  }
  
  public static class Builder implements QueryContextBuilder {
    private TSDB tsdb;
    private SemanticQuery query;
    private QueryStats stats;
    private List<QuerySinkConfig> sink_configs;
    private List<QuerySink> sinks;
    private AuthState auth_state;
    
    public QueryContextBuilder setTSDB(final TSDB tsdb) {
      this.tsdb = tsdb;
      return this;
    }
    
    @Override
    public QueryContextBuilder setQuery(final TimeSeriesQuery query) {
      if (!(query instanceof SemanticQuery)) {
        throw new IllegalArgumentException("Hey, we want a semantic query here.");
      }
      this.query = (SemanticQuery) query;
      return this;
    }

    @Override
    public QueryContextBuilder setMode(final QueryMode mode) {
      // TODO Auto-generated method stub
      return this;
    }

    @Override
    public QueryContextBuilder setStats(final QueryStats stats) {
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
      return (QueryContext) new SemanticQueryContext(this);
    }
    
  }

  class LocalPipeline extends AbstractQueryPipelineContext {

    public LocalPipeline(final QueryContext context, final List<QuerySink> direct_sinks) {
      super(context);
      if (direct_sinks != null && !direct_sinks.isEmpty()) {
        sinks.addAll(direct_sinks);
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
