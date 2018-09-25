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

import net.opentsdb.core.TSDB;
import net.opentsdb.stats.QueryStats;
import net.opentsdb.stats.Span;

public class SemanticQueryContext implements QueryContext {

  /** The TSDB to which we belong. */
  private TSDB tsdb;
  
  /** The query we're executing. */
  private SemanticQuery query;
  
  /** A stats object. */
  private QueryStats stats;
  
  /** The pipeline. */
  private LocalPipeline pipeline;
  
  /** A local span for tracing. */
  private Span local_span;
  
  SemanticQueryContext(final Builder builder) {
    tsdb = builder.tsdb;
    query = builder.query;
    stats = builder.stats;
    if (stats != null && stats.trace() != null) {
      local_span = stats.trace().newSpan("Query Context Initialization")
          .asChildOf(stats.querySpan())
          .start();
    }
    
    pipeline = new LocalPipeline(this);
    pipeline.initialize(local_span);
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
  public TimeSeriesQuery query() {
    return query;
  }
  
  @Override
  public TSDB tsdb() {
    return tsdb;
  }
  
  public static Builder newBuilder() {
    return new Builder();
  }
  
  public static class Builder implements QueryContextBuilder {
    private TSDB tsdb;
    private SemanticQuery query;
    private QueryStats stats;
    
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
    public QueryContext build() {
      return (QueryContext) new SemanticQueryContext(this);
    }
    
  }

  class LocalPipeline extends AbstractQueryPipelineContext {

    public LocalPipeline(final QueryContext context) {
      super(context);
    }

    @Override
    public void initialize(Span span) {
      final Span child;
      if (span != null) {
        child = span.newChild(getClass().getSimpleName() + ".initialize()")
                     .start();
      } else {
        child = null;
      }
      initializeGraph(child);
      if (child != null) {
        child.setSuccessTags().finish();
      }
    }
    
  }
  
}
