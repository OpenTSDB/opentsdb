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
import java.util.Set;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import net.opentsdb.core.TSDB;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.execution.graph.ExecutionGraph;
import net.opentsdb.query.execution.graph.ExecutionGraphNode;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.pojo.Downsampler;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.pojo.Filter;
import net.opentsdb.query.pojo.Metric;
import net.opentsdb.query.pojo.RateOptions;
import net.opentsdb.query.pojo.TagVFilter;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.query.processor.groupby.GroupByConfig;
import net.opentsdb.stats.QueryStats;
import net.opentsdb.stats.Span;

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
  
  /** The list of sinks to callback with results. */
  private List<QuerySink> sinks;
  
  /** The query. */
  private TimeSeriesQuery query;
  
  /** The mode of operation. */
  private QueryMode mode;
  
  /** The stats object. */
  private QueryStats stats;
  
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
  public QueryContextBuilder addQuerySink(final QuerySink listener) {
    if (built) {
      throw new IllegalStateException("Builder was already built.");
    }
    if (sinks == null) {
      sinks = Lists.newArrayListWithExpectedSize(1);
    }
    sinks.add(listener);
    return this;
  }

  @Override
  public QueryContextBuilder setQuerySinks(
      final Collection<QuerySink> listeners) {
    if (built) {
      throw new IllegalStateException("Builder was already built.");
    }
    this.sinks = Lists.newArrayList(listeners);
    return this;
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
  public QueryContext build() {
    if (sinks == null || sinks.isEmpty()) {
      throw new IllegalArgumentException("At least one sink must be provided.");
    }
    if (query == null) {
      throw new IllegalArgumentException("Query cannot be null.");
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
    private QueryPipelineContext context;
    
    /** A local span for tracing. */
    private Span local_span;
    
    public LocalContext() {
      if (stats != null && stats.trace() != null) {
        local_span = stats.trace().newSpan("Query Context Initialization")
            .asChildOf(stats.querySpan())
            .start();
      }
      context = new LocalPipeline(tsdb, query, this, 
          ((SemanticQuery) query).getExecutionGraph(), sinks);
      context.initialize(local_span);
    }
    
    @Override
    public Collection<QuerySink> sinks() {
      return sinks;
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
    public TimeSeriesQuery query() {
      return query;
    }

  }
  
  class LocalPipeline extends AbstractQueryPipelineContext {

    public LocalPipeline(TSDB tsdb, TimeSeriesQuery query, QueryContext context,
        ExecutionGraph execution_graph, Collection<QuerySink> sinks) {
      super(tsdb, query, context, sinks);
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

    @Override
    public String id() {
      return "TsdbV2Pipeline";
    }
    
  }
}
