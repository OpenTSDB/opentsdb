// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.query;

import java.util.Collection;
import java.util.List;

import com.google.common.collect.Lists;

import net.opentsdb.core.TSDB;
import net.opentsdb.stats.QueryStats;
import net.opentsdb.stats.Span;

/**
 * The default implementation of a context builder.
 * 
 * TODO - more work coming on this.
 * 
 * @since 3.0
 */
public class DefaultQueryContextBuilder implements QueryContextBuilder {
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
    final DefaultQueryContextBuilder builder = new DefaultQueryContextBuilder();
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
      if (stats != null && stats.tracer() != null) {
        local_span = stats.tracer().newSpan("Query Context Initialization")
            .asChildOf(stats.querySpan())
            .start();
      }
      context = new TSDBV2Pipeline(tsdb, query, this, sinks);
      context.initialize();
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
    public void fetchNext() {
      context.fetchNext();
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
    
  }
  
}
