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

import com.google.common.collect.Lists;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import com.google.common.reflect.TypeToken;
import net.opentsdb.auth.AuthState;
import net.opentsdb.common.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.query.TimeSeriesQuery.LogLevel;
import net.opentsdb.query.filter.NamedFilter;
import net.opentsdb.stats.QueryStats;
import net.opentsdb.stats.Span;
import net.opentsdb.utils.Deferreds;

/**
 * A base class for QueryContext's.
 * 
 * @since 3.0
 */
public abstract class BaseQueryContext implements QueryContext {

  protected static final DateTimeFormatter TS_FORMATTER = 
      DateTimeFormatter.ofPattern("kk:mm:ss,SSS").withZone(Const.UTC);
  
  /** The TSDB to which we belong. */
  protected TSDB tsdb;
  
  /** The query we're executing. */
  protected SemanticQuery query;
  
  /** A stats object. */
  protected QueryStats stats;
  
  /** The sinks we'll write to. */
  protected List<QuerySinkConfig> sink_configs;
  
  /** The pipeline. */
  protected LocalPipeline pipeline;
  
  /** The authentication state. */
  protected AuthState auth_state;
  
  /** Our logs. */
  protected List<String> logs;
  
  /** A local span for tracing. */
  protected Span local_span;
  
  /** Returns true if the pipeline is closed. */
  private boolean isClosed;
  
  protected BaseQueryContext(final Builder builder) {
    tsdb = builder.tsdb;
    query = builder.query;
    stats = builder.stats;
    sink_configs = builder.sink_configs;
    if (stats != null && stats.trace() != null) {
      local_span = stats.trace().newSpan("Query Context Initialization")
          .asChildOf(stats.querySpan())
          .start();
    }
    auth_state = builder.auth_state;
    if (stats != null) {
      stats.setQueryContext(this);
    }

    isClosed = false;
    
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
    isClosed = true;
  }
  
  @Override
  public boolean isClosed() {
    return isClosed;
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
  
  @Override
  public TimeSeriesId getId(final long hash, 
                            final TypeToken<? extends TimeSeriesId> type) {
    return pipeline.getId(hash, type);
  }
  
  @Override
  public List<String> logs() {
    return logs != null ? logs : Collections.emptyList();
  }
  
  @Override
  public void logError(final String log) {
    log(LogLevel.ERROR, null, log);
  }
  
  @Override
  public void logError(final QueryNode node, final String log) {
    log(LogLevel.ERROR, node, log);
  }
  
  @Override
  public void logWarn(final String log) {
    log(LogLevel.WARN, null, log);
  }
  
  @Override
  public void logWarn(final QueryNode node, final String log) {
    log(LogLevel.WARN, node, log);
  }
  
  @Override
  public void logInfo(final String log) {
    log(LogLevel.INFO, null, log);
  }
  
  @Override
  public void logInfo(final QueryNode node, final String log) {
    log(LogLevel.INFO, node, log);
  }
  
  @Override
  public void logDebug(final String log) {
    log(LogLevel.DEBUG, null, log);
  }
  
  @Override
  public void logDebug(final QueryNode node, final String log) {
    log(LogLevel.DEBUG, node, log);
  }
  
  @Override
  public void logTrace(final String log) {
    log(LogLevel.TRACE, null, log);
  }
  
  @Override
  public void logTrace(final QueryNode node, final String log) {
    log(LogLevel.TRACE, node, log);
  }
  
  /**
   * Helper to figure out if we need to log or not. Formats similar to Logback.
   * @param level The non-null log level.
   * @param node The optional node to pull an ID from.
   * @param log The log.
   */
  protected void log(final LogLevel level, final QueryNode node, final String log) {
    if (level.ordinal() > query.getLogLevel().ordinal()) {
      return;
    }
    
    synchronized (this) {
      if (logs == null) {
        logs = Lists.newArrayList();
      }
      final StringBuilder buf = new StringBuilder(15 + 6 + 32 + log.length())
          .append(TS_FORMATTER.format(Instant.now()))
          .append(" ")
          .append(level)
          .append("  [")
          .append(node == null ? "None" : node.config().getId())
          .append("] - ")
          .append(log);
      logs.add(buf.toString());
    }
  }
  
  /**
   * Simple pipeline implementation.
   */
  protected class LocalPipeline extends AbstractQueryPipelineContext {

    public LocalPipeline(final QueryContext context, 
                         final List<QuerySink> direct_sinks) {
      super(context);
      if (direct_sinks != null && !direct_sinks.isEmpty()) {
        sinks.addAll(direct_sinks);
      }
    }

    @Override
    public Deferred<Void> initialize(final Span span) {
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
  
  /**
   * Base builder class.
   */
  public static abstract class Builder implements QueryContextBuilder {
    protected TSDB tsdb;
    protected SemanticQuery query;
    protected QueryStats stats;
    protected List<QuerySinkConfig> sink_configs;
    protected List<QuerySink> sinks;
    protected AuthState auth_state;
    
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
    
  }
}
