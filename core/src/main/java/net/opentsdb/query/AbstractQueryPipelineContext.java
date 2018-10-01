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

import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.traverse.BreadthFirstIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;

import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.query.execution.graph.ExecutionGraph;
import net.opentsdb.query.plan.DefaultQueryPlanner;
import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.stats.Span;

/**
 * A useful base class for {@link QueryPipelineContext}s that stores references
 * to the TSDB, query, graph, roots and sinks.
 * <b>Warning:</b> Don't forgot to add an edge from this to the root of the 
 * query nodes in your graph or the query results won't make it to the sinks.
 * 
 * TODO - need to handle the case where the sources fail to call on complete.
 * 
 * TODO - assumptions made: All query results in the SINGLE mode will have the 
 * same timespecs. This may not be the case.
 * 
 * <b>Invariants:</b>
 * <ul>
 * <li>Each {@link ExecutionGraphNode} must have a unique ID within the plan.graph().</li>
 * <li>The graph must have at most one sink that will be used to execution a
 * query.</li>
 * <li>The graph must be a non-cyclical DAG.</li>
 * </ul>
 * 
 * @since 3.0
 */
public abstract class AbstractQueryPipelineContext implements QueryPipelineContext {
  private static final Logger LOG = LoggerFactory.getLogger(
      AbstractQueryPipelineContext.class);
  
  /** The list of sink nodes. */
  protected final List<QuerySink> sinks;
  
  /** A list of query results expected per type so we can call close on
   * sinks when we've passed them through. */
  protected final Map<String, AtomicInteger> countdowns;
  
  /** The upstream query context this pipeline context belongs to. */
  protected final QueryContext context;

  /** The query plan. */
  protected final DefaultQueryPlanner plan;
  
  /** Used to iterate over sources when in a client streaming mode. */
  protected int source_idx = 0;
  
  /**
   * Default ctor.
   * @param context The user's query context.
   * @throws IllegalArgumentException if any argument was null.
   */
  public AbstractQueryPipelineContext(final QueryContext context) {
    if (context == null) {
      throw new IllegalArgumentException("The context cannot be null.");
    }
    this.context = context;
    if (context.sinkConfigs() == null || 
        context.sinkConfigs().isEmpty()) {
      throw new IllegalArgumentException("The query must have at least "
          + "one sink config.");
    }
    plan = new DefaultQueryPlanner(this, (QueryNode) this);
    sinks = Lists.newArrayListWithExpectedSize(1);
    countdowns = Maps.newHashMap();
  }
  
  @Override
  public TSDB tsdb() {
    return context.tsdb();
  }
  
  @Override
  public QueryContext queryContext() {
    return context;
  }

  @Override
  public QueryPipelineContext pipelineContext() {
    return this;
  }
  
  @Override
  public ExecutionGraph executionGraph() {
    return context.query().getExecutionGraph();
  }
  
  @Override
  public Collection<QueryNode> upstream(final QueryNode node) {
    if (node == null) {
      throw new IllegalArgumentException("Node cannot be null.");
    }
    if (!plan.graph().containsVertex(node)) {
      throw new IllegalArgumentException("The given node wasn't in this graph: " 
          + node);
    }
    final Set<DefaultEdge> upstream = plan.graph().incomingEdgesOf(node);
    if (upstream.isEmpty()) {
      return Collections.emptyList();
    }
    final List<QueryNode> listeners = Lists.newArrayListWithCapacity(
        upstream.size());
    for (final DefaultEdge e : upstream) {
      listeners.add(plan.graph().getEdgeSource(e));
    }
    return listeners;
  }
  
  @Override
  public Collection<QueryNode> downstream(final QueryNode node) {
    if (node == null) {
      throw new IllegalArgumentException("Node cannot be null.");
    }
    if (!plan.graph().containsVertex(node)) {
      throw new IllegalArgumentException("The given node wasn't in this graph: " 
          + node);
    }
    final Set<DefaultEdge> downstream = plan.graph().outgoingEdgesOf(node);
    if (downstream.isEmpty()) {
      return Collections.emptyList();
    }
    final List<QueryNode> downstreams = Lists.newArrayListWithCapacity(
        downstream.size());
    for (final DefaultEdge e : downstream) {
      downstreams.add(plan.graph().getEdgeTarget(e));
    }
    return downstreams;
  }
  
  @Override
  public Collection<TimeSeriesDataSource> downstreamSources(final QueryNode node) {
    if (node == null) {
      throw new IllegalArgumentException("Node cannot be null.");
    }
    if (!plan.graph().containsVertex(node)) {
      throw new IllegalArgumentException("The given node wasn't in this graph: " 
          + node);
    }
    final Set<DefaultEdge> downstream = plan.graph().outgoingEdgesOf(node);
    if (downstream.isEmpty()) {
      return Collections.emptyList();
    }
    final Set<TimeSeriesDataSource> downstreams = Sets.newHashSetWithExpectedSize(
        downstream.size());
    for (final DefaultEdge e : downstream) {
      final QueryNode target = plan.graph().getEdgeTarget(e);
      if (downstreams.contains(target)) {
        continue;
      }
      
      if (target instanceof TimeSeriesDataSource) {
        downstreams.add((TimeSeriesDataSource) target);
      } else {
        downstreams.addAll(downstreamSources(target));
      }
    }
    return downstreams;
  }
  
  @Override
  public Collection<QueryNode> upstreamOfType(final QueryNode node, 
                                              final Class<? extends QueryNode> type) {
    if (node == null) {
      throw new IllegalArgumentException("Node cannot be null.");
    }
    if (type == null) {
      throw new IllegalArgumentException("Type cannot be null.");
    }
    if (!plan.graph().containsVertex(node)) {
      throw new IllegalArgumentException("The given node wasn't in this graph: " 
          + node);
    }
    
    final Set<DefaultEdge> upstream = plan.graph().incomingEdgesOf(node);
    if (upstream.isEmpty()) {
      return Collections.emptyList();
    }
    
    List<QueryNode> upstreams = null;
    for (final DefaultEdge e : upstream) {
      final QueryNode source = plan.graph().getEdgeSource(e);
      if (source.getClass().equals(type)) {
        if (upstreams == null) {
          upstreams = Lists.newArrayList();
        }
        upstreams.add(source);
      } else {
        final Collection<QueryNode> upstream_of_source = 
            upstreamOfType(source, type);
        if (!upstream_of_source.isEmpty()) {
          if (upstreams == null) {
            upstreams = Lists.newArrayList();
          }
          upstreams.addAll(upstream_of_source);
        }
      }
    }
    return upstreams == null ? Collections.emptyList() : upstreams;
  }
  
  @Override
  public Collection<QueryNode> downstreamOfType(final QueryNode node, 
                                                final Class<? extends QueryNode> type) {
    if (node == null) {
      throw new IllegalArgumentException("Node cannot be null.");
    }
    if (type == null) {
      throw new IllegalArgumentException("Type cannot be null.");
    }
    if (!plan.graph().containsVertex(node)) {
      throw new IllegalArgumentException("The given node wasn't in this graph: " 
          + node);
    }
    
    final Set<DefaultEdge> downstream = plan.graph().outgoingEdgesOf(node);
    if (downstream.isEmpty()) {
      return Collections.emptyList();
    }
    
    List<QueryNode> downstreams = null;
    for (final DefaultEdge e : downstream) {
      final QueryNode target = plan.graph().getEdgeTarget(e);
      if (target.getClass().equals(type)) {
        if (downstreams == null) {
          downstreams = Lists.newArrayList();
        }
        downstreams.add(target);
      } else {
        final Collection<QueryNode> downstream_of_target = 
            downstreamOfType(target, type);
        if (!downstream_of_target.isEmpty()) {
          if (downstreams == null) {
            downstreams = Lists.newArrayList();
          }
          downstreams.addAll(downstream_of_target);
        }
      }
    }
    return downstreams == null ? Collections.emptyList() : downstreams;
  }
  
  @Override
  public Collection<QuerySink> sinks() {
    return sinks;
  }
  
  @Override
  public TimeSeriesQuery query() {
    return context.query();
  }
  
  @Override
  public void close() {
    final BreadthFirstIterator<QueryNode, DefaultEdge> bf_iterator = 
        new BreadthFirstIterator<QueryNode, DefaultEdge>(plan.graph());
    while (bf_iterator.hasNext()) {
      final QueryNode node = bf_iterator.next();
      if (node == this) {
        continue;
      }
      try {
        node.close();
      } catch (Exception e) {
        LOG.warn("Failed to close query node: " + node, e);
      }
    }
  }
  
  @Override
  public void fetchNext(final Span span) {
    if (context.mode() == QueryMode.SINGLE ||
        context.mode() == QueryMode.BOUNDED_SERVER_SYNC_STREAM || 
        context.mode() == QueryMode.CONTINOUS_SERVER_SYNC_STREAM ||
        context.mode() == QueryMode.BOUNDED_SERVER_ASYNC_STREAM ||
        context.mode() == QueryMode.CONTINOUS_SERVER_ASYNC_STREAM) {
      for (final TimeSeriesDataSource source : plan.sources()) {
        try {
          source.fetchNext(span);
        } catch (Exception e) {
          LOG.error("Failed to fetch next from source: " 
              + source, e);
          onError(e);
          break;
        }
      }
      return;
    }
    
    synchronized(this) {
      if (source_idx >= plan.sources().size()) {
        source_idx = 0;
      }
      try {
        plan.sources().get(source_idx++).fetchNext(span);
      } catch (Exception e) {
        LOG.error("Failed to fetch next from source: " 
            + plan.sources().get(source_idx - 1), e);
        onError(e);
      }
    }
  }
  
  @Override
  public void onComplete(final QueryNode downstream, 
                         final long final_sequence,
                         final long total_sequences) {
    // TODO - handle this with streaming.
  }
  
  @Override
  public void onNext(final QueryResult next) {
    final ResultWrapper wrapped = new ResultWrapper(next);
    for (final QuerySink sink : sinks) {
      try {
        sink.onNext(wrapped);
      } catch (Throwable e) {
        LOG.error("Exception thrown passing results to sink: " + sink, e);
        // TODO - should we kill the query here?
      }
    }
  }
  
  @Override
  public void onError(final Throwable t) {
    for (final QuerySink sink : sinks) {
      try {
        sink.onError(t);
      } catch (Exception e) {
        LOG.error("Exception thrown passing exception to sink: " + sink, e);
      }
    }
    // TODO - decide if we should *auto* close here.
  }
  
  @Override
  public QueryNodeConfig config() {
    return null;
  }
  
  /** @return The planner. */
  public DefaultQueryPlanner plan() {
    return plan;
  }
  
  /**
   * A helper to initialize the nodes in depth-first order.
   */
  protected void initializeGraph(final Span span) {
    final Span child;
    if (span != null) {
      child = span.newChild(getClass() + ".initializeGraph")
        .withTag("graphId", 
            context.query().getExecutionGraph().getId() == null ? 
                "null" : context.query().getExecutionGraph().getId())
        .start();
    } else {
      child = null;
    }
    plan.plan(child);
    
    // setup sinks if the graph is happy
    for (final QuerySinkConfig config : context.sinkConfigs()) {
      final QuerySinkFactory factory = context.tsdb().getRegistry()
          .getPlugin(QuerySinkFactory.class, config.getId());
      if (factory == null) {
        throw new IllegalArgumentException("No sink factory found for " 
            + config.getId());
      }
      
      final QuerySink sink = factory.newSink(context, config);
      if (sink == null) {
        throw new IllegalArgumentException("Factory returned a null sink for " 
            + config.getId());
      }
      sinks.add(sink);
    }
    
    for (final String source : plan.serializationSources()) {
      countdowns.put(source, new AtomicInteger(sinks.size()));
    }
    
    if (child != null) {
      child.setSuccessTags().finish();
    }
  }
  
  /**
   * A helper to determine if the stream is finished and calls the sink's 
   * {@link QuerySink#onComplete()} method.
   * <b>NOTE:</b> This method must be synchronized.
   */
  protected void checkComplete() {
    for (final AtomicInteger integer : countdowns.values()) {
      if (integer.get() > 0) {
        return;
      }
    }
    
    // done!
    for (final QuerySink sink : sinks) {
      sink.onComplete();
    }
  }
  
  /**
   * A simple pass-through wrapper that will decrement the proper counter
   * when the result is closed.
   */
  private class ResultWrapper implements QueryResult {
    
    private final QueryResult result;
    
    ResultWrapper(final QueryResult result) {
      this.result = result;
    }

    @Override
    public TimeSpecification timeSpecification() {
      return result.timeSpecification();
    }

    @Override
    public Collection<TimeSeries> timeSeries() {
      return result.timeSeries();
    }

    @Override
    public long sequenceId() {
      return result.sequenceId();
    }

    @Override
    public QueryNode source() {
      return result.source();
    }

    @Override
    public TypeToken<? extends TimeSeriesId> idType() {
      return result.idType();
    }

    @Override
    public ChronoUnit resolution() {
      return result.resolution();
    }

    @Override
    public RollupConfig rollupConfig() {
      return result.rollupConfig();
    }

    @Override
    public String dataSource() {
      return result.dataSource();
    }
    
    @Override
    public void close() {
      if (result.source().config() instanceof QuerySourceConfig ||
          result.source().config().joins()) {
        countdowns.get(result.dataSource()).decrementAndGet();
      } else {
        countdowns.get(result.source().config().getId() + ":" 
            + result.dataSource()).decrementAndGet();
      }
      checkComplete();
    }
    
  }
  
}
