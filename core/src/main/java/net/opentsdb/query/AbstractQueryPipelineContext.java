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
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.jgrapht.experimental.dag.DirectedAcyclicGraph;
import org.jgrapht.experimental.dag.DirectedAcyclicGraph.CycleFoundException;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.traverse.BreadthFirstIterator;
import org.jgrapht.traverse.DepthFirstIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import net.opentsdb.core.DefaultTSDB;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.data.TimeSpecification;

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
 * @since 3.0
 */
public abstract class AbstractQueryPipelineContext implements QueryPipelineContext {
  private static final Logger LOG = LoggerFactory.getLogger(
      AbstractQueryPipelineContext.class);
  
  /** The TSDB to which we belong. */
  protected final DefaultTSDB tsdb;
  
  /** The query we're working on. */
  protected TimeSeriesQuery query;
  
  /** The upstream query context this pipeline context belongs to. */
  protected QueryContext context;
  
  /** The set of query sinks. */
  protected Collection<QuerySink> sinks;
  
  /** The set of node roots to link to the sinks. */
  protected Set<QueryNode> roots;
  
  /** The list of source nodes. */
  protected List<TimeSeriesDataSource> sources;
  
  /** The cumulative result object when operating in {@link QueryMode#SINGLE}. */
  protected CumulativeQueryResult single_results;
  
  /** Used to iterate over sources when in a client streaming mode. */
  protected int source_idx = 0;
  
  /** Tracks the total number of sequences sent by the downstream sources so
   * we know when to call {@link QuerySink#onComplete()}.
   */
  protected long total_sequences;
  
  /** Used in a streaming mode to track how many roots are complete. */
  protected int completed_downstream;
  
  /** Used in a sync streaming mode to track how many sinks are done. */
  protected int completed_sinks;
  
  /** The graph of query nodes. 
   * PRIVATE so that we can swap out the graph implementation at a later date.
   */
  private DirectedAcyclicGraph<QueryNode, DefaultEdge> graph;
  
  /**
   * Default ctor.
   * @param tsdb A non-null TSDB to work with.
   * @param query A non-null query to execute.
   * @param context The user's query context.
   * @param sinks A collection of one or more sinks to publish to.
   * @throws IllegalArgumentException if any argument was null.
   */
  public AbstractQueryPipelineContext(final DefaultTSDB tsdb, 
                                      final TimeSeriesQuery query, 
                                      final QueryContext context,
                                      final Collection<QuerySink> sinks) {
    if (tsdb == null) {
      throw new IllegalArgumentException("TSDB object cannot be null.");
    }
    if (query == null) {
      throw new IllegalArgumentException("The query cannot be null.");
    }
    if (context == null) {
      throw new IllegalArgumentException("The context cannot be null.");
    }
    if (sinks == null || sinks.isEmpty()) {
      throw new IllegalArgumentException("The sinks cannot be null or empty");
    }
    this.tsdb = tsdb;
    this.query = query;
    this.context = context;
    graph = new DirectedAcyclicGraph<QueryNode, DefaultEdge>(DefaultEdge.class);
    graph.addVertex(this);
    this.sinks = sinks;
    roots = Sets.newHashSet();
    sources = Lists.newArrayList();
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
  public Collection<QueryNode> upstream(final QueryNode node) {
    if (node == null) {
      throw new IllegalArgumentException("Node cannot be null.");
    }
    if (!graph.containsVertex(node)) {
      throw new IllegalArgumentException("The given node wasn't in this graph: " 
          + node);
    }
    final Set<DefaultEdge> upstream = graph.incomingEdgesOf(node);
    if (upstream.isEmpty()) {
      return Collections.emptyList();
    }
    final List<QueryNode> listeners = Lists.newArrayListWithCapacity(
        upstream.size());
    for (final DefaultEdge e : upstream) {
      listeners.add(graph.getEdgeSource(e));
    }
    return listeners;
  }
  
  @Override
  public Collection<QueryNode> downstream(final QueryNode node) {
    if (node == null) {
      throw new IllegalArgumentException("Node cannot be null.");
    }
    if (!graph.containsVertex(node)) {
      throw new IllegalArgumentException("The given node wasn't in this graph: " 
          + node);
    }
    final Set<DefaultEdge> downstream = graph.outgoingEdgesOf(node);
    if (downstream.isEmpty()) {
      return Collections.emptyList();
    }
    final List<QueryNode> downstreams = Lists.newArrayListWithCapacity(
        downstream.size());
    for (final DefaultEdge e : downstream) {
      downstreams.add(graph.getEdgeTarget(e));
    }
    return downstreams;
  }
  
  @Override
  public Collection<QuerySink> sinks() {
    return sinks;
  }
  
  @Override
  public Collection<QueryNode> roots() {
    return roots;
  }
  
  @Override
  public TimeSeriesQuery query() {
    return query;
  }
  
  @Override
  public void close() {
    final BreadthFirstIterator<QueryNode, DefaultEdge> bf_iterator = 
        new BreadthFirstIterator<QueryNode, DefaultEdge>(graph);
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
  public void fetchNext() {
    if (context.mode() == QueryMode.SINGLE ||
        context.mode() == QueryMode.BOUNDED_SERVER_SYNC_STREAM || 
        context.mode() == QueryMode.CONTINOUS_SERVER_SYNC_STREAM ||
        context.mode() == QueryMode.BOUNDED_SERVER_ASYNC_STREAM ||
        context.mode() == QueryMode.CONTINOUS_SERVER_ASYNC_STREAM) {
      for (final TimeSeriesDataSource source : sources) {
        source.fetchNext();
      }
      return;
    }
    
    synchronized(this) {
      if (source_idx >= sources.size()) {
        source_idx = 0;
      }
      try {
        sources.get(source_idx++).fetchNext();
      } catch (Exception e) {
        LOG.error("Failed to fetch next from source: " 
            + sources.get(source_idx - 1), e);
        onError(e);
      }
    }
  }
  
  @Override
  public void onComplete(final QueryNode downstream, 
                         final long final_sequence,
                         final long total_sequences) {
    synchronized(this) {
      this.total_sequences += total_sequences;
      completed_downstream++;
      checkComplete();
    }
  }
  
  @Override
  public void onNext(final QueryResult next) {
    if (context.mode() == QueryMode.SINGLE) {
      synchronized(this) {
        if (single_results == null) {
          single_results = new CumulativeQueryResult(next);
        } else {
          single_results.addResults(next.timeSeries());
        }
      }
      try {
        next.close();
      } catch (Exception e) {
        LOG.error("Failed to close result: " + next, e);
      }
    } else {
      for (final QuerySink sink : sinks) {
        try {
          sink.onNext(next);
        } catch (Throwable e) {
          LOG.error("Exception thrown passing results to sink: " + sink, e);
          // TODO - should we kill the query here?
        }
      }
    }
    
    synchronized(this) {
      completed_sinks++;
      checkComplete();
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
  
  /**
   * Adds a node to the execution graph.
   * @param node The non-null node to add.
   */
  protected void addVertex(final QueryNode node) {
    if (node == null) {
      throw new IllegalArgumentException("Node cannot be null.");
    }
    graph.addVertex(node);
  }
  
  /**
   * Links the source to the target in the graph.
   * @param source A non-null source node.
   * @param target A non-null target node.
   */
  protected void addDagEdge(final QueryNode source, final QueryNode target) {
    if (source == null) {
      throw new IllegalArgumentException("Source cannot be null.");
    }
    if (target == null) {
      throw new IllegalArgumentException("Target cannot be null.");
    }
    try {
      graph.addDagEdge(source, target);
    } catch (CycleFoundException e) {
      throw new IllegalArgumentException("Cycles are not allowed", e);
    }
  }
  
  /**
   * A helper to initialize the nodes in depth-first order.
   */
  protected void initializeGraph() {
    if (graph.vertexSet().size() == 1) {
      throw new IllegalStateException("Graph cannot be empty (with only the context).");
    }
    final DepthFirstIterator<QueryNode, DefaultEdge> df_iterator = 
      new DepthFirstIterator<QueryNode, DefaultEdge>(graph);
    while (df_iterator.hasNext()) {
      final QueryNode node = df_iterator.next();
      if (node == this) {
        continue;
      }
      node.initialize();
      final Set<DefaultEdge> incoming = graph.incomingEdgesOf(node);
      if (incoming.size() == 1 && graph.getEdgeSource(incoming.iterator().next()) == this) {
        roots.add(node);
      }
      if (node instanceof TimeSeriesDataSource) {
        sources.add((TimeSeriesDataSource) node);
      }
    }
  }

  /**
   * A helper to determine if the stream is finished and calls the sink's 
   * {@link QuerySink#onComplete()} method.
   * <b>NOTE:</b> This method must be synchronized.
   */
  protected void checkComplete() {
    if (completed_sinks >= total_sequences &&
        completed_downstream >= roots.size()) {
      for (final QuerySink sink : sinks) {
        if (context.mode() == QueryMode.SINGLE && single_results != null) {
          try {
            sink.onNext(single_results);
          } catch (Throwable e) {
            LOG.error("Failed to send cumulative results to sink: " + sink, e);
          }
        }
        
        try {
          sink.onComplete();
        } catch (Exception e) {
          LOG.error("Failed to close sink: " + sink, e);
        }
      }
    }
  }
  
  /**
   * A class that accumulates the results for multi-source queries when the mode
   * is set to {@link QueryMode#SINGLE}.
   */
  class CumulativeQueryResult implements QueryResult {
    /** The time spec pulled from the first result. 
     * TODO - if the sources have different specs, this needs fixing. */
    private final TimeSpecification time_specification;
    
    /** The accumulation of time series. */
    private final List<TimeSeries> series;
    
    public CumulativeQueryResult(final QueryResult result) {
      series = Lists.newArrayList(result.timeSeries());
      time_specification = result.timeSpecification();
    }
    
    @Override
    public TimeSpecification timeSpecification() {
      return time_specification;
    }

    @Override
    public Collection<TimeSeries> timeSeries() {
      return series;
    }

    @Override
    public long sequenceId() {
      return 0;
    }

    @Override
    public QueryNode source() {
      return AbstractQueryPipelineContext.this;
    }

    @Override
    public void close() {
      // No-Op
    }
    
    /**
     * Add the resulting time series to the accumulation.
     * @param results A non-null collection of results. May be empty.
     */
    protected void addResults(final Collection<TimeSeries> results) {
      series.addAll(results);
    }
  }
}
