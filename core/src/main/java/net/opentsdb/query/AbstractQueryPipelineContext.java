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

import org.jgrapht.experimental.dag.DirectedAcyclicGraph;
import org.jgrapht.experimental.dag.DirectedAcyclicGraph.CycleFoundException;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.traverse.BreadthFirstIterator;
import org.jgrapht.traverse.DepthFirstIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
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
import net.opentsdb.query.execution.graph.ExecutionGraphNode;
import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.stats.Span;
import net.opentsdb.utils.Pair;

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
 * <li>Each {@link ExecutionGraphNode} must have a unique ID within the graph.</li>
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
  
  /** The TSDB to which we belong. */
  protected final TSDB tsdb;
  
  /** The query we're working on. */
  protected final TimeSeriesQuery query;
  
  /** The upstream query context this pipeline context belongs to. */
  protected final QueryContext context;
  
  /** The original user defined execution graph. */
  protected final ExecutionGraph execution_graph;
  
  /** A mutable copy of the nodes. */
  protected final List<ExecutionGraphNode> nodes;
  
  /** The set of query sinks. */
  protected final Collection<QuerySink> sinks;
  
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
  protected DirectedAcyclicGraph<QueryNode, DefaultEdge> graph;
  
  /**
   * Default ctor.
   * @param tsdb A non-null TSDB to work with.
   * @param query A non-null query to execute.
   * @param context The user's query context.
   * @param sinks A collection of one or more sinks to publish to.
   * @throws IllegalArgumentException if any argument was null.
   */
  public AbstractQueryPipelineContext(final TSDB tsdb, 
                                      final TimeSeriesQuery query, 
                                      final QueryContext context,
                                      final ExecutionGraph execution_graph,
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
    if (execution_graph == null) {
      throw new IllegalArgumentException("Execution graph cannot be null.");
    }
    if (sinks == null || sinks.isEmpty()) {
      throw new IllegalArgumentException("The sinks cannot be null or empty");
    }
    this.tsdb = tsdb;
    this.query = query;
    this.context = context;
    this.execution_graph = execution_graph;
    this.sinks = sinks;
    graph = new DirectedAcyclicGraph<QueryNode, DefaultEdge>(DefaultEdge.class);
    graph.addVertex(this);
    roots = Sets.newHashSet();
    sources = Lists.newArrayList();
    nodes = Lists.newArrayList(execution_graph.getNodes());
  }
  
  @Override
  public TSDB tsdb() {
    return tsdb;
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
  public Collection<TimeSeriesDataSource> downstreamSources(final QueryNode node) {
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
    final Set<TimeSeriesDataSource> downstreams = Sets.newHashSetWithExpectedSize(
        downstream.size());
    for (final DefaultEdge e : downstream) {
      final QueryNode target = graph.getEdgeTarget(e);
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
    if (!graph.containsVertex(node)) {
      throw new IllegalArgumentException("The given node wasn't in this graph: " 
          + node);
    }
    
    final Set<DefaultEdge> upstream = graph.incomingEdgesOf(node);
    if (upstream.isEmpty()) {
      return Collections.emptyList();
    }
    
    List<QueryNode> upstreams = null;
    for (final DefaultEdge e : upstream) {
      final QueryNode source = graph.getEdgeSource(e);
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
    if (!graph.containsVertex(node)) {
      throw new IllegalArgumentException("The given node wasn't in this graph: " 
          + node);
    }
    
    final Set<DefaultEdge> downstream = graph.outgoingEdgesOf(node);
    if (downstream.isEmpty()) {
      return Collections.emptyList();
    }
    
    List<QueryNode> downstreams = null;
    for (final DefaultEdge e : downstream) {
      final QueryNode target = graph.getEdgeTarget(e);
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
  public void fetchNext(final Span span) {
    if (context.mode() == QueryMode.SINGLE ||
        context.mode() == QueryMode.BOUNDED_SERVER_SYNC_STREAM || 
        context.mode() == QueryMode.CONTINOUS_SERVER_SYNC_STREAM ||
        context.mode() == QueryMode.BOUNDED_SERVER_ASYNC_STREAM ||
        context.mode() == QueryMode.CONTINOUS_SERVER_ASYNC_STREAM) {
      for (final TimeSeriesDataSource source : sources) {
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
      if (source_idx >= sources.size()) {
        source_idx = 0;
      }
      try {
        sources.get(source_idx++).fetchNext(span);
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
          single_results.addResults(next);
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
   * A helper to initialize the nodes in depth-first order.
   */
  protected void initializeGraph(final Span span) {
    final Span child;
    if (span != null) {
      child = span.newChild(getClass() + ".initializeGraph")
                  .withTag("graphId", execution_graph.getId() == null ? "null" : execution_graph.getId())
                  .start();
    } else {
      child = null;
    }
    
    final Map<String, QueryNodeConfig> node_configs = 
        execution_graph.nodeConfigs();
    final Map<String, QueryNode> map = 
        Maps.newHashMapWithExpectedSize(execution_graph.getNodes().size());
    final Set<String> unique_ids = 
        Sets.newHashSetWithExpectedSize(execution_graph.getNodes().size());
    List<Pair<MultiQueryNodeFactory, ExecutionGraphNode>> multis = null;
    
    // first pass to instantiate the nodes.
    for (final ExecutionGraphNode node : nodes) {
      if (unique_ids.contains(node.getId())) {
        throw new IllegalArgumentException("The node id \"" 
            + node.getId() + "\" appeared more than once in the "
            + "graph. It must be unique.");
      }
      unique_ids.add(node.getId());
      
      final QueryNodeFactory factory;
      if (!Strings.isNullOrEmpty(node.getType())) {
        factory = tsdb.getRegistry().getQueryNodeFactory(node.getType().toLowerCase());
      } else {
        factory = tsdb.getRegistry().getQueryNodeFactory(node.getId().toLowerCase());
      }
      if (factory == null) {
        throw new IllegalArgumentException("No node factory found for "
            + "configuration " + node);
      }
      
      // we can only handle singles here, multis we run through in a second
      // pass.
      if (!(factory instanceof SingleQueryNodeFactory)) {
        if (multis == null) {
          multis = Lists.newArrayList();
        }
        multis.add(new Pair<MultiQueryNodeFactory, ExecutionGraphNode>(
            (MultiQueryNodeFactory) factory, node));
        continue;
      }
      
      final QueryNode query_node;
      QueryNodeConfig node_config = node.getConfig() != null ? 
          node.getConfig() : node_configs.get(node.getId());
      if (node_config == null) {
        node_config = node_configs.get(node.getType());
      }
      if (node_config != null) {
        query_node = ((SingleQueryNodeFactory) factory)
            .newNode(this, node.getId(), node_config);
      } else {
        query_node = ((SingleQueryNodeFactory) factory)
            .newNode(this, node.getId());
      }
      if (query_node == null) {
        throw new IllegalStateException("Factory returned a null "
            + "instance for " + node);
      }
      
      map.put(node.getId(), query_node);
      
      graph.addVertex(query_node);
    }
    
    // if there are multis that change the DAG, let's set em up now
    if (multis != null) {
      for (final Pair<MultiQueryNodeFactory, ExecutionGraphNode> pair : multis) {
        QueryNodeConfig node_config = pair.getValue().getConfig() != null ? 
            pair.getValue().getConfig() : node_configs.get(pair.getValue().getId());
        if (node_config == null) {
          node_config = node_configs.get(pair.getValue().getType());
        }
        if (node_config == null) {
          throw new IllegalArgumentException("No config supplied for "
              + "node: " + pair.getValue().getId());
        }
        
        final Collection<QueryNode> query_nodes = pair.getKey().newNodes(
            this, pair.getValue().getId(), node_config, nodes);
        if (query_nodes == null || query_nodes.isEmpty()) {
          throw new IllegalStateException("Factory returned a null or "
              + "empty list of nodes for " + pair.getValue().getId());
        }
        for (final QueryNode node : query_nodes) {
          if (node == null) {
            throw new IllegalStateException("Factory returned a null "
                + "node for " + pair.getValue().getId());
          }
          
          map.put(node.id(), node);
          graph.addVertex(node);
        }
        
        // Important! We have to remove the original node config for
        // debugging so we know it was replaced.
        nodes.remove(pair.getValue());
      }
    }
    
    // second (or third) pass to build the graph.
    for (final ExecutionGraphNode node : nodes) {
      if (node != null && 
          node.getSources() != null && 
          !node.getSources().isEmpty()) {
        final QueryNode query_node = map.get(node.getId());
        for (final String source : node.getSources()) {
          try {
            graph.addDagEdge(query_node, map.get(source));
          } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Failed to add node: " 
                + node, e);
          } catch (CycleFoundException e) {
            throw new IllegalArgumentException("A cycle was detected "
                + "adding node: " + node, e);
          }
        }
      }
    }
    
    // depth first initiation of the executors since we have to init
    // the ones without any downstream dependencies first.
    final DepthFirstIterator<QueryNode, DefaultEdge> iterator = 
        new DepthFirstIterator<QueryNode, DefaultEdge>(graph);
    final Set<TimeSeriesDataSource> source_set = Sets.newHashSet();
    while (iterator.hasNext()) {
      final QueryNode node = iterator.next();
      
      final Set<DefaultEdge> incoming = graph.incomingEdgesOf(node);
      if (incoming.size() == 0 && node != this) {
        try {
          graph.addDagEdge(this, node);
        } catch (CycleFoundException e) {
          throw new IllegalArgumentException(
              "Invalid graph configuration", e);
        }
        roots.add(node);
      }
      
      if (node instanceof TimeSeriesDataSource) {
        source_set.add((TimeSeriesDataSource) node);
      }
      
      if (node != this) {
        node.initialize(child);
      }
    }
    sources.addAll(source_set);
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
    
    /** The type of ID token pulled from the result. */
    private final TypeToken<? extends TimeSeriesId> id_type;
    
    /** The max resolution for the results. */
    private ChronoUnit resolution;
    
    /** The sequence ID to return. */
    private long sequence_id = 0;
    
    /** Rollup config if a result gave us one. */
    private RollupConfig rollup_config;
    
    public CumulativeQueryResult(final QueryResult result) {
      series = Lists.newArrayList(result.timeSeries());
      time_specification = result.timeSpecification();
      id_type = result.idType();
      resolution = result.resolution();
      sequence_id = result.sequenceId();
      rollup_config = result.rollupConfig();
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
      return sequence_id;
    }

    @Override
    public QueryNode source() {
      return AbstractQueryPipelineContext.this;
    }

    @Override
    public TypeToken<? extends TimeSeriesId> idType() {
      return id_type;
    }
    
    @Override
    public ChronoUnit resolution() {
      return resolution;
    }
    
    @Override
    public RollupConfig rollupConfig() {
      return rollup_config;
    }
    
    @Override
    public void close() {
      // No-Op
    }
    
    /**
     * Add the resulting time series to the accumulation.
     * @param next A non-null result.
     */
    protected void addResults(final QueryResult next) {
      if (time_specification != null || next.timeSpecification() != null) {
        if ((time_specification == null && next.timeSpecification() != null) ||
            (time_specification != null && next.timeSpecification() == null)) {
          throw new IllegalStateException("Received a different time "
              + "specification in query result: " + next);
        }
        
        if (!time_specification.equals(next.timeSpecification())) {
          throw new IllegalStateException("Received a different time "
              + "specification in query result: " + next);
        }
      }
      if (next.sequenceId() > sequence_id) {
        sequence_id = next.sequenceId();
      }
      if (next.resolution().ordinal() < resolution.ordinal()) {
        resolution = next.resolution();
      }
      series.addAll(next.timeSeries());
    }

    
  }
}
