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
package net.opentsdb.query.execution.graph;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jgrapht.experimental.dag.DirectedAcyclicGraph;
import org.jgrapht.experimental.dag.DirectedAcyclicGraph.CycleFoundException;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.traverse.DepthFirstIterator;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.query.execution.QueryExecutor;
import net.opentsdb.query.execution.QueryExecutorFactory;

/**
 * An execution graph that defines a set of executors, default configs and the
 * path of execution. The class must be serialiazble so that one or more 
 * graphs can be setup per TSD and consumed or overridden at query time.
 * <p>
 * Note that the builder is used to instantiate the object (via code or JSON
 * deserialization) but the graph will not be ready to use until 
 * {@link #initialize(TSDB)} is called.
 * <p>
 * If the graph is part of a cluster or a sub config, give a unique prefix
 * to {@link #initialize(TSDB, String)} and all executors and configs will be
 * pre-pended with the prefix. That will allow users to override configs at
 * query time.
 * <p>
 * Note the executor may be registered with the TSD but must be done so outside
 * of this class.
 * <p>
 * <b>Invariants:</b>
 * <ul>
 * <li>Each {@link ExecutionGraphNode} must have a unique ID within the graph.</li>
 * <li>The graph must have at most one sink that will be used to execution a
 * query.</li>
 * <li>The graph must be an uncyclical DAG.</li>
 * </ul>
 * @since 3.0
 */
@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonDeserialize(builder = ExecutionGraph.Builder.class)
public class ExecutionGraph implements Comparable<ExecutionGraph> {
  /** The TSDB to which this graph belongs. */
  protected TSDB tsdb;
  
  /** The ID of this execution graph. */
  protected String id;
  
  /** The list of nodes given by the user or config. */
  protected List<ExecutionGraphNode> nodes;
  
  /** The instantiated executors for each node. Used when closing. */
  protected final Map<String, QueryExecutor<?>> executors;
  
  /** The graph of node IDs .*/
  protected final DirectedAcyclicGraph<String, DefaultEdge> graph;
  
  /** The sink executor for the graph. */
  protected QueryExecutor<?> sink_executor;
  
  /**
   * Protected ctor that sets up maps but doesn't generate the graph.
   * @param builder A non-null builder.
   */
  protected ExecutionGraph(final Builder builder) {
    if (builder.nodes == null || builder.nodes.isEmpty()) {
      throw new IllegalArgumentException("Executors cannot be null or empty.");
    }
    id = builder.id;
    nodes = builder.nodes;
    executors = Maps.newHashMapWithExpectedSize(nodes.size());
    graph = new DirectedAcyclicGraph<String,
        DefaultEdge>(DefaultEdge.class);
  }
  
  /**
   * Initializes the graph as per the config, looking for factories and 
   * instantiating executors.
   * @param tsdb A non-null TSDB to pull factories from.
   * @return A deferred to wait on for initialization to complete. May return
   * an exception if the graph does not conform to specs.
   * @throws IllegalArgumentException if the TSDB was null.
   */
  public Deferred<Object> initialize(final TSDB tsdb) {
    return initialize(tsdb, null);
  }
  
  /**
   * Initializes the graph as per the config, looking for factories and 
   * instantiating executors.
   * @param tsdb A non-null TSDB to pull factories from.
   * @param id_prefix An optional prefix to use if this graph belongs to an
   * executor such as a cluster executor.
   * @return A deferred to wait on for initialization to complete. May return
   * an exception if the graph does not conform to specs.
   * @throws IllegalArgumentException if the TSDB was null.
   */
  public Deferred<Object> initialize(final TSDB tsdb, final String id_prefix) {
    if (tsdb == null) {
      throw new IllegalArgumentException("TSDB cannot be null.");
    }
    this.tsdb = tsdb;
    try {
      final Map<String, ExecutionGraphNode> map = 
          Maps.newHashMapWithExpectedSize(nodes.size());
      final Set<String> unique_ids = 
          Sets.newHashSetWithExpectedSize(nodes.size());
      for (final ExecutionGraphNode node : nodes) {
        node.setExecutionGraph(this);
        
        // prepend the ID and upstream ID if we have one.
        if (!Strings.isNullOrEmpty(id_prefix)) {
          node.resetId(id_prefix + "_" + node.getExecutorId());
          if (!Strings.isNullOrEmpty(node.getUpstream())) {
            node.resetUpstream(id_prefix + "_" + node.getUpstream());
          }
        }
        
        if (unique_ids.contains(node.getExecutorId())) {
          return Deferred.<Object>fromResult(new IllegalArgumentException(
              "The node id \"" + node.getExecutorId() 
                + "\" appeared more than once in "
                + "the graph. It must be unique."));
        }
        unique_ids.add(node.getExecutorId());
        map.put(node.getExecutorId(), node);
        
        graph.addVertex(node.getExecutorId());
        if (!Strings.isNullOrEmpty(node.getUpstream())) {
          graph.addVertex(node.getUpstream());
          try {
            graph.addDagEdge(node.getUpstream(), node.getExecutorId());
          } catch (CycleFoundException e) {
            return Deferred.<Object>fromResult(
                new IllegalArgumentException("A cycle was "
                + "detected adding node: " + node, e));
          }
        }
      }
      
      // depth first initiation of the executors since we have to init
      // the ones without any downstream dependencies first.
      final DepthFirstIterator<String, DefaultEdge> iterator = 
          new DepthFirstIterator<String, DefaultEdge>(graph);
      while (iterator.hasNext()) {
        final String id = iterator.next();
        recursiveInit(id, map);
      }
      
      return Deferred.<Object>fromResult(null);
    } catch (Exception e) {
      return Deferred.<Object>fromResult(e);
    }
  }

  /**
   * Helper that recursively walks the graph, initializing the deepest executors
   * first so that parents that depend on downstream executors will find their
   * children on construction.
   * @param id A non-null graph node ID.
   * @param map The map of Ids to nodes to use for lookups.
   */
  private void recursiveInit(final String id, 
                             final Map<String, ExecutionGraphNode> map) {
    if (executors.containsKey(id)) {
      return;
    }
    
    final Set<DefaultEdge> outgoing = graph.outgoingEdgesOf(id);
    for (final DefaultEdge edge : outgoing) {
      final String downstream = graph.getEdgeTarget(edge);
      recursiveInit(downstream, map);
    }
    
    final ExecutionGraphNode node = map.get(id);
    if (node == null) {
      throw new IllegalArgumentException("No upstream node found with ID: " 
          + id);
    }
    
    // init an executor
    final QueryExecutorFactory<?> factory = 
        tsdb.getRegistry().getFactory(node.getExecutorType());
    if (factory == null) {
      throw new IllegalArgumentException("No factory "
          + "found for executor: " + node.getExecutorType());
    }
    
    final QueryExecutor<?> executor = factory.newExecutor(node);
    if (executor == null) {
      throw new IllegalStateException("Factory "
          + "returned a null executor.");
    }
    executors.put(node.getExecutorId(), executor);
    
    // find the sink executor
    if (graph.incomingEdgesOf(node.getExecutorId()).isEmpty()) {
      // we can't have more than one sink executor.
      if (sink_executor != null && sink_executor != executor) {
        if (graph.incomingEdgesOf(sink_executor.id()).isEmpty()) {
          throw new IllegalArgumentException(
              "Can't replace the existing sink: " + sink_executor 
              + " without adding an edge.");
        }
      }
      sink_executor = executor;
    }
  }
  
  /** @return The unique ID of this graph. */
  public String getId() {
    return id;
  }
  
  /** @return The list of nodes configured in this graph. */
  public List<ExecutionGraphNode> getNodes() {
    return Collections.unmodifiableList(nodes);
  }
  
  /** @return The sink executor to send queries to. */
  public QueryExecutor<?> sinkExecutor() {
    return sink_executor;
  }
  
  /**
   * Looks for the first of the downstream executors and returns it.
   * TODO - for multi-downstream graphs we'll need to add an override.
   * @param executor_id A non-null and non-empty executor ID.
   * @return An instantiated executor if found.
   * @throws IllegalArgumentException if the id was null or empty or not present
   * in the graph.
   * @throws IllegalStateException if the executor didn't have any downstream
   * executors.
   */
  public QueryExecutor<?> getDownstreamExecutor(final String executor_id) {
    if (Strings.isNullOrEmpty(executor_id)) {
      throw new IllegalArgumentException("Executor ID cannot be null or empty.");
    }
    if (!graph.containsVertex(executor_id)) {
      throw new IllegalArgumentException("No such executor ID in graph: " 
          + executor_id);
    }
    
    final Set<DefaultEdge> downstream = graph.outgoingEdgesOf(executor_id);
    if (downstream.isEmpty()) {
      throw new IllegalStateException("Executor " + executor_id + " does not "
          + "have any downstream executors in graph: " + this);
    }
    DefaultEdge target = downstream.iterator().next();
    String exec = graph.getEdgeTarget(target);
    return executors.get(exec);
  }
  
  /** The TSDB this graph belongs to. */
  public TSDB tsdb() {
    return tsdb;
  }
  
  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    final ExecutionGraph graph = (ExecutionGraph) o;
    return Objects.equal(id, graph.id)
        && Objects.equal(nodes, graph.nodes);
  }
  
  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }
  
  /** @return A HashCode object for deterministic, non-secure hashing */
  public HashCode buildHashCode() {
    final HashCode hc = Const.HASH_FUNCTION().newHasher()
        .putString(Strings.nullToEmpty(id), Const.UTF8_CHARSET)
        .hash();
    if (executors != null) {
      final List<HashCode> hashes = 
          Lists.newArrayListWithCapacity(nodes.size() + 1);
      hashes.add(hc);
      for (final ExecutionGraphNode node : nodes) {
        hashes.add(node.buildHashCode());
      }
      return Hashing.combineOrdered(hashes);
    }
    return hc;
  }
  
  @Override
  public int compareTo(final ExecutionGraph o) {
    return ComparisonChain.start()
        .compare(id, o.id, Ordering.natural().nullsFirst())
        .compare(nodes, o.nodes, 
            Ordering.<ExecutionGraphNode>natural().lexicographical().nullsFirst())
        .result();
  }
  
  @Override
  public String toString() {
    return new StringBuilder()
        .append("id=")
        .append(id)
        .append(", nodes=")
        .append(nodes)
        .append(", sinkExecutor=")
        .append(sink_executor)
        .toString();
  }
  
  /** @return A new builder for constructing graphs. */
  public static Builder newBuilder() {
    return new Builder();
  }
  
  /**
   * Clones a graph, returning a builder. <b>NOTE:</b> The cloned graph must be
   * initialized via {@link #initialize(TSDB)}.
   * @param graph A non-null graph to clone from.
   * @return A cloned builder using configs from the source graph.
   */
  public static Builder newBuilder(final ExecutionGraph graph) {
    final Builder builder = new Builder()
        .setId(graph.id);
    final List<ExecutionGraphNode> nodes = 
        Lists.newArrayListWithExpectedSize(graph.nodes.size());
    for (final ExecutionGraphNode node : graph.nodes) {
      nodes.add(ExecutionGraphNode.newBuilder(node).build());
    }
    builder.setNodes(nodes);
    return builder;
  }
  
  /** A builder for ExecutionGraphs. */
  public static class Builder {
    @JsonProperty
    private String id;
    @JsonProperty
    private List<ExecutionGraphNode> nodes;
    
    /**
     * @param id The non-null and non-empty ID of the graph.
     * @return The builder.
     */
    public Builder setId(final String id) {
      this.id = id;
      return this;
    }
   
    /**
     * @param nodes A non-null and non-empty set of graph nodes.
     * @return The builder.
     */
    public Builder setNodes(final List<ExecutionGraphNode> nodes) {
      this.nodes = nodes;
      if (this.nodes != null) {
        Collections.sort(this.nodes);
      }
      return this;
    }
    
    /**
     * @param node A non-null node to add to the configuration.
     * @return The builder.
     */
    public Builder addNode(final ExecutionGraphNode node) {
      if (nodes == null) {
        nodes = Lists.newArrayList(node);
      } else {
        nodes.add(node);
        Collections.sort(nodes);
      }
      return this;
    }
    
    /**
     * @param node A non-null node builder to add to the configuration.
     * @return The builder.
     */
    @JsonIgnore
    public Builder addNode(final ExecutionGraphNode.Builder node) {
      if (nodes == null) {
        nodes = Lists.newArrayList(node.build());
      } else {
        nodes.add(node.build());
        Collections.sort(nodes);
      }
      return this;
    }
    
    /** @return An ExecutionGraph instance that needs to be initialized. */
    public ExecutionGraph build() {
      return new ExecutionGraph(this);
    }
  }
}
