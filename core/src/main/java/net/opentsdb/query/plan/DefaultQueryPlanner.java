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
package net.opentsdb.query.plan;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jgrapht.Graph;
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

import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.query.MultiQueryNodeFactory;
import net.opentsdb.query.QueryDataSourceFactory;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QuerySourceConfig;
import net.opentsdb.query.SingleQueryNodeFactory;
import net.opentsdb.query.execution.graph.ExecutionGraph;
import net.opentsdb.query.execution.graph.ExecutionGraphNode;
import net.opentsdb.storage.TimeSeriesDataStoreFactory;

/**
 * A query planner that handles push-down operations to data sources.
 * 
 * TODO - more work and break it into an interface like the old one.
 * 
 * @since 3.0
 */
public class DefaultQueryPlanner {
  private static final Logger LOG = LoggerFactory.getLogger(
      DefaultQueryPlanner.class);
  
  /** The context we belong to. We get the query here. */
  private final QueryPipelineContext context;
  
  /** The roots (sent to sinks) of the user given graph. */
  private List<QueryNode> roots;
  
  /** The sinks to report to. */
  private List<QueryNode> sinks;
  
  /** The planned execution graph. */
  private DirectedAcyclicGraph<QueryNode, DefaultEdge> graph;
  
  /** The list of data sources we're fetching from. */
  private List<TimeSeriesDataSource> sources;
  
  /** The configuration graph. */
  private DirectedAcyclicGraph<ExecutionGraphNode, DefaultEdge> config_graph;
  
  /**
   * Default ctor.
   * @param context The non-null context to pull the query from.
   * @param sinks The non-null list of one or more sinks to report to.
   */
  public DefaultQueryPlanner(final QueryPipelineContext context,
                             final List<QueryNode> sinks) {
    this.context = context;
    roots = Lists.newArrayList();
    this.sinks = sinks;
    sources = Lists.newArrayList();
  }
  
  /**
   * Does the hard work.
   */
  public void plan() {
    final Map<String, ExecutionGraphNode> config_map = 
        Maps.newHashMapWithExpectedSize(
            context.query().getExecutionGraph().getNodes().size());
    config_graph = 
        new DirectedAcyclicGraph<ExecutionGraphNode, DefaultEdge>(
            DefaultEdge.class);
    
    // the first step is to add the vertices to the graph and we'll stash
    // the nodes in a map by node ID so we can link them later.
    for (final ExecutionGraphNode node : 
      context.query().getExecutionGraph().getNodes()) {
      if (config_map.putIfAbsent(node.getId(), node) != null) {
        throw new IllegalArgumentException("The node id \"" 
            + node.getId() + "\" appeared more than once in the "
            + "graph. It must be unique.");
      }
      config_graph.addVertex(node);
    }
    
    // now link em with the edges.
    for (final ExecutionGraphNode node : 
        context.query().getExecutionGraph().getNodes()) {
      if (node.getSources() != null) {
        for (final String source : node.getSources()) {
          try {
            config_graph.addDagEdge(node, config_map.get(source));
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
    
    final Map<String, QueryNodeFactory> factory_cache = Maps.newHashMap();
    
    // next we walk and let the factories update the graph as needed.
    // Note the clone to avoid concurrent modification of the graph.
    DepthFirstIterator<ExecutionGraphNode, DefaultEdge> iterator = 
        new DepthFirstIterator<ExecutionGraphNode, DefaultEdge>(
            (Graph<ExecutionGraphNode, DefaultEdge>) config_graph.clone());
    final List<ExecutionGraphNode> data_sources = Lists.newArrayList();
    while (iterator.hasNext()) {
      final ExecutionGraphNode node = iterator.next();
      if (node.getConfig() != null && 
          node.getConfig() instanceof QuerySourceConfig) {
        data_sources.add(node);
      }
      
      final String factory_id;
      if (!Strings.isNullOrEmpty(node.getType())) {
        factory_id = node.getType().toLowerCase();
      } else {
        factory_id = node.getId().toLowerCase();
      }
      final QueryNodeFactory factory = context.tsdb().getRegistry()
          .getQueryNodeFactory(factory_id);
      factory.setupGraph(context.query(), node, config_graph);
      factory_cache.put(factory_id, factory);
    }
    
    // next, push down by walking up from the data sources.
    for (final ExecutionGraphNode node : data_sources) {
      final QueryNodeFactory factory;
      if (!Strings.isNullOrEmpty(node.getType())) {
        factory = factory_cache.get(node.getType().toLowerCase());
      } else {
        factory = factory_cache.get(node.getId().toLowerCase());
      }
      
      // TODO - cleanup the source factories. ugg!!!
      if (factory == null || !(factory instanceof QueryDataSourceFactory)) {
        throw new IllegalArgumentException("No node factory found for "
            + "configuration " + node);
      }
      
      final List<ExecutionGraphNode> push_downs = Lists.newArrayList();
      for (final DefaultEdge edge : 
        Sets.newHashSet(config_graph.incomingEdgesOf(node))) {
        final ExecutionGraphNode n = config_graph.getEdgeSource(edge);
        
        // TODO - temp! Get named source.
        final TimeSeriesDataStoreFactory source_factory = 
            context.tsdb().getRegistry().getDefaultPlugin(
                TimeSeriesDataStoreFactory.class);
        if (source_factory == null) {
          throw new IllegalArgumentException("Unable to find a default "
              + "time series data store factory class!");
        }
        final DefaultEdge e = pushDown(node, node, source_factory, n, push_downs);
        if (e != null) {
          config_graph.removeEdge(e);
          push_downs.add(n);
        }
        
        if (config_graph.outgoingEdgesOf(n).isEmpty()) {
          config_graph.removeVertex(n);
        }
      }
      
      if (!push_downs.isEmpty()) {
        // now dump the push downs into this node.
        final QuerySourceConfig new_config = QuerySourceConfig.newBuilder(
            (QuerySourceConfig) node.getConfig())
            .setPushDownNodes(push_downs)
            .build();
        final ExecutionGraphNode new_node = ExecutionGraphNode.newBuilder(node)
            .setConfig(new_config)
            .build();
        ExecutionGraph.replace(node, new_node, config_graph);
      }
    }
    
    // now go and build the node graph
    graph = new DirectedAcyclicGraph<QueryNode, DefaultEdge>(DefaultEdge.class);
    final Map<String, QueryNode> nodes_map = Maps.newHashMap();
    for (final QueryNode sink : sinks) {
      graph.addVertex(sink);
      nodes_map.put(sink.id(), sink);
    }
    
    final List<Long> constructed = Lists.newArrayList();
    final BreadthFirstIterator<ExecutionGraphNode, DefaultEdge> bfi = 
        new BreadthFirstIterator<ExecutionGraphNode, DefaultEdge>(config_graph);
    while (bfi.hasNext()) {
      final ExecutionGraphNode node = bfi.next();
      if (config_graph.incomingEdgesOf(node).isEmpty()) {
        buildNodeGraph(context, node, constructed, nodes_map, factory_cache);
      }
    }
    
    // depth first initiation of the executors since we have to init
    // the ones without any downstream dependencies first.
    final DepthFirstIterator<QueryNode, DefaultEdge> node_iterator = 
        new DepthFirstIterator<QueryNode, DefaultEdge>(graph);
    final Set<TimeSeriesDataSource> source_set = Sets.newHashSet();
    while (node_iterator.hasNext()) {
      final QueryNode node = node_iterator.next();
      if (sinks.contains(node)) {
        continue;
      }
      
      final Set<DefaultEdge> incoming = graph.incomingEdgesOf(node);
      if (incoming.size() == 0 && node != this) {
        try {
          for (final QueryNode sink : sinks) {
            graph.addDagEdge(sink, node);
          }
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
        node.initialize(null /* TODO */);
      }
    }
    sources.addAll(source_set);
  }
  
  /**
   * Recursive method extract 
   * @param parent The parent of this node.
   * @param source The data source node.
   * @param factory The data source factory.
   * @param node The current node.
   * @param push_downs The non-null list of node configs that we'll 
   * populate any time we can push down.
   * @return An edge to link with if the previous node was pushed down.
   */
  private DefaultEdge pushDown(
      final ExecutionGraphNode parent,
      final ExecutionGraphNode source, 
      final TimeSeriesDataStoreFactory factory, 
      final ExecutionGraphNode node,
      final List<ExecutionGraphNode> push_downs) {
    if (!factory.supportsPushdown(node.getConfig().getClass())) {
      if (!config_graph.containsEdge(node, parent)) {
        try {
          config_graph.addDagEdge(node, parent);
        } catch (CycleFoundException e) {
          throw new IllegalArgumentException(
              "Invalid graph configuration", e);
        }
      }
      return null;
    }
    
    if (!node.getConfig().pushDown()) {
      // reached a node config that doesn't allow push downs.
      return null;
    }
    
    final DefaultEdge delete_edge = config_graph.getEdge(node, source);
    
    // see if we can walk up for more
    final Set<DefaultEdge> incoming = config_graph.incomingEdgesOf(node);
    if (!incoming.isEmpty()) {
      List<DefaultEdge> removals = Lists.newArrayList();
      List<ExecutionGraphNode> nodes = Lists.newArrayList();
      for (final DefaultEdge edge : incoming) {
        final ExecutionGraphNode n = config_graph.getEdgeSource(edge);
        nodes.add(n);
        DefaultEdge e = pushDown(parent, node, factory, n, push_downs);
        if (e != null) {
          removals.add(e);
        }
      }
      
      if (!removals.isEmpty()) {
        for (final DefaultEdge e : removals) {
          config_graph.removeEdge(e);
        }
      }
      
      for (final ExecutionGraphNode n : nodes) {
        if (config_graph.outgoingEdgesOf(n).isEmpty()) {
          if (config_graph.removeVertex(n)) {
            push_downs.add(n);
          }
        }
      }
    }
    
    // purge if we pushed everything down
    if (config_graph.outgoingEdgesOf(node).isEmpty()) {
      if (config_graph.removeVertex(node)) {
        push_downs.add(node);
      }
    }
    
    return delete_edge;
  }

  /**
   * Resursive helper to build and link the actual node graph.
   * @param context The non-null context we're working with.
   * @param node The current node config.
   * @param constructed A cache to determine if we've already instantated
   * and linked the node.
   * @param nodes_map A map of instantiated nodes to use for linking.
   * @param factory_cache The cache of factories so we don't have to keep
   * looking them up.
   * @return A node to link with.
   */
  private QueryNode buildNodeGraph(
      final QueryPipelineContext context, 
      final ExecutionGraphNode node, 
      final List<Long> constructed,
      final Map<String, QueryNode> nodes_map,
      final Map<String, QueryNodeFactory> factory_cache) {
    // short circuit initialized nodes.
    if (constructed.contains(node.buildHashCode().asLong())) {
      return nodes_map.get(node.getId());
    }
    
    // walk up the graph.
    final List<QueryNode> sources = Lists.newArrayList();
    for (final DefaultEdge edge : config_graph.outgoingEdgesOf(node)) {
      sources.add(buildNodeGraph(
          context, 
          config_graph.getEdgeTarget(edge), 
          constructed, 
          nodes_map, 
          factory_cache));
    }
    
    final QueryNodeFactory factory;
    if (!Strings.isNullOrEmpty(node.getType())) {
      factory = factory_cache.get(node.getType().toLowerCase());
    } else {
      factory = factory_cache.get(node.getId().toLowerCase());
    }
    if (factory == null) {
      throw new IllegalArgumentException("No node factory found for "
          + "configuration " + node);
    }
    
    QueryNodeConfig node_config = node.getConfig() != null ? node.getConfig() : 
          context.query().getExecutionGraph().nodeConfigs().get(node.getId());
    if (node_config == null) {
      node_config = context.query().getExecutionGraph().nodeConfigs()
          .get(node.getType());
    }
    if (node_config == null) {
      throw new IllegalArgumentException("No node config for " 
          + node.getId() + " or " + node.getType());
    }
    
    QueryNode query_node = null;
    final List<ExecutionGraphNode> configs = Lists.newArrayList(
        context.query().getExecutionGraph().getNodes());
    if (!(factory instanceof SingleQueryNodeFactory)) {
      final Collection<QueryNode> query_nodes = 
          ((MultiQueryNodeFactory) factory).newNodes(
              context, node.getId(), node_config, configs);
      if (query_nodes == null || query_nodes.isEmpty()) {
        throw new IllegalStateException("Factory returned a null or "
            + "empty list of nodes for " + node.getId());
      }
      
      QueryNode last = null;
      for (final QueryNode n : query_nodes) {
        if (n == null) {
          throw new IllegalStateException("Factory returned a null "
              + "node for " + node.getId());
        }
        if (query_node == null) {
          query_node = n;
        }
        last = n;
        
        graph.addVertex(n);
        nodes_map.put(n.id(), n);
      }
      
      constructed.add(node.buildHashCode().asLong());
      for (final QueryNode source : sources) {
        try {
          graph.addDagEdge(last, source);
        } catch (CycleFoundException e) {
          throw new IllegalArgumentException(
              "Invalid graph configuration", e);
        }
      }
    } else {
      query_node = ((SingleQueryNodeFactory) factory)
          .newNode(context, node.getId(), node_config);
      if (query_node == null) {
        throw new IllegalStateException("Factory returned a null "
            + "instance for " + node);
      }
      
      graph.addVertex(query_node);
      nodes_map.put(query_node.id(), query_node);
    }
    
    constructed.add(node.buildHashCode().asLong());
    
    for (final QueryNode source_node : sources) {
      try {
        graph.addDagEdge(query_node, source_node);
      } catch (CycleFoundException e) {
        throw new IllegalArgumentException(
            "Invalid graph configuration", e);
      }
    }
    
    return query_node;
  }

  public List<QueryNode> roots() {
    return roots;
  }
  
  public DirectedAcyclicGraph<QueryNode, DefaultEdge> graph() {
    return graph;
  }
  
  public List<TimeSeriesDataSource> sources() {
    return sources;
  }
}