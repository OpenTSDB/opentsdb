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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.hash.HashCode;
import com.stumbleupon.async.Deferred;

import net.opentsdb.configuration.Configuration;
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
import net.opentsdb.query.serdes.SerdesOptions;
import net.opentsdb.stats.Span;
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

  /** The pass-through context sink node. */
  private final QueryNode context_sink;
  
  /** A reference to the sink config. */
  private final ContextNodeConfig context_sink_config;
  
  private final Map<String, String> sink_filter;
  
  /** The roots (sent to sinks) of the user given graph. */
  private List<ExecutionGraphNode> roots;
  
  /** The planned execution graph. */
  private DirectedAcyclicGraph<QueryNode, DefaultEdge> graph;
  
  /** The list of data sources we're fetching from. */
  private List<TimeSeriesDataSource> data_sources;
  
  /** The configuration graph. */
  private DirectedAcyclicGraph<ExecutionGraphNode, DefaultEdge> config_graph;
  
  /** Map of the config IDs to nodes for use in linking and unit testing. */
  private final Map<String, QueryNode> nodes_map;
  
  /** The context node from the query pipeline context. All results pass
   * through this. */
  private ExecutionGraphNode context_node;
  
  /** The set of QueryResult objects we should see. */
  private Set<String> serialization_sources;
  
  /**
   * Default ctor.
   * @param context The non-null context to pull the query from.
   * @param context_sink The non-null context pass-through node.
   */
  public DefaultQueryPlanner(final QueryPipelineContext context,
                             final QueryNode context_sink) {
    this.context = context;
    this.context_sink = context_sink;
    sink_filter = Maps.newHashMap();
    roots = Lists.newArrayList();
    data_sources = Lists.newArrayList();
    nodes_map = Maps.newHashMap();
    context_sink_config = new ContextNodeConfig();
    
    if (context.query().getSerdesConfigs() != null) {
      for (final SerdesOptions config : context.query().getSerdesConfigs()) {
        if (config.getFilter() != null) {
          for (final String filter : config.getFilter()) {
            // Note: Assuming input validation here, that one or either
            // side is not null and includes a proper node Id.
            final String[] split = filter.split(":");
            if (split.length == 2) {
              sink_filter.put(split[0], split[1]);
            } else if (split.length == 1) {
              sink_filter.put(split[0], null);
            } else {
              throw new RuntimeException("WTF?? Invalid filter: " + filter);
            }
          }
        }
      }
    }
  }
  
  /**
   * Does the hard work.
   */
  @SuppressWarnings("unchecked")
  public Deferred<Void> plan(final Span span) {
    final Map<String, ExecutionGraphNode> config_map = 
        Maps.newHashMapWithExpectedSize(
            context.query().getExecutionGraph().getNodes().size());
    config_graph = 
        new DirectedAcyclicGraph<ExecutionGraphNode, DefaultEdge>(
            DefaultEdge.class);
    
    context_node = ExecutionGraphNode.newBuilder()
          .setId("QueryContext")
          .setConfig(context_sink_config)
          .build();
    config_graph.addVertex(context_node);
    config_map.put("QueryContext", context_node);
    
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
    final Set<String> satisfied_filters = Sets.newHashSet();
    
    // next we walk and let the factories update the graph as needed.
    // Note the clone to avoid concurrent modification of the graph.
    DepthFirstIterator<ExecutionGraphNode, DefaultEdge> iterator = 
        new DepthFirstIterator<ExecutionGraphNode, DefaultEdge>(
            (Graph<ExecutionGraphNode, DefaultEdge>) config_graph.clone());
    final List<ExecutionGraphNode> source_nodes = Lists.newArrayList();
    while (iterator.hasNext()) {
      final ExecutionGraphNode node = iterator.next();
      if (node.getConfig() != null && 
          node.getConfig() instanceof QuerySourceConfig) {
        // TODO - do this async
        if (((QuerySourceConfig) node.getConfig()).filter() != null) {
          try {
            ((QuerySourceConfig) node.getConfig()).filter().initialize(span).join();
          } catch (InterruptedException e) {
            throw new RuntimeException("WTF?", e);
          } catch (Exception e) {
            throw new RuntimeException("WTF?", e);
          }
        }
        source_nodes.add(node);
      }
      
      if (node.getConfig() instanceof ContextNodeConfig) {
        continue;
      }
      
      final Set<DefaultEdge> incoming = config_graph.incomingEdgesOf(node);
      if (incoming.isEmpty()) {
        if (sink_filter.isEmpty()) {
          try {
            config_graph.addDagEdge(context_node, node);
          } catch (CycleFoundException e) {
            throw new IllegalArgumentException(
                "Invalid graph configuration", e);
          }
        } else {
          roots.add(node);
        }
      }
      
      if (sink_filter.containsKey(node.getConfig().getId())) {
        final String source = sink_filter.get(node.getConfig().getId());
        if (source != null) {
          // TODO - make sure this links to the source, otherwise skip it.
          try {
            config_graph.addDagEdge(context_node, node);
          } catch (CycleFoundException e) {
            throw new IllegalArgumentException(
                "Invalid graph configuration", e);
          }
          satisfied_filters.add(node.getConfig().getId());
        } else {
          // we want the link.
          try {
            config_graph.addDagEdge(context_node, node);
          } catch (CycleFoundException e) {
            throw new IllegalArgumentException(
                "Invalid graph configuration", e);
          }
          satisfied_filters.add(node.getConfig().getId());
        }
      }
      
      final String factory_id;
      if (!Strings.isNullOrEmpty(node.getType())) {
        factory_id = node.getType().toLowerCase();
      } else {
        factory_id = node.getId().toLowerCase();
      }
      final QueryNodeFactory factory = context.tsdb().getRegistry()
          .getQueryNodeFactory(factory_id);
      if (factory == null) {
        throw new IllegalArgumentException("No node factory found for: " 
            + factory_id);
      }
      factory.setupGraph(context.query(), node, config_graph);
      factory_cache.put(factory_id, factory);
    }
    
    // before doing any more work, make sure the the filters have been
    // satisfied.
    for (final String key : sink_filter.keySet()) {
      if (!satisfied_filters.contains(key)) {
        throw new IllegalArgumentException("Unsatisfied sink filter: " + key);
      }
    }
    
    // next, push down by walking up from the data sources.
    for (final ExecutionGraphNode node : source_nodes) {
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
    
    // TODO clean out nodes that won't contribute to serialization.
    
    // compute source IDs.
    serialization_sources = computeSerializationSources(context_node);
    
    // now go and build the node graph
    graph = new DirectedAcyclicGraph<QueryNode, DefaultEdge>(DefaultEdge.class);
    graph.addVertex(context_sink);
    nodes_map.put(context_sink_config.getId(), context_sink);
    
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
      if (node == context_sink) {
        continue;
      }
      
      if (node instanceof TimeSeriesDataSource) {
        source_set.add((TimeSeriesDataSource) node);
      }
      
      if (node != this) {
        node.initialize(span);
      }
    }
    data_sources.addAll(source_set);
    
    // TODO!!!
    return Deferred.fromResult(null);
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
    
    // special case, ug.
    if (node.getConfig() instanceof ContextNodeConfig) {
      for (final QueryNode source_node : sources) {
        try {
          graph.addDagEdge(context_sink, source_node);
        } catch (CycleFoundException e) {
          throw new IllegalArgumentException(
              "Invalid graph configuration", e);
        }
      }
      return context_sink;
    }
    
    QueryNodeFactory factory;
    if (!Strings.isNullOrEmpty(node.getType())) {
      factory = factory_cache.get(node.getType().toLowerCase());
      if (factory == null) {
        // last chance
        factory = context.tsdb().getRegistry()
            .getQueryNodeFactory(node.getType().toLowerCase());
      }
    } else {
      factory = factory_cache.get(node.getId().toLowerCase());
      if (factory == null) {
        // last chance
        factory = context.tsdb().getRegistry()
            .getQueryNodeFactory(node.getId().toLowerCase());
      }
    }
    if (factory == null) {
      throw new IllegalArgumentException("No node factory found for "
          + "configuration " + node);
    }
    
    if (node.getConfig() == null) {
      throw new IllegalArgumentException("No node config for " 
          + node.getId() + " or " + node.getType());
    }
    
    QueryNode query_node = null;
    final List<ExecutionGraphNode> configs = Lists.newArrayList(
        context.query().getExecutionGraph().getNodes());
    if (!(factory instanceof SingleQueryNodeFactory)) {
      final Collection<QueryNode> query_nodes = 
          ((MultiQueryNodeFactory) factory).newNodes(
              context, node.getId(), node.getConfig(), configs);
      if (query_nodes == null) {
        throw new IllegalStateException("Factory returned a null list "
            + "of nodes for " + node.getId());
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
        nodes_map.put(n.config().getId(), n);
      }
      
      constructed.add(node.buildHashCode().asLong());
      if (last != null) {
        for (final QueryNode source : sources) {
          try {
            graph.addDagEdge(last, source);
          } catch (CycleFoundException e) {
            throw new IllegalArgumentException(
                "Invalid graph configuration", e);
          }
        }
      }
    } else {
      query_node = ((SingleQueryNodeFactory) factory)
          .newNode(context, node.getId(), node.getConfig());
      if (query_node == null) {
        throw new IllegalStateException("Factory returned a null "
            + "instance for " + node);
      }
      
      graph.addVertex(query_node);
      nodes_map.put(query_node.config().getId(), query_node);
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
  
  /** @return The non-null node graph. */
  public DirectedAcyclicGraph<QueryNode, DefaultEdge> graph() {
    return graph;
  }
  
  /** @return The non-null data sources list. */
  public List<TimeSeriesDataSource> sources() {
    return data_sources;
  }

  /** @return The non-null list of result IDs to watch for. */
  public Set<String> serializationSources() {
    return serialization_sources;
  }
  
  /**
   * Recursive function that calculates the IDs that we should see 
   * emitted through the pipeline as QueryResult objects.
   * TODO - this assumes one result per data source.
   * @param node The non-null node to work from.
   * @return A set of unique results we should see.
   */
  private Set<String> computeSerializationSources(
      final ExecutionGraphNode node) {
    if (node.getConfig() instanceof QuerySourceConfig ||
        node.getConfig().joins()) {
      return Sets.newHashSet(node.getConfig().getId());
    }
    
    final Set<String> ids = Sets.newHashSet();
    for (final DefaultEdge edge : config_graph.outgoingEdgesOf(node)) {
      final ExecutionGraphNode downstream = config_graph.getEdgeTarget(edge);
      final Set<String> downstream_ids = computeSerializationSources(downstream);
      if (node == context_node) {
        // prepend
        if (downstream.getConfig() instanceof QuerySourceConfig ||
            downstream.getConfig().joins()) {
          ids.addAll(downstream_ids);
        } else {
          for (final String id : downstream_ids) {
            ids.add(downstream.getConfig().getId() + ":" + id);
          }
        }
      } else {
        ids.addAll(downstream_ids);
      }
    }
    return ids;
  }
  
  @VisibleForTesting
  Map<String, QueryNode> nodesMap() {
    return nodes_map;
  }
  
  /**
   * TODO - look at this to find a better way than having a generic
   * config.
   */
  class ContextNodeConfig implements QueryNodeConfig {

    @Override
    public int compareTo(QueryNodeConfig o) {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public String getId() {
      return "QueryContext";
    }

    @Override
    public HashCode buildHashCode() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public boolean pushDown() {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public boolean joins() {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public Map<String, String> getOverrides() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public String getString(Configuration config, String key) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public int getInt(Configuration config, String key) {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public long getLong(Configuration config, String key) {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public boolean getBoolean(Configuration config, String key) {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public double getDouble(Configuration config, String key) {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public boolean hasKey(String key) {
      // TODO Auto-generated method stub
      return false;
    }
    
  }
}