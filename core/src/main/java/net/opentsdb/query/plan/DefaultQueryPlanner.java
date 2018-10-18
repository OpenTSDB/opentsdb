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
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.common.Const;
import net.opentsdb.configuration.Configuration;
import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.data.TimeSeriesDataSourceFactory;
//import net.opentsdb.query.QueryDataSourceFactory;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.serdes.SerdesOptions;
import net.opentsdb.stats.Span;
import net.opentsdb.utils.Deferreds;

/**
 * A query planner that handles push-down operations to data sources.
 * 
 * TODO - more work and break it into an interface like the old one.
 * 
 * @since 3.0
 */
public class DefaultQueryPlanner implements QueryPlanner {
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
  private List<QueryNodeConfig> roots;
  
  /** The planned execution graph. */
  private DirectedAcyclicGraph<QueryNode, DefaultEdge> graph;
  
  /** The list of data sources we're fetching from. */
  private List<TimeSeriesDataSource> data_sources;
  
  /** The configuration graph. */
  private DirectedAcyclicGraph<QueryNodeConfig, DefaultEdge> config_graph;
  
  /** Map of the config IDs to nodes for use in linking and unit testing. */
  private final Map<String, QueryNode> nodes_map;
  
  /** The context node from the query pipeline context. All results pass
   * through this. */
  private QueryNodeConfig context_node;
  
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
    final Map<String, QueryNodeConfig> config_map = 
        Maps.newHashMapWithExpectedSize(
            context.query().getExecutionGraph().size());
    config_graph = 
        new DirectedAcyclicGraph<QueryNodeConfig, DefaultEdge>(
            DefaultEdge.class);
    
    context_node = context_sink_config;
    config_graph.addVertex(context_node);
    config_map.put("QueryContext", context_node);
    
    // the first step is to add the vertices to the graph and we'll stash
    // the nodes in a map by node ID so we can link them later.
    for (final QueryNodeConfig node : context.query().getExecutionGraph()) {
      if (config_map.putIfAbsent(node.getId(), node) != null) {
        throw new IllegalArgumentException("The node id \"" 
            + node.getId() + "\" appeared more than once in the "
            + "graph. It must be unique.");
      }
      config_graph.addVertex(node);
    }
    
    // now link em with the edges.
    for (final QueryNodeConfig node : context.query().getExecutionGraph()) {
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
    DepthFirstIterator<QueryNodeConfig, DefaultEdge> iterator = 
        new DepthFirstIterator<QueryNodeConfig, DefaultEdge>(
            (Graph<QueryNodeConfig, DefaultEdge>) config_graph.clone());
    final List<QueryNodeConfig> source_nodes = Lists.newArrayList();
    while (iterator.hasNext()) {
      final QueryNodeConfig node = iterator.next();
      if (node instanceof TimeSeriesDataSourceConfig) {
        source_nodes.add(node);
      }
      
      if (node instanceof ContextNodeConfig) {
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
      
      if (sink_filter.containsKey(node.getId())) {
        final String source = sink_filter.get(node.getId());
        if (source != null) {
          // TODO - make sure this links to the source, otherwise skip it.
          try {
            config_graph.addDagEdge(context_node, node);
          } catch (CycleFoundException e) {
            throw new IllegalArgumentException(
                "Invalid graph configuration", e);
          }
          satisfied_filters.add(node.getId());
        } else {
          // we want the link.
          try {
            config_graph.addDagEdge(context_node, node);
          } catch (CycleFoundException e) {
            throw new IllegalArgumentException(
                "Invalid graph configuration", e);
          }
          satisfied_filters.add(node.getId());
        }
      }
      
      final String factory_id;
      if (node instanceof TimeSeriesDataSourceConfig) {
        factory_id = Strings.isNullOrEmpty(((TimeSeriesDataSourceConfig) node)
            .getSourceId()) ? null : 
              ((TimeSeriesDataSourceConfig) node)
              .getSourceId().toLowerCase();
        final TimeSeriesDataSourceFactory factory = context.tsdb().getRegistry()
            .getPlugin(TimeSeriesDataSourceFactory.class, factory_id);
        if (factory == null) {
          throw new IllegalArgumentException("No data source factory found for: " 
              + factory_id);
        }
        factory.setupGraph(context.query(), node, this);
        factory_cache.put(factory_id, factory);
      } else {
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
        factory.setupGraph(context.query(), node, this);
        factory_cache.put(factory_id, factory);
      }
    }
    
    class ConfigInitCB implements Callback<Void, Void> {

      @Override
      public Void call(final Void ignored) throws Exception {
        // before doing any more work, make sure the the filters have been
        // satisfied.
        for (final String key : sink_filter.keySet()) {
          if (!satisfied_filters.contains(key)) {
            throw new IllegalArgumentException("Unsatisfied sink filter: " + key);
          }
        }
        
        // next, push down by walking up from the data sources.
        for (final QueryNodeConfig node : source_nodes) {
          final QueryNodeFactory factory;
          if (node instanceof TimeSeriesDataSourceConfig) {
            factory = factory_cache.get( 
                Strings.isNullOrEmpty(((TimeSeriesDataSourceConfig) node)
                  .getSourceId()) ? null : 
                    ((TimeSeriesDataSourceConfig) node)
                      .getSourceId().toLowerCase());
            } else if (!Strings.isNullOrEmpty(node.getType())) {
            factory = factory_cache.get(node.getType().toLowerCase());
          } else {
            factory = factory_cache.get(node.getId().toLowerCase());
          }
          
          // TODO - cleanup the source factories. ugg!!!
          if (factory == null || !(factory instanceof TimeSeriesDataSourceFactory)) {
            throw new IllegalArgumentException("No node factory found for "
                + "configuration " + node);
          }
          
          final List<QueryNodeConfig> push_downs = Lists.newArrayList();
          for (final DefaultEdge edge : 
            Sets.newHashSet(config_graph.incomingEdgesOf(node))) {
            final QueryNodeConfig n = config_graph.getEdgeSource(edge);
            final DefaultEdge e = pushDown(
                node, 
                node, 
                (TimeSeriesDataSourceFactory) factory, 
                n, 
                push_downs);
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
            final TimeSeriesDataSourceConfig new_config = 
                ((TimeSeriesDataSourceConfig) node).getBuilder()
                .setPushDownNodes(push_downs)
                .build();
            replace(node, new_config);
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
        final BreadthFirstIterator<QueryNodeConfig, DefaultEdge> bfi = 
            new BreadthFirstIterator<QueryNodeConfig, DefaultEdge>(config_graph);
        while (bfi.hasNext()) {
          final QueryNodeConfig node = bfi.next();
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
        return null;
      }
      
    }
    
    final List<Deferred<Void>> deferreds = 
        Lists.newArrayListWithExpectedSize(source_nodes.size());
    for (final QueryNodeConfig c : source_nodes) {
      if (((TimeSeriesDataSourceConfig) c).getFilter() != null) {
        deferreds.add(((TimeSeriesDataSourceConfig) c)
            .getFilter().initialize(span));
      }
    }
    
    return Deferred.group(deferreds)
        .addCallback(Deferreds.VOID_GROUP_CB)
        .addCallback(new ConfigInitCB());
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
      final QueryNodeConfig parent,
      final QueryNodeConfig source, 
      final TimeSeriesDataSourceFactory factory, 
      final QueryNodeConfig node,
      final List<QueryNodeConfig> push_downs) {
    if (!factory.supportsPushdown(node.getClass())) {
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
    
    if (!node.pushDown()) {
      // reached a node config that doesn't allow push downs.
      return null;
    }
    
    final DefaultEdge delete_edge = config_graph.getEdge(node, source);
    
    // see if we can walk up for more
    final Set<DefaultEdge> incoming = config_graph.incomingEdgesOf(node);
    if (!incoming.isEmpty()) {
      List<DefaultEdge> removals = Lists.newArrayList();
      List<QueryNodeConfig> nodes = Lists.newArrayList();
      for (final DefaultEdge edge : incoming) {
        final QueryNodeConfig n = config_graph.getEdgeSource(edge);
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
      
      for (final QueryNodeConfig n : nodes) {
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
      final QueryNodeConfig node, 
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
    if (node instanceof ContextNodeConfig) {
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
    if (node instanceof TimeSeriesDataSourceConfig) {
      factory = factory_cache.get(
          Strings.isNullOrEmpty(((TimeSeriesDataSourceConfig) node)
            .getSourceId()) ? null : 
              ((TimeSeriesDataSourceConfig) node)
              .getSourceId().toLowerCase());
      if (factory == null) {
        // last chance
        factory = context.tsdb().getRegistry()
            .getQueryNodeFactory(((TimeSeriesDataSourceConfig) node)
                .getSourceId().toLowerCase());
      }
    } else if (!Strings.isNullOrEmpty(node.getType())) {
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
    
    QueryNode query_node = factory.newNode(context, node);
    if (query_node == null) {
      throw new IllegalStateException("Factory returned a null "
          + "instance for " + node);
    }
    graph.addVertex(query_node);
    nodes_map.put(query_node.config().getId(), query_node);
    
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
  
  public DirectedAcyclicGraph<QueryNodeConfig, DefaultEdge> configGraph() {
    return config_graph;
  }
  
  @Override
  public QueryPipelineContext context() {
    return context;
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
      final QueryNodeConfig node) {
    if (node instanceof TimeSeriesDataSourceConfig ||
        node.joins()) {
      return Sets.newHashSet(node.getId());
    }
    
    final Set<String> ids = Sets.newHashSet();
    for (final DefaultEdge edge : config_graph.outgoingEdgesOf(node)) {
      final QueryNodeConfig downstream = config_graph.getEdgeTarget(edge);
      final Set<String> downstream_ids = computeSerializationSources(downstream);
      if (node == context_node) {
        // prepend
        if (downstream instanceof TimeSeriesDataSourceConfig ||
            downstream.joins()) {
          ids.addAll(downstream_ids);
        } else {
          for (final String id : downstream_ids) {
            ids.add(downstream.getId() + ":" + id);
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
    public String getType() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public List<String> getSources() {
      // TODO Auto-generated method stub
      return null;
    }
    
    @Override
    public HashCode buildHashCode() {
      // TODO Auto-generated method stub
      return Const.HASH_FUNCTION().newHasher()
          .putInt(System.identityHashCode(this)) // TEMP!
          .hash();
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

  /**
   * Helper to replace a node with a new one, moving edges.
   * @param old_config The non-null old node that is present in the graph.
   * @param new_config The non-null new node that is not present in the graph.
   * @param graph The non-null graph to mutate.
   */
  public void replace(final QueryNodeConfig old_config,
                      final QueryNodeConfig new_config) {
    final List<QueryNodeConfig> upstream = Lists.newArrayList();
    for (final DefaultEdge up : config_graph.incomingEdgesOf(old_config)) {
      final QueryNodeConfig n = config_graph.getEdgeSource(up);
      upstream.add(n);
    }
    for (final QueryNodeConfig n : upstream) {
      config_graph.removeEdge(n, old_config);
    }
    
    final List<QueryNodeConfig> downstream = Lists.newArrayList();
    for (final DefaultEdge down : config_graph.outgoingEdgesOf(old_config)) {
      final QueryNodeConfig n = config_graph.getEdgeTarget(down);
      downstream.add(n);
    }
    for (final QueryNodeConfig n : downstream) {
      config_graph.removeEdge(old_config, n);
    }
    
    config_graph.removeVertex(old_config);
    config_graph.addVertex(new_config);
    
    for (final QueryNodeConfig up : upstream) {
      try {
        config_graph.addDagEdge(up, new_config);
      } catch (CycleFoundException e) {
        throw new IllegalArgumentException("The factory created a cycle "
            + "setting up the graph from config: " + old_config, e);
      }
    }
    
    for (final QueryNodeConfig down : downstream) {
      try {
        config_graph.addDagEdge(new_config, down);
      } catch (CycleFoundException e) {
        throw new IllegalArgumentException("The factory created a cycle "
            + "setting up the graph from config: " + old_config, e);
      }
    }
  }

}