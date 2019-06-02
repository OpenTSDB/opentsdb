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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.graph.EndpointPair;
import com.google.common.graph.GraphBuilder;
import com.google.common.graph.Graphs;
import com.google.common.graph.MutableGraph;
import com.google.common.graph.Traverser;
import com.google.common.hash.HashCode;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.common.Const;
import net.opentsdb.configuration.Configuration;
import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.data.TimeSeriesDataSourceFactory;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.idconverter.ByteToStringIdConverterConfig;
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
  protected final QueryPipelineContext context;

  /** The pass-through context sink node. */
  protected final QueryNode context_sink;
  
  /** A reference to the sink config. */
  protected final ContextNodeConfig context_sink_config;
  
  /** A list of filters to be satisfied. */
  protected final Map<String, String> sink_filter;
  
  /** The roots (sent to sinks) of the user given graph. */
  protected List<QueryNodeConfig> roots;
  
  /** The planned execution graph. */
  protected MutableGraph<QueryNode> graph;
  
  /** The list of data sources we're fetching from. */
  protected List<TimeSeriesDataSource> data_sources;
  
  /** The set of data source config nodes. */
  protected final Set<QueryNodeConfig> source_nodes;
  
  /** The configuration graph. */
  protected MutableGraph<QueryNodeConfig> config_graph;
  
  /** Map of the config IDs to nodes for use in linking and unit testing. */
  protected final Map<String, QueryNode> nodes_map;
  
  /** The cache of factories. */
  protected final Map<String, QueryNodeFactory> factory_cache;
  
  /** The context node from the query pipeline context. All results pass
   * through this. */
  protected QueryNodeConfig context_node;
  
  /** The set of QueryResult objects we should see. */
  protected Set<String> serialization_sources;
  
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
    factory_cache = Maps.newHashMap();
    context_sink_config = new ContextNodeConfig();
    source_nodes = Sets.newHashSet();
    config_graph = GraphBuilder.directed()
        .allowsSelfLoops(false)
        .build();
    
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
              throw new RuntimeException("Invalid filter: " + filter);
            }
          }
        }
      }
    }
  }
  
  /**
   * Does the hard work.
   */
  public Deferred<Void> plan(final Span span) {
    final Map<String, QueryNodeConfig> config_map = 
        Maps.newHashMapWithExpectedSize(
            context.query().getExecutionGraph().size());
    context_node = context_sink_config;
    config_graph.addNode(context_node);
    config_map.put("QueryContext", context_node);
    
    // the first step is to add the vertices to the graph and we'll stash
    // the nodes in a map by node ID so we can link them later.
    for (final QueryNodeConfig node : context.query().getExecutionGraph()) {
      if (config_map.putIfAbsent(node.getId(), node) != null) {
        throw new QueryExecutionException("The node id \"" 
            + node.getId() + "\" appeared more than once in the "
            + "graph. It must be unique.", 400);
      }
      config_graph.addNode(node);
    }
    
    // now link em with the edges.
    for (final QueryNodeConfig node : context.query().getExecutionGraph()) {
      if (node instanceof TimeSeriesDataSourceConfig) {
        source_nodes.add(node);
      }
      
      if (node.getSources() != null) {
        for (final String source : node.getSources()) {
          final QueryNodeConfig src = config_map.get(source);
          if (src == null) {
            throw new QueryExecutionException("No source node with ID " 
                + source + " found for config " + node.getId(), 400);
          }
          config_graph.putEdge(node, src);
          if (Graphs.hasCycle(config_graph)) {
            throw new IllegalArgumentException("Cycle found linking node " 
                + node.getId() + " to " + config_map.get(source).getId());
          }
        }
      }
    }
    
    final Set<String> satisfied_filters = Sets.newHashSet();
    // next we walk and let the factories update the graph as needed.
    // Note the clone to avoid concurrent modification of the graph.
    Set<Integer> already_setup = Sets.newHashSet();
    boolean modified = true;
    while (modified) {
      if (source_nodes.isEmpty()) {
        break;
      }
      final List<QueryNodeConfig> srcs = Lists.newArrayList(source_nodes);
      for (final QueryNodeConfig node : srcs) {
        modified = recursiveSetup(node, already_setup, satisfied_filters);
        if (modified) {
          break;
        }
      }
    }
    
    class ConfigInitCB implements Callback<Deferred<Void>, Void> {

      @Override
      public Deferred<Void> call(final Void ignored) throws Exception {
        // before doing any more work, make sure the the filters have been
        // satisfied.
        for (final String key : sink_filter.keySet()) {
          if (!satisfied_filters.contains(key)) {
            // it may have been added in a setup step so check for the node in the
            // graph.
            boolean found = false;
            if (!found) {
              throw new QueryExecutionException("Unsatisfied sink filter: " 
                  + key, 400);
            }
          }
        }
        
        // next, push down by walking up from the data sources.
        final List<QueryNodeConfig> copy = Lists.newArrayList(source_nodes);
        for (final QueryNodeConfig node : copy) {
          final QueryNodeFactory factory = getFactory(node);
          // TODO - cleanup the source factories. ugg!!!
          if (factory == null || !(factory instanceof TimeSeriesDataSourceFactory)) {
            throw new QueryExecutionException("No node factory found for "
                + "configuration " + node + "  Factory=" + factory, 400);
          }
          
          final List<QueryNodeConfig> push_downs = Lists.newArrayList();
          for (final QueryNodeConfig n : config_graph.predecessors(node)) {
            pushDown(
                node, 
                node, 
                (TimeSeriesDataSourceFactory) factory, 
                n, 
                push_downs);
          }
          
          if (!push_downs.isEmpty()) {
            // now dump the push downs into this node.
            final TimeSeriesDataSourceConfig new_config = 
                ((TimeSeriesDataSourceConfig.Builder) node.toBuilder())
                .setPushDownNodes(push_downs)
                .build();
            replace(node, new_config);
          }
        }
        
        // TODO clean out nodes that won't contribute to serialization.
        // compute source IDs.
        serialization_sources = computeSerializationSources(context_node);
        // now go and build the node graph
        graph = GraphBuilder.directed()
            .allowsSelfLoops(false)
            .build();
        graph.addNode(context_sink);
        nodes_map.put(context_sink_config.getId(), context_sink);
        
        Traverser<QueryNodeConfig> traverser = Traverser.forGraph(config_graph);
        for (final QueryNodeConfig node : traverser.breadthFirst(context_node)) {
          if (config_graph.predecessors(node).isEmpty()) {
            buildNodeGraph(context, node, nodes_map);
          }
        }
        
        if (LOG.isTraceEnabled()) {
          LOG.trace(printConfigGraph());          
        }
        
        if (context.query().isTraceEnabled()) {
          context.queryContext().logTrace(printConfigGraph());
        }
        
        // depth first initiation of the executors since we have to init
        // the ones without any downstream dependencies first.
        Set<QueryNode> initialized = Sets.newHashSet();
        return recursiveInit(context_sink, initialized, span)
            .addCallbackDeferring(new Callback<Deferred<Void>, Void>() {

              @Override
              public Deferred<Void> call(Void arg) throws Exception {
                if (data_sources.isEmpty()) {
                  LOG.error("No data sources in the final graph for: " 
                      + context.query() + " " + printConfigGraph());
                  return Deferred.<Void>fromError(new RuntimeException(
                      "No data sources in the final graph for: " 
                          + context.query() + " " + printConfigGraph()));
                }
                return null;
              }
              
            });
      }
      
    }
    
    // one more iteration to make sure we capture all the source nodes
    // from the graph setup.
    source_nodes.clear();
    for (final QueryNodeConfig node : config_graph.nodes()) {
      if (node instanceof TimeSeriesDataSourceConfig) {
        source_nodes.add(node);
      }
    }
    
    final List<Deferred<Void>> deferreds = 
        Lists.newArrayListWithExpectedSize(source_nodes.size());
    ByteToStringIdConverterConfig.Builder converter_builder = null;
    boolean push_mode = context.tsdb().getConfig().hasProperty("tsd.storage.enable_push") &&
        context.tsdb().getConfig().getBoolean("tsd.storage.enable_push");
    for (final QueryNodeConfig c : Lists.newArrayList(source_nodes)) {
      // see if we need to insert a byte Id converter upstream.
      if (push_mode) {
        TimeSeriesDataSourceFactory factory = ((TimeSeriesDataSourceFactory) getFactory(c));
        if (factory.idType() != Const.TS_STRING_ID) {
          if (converter_builder == null) {
            converter_builder = ByteToStringIdConverterConfig.newBuilder();
          }
          converter_builder.addDataSource(c.getId(), factory);
        }
      } else {
        needByteIdConverter(c);
      }
      
      if (((TimeSeriesDataSourceConfig) c).getFilter() != null) {
        deferreds.add(((TimeSeriesDataSourceConfig) c)
            .getFilter().initialize(span));
      }
    }
    
    if (push_mode && converter_builder != null) {
      final QueryNodeConfig converter = converter_builder
          .setId("IDConverter")
          .build();
      replace(context_sink_config, converter);
      addEdge(context_sink_config, converter);
    }
    
    return Deferred.group(deferreds)
        .addCallback(Deferreds.VOID_GROUP_CB)
        .addCallbackDeferring(new ConfigInitCB());
  }
  
  /**
   * Recursive setup that will stop and allow the loop to restart setup
   * if the graph has changed.
   * @param node The non-null current node.
   * @param already_setup Nodes already setup to avoid repeats.
   * @param satisfied_filters Filters.
   * @return true if the graph has mutated and we should restart, false
   * if not.
   */
  private boolean recursiveSetup(
      final QueryNodeConfig node, 
      final Set<Integer> already_setup, 
      final Set<String> satisfied_filters) {
    if (!already_setup.contains(System.identityHashCode(node)) && 
        node != context_sink_config) {
      // TODO - ugg!! There must be a better way to determine if the graph
      // has been modified.
      final MutableGraph<QueryNodeConfig> clone = Graphs.copyOf(config_graph);
      
      final Set<QueryNodeConfig> incoming = config_graph.predecessors(node);
      if (incoming.isEmpty()) {
        if (sink_filter.isEmpty()) {
          config_graph.putEdge(context_node, node);
          if (Graphs.hasCycle(config_graph)) {
            throw new IllegalArgumentException("Cycle found linking node " 
                + context_node.getId() + " to " + node.getId());
          }
        } else {
          roots.add(node);
        }
      }
      
      if (sink_filter.containsKey(node.getId())) {
        final String source = sink_filter.get(node.getId());
        if (source != null) {
          // TODO - make sure this links to the source, otherwise skip it.
          config_graph.putEdge(context_node, node);
          if (Graphs.hasCycle(config_graph)) {
            throw new IllegalArgumentException("Cycle found linking node " 
                + context_node.getId() + " to " + node.getId());
          }
          satisfied_filters.add(node.getId());
        } else {
          // we want the link.
          config_graph.putEdge(context_node, node);
          if (Graphs.hasCycle(config_graph)) {
            throw new IllegalArgumentException("Cycle found linking node " 
                + context_node.getId() + " to " + node.getId());
          }
          satisfied_filters.add(node.getId());
        }
      }
      
      final QueryNodeFactory factory = getFactory(node);
      if (factory == null) {
        throw new QueryExecutionException("No data source factory found for: " 
            + node, 400);
      }
      factory.setupGraph(context, node, this);
      already_setup.add(System.identityHashCode(node));
      if (!config_graph.equals(clone)) {
        return true;
      }
    }
    
    // all done, move up.
    for (final QueryNodeConfig upstream : 
        Sets.newHashSet(config_graph.predecessors(node))) {
      if (recursiveSetup(upstream, already_setup, satisfied_filters)) {
        return true;
      }
    }
    
    return false;
  }
  
  /**
   * Helper to DFS initialize the nodes.
   * @param node The non-null current node.
   * @param initialized A set of already initialized nodes.
   * @param span An optional tracing span.
   * @return A deferred resolving to null or an exception.
   */
  private Deferred<Void> recursiveInit(final QueryNode node, 
                                       final Set<QueryNode> initialized, 
                                       final Span span) {
    if (initialized.contains(node)) {
      return Deferred.fromResult(null);
    }
    
    final Set<QueryNode> successors = graph.successors(node);
    if (successors.isEmpty()) {
      initialized.add(node);
      if (node == context_sink) {
        return Deferred.fromResult(null);
      }
      return node.initialize(span);
    }
    
    List<Deferred<Void>> deferreds = Lists.newArrayListWithExpectedSize(successors.size());
    for (final QueryNode successor : successors) {
      deferreds.add(recursiveInit(successor, initialized, span));
    }

    class InitCB implements Callback<Deferred<Void>, Void> {
      @Override
      public Deferred<Void> call(final Void ignored) throws Exception {
        initialized.add(node);
        if (node == context_sink) {
          return Deferred.fromResult(null);
        }
        return node.initialize(span);
      }
    }
    
    return Deferred.group(deferreds)
        .addCallback(Deferreds.VOID_GROUP_CB)
        .addCallbackDeferring(new InitCB());
  }
  
  /**
   * Recursive method extract 
   * @param parent The parent of this node.
   * @param source The data source node.
   * @param factory The data source factory.
   * @param node The current node.
   * @param push_downs The non-null list of node configs that we'll 
   * populate any time we can push down.
   * @param clone The clone to work from while mutating.
   * @return An edge to link with if the previous node was pushed down.
   */
  public void pushDown(
      final QueryNodeConfig parent,
      final QueryNodeConfig source, 
      final TimeSeriesDataSourceFactory factory, 
      final QueryNodeConfig node,
      final List<QueryNodeConfig> push_downs) {
    if (!factory.supportsPushdown(node.getClass())) {
      return;
    }
    
    if (!node.pushDown()) {
      return;
    }
    
    // we can push this one down so add to the list and yank the edge.
    push_downs.add(node);
    config_graph.removeEdge(node, parent);
    if (serialization_sources != null && 
        serialization_sources.contains(node.getId())) {
      serialization_sources.remove(node.getId());
      serialization_sources.add(parent.getId());
    }
    
    Set<QueryNodeConfig> incoming = config_graph.predecessors(node);
    for (final QueryNodeConfig n : incoming) {
      config_graph.putEdge(n, parent);
      if (Graphs.hasCycle(config_graph)) {
        throw new IllegalArgumentException("Cycle found linking node " 
            + node.getId() + " to " + parent.getId());
      }
    }
    
    // purge if we pushed everything down
    if (config_graph.successors(node).isEmpty()) {
      config_graph.removeNode(node);
    }
    
    // see if we can walk up for more
    if (!incoming.isEmpty()) {
      incoming = Sets.newHashSet(incoming);
      for (final QueryNodeConfig n : incoming) {
        pushDown(parent, node, factory, n, push_downs);
      }
    }

    return;
  }

  /**
   * Recursive helper to build and link the actual node graph.
   * @param context The non-null context we're working with.
   * @param node The current node config.
   * @param constructed A cache to determine if we've already instantiated
   * and linked the node.
   * @param nodes_map A map of instantiated nodes to use for linking.
   * @return A node to link with.
   */
  private QueryNode buildNodeGraph(
      final QueryPipelineContext context, 
      final QueryNodeConfig node,
      final Map<String, QueryNode> nodes_map) {
    // walk up the graph.
    final List<QueryNode> sources = Lists.newArrayList();
    for (final QueryNodeConfig n : config_graph.successors(node)) {
      sources.add(buildNodeGraph(
          context, 
          n,
          nodes_map));
    }
    
    // special case, ug.
    if (node instanceof ContextNodeConfig) {
      for (final QueryNode source_node : sources) {
          graph.putEdge(context_sink, source_node);
          if (Graphs.hasCycle(graph)) {
            throw new IllegalArgumentException("!TF?");
          }
      }
      return context_sink;
    }
    
    QueryNode query_node = nodes_map.get(node.getId());
    if (query_node == null) {
      QueryNodeFactory factory = getFactory(node);
      if (factory == null) {
        throw new QueryExecutionException("No node factory found for "
            + "configuration " + node, 400);
      }
      
      query_node = factory.newNode(context, node);
      if (query_node == null) {
        throw new IllegalStateException("Factory returned a null "
            + "instance for " + node);
      }
      
      graph.addNode(query_node);
      nodes_map.put(query_node.config().getId(), query_node);
    }
    
    if (query_node instanceof TimeSeriesDataSource) {
      // TODO - make it a set but then convert to list as the pipeline
      // needs indexing (or we can make it an iterator there).
      if (!data_sources.contains((TimeSeriesDataSource) query_node)) {
        data_sources.add((TimeSeriesDataSource) query_node);
      }
    }
    
    for (final QueryNode source_node : sources) {
      graph.putEdge(query_node, source_node);
      if (Graphs.hasCycle(graph)) {
        throw new IllegalArgumentException("Cycle adding " 
            + query_node + " => " + source_node);
      }
    }
    
    return query_node;
  }
  
  /** @return The non-null node graph. */
  public MutableGraph<QueryNode> graph() {
    return graph;
  }
  
  public MutableGraph<QueryNodeConfig> configGraph() {
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
  
  public Map<String, String> sinkFilters() {
    return sink_filter;
  }
  
  @Override
  public QueryNode nodeForId(final String id) {
    return nodes_map.get(id);
  }
  
  /**
   * Helper for unit testing.
   * @param id A non-null ID to search for.
   * @return The matching config node if found, null if not.
   */
  public QueryNodeConfig configNodeForId(final String id) {
    for (final QueryNodeConfig config : config_graph.nodes()) {
      if (config.getId().equals(id)) {
        return config;
      }
    }
    return null;
  }
  
  /**
   * Recursive function that calculates the IDs that we should see 
   * emitted through the pipeline as QueryResult objects.
   * TODO - this assumes one result per data source.
   * @param node The non-null node to work from.
   * @return A set of unique results we should see.
   */
  private Set<String> computeSerializationSources(final QueryNodeConfig node) {
    if (node instanceof TimeSeriesDataSourceConfig ||
        node.joins()) {
      return Sets.newHashSet(node.getId());
    }
    
    final Set<String> ids = Sets.newHashSetWithExpectedSize(1);
    for (final QueryNodeConfig downstream : config_graph.successors(node)) {
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

    @Override
    public Builder toBuilder() {
      // TODO Auto-generated method stub
      return null;
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
    for (final QueryNodeConfig n : config_graph.predecessors(old_config)) {
      upstream.add(n);
    }
    for (final QueryNodeConfig n : upstream) {
      config_graph.removeEdge(n, old_config);
    }
    
    final List<QueryNodeConfig> downstream = Lists.newArrayList();
    for (final QueryNodeConfig n : config_graph.successors(old_config)) {
      downstream.add(n);
    }
    for (final QueryNodeConfig n : downstream) {
      config_graph.removeEdge(old_config, n);
    }
    
    config_graph.removeNode(old_config);
    config_graph.addNode(new_config);
    
    if (old_config instanceof TimeSeriesDataSourceConfig && 
        source_nodes.contains(old_config)) {
      source_nodes.remove(old_config);
    }
    
    if (new_config instanceof TimeSeriesDataSourceConfig) {
      source_nodes.add(new_config);
    }
    
    for (final QueryNodeConfig up : upstream) {
      config_graph.putEdge(up, new_config);
      if (Graphs.hasCycle(config_graph)) {
        throw new IllegalArgumentException("Cycle found linking node " 
            + up.getId() + " to " + new_config.getId());
      }
    }
    
    for (final QueryNodeConfig down : downstream) {
      config_graph.putEdge(new_config, down);
      if (Graphs.hasCycle(config_graph)) {
        throw new IllegalArgumentException("Cycle found linking node " 
            + new_config.getId() + " to " + down.getId());
      }
    }
  }

  @Override
  public boolean addEdge(final QueryNodeConfig from, 
                        final QueryNodeConfig to) {
    final boolean added = config_graph.putEdge(from, to);
    if (Graphs.hasCycle(config_graph)) {
      throw new IllegalArgumentException("Cycle found linking node " 
          + from.getId() + " to " + to.getId()); 
    }
    
    if (from instanceof TimeSeriesDataSourceConfig) {
      source_nodes.add(from);
    }
    if (to instanceof TimeSeriesDataSourceConfig) {
      source_nodes.add(to);
    }
    return added;
  }

  @Override
  public boolean removeEdge(final QueryNodeConfig from, 
                            final QueryNodeConfig to) {
    if (config_graph.removeEdge(from, to)) {
      if (config_graph.predecessors(from).isEmpty() && 
          config_graph.successors(from).isEmpty()) {
        config_graph.removeNode(from);
        if (from instanceof TimeSeriesDataSourceConfig) {
          source_nodes.remove(from);
        }
      }
      
      if (config_graph.predecessors(to).isEmpty() && 
          config_graph.successors(to).isEmpty()) {
        config_graph.removeNode(to);
        if (to instanceof TimeSeriesDataSourceConfig) {
          source_nodes.remove(to);
        }
      }
      return true;
    }
    return false;
  }

  @Override
  public boolean removeNode(final QueryNodeConfig config) {
    if (config_graph.removeNode(config)) {
      if (config instanceof TimeSeriesDataSourceConfig) {
        source_nodes.remove(config);
      }
      return true;
    }
    return false;
  }
  
  @Override
  public QueryNodeFactory getFactory(final QueryNodeConfig node) {
    final String key;
    if (node instanceof TimeSeriesDataSourceConfig) {
      key = Strings.isNullOrEmpty(((TimeSeriesDataSourceConfig) node)
              .getSourceId()) ? null : 
                ((TimeSeriesDataSourceConfig) node)
                  .getSourceId().toLowerCase();
    } else if (!Strings.isNullOrEmpty(node.getType())) {
      key = node.getType().toLowerCase();
    } else {
      key = node.getId().toLowerCase();
    }
    
    QueryNodeFactory factory = factory_cache.get(key);
    if (factory != null) {
      return factory;
    }
    factory = context.tsdb().getRegistry().getQueryNodeFactory(key);
    if (factory != null) {
      factory_cache.put(key, factory);
    }
    return factory;
  }
  
  @Override
  public Collection<QueryNodeConfig> terminalSourceNodes(final QueryNodeConfig config) {
    final Set<QueryNodeConfig> successors = config_graph.successors(config);
    if (successors.isEmpty()) {
      // some nodes in between may be sources but NOT a terminal. We only want
      // the terminals.
      if (config instanceof TimeSeriesDataSourceConfig) {
        return Lists.newArrayList(config);
      }
      return Collections.emptyList();
    }
    
    Set<QueryNodeConfig> sources = Sets.newHashSet();
    for (final QueryNodeConfig successor : successors) {
      sources.addAll(terminalSourceNodes(successor));
    }
    return sources;
  }
  
  /**
   * Recursive search for joining nodes (like mergers) that would run
   * into multiple sources with different byte IDs (or byte IDs and string
   * IDs) that need to be converted to strings for proper joins. Start
   * by passing the source node and it will walk up to find joins.
   * 
   * @param current The non-null current node.
   */
  private void needByteIdConverter(final QueryNodeConfig current) {
    if (!(current instanceof TimeSeriesDataSourceConfig) &&
        current.joins()) {
      final Map<String, TypeToken<? extends TimeSeriesId>> source_ids = 
          Maps.newHashMap();
      uniqueSources(current, source_ids);
      if (!source_ids.isEmpty() && source_ids.size() > 1) {
        // check to see we have at least a byte ID in the mix
        int byte_ids = 0;
        for (final TypeToken<? extends TimeSeriesId> type : source_ids.values()) {
          if (type == Const.TS_BYTE_ID) {
            byte_ids++;
          }
        }
        
        if (byte_ids > 0) {
          // OOH we may need to add one!
          Set<QueryNodeConfig> successors = config_graph.successors(current);
          if (successors.size() == 1 && 
              successors.iterator().next() instanceof ByteToStringIdConverterConfig) {
            // nothing to do!
          } else {
            // woot, add one!
            QueryNodeConfig config = ByteToStringIdConverterConfig.newBuilder()
                .setId(current.getId() + "_IdConverter")
                .build();
            successors = Sets.newHashSet(successors);
            for (final QueryNodeConfig successor : successors) {
              removeEdge(current, successor);
              addEdge(config, successor);
            }
            addEdge(current, config);
          }
        }
        return;
      } 
    }
    
    Set<QueryNodeConfig> predecessors = config_graph.predecessors(current);
    if (!predecessors.isEmpty()) {
      predecessors = Sets.newHashSet(predecessors);
    }
    for (final QueryNodeConfig predecessor : predecessors) {
      needByteIdConverter(predecessor);
    }
  }
  
  /**
   * Helper that walks down from the join config to determine if a the 
   * sources feeding that node have byte IDs or not.
   * 
   * @param current The non-null current node.
   * @param source_ids A non-null map of data source to ID types.
   */
  private void uniqueSources(final QueryNodeConfig current, 
                     final Map<String, TypeToken<? extends TimeSeriesId>> source_ids) {
    if (current instanceof TimeSeriesDataSourceConfig && 
        current.getSources().isEmpty()) {
      TimeSeriesDataSourceFactory factory = (TimeSeriesDataSourceFactory) 
          getFactory(current);
      source_ids.put(
        Strings.isNullOrEmpty(factory.id()) ? "null" : factory.id(),
        factory.idType());
    } else {
      for (final QueryNodeConfig successor : config_graph.successors(current)) {
        uniqueSources(successor, source_ids);
      }
    }
  }
  
  /**
   * Helper for UTs and debugging to print the graph.
   */
  public String printConfigGraph() {
    final StringBuilder buffer = new StringBuilder();
    buffer.append(" -------------------------\n");
    for (final QueryNodeConfig node : config_graph.nodes()) {
      buffer.append("[V] " + node.getId() + " (" + node.getClass().getSimpleName() + ")\n");
    }
    buffer.append("\n");
    for (final EndpointPair<QueryNodeConfig> pair : config_graph.edges()) {
      buffer.append("[E] " + pair.nodeU().getId() + " => " + pair.nodeV().getId() + "\n");
    }
    buffer.append(" -------------------------\n");
    return buffer.toString();
  }
  
  /**
   * Helper for UTs and debugging to print the graph.
   */
  public void printNodeGraph() {
    System.out.println(" ------------------------- ");
    for (final QueryNode node : graph.nodes()) {
      System.out.println("[V] " + node.config().getId() + " (" 
          + node.getClass().getSimpleName() + ")");
    }
    System.out.println();
    for (final EndpointPair<QueryNode> pair : graph.edges()) {
      System.out.println("[E] " + pair.nodeU().config().getId() 
          + " => " + pair.nodeV().config().getId());
    }
    System.out.println(" ------------------------- ");
  }
}