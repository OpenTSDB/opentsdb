// This file is part of OpenTSDB.
// Copyright (C) 2018-2020  The OpenTSDB Authors.
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

import java.time.Duration;
import java.time.temporal.TemporalAmount;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.query.BaseTimeSeriesDataSourceConfig;
import net.opentsdb.query.DefaultQueryResultId;

import net.opentsdb.query.QueryNodeConfigOptions;
import net.opentsdb.query.TimeSeriesDataSourceConfig.Builder;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.query.processor.downsample.DownsampleFactory;
import net.opentsdb.query.processor.timeshift.TimeShiftConfig;
import net.opentsdb.utils.DateTime;
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
import net.opentsdb.query.QueryResultId;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.idconverter.ByteToStringIdConverterConfig;
import net.opentsdb.query.processor.expressions.ExpressionConfig;
import net.opentsdb.query.processor.expressions.ExpressionParseNode;
import net.opentsdb.query.processor.merge.MergerConfig;
import net.opentsdb.query.processor.summarizer.SummarizerConfig;
import net.opentsdb.query.serdes.SerdesOptions;
import net.opentsdb.stats.Span;
import net.opentsdb.utils.Deferreds;
import net.opentsdb.utils.Pair;

/**
 * A query planner that handles push-down operations to data sources.
 *
 * <b>NOTE:</b> The planner is NOT thread safe.
 * {@link QueryNodeFactory#setupGraph(QueryPipelineContext, QueryNodeConfig, QueryPlanner)}
 * calls may <b>NOT</b> mutate the graph in a different thread.
 * 
 * TODO - Improve the performance. There are multiple iterations "up" and "down"
 * the graph right now. We can likely track state better and reduce that, even
 * though the vast majority of graphs would have a handful or a couple dozen
 * nodes at the most.
 *
 * ID Converter - This is needed for sources like HBase that will maintain UID
 * ID as long as possible before converting to strings, seeing as how most of
 * the time data is grouped so most of those tags don't need resolution. When
 * dealing with the converter, remember:
 * <ol>
 *   <li>It must appear before the serializer whenever one or more sources
 *   have a byte ID.</li>
 *   <li>If all sources have string IDs, it's not needed.</li>
 *   <li>If one byte ID source and a string ID source <b>OR</b> a
 *   <i>different</i> source feed into a node that <i>joins</i> two or more
 *   sources, the converter must be before the join in the graph. E.g. two
 *   different HBase sources in the same query will have different UIDs.</li>
 *   <li>It's ok to feed sources into an ID that are already strings. They're
 *   just passed through.</li>
 *   <li>Join nodes may need the converted IDs at node initialization so we
 *   have to walk and add the converter <i>before</i> constructing the node
 *   graph.</li>
 * </ol>
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
  protected List<QueryResultId> serialization_sources;
  
  /** Flag set when one of the config graph modification methods are called from
   * a config setup method. */
  protected boolean modified_during_setup;
  
  /** The set of satisfied filters. */
  protected Set<String> satisfied_filters;
  
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
    buildInitialConfigGraph();
    setupConfigGraph();
    final List<Deferred<Void>> deferreds = checkForConvertersAndInitFilters();
    
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
    if (!already_setup.contains(node.hashCode())) {
      for (final QueryNodeConfig downstream : 
          Sets.newHashSet(config_graph.successors(node))) {
        if (recursiveSetup(downstream, already_setup, satisfied_filters)) {
          return true;
        }
      }
      
      if (node == context_sink_config) {
        return false;
      }
      
      if (sink_filter.containsKey(node.getId())) {
        config_graph.putEdge(context_node, node);
        if (Graphs.hasCycle(config_graph)) {
          throw new IllegalArgumentException("Cycle found linking node " 
              + context_node.getId() + " to " + node.getId());
        }
        satisfied_filters.add(node.getId());
      }
      
      final QueryNodeFactory factory = getFactory(node);
      if (factory == null) {
        throw new QueryExecutionException("No factory found for: "
            + node.getId(), 400);
      }
      factory.setupGraph(context, node, this);
      already_setup.add(node.hashCode());
      if (modified_during_setup) {
        return true;
      }
    } else {
      // TODO - TEMP!! Special summary pass through code
      if (node instanceof SummarizerConfig && 
          ((SummarizerConfig) node).passThrough() &&
          (!sink_filter.isEmpty() ? sink_filter.containsKey(node.getId()) : true)) {
        final Set<QueryNodeConfig> successors = 
            Sets.newHashSet(config_graph.successors(node));
        for (final QueryNodeConfig successor : successors) {
          sink_filter.remove(successor.getId());
          roots.remove(successor);
          satisfied_filters.add(successor.getId());
          config_graph.removeEdge(context_node, successor);
        }
      }

      // skip the node that's already been setup.
      return false;
    }
    
    // Default code path that simply brings forward the sources and replaces the
    // node ID if no sources were set during the setup phase.
    if (node.resultIds().isEmpty()) {
      List<QueryResultId> ids = compileResultIds(node);
      QueryNodeConfig.Builder builder = node.toBuilder()
          .setResultIds(ids)
          .setSources(node.getSources());
      this.replace(node, builder.build());
      return true;
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
    if (serialization_sources != null) {
      for (final QueryResultId id : (List<QueryResultId>) node.resultIds()) {
        if (serialization_sources.contains(id)) {
          serialization_sources.remove(id);
          for (final QueryResultId parent_id : (List<QueryResultId>) parent.resultIds()) {
            if (parent_id.dataSource().equals(id.dataSource()) &&
                !serialization_sources.contains(parent_id)) {
              serialization_sources.add(parent_id);
            }
          }
        }
      }
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
            throw new IllegalArgumentException("Cycle adding " 
                + context_sink + " => " + source_node);
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
      if (!data_sources.contains(query_node)) {
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
  @Override
  public MutableGraph<QueryNode> graph() {
    return graph;
  }
  
  @Override
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
  public List<QueryResultId> serializationSources() {
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
   * TODO - look at this to find a better way than having a generic
   * config.
   */
  public class ContextNodeConfig implements QueryNodeConfig {

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
    public boolean nodeFlag(QueryNodeConfigOptions option) {
      return false;
    }

    @Override
    public Object nodeOption(QueryNodeConfigOptions option) {
      return null;
    }

    @Override
    public boolean readCacheable() {
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
      return null;
    }

    @Override
    public int compareTo(Object o) {
      return 0;
    }
  
    @Override
    public List<Pair<String, String>> resultIds() {
      return Collections.emptyList();
    }

    @Override
    public boolean markedCacheable() {
      return false;
    }
    
    @Override
    public void markCacheable(final boolean cacheable) {
      // no-op
    }
    
  }

  /**
   * Helper to replace a node with a new one, moving edges.
   * @param old_config The non-null old node that is present in the graph.
   * @param new_config The non-null new node that is not present in the graph.
   */
  public void replace(final QueryNodeConfig old_config,
                      final QueryNodeConfig new_config) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Replacing node " + old_config.getId() 
        + " (" + System.identityHashCode(old_config) + ") with " 
          + new_config.getId() + " (" + System.identityHashCode(new_config) + ")");
    }
    modified_during_setup = true;
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
    modified_during_setup = true;
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
      modified_during_setup = true;
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
      modified_during_setup = true;
      return true;
    }
    return false;
  }
  
  @Override
  public QueryNodeFactory getFactory(final QueryNodeConfig node) {
    String key;
    if (node instanceof TimeSeriesDataSourceConfig) {
      key = Strings.isNullOrEmpty(((TimeSeriesDataSourceConfig) node)
              .getSourceId()) ? null : 
                ((TimeSeriesDataSourceConfig) node)
                  .getSourceId().toLowerCase();
      if (key != null && key.contains(":")) {
        key = key.substring(0, key.indexOf(":"));
      }
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
  
  @Override
  public String getMetricForDataSource(final QueryNodeConfig node, 
                                       final String data_source_id) {
    if (node instanceof TimeSeriesDataSourceConfig &&
        (node.getId().equals(data_source_id) /*||
         ((TimeSeriesDataSourceConfig) node).getDataSourceId().equals(data_source_id)*/)) {
      return ((TimeSeriesDataSourceConfig) node).getMetric().getMetric();
    } else if (node.joins()) {
      if (node instanceof MergerConfig) {
        if (!((MergerConfig) node).getDataSource().equals(data_source_id)) {
          return null;
        }
        
        // depth first as we're guaranteed, at least for now, to have something
        // like merger <- ha <- src1
        //                 ^--- src2
        // where each src has the same metric.
        QueryNodeConfig config = config_graph.successors(node).iterator().next();
        while (config != null && !(config instanceof TimeSeriesDataSourceConfig)) {
          final Set<QueryNodeConfig> successors = config_graph.successors(config);
          if (successors.isEmpty()) {
            config = null;
          } else {
            config = successors.iterator().next();
          }
        }
        
        if (config == null) {
          return null;
        }
        
        return ((TimeSeriesDataSourceConfig) config).getMetric().getMetric();
      } else if (node instanceof ExpressionConfig) {
        return ((ExpressionConfig) node).getAs() == null ? node.getId() : 
          ((ExpressionConfig) node).getAs();
      } else if (node instanceof ExpressionParseNode) {
        return ((ExpressionParseNode) node).getAs() == null ? node.getId() : 
          ((ExpressionParseNode) node).getAs();
      }
    }
    
    for (final QueryNodeConfig successor : config_graph.successors(node)) {
      final String metric = getMetricForDataSource(successor, data_source_id);
      if (metric != null) {
        return metric;
      }
    }
    
    return null;
  }

  @Override
  public TimeAdjustments getAdjustments(
          final TimeSeriesDataSourceConfig config) {
    return recursiveAdjustments(config, null);
  }

  /**
   * Helper to traverse the graph and find the proper adjustments if any.
   * @param config The non-null config to walk back from.
   * @param adjustments An optional adjustment. Can be null.
   * @return Adjustments if found, null if not.
   */
  protected TimeAdjustments recursiveAdjustments(final QueryNodeConfig config,
                                                 TimeAdjustments adjustments) {
    final Set<QueryNodeConfig> predecessors = config_graph.predecessors(config);
    for (final QueryNodeConfig predecessor : predecessors) {
      if (predecessor instanceof DownsampleConfig) {
        if (adjustments == null) {
          adjustments = new TimeAdjustments();
          adjustments.downsampleInterval =
                  ((DownsampleConfig) predecessor).getInterval();
          if (adjustments.downsampleInterval.equals(DownsampleConfig.AUTO)) {
            final long deltaSeconds = context.query().endTime().epoch() -
                    context.query().startTime().epoch();
            adjustments.downsampleInterval =
                    DownsampleFactory.getAutoInterval(context().tsdb(),
                            deltaSeconds,
                            ((DownsampleConfig) predecessor).getMinInterval());
          } else if (((DownsampleConfig) predecessor).getRunAll()) {
            adjustments.downsampleInterval = null;
          }
        }

        adjustments = recursiveAdjustments(predecessor, adjustments);
        continue;
      }

      final Integer paddingIntervals = (Integer) predecessor.nodeOption(
              QueryNodeConfigOptions.PREVIOUS_INTERVALS);
      if (paddingIntervals != null) {
        if (adjustments == null) {
          adjustments = new TimeAdjustments();
        }
        if (paddingIntervals > adjustments.previousIntervals) {
          adjustments.previousIntervals = paddingIntervals;
        }
      }

      final String paddingWindow = (String) predecessor.nodeOption(
              QueryNodeConfigOptions.PADDING_WINDOW);
      if (paddingWindow != null) {
        if (adjustments == null) {
          adjustments = new TimeAdjustments();
        }

        if (adjustments.windowInterval == null) {
          adjustments.windowInterval = paddingWindow;
        } else {
          // find the largest.
          long extant = DateTime.parseDuration(adjustments.windowInterval);
          long next = DateTime.parseDuration(paddingWindow);
          if (next > extant) {
            adjustments.windowInterval = paddingWindow;
          }
        }

        if (adjustments.downsampleInterval != null) {
          // validation that the sliding window is >= ds interval.
          long ds = DateTime.parseDuration(adjustments.downsampleInterval);
          long window = DateTime.parseDuration(adjustments.windowInterval);
          if (window < ds) {
            throw new IllegalArgumentException("The window of config "
                    + predecessor.getId() + " is smaller than the first " +
                    "downsample interval of " + adjustments.downsampleInterval
                    + ". The window must be larger than the interval and it " +
                    "should be a multiple of the interval.");
          }
        }
      }

      // TODO - figure out the best traversal for this.
      adjustments = recursiveAdjustments(predecessor, adjustments);
    }

    return adjustments;
  }

  /**
   * Super simple, just runs through the given list of nodes and tries to
   * generate a DAG from the given list of sources to each node.
   */
  protected void buildInitialConfigGraph() {
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

      List<String> sources = node.getSources();
      if (sources != null) {
        for (final String source : sources) {
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
    
    // ugg... loop again and setup the links to the context config so we can do
    // a proper depth first setup recursion.
    for (final QueryNodeConfig node : config_graph.nodes()) {
      if (node == context_node) {
        continue;
      }
      
      if (sink_filter.containsKey(node.getId())) {
        config_graph.putEdge(context_node, node);
        if (Graphs.hasCycle(config_graph)) {
          throw new IllegalArgumentException("Cycle found linking node " 
              + context_node.getId() + " to " + node.getId());
        }
        continue;
      }
      
      if (config_graph.predecessors(node).isEmpty() && sink_filter.isEmpty()) {
        config_graph.putEdge(context_node, node);
        if (Graphs.hasCycle(config_graph)) {
          throw new IllegalArgumentException("Cycle found linking node " 
              + context_node.getId() + " to " + node.getId());
        }
      }
    }
  }

  /**
   * This is the optimizer in that it calls the
   * {@link QueryNodeFactory#setupGraph(QueryPipelineContext, QueryNodeConfig, QueryPlanner)}
   * method on each config object until the graph is no longer modified.
   */
  protected void setupConfigGraph() {
    satisfied_filters = Sets.newHashSet();
    // next we walk and let the factories update the graph as needed.
    // Note the clone to avoid concurrent modification of the graph.
    Set<Integer> already_setup = Sets.newHashSet();
    //boolean modified = true;
    modified_during_setup = true;
    while (modified_during_setup) {
      if (source_nodes.isEmpty()) {
        break;
      }
      modified_during_setup = false;
      recursiveSetup(context_node, already_setup, satisfied_filters);
    }
    
    // one more iteration to make sure we capture all the source nodes
    // from the graph setup.
    source_nodes.clear();
    for (final QueryNodeConfig node : config_graph.nodes()) {
      if (node instanceof TimeSeriesDataSourceConfig) {
        source_nodes.add(node);
      }
    }
  }
  
  protected List<Deferred<Void>> checkForConvertersAndInitFilters() {
    final List<Deferred<Void>> deferreds = 
        Lists.newArrayListWithExpectedSize(source_nodes.size());
    boolean needsTopLevelConverter = false;

    for (final QueryNodeConfig config : Lists.newArrayList(source_nodes)) {
      // see if we need to insert a byte Id converter upstream.
      final TimeSeriesDataSourceFactory factory = ((TimeSeriesDataSourceFactory) getFactory(config));
      if (((TimeSeriesDataSourceConfig) config).getFilter() != null) {
        deferreds.add(((TimeSeriesDataSourceConfig) config)
                .getFilter().initialize(null /* TODO */));
      }

      if (!needByteIdConverter(config)) {
        needsTopLevelConverter = factory.idType() != Const.TS_STRING_ID;
      }
    }
    
    if (needsTopLevelConverter) {
      computeSerializationSources();
      final QueryNodeConfig converter = ByteToStringIdConverterConfig.newBuilder()
          .setResultIds(serialization_sources)
          // TODO - find a better unique ID.
          .setId(globalIDConverter())
          // sources
          .build();
      replace(context_sink_config, converter);
      addEdge(context_sink_config, converter);
    }
    return deferreds;
  }
  
  protected void verifySinkFilters() {
    for (final String key : sink_filter.keySet()) {
      if (!satisfied_filters.contains(key)) {
        // it may have been added in a setup step so check for the node in the
        // graph.
        boolean found = false;
        if (!found) {
          throw new QueryExecutionException("Unsatisfied sink filter: " 
              + key + printConfigGraph(), 400);
        }
      }
    }
  }
  
  protected void setupPushDowns() {
    // next, push down by walking up from the data sources.
    final List<QueryNodeConfig> copy = Lists.newArrayList(source_nodes);
    for (final QueryNodeConfig node : copy) {
      // TODO - this COULD be ok in the future, we just need to add the feature.
      if (sink_filter != null && sink_filter.containsKey(node.getId())) {
        continue;
      }

      final QueryNodeFactory factory = getFactory(node);
      // TODO - cleanup the source factories. ugg!!!
      if (factory == null || !(factory instanceof TimeSeriesDataSourceFactory)) {
        throw new QueryExecutionException("No node factory found for "
            + "configuration " + node + "  Factory=" + factory, 400);
      }
      
      final List<QueryNodeConfig> push_downs = Lists.newArrayList();
      final Set<QueryNodeConfig> nodes = Sets.newHashSet(config_graph.predecessors(node));
      for (final QueryNodeConfig n : nodes) {
        pushDown(
            node, 
            node, 
            (TimeSeriesDataSourceFactory) factory, 
            n, 
            push_downs);
      }
      
      if (!push_downs.isEmpty()) {
        // fix up sources and result IDs in case there were multiple sources feeding
        // into the pushdown nodes.
        for (int i = 0; i < push_downs.size(); i++) {
          final QueryNodeConfig config = push_downs.get(i);
          if (config.getSources().size() > 1 || config.resultIds().size() > 1) {
            final String wanted = i > 0 ?
                    push_downs.get(i - 1).getId() :
                    node.getId();
            final QueryNodeConfig.Builder builder = config.toBuilder();
            builder.setSources(Lists.newArrayList(wanted))
                    // TODO - WARNING - Gotcha here in that in the future if we
                    // push down something like a summarizer -> gb -> ds we want
                    // to serialize the summarizer && gb, we'd need to NOT remove
                    // the gb node.
                    .setResultIds(Lists.newArrayList(
                            new DefaultQueryResultId(config.getId(), node.getId())));
            push_downs.set(i, builder.build());
          }
        }
        // now dump the push downs into this node.
        final TimeSeriesDataSourceConfig tsDataSourceconfig = 
            (TimeSeriesDataSourceConfig) node;
        final TimeSeriesDataSourceConfig new_config =
            (TimeSeriesDataSourceConfig) 
            ((BaseTimeSeriesDataSourceConfig.Builder) 
                tsDataSourceconfig.toBuilder())
            .setPushDownNodes(push_downs)
            .setResultIds(push_downs.get(push_downs.size() - 1).resultIds())
            .build();
        replace(node, new_config);
      }
    }
  }
  
  protected void computeSerializationSources() {
    if (serialization_sources != null) {
      return;
    }
    serialization_sources = Lists.newArrayList();
    for (final QueryNodeConfig node : config_graph.successors(context_node)) {
      for (final QueryResultId source : (List<QueryResultId>) node.resultIds()) {
        if (serialization_sources.contains(source)) {
          continue;
        }
        serialization_sources.add(source);
      }
    }
    
    // cleanout nodes that don't contribute to serialization. This can save
    // some fetch time if we get a data source that isn't serialized or used
    // in some computation!
    final Set<QueryNodeConfig> nodes = Sets.newHashSet(config_graph.nodes());
    for (final QueryNodeConfig config : nodes) {
      if (!linksToContext(config)) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Removing non-contributing node: " + config.getId());
        }
        removeNode(config);
      }
    }
  }
  
  protected Deferred<Void> buildAndInitNodes() {
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
    return recursiveInit(context_sink, initialized, null /* TODO */)
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
  
  /**
   * Method to iterate over the immediate successors of the given node config to
   * compile a combined list of result IDs.
   * @param config A non-null config in the graph.
   * @return A non-null, potentially empty, list of result IDs.
   */
  public List<QueryResultId> compileResultIds(final QueryNodeConfig config) {
    final List<QueryResultId> ids = Lists.newArrayList();
    Set<QueryNodeConfig> downstreams = Sets.newHashSet(config_graph.successors(config));
    for (QueryNodeConfig downstream : downstreams) {
      if (downstream.resultIds() == null || downstream.resultIds().isEmpty()) {
        QueryNodeConfig newConfig = downstream.toBuilder()
                .setResultIds(compileResultIds(downstream))
                .build();
        replace(downstream, newConfig);
        downstream = newConfig;
      }
      for (final QueryResultId source : 
          (List<QueryResultId>) downstream.resultIds()) {
        ids.add(new DefaultQueryResultId(config.getId(), source.dataSource()));
      }
    }
    return ids;
  }
  
  class ConfigInitCB implements Callback<Deferred<Void>, Void> {

    @Override
    public Deferred<Void> call(final Void ignored) throws Exception {
      // before doing any more work, make sure the the filters have been
      // satisfied.
      verifySinkFilters();
      setupPushDowns();
      
      // TODO clean out nodes that won't contribute to serialization.
      // compute source IDs.
      computeSerializationSources();

      // now go and build the node graph
      return buildAndInitNodes();
    }
    
  }
  
  /**
   * Recursive search for joining nodes (like mergers) that would run
   * into multiple sources with different byte IDs (or byte IDs and string
   * IDs) that need to be converted to strings for proper joins. Start
   * by passing the source node and it will walk up to find joins.
   * 
   * @param current The non-null current node.
   * @return True if an ID was inserted before a join in which case we may not
   * need the top-level converter.
   */
  private boolean needByteIdConverter(final QueryNodeConfig current) {
    if (current instanceof ByteToStringIdConverterConfig) {
      return true;
    }

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
            return true;
          } else {
            // woot, add one!
            QueryNodeConfig.Builder builder = ByteToStringIdConverterConfig.newBuilder()
                .setId(current.getId() + "_IdConverter");
            final QueryNodeConfig temp = builder.build();
            successors = Sets.newHashSet(successors);
            for (final QueryNodeConfig successor : successors) {
              removeEdge(current, successor);
              addEdge(temp, successor);
              builder.addSource(successor.getId());
              for (final QueryResultId qri : (List<QueryResultId>) successor.resultIds()) {
                builder.addResultId(qri);
              }
            }
            addEdge(current, temp);
            replace(temp, builder.build());
            return true;
          }
        }
        return byte_ids > 0;
      } 
    }
    
    Set<QueryNodeConfig> predecessors = config_graph.predecessors(current);
    if (!predecessors.isEmpty()) {
      predecessors = Sets.newHashSet(predecessors);
    }

    boolean added = false;
    for (final QueryNodeConfig predecessor : predecessors) {
      if (needByteIdConverter(predecessor)) {
        added = true;
      }
    }
    return added;
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
    if (current instanceof TimeSeriesDataSourceConfig) {
      final TimeSeriesDataSourceFactory factory =
              (TimeSeriesDataSourceFactory) getFactory(current);
      source_ids.put(
              Strings.isNullOrEmpty(factory.id()) ? "null" : factory.id(),
              factory.idType());
    } else if (current instanceof ByteToStringIdConverterConfig) {
      // found a converter, upstream of this will be strings.
      return;
    } else {
      for (final QueryNodeConfig successor : config_graph.successors(current)) {
        uniqueSources(successor, source_ids);
      }
    }

    // TODO - what if we hit a join? For now we're walking past it and if we
    // see that there is already a converter there, we can just walk back up.
  }
  
  /**
   * Recursive method to find out if a node contributes to an operation that will
   * be serialized.
   * @param config A non-null config to work on.
   * @return True if the node does link to the query context config through the
   * graph, false if it doesn't.
   */
  private boolean linksToContext(final QueryNodeConfig config) {
    if (config == context_sink_config) {
      return true;
    }
    
    final Set<QueryNodeConfig> predecessors = config_graph.predecessors(config);
    if (predecessors.isEmpty()) {
      return false;
    }
    
    for (final QueryNodeConfig predecessor : predecessors) {
      if (linksToContext(predecessor)) {
        return true;
      }
    }
    
    return false;
  }
  
  /**
   * Helper for UTs and debugging to print the graph.
   */
  public String printConfigGraph() {
    final StringBuilder buffer = new StringBuilder();
    buffer.append(" -------------------------\n");
    for (final QueryNodeConfig node : config_graph.nodes()) {
      buffer.append("[V] " + node.getId() + " {" 
          + node.getClass().getSimpleName() + "} (" 
          + System.identityHashCode(node) + ") "
          + node.resultIds() + "\n");
    }
    buffer.append("\n");
    for (final EndpointPair<QueryNodeConfig> pair : config_graph.edges()) {
      buffer.append("[E] " + pair.nodeU().getId() 
          + " (" + System.identityHashCode(pair.nodeU()) + ") => " 
          + pair.nodeV().getId() + " (" 
          + System.identityHashCode(pair.nodeV()) + ")\n");
    }
    buffer.append(" -------------------------\n");
    return buffer.toString();
  }
  
  /**
   * Helper for UTs and debugging to print the graph.
   */
  public String printNodeGraph() {
    final StringBuilder buffer = new StringBuilder();
    buffer.append(" -------------------------\n");
    for (final QueryNode node : graph.nodes()) {
      buffer.append("[V] " + node.config().getId() 
          + " {" + node.getClass().getSimpleName() + "} (" 
          + System.identityHashCode(node) + ")\n");
    }
    buffer.append("\n");
    for (final EndpointPair<QueryNode> pair : graph.edges()) {
      buffer.append("[E] " + pair.nodeU().config().getId() 
          + " (" + System.identityHashCode(pair.nodeU()) + ") => " 
          + pair.nodeV().config().getId() + " (" 
          + System.identityHashCode(pair.nodeV()) + ")\n");
    }
    buffer.append(" -------------------------\n");
    return buffer.toString();
  }

  /** @return A unique ID for the converter that _shouldn't_ conflict with an
   * existing node in the graph. Uses the hash Id of the planner. */
  @VisibleForTesting
  public String globalIDConverter() {
    return "IDConverter_" + System.identityHashCode(this);
  }

  /**
   * Sets the time stamps for a TimeSeriesDataSourceConfig based on the query
   * and optional adjustments.
   *
   * <b>NOTE</b> Does _NOT_ account for timeshifts. Sources <b>STILL</b> need
   * to handle shifts. The reason being that nodes will look at the config
   * timestamps instead of the query time and we don't want every node to
   * have to worry about whether or not they appear after the time shift node
   * that converts the timestamps to query time.
   *
   * @param context The non-null context.
   * @param config The non-null config.
   * @param adjustments An optional adjustment object. Can be null if no
   *                    adjustments were required.
   * @return A non-null builder from the config with the timestamps set.
   */
  public static TimeSeriesDataSourceConfig.Builder setTimeOverrides(
          final QueryPipelineContext context,
          final TimeSeriesDataSourceConfig config,
          final TimeAdjustments adjustments) {
    final TimeSeriesDataSourceConfig.Builder builder =
            (TimeSeriesDataSourceConfig.Builder) config.toBuilder();
    // already setup. May have been sent from a middletier.
    if (config.startTimestamp() != null && config.endTimestamp() != null) {
      return builder;
    }

    if (adjustments != null) {
      final TimeStamp startOverride = context.query().startTime().getCopy();
      final TimeStamp endOverride = context.query().endTime().getCopy();

      if (adjustments.windowInterval != null) {
        // TODO - seconds only here. May need millis
        final long windowMs = DateTime.parseDuration(adjustments.windowInterval)
                / 1000;
        final long paddingSeconds = windowMs *
                (adjustments.previousIntervals > 0 ? adjustments.previousIntervals : 1);
        // TODO - find the limit here
        startOverride.subtract(Duration.ofSeconds(paddingSeconds));
      }

      builder.setStartTimeStamp(startOverride);
      builder.setEndTimeStamp(endOverride);
    } else {
      builder.setStartTimeStamp(context.query().startTime());
      builder.setEndTimeStamp(context.query().endTime());
    }
    return builder;
  }

  @Override
  public void baseSetupGraph(final QueryPipelineContext context,
                             final TimeSeriesDataSourceConfig config) {
    if (config.hasBeenSetup()) {
      // all done.
      return;
    }

    if (config.startTimestamp() == null) {
      TimeSeriesDataSourceConfig.Builder builder = DefaultQueryPlanner.setTimeOverrides(
              context,
              config,
              getAdjustments(config)
      );
      builder.setHasBeenSetup(true);

      // time shift
      if (builder.timeShifts() != null) {
        addShift(config, builder);
      } else {
        replace(config, builder.build());
      }
    } else if (config.timeShifts() != null) {
      addShift(config, (Builder) config.toBuilder());
    }
  }

  /**
   * Helper to search and add a time shift node to update the graph when a
   * time shift is in place.
   * @param config The non-null config that is shifted.
   * @param builder The non-null builder from the config.
   */
  void addShift(final TimeSeriesDataSourceConfig config,
                final TimeSeriesDataSourceConfig.Builder builder) {
    // if it's here, leave it be.
    Set<QueryNodeConfig> predecessors = config_graph.predecessors(config);

    // TODO - Ugly ugly hack for mergers. Generalize it.
    MergerConfig merger = null;
    for (final QueryNodeConfig pred : predecessors) {
      if (pred instanceof MergerConfig) {
        merger = (MergerConfig) pred;
        continue;
      }

      if (pred instanceof TimeShiftConfig) {
        // matched!
        return;
      }
    }

    final TimeShiftConfig shift = TimeShiftConfig.newBuilder()
            .setTimeshiftInterval(config.getTimeShiftInterval())
            .setId(config.getId() + "_timeShift")
            .addResultId(new DefaultQueryResultId(config.getId() + "_timeShift",
                    ((QueryResultId) config.resultIds().get(0)).dataSource()))
            .addSource(config.getId())
            .build();

    // IMPORTANT if the data source is in the serdes list we need to mutate it
    // so that it will point to this source.
    if (sink_filter.containsKey(config.getId())) {
      sink_filter.remove(config.getId());
      sink_filter.put(shift.getId(), null);
    }

    final TimeSeriesDataSourceFactory factory = (TimeSeriesDataSourceFactory)
            getFactory(config);
    if (factory.supportsPushdown(TimeShiftConfig.class)) {
      if (builder.pushDowns() == null) {
        builder.setPushDownNodes(Lists.newArrayList(shift));
      } else {
        builder.pushDowns().add(0, shift);
        if (builder.pushDowns().size() >= 2) {
          final QueryNodeConfig.Builder pdBuilder =
                  ((QueryNodeConfig) builder.pushDowns().get(1)).toBuilder();
          pdBuilder.setSources(Lists.newArrayList(shift.getId()));
          builder.pushDowns().set(1, pdBuilder.build());
        }
      }
      replace(config, builder.build());
    } else {
      replace(config, shift);
      addEdge(shift, builder.build());
    }
  }
}