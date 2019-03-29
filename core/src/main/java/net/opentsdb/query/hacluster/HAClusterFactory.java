// This file is part of OpenTSDB.
// Copyright (C) 2017-2018  The OpenTSDB Authors.
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
package net.opentsdb.query.hacluster;

import java.time.temporal.TemporalAmount;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.graph.Graphs;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.common.Const;
import net.opentsdb.configuration.ConfigurationCallback;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesDataSourceFactory;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.TimeSeriesQuery;
import net.opentsdb.query.hacluster.HAClusterConfig.Builder;
import net.opentsdb.query.idconverter.ByteToStringIdConverterConfig;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.plan.DefaultQueryPlanner;
import net.opentsdb.query.plan.QueryPlanner;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.processor.BaseQueryNodeFactory;
import net.opentsdb.query.processor.merge.MergerConfig;
import net.opentsdb.query.processor.timeshift.TimeShiftConfig;
import net.opentsdb.stats.Span;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.Pair;

/**
 * A factory that modifies the execution graph with nodes to execute an
 * HA query where the same data is written to two or more clusters and
 * the same query is executed against each source. The results are merged
 * so if one cluster was down for a period of time, data is filled from
 * an alternate.
 * <p>
 * Note that this factory performs extensive modifications of the graph. 
 * For each data source (either from the defaults or the config) a new 
 * node is added along with a new HA config and a merger node. Setup
 * will also walk up the graph to push down any nodes it can to the 
 * sources, in some cases moving the nodes so that they feed into the
 * merger. If only one data source is selected, the HA node is thrown
 * away and replaced with the source.
 * <p>
 * Note that the first entry in the {@link #default_sources} list is 
 * always considered the primary.
 * 
 * TODO - select the aggregator based on pushed down nodes like group by
 * and/or downsample. E.g. for average we may want to merge with min 
 * instead of max.
 * 
 * TODO - may need more work around > 2 nodes, i.e. do we wait for them
 * all to arrive?
 *
 * @since 3.0
 */
public class HAClusterFactory extends BaseQueryNodeFactory implements 
    TimeSeriesDataSourceFactory {
  public static final String TYPE = "HACluster";
  
  public static final String KEY_PREFIX = "tsd.query.";
  public static final String SOURCES_KEY = "hacluster.sources";
  public static final String AGGREGATOR_KEY = "hacluster.default.aggregator";
  public static final String PRIMARY_KEY = "hacluster.default.timeout.primary";
  public static final String SECONDARY_KEY = "hacluster.default.timeout.secondary";
  
  /** The default sources updated on config callback. */
  protected final List<String> default_sources;

  /**
   * Default plugin ctor.
   */
  public HAClusterFactory() {
    default_sources = Lists.newArrayListWithExpectedSize(2);
  }
  
  @Override
  public QueryNodeConfig parseConfig(final ObjectMapper mapper, 
                                     final TSDB tsdb,
                                     final JsonNode node) {
    return HAClusterConfig.parse(mapper, tsdb, node);
  }

  @Override
  public boolean supportsQuery(final TimeSeriesQuery query, 
                               final TimeSeriesDataSourceConfig config) {
    if (config instanceof HAClusterConfig) {
      final HAClusterConfig cluster_config = (HAClusterConfig) config;
      if (cluster_config.getHasBeenSetup()) {
        return true;
      }
      
      final List<String> sources;
      if (cluster_config.getDataSources().isEmpty() && 
          cluster_config.getDataSourceConfigs().isEmpty()) {
        // sub in the defaults.
        synchronized (default_sources) {
          sources = Lists.newArrayList(default_sources);
        }
      } else {
        sources = cluster_config.getDataSources();
      }
      
      for (final String source : sources) {
        final TimeSeriesDataSourceFactory factory = 
            tsdb.getRegistry().getPlugin(
                TimeSeriesDataSourceFactory.class, source);
        if (factory != null && factory.supportsQuery(query, config)) {
          return true;
        }
      }
      
      for (final TimeSeriesDataSourceConfig source : 
        cluster_config.getDataSourceConfigs()) {
        final TimeSeriesDataSourceFactory factory = 
            tsdb.getRegistry().getPlugin(
                TimeSeriesDataSourceFactory.class, source.getSourceId());
        if (factory != null && factory.supportsQuery(query, config)) {
          return true;
        }
      }
    } else {
      final List<String> sources;
      synchronized (default_sources) {
        sources = Lists.newArrayList(default_sources);
      }
      
      for (final String source : sources) {
        final TimeSeriesDataSourceFactory factory = 
            tsdb.getRegistry().getPlugin(
                TimeSeriesDataSourceFactory.class, source);
        if (factory != null && factory.supportsQuery(query, config)) {
          return true;
        }
      }
    }
    
    
    return false;
  }
  
  @Override
  public void setupGraph(final QueryPipelineContext context, 
                         final QueryNodeConfig config,
                         final QueryPlanner planner) {
    if (((TimeSeriesDataSourceConfig) config).hasBeenSetup()) {
      return;
    }
    
    final HAClusterConfig.Builder builder;
    boolean needs_id_converter = false;
    
    if (config instanceof HAClusterConfig) {
      final HAClusterConfig cluster_config = (HAClusterConfig) config;
      builder = (Builder) ((HAClusterConfig) config).toBuilder()
          .setHasBeenSetup(true);
      
      if (Strings.isNullOrEmpty(cluster_config.getMergeAggregator())) {
        builder.setMergeAggregator(tsdb.getConfig().getString(
            getConfigKey(AGGREGATOR_KEY)));
      }
      if (Strings.isNullOrEmpty(cluster_config.getPrimaryTimeout())) {
        builder.setPrimaryTimeout(tsdb.getConfig().getString(
            getConfigKey(PRIMARY_KEY)));
      }
      if (Strings.isNullOrEmpty(cluster_config.getSecondaryTimeout())) {
        builder.setSecondaryTimeout(tsdb.getConfig().getString(
            getConfigKey(SECONDARY_KEY)));
      }
      
      if (cluster_config.getDataSources().isEmpty() && 
          cluster_config.getDataSourceConfigs().isEmpty()) {
        // sub in the defaults.
        synchronized (default_sources) {
          builder.setDataSources(Lists.newArrayList(default_sources));
        }
      }
    } else {
      builder = HAClusterConfig.newBuilder();
      HAClusterConfig.newBuilder((TimeSeriesDataSourceConfig) config,
            builder);
      builder.setMergeAggregator(tsdb.getConfig().getString(
               getConfigKey(AGGREGATOR_KEY)))
             .setPrimaryTimeout(tsdb.getConfig().getString(
               getConfigKey(PRIMARY_KEY)))
             .setSecondaryTimeout(tsdb.getConfig().getString(
                getConfigKey(SECONDARY_KEY)))
             .setHasBeenSetup(true);
      synchronized (default_sources) {
        builder.setDataSources(Lists.newArrayList(default_sources));
      }
    }
    
    final String new_id = "ha_" + config.getId();
    if (context.query().isTraceEnabled()) {
      context.queryContext().logTrace("Elligible sources: " + builder.dataSources());
    }
    
    // if there is only one source, drop the merger and ha config nodes
    if (builder.dataSources().size() + builder.dataSourceConfigs().size() == 1) {
      if (!builder.dataSources().isEmpty()) {
        if (planner.context().tsdb().getRegistry().getPlugin(
              TimeSeriesDataSourceFactory.class, 
              builder.dataSources().get(0)) == null) {
          throw new IllegalArgumentException("No data source found for: " 
            + builder.dataSources().get(0));
        }
        
        QueryNodeConfig rebuilt = builder
            .setSourceId(builder.dataSources().get(0))
            .setId(config.getId())
            .build();
        planner.replace(config, rebuilt);
        
        // Check for time offsets.
        setupTimeShiftSingleNode((TimeSeriesDataSourceConfig) rebuilt, planner);
        return;
      }
      
      if (Strings.isNullOrEmpty(
          builder.dataSourceConfigs().get(0).getSourceId())) {
        throw new IllegalArgumentException("The sourceId cannot be null "
            + "or empty for the config override: " 
            + builder.dataSourceConfigs().get(0));
      }
      
      TimeSeriesDataSourceConfig rebuilt = (TimeSeriesDataSourceConfig)
         builder.dataSourceConfigs().get(0).toBuilder()
            .setId(config.getId())
            .build();
      planner.replace(config, rebuilt);
      if (context.query().isTraceEnabled()) {
        context.queryContext().logTrace("Only one source available for query: " 
            + rebuilt.getSourceId());
      }
      
      // Check for time offsets.
      setupTimeShiftSingleNode(rebuilt, planner);
      return;
    }
    
    final List<TimeSeriesDataSourceConfig.Builder> new_sources = 
        Lists.newArrayList();
    final Map<String, TimeSeriesDataSourceFactory> factories = Maps.newHashMap();
   
    if (config instanceof HAClusterConfig) {
      for (final TimeSeriesDataSourceConfig source : 
            ((HAClusterConfig) config).getDataSourceConfigs()) {
        if (Strings.isNullOrEmpty(source.getSourceId())) {
          throw new IllegalArgumentException("The sourceId cannot be null "
              + "or empty for the config override: " + source);
        }
        // we have to fix the ID here to avoid dupes and collisions.
        TimeSeriesDataSourceConfig.Builder rebuilt = (TimeSeriesDataSourceConfig.Builder)
            ((TimeSeriesDataSourceConfig.Builder) source.toBuilder())
              .setTimeShiftInterval(null)
              .setPreviousIntervals(0)
              .setNextIntervals(0)
              .setId(new_id + "_" + source.getSourceId());
        for (final TimeSeriesDataSourceConfig.Builder extant : new_sources) {
          if (extant.id().equals(rebuilt.id())) {
            throw new IllegalArgumentException("Duplicate source IDs are "
                + "not allowed: " + source);
          }
        }
        
        final TimeSeriesDataSourceFactory factory = 
            planner.context().tsdb().getRegistry().getPlugin(
              TimeSeriesDataSourceFactory.class, 
              source.getSourceId());
        if (factory == null) {
          throw new IllegalArgumentException("No data source found for: " 
              + source.getSourceId());
        }
        if (factory.idType() != Const.TS_STRING_ID) {
          needs_id_converter = true;
        }
        factories.put(source.getSourceId(), factory);
        new_sources.add(rebuilt);
      }
    }
    
    for (final String source : builder.dataSources()) {
      final TimeSeriesDataSourceFactory factory = 
          planner.context().tsdb().getRegistry().getPlugin(
              TimeSeriesDataSourceFactory.class, source); 
      if (factory == null) {
        throw new IllegalArgumentException("No data source found for: " 
          + source);
      }
      
      if (factory.idType() != Const.TS_STRING_ID) {
        needs_id_converter = true;
      }
      
      factories.put(source, factory);
      
      TimeSeriesDataSourceConfig.Builder rebuilt = (TimeSeriesDataSourceConfig.Builder)
            ((TimeSeriesDataSourceConfig.Builder) config.toBuilder())
            .setTimeShiftInterval(null)
            .setPreviousIntervals(0)
            .setNextIntervals(0)
            .setSourceId(source)
            .setId(new_id + "_" + source);
      for (final TimeSeriesDataSourceConfig.Builder extant : new_sources) {
        if (extant.id().equals(rebuilt.id())) {
          throw new IllegalArgumentException("Duplicate source IDs are "
              + "not allowed: " + source);
        }
      }
      new_sources.add(rebuilt);
    }
    
    // Pull down, e.g. if we send to remote sources then we can merge
    // at a higher level, after downsample and groupby for example. 
    // This may also determine the merge aggregator.
    Set<QueryNodeConfig> predecessors = planner.configGraph().predecessors(config);
    if (!predecessors.isEmpty() && predecessors.size() == 1) {
      QueryNodeConfig predecessor = predecessors.iterator().next();
      
      final List<List<QueryNodeConfig>> push_downs = Lists.newArrayList();
      int max_pushdowns = Integer.MIN_VALUE;
      int min_pushdowns = Integer.MAX_VALUE;
      int max_index = -1;
      for (int i = 0; i < new_sources.size(); i++) {
        final List<QueryNodeConfig> source_push_downs = Lists.newArrayList();
        canPushDown(predecessor, 
                    factories.get(new_sources.get(i).sourceId()), 
                    source_push_downs, 
                    planner);
        push_downs.add(source_push_downs);
        if (source_push_downs.size() > max_pushdowns) {
          max_pushdowns = source_push_downs.size();
          max_index = i;
        }
        if (source_push_downs.size() < min_pushdowns) {
          min_pushdowns = source_push_downs.size();
        }
      }
      
      if (max_pushdowns > 0) {
        MergerConfig merger = (MergerConfig) MergerConfig.newBuilder()
            .setAggregator(builder.mergeAggregator())
            // TODO - may want to make this configurable.
            .addInterpolatorConfig(NumericInterpolatorConfig.newBuilder()
              .setFillPolicy(FillPolicy.NONE)
              .setRealFillPolicy(FillWithRealPolicy.NONE)
              .setDataType(NumericType.TYPE.toString())
              .build())
            .addSource(new_id)
            .setId(builder.id())
            .build();
        planner.replace(config, merger);
        
        QueryNodeConfig converter;
        if (needs_id_converter) {
          converter = ByteToStringIdConverterConfig.newBuilder()
              .setId(config.getId() + "_converter")
              .build();
          planner.addEdge(merger, converter);
        } else {
          converter = null;
        }
        
        final List<String> data_sources = 
            Lists.newArrayListWithExpectedSize(new_sources.size());
        for (final TimeSeriesDataSourceConfig.Builder source : new_sources) {
          data_sources.add(source.id());
        }
        
        HAClusterConfig rebuilt = (HAClusterConfig) builder
            .setDataSources(data_sources)
            .setId(new_id)
            .build();
        planner.addEdge(converter != null ? converter : merger, rebuilt);
        
        final List<QueryNodeConfig> max = push_downs.get(max_index);
        final Map<String, QueryNodeConfig> new_push_downs = 
            Maps.newHashMapWithExpectedSize(max.size());
        QueryNodeConfig last = null;
        for (int i = min_pushdowns; i < max_pushdowns; i++) {
          final QueryNodeConfig push = max.get(i);
          if (last == null) {
            last = push.toBuilder()
                .setId(new_id + "_" + push.getId())
                // TODO .setPushDown(false)
                .build();
            new_push_downs.put(push.getId(), last);
          } else {
            QueryNodeConfig new_node = push.toBuilder()
                .setId(new_id + "_" + push.getId())
                // TODO .setPushDown(false)
                .build();
            new_push_downs.put(push.getId(), new_node);
            planner.addEdge(new_node, last);
            last = new_node;
          }
        }
        if (last != null) {
          planner.addEdge(rebuilt, last);
        }
        
//        // re-link
//        planner.removeEdge(max.get(0), merger);
        predecessor = max.get(max.size() - 1);
        predecessors = Sets.newHashSet(planner.configGraph().predecessors(predecessor));
        for (final QueryNodeConfig pred : predecessors) {
          planner.addEdge(pred, merger);
          planner.removeEdge(pred, predecessor);
        }
        
        // re-link
        planner.removeEdge(max.get(0), merger);
                
        // we can shuffle the graph
        for (int i = 0; i < new_sources.size(); i++) {
          final List<QueryNodeConfig> source_push_downs = push_downs.get(i);
          if (source_push_downs.isEmpty()) {
            // no push down, just link it in
            planner.addEdge(new_push_downs.get(
                max.get(0).getId()), new_sources.get(i).build());
            continue;
          }
          
          // We need to rename the sources for these nodes if they pull from the
          // source.
          final String pushdown_id = new_sources.get(i).id();
          List<QueryNodeConfig> renamed_pushdowns = 
              Lists.newArrayListWithExpectedSize(source_push_downs.size());
          for (final QueryNodeConfig pd : source_push_downs) {
            if (((DefaultQueryPlanner) planner).sinkFilters().containsKey(pd.getId())) {
              ((DefaultQueryPlanner) planner).sinkFilters().remove(pd.getId());
              ((DefaultQueryPlanner) planner).sinkFilters().put(merger.getId(), merger.getId());
            }
            if (pd.getSources().contains(config.getId())) {
              renamed_pushdowns.add(
                  pd.toBuilder()
                  .setSources(Lists.newArrayList(pushdown_id))
                  .build());
            } else {
              renamed_pushdowns.add(pd);
            }
          }
          new_sources.get(i).setPushDownNodes(renamed_pushdowns);
          
          final TimeSeriesDataSourceConfig new_source = new_sources.get(i).build();
          if (source_push_downs.size() == max_pushdowns) {
            planner.addEdge(rebuilt, new_source);
          } else {
            final QueryNodeConfig mx = new_push_downs.get(
                max.get(source_push_downs.size()).getId());
            planner.addEdge(mx, new_source);
          }
          
          if (context.query().isTraceEnabled()) {
            context.queryContext().logTrace("Adding pushdown source: " 
                + new_source.getSourceId());
          }
        }
        
        // don't fall through!
        setupTimeShiftMultiNode(rebuilt, planner, merger);
        return;
      }
    }

    // no push down, just replace
    MergerConfig merger = (MergerConfig) MergerConfig.newBuilder()
        .setAggregator(
            Strings.isNullOrEmpty(builder.mergeAggregator()) ? 
                tsdb.getConfig().getString(getConfigKey(AGGREGATOR_KEY)) : 
                builder.mergeAggregator())
        .addInterpolatorConfig(NumericInterpolatorConfig.newBuilder()
          .setFillPolicy(FillPolicy.NONE)
          .setRealFillPolicy(FillWithRealPolicy.NONE)
          .setDataType(NumericType.TYPE.toString())
          .build())
        .addSource(new_id)
        .setId(builder.id())
        .build();
    planner.replace(config, merger);
    
    QueryNodeConfig converter;
    if (needs_id_converter) {
      converter = ByteToStringIdConverterConfig.newBuilder()
          .setId(config.getId() + "_converter")
          .build();
      planner.addEdge(merger, converter);
    } else {
      converter = null;
    }
    
    final List<String> data_sources = 
        Lists.newArrayListWithExpectedSize(new_sources.size());
    for (final TimeSeriesDataSourceConfig.Builder source : new_sources) {
      data_sources.add(source.id());
    }
    
    HAClusterConfig rebuilt = (HAClusterConfig) builder
        .setDataSources(data_sources)
        .setId(new_id)
        .build();
    planner.addEdge(converter != null ? converter : merger, rebuilt);
    for (final TimeSeriesDataSourceConfig.Builder source : new_sources) {
      planner.addEdge(rebuilt, source.build());
      if (Graphs.hasCycle(planner.configGraph())) {
        throw new IllegalStateException("Cycle created when linking " 
            + rebuilt.getId() + " => " + source.id());
      }
      if (context.query().isTraceEnabled()) {
        context.queryContext().logTrace("Adding query source: " 
            + rebuilt.getSourceId());
      }
    }
    setupTimeShiftMultiNode(rebuilt, planner, merger);
  }

  @Override
  public QueryNode newNode(final QueryPipelineContext context) {
    throw new UnsupportedOperationException("A config is required.");
  }

  @Override
  public QueryNode newNode(final QueryPipelineContext context,
                           final QueryNodeConfig config) {
    return new HACluster(this, context, (HAClusterConfig) config);
  }

  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
    this.tsdb = tsdb;
    registerConfigs(tsdb);
    
    // validate defaults
    if (Strings.isNullOrEmpty(tsdb.getConfig().getString(
        getConfigKey(AGGREGATOR_KEY)))) {
      return Deferred.fromError(new IllegalArgumentException(
          "Default aggregator cannot be empty."));
    }
    
    try {
      DateTime.parseDuration(tsdb.getConfig().getString(getConfigKey(PRIMARY_KEY)));
    } catch (Exception e) {
      return Deferred.fromError(new IllegalArgumentException(
          "Failed to parse default primary timeout: " 
              + tsdb.getConfig().getString(getConfigKey(PRIMARY_KEY)), e));
    }
    
    try {
      DateTime.parseDuration(tsdb.getConfig().getString(getConfigKey(SECONDARY_KEY)));
    } catch (Exception e) {
      return Deferred.fromError(new IllegalArgumentException(
          "Failed to parse default secondary timeout: " 
              + tsdb.getConfig().getString(getConfigKey(SECONDARY_KEY)), e));
    }
    
    return Deferred.fromResult(null);
  }

  @Override
  public String type() {
    return TYPE;
  }
  
  @Override
  public TypeToken<? extends TimeSeriesId> idType() {
    // TODO - need to make sure downstream returns identical types.
    return Const.TS_STRING_ID;
  }

  @Override
  public boolean supportsPushdown(
      final Class<? extends QueryNodeConfig> operation) {
    // TODO - need to compute a join on source operations.
    return false;
  }

  @Override
  public Deferred<TimeSeriesStringId> resolveByteId(
      final TimeSeriesByteId id,
      final Span span) {
    // TODO - need to make sure downstream returns identical types.
    throw new UnsupportedOperationException();
  }

  @Override
  public Deferred<List<byte[]>> encodeJoinKeys(
      final List<String> join_keys,
      final Span span) {
    // TODO - need to make sure downstream returns identical types.
    throw new UnsupportedOperationException();
  }

  @Override
  public Deferred<List<byte[]>> encodeJoinMetrics(
      final List<String> join_metrics,
      final Span span) {
    // TODO - need to make sure downstream returns identical types.
    throw new UnsupportedOperationException();
  }
  
  /**
   * Helper to build the config key with a factory id.
   * @param suffix The non-null and non-empty config suffix.
   * @return The key containing the id.
   */
  @VisibleForTesting
  String getConfigKey(final String suffix) {
    if (id == null || id == TYPE) { // yes, same addy here.
      return KEY_PREFIX + suffix;
    } else {
      return KEY_PREFIX + id + "." + suffix;
    }
  }
  
  /** A callback for the sources that updates default_sources. */
  class SettingsCallback implements ConfigurationCallback<Object> {

    @Override
    public void update(final String key, final Object value) {
      if (key.equals(getConfigKey(SOURCES_KEY))) {
        if (value == null) {
          return;
        }
        
        final String[] sources = ((String) value).split(",");
        synchronized (default_sources) {
          default_sources.clear();
          for (String source : sources) {
            source = source.trim();
            default_sources.add(source);
          }
        }
      }
    }
    
  }
  
  /**
   * Helper to register the configs.
   * @param tsdb A non-null TSDB.
   */
  @VisibleForTesting
  void registerConfigs(final TSDB tsdb) {
    if (!tsdb.getConfig().hasProperty(getConfigKey(SOURCES_KEY))) {
      tsdb.getConfig().register(getConfigKey(SOURCES_KEY), null, true,
          "A comma separated list of one or more data sources to query "
          + "with the primary source first in the list.");
    }
    if (!tsdb.getConfig().hasProperty(getConfigKey(AGGREGATOR_KEY))) {
      tsdb.getConfig().register(getConfigKey(AGGREGATOR_KEY), "max", true,
          "The default aggregator to use when merging sources.");
    }
    if (!tsdb.getConfig().hasProperty(getConfigKey(PRIMARY_KEY))) {
      tsdb.getConfig().register(getConfigKey(PRIMARY_KEY), "10s", true,
          "A duration defining how long to wait for the primary data when "
          + "a secondary source responds first.");
    }
    if (!tsdb.getConfig().hasProperty(getConfigKey(SECONDARY_KEY))) {
      tsdb.getConfig().register(getConfigKey(SECONDARY_KEY), "5s", true,
          "A duration defining how long to wait for a secondary source "
              + "the primary source responds first.");
    }
    
    tsdb.getConfig().bind(getConfigKey(SOURCES_KEY), new SettingsCallback());
  }

  /**
   * Recursive helper to determine what nodes can be pushed down.
   * @param current The current node to examine.
   * @param factory The source factory.
   * @param push_downs The list of push downs to populate in order.
   * @param planner The planner.
   */
  private void canPushDown(final QueryNodeConfig current,
                           final TimeSeriesDataSourceFactory factory,
                           final List<QueryNodeConfig> push_downs,
                           final QueryPlanner planner) {
    if (factory.supportsPushdown(current.getClass()) &&
        current.pushDown()) {
      push_downs.add(current);
      final Set<QueryNodeConfig> predecessors = 
          planner.configGraph().predecessors(current);
      if (predecessors.isEmpty() || predecessors.size() > 1) {
        return;
      }
      
      canPushDown(predecessors.iterator().next(), factory, push_downs, planner);
    }
  }
  
  /**
   * Clones the config and creates time offset nodes.
   * @param config The non-null original config.
   * @param planner The non-null planner.
   */
  private void setupTimeShiftSingleNode(final TimeSeriesDataSourceConfig config, 
                                        final QueryPlanner planner) {
    if (config.timeShifts() == null || config.timeShifts().isEmpty()) {
      return;
    }
    
    // since we're cloning, purge the original
    TimeSeriesDataSourceConfig shiftless = ((TimeSeriesDataSourceConfig.Builder) 
        config.toBuilder())
        .setTimeShiftInterval(null)
        .setPreviousIntervals(0)
        .setNextIntervals(0)
        .build();
    planner.replace(config, shiftless);
    
    if (config.timeShifts().containsKey(config.getId())) {
      // child who has already been initialized.
      return;
    }
    
    final Set<QueryNodeConfig> predecessors = planner.configGraph().predecessors(config);
    final TimeShiftConfig shift_config = (TimeShiftConfig) TimeShiftConfig.newBuilder()
        .setConfig((TimeSeriesDataSourceConfig) config)
        .setId(config.getId() + "-time-shift")
        .build();
    if (planner.configGraph().nodes().contains(shift_config)) {
      return;
    }
    
    for (final QueryNodeConfig predecessor : predecessors) {
      planner.addEdge(predecessor, shift_config);
    }
    
    for (final String shift_id : config.timeShifts().keySet()) {
      final Map<String, Pair<Boolean, TemporalAmount>> amounts = 
          Maps.newHashMapWithExpectedSize(1);
      amounts.put(shift_id, config.timeShifts().get(shift_id));
      QueryNodeConfig rebuilt = ((TimeSeriesDataSourceConfig.Builder)
          config.toBuilder())
          .setTimeShifts(amounts)
          .setId(shift_id)
          .build();
      planner.addEdge(shift_config, rebuilt);
    }
  }
  
  /**
   * Handles cloning a sub-graph of HA node configs (group bys, downsamples etc)
   * for each time shift offset that we have to query.
   * @param config The non-null parent config.
   * @param planner The planner.
   * @param merger The merger.
   */
  private void setupTimeShiftMultiNode(final TimeSeriesDataSourceConfig config, 
                                       final QueryPlanner planner,
                                       final MergerConfig merger) {
    if (config.timeShifts() == null || config.timeShifts().isEmpty()) {
      return;
    }
    
    // since we're cloning, purge the original
    TimeSeriesDataSourceConfig shiftless = ((TimeSeriesDataSourceConfig.Builder) 
        config.toBuilder())
        .setTimeShiftInterval(null)
        .setPreviousIntervals(0)
        .setNextIntervals(0)
        .build();
    planner.replace(config, shiftless);
    
    final Set<QueryNodeConfig> shift_predecessors = 
        planner.configGraph().predecessors(merger);
    final TimeShiftConfig shift_config = (TimeShiftConfig) TimeShiftConfig.newBuilder()
        .setConfig((TimeSeriesDataSourceConfig) config)
        .setId(merger.getId() + "-time-shift")
        .build();
    for (final QueryNodeConfig predecessor : shift_predecessors) {
      planner.addEdge(predecessor, shift_config);
    }
    
    // now for each time shift we have to duplicate the sub-graph from the 
    // merger to the destinations. *sigh*.
    // TODO - make this cleaner some day. This is SUPER ugly. For now we do it
    // so that we have the proper timeouts and distribute the query load.
    for (final String shift_id : config.timeShifts().keySet()) {
      final MergerConfig merger_shift = (MergerConfig) merger.toBuilder()
          .setId(shift_id)
          .build();
      planner.addEdge(shift_config, merger_shift);
      
      final Set<QueryNodeConfig> successors = 
          Sets.newHashSet(planner.configGraph().successors(merger));
      for (final QueryNodeConfig successor : successors) {
        recursiveTimeShift(planner, 
                           merger_shift, 
                           merger_shift, 
                           successor, 
                           config.timeShifts(), 
                           shift_id);
      }
    }
  }
  
  /**
   * Recursive walker for sub-graphs to create time shift offsets.
   * @param planner The non-null planner.
   * @param parent the NEW time shifted parent to link to.
   * @param config The old config to clone.
   * @param shifts The shifts to pass down.
   * @param shift_id The shift ID to append to IDs.
   */
  private void recursiveTimeShift(final QueryPlanner planner,
                                  final QueryNodeConfig parent,
                                  final QueryNodeConfig new_parent,
                                  final QueryNodeConfig config, 
                                  final Map<String, Pair<Boolean, TemporalAmount>> shifts,
                                  final String shift_id) {
    final QueryNodeConfig shift;
    if (config instanceof TimeSeriesDataSourceConfig) {
      // for the shift to happen properly we need to rename the shift node and
      // send that to the query target.
      final Map<String, Pair<Boolean, TemporalAmount>> amounts = 
          Maps.newHashMapWithExpectedSize(1);
      amounts.put(config.getId() + "-" + shift_id, shifts.get(shift_id));
      shift = ((TimeSeriesDataSourceConfig.Builder) config.toBuilder())
          .setTimeShifts(amounts)
          .setId(config.getId() + "-" + shift_id)
          .build();
    } else {
      shift = config.toBuilder().setId(config.getId() + "-" + shift_id)
          .build();
    }
    
    planner.addEdge(new_parent, shift);
    final Set<QueryNodeConfig> successors = 
        Sets.newHashSet(planner.configGraph().successors(config));
    for (final QueryNodeConfig successor : successors) {
      recursiveTimeShift(planner, config, shift, successor, shifts, shift_id);
    }
  }
  
}
