// This file is part of OpenTSDB.
// Copyright (C) 2017-2021  The OpenTSDB Authors.
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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
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
import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.query.DefaultQueryResultId;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResultId;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.plan.DefaultQueryPlanner;
import net.opentsdb.query.plan.QueryPlanner;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.processor.BaseQueryNodeFactory;
import net.opentsdb.query.processor.merge.MergerConfig;
import net.opentsdb.query.router.RoutingUtils;
import net.opentsdb.query.router.TimeRouterConfigEntry;
import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.stats.Span;
import net.opentsdb.utils.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

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
 * Note that the first entry in the {@link #sources} list is
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
public class HAClusterFactory extends BaseQueryNodeFactory<
    TimeSeriesDataSourceConfig, HACluster> implements
      TimeSeriesDataSourceFactory<TimeSeriesDataSourceConfig, HACluster> {

  private static final Logger LOG =
          LoggerFactory.getLogger(HAClusterFactory.class);
  public static final String TYPE = "HACluster";

  public static final String KEY_PREFIX = "tsd.query.";
  public static final String SOURCES_KEY = "hacluster.sources";
  public static final String AGGREGATOR_KEY = "hacluster.default.aggregator";
  public static final String PRIMARY_KEY = "hacluster.default.timeout.primary";
  public static final String SECONDARY_KEY = "hacluster.default.timeout.secondary";

  /** The default sources updated on config callback. */
  protected volatile List<TimeRouterConfigEntry> sources;

  @Override
  public HAClusterConfig parseConfig(final ObjectMapper mapper,
                                     final TSDB tsdb,
                                     final JsonNode node) {
    return HAClusterConfig.parse(mapper, tsdb, node);
  }

  @Override
  public boolean supportsQuery(final QueryPipelineContext context, 
                               final TimeSeriesDataSourceConfig config) {
    // shadow snapshot
    List<TimeRouterConfigEntry> sources = this.sources;
    for (int i = 0; i < sources.size(); i++) {
      if (!sources.get(i).factory().supportsQuery(context, config)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void setupGraph(final QueryPipelineContext context, 
                         final TimeSeriesDataSourceConfig config,
                         final QueryPlanner planner) {
    if (((TimeSeriesDataSourceConfig) config).hasBeenSetup()) {
      return;
    }

    // shadow to snapshot the state while we work.
    List<TimeRouterConfigEntry> sources = this.sources;

    // if we have a cluster config, we need to honor overrides. This is useful
    // in the case we're querying 3 or more clusters at once and we want to test
    // an override with N-1 or fewer sources.
    if (config instanceof HAClusterConfig) {
      final List<String> dataSources = ((HAClusterConfig) config).getDataSources();
      if (dataSources != null && !dataSources.isEmpty()) {
        final List<TimeRouterConfigEntry> newSources =
                Lists.newArrayListWithExpectedSize(dataSources.size());
        for (TimeRouterConfigEntry entry : sources) {
          if (dataSources.contains(entry.getSourceId())) {
            newSources.add(entry);
          }
        }
        sources = newSources;
      }
    }

    List<TimeRouterConfigEntry> supportedSources = null;
    for (int i = 0; i < sources.size(); i++) {
      final TimeRouterConfigEntry entry = sources.get(i);
      if (!entry.factory().supportsQuery(context, config)) {
        if (supportedSources == null) {
          supportedSources = Lists.newArrayList(sources);
        }
        supportedSources.remove(entry);
      }
    }
    if (supportedSources != null) {
      sources = supportedSources;
    }

    if (sources.isEmpty()) {
      if (config instanceof HAClusterConfig &&
              ((HAClusterConfig) config).getDataSources() != null) {
        final List<String> dataSources = ((HAClusterConfig) config).getDataSources();
        if (!dataSources.isEmpty()) {
          throw new QueryExecutionException("No sources configured for " + id +
                  " with HA overrides " + dataSources, 500);
        }
      }
      throw new QueryExecutionException("No sources configured for " + id, 400);
    }

    if (sources.size() == 1) {
      TimeSeriesDataSourceConfig.Builder builder = ((TimeSeriesDataSourceConfig.Builder) config.toBuilder())
              .setSourceId(sources.get(0).getSourceId());
      if (config.getPushDownNodes() != null && !config.getPushDownNodes().isEmpty()) {
        cloneAndSetPushdowns(builder, config.getPushDownNodes());
      }
      planner.replace(config, builder.build());
      if (context.query().isTraceEnabled()) {
        context.queryContext().logTrace("Only one route available: "
                + sources.get(0));
      }
      return;
    }

    final List<List<QueryNodeConfig>> pushdowns =
            RoutingUtils.potentialPushDowns(config, sources, planner);

    List<TimeSeriesDataSourceConfig.Builder> newSources =
            Lists.newArrayListWithExpectedSize(sources.size());
    final List<String> sortedSourceIds = Lists.newArrayListWithCapacity(newSources.size());
    final List<String> timeouts = Lists.newArrayListWithCapacity(newSources.size());
    boolean needsIdConverter = false;
    for (int i = 0; i < sources.size(); i++) {
      final TimeRouterConfigEntry src = sources.get(i);
      final TimeSeriesDataSourceConfig.Builder localBuilder =
              DefaultQueryPlanner.setTimeOverrides(
                      context,
                      config,
                      planner.getAdjustments(config)
              );

      final String srcId = config.getId() + "_" + src.getSourceId();
      final QueryResultId id = new DefaultQueryResultId(
              "ha_" + srcId, srcId);
      sortedSourceIds.add(srcId);
      timeouts.add(src.getTimeout());
      localBuilder.setSourceId(src.getSourceId())
              .setDataSource(srcId)
              .setHasBeenSetup(true)
              //.setId("ha_" + srcId)
              .setId(srcId)
              .setResultIds(Lists.newArrayList(id));
      if (config.getPushDownNodes() != null &&
          !config.getPushDownNodes().isEmpty()) {
        cloneAndSetPushdowns(localBuilder, config.getPushDownNodes());
      } else if (pushdowns != null && pushdowns.get(i) != null) {
        localBuilder.setPushDownNodes(pushdowns.get(i));
      }
      if (sources.get(i).factory().idType() != Const.TS_STRING_ID) {
        needsIdConverter = true;
      }
      newSources.add(localBuilder);
    }

    MergerConfig.Builder mergerBuilder = (MergerConfig.Builder) MergerConfig.newBuilder()
            // TODO - want to make this configurable. Particularly when we're
            // averaging or min-ing, etc.
            .setAggregator("max")
            .setMode(MergerConfig.MergeMode.HA)
            .setTimeouts(timeouts)
            .setSortedDataSources(sortedSourceIds)
            .addInterpolatorConfig(NumericInterpolatorConfig.newBuilder()
                    .setFillPolicy(FillPolicy.NONE)
                    .setRealFillPolicy(FillWithRealPolicy.NONE)
                    .setDataType(NumericType.TYPE.toString())
                    .build())
            .setDataSource(config.getId())
            .setId(config.getId());

    MergerConfig merger = mergerBuilder.build();
    planner.replace(config, merger);

    RoutingUtils.rebuildGraph(context,
            merger,
            needsIdConverter,
            pushdowns,
            newSources,
            planner);
  }

  @Override
  public HACluster newNode(final QueryPipelineContext context) {
    throw new UnsupportedOperationException("This node config should have been removed.");
  }

  @Override
  public HACluster newNode(final QueryPipelineContext context,
                           final TimeSeriesDataSourceConfig config) {
    throw new UnsupportedOperationException("This node config should have been removed.");
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
    for (int i = 0; i < sources.size(); i++) {
      if (!sources.get(i).factory().supportsPushdown(operation)) {
        return false;
      }
    }
    return true;
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

  @Override
  public RollupConfig rollupConfig() {
    // TODO - need to make sure downstream returns identical configs.
    for (int i = 0; i < sources.size(); i++) {
      if (sources.get(i).factory().rollupConfig() != null) {
        return sources.get(i).factory().rollupConfig();
      }
    }
    return null;
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
        final List<TimeRouterConfigEntry> newSources = Lists.newArrayListWithExpectedSize(sources.length);
        for (int i = 0; i < sources.length; i++) {

          final TimeSeriesDataSourceFactory factory = HAClusterFactory.this.tsdb.getRegistry().getPlugin(
                  TimeSeriesDataSourceFactory.class, sources[i]);
          if (factory == null) {
            LOG.error("No factory found for HA source " + sources[i] +
                    ". Dropping it from the config.");
            continue;
          }

          final String timeout = i == 0 ?
            tsdb.getConfig().getString(getConfigKey(PRIMARY_KEY)) :
            tsdb.getConfig().getString(getConfigKey(SECONDARY_KEY));

          TimeRouterConfigEntry entry = TimeRouterConfigEntry.newBuilder()
                  .setSourceId(sources[i])
                  .setFactory(factory)
                  .setTimeout(timeout)
                  .build();
          newSources.add(entry);
        }

        HAClusterFactory.this.sources = newSources;
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

  void cloneAndSetPushdowns(final TimeSeriesDataSourceConfig.Builder builder,
                            final List<QueryNodeConfig> pushdowns) {
    final List<QueryNodeConfig> clones = Lists.newArrayListWithExpectedSize(pushdowns.size());
    for (int i = 0; i < pushdowns.size(); i++) {
      clones.add(pushdowns.get(i).toBuilder().build());
    }
    builder.setPushDownNodes(clones);
  }
}
