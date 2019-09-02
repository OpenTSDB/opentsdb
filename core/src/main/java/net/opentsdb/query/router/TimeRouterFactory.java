// This file is part of OpenTSDB.
// Copyright (C) 2017-2019  The OpenTSDB Authors.
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
package net.opentsdb.query.router;

import java.util.List;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.common.Const;
import net.opentsdb.configuration.ConfigurationCallback;
import net.opentsdb.configuration.ConfigurationEntrySchema;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.data.TimeSeriesDataSourceFactory;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.query.DefaultTimeSeriesDataSourceConfig;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.plan.QueryPlanner;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.processor.merge.MergerConfig;
import net.opentsdb.query.router.TimeRouterConfigEntry.MatchType;
import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.stats.Span;

/**
 * A node that manages slicing and routing queries across multiple sources
 * over configurable time ranges. For example, if the time series storage 
 * system has a 24h in-memory cache (e.g. Facebook's Beringei) and 
 * there's a long-term storage layer, the router will send querys for 
 * the current 24h of data to the cache and longer and/or older queries 
 * to the long-term store.
 * <p> 
 * Additionally the long-term layer can be sharded across tables or data 
 * stores. For example, if a TSD runs out of UIDs and users need more, 
 * they can create a new set of tables with a larger UID size. The 
 * router will then split queries at the appropriate date and merge the 
 * results.
 * 
 * @since 3.0
 */
public class TimeRouterFactory extends BaseTSDBPlugin implements 
    TimeSeriesDataSourceFactory<TimeSeriesDataSourceConfig, TimeSeriesDataSource> {
  
  public static final String TYPE = "TimeNamespaceRouter";

  public static final String KEY_PREFIX = "tsd.query.tnrouter.";
  public static final String CONFIG_KEY = "config";
  
  /** Type for parsing the config. */
  public static final TypeReference<List<TimeRouterConfigEntry>> TYPE_REF = 
      new TypeReference<List<TimeRouterConfigEntry>>() { };
  
  /** The parsed config. */
  protected volatile List<TimeRouterConfigEntry> config;
  
  @Override
  public TimeSeriesDataSourceConfig parseConfig(final ObjectMapper mapper,
                                     final TSDB tsdb,
                                     final JsonNode node) {
    return DefaultTimeSeriesDataSourceConfig.parseConfig(mapper, tsdb, node);
  }

  @Override
  public boolean supportsQuery(final QueryPipelineContext context, 
                               final TimeSeriesDataSourceConfig config) {
    return true;
  }
  
  @Override
  public void setupGraph(final QueryPipelineContext context, 
                         final TimeSeriesDataSourceConfig config,
                         final QueryPlanner planner) {
    final List<TimeRouterConfigEntry> config_ref = this.config;
    List<TimeRouterConfigEntry> sources = Lists.newArrayList();
    for (final TimeRouterConfigEntry entry : config_ref) {
      final MatchType match = entry.match(context, config, tsdb);
      if (match == MatchType.NONE) {
        continue;
      } else if (match == MatchType.FULL) {
        sources.add(entry);
        break;
      } else {
        // add partials
        sources.add(entry);
      }
    }
    if (context.query().isDebugEnabled()) {
      StringBuilder buf = new StringBuilder();
      for (int i = 0; i < sources.size(); i++) {
        if (i >= 0) {
          buf.append(", ");
        }
        buf.append(sources.get(i).getSourceId());
      }
      context.queryContext().logTrace("Potential data sources: " + buf.toString());
    }
    
    if (sources.isEmpty()) {
      // TODO what do I do? Throw exception?
      throw new QueryExecutionException("No data source found for "
          + "config: " + config, 0);
    }
    
    // TODO - when we do time offsets we can add timestamps for each
    // data source. In the mean time we just pass the same query to each.
    if (sources.size() == 1) {

      TimeSeriesDataSourceConfig.Builder builder = 
          (TimeSeriesDataSourceConfig.Builder) config.toBuilder();
      builder.setSourceId(sources.get(0).getSourceId());
      TimeSeriesDataSourceConfig rebuilt = (TimeSeriesDataSourceConfig) builder.build();
      planner.replace(config, rebuilt);
      if (context.query().isTraceEnabled()) {
        context.queryContext().logTrace("Only one route available: " 
            + rebuilt.getSourceId());
      }
      return;
    }
    
    MergerConfig merger = (MergerConfig) MergerConfig.newBuilder()
        // TODO - want to make this configurable.
        .setAggregator("max")
        .addInterpolatorConfig(NumericInterpolatorConfig.newBuilder()
          .setFillPolicy(FillPolicy.NONE)
          .setRealFillPolicy(FillWithRealPolicy.NONE)
          .setDataType(NumericType.TYPE.toString())
          .build())
        .setDataSource(config.getId())
        .setId(config.getId())
        .build();
    planner.replace(config, merger);
    for (final TimeRouterConfigEntry entry : sources) {
      final TimeSeriesDataSourceConfig rebuilt = (TimeSeriesDataSourceConfig)
          ((TimeSeriesDataSourceConfig.Builder) config.toBuilder())
          .setSourceId(entry.getSourceId())
          .setId(config.getId() + "_" + entry.getSourceId())
          .build();
      planner.addEdge(merger, rebuilt);
      if (context.query().isTraceEnabled()) {
        context.queryContext().logTrace("Adding router source: " 
            + rebuilt.getSourceId());
      }
    }
  }

  @Override
  public TimeSeriesDataSource newNode(QueryPipelineContext context) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TimeSeriesDataSource newNode(final QueryPipelineContext context,
                           final TimeSeriesDataSourceConfig config) {
    throw new UnsupportedOperationException("This node should have been "
        + "removed from the graph.");
  }

  @Override
  public TypeToken<? extends TimeSeriesId> idType() {
    // TODO - need to make sure downstream returns identical types.
    return Const.TS_STRING_ID;
  }

  @Override
  public boolean supportsPushdown(
      final Class<? extends QueryNodeConfig> operation) {
    // TODO figure out what to do here.
    return false;
  }

  @Override
  public Deferred<TimeSeriesStringId> resolveByteId(
      final TimeSeriesByteId id,
      final Span span) {
    return Deferred.fromError(new UnsupportedOperationException(
        "This node should have been removed from the graph."));
  }

  @Override
  public Deferred<List<byte[]>> encodeJoinKeys(
      final List<String> join_keys,
      final Span span) {
    return Deferred.fromError(new UnsupportedOperationException(
        "This node should have been removed from the graph."));
  }

  @Override
  public Deferred<List<byte[]>> encodeJoinMetrics(
      final List<String> join_metrics,
      final Span span) {
    return Deferred.fromError(new UnsupportedOperationException(
        "This node should have been removed from the graph."));
  }

  @Override
  public RollupConfig rollupConfig() {
    return null;
  }
  
  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
    this.tsdb = tsdb;
    registerConfigs(tsdb);
    // TODO - validate config.
    return Deferred.fromResult(null);
  }

  @Override
  public String type() {
    return TYPE;
  }

  /** A callback for the sources that updates default_sources. */
  class SettingsCallback implements ConfigurationCallback<Object> {

    @SuppressWarnings("unchecked")
    @Override
    public void update(final String key, final Object value) {
      if (key.equals(getConfigKey(CONFIG_KEY))) {
        if (value == null) {
          return;
        }
        
        synchronized (TimeRouterFactory.this) {
          config = (List<TimeRouterConfigEntry>) value;
        }
      }
    }
    
  }
  
  @VisibleForTesting
  void registerConfigs(final TSDB tsdb) {
    if (!tsdb.getConfig().hasProperty(getConfigKey(CONFIG_KEY))) {
      tsdb.getConfig().register(
          ConfigurationEntrySchema.newBuilder()
          .setKey(getConfigKey(CONFIG_KEY))
          .setType(TYPE_REF)
          .setDescription("The JSON or YAML config for the router.")
          .isDynamic()
          .isNullable()
          .setSource(getClass().getName())
          .build());
    }
    tsdb.getConfig().bind(getConfigKey(CONFIG_KEY), new SettingsCallback());
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
  
}
