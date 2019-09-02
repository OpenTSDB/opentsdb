// This file is part of OpenTSDB.
// Copyright (C) 2018-2019  The OpenTSDB Authors.
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
package net.opentsdb.query.execution;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import java.util.ArrayList;
import java.util.List;
import net.opentsdb.common.Const;
import net.opentsdb.configuration.ConfigurationEntrySchema;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.query.BaseTimeSeriesDataSourceConfig;
import net.opentsdb.query.DefaultTimeSeriesDataSourceConfig;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.plan.QueryPlanner;
import net.opentsdb.query.processor.timeshift.TimeShiftConfig;
import net.opentsdb.rollup.DefaultRollupConfig;
import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.stats.Span;

public class HttpQueryV3Factory
    extends BaseHttpExecutorFactory<
        TimeSeriesDataSourceConfig<
            ? extends TimeSeriesDataSourceConfig.Builder, ? extends TimeSeriesDataSourceConfig>,
        HttpQueryV3Source> {

  public static final String TYPE = "HttpQueryV3";
  public static final String ROLLUP_KEY = "rollups.config";
  
  /** The path to hit. */
  protected String endpoint;

  /** The rollup config. */
  protected DefaultRollupConfig rollup_config;
  
  @Override
  public TypeToken<? extends TimeSeriesId> idType() {
    return Const.TS_STRING_ID;
  }

  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    return super.initialize(tsdb, id).addCallback(
        new Callback<Object, Object>() {
          @Override
          public Object call(final Object ignored) throws Exception {
            registerLocalConfigs(tsdb);
            rollup_config = tsdb.getConfig().getTyped(getConfigKey(ROLLUP_KEY), 
                DefaultRollupConfig.class);
            endpoint = tsdb.getConfig().getString(getConfigKey(ENDPOINT_KEY));
            return null;
          }
    });
  }

  @Override
  public boolean supportsQuery(
      final QueryPipelineContext context,
      final TimeSeriesDataSourceConfig<
              ? extends TimeSeriesDataSourceConfig.Builder,
              ? extends TimeSeriesDataSourceConfig>
          config) {
    // TODO - find a way if it's in sync
    return true;
  }

  @Override
  public boolean supportsPushdown(
      final Class<? extends QueryNodeConfig>
          operation) {
    // TODO - find a way if it's in sync
    return true;
  }

  @Override
  public Deferred<TimeSeriesStringId> resolveByteId(final TimeSeriesByteId id,
                                                    final Span span) {
    return Deferred.fromError(new UnsupportedOperationException());
  }

  @Override
  public Deferred<List<byte[]>> encodeJoinKeys(final List<String> join_keys,
                                               final Span span) {
    return Deferred.fromError(new UnsupportedOperationException());
  }

  @Override
  public Deferred<List<byte[]>> encodeJoinMetrics(final List<String> join_metrics,
                                                  final Span span) {
    return Deferred.fromError(new UnsupportedOperationException());
  }

  @Override
  public String type() {
    return TYPE;
  }

  @Override
  public TimeSeriesDataSourceConfig<
          ? extends TimeSeriesDataSourceConfig.Builder,
          ? extends TimeSeriesDataSourceConfig>
      parseConfig(final ObjectMapper mapper, final TSDB tsdb, final JsonNode node) {
    return DefaultTimeSeriesDataSourceConfig.parseConfig(mapper, tsdb, node);
  }

  @Override
  public void setupGraph(
      final QueryPipelineContext context,
      final TimeSeriesDataSourceConfig<
              ? extends TimeSeriesDataSourceConfig.Builder,
              ? extends TimeSeriesDataSourceConfig>
          config,
      final QueryPlanner planner) {
    if (config.timeShifts() == null) {
      return;
    }
    
    TimeSeriesDataSourceConfig.Builder<
            ? extends TimeSeriesDataSourceConfig.Builder,
            ? extends TimeSeriesDataSourceConfig>
        builder = config.toBuilder();

    TimeSeriesDataSourceConfig new_config =
        (TimeSeriesDataSourceConfig) builder
        .setTimeShifts(config.timeShifts())
        .setTimeShiftInterval(config.getTimeShiftInterval())
        .build();

    planner.replace(config, new_config);
    
    // Add timeshift node as a push down
    final TimeShiftConfig shift_config = TimeShiftConfig.newBuilder()
        .setTimeshiftInterval(new_config.getTimeShiftInterval())
        .setId(new_config.getId() + "-timeShift")
        .addSource(new_config.getId())
        .build();

    // change the source of the predecessor to timeshift instead of original
    List<QueryNodeConfig> pushDownNodes = new_config.getPushDownNodes();
    for (QueryNodeConfig pushdown : pushDownNodes) {
      if (pushdown.getSources().contains(new_config.getId())) {
        pushdown.getSources().remove(new_config.getId());
        pushdown.getSources().add(shift_config.getId());
      }
    }

    if (pushDownNodes.size() == 0) { // err. TODO: find a better way?
      List<QueryNodeConfig> pushdown = new ArrayList<>();
      pushdown.add(shift_config);

      final BaseTimeSeriesDataSourceConfig.Builder b = 
          (BaseTimeSeriesDataSourceConfig.Builder) new_config.toBuilder();
      ((BaseTimeSeriesDataSourceConfig)new_config)
        .cloneBuilder((BaseTimeSeriesDataSourceConfig) new_config, b);
      b.setPushDownNodes(pushdown);
    } else {
      pushDownNodes.add(shift_config);
    }
  }

  @Override
  public HttpQueryV3Source newNode(final QueryPipelineContext context) {
    throw new UnsupportedOperationException("Node config required.");
  }

  @Override
  public HttpQueryV3Source newNode(final QueryPipelineContext context,
                           final TimeSeriesDataSourceConfig config) {
    return new HttpQueryV3Source(this, 
                                 context, 
                                 config,
                                 client, 
                                 nextHost(),
                                 endpoint);
  }

  @Override
  public RollupConfig rollupConfig() {
    return rollup_config;
  }
  
  void registerLocalConfigs(final TSDB tsdb) {
    if (!tsdb.getConfig().hasProperty(getConfigKey(ENDPOINT_KEY))) {
      tsdb.getConfig().register(getConfigKey(ENDPOINT_KEY), "/api/query/graph", true,
          "The endpoint to send queries to.");
    }
    // TEMP This is ugly. We should fetch the rollup config from the source.
    if (!tsdb.getConfig().hasProperty(getConfigKey(ROLLUP_KEY))) {
      tsdb.getConfig().register(
          ConfigurationEntrySchema.newBuilder()
          .setKey(getConfigKey(ROLLUP_KEY))
          .setType(DefaultRollupConfig.class)
          .setDescription("The JSON or YAML config with a mapping of "
              + "aggregations to numeric IDs.")
          .isNullable()
          .setSource(getClass().getName())
          .build());
    }
  }
  
}
