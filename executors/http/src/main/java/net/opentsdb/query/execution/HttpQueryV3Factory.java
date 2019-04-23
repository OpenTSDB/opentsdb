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
package net.opentsdb.query.execution;

import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.common.Const;
import net.opentsdb.configuration.ConfigurationEntrySchema;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.query.DefaultTimeSeriesDataSourceConfig;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.TimeSeriesQuery;
import net.opentsdb.query.plan.QueryPlanner;
import net.opentsdb.rollup.DefaultRollupConfig;
import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.stats.Span;

public class HttpQueryV3Factory extends BaseHttpExecutorFactory {
  
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
  public boolean supportsQuery(final TimeSeriesQuery query,
                               final TimeSeriesDataSourceConfig config) {
    // TODO - find a way if it's in sync
    return true;
  }

  @Override
  public boolean supportsPushdown(final Class<? extends QueryNodeConfig> operation) {
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
  public QueryNodeConfig parseConfig(final ObjectMapper mapper, 
                                     final TSDB tsdb,
                                     final JsonNode node) {
    return DefaultTimeSeriesDataSourceConfig.parseConfig(mapper, tsdb, node);
  }

  @Override
  public void setupGraph(final QueryPipelineContext context, 
                         final QueryNodeConfig config,
                         final QueryPlanner planner) {
    // No-op
  }

  @Override
  public QueryNode newNode(final QueryPipelineContext context) {
    throw new UnsupportedOperationException("Node config required.");
  }

  @Override
  public QueryNode newNode(final QueryPipelineContext context,
                           final QueryNodeConfig config) {
    return new HttpQueryV3Source(this, 
                                 context, 
                                 (TimeSeriesDataSourceConfig) config, 
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
