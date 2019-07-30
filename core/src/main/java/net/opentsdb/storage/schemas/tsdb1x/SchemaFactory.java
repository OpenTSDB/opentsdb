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
package net.opentsdb.storage.schemas.tsdb1x;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;
import net.opentsdb.common.Const;
import net.opentsdb.configuration.ConfigurationEntrySchema;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesDataSourceFactory;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.query.DefaultTimeSeriesDataSourceConfig;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.plan.QueryPlanner;
import net.opentsdb.query.processor.timeshift.TimeShiftConfig;
import net.opentsdb.rollup.DefaultRollupConfig;
import net.opentsdb.rollup.NoSuchRollupForIntervalException;
import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.stats.Span;
import net.opentsdb.storage.WritableTimeSeriesDataStore;
import net.opentsdb.storage.WritableTimeSeriesDataStoreFactory;
import net.opentsdb.uid.UniqueIdType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Simple singleton factory that implements a default and named schemas
 * (for different configurations).
 * 
 * @since 3.0
 */
public class SchemaFactory extends BaseTSDBPlugin 
                           implements TimeSeriesDataSourceFactory<TimeSeriesDataSourceConfig, Tsdb1xQueryNode>,
                                      WritableTimeSeriesDataStoreFactory {
  private static final Logger LOG = LoggerFactory.getLogger(SchemaFactory.class);

  public static final String TYPE = "Tsdb1xSchemaFactory";
  
  public static final String KEY_PREFIX = "tsd.storage.";
  public static final String ROLLUP_ENABLED_KEY = "rollups.enable";
  public static final String ROLLUP_KEY = "rollups.config";
  
  /** The default schema. */
  protected Schema schema;
  
  /** The rollup config. */
  protected DefaultRollupConfig rollup_config;
  
  @Override
  public WritableTimeSeriesDataStore newStoreInstance(final TSDB tsdb, 
                                                      final String id) {
    return schema;
  }
  
  @Override
  public TypeToken<? extends TimeSeriesId> idType() {
    return Const.TS_BYTE_ID;
  }
  
  @Override
  public boolean supportsQuery(final QueryPipelineContext context, 
                               final TimeSeriesDataSourceConfig config) {
    // TODO - let the underlying store handle this.
    return true;
  }
  
  @Override
  public boolean supportsPushdown(
      final Class<? extends QueryNodeConfig> function) {
    // TODO Auto-generated method stub
    return false;
  }
  
  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
    registerConfigs(tsdb);
    if (tsdb.getConfig().getBoolean(getConfigKey(ROLLUP_ENABLED_KEY))) {
      rollup_config = tsdb.getConfig().getTyped(getConfigKey(ROLLUP_KEY), 
          DefaultRollupConfig.class);
    }
    
    schema = new Schema(this, tsdb, id);
    return Deferred.fromResult(null);
  }
  
  @Override
  public String type() {
    return TYPE;
  }


  @Override
  public TimeSeriesDataSourceConfig parseConfig(final ObjectMapper mapper,
                                     final TSDB tsdb,
                                     final JsonNode node) {
    return DefaultTimeSeriesDataSourceConfig.parseConfig(mapper, tsdb, node);
  }

  @Override
  public void setupGraph(
      final QueryPipelineContext context,
      final TimeSeriesDataSourceConfig config,
      final QueryPlanner planner) {

    if (config.hasBeenSetup()) {
      // all done.
      return;
    }

    List<QueryNodeConfig> pushDownNodes = config.getPushDownNodes();
    for (QueryNodeConfig pushdowns : pushDownNodes) {
      if (pushdowns instanceof TimeShiftConfig) {
        return;
      }
    }

    if ((config).timeShifts() != null) {
      DefaultTimeSeriesDataSourceConfig.setupTimeShift(config, planner);
    }
  }

  @Override
  public Tsdb1xQueryNode newNode(final QueryPipelineContext context) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Tsdb1xQueryNode newNode(final QueryPipelineContext context,
                           final TimeSeriesDataSourceConfig config) {
    TimeSeriesDataSourceConfig source_config = config;
    if (!Strings.isNullOrEmpty((source_config).getSummaryInterval())) {
      TimeSeriesDataSourceConfig.Builder builder = (TimeSeriesDataSourceConfig.Builder) source_config.toBuilder();

      if ((source_config).getSummaryInterval().toLowerCase().endsWith("all")) {
        // TODO - revisit this one.
//        if (rollup_config != null) {
//          // compute an interval from the query span
//          // TODO - other timestamps. For now just seconds.
//          final long span = context.query().endTime().epoch() - 
//              context.query().startTime().epoch();
//          for (final RollupInterval interval : rollup_config.getRollupIntervals()) {
//            if (span % interval.getIntervalSeconds() == 0) {
//              builder.addRollupInterval(interval.getInterval());
//            }
//          }
//        }
        
        // TODO - figure out padding
      } else {
        if (rollup_config != null) {
          try {
            builder.setRollupIntervals(rollup_config.getPossibleIntervals(
                (source_config).getSummaryInterval()));
          } catch (NoSuchRollupForIntervalException e) {
            // ignore, we'll use raw.
          }
        }
        
        // TODO compute the padding
        builder.setPrePadding("1h");
        builder.setPostPadding("30m");
      }
      
      source_config = (TimeSeriesDataSourceConfig) builder.build();
    }
    
    return schema.dataStore().newNode(context, source_config);
  }

  @Override
  public Deferred<TimeSeriesStringId> resolveByteId(
      final TimeSeriesByteId id, 
      final Span span) {
    return schema.resolveByteId(id, span);
  }
  
  @Override
  public Deferred<List<byte[]>> encodeJoinKeys(
      final List<String> join_keys, 
      final Span span) {
    return schema.getIds(UniqueIdType.TAGK, join_keys, span);
  }
  
  @Override
  public Deferred<List<byte[]>> encodeJoinMetrics(
      final List<String> join_metrics, 
      final Span span) {
    return schema.getIds(UniqueIdType.METRIC, join_metrics, span);
  }
  
  @Override
  public RollupConfig rollupConfig() {
    return rollup_config;
  }
  
  void registerConfigs(final TSDB tsdb) {
    if (!tsdb.getConfig().hasProperty(getConfigKey(ROLLUP_KEY))) {
      tsdb.getConfig().register(
          ConfigurationEntrySchema.newBuilder()
          .setKey(getConfigKey(ROLLUP_KEY))
          .setType(DefaultRollupConfig.class)
          .setDescription("The JSON or YAML config with a mapping of "
              + "aggregations to numeric IDs and intervals to tables and spans.")
          .isNullable()
          .setSource(getClass().getName())
          .build());
    }
    if (!tsdb.getConfig().hasProperty(getConfigKey(ROLLUP_ENABLED_KEY))) {
      tsdb.getConfig().register(getConfigKey(ROLLUP_ENABLED_KEY), false, false, 
          "Whether or not rollups are enabled for this schema.");
    }
  }

  String getConfigKey(final String suffix) {
    return KEY_PREFIX + (Strings.isNullOrEmpty(id) || id.equals(TYPE) ? 
        "" : id + ".")
      + suffix;
  }
}
