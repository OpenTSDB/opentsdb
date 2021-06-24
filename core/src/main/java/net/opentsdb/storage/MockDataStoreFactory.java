// This file is part of OpenTSDB.
// Copyright (C) 2018-2021  The OpenTSDB Authors.
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
package net.opentsdb.storage;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.common.Const;
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
import net.opentsdb.rollup.NoSuchRollupForIntervalException;
import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.stats.Span;

/**
 * Simple little factory that returns a {@link MockDataStore}.
 * 
 * @since 3.0
 */
public class MockDataStoreFactory extends BaseTSDBPlugin 
  implements TimeSeriesDataSourceFactory<TimeSeriesDataSourceConfig, MockDataStore.LocalNode>,
             TimeSeriesDataConsumerFactory {
  private static final Logger LOG = LoggerFactory.getLogger(
      MockDataStoreFactory.class);
  
  public static final String TYPE = "MockDataStore";
  
  /** The data store. */
  private MockDataStore mds;
  
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
    if (config.hasBeenSetup()) {
      // all done.
      return;
    }
  }

  @Override
  public MockDataStore.LocalNode newNode(final QueryPipelineContext context) {
    throw new UnsupportedOperationException("No default configs for "
        + "MockDataStore.");
  }

  @Override
  public MockDataStore.LocalNode newNode(final QueryPipelineContext context,
                                         final TimeSeriesDataSourceConfig config) {
    TimeSeriesDataSourceConfig source_config = config;
    if (!Strings.isNullOrEmpty((source_config).getSummaryInterval())) {
      final TimeSeriesDataSourceConfig.Builder builder = 
          (TimeSeriesDataSourceConfig.Builder) source_config.toBuilder();
      if (mds.rollupConfig() != null) {
        try {
          builder.setRollupIntervals(mds.rollupConfig().getPossibleIntervals(
              (source_config).getSummaryInterval()));
        } catch (NoSuchRollupForIntervalException e) {
          // ignore, we'll use raw.
        }
        // TODO compute the padding
        builder.setPrePadding("1h");
        builder.setPostPadding("30m");
      }
      return mds.new LocalNode(context, (TimeSeriesDataSourceConfig) builder.build());
    }
    return mds.new LocalNode(context, config);
  }
  
  @Override
  public TypeToken<? extends TimeSeriesId> idType() {
    return Const.TS_STRING_ID;
  }
  
  @Override
  public boolean supportsPushdown(
      final Class<? extends QueryNodeConfig> function) {
    // TODO Auto-generated method stub
    return false;
  }
  
  @Override
  public String type() {
    return TYPE;
  }
  
  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
    mds = new MockDataStore(tsdb, this.id);
    
    if (!tsdb.getConfig().hasProperty("MockDataStore.register.writer")) {
      // TODO - named configs
      tsdb.getConfig().register("MockDataStore.register.writer", false, false, 
          "Whether or not to register the mock data store as a writer as well.");
    }
    
    if (tsdb.getConfig().getBoolean("MockDataStore.register.writer")) {
      tsdb.getRegistry().registerPlugin(TimeSeriesDataConsumerFactory.class,
          this.id.equals(TYPE) ? null : this.id, this);
      LOG.info("Registered Mock Data Store writer as: " 
          + (this.id.equals(TYPE) ? null : this.id));
    }
    return Deferred.fromResult(null);
  }

  @Override
  public String version() {
    // TODO Implement
    return "3.0.0";
  }
  
  @Override
  public Deferred<TimeSeriesStringId> resolveByteId(
      final TimeSeriesByteId id, 
      final Span span) {
    return Deferred.fromError(new UnsupportedOperationException());
  }
  
  @Override
  public Deferred<List<byte[]>> encodeJoinKeys(
      final List<String> join_keys, 
      final Span span) {
    return Deferred.fromError(new UnsupportedOperationException());
  }
  
  @Override
  public Deferred<List<byte[]>> encodeJoinMetrics(
      final List<String> join_metrics, 
      final Span span) {
    return Deferred.fromError(new UnsupportedOperationException());
  }
  
  @Override
  public RollupConfig rollupConfig() {
    return null;
  }

  @Override
  public TimeSeriesDataConsumer consumer() {
    return mds;
  }

  MockDataStore mds() {
    return mds;
  }

}
