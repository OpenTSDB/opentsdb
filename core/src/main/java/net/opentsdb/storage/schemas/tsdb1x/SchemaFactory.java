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
package net.opentsdb.storage.schemas.tsdb1x;

import java.util.List;

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
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.TimeSeriesQuery;
import net.opentsdb.query.plan.QueryPlanner;
import net.opentsdb.stats.Span;
import net.opentsdb.storage.WritableTimeSeriesDataStore;
import net.opentsdb.storage.WritableTimeSeriesDataStoreFactory;
import net.opentsdb.uid.UniqueIdType;

/**
 * Simple singleton factory that implements a default and named schemas
 * (for different configurations).
 * 
 * @since 3.0
 */
public class SchemaFactory extends BaseTSDBPlugin 
                           implements TimeSeriesDataSourceFactory,
                                      WritableTimeSeriesDataStoreFactory {
  public static final String TYPE = "Tsdb1xSchemaFactory";
  
  /** The default schema. */
  protected Schema schema;
  
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
  public boolean supportsPushdown(
      final Class<? extends QueryNodeConfig> function) {
    // TODO Auto-generated method stub
    return false;
  }
  
  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
    schema = new Schema(this, tsdb, id);
    return Deferred.fromResult(null);
  }
  
  @Override
  public String type() {
    return TYPE;
  }

  @Override
  public String version() {
    // TODO Implement
    return "3.0.0";
  }

  @Override
  public QueryNodeConfig parseConfig(final ObjectMapper mapper, 
                                     final TSDB tsdb,
                                     final JsonNode node) {
    return DefaultTimeSeriesDataSourceConfig.parseConfig(mapper, tsdb, node);
  }

  @Override
  public void setupGraph(final TimeSeriesQuery query, 
                         final QueryNodeConfig config,
                         final QueryPlanner planner) {
    // TODO Auto-generated method stub
  }

  @Override
  public QueryNode newNode(final QueryPipelineContext context) {
    throw new UnsupportedOperationException();
  }

  @Override
  public QueryNode newNode(final QueryPipelineContext context,
                           final QueryNodeConfig config) {
    return schema.dataStore().newNode(context, config);
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
}
