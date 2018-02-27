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
package net.opentsdb.storage;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import org.hbase.async.HBaseClient;

import com.google.common.base.Strings;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.query.QueryIteratorFactory;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.stats.Span;
import net.opentsdb.storage.schemas.tsdb1x.Schema;

/**
 * TODO - complete.
 * 
 * @since 3.0
 */
public class Tsdb1xHBaseDataStore extends TimeSeriesDataStore {

  /** The AsyncHBase client. */
  private HBaseClient client;
  
  private Schema schema;
  
  public Tsdb1xHBaseDataStore(final TSDB tsdb, final String id) {
    super(tsdb, id);
    final org.hbase.async.Config async_config = new org.hbase.async.Config();
    if (Strings.isNullOrEmpty(
        async_config.getString("asynchbase.zk.base_path"))) {
      async_config.overrideConfig("asynchbase.zk.base_path", 
          tsdb.getConfig().getString("tsd.storage.hbase.zk_basedir"));
    }
    if (Strings.isNullOrEmpty(async_config.getString("asynchbase.zk.quorum"))) {
      async_config.overrideConfig("asynchbase.zk.quorum", 
          tsdb.getConfig().getString("tsd.storage.hbase.zk_quorum"));
    }
    
    client = new HBaseClient(async_config);
  }
  
  @Override
  public Deferred<Object> shutdown() {
    if (client != null) {
      return client.shutdown();
    }
    return Deferred.fromResult(null);
  }

  @Override
  public String id() {
    return "AsyncHBaseDataStore";
  }

  @Override
  public String version() {
    return "3.0.0";
  }
  
  @Override
  public Deferred<Object> write(final TimeSeriesStringId id, 
                                final TimeSeriesValue<?> value, 
                                final Span span) {
    // TODO Auto-generated method stub
    return null;
  }
  
  @Override
  public QueryNode newNode(QueryPipelineContext context,
      QueryNodeConfig config) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Collection<TypeToken<?>> types() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void registerIteratorFactory(TypeToken<?> type,
      QueryIteratorFactory factory) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> newIterator(
      TypeToken<?> type, QueryNode node, Collection<TimeSeries> sources) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> newIterator(
      TypeToken<?> type, QueryNode node, Map<String, TimeSeries> sources) {
    // TODO Auto-generated method stub
    return null;
  }

}
