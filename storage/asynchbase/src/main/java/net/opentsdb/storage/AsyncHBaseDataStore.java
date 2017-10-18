// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.storage;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import org.hbase.async.HBaseClient;

import com.google.common.base.Strings;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import io.opentracing.Span;
import net.opentsdb.core.DefaultTSDB;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.iterators.IteratorGroups;
import net.opentsdb.query.QueryIteratorFactory;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.context.QueryContext;
import net.opentsdb.query.execution.QueryExecution;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.stats.TsdbTrace;

/**
 * TODO - complete.
 * 
 * @since 3.0
 */
public class AsyncHBaseDataStore extends TimeSeriesDataStore {

  /** The AsyncHBase client. */
  private HBaseClient client;
  
  @Override
  public Deferred<Object> initialize(final TSDB tsdb) {
    this.tsdb = tsdb;
    
    final org.hbase.async.Config async_config;
    if (tsdb.getConfig().configLocation() != null && !tsdb.getConfig().configLocation().isEmpty()) {
      try {
        async_config = new org.hbase.async.Config(tsdb.getConfig().configLocation());
      } catch (final IOException e) {
        throw new RuntimeException("Failed to read the config file: " + 
            tsdb.getConfig().configLocation(), e);
      }
    } else {
      async_config = new org.hbase.async.Config();
    }
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
    return Deferred.fromResult(null);
  }
  
  @Override
  public void collectStats(final StatsCollector collector) {
    
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
  public Deferred<Object> write(TimeSeriesId id, 
      TimeSeriesValue<?> value, TsdbTrace trace,
      Span upstream_span) {
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
