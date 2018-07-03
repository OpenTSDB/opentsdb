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
package net.opentsdb.query;

import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSDBPlugin;
import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.storage.TimeSeriesDataStore;
import net.opentsdb.storage.TimeSeriesDataStoreFactory;

/**
 * The basic factory for data sources that will look for the named
 * source in the registry. Note that configurations must always be
 * specified for a data source as we need to know which metric to fetch.
 * Thus {@link #newNode(QueryPipelineContext, String)} always throws
 * an exception.
 * 
 * @since 3.0
 */
public class QueryDataSourceFactory implements SingleQueryNodeFactory, TSDBPlugin {
  
  /** The TSDB to pull the registry from. */
  private TSDB tsdb;
  
  @Override
  public QueryNode newNode(final QueryPipelineContext context, 
                           final String id) {
    throw new QueryExecutionException("Data Source nodes require a config.", 0);
  }

  @Override
  public QueryNode newNode(final QueryPipelineContext context, 
                           final String id,
                           final QueryNodeConfig config) {
    final TimeSeriesDataStoreFactory factory = tsdb.getRegistry()
        .getDefaultPlugin(TimeSeriesDataStoreFactory.class);
      if (factory == null) {
        throw new RuntimeException("No factory!");
      }
      
      final TimeSeriesDataStore store = factory.newInstance(tsdb, null);
      if (store == null) {
        throw new QueryExecutionException("Unable to get a data store "
            + "instance from factory: " + factory.id(), 0);
      }
      return store.newNode(context, id, config);
  }

  @Override
  public String id() {
    return "datasource";
  }

  @Override
  public Class<? extends QueryNodeConfig> nodeConfigClass() {
    return QuerySourceConfig.class;
  }

  @Override
  public Deferred<Object> initialize(final TSDB tsdb) {
    this.tsdb = tsdb;
    return Deferred.fromResult(null);
  }

  @Override
  public Deferred<Object> shutdown() {
    return Deferred.fromResult(null);
  }

  @Override
  public String version() {
    return "3.0.0";
  }

}
