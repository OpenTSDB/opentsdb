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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSDBPlugin;
import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.query.filter.MetricFilter;
import net.opentsdb.query.filter.QueryFilter;
import net.opentsdb.query.filter.QueryFilterFactory;
import net.opentsdb.storage.ReadableTimeSeriesDataStore;
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
      
      final ReadableTimeSeriesDataStore store = factory.newInstance(tsdb, null);
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
  
  @Override
  public QueryNodeConfig parseConfig(final ObjectMapper mapper, 
                                     final TSDB tsdb,
                                     final JsonNode node) {
    QuerySourceConfig.Builder builder = QuerySourceConfig.newBuilder();
    // TODO - types
    
    JsonNode n = node.get("metric");
    if (n == null) {
      throw new IllegalArgumentException("Missing the metric field.");
    }
    JsonNode type_node = n.get("type");
    if (type_node == null) {
      throw new IllegalArgumentException("Missing the metric type field.");
    }
    String type = type_node.asText();
    if (Strings.isNullOrEmpty(type)) {
      throw new IllegalArgumentException("Metric type field cannot be null or empty.");
    }
    QueryFilterFactory factory = tsdb.getRegistry().getPlugin(QueryFilterFactory.class, type);
    if (factory == null) {
      throw new IllegalArgumentException("No query filter factory found for: " + type);
    }
    QueryFilter filter = factory.parse(tsdb, mapper, n);
    if (filter == null || !(filter instanceof MetricFilter)) {
      throw new IllegalArgumentException("Metric query filter was not "
          + "an instanceof MetricFilter: " + filter.getClass());
    }
    builder.setMetric((MetricFilter) filter);
    
    n = node.get("filterId");
    if (n != null && !Strings.isNullOrEmpty(n.asText())) {
      builder.setFilterId(n.asText());
    } else {
      n = node.get("filter");
      if (n != null) {
        type_node = n.get("type");
        if (type_node == null) {
          throw new IllegalArgumentException("Missing the filter type field.");
        }
        
        type = type_node.asText();
        if (Strings.isNullOrEmpty(type)) {
          throw new IllegalArgumentException("Filter type field cannot be null or empty.");
        }
        
        factory = tsdb.getRegistry().getPlugin(QueryFilterFactory.class, type);
        if (factory == null) {
          throw new IllegalArgumentException("No query filter factory found for: " + type);
        }
        filter = factory.parse(tsdb, mapper, n);
        if (filter == null) {
          throw new IllegalArgumentException("Unable to parse filter config.");
        }
        builder.setQueryFilter(filter);
      }
    }
    
    return builder.build();
  }

}
