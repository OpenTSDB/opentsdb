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

import java.util.List;

import net.opentsdb.query.filter.MetricFilter;
import net.opentsdb.query.filter.QueryFilter;

/**
 * The base class for a data source, including ddata types, filters
 * and the metric we want.
 * 
 * @since 3.0
 */
public interface TimeSeriesDataSourceConfig extends QueryNodeConfig {

  public static final String DEFAULT = "TimeSeriesDataSource";
  
  /** @return The source ID. May be null in which case we use the default. */
  public String getSourceId();
  
  /** @return A list of data types to filter on. If null or empty, fetch
   * all. */
  public List<String> getTypes();
  
  /** @return The non-null metric filter. */
  public MetricFilter getMetric();
  
  /** @return An optional filter ID to fetch. */
  public String getFilterId();
  
  /** @return The local filter if set, null if not. */
  public QueryFilter getFilter();
  
  /** @return Whether or not to fetch just the last (latest) value. */
  public boolean getFetchLast();
  
  /** @return An optional list of push down nodes. May be null. */
  public List<QueryNodeConfig> getPushDownNodes();
  
  /** @return A non-null clone of the current config as a builder
   * that can be modified. */
  public Builder getBuilder();
  
  /**
   * A base builder interface for data source configs.
   */
  public static interface Builder {
    public Builder setSourceId(final String source_id);
    
    public Builder setTypes(final List<String> types);
    
    public Builder addType(final String type);
    
    public Builder setMetric(final MetricFilter metric);
    
    public Builder setFilterId(final String filter_id);
    
    public Builder setQueryFilter(final QueryFilter filter);
    
    public Builder setFetchLast(final boolean fetch_last);
    
    public Builder setPushDownNodes(
        final List<QueryNodeConfig> push_down_nodes);
    
    public Builder addPushDownNode(final QueryNodeConfig node);
    
    public TimeSeriesDataSourceConfig build();
    
  }
}
