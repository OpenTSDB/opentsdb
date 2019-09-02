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
package net.opentsdb.query;

import java.time.temporal.TemporalAmount;
import java.util.Collection;
import java.util.List;

import net.opentsdb.query.filter.MetricFilter;
import net.opentsdb.query.filter.QueryFilter;
import net.opentsdb.utils.Pair;

/**
 * The base class for a data source, including ddata types, filters and the metric we want.
 *
 * @since 3.0
 */
public interface TimeSeriesDataSourceConfig<
        B extends TimeSeriesDataSourceConfig.Builder<B, C>, C extends TimeSeriesDataSourceConfig>
    extends QueryNodeConfig<B, C> {

  public static final String DEFAULT = "TimeSeriesDataSource";
  
  /** @return The ID of the node as set by the user and populates the 
   * {@link QueryResult#dataSource()} field. May be the same as the ID. */
  public String getDataSourceId();
  
  /** @return The source ID. May be null in which case we use the default. */
  public String getSourceId();
  
  /** @return A list of data types to filter on. If null or empty, fetch
   * all. */
  public List<String> getTypes();
  
  /** @return An optional namespace for such systems as support it. */
  public String getNamespace();

  /** @return An optional starting index for pagination. */
  public int getFrom();

  /** @return An optional size for pagination. */
  public int getSize();

  /** @return The optional metric filter. */
  public MetricFilter getMetric();
  
  /** @return An optional filter ID to fetch. */
  public String getFilterId();
  
  /** @return The local filter if set, null if not. */
  public QueryFilter getFilter();
  
  /** @return Whether or not to fetch just the last (latest) value. */
  public boolean getFetchLast();

  /** @return An optional summary interval from an upstream downsampler. */
  public String getSummaryInterval();

  /** @return An optional list of summary aggregations from an upstream downsampler. */
  public List<String> getSummaryAggregations();
  
  /** @return An optional list of rollup intervals as durations. */
  public List<String> getRollupIntervals();

  /** @return An optional list of push down nodes. May be null. */
  public List<QueryNodeConfig> getPushDownNodes();
  
  public Collection<String> pushDownSinks();
  
  /** @return An optional pre-query start time padding string as a duration. */
  public String getPrePadding();
  
  /** @return An optional post-query end time padding string as a duration. */
  public String getPostPadding();
  
  /** @return An optional time shift interval for emitting additional time series
   * with the same metric + filters but at additional offsets. Useful for 
   * period over period plots. In the TSDB duration format, e.g. "1w". */
  public String getTimeShiftInterval();
  
  /** @return An optional map of dataSource() names to temporal amounts.
   * The values are <previous == true/post == false, TemporalAmount> */
  public Pair<Boolean, TemporalAmount> timeShifts();
  
  /** @return Whether or not the node has been setup so we can avoid infinite
   * loops when configuring the graph. */
  public boolean hasBeenSetup();
  
  /**
   * A base builder interface for data source configs.
   */
  interface Builder<B extends Builder<B, C>, 
      C extends TimeSeriesDataSourceConfig> extends QueryNodeConfig.Builder<B, C> {
    
    B setDataSourceId(final String data_source_id);
    
    B setSourceId(final String source_id);
    
    B setTypes(final List<String> types);
    
    B addType(final String type);
    
    B setNamespace(final String namespace);

    B setFrom(final int from);

    B setSize(final int size);

    B setFilterId(final String filter_id);
    
    B setQueryFilter(final QueryFilter filter);
    
    B setFetchLast(final boolean fetch_last);

    B setSummaryInterval(final String summary_interval);

    B setSummaryAggregations(final List<String> summary_aggregations);

    B addSummaryAggregation(final String summary_aggregation);
    
    B setRollupIntervals(final List<String> rollup_intervals);
    
    B addRollupInterval(final String rollup_interval);

    B addPushDownNode(final QueryNodeConfig node);

    B setPushDownNodes(final List<QueryNodeConfig> push_down_nodes);
    
    B setPrePadding(final String pre_padding);
    
    B setPostPadding(final String post_padding);
    
    B setTimeShiftInterval(final String interval);
    
    B setTimeShifts(final Pair<Boolean, TemporalAmount> amounts);
    
    B setHasBeenSetup(final boolean has_been_setup);
    
    String id();
    
    String sourceId();
    
  }
}
