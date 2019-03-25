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
import java.util.List;
import java.util.Map;

import net.opentsdb.query.filter.MetricFilter;
import net.opentsdb.query.filter.QueryFilter;
import net.opentsdb.utils.Pair;

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
  
  /** @return An optional namespace for such systems as support it. */
  public String getNamespace();
  
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
  
  /** @return An optional summary interval from an upstream downsampler. */
  public String getSummaryInterval();
  
  /** @return An optional list of summary aggregations from an upstream downsampler. */
  public List<String> getSummaryAggregations();
  
  /** @return An optional list of rollup intervals as durations. */
  public List<String> getRollupIntervals();
  
  /** @return An optional pre-query start time padding string as a duration. */
  public String getPrePadding();
  
  /** @return An optional post-query end time padding string as a duration. */
  public String getPostPadding();
  
  /** @return An optional time shift interval for emitting additional time series
   * with the same metric + filters but at additional offsets. Useful for 
   * period over period plots. In the TSDB duration format, e.g. "1w". */
  public String getTimeShiftInterval();
  
  /** @return The number of intervals previous to the current query time range
   * to fetch data for, offset by {@link #getTimeShiftInterval()}. */
  public int getPreviousIntervals();
  
  /** @return The number of intervals after the current query time range to 
   * fetch data for, offset by {@link #getTimeShiftInterval()}. */
  public int getNextIntervals();
  
  /** @return An optional map of dataSource() names to temporal amounts. 
   * The values are <previous == true/post == false, TemporalAmount> */
  public Map<String, Pair<Boolean, TemporalAmount>> timeShifts();
  
  /** @return Whether or not the node has been setup so we can avoid infinite
   * loops when configuring the graph. */
  public boolean hasBeenSetup();
  
  /**
   * A base builder interface for data source configs.
   */
  public static interface Builder extends QueryNodeConfig.Builder {
    public Builder setSourceId(final String source_id);
    
    public Builder setTypes(final List<String> types);
    
    public Builder addType(final String type);
    
    public Builder setNamespace(final String namespace);
    
    public Builder setMetric(final MetricFilter metric);
    
    public Builder setFilterId(final String filter_id);
    
    public Builder setQueryFilter(final QueryFilter filter);
    
    public Builder setFetchLast(final boolean fetch_last);
    
    public Builder setPushDownNodes(
        final List<QueryNodeConfig> push_down_nodes);
    
    public Builder addPushDownNode(final QueryNodeConfig node);

    public Builder setSummaryInterval(final String summary_interval);
    
    public Builder setSummaryAggregations(final List<String> summary_aggregations);
    
    public Builder addSummaryAggregation(final String summary_aggregation);
    
    public Builder setRollupIntervals(final List<String> rollup_intervals);
    
    public Builder addRollupInterval(final String rollup_interval);
    
    public Builder setPrePadding(final String pre_padding);
    
    public Builder setPostPadding(final String post_padding);
    
    public Builder setTimeShiftInterval(final String interval);
    
    public Builder setPreviousIntervals(final int intervals);
    
    public Builder setNextIntervals(final int intervals);
    
    public Builder setTimeShifts(
        final Map<String, Pair<Boolean, TemporalAmount>> amounts);
    
    public Builder setHasBeenSetup(final boolean has_been_setup);
    
    public String id();
    
    public String sourceId();
    
    public TimeSeriesDataSourceConfig build();
    
  }
}
