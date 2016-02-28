// This file is part of OpenTSDB.
// Copyright (C) 2013  The OpenTSDB Authors.
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
package net.opentsdb.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.utils.ByteSet;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;

/**
 * Represents the parameters for an individual sub query on a metric or specific
 * timeseries. When setting up a query, use the setter methods to store user 
 * information such as the start time and list of queries. After setting the 
 * proper values, add the sub query to a {@link TSQuery}. 
 * <p>
 * When the query is processed by the TSD, if the {@code tsuids} list has one
 * or more timeseries, the {@code metric} and {@code tags} fields will be 
 * ignored and only the tsuids processed.
 * <p>
 * <b>Note:</b> You do not need to call {@link #validateAndSetQuery} directly as
 * the {@link TSQuery} object will call this for you when the entire set of 
 * queries has been compiled.
 * <b>Note:</b> If using POJO deserialization, make sure to avoid setting the 
 * {@code agg} and {@code downsample_specifier} fields.
 * @since 2.0
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class TSSubQuery {
  /** User given name of an aggregation function to use */
  private String aggregator;
  
  /** User given name for a metric, e.g. "sys.cpu.0" */
  private String metric;
  
  /** User provided list of timeseries UIDs */
  private List<String> tsuids;

  /** User given downsampler */
  private String downsample;
  
  /** Whether or not the user wants to perform a rate conversion */
  private boolean rate;
  
  /** Rate options for counter rollover/reset */
  private RateOptions rate_options;
  
  /** Parsed aggregation function */
  private Aggregator agg;
  
  /** Parsed downsampling specification. */
  private DownsamplingSpecification downsample_specifier;
  
  /** A list of filters for this query. For now these are pulled out of the
   * tags map. In the future we'll have special JSON objects for them. */
  private List<TagVFilter> filters;
  
  /** Whether or not to match series with ONLY the given tags */
  private boolean explicit_tags;
  
  /** Index of the sub query */
  private int index;
  
  /**
   * Default constructor necessary for POJO de/serialization
   */
  public TSSubQuery() {
    // Assume no downsampling until told otherwise.
    downsample_specifier = DownsamplingSpecification.NO_DOWNSAMPLER;
  }

  @Override
  public int hashCode() {
    // NOTE: Do not add any non-user submitted variables to the hash. We don't
    // want the hash to change after validation.
    return Objects.hashCode(aggregator, metric, tsuids, downsample, rate, 
        rate_options, filters, explicit_tags);
  }
  
  @Override
  public boolean equals(final Object obj) {
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof TSSubQuery)) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    
    // NOTE: Do not add any non-user submitted variables to the comparator. We 
    // don't want the value to change after validation.
    final TSSubQuery query = (TSSubQuery)obj;
    return Objects.equal(aggregator, query.aggregator)
        && Objects.equal(metric, query.metric)
        && Objects.equal(tsuids, query.tsuids)
        && Objects.equal(downsample, query.downsample)
        && Objects.equal(rate, query.rate)
        && Objects.equal(rate_options, query.rate_options)
        && Objects.equal(filters, query.filters) 
        && Objects.equal(explicit_tags, query.explicit_tags);
  }
  
  public String toString() {
    final StringBuilder buf = new StringBuilder();
    buf.append("TSSubQuery(metric=")
      .append(metric == null || metric.isEmpty() ? "" : metric);
    buf.append(", filters=[");
    if (filters != null && !filters.isEmpty()) {
      int counter = 0;
      for (final TagVFilter filter : filters) {
        if (counter > 0) {
          buf.append(", ");
        }
        buf.append(filter);
        ++counter;
      }
    }
    buf.append("], tsuids=[");
    if (tsuids != null && !tsuids.isEmpty()) {
      int counter = 0;
      for (String tsuid : tsuids) {
        if (counter > 0) {
          buf.append(", ");
        }
        buf.append(tsuid);
        counter++;
      }
    }
    buf.append("], agg=")
      .append(aggregator)
      .append(", downsample=")
      .append(downsample)
      .append(", ds_interval=")
      .append(downsample_specifier.getInterval())
      .append(", rate=")
      .append(rate)
      .append(", rate_options=")
      .append(rate_options)
      .append(", explicit_tags=")
      .append("explicit_tags")
      .append(", index=")
      .append(index)
      .append(")");
    return buf.toString();
  }
  
  /**
   * Runs through query parameters to make sure it's a valid request.
   * This includes parsing the aggregator, downsampling info, metrics, tags or
   * timeseries and setting the local parsed fields needed by the TSD for proper
   * execution. If no exceptions are thrown, the query is considered valid.
   * <b>Note:</b> You do not need to call this directly as it will be executed
   * by the {@link TSQuery} object the sub query is assigned to.
   * @throws IllegalArgumentException if something is wrong with the query
   */
  public void validateAndSetQuery() {
    if (aggregator == null || aggregator.isEmpty()) {
      throw new IllegalArgumentException("Missing the aggregation function");
    }
    try {
      agg = Aggregators.get(aggregator);
    } catch (NoSuchElementException nse) {
      throw new IllegalArgumentException(
          "No such aggregation function: " + aggregator);
    }
    
    // we must have at least one TSUID OR a metric
    if ((tsuids == null || tsuids.isEmpty()) && 
        (metric == null || metric.isEmpty())) {
      throw new IllegalArgumentException(
          "Missing the metric or tsuids, provide at least one");
    }
    
    // Make sure we have a filter list
    if (filters == null) {
      filters = new ArrayList<TagVFilter>();
    }

    // parse the downsampler if we have one
    if (downsample != null && !downsample.isEmpty()) {
      // downsampler given, so parse it
      downsample_specifier = new DownsamplingSpecification(downsample);
    } else {
      // no downsampler
      downsample_specifier = DownsamplingSpecification.NO_DOWNSAMPLER;
    }
  }

  /** @return the parsed aggregation function */
  public Aggregator aggregator() {
    return this.agg;
  }
  
  /** @return the parsed downsampler aggregation function */
  public Aggregator downsampler() {
    return downsample_specifier.getFunction();
  }
  
  /** @return the parsed downsample interval in seconds */
  public long downsampleInterval() {
    return downsample_specifier.getInterval();
  }
  
  /**
   * @return the downsampling fill policy
   * @since 2.2
   */
  public FillPolicy fillPolicy() {
    return downsample_specifier.getFillPolicy();
  }
  
  /** @return the user supplied aggregator */
  public String getAggregator() {
    return aggregator;
  }

  /** @return the user supplied metric */
  public String getMetric() {
    return metric;
  }

  /** @return the user supplied list of TSUIDs */
  public List<String> getTsuids() {
    return tsuids;
  }

  /** @return the user supplied list of group by query tags, may be empty.
   * Note that as of version 2.2 this is an immutable list of tags built from
   * the filter list.
   * @deprecated */
  public Map<String, String> getTags() {
    if (filters == null) {
      return Collections.emptyMap();
    }
    final Map<String, String> tags = new HashMap<String, String>(filters.size());
    for (final TagVFilter filter : filters) {
      if (filter.isGroupBy()) {
        tags.put(filter.getTagk(), filter.getType() + 
            "(" + filter.getFilter() + ")");
      }
    }
    return ImmutableMap.copyOf(tags);
  }

  /** @return the raw downsampling function request from the user, 
   * e.g. "1h-avg" or "15m-sum-nan" */
  public String getDownsample() {
    return downsample;
  }

  /** @return whether or not the user requested a rate conversion */
  public boolean getRate() {
    return rate;
  }

  /** @return options to use for rate calculations */
  public RateOptions getRateOptions() {
    return rate_options;
  }
  
  /** @return the filters pulled from the tags object 
   * @since 2.2 */
  public List<TagVFilter> getFilters() {
    if (filters == null) {
      filters = new ArrayList<TagVFilter>();
    }
    // send a copy so ordering doesn't mess up the hash code
    return new ArrayList<TagVFilter>(filters);
  }
  
  /** @return the unique set of tagks from the filters. May be null if no filters
   * were set. Must make sure to resolve the string tag to UIDs in the filter first.
   * @since 2.3
   */
  public ByteSet getFilterTagKs() {
    if (filters == null || filters.isEmpty()) {
      return null;
    }
    final ByteSet tagks = new ByteSet();
    for (final TagVFilter filter : filters) {
      if (filter != null && filter.getTagkBytes() != null) {
        tagks.add(filter.getTagkBytes());
      }
    }
    return tagks;
  }
  
  /** @return whether or not to match series with ONLY the given tags 
   * @since 2.3 */
  public boolean getExplicitTags() {
    return explicit_tags;
  }
  
  /** @return the index of the sub query
   * @since 2.3 */
  public int getIndex() {
    return index;
  }
  
  /** @param aggregator the name of an aggregation function */
  public void setAggregator(String aggregator) {
    this.aggregator = aggregator;
  }

  /** @param metric the name of a metric to fetch */
  public void setMetric(String metric) {
    this.metric = metric;
  }

  /** @param tsuids a list of timeseries UIDs as hex encoded strings to fetch */
  public void setTsuids(List<String> tsuids) {
    this.tsuids = tsuids;
  }

  /** @param tags an optional list of tags for specificity or grouping
   * As of 2.2 this will convert the existing tags to filter
   * @deprecated */
  public void setTags(Map<String, String> tags) {
    if (filters == null) {
      filters = new ArrayList<TagVFilter>(tags.size());
    } else {
      filters.clear();
    }
    TagVFilter.tagsToFilters(tags, filters);
  }

  /** @param downsample the downsampling function to use, e.g. "2h-avg" */
  public void setDownsample(String downsample) {
    this.downsample = downsample;
  }

  /** @param rate whether or not the result should be rate converted */
  public void setRate(boolean rate) {
    this.rate = rate;
  }

  /** @param options Options to set when calculating rates */
  public void setRateOptions(RateOptions options) {
    this.rate_options = options;
  }
  
  /** @param filters A list of filters to use when querying
   * @since 2.2 */
  public void setFilters(List<TagVFilter> filters) {
    this.filters = filters;
  }
  
  /** @param whether or not to match series with ONLY the given tags 
   * @since 2.3 */
  public void setExplicitTags(final boolean explicit_tags) {
    this.explicit_tags = explicit_tags;
  }
  
  /** @param index the index of the sub query
   * @since 2.3 */
  public void setIndex(final int index) {
    this.index = index;
  }
  
}
