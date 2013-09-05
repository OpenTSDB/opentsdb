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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import net.opentsdb.utils.DateTime;

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
 * {@code agg}, {@code downsampler} and {@code downsample_interval} fields.
 * @since 2.0
 */
public final class TSSubQuery {
  /** User given name of an aggregation function to use */
  private String aggregator;
  
  /** User given name for a metric, e.g. "sys.cpu.0" */
  private String metric;
  
  /** User provided list of timeseries UIDs */
  private List<String> tsuids;
  
  /** User supplied list of tags for specificity or grouping. May be null or 
   * empty */
  private HashMap<String, String> tags;
  
  /** User given downsampler */
  private String downsample;
  
  /** Whether or not the user wants to perform a rate conversion */
  private boolean rate;
  
  /** Rate options for counter rollover/reset */
  private RateOptions rate_options;
  
  /** Parsed aggregation function */
  private Aggregator agg;
  
  /** Parsed downsampler function */
  private Aggregator downsampler;
  
  /** Parsed downsample interval */
  private long downsample_interval;
  
  /**
   * Default constructor necessary for POJO de/serialization
   */
  public TSSubQuery() {
    
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
    
    // parse the downsampler if we have one
    if (downsample != null && !downsample.isEmpty()) {
      final int dash = downsample.indexOf('-', 1); // 1st char can't be
                                                        // `-'.
      if (dash < 0) {
        throw new IllegalArgumentException("Invalid downsampling specifier '" 
            + downsample + "' in [" + downsample + "]");
      }
      try {
        downsampler = Aggregators.get(downsample.substring(dash + 1));
      } catch (NoSuchElementException e) {
        throw new IllegalArgumentException("No such downsampling function: "
            + downsample.substring(dash + 1));
      }
      downsample_interval = DateTime.parseDuration(
          downsample.substring(0, dash));
    }
  }

  /** @return the parsed aggregation function */
  public Aggregator aggregator() {
    return this.agg;
  }
  
  /** @return the parsed downsampler aggregation function */
  public Aggregator downsampler() {
    return this.downsampler;
  }
  
  /** @return the parsed downsample interval in seconds */
  public long downsampleInterval() {
    return this.downsample_interval;
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

  /** @return the user supplied list of query tags, may be empty */
  public Map<String, String> getTags() {
    if (tags == null) {
      return Collections.emptyMap();
    }
    return tags;
  }

  /** @return the raw downsampling function request from the user, 
   * e.g. "1h-avg" */
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

  /** @param tags an optional list of tags for specificity or grouping */
  public void setTags(HashMap<String, String> tags) {
    this.tags = tags;
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
}
