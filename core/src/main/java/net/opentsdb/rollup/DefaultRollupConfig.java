// This file is part of OpenTSDB.
// Copyright (C) 2015-2018 The OpenTSDB Authors.
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
package net.opentsdb.rollup;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import net.opentsdb.core.TSDB;
import net.opentsdb.exceptions.IllegalDataException;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.JSON;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.TreeMap;

/**
 * A class that contains the runtime configuration for a TSD's raw and rollup
 * tables.
 * 
 * Each rollup requires two table names for writing, an interval and a span.
 * temporal_table - The table name for raw, temporal only rollup data
 * groupby_table - The table name for pre-aggregated and rolled up data
 * interval - A interval that the rollup is aligned on, e.g. 10 minute intervals
 *            would be denoted as "10m" and hourly would be "1h".
 * span - A time unit describing the width of the row, or how many data points
 *        could be in it. E.g 'h' would mean the row holds an hour of data while
 *        'y' holds a full year. Possible values are:
 *        'h' = hour
 *        'd' = day
 *        'n' = month
 *        'y' = year
 * 
 * @since 2.4
 */
@JsonDeserialize(builder = DefaultRollupConfig.Builder.class)
public class DefaultRollupConfig implements RollupConfig {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultRollupConfig.class);
  
  /** The interval to interval map where keys are things like "10m" or "1d"*/
  protected final Map<String, RollupInterval> forward_intervals;
  
  /** The table name to interval map for queries */
  protected final Map<String, RollupInterval> reverse_intervals;
  
  /** The map of IDs to aggregators for use at query time. */
  protected final Map<Integer, String> ids_to_aggregations;
  
  /** The map of aggregators to IDs for use at write time. */
  protected final Map<String, Integer> aggregations_to_ids;
  
  /**
   * Default ctor for the builder.
   * @param builder A non-null builder to load from.
   */
  protected DefaultRollupConfig(final Builder builder) {
    forward_intervals = Maps.newHashMapWithExpectedSize(2);
    reverse_intervals = Maps.newHashMapWithExpectedSize(2);
    ids_to_aggregations = Maps.newHashMapWithExpectedSize(4);
    aggregations_to_ids = Maps.newHashMapWithExpectedSize(4);
    
    if (builder.intervals == null || builder.intervals.isEmpty()) {
      throw new IllegalArgumentException("Rollup config given but no intervals "
          + "were found.");
    }
    if (builder.aggregationIds == null || builder.aggregationIds.isEmpty()) {
      throw new IllegalArgumentException("Rollup config given but no aggegation "
          + "ID mappings found.");
    }
    int defaults = 0;
    for (final RollupInterval config_interval : builder.intervals) {
      if (forward_intervals.containsKey(config_interval.getInterval())) {
        throw new IllegalArgumentException(
            "Only one interval of each type can be configured: " + 
            config_interval);
      }
      if (config_interval.isDefaultInterval() && defaults++ >= 1) {
        throw new IllegalArgumentException("Multiple default intervals "
            + "configured. Only one is allowed: " + config_interval);
      }
      
      forward_intervals.put(config_interval.getInterval(), config_interval);
      reverse_intervals.put(config_interval.getTable(), config_interval);
      reverse_intervals.put(config_interval.getPreAggregationTable(), 
          config_interval);
      config_interval.setConfig(this);
      LOG.info("Loaded rollup interval: " + config_interval);
    }
    
    for (final Entry<String, Integer> entry : builder.aggregationIds.entrySet()) {
      if (entry.getValue() < 0 || entry.getValue() > 127) {
        throw new IllegalArgumentException("ID for aggregator must be between "
            + "0 and 127: " + entry);
      }
      final String agg = entry.getKey().toLowerCase();
      if (ids_to_aggregations.containsKey(entry.getValue())) {
        throw new IllegalArgumentException("Multiple mappings for the "
            + "ID '" + entry.getValue() + "' are not allowed."); 
      }
//      if (Aggregators.get(agg) == null) {
//        throw new IllegalArgumentException("No such aggregator found for " + agg);
//      }
      aggregations_to_ids.put(agg, entry.getValue());
      ids_to_aggregations.put(entry.getValue(), agg);
      LOG.info("Mapping aggregator '" + agg + "' to ID " + entry.getValue());
    }
    
    LOG.info("Configured [" + forward_intervals.size() + "] rollup intervals");
  }
  
  /**
   * Fetches the RollupInterval corresponding to the forward interval string map
   * @param interval The interval to lookup
   * @return The RollupInterval object configured for the given interval
   * @throws IllegalArgumentException if the interval is null or empty
   * @throws NoSuchRollupForIntervalException if the interval was not configured
   */
  public RollupInterval getRollupInterval(final String interval) {
    if (interval == null || interval.isEmpty()) {
      throw new IllegalArgumentException("Interval cannot be null or empty");
    }
    final RollupInterval rollup = forward_intervals.get(interval);
    if (rollup == null) {
      throw new NoSuchRollupForIntervalException(interval);
    }
    return rollup;
  }
  
  /**
   * Fetches the RollupInterval corresponding to the integer interval in seconds.
   * It returns a list of matching RollupInterval and best next matches in the 
   * order. It will help to search on the next best rollup tables.
   * It is guaranteed that it return a non-empty list
   * For example if the interval is 1 day
   *  then it may return RollupInterval objects in the order
   *    1 day, 1 hour, 10 minutes, 1 minute
   * @param interval The interval in seconds to lookup
   * @param str_interval String representation of the  interval, for logging
   * @return The RollupInterval object configured for the given interval
   * @throws IllegalArgumentException if the interval is null or empty
   * @throws NoSuchRollupForIntervalException if the interval was not configured
   */
  public List<RollupInterval> getRollupIntervals(final long interval, 
                                                 final String str_interval) {
    return getRollupIntervals(interval, str_interval, false);
  }
  
  /**
   * Fetches the RollupInterval corresponding to the integer interval in seconds.
   * It returns a list of matching RollupInterval and best next matches in the 
   * order. It will help to search on the next best rollup tables.
   * It is guaranteed that it return a non-empty list
   * For example if the interval is 1 day
   *  then it may return RollupInterval objects in the order
   *    1 day, 1 hour, 10 minutes, 1 minute
   * @param interval The interval in seconds to lookup
   * @param str_interval String representation of the  interval, for logging
   * @param skip_default Whether or not to include the default interval 
   * the results.
   * @return The RollupInterval object configured for the given interval
   * @throws IllegalArgumentException if the interval is null or empty
   * @throws NoSuchRollupForIntervalException if the interval was not configured
   */
  public List<RollupInterval> getRollupIntervals(final long interval, 
                                                 final String str_interval,
                                                 final boolean skip_default) {
    
    if (interval <= 0) {
      throw new IllegalArgumentException("Interval cannot be null or empty");
    }
    
    final Map<Long, RollupInterval> rollups = 
        new TreeMap<Long, RollupInterval>(Collections.reverseOrder());
    boolean right_match = false;
    
    for (RollupInterval rollup: forward_intervals.values()) {
      if (rollup.getIntervalSeconds() == interval) {
        if (rollup.isDefaultInterval() && skip_default) {
          right_match = true;
          continue;
        }
        
        rollups.put((long) rollup.getIntervalSeconds(), rollup);
        right_match = true;
      } else if (interval % rollup.getIntervalSeconds() == 0) {
        if (rollup.isDefaultInterval() && skip_default) {
          right_match = true;
          continue;
        }
        
        rollups.put((long) rollup.getIntervalSeconds(), rollup);
      }
    }

    if (rollups.isEmpty() && !right_match) {
      throw new NoSuchRollupForIntervalException(Long.toString(interval));
    }
    
    List<RollupInterval> best_matches = 
            new ArrayList<RollupInterval>(rollups.values());
    
    if (!right_match) {
      LOG.warn("No such rollup interval found, " + str_interval + ". So falling "
              + "back to the next best match " + best_matches.get(0).
                      getInterval());
    }
    
    return best_matches;
  }
  
  /**
   * Fetches the RollupInterval corresponding to the rollup or pre-agg table
   * name.
   * @param table The name of the table to fetch
   * @return The RollupInterval object matching the table
   * @throws IllegalArgumentException if the table is null or empty
   * @throws NoSuchRollupForTableException if the interval was not configured
   * for the given table
   */
  public RollupInterval getRollupIntervalForTable(final String table) {
    if (table == null || table.isEmpty()) {
      throw new IllegalArgumentException("The table name cannot be null or empty");
    }
    final RollupInterval rollup = reverse_intervals.get(table);
    if (rollup == null) {
      throw new NoSuchRollupForTableException(table);
    }
    return rollup;
  }

  /**
   * Makes sure each of the rollup tables exists
   * @param tsdb The TSDB to use for fetching the HBase client
   */
  public void ensureTablesExist(final TSDB tsdb) {
//    final List<Deferred<Object>> deferreds = 
//        new ArrayList<Deferred<Object>>(forward_intervals.size() * 2);
//    
//    for (RollupInterval interval : forward_intervals.values()) {
//      deferreds.add(tsdb.getClient()
//          .ensureTableExists(interval.getTemporalTable()));
//      deferreds.add(tsdb.getClient()
//          .ensureTableExists(interval.getGroupbyTable()));
//    }
//    
//    try {
//      Deferred.group(deferreds).joinUninterruptibly();
//    } catch (DeferredGroupException e) {
//      throw new RuntimeException(e.getCause());
//    } catch (InterruptedException e) {
//      LOG.warn("Interrupted", e);
//      Thread.currentThread().interrupt();
//    } catch (Exception e) {
//      throw new RuntimeException("Unexpected exception", e);
//    }
  }
  
  /** @return an unmodifiable map of the rollups for printing and debugging */
  @JsonIgnore
  public Map<String, RollupInterval> getRollups() {
    return Collections.unmodifiableMap(forward_intervals);
  }
  
  /** @return The immutable list of rollup intervals for serialization. */
  public List<RollupInterval> getIntervals() {
    return Lists.newArrayList(forward_intervals.values());
  }
  
  @Override
  public Map<String, Integer> getAggregationIds() {
    return Collections.unmodifiableMap(aggregations_to_ids);
  }
  
  @Override
  public String getAggregatorForId(final int id) {
    return ids_to_aggregations.get(id);
  }
  
  @Override
  public int getIdForAggregator(final String aggregator) {
    if (Strings.isNullOrEmpty(aggregator)) {
      throw new IllegalArgumentException("Aggregator cannot be null or empty.");
    }
    Integer id = aggregations_to_ids.get(queryToRollupAggregation(aggregator));
    if (id == null) {
      throw new IllegalArgumentException("No ID found mapping to aggregator: " 
          + aggregator);
    }
    return id;
  }
  
  /**
   * Converts the old 2.x style qualifiers from {@code sum:<offset>} to 
   * the assigned ID. 
   * @param qualifier A non-null qualifier of at least 6 bytes.
   * @return The mapped ID if configured or IllegalArgumentException if 
   * the mapping wasn't found.
   * @throws IllegalArgumentException if the qualifier as null, less than
   * 6 bytes or the aggregation was not assigned an ID.
   * @throws IllegalDataException if the aggregation was unrecognized.
   */
  public int getIdForAggregator(final byte[] qualifier) {
    if (qualifier == null || qualifier.length < 6) {
      throw new IllegalArgumentException("Qualifier can't be null or "
          + "less than 6 bytes.");
    }
    switch (qualifier[0]) {
    case 'S':
    case 's':
      return getIdForAggregator("sum");
    case 'c':
    case 'C':
      return getIdForAggregator("count");
    case 'm':
    case 'M':
      if (qualifier[1] == 'A' || qualifier[1] == 'a') {
        return getIdForAggregator("max");
      } else {
        return getIdForAggregator("min");
      }
    }
    throw new IllegalDataException("Unrecognized qualifier: " 
        + Bytes.pretty(qualifier));
  }
  
  /**
   * Returns the index of the time offset in the qualifier given an
   * older 2.x style rollup of the form {@code sum:<offset>}.
   * @param qualifier A non-null qualifier of at least 6 bytes.
   * @return A 0 based index in the array when the offset begins.
   * @throws IllegalArgumentException if the qualifier as null or less 
   * than 6 bytes
   * @throws IllegalDataException if the aggregation was unrecognized.
   */
  public int getOffsetStartFromQualifier(final byte[] qualifier) {
    if (qualifier == null || qualifier.length < 6) {
      throw new IllegalArgumentException("Qualifier can't be null or "
          + "less than 6 bytes.");
    }
    switch (qualifier[0]) {
    case 'S':
    case 's':
    case 'M':
    case 'm':
      return 4;
    case 'C':
    case 'c':
      return 6;
    }
    throw new IllegalDataException("Unrecognized qualifier: " 
        + Bytes.pretty(qualifier));
  }
  
  @Override
  public String toString() {
    return JSON.serializeToString(this);
  }
  
  /**
   * Converts aggregations from a query to those that may be stored in
   * a database. E.g. if the query asks for `zimsum` we only store `sum`.
   * @param agg The non-null aggregate to convert.
   * @return A converted aggregate or the original aggregate (lowercased)
   * @throws NullPointerException if the agg was null.
   */
  public static String queryToRollupAggregation(String agg) {
    agg = agg.toLowerCase();
    if (agg.equals("zimsum")) {
      return "sum";
    }
    if (agg.equals("mimmax")) {
      return "max";
    }
    if (agg.equals("mimmin")) {
      return "min";
    }
    return agg;
  }
  
  public static Builder builder() {
    return new Builder();
  }
  
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "")
  public static class Builder {
    @JsonProperty
    private Map<String, Integer> aggregationIds;
    @JsonProperty
    private List<RollupInterval> intervals;
    
    public Builder setAggregationIds(final Map<String, Integer> aggregationIds) {
      this.aggregationIds = aggregationIds;
      return this;
    }
    
    @JsonIgnore
    public Builder addAggregationId(final String aggregation, final int id) {
      if (aggregationIds == null) {
        aggregationIds = Maps.newHashMapWithExpectedSize(1);
      }
      aggregationIds.put(aggregation, id);
      return this;
    }
    
    public Builder setIntervals(final List<RollupInterval> intervals) {
      this.intervals = intervals;
      return this;
    }
    
    @JsonIgnore
    public Builder addInterval(final RollupInterval interval) {
      if (intervals == null) {
        intervals = Lists.newArrayList();
      }
      intervals.add(interval);
      return this;
    }
    
    @JsonIgnore
    public Builder addInterval(final RollupInterval.Builder interval) {
      if (intervals == null) {
        intervals = Lists.newArrayList();
      }
      intervals.add(interval.build());
      return this;
    }
    
    public DefaultRollupConfig build() {
      return new DefaultRollupConfig(this);
    }
  }
}
