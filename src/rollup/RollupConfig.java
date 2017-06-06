// This file is part of OpenTSDB.
// Copyright (C) 2015-2017  The OpenTSDB Authors.
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
package net.opentsdb.rollup;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import net.opentsdb.core.Aggregators;
import net.opentsdb.core.TSDB;
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
import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.DeferredGroupException;
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
@JsonDeserialize(builder = RollupConfig.Builder.class)
public class RollupConfig {
  private static final Logger LOG = LoggerFactory.getLogger(RollupConfig.class);
  
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
  protected RollupConfig(final Builder builder) {
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
      if (Aggregators.get(agg) == null) {
        throw new IllegalArgumentException("No such aggregator found for " + agg);
      }
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
  public List<RollupInterval> getRollupInterval(final long interval, 
                                                final String str_interval) {
    
    if (interval <= 0) {
      throw new IllegalArgumentException("Interval cannot be null or empty");
    }
    
    final Map<Long, RollupInterval> rollups = 
        new TreeMap<Long, RollupInterval>(Collections.reverseOrder());
    boolean right_match = false;
    
    for (RollupInterval rollup: forward_intervals.values()) {
      if (rollup.getIntervalSeconds() == interval) {
        rollups.put((long) rollup.getIntervalSeconds(), rollup);
        right_match = true;
      }
      else if (interval % rollup.getIntervalSeconds() == 0) {
        rollups.put((long) rollup.getIntervalSeconds(), rollup);
      }
    }

    if (rollups.isEmpty()) {
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
    final List<Deferred<Object>> deferreds = 
        new ArrayList<Deferred<Object>>(forward_intervals.size() * 2);
    
    for (RollupInterval interval : forward_intervals.values()) {
      deferreds.add(tsdb.getClient()
          .ensureTableExists(interval.getTemporalTable()));
      deferreds.add(tsdb.getClient()
          .ensureTableExists(interval.getGroupbyTable()));
    }
    
    try {
      Deferred.group(deferreds).joinUninterruptibly();
    } catch (DeferredGroupException e) {
      throw new RuntimeException(e.getCause());
    } catch (InterruptedException e) {
      LOG.warn("Interrupted", e);
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      throw new RuntimeException("Unexpected exception", e);
    }
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
  
  /** @return The immutable map of aggregations to IDs for serialization. */
  public Map<String, Integer> getAggregationIds() {
    return Collections.unmodifiableMap(aggregations_to_ids);
  }
  
  /**
   * @param id The ID of an aggregator to search for.
   * @return The aggregator if found, null if it was not mapped.
   */
  public String getAggregatorForId(final int id) {
    return ids_to_aggregations.get(id);
  }
  
  /**
   * @param aggregator The non-null and non-empty aggregator to search for.
   * @return The ID of the aggregator if found.
   * @throws IllegalArgumentException if the aggregator was not found or if the
   * aggregator was null or empty.
   */
  public int getIdForAggregator(final String aggregator) {
    if (Strings.isNullOrEmpty(aggregator)) {
      throw new IllegalArgumentException("Aggregator cannot be null or empty.");
    }
    Integer id = aggregations_to_ids.get(aggregator.toLowerCase());
    if (id == null) {
      throw new IllegalArgumentException("No ID found mapping to aggregator " 
          + aggregator);
    }
    return id;
  }
  
  @Override
  public String toString() {
    return JSON.serializeToString(this);
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
    
    public RollupConfig build() {
      return new RollupConfig(this);
    }
  }
}
