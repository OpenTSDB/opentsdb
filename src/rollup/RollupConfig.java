// This file is part of OpenTSDB.
// Copyright (C) 2010-2015  The OpenTSDB Authors.
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.opentsdb.core.TSDB;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.DeferredGroupException;
import java.util.TreeMap;

/**
 * A hard-coded rollup configuration class that stores the lookup map, config
 * and other bits surrounding rollups and pre-aggregates.
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
 *        'm' = month
 *        'y' = year
 * @since 2.4
 */
public class RollupConfig {
  private static final Logger LOG = LoggerFactory.getLogger(RollupConfig.class);

  /** The interval to interval map where keys are things like "10m" or "1d"*/
  final Map<String, RollupInterval> forward_intervals = 
      new HashMap<String, RollupInterval>();
  
  /** The table name to interval map for queries */
  final Map<String, RollupInterval> reverse_intervals = 
      new HashMap<String, RollupInterval>();
  
  /**
   * Ctor that contains the hard coded intervals. 
   * TODO - now that we're not writing to a single table, we can load
   * this from a config file
   */
  public RollupConfig() {
    final List<RollupInterval> config = new ArrayList<RollupInterval>();
    
    /** ---------------- CONFIG ---------------------
     * WARNING: Do NOT change these maps after you start pushing data or you 
     * will invalidate anything you've written to the database. You can always
     * add intervals and delete them to stop accepting data, but never remove
     */
    config.add(new RollupInterval("tsdb", 
        "tsdb-agg", "1m", "1h", true));
    config.add(new RollupInterval("tsdb-rollup-1h", 
        "tsdb-agg-rollup-1h", "1h", "1d"));
    
    // don't remove this
    validateAndCompileIntervals(config);
  }
  
  /**
   * Ctor for unit testing or loading intervals from an alternate source
   * @param config The list of rollup intervals to store in the config
   */
  public RollupConfig(final List<RollupInterval> config) {
    validateAndCompileIntervals(config);
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
    
    Map<Long, RollupInterval> rollups = new TreeMap<Long, RollupInterval>(Collections.reverseOrder());
    boolean right_match = false;
    
    for (RollupInterval rollup: forward_intervals.values()) {
      if (rollup.getInterval() == interval) {
        rollups.put(new Long(rollup.getInterval()), rollup);
        right_match = true;
      }
      else if (interval % rollup.getInterval() == 0) {
        rollups.put(new Long(rollup.getInterval()), rollup);
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
                      getStringInterval());
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
  public Map<String, RollupInterval> getRollups() {
    return Collections.unmodifiableMap(forward_intervals);
  }
  
  /**
   * Determines if the config supplied in the ctor is valid. This will throw
   * exceptions if:
   * 1) One of the strings is bad when passed to {@link getIntervals} above
   * 2) A table name is missing
   * 3) If more than one interval (e.g. "1m") is configured. These must
   *    be unique.
   * @param config The list of RollupIntervals to process
   * @throws IllegalArgumentException if something is invalid
   */
  void validateAndCompileIntervals(final List<RollupInterval> config) {
    if (config.isEmpty()) {
      LOG.info("No intervals configured for this TSD");
      return;
    }
    
    for (final RollupInterval config_interval : config) {

      if (forward_intervals.containsKey(config_interval.getStringInterval())) {
        throw new IllegalArgumentException(
            "Only one interval of each type can be configured: " + 
            config_interval);
      }
      
      forward_intervals.put(config_interval.getStringInterval(), config_interval);
      reverse_intervals.put(config_interval.getTemporalTableName(), config_interval);
      reverse_intervals.put(config_interval.getGroupbyTableName(), config_interval);
      LOG.debug("Configured rollup: " + config_interval);
    }
    
    LOG.info("Configured [" + forward_intervals.size() + "] rollup intervals");
  }

}
