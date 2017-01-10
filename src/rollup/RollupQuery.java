// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Objects;

import net.opentsdb.core.Aggregator;
import net.opentsdb.core.Aggregators;

/**
 * Holds information about a rollup interval and rollup aggregator.
 * Every RollupQuery object will have a valid RollupInterval and Aggregator
 */
public class RollupQuery {

  // TEMP
  public static final byte[] SUM = new byte[] { 's', 'u', 'm' };
  public static final byte[] COUNT = new byte[] { 'c', 'o', 'u', 'n', 't' };
  
  private final RollupInterval rollup_interval;
  private final Aggregator rollup_agg;
  /** Rollup aggregate prefix along with the delimiter as byte array. It will be
   * the same for the same rollup aggregator, but is defined here to 
   * reduce the number of calculations at scan time*/
  private final byte[] agg_prefix;
  
  /** Initial downsampling interval form the user, will be used to 
   * downsample the lower sampling rate, if data is not available for the 
   * requested sampling rate. It is in milliseconds*/
  private final long sample_interval_ms;

  /**
   * Default private constructor
   * @param rollup_interval RollupInterval object
   * @param rollup_agg Aggregator object
   * @param sample_interval_ms Initial downsaple interval in milliseconds
   * @throws IllegalStateException if rollup interval or rollup aggregator is
   *   null
   */
  public RollupQuery(final RollupInterval rollup_interval, 
      final Aggregator rollup_agg, long sample_interval_ms) {
    
    if (rollup_interval == null) {
      throw new IllegalStateException("Rollup interval is null");
    }
    
    if (rollup_agg == null) {
      throw new IllegalStateException("Rollup aggregator is null");
    }
    
    this.rollup_interval = rollup_interval;
    // we need to convert zimsum => sum, mimmax => max, mimmin => min so that
    // we match properly on the column names
    if (rollup_agg == Aggregators.ZIMSUM) {
      this.rollup_agg = Aggregators.SUM;
    } else if (rollup_agg == Aggregators.MIMMAX) {
      this.rollup_agg = Aggregators.MAX;
    } else if (rollup_agg == Aggregators.MIMMIN) {
      this.rollup_agg = Aggregators.MIN;
    } else {
      this.rollup_agg = rollup_agg;
    }
    this.agg_prefix = RollupUtils.getRollupQualifierPrefix(this.rollup_agg.toString());
    this.sample_interval_ms = sample_interval_ms;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(rollup_interval, rollup_agg.toString());
  }
  
  @Override
  public boolean equals(final Object obj) {
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof RollupQuery)) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    final RollupQuery query = (RollupQuery)obj;
    return Objects.equal(rollup_agg, query.rollup_agg) 
        && rollup_interval.equals(query.rollup_interval);
  }
  
  @Override
  public String toString() {
    final StringBuilder buf = new StringBuilder();
    buf.append("rollup interval=")
        .append(rollup_interval.getStringInterval())
        .append(", rollup aggregator=")
        .append(rollup_agg.toString());
    return buf.toString();
  }

  /**
   * @return the count of intervals in this span
   */
  public RollupInterval getRollupInterval() {
    return rollup_interval;
  }

  /**
   * @return the rollup aggregator
   */
  @JsonIgnore
  public Aggregator getRollupAgg() {
    return rollup_agg;
  }
  /**
   * Does it contain a valid rollup interval, mainly says it is not the default
   * rollup. Default rollup is of same resolution as raw data. So if true, 
   * which means the raw cell column qualifier is encoded with the aggregate 
   * function and the cell is not appended or compacted 
   * @param rollup_query related RollupQuery object, null if rollup is disabled
   * @return true if it is rollup query
   */
  public static boolean isValidQuery(final RollupQuery rollup_query) {
    return (rollup_query != null && rollup_query.rollup_interval != null &&
      !rollup_query.rollup_interval.isDefaultRollupInterval());
  }
  
  /**
   * Rollup aggregate prefix
   * @return aggregate prefix along with the delimiter as byte array 
   */
  public byte[] getRollupAggPrefix() {
    return agg_prefix;
  }

  /** @return The sample interval in milliseconds. */
  public long getSampleIntervalInMS() {
    return sample_interval_ms;
  }
  
  /**
   * Tells whether the current rollup query sampling rate is lower than the
   * initial request
   * 
   * @return true if it is of lower sampling rate else false
   */
  public boolean isLowerSamplingRate() {
    return this.rollup_interval.getInterval() * 1000 < sample_interval_ms;
  }
}
