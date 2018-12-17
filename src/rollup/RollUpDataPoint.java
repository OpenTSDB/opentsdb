// This file is part of OpenTSDB.
// Copyright (C) 2015  The OpenTSDB Authors.
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

import com.fasterxml.jackson.annotation.JsonInclude;

import net.opentsdb.core.Aggregators;
import net.opentsdb.core.IncomingDataPoint;

import java.util.List;
import java.util.Map;

/**
 * Represents a single rolled up data point. It overrides the 
 * {@link IncomingDataPoint} class.
 * @since 2.4
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RollUpDataPoint extends IncomingDataPoint {
  
  /** The interval in the format <#><units> such as 1m or 2h */
  private String interval;
  
  /** The name of the aggregator that created this data point */
  private String aggregator;
  
  /** Optional aggregation function if this was a pre-aggregated data point. */
  protected String groupby_aggregator;
  
  /**
   * Default Ctor necessary for de/serialization
   */
  public RollUpDataPoint() {
  }

  @Override
  public String toString() {
    final StringBuilder buf = new StringBuilder();
    buf.append("metric=").append(metric)
       .append(" ts=").append(this.timestamp)
       .append(" value=").append(this.value).append(" ");
    if (this.tags != null) {
      for (Map.Entry<String, String> entry : this.tags.entrySet()) {
        buf.append(entry.getKey()).append("=").append(entry.getValue());
      }
    }
    buf.append(" groupByAggregator=")
       .append(groupby_aggregator)
       .append(" interval=").append(interval)
       .append(" aggregator=").append(aggregator);
    return buf.toString();
  }
  
  /** @return the interval as a string */
  public String getInterval() {
    return interval;
  }

  /** @param interval The interval for this data point such as "1m" */
  public void setInterval(final String interval) {
    this.interval = interval;
  }
  
  /** @return the name of the aggregator used to generate this data point */
  public String getAggregator() {
        return aggregator;
    }

  /** @param aggregator The name of the aggregator used to generate this dp */
  public void setAggregator(final String aggregator) {
    this.aggregator = aggregator;
  }

  /** @return If pre-aggregated, the function used. May be null. */
  public final String getGroupByAggregator() {
    return groupby_aggregator;
  }

  /** @param groupby_aggregator an optional aggregation function if the data 
   * point was pre-aggregated */
  public final void setGroupByAggregator(final String groupby_aggregator) {
    this.groupby_aggregator = groupby_aggregator;
  }
  
  @Override
  public boolean validate(final List<Map<String, Object>> details) {
    if (!super.validate(details))
      return false;
    
    boolean is_groupby = false;
    if (groupby_aggregator != null && !groupby_aggregator.isEmpty()) {
      // Don't need to perform this check here as the addAggregatePoint()
      // will handle that validation for us.
      //Aggregators.get(groupby_aggregator.toLowerCase());
      is_groupby = true;
    }
    
    // interval is only required if the the group by is NOT set
    if (interval == null || interval.isEmpty()) {
      if (!is_groupby) {
        if (details != null) {
          details.add(getHttpDetails("Missing interval"));
        }
        return false;
      }
    }

    if (aggregator == null || aggregator.isEmpty()) {
      if (!is_groupby) {
        // only error out if the groupby is false.
        if (details != null) {
          details.add(getHttpDetails("Missing aggregator"));
        }
        return false;
      }
      // Don't need to perform this check here as the addAggregatePoint()
      // will handle that validation for us.
      //Aggregators.get(aggregator);
    }

    return true;
  }
}
