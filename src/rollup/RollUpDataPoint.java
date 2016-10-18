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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Represents a single rolled up data point. It overrides the 
 * {@link IncomingDataPoint} class.
 * @since 2.4
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RollUpDataPoint extends IncomingDataPoint {
  private static final Logger LOG = LoggerFactory.getLogger(RollUpDataPoint.class);
  
  /** The interval in the format <#><units> such as 1m or 2h */
  private String interval;
  
  /** The name of the aggregator that created this data point */
  private String aggregator;

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
    buf.append(" interval=").append(interval)
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

  @Override
  public boolean validate(final List<Map<String, Object>> details) {
    if (!super.validate(details))
      return false;

    if (this.getInterval() == null || this.getInterval().isEmpty()) {
      if (details != null) {
        details.add(getHttpDetails("Missing interval"));
      }
      LOG.warn("Missing interval: " + this);
      return false;
    }

    if (this.getAggregator() == null || this.getAggregator().isEmpty()) {
      if (details != null) {
        details.add(getHttpDetails("Missing aggregator"));
      }
      LOG.warn("Missing aggregator: " + this);
      return false;
    }

    if (!Aggregators.set().contains(this.getAggregator().toLowerCase())) {
      if (details != null) {
        details.add(getHttpDetails("Invalid aggregator"));
      }
      LOG.warn("Invalid aggregator: " + this);
      return false;
    }

    return true;
  }
}
