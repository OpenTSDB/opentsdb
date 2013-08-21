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

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * Bridging class that stores a normalized data point parsed from the "put" 
 * RPC methods and gets it ready for storage. Also has some helper methods that
 * were formerly in the Tags class for parsing values.
 * <p>
 * The data point value is a string in order to accept a wide range of values
 * including floating point and scientific. Before storage, the value will
 * be parsed to the appropriate numeric type.
 * <p>
 * Note the class is not marked as final since some serializers may want to
 * overload with their own fields or parsing methods.
 * @since 2.0
 */
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
public class IncomingDataPoint {
  /** The incoming metric name */
  private String metric;
  
  /** The incoming timestamp in Unix epoch seconds or milliseconds */
  private long timestamp;
  
  /** The incoming value as a string, we'll parse it to float or int later */
  private String value;
  
  /** A hash map of tag name/values */
  private HashMap<String, String> tags;
  
  /** TSUID for the data point */
  private String tsuid;
  
  /**
   * Empty constructor necessary for some de/serializers
   */
  public IncomingDataPoint() {
    
  }
  
  /**
   * Constructor used when working with a metric and tags
   * @param metric The metric name
   * @param timestamp The Unix epoch timestamp
   * @param value The value as a string
   * @param tags The tag name/value map
   */
  public IncomingDataPoint(final String metric,
      final long timestamp,
      final String value,
      final HashMap<String, String> tags) {
    this.metric = metric;
    this.timestamp = timestamp;
    this.value = value;
    this.tags = tags;
  }
  
  /**
   * Constructor used when working with tsuids
   * @param tsuid The TSUID
   * @param timestamp The Unix epoch timestamp
   * @param value The value as a string
   */
  public IncomingDataPoint(final String tsuid,
      final long timestamp,
      final String value) {
    this.tsuid = tsuid;
    this.timestamp = timestamp;
    this.value = value;
  }
  
  /**
   * @return information about this object
   */
  @Override
  public String toString() {
    final StringBuilder buf = new StringBuilder();
    buf.append("metric=").append(this.metric);
    buf.append(" ts=").append(this.timestamp);
    buf.append(" value=").append(this.value).append(" ");
    if (this.tags != null) {
      for (Map.Entry<String, String> entry : this.tags.entrySet()) {
        buf.append(entry.getKey()).append("=").append(entry.getValue());
      }
    }
    return buf.toString();
  }
  
  /** @return the metric */
  public final String getMetric() {
    return metric;
  }

  /** @return the timestamp */
  public final long getTimestamp() {
    return timestamp;
  }

  /** @return the value */
  public final String getValue() {
    return value;
  }

  /** @return the tags */
  public final HashMap<String, String> getTags() {
    return tags;
  }

  /** @return the TSUID */
  public final String getTSUID() {
    return tsuid;
  }
  
  /** @param metric the metric to set */
  public final void setMetric(String metric) {
    this.metric = metric;
  }

  /** @param timestamp the timestamp to set */
  public final void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  /** @param value the value to set */
  public final void setValue(String value) {
    this.value = value;
  }

  /** @param tags the tags to set */
  public final void setTags(HashMap<String, String> tags) {
    this.tags = tags;
  }

  /** @param tsuid the TSUID to set */
  public final void setTSUID(String tsuid) {
    this.tsuid = tsuid;
  }
}
