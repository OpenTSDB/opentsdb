// This file is part of OpenTSDB.
// Copyright (C) 2013-2015  The OpenTSDB Authors.
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
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class IncomingDataPoint {
  private static final Logger LOG = LoggerFactory.getLogger(IncomingDataPoint.class);
  
  /** The incoming metric name */
  protected String metric;
  
  /** The incoming timestamp in Unix epoch seconds or milliseconds */
  protected long timestamp;
  
  /** The incoming value as a string, we'll parse it to float or int later */
  protected String value;
  
  /** A hash map of tag name/values */
  protected Map<String, String> tags;
  
  /** TSUID for the data point */
  protected String tsuid;

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
      final Map<String, String> tags) {
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
  public final Map<String, String> getTags() {
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

  /**
   * Pre-validation of the various fields to make sure they're valid
   * @param details a map to hold detailed error message. If null then 
   * the errors will be logged
   * @return true if data point is valid, otherwise false
   */
  public boolean validate(final List<Map<String, Object>> details) {
    if (this.getMetric() == null || this.getMetric().isEmpty()) {
      if (details != null) {
        details.add(getHttpDetails("Metric name was empty"));
      }
      LOG.warn("Metric name was empty: " + this);
      return false;
    }

    //TODO add blacklisted metric validatin here too
    
    if (this.getTimestamp() <= 0) {
      if (details != null) {
        details.add(getHttpDetails("Invalid timestamp"));
      }
      LOG.warn("Invalid timestamp: " + this);
      return false;
    }

    if (this.getValue() == null || this.getValue().isEmpty()) {
      if (details != null) {
        details.add(getHttpDetails("Empty value"));
      }
      LOG.warn("Empty value: " + this);
      return false;
    }

    if (this.getTags() == null || this.getTags().size() < 1) {
      if (details != null) {
        details.add(getHttpDetails("Missing tags"));
      }
      LOG.warn("Missing tags: " + this);
      return false;
    }
    return true;
  }
  
  /**
   * Creates a map with an error message and this data point to return
   * to the HTTP put data point RPC handler
   * @param message The message to log
   * @return A map to append to the HTTP response
   */
  protected final Map<String, Object> getHttpDetails(final String message) {
    final Map<String, Object> map = new HashMap<String, Object>();
    map.put("error", message);
    map.put("datapoint", this);
    return map;
  }
}
