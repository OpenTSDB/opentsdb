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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.opentsdb.utils.DateTime;

/**
 * Parameters and state to query the underlying storage system for 
 * timeseries data points. When setting up a query, use the setter methods to
 * store user information such as the start time and list of queries. After 
 * setting the proper values, call the {@link #validateAndSetQuery} method to
 * validate the request. If required information is missing or cannot be parsed
 * it will throw an exception. If validation passes, use {@link #buildQueries} 
 * to compile the query into {@link Query} objects for processing. 
 * <b>Note:</b> If using POJO deserialization, make sure to avoid setting the 
 * {@code start_time} and {@code end_time} fields.
 * @since 2.0
 */
public final class TSQuery {

  /** User given start date/time, could be relative or absolute */
  private String start;
  
  /** User given end date/time, could be relative, absolute or empty */
  private String end;
  
  /** User's timezone used for converting absolute human readable dates */
  private String timezone;
  
  /** Options for serializers, graphs, etc */
  private HashMap<String, ArrayList<String>> options;
  
  /** 
   * Whether or not to include padding, i.e. data to either side of the start/
   * end dates
   */
  private boolean padding;
  
  /** Whether or not to suppress annotation output */
  private boolean no_annotations;
  
  /** Whether or not to scan for global annotations in the same time range */
  private boolean with_global_annotations;

  /** Whether or not to show TSUIDs when returning data */
  private boolean show_tsuids;
  
  /** A list of parsed sub queries, must have one or more to fetch data */
  private ArrayList<TSSubQuery> queries;

  /** The parsed start time value 
   * <b>Do not set directly</b> */
  private long start_time;
  
  /** The parsed end time value 
   * <b>Do not set directly</b> */
  private long end_time;
  
  /** Whether or not the user wasn't millisecond resolution */
  private boolean ms_resolution;
  
  /**
   * Default constructor necessary for POJO de/serialization
   */
  public TSQuery() {
    
  }
  
  /**
   * Runs through query parameters to make sure it's a valid request.
   * This includes parsing relative timestamps, verifying that the end time is
   * later than the start time (or isn't set), that one or more metrics or
   * TSUIDs are present, etc. If no exceptions are thrown, the query is 
   * considered valid.
   * <b>Warning:</b> You must call this before passing it on for processing as
   * it sets the {@code start_time} and {@code end_time} fields as well as 
   * sets the {@link TSSubQuery} fields necessary for execution.
   * @throws IllegalArgumentException if something is wrong with the query
   */
  public void validateAndSetQuery() {
    if (start == null || start.isEmpty()) {
      throw new IllegalArgumentException("Missing start time");
    }
    start_time = DateTime.parseDateTimeString(start, timezone);
    
    if (end != null && !end.isEmpty()) {
      end_time = DateTime.parseDateTimeString(end, timezone);
    } else {
      end_time = System.currentTimeMillis();
    }
    if (end_time <= start_time) {
      throw new IllegalArgumentException(
          "End time must be greater than the start time");
    }
    
    if (queries == null || queries.isEmpty()) {
      throw new IllegalArgumentException("Missing queries");
    }
    
    // validate queries
    for (TSSubQuery sub : queries) {
      sub.validateAndSetQuery();
    }
  }
  
  /**
   * Compiles the TSQuery into an array of Query objects for execution.
   * If the user has not set a down sampler explicitly, and they don't want 
   * millisecond resolution, then we set the down sampler to 1 second to handle
   * situations where storage may have multiple data points per second.
   * @param tsdb The tsdb to use for {@link newQuery}
   * @return An array of queries
   */
  public Query[] buildQueries(final TSDB tsdb) {
    final Query[] queries = new Query[this.queries.size()];
    int i = 0;
    for (TSSubQuery sub : this.queries) {
      final Query query = tsdb.newQuery();
      query.setStartTime(start_time);
      query.setEndTime(end_time);
      if (sub.downsampler() != null) {
        query.downsample((int)sub.downsampleInterval(), sub.downsampler());
      } else if (!ms_resolution) {
        // we *may* have multiple millisecond data points in the set so we have
        // to downsample. use the sub query's aggregator
        query.downsample(1000, sub.aggregator());
      }
      if (sub.getTsuids() != null && !sub.getTsuids().isEmpty()) {
        query.setTimeSeries(sub.getTsuids(), sub.aggregator(), sub.getRate());
      } else {
        query.setTimeSeries(sub.getMetric(), sub.getTags(), sub.aggregator(), 
            sub.getRate());
      }
      queries[i] = query;
      i++;
    }
    return queries;
  }
  
  /** @return the parsed start time for all queries */
  public long startTime() {
    return this.start_time;
  }
  
  /** @return the parsed end time for all queries */
  public long endTime() {
    return this.end_time;
  }
  
  /** @return the user given, raw start time */
  public String getStart() {
    return start;
  }

  /** @return the user given, raw end time */
  public String getEnd() {
    return end;
  }

  /** @return the user supplied timezone */
  public String getTimezone() {
    return timezone;
  }

  /** @return a map of serializer options */
  public Map<String, ArrayList<String>> getOptions() {
    return options;
  }

  /** @return whether or not the user wants padding */
  public boolean getPadding() {
    return padding;
  }

  /** @return whether or not to supress annotatino output */
  public boolean getNoAnnotations() {
    return no_annotations;
  }
  
  /** @return whether or not to load global annotations for the time range */
  public boolean getGlobalAnnotations() {
    return with_global_annotations;
  }
  
  /** @return whether or not to display TSUIDs with the results */
  public boolean getShowTSUIDs() {
    return show_tsuids;
  }
  
  /** @return the list of sub queries */
  public List<TSSubQuery> getQueries() {
    return queries;
  }

  /** @return whether or not the requestor wants millisecond resolution */
  public boolean getMsResolution() {
    return ms_resolution;
  }
  
  /**
   * Sets the start time for further parsing. This can be an absolute or 
   * relative value. See {@link DateTime#parseDateTimeString} for details.
   * @param a start time from the user 
   */
  public void setStart(String start) {
    this.start = start;
  }

  /** 
   * Optionally sets the end time for all queries. If not set, the current 
   * system time will be used. This can be an absolute or relative value. See
   * {@link DateTime#parseDateTimeString} for details.
   * @param an end time from the user
   */
  public void setEnd(String end) {
    this.end = end;
  }

  /** @param timezone an optional timezone for date parsing */
  public void setTimezone(String timezone) {
    this.timezone = timezone;
  }

  /** @param options a map of options to pass on to the serializer */
  public void setOptions(HashMap<String, ArrayList<String>> options) {
    this.options = options;
  }

  /** @param padding whether or not the query should include padding */
  public void setPadding(boolean padding) {
    this.padding = padding;
  }

  /** @param no_annotations whether or not to suppress annotation output */
  public void setNoAnnotations(boolean no_annotations) {
    this.no_annotations = no_annotations;
  }
  
  /** @param with_global whether or not to load global annotations */
  public void setGlobalAnnotations(boolean with_global) {
    with_global_annotations = with_global;
  }
  
  /** @param show_tsuids whether or not to show TSUIDs in output */
  public void setShowTSUIDs(boolean show_tsuids) {
    this.show_tsuids = show_tsuids;
  }
  
  /** @param queries a list of {@link TSSubQuery} objects to store*/
  public void setQueries(ArrayList<TSSubQuery> queries) {
    this.queries = queries;
  }

  /** @param ms_resolution whether or not the user wants millisecond resolution */
  public void setMsResolution(boolean ms_resolution) {
    this.ms_resolution = ms_resolution;
  }
}
