// This file is part of OpenTSDB.
// Copyright (C) 2013-2017  The OpenTSDB Authors.
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
package net.opentsdb.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.base.Objects;

import net.opentsdb.query.pojo.Downsampler;
import net.opentsdb.query.pojo.DownsamplingSpecification;
import net.opentsdb.query.pojo.Filter;
import net.opentsdb.query.pojo.Metric;
import net.opentsdb.query.pojo.NumericFillPolicy;
import net.opentsdb.query.pojo.TagVFilter;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.query.pojo.Timespan;
import net.opentsdb.utils.DateTime;

/**
 * Parameters and state to query the underlying storage system for 
 * timeseries data points. When setting up a query, use the setter methods to
 * store user information such as the start time and list of queries. After 
 * setting the proper values, call the {@link #validateAndSetQuery()} method to
 * validate the request. If required information is missing or cannot be parsed
 * it will throw an exception. If validation passes, use 
 * {@code buildQueries(TSDB)} to compile the query into {@code Query} objects 
 * for processing. 
 * <b>Note:</b> If using POJO deserialization, make sure to avoid setting the 
 * {@code start_time} and {@code end_time} fields.
 * @since 2.0
 */
@JsonIgnoreProperties(ignoreUnknown = true)
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
  private List<TSSubQuery> queries;

  /** The parsed start time value 
   * <b>Do not set directly</b> */
  private long start_time;
  
  /** The parsed end time value 
   * <b>Do not set directly</b> */
  private long end_time;
  
  /** Whether or not the user wasn't millisecond resolution */
  private boolean ms_resolution;
  
  /** Whether or not to show the sub query with the results */
  private boolean show_query;
  
  /** Whether or not to include stats in the output */
  private boolean show_stats;
  
  /** Whether or not to include stats summary in the output */
  private boolean show_summary;
  
  /** Whether or not to delete the queried data */
  private boolean delete = false;
  
  /** A flag denoting whether or not to align intervals based on the calendar */
  private boolean use_calendar;
  
  /** Override default max byte limit */
  private boolean override_byte_limit;
  
  /** Override default max row count limit */
  private boolean override_data_point_limit;
  
  /**
   * Default constructor necessary for POJO de/serialization
   */
  public TSQuery() {
    
  }
  
  @Override
  public int hashCode() {
    // NOTE: Do not add any non-user submitted variables to the hash. We don't
    // want the hash to change after validation.
    // We also don't care about stats or summary
    return Objects.hashCode(start, end, timezone, use_calendar, options, padding, 
        no_annotations, with_global_annotations, show_tsuids, queries, 
        ms_resolution);
  }
  
  @Override
  public boolean equals(final Object obj) {
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof TSQuery)) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    
    // NOTE: Do not add any non-user submitted variables to the comparator. We 
    // don't want the value to change after validation.
    // We also don't care about stats or summary
    final TSQuery query = (TSQuery)obj;
    return Objects.equal(start, query.start)
        && Objects.equal(end, query.end)
        && Objects.equal(timezone, query.timezone)
        && Objects.equal(use_calendar,query.use_calendar)
        && Objects.equal(options, query.options)
        && Objects.equal(padding, query.padding)
        && Objects.equal(no_annotations, query.no_annotations)
        && Objects.equal(with_global_annotations, query.with_global_annotations)
        && Objects.equal(show_tsuids, query.show_tsuids)
        && Objects.equal(queries, query.queries)
        && Objects.equal(ms_resolution, query.ms_resolution);
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
          "End time [" + end_time + "] must be greater than the start time ["
          + start_time +"]");
    }
    
    if (queries == null || queries.isEmpty()) {
      throw new IllegalArgumentException("Missing queries");
    }
    
    // validate queries
    int i = 0;
    for (TSSubQuery sub : queries) {
      sub.validateAndSetQuery();
//      final DownsamplingSpecification ds = sub.downsamplingSpecification();
//      if (ds != null && timezone != null && !timezone.isEmpty() && 
//          ds != DownsamplingSpecification.NO_DOWNSAMPLER) {
//        final TimeZone tz = DateTime.timezones.get(timezone);
//        if (tz == null) {
//          throw new IllegalArgumentException(
//              "The timezone specification could not be found");
//        }
//        ds.setTimezone(tz);
//      }
//      if (ds != null && use_calendar && 
//          ds != DownsamplingSpecification.NO_DOWNSAMPLER) {
//        ds.setUseCalendar(true);
//      }
      
      sub.setIndex(i++);
    }
  }
  
  /**
   * Compiles the TSQuery into an array of Query objects for execution.
   * If the user has not set a down sampler explicitly, and they don't want 
   * millisecond resolution, then we set the down sampler to 1 second to handle
   * situations where storage may have multiple data points per second.
   * @param tsdb The tsdb to use for {@link TSDB#newQuery}
   * @return An array of queries
   */
//  public Query[] buildQueries(final TSDB tsdb) {
//    try {
//      return buildQueriesAsync(tsdb).joinUninterruptibly();
//    } catch (final Exception e) {
//      throw new RuntimeException("Unexpected exception", e);
//    }
//  }
  
  /**
   * Compiles the TSQuery into an array of Query objects for execution.
   * If the user has not set a down sampler explicitly, and they don't want 
   * millisecond resolution, then we set the down sampler to 1 second to handle
   * situations where storage may have multiple data points per second.
   * param tsdb The tsdb to use for {@code TSDB#newQuery}
   * @return A deferred array of queries to wait on for compilation.
   * @since 2.2
   */
//  public Deferred<Query[]> buildQueriesAsync(final TSDB tsdb) {
//    final Query[] tsdb_queries = new Query[queries.size()];
//    
//    final List<Deferred<Object>> deferreds =
//        new ArrayList<Deferred<Object>>(queries.size());
//    for (int i = 0; i < queries.size(); i++) {
//      final Query query = tsdb.newQuery();
//      deferreds.add(query.configureFromQuery(this, i));
//      tsdb_queries[i] = query;
//    }
//    
//    class GroupFinished implements Callback<Query[], ArrayList<Object>> {
//      @Override
//      public Query[] call(final ArrayList<Object> deferreds) {
//        return tsdb_queries;
//      }
//      @Override
//      public String toString() {
//        return "Query compile group callback";
//      }
//    }
//    
//    return Deferred.group(deferreds).addCallback(new GroupFinished());
//  }
  
  public String toString() {
    final StringBuilder buf = new StringBuilder();
    buf.append("TSQuery(start_time=")
      .append(start)
      .append(", end_time=")
      .append(end)
      .append(", subQueries[");
    if (queries != null && !queries.isEmpty()) {
      int counter = 0;
      for (TSSubQuery sub : queries) {
        if (counter > 0) {
          buf.append(", ");
        }
        buf.append(sub);
        counter++;
      }
    }
    buf.append("] padding=")
      .append(padding)
      .append(", no_annotations=")
      .append(no_annotations)
      .append(", with_global_annotations=")
      .append(with_global_annotations)
      .append(", show_tsuids=")
      .append(show_tsuids)
      .append(", ms_resolution=")
      .append(ms_resolution)
      .append(", options=[");
    if (options != null && !options.isEmpty()) {
      int counter = 0;
      for (Map.Entry<String, ArrayList<String>> entry : options.entrySet()) {
        if (counter > 0) {
          buf.append(", ");
        }
        buf.append(entry.getKey())
          .append("=[");
        final ArrayList<String> values = entry.getValue();
        for (int i = 0; i < values.size(); i++) {
          if (i > 0) {
            buf.append(", ");
          }
          buf.append(values.get(i));
        }
      }
    }
    buf.append("])");
    return buf.toString();
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
  
  /** @return whether or not to show the query with the results */
  public boolean getShowQuery() {
    return show_query;
  }
  
  /** @return whether or not to return stats per query */
  public boolean getShowStats() {
    return show_stats;
  }
  
  /** @return Whether or not to show the query summary */
  public boolean getShowSummary() {
    return this.show_summary;
  }
  
  /** @return Whether or not to delete the queried data @since 2.2 */
  public boolean getDelete() {
    return this.delete;
  }
  
  /** @return the flag denoting whether intervals should be aligned based on 
   * the calendar
   * @since 2.3 */
  public boolean getUseCalendar() {
    return use_calendar;
  }

  /**
   * Sets the start time for further parsing. This can be an absolute or 
   * relative value. See {@link DateTime#parseDateTimeString} for details.
   * @param start A start time from the user 
   */
  public void setStart(String start) {
    this.start = start;
  }

  /** 
   * Optionally sets the end time for all queries. If not set, the current 
   * system time will be used. This can be an absolute or relative value. See
   * {@link DateTime#parseDateTimeString} for details.
   * @param end An end time from the user
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
  public void setQueries(final List<TSSubQuery> queries) {
    this.queries = queries;
  }

  /** @param ms_resolution whether or not the user wants millisecond resolution */
  public void setMsResolution(boolean ms_resolution) {
    this.ms_resolution = ms_resolution;
  }

  /** @param show_query whether or not to show the query with the serialization */
  public void setShowQuery(boolean show_query) { 
    this.show_query = show_query;
  }
  
  /** @param show_stats whether or not to show stats in the serialization */
  public void setShowStats(boolean show_stats) { 
    this.show_stats = show_stats;
  }

  /** @param show_summary whether or not to show the query summary */
  public void setShowSummary(boolean show_summary) { 
    this.show_summary = show_summary;
  }

  /** @param delete whether or not to delete the queried data @since 2.2 */
  public void setDelete(boolean delete) {
    this.delete = delete;
  }
  
  /** @param use_calendar a flag denoting whether or not to align intervals 
   * based on the calendar @since 2.3 */
  public void setUseCalendar(boolean use_calendar) {
    this.use_calendar = use_calendar;
  }

  /**
   * Converts a TSQuery into a Time Series Query for further processing.
   * @param query A non-null and pre-validated TSQuery. 
   * @return A TimeSeriesQuery to pass downstream.
   */
  public static TimeSeriesQuery convertQuery(final TSQuery query) {
    final TimeSeriesQuery.Builder q = TimeSeriesQuery.newBuilder();
    
    final Timespan.Builder time = Timespan.newBuilder()
        .setStart(query.getStart())
        .setAggregator("none");
    if (query.getEnd() != null && !query.getEnd().isEmpty()) {
      time.setEnd(query.getEnd());
    }
    q.setTime(time);
    
    int metric_idx = 1;
    int filter_idx = 1;
    for (final TSSubQuery sub : query.getQueries()) {
      final Metric.Builder metric = Metric.newBuilder()
          .setId("m" + metric_idx++)
          .setMetric(sub.getMetric())
          .setAggregator(sub.getAggregator());
      if (sub.getDownsample() != null) {
        final DownsamplingSpecification spec = 
            new DownsamplingSpecification(sub.getDownsample());
        final Downsampler.Builder builder = Downsampler.newBuilder()
            .setAggregator(spec.getFunction().toString())
            .setInterval(spec.getStringInterval());
        if (spec.getFillPolicy() != null) {
          builder.setFillPolicy(NumericFillPolicy.newBuilder()
              .setPolicy(spec.getFillPolicy()));
        }
        metric.setDownsampler(builder);
      }
      if (sub.getRate()) {
        metric.setIsRate(true);
        metric.setRateOptions(sub.getRateOptions());
      }
      
      if (sub.getFilters() != null && !sub.getFilters().isEmpty()) {
        final String filter_id = "f" + filter_idx++;
        final Filter.Builder filter_set = Filter.newBuilder()
            .setId(filter_id)
            .setExplicitTags(sub.getExplicitTags());
        metric.setFilter(filter_id);
        
        for (final TagVFilter f : sub.getFilters()) {
          filter_set.addFilter(TagVFilter.newBuilder()
              .setTagk(f.getTagk())
              .setFilter(f.getFilter())
              .setType(f.getType())
              .setGroupBy(f.isGroupBy()));
        }
        filter_set.setExplicitTags(sub.getExplicitTags());
        q.addFilter(filter_set);
      }
      
      q.addMetric(metric);
    }
    
    return q.build();
  }

  /** @return Whether or not the query would like to override the byte limiter. */
  public boolean overrideByteLimit() {
    return override_byte_limit;
  }

  /** @param override_byte_limit Whether or not the query would like to override 
   * the byte limiter. */
  public void setOverrideByteLimit(boolean override_byte_limit) {
    this.override_byte_limit = override_byte_limit;
  }

  /** @return Whether or not the query would like to override the data point limit. */
  public boolean overrideDataPointLimit() {
    return override_data_point_limit;
  }

  /** @param override_data_point_limit Whether or not the query would like to 
   * override the data point limit. */
  public void setOverrideDataPointLimit(boolean override_data_point_limit) {
    this.override_data_point_limit = override_data_point_limit;
  }

}
