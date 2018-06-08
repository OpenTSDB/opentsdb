// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
package net.opentsdb.query.execution;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import io.opentracing.Span;
import net.opentsdb.core.Const;
import net.opentsdb.core.DefaultRegistry;
import net.opentsdb.data.iterators.DefaultIteratorGroups;
import net.opentsdb.exceptions.QueryExecutionCanceled;
import net.opentsdb.query.BaseQueryNodeConfig;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.context.QueryContext;
import net.opentsdb.query.execution.graph.ExecutionGraphNode;
import net.opentsdb.query.plan.QueryPlanner;
import net.opentsdb.query.plan.QueryPlannnerFactory;
import net.opentsdb.query.plan.TimeSlicedQueryPlanner;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.query.pojo.Timespan;
import net.opentsdb.stats.TsdbTrace;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.JSON;

/**
 * An executor that manages slicing and routing queries across multiple sources
 * over configurable time ranges. For example, if the time series storage system
 * has a 24h in-memory cache (e.g. Facebook's Beringei) and there's a long-term
 * storage layer, the router will send querys for the current 24h of data to the
 * cache and longer and/or older queries to the long-term store.
 * <p> 
 * Additionally the long-term layer can be sharded across tables or data stores.
 * For example, if a TSD runs out of UIDs and users need more, they can create
 * a new set of tables with a larger UID size. The router will then split queries
 * at the appropriate date and merge the results.
 * <p>
 * Configuration is performed via {@link TimeRange}s. Two types of sources are
 * allowed at this time:
 * <ul>
 * <li><b>Cache</b> - These are denoted by {@link TimeRange}s that only have 
 * their {@link TimeRange#end} values configured with a relative timestamp, e.g.
 * "24h-ago". This indicates the cache has rolling data from "now" until 24 hours
 * in the past. More than one cache *can* be configured but they MUST overlap,
 * e.g. if there is a "24h-ago" cache and a "48h-ago" cache, the 48 hour cache
 * MUST have all of the data from "now" until 48 hours ago. (Eventually we can 
 * try sharding).</li>
 * <li><b>TSDB</b> - These are standard long-term stores that hold data from 
 * "now" until the last {@link TimeRange#end} timestamp. {@link TimeRange#start}
 * and {@link TimeRange#end} times must be absolute timestamps, e.g. 
 * "2016/07/31-00:00:00". For the "now" slice, the start time can be null and 
 * the end some time in the past like "1979/01/01-00:00:00". If more than one
 * TSDB is configured, it's start and end times must be the same as adjacent
 * time ranges.</li> 
 * </ul>
 * 
 * <b>Notes:</b>
 * If the end time of a query is beyond the end time of the final cache or TSDB
 * then the query will always respond with an empty data set.
 * 
 * TODO - Eventually we could shard across cache and tsdbs, right now it's 
 * either or. Either the fully query must be served from cache or it's sharded
 * across the TSDs.
 * 
 * TODO - Need to implement fallback options if the cache is empty.
 *
 * @param <T> The type of data returned by the executor.
 * 
 * @since 3.0
 */
public class TimeBasedRoutingExecutor<T> extends QueryExecutor<T> {
  private static final Logger LOG = LoggerFactory.getLogger(
      TimeBasedRoutingExecutor.class);
  
  /** The map of time ranges to executors. */
  private Map<String, QueryExecutor<T>> executors;
  
  /** The sorted list of caches. */
  private List<TimeRange> caches;
  
  /** The sorted list of tsdbs. */
  private List<TimeRange> tsds;
  
  /**
   * Default ctor.
   * @param node A non-null node with a default configuration.
   */
  public TimeBasedRoutingExecutor(final ExecutionGraphNode node) {
    super(node);
//    if (node.getConfig() == null) {
//      throw new IllegalArgumentException("Default config cannot be null.");
//    }
//    if (node.graph() == null) {
//      throw new IllegalStateException("Execution graph cannot be null.");
//    }
//    if (Strings.isNullOrEmpty(((Config) node.getConfig()).getPlannerId())) {
//      throw new IllegalArgumentException("Default planner ID must be non-empty.");
//    }
//    executors = Maps.newHashMapWithExpectedSize(
//        ((Config) node.getConfig()).times.size());
//    for (final TimeRange range : ((Config) node.getConfig()).times) {
//      @SuppressWarnings("unchecked")
//      final QueryExecutor<T> executor = (QueryExecutor<T>) 
//          node.graph().getDownstreamExecutor(range.executorId);
//      if (executor == null) {
//        throw new IllegalStateException("No downstram executor found for ID " 
//            + range.executorId);
//      }
//      executors.put(range.executorId, executor);
//      registerDownstreamExecutor(executor);
//    }
//    
//    caches = ((Config) node.getConfig()).caches;
//    tsds = ((Config) node.getConfig()).tsds;
  }

  @Override
  public QueryExecution<T> executeQuery(final QueryContext context,
                                        final TimeSeriesQuery query, 
                                        final Span upstream_span) {
    final LocalExecution execution = 
        new LocalExecution(context, query, upstream_span);
    execution.execute();
    return execution;
  }

  /**
   * Method that searches the caches and tsdbs for the proper combination of
   * downstream executors to run the query against.
   * @param query A non-null query to pull the start and end times from.
   * @return A list of 1 or more time ranges with execution IDs to run against.
   * If no ranges would satisfy the query, the response is null.
   */
  List<TimeRange> getSources(final TimeSeriesQuery query) {
    if (caches != null) {
      long now = DateTime.currentTimeMillis();
      for (final TimeRange range : caches) {
        if (query.getTime().startTime().msEpoch() >= now - range.end_long) {
          return Lists.newArrayList(range);
        }
      }
    }
    
    if (tsds == null) {
      return null;
    }
    
    // TODO - check range inclusion
    final List<TimeRange> sources = Lists.newArrayListWithCapacity(1);
    for (final TimeRange range : tsds) {
      if (query.getTime().endTime().msEpoch() < range.end_long) {
        continue;
      }
      if (query.getTime().startTime().msEpoch() >= range.end_long) {
        sources.add(range);
        return sources;
      }
      if (query.getTime().startTime().msEpoch() < range.end_long) {
        sources.add(range);
      }
    }
    return sources;
  }
    
  /**
   * The local execution that computes the downstream executors to fire the 
   * queries against.
   */
  class LocalExecution extends QueryExecution<T> {
    
    /** The query context. */
    private final QueryContext context;
    
    /** The time ranges detected that we should use for the query. */
    private final List<TimeRange> ranges;
    
    /** Outstanding executions fired downstream. Used for cancelation. */
    private final QueryExecution<T>[] executions;
    
    /** The query planner used to merge tsd slices. */
    private TimeSlicedQueryPlanner<T> planner;
    
    /**
     * Default ctor.
     * @param context The non-null query context.
     * @param query The non-null query to parse.
     * @param upstream_span An optional upstream span for tracing.
     */
    @SuppressWarnings("unchecked")
    public LocalExecution(final QueryContext context,
                          final TimeSeriesQuery query,
                          final Span upstream_span) {
      super(query);
      this.context = context;
      outstanding_executions.add(this);
      
      if (context.getTracer() != null) {
        setSpan(context, 
            TimeBasedRoutingExecutor.this.getClass().getSimpleName(),
            upstream_span,
            TsdbTrace.addTags(
                "order", Integer.toString(query.getOrder()),
                "query", JSON.serializeToString(query),
                "startThread", Thread.currentThread().getName()));
      }
      
      ranges = getSources(query);
      executions = ranges != null ? new QueryExecution[ranges.size()] : null;
    }

    @SuppressWarnings("unchecked")
    void execute() {
      try {
        if (ranges == null || ranges.isEmpty()) {
          // ------------------------------------------------------------
          // no ranges matched, e.g. maybe there's a short ttl
          callback(new DefaultIteratorGroups(),
              TsdbTrace.successfulTags("downstreams", "null"));
          outstanding_executions.remove(LocalExecution.this);
          return;
        } else if (ranges.size() == 1) {
          // ------------------------------------------------------------
          // only querying a single host so foward and attach the callback
          final QueryExecutor<T> downstream = 
              executors.get(ranges.get(0).executorId);
          if (downstream == null) {
            final IllegalStateException ex = new IllegalStateException(
                "No executor found for executor ID: " 
                    + ranges.get(0).executorId);
            callback(ex,
              TsdbTrace.exceptionTags(ex),
              TsdbTrace.exceptionAnnotation(ex));
            outstanding_executions.remove(LocalExecution.this);
            return;
          }
          
          final QueryExecution<T> execution = 
              downstream.executeQuery(context, query, tracer_span);
          executions[0] = execution;
          
          /** The success callback that just passes results upstream and logs
           * the span. */
          class DSSuccessCB implements Callback<Object, T> {
            @Override
            public Object call(final T results) throws Exception {
              callback(results, 
                  TsdbTrace.successfulTags("downstreams", 
                      ranges.get(0).executorId));
              outstanding_executions.remove(LocalExecution.this);
              return null;
            }
          }
          
          /** The error callback returning the exception upstream and completing 
           * the span. */
          class DSErrorCB implements Callback<Object, Exception> {
            @Override
            public Object call(final Exception e) throws Exception {
              try {
                callback(e, 
                    TsdbTrace.exceptionTags(e),
                    TsdbTrace.exceptionAnnotation(e));
              } catch (IllegalStateException ex) { 
                // Safe to ignore as it's already been called.
              } catch (Exception ex) {
                LOG.error("Unexpected exception when handling exception for " 
                    + this, ex);
              }
              outstanding_executions.remove(LocalExecution.this);
              return null;
            }
          }
          
          execution.deferred()
            .addCallback(new DSSuccessCB())
            .addErrback(new DSErrorCB());
          return;
        }
        
        // ------------------------------------------------------------
        // fall through means we need to query 2 or more tsds.
        final QueryExecutorConfig override = 
            context.getConfigOverride(node.getId());
        final Config config;
//        if (override != null) {
//          config = (Config) override;
//        } else {
//          config = (Config) node.getConfig();
//        }
        
//        final String plan_id = Strings.isNullOrEmpty(config.getPlannerId()) ? 
//            ((Config) node.getConfig()).getPlannerId() : config.getPlannerId();
        final String plan_id = null;
        final QueryPlannnerFactory<?> plan_factory = ((DefaultRegistry) context.getTSDB().getRegistry())
            .getQueryPlanner(plan_id);
        if (plan_factory == null) {
          throw new IllegalArgumentException("No such query plan factory: " 
              );//+ config.getPlannerId());
        }
        final QueryPlanner<?> temp_planner = plan_factory.newPlanner(query);
        if (temp_planner == null) {
          throw new IllegalStateException("Plan factory returned a null planner: " 
              + plan_factory);
        }
        if (!(temp_planner instanceof TimeSlicedQueryPlanner)) {
          throw new IllegalStateException("Planner was of the wrong type: " 
              + temp_planner.getClass().getCanonicalName() + " while it must be "
                  + "a TimeSlicedQueryPlanner.");
        }
        planner = (TimeSlicedQueryPlanner<T>) temp_planner;
        
        int i = 0;
        final List<Deferred<T>> deferreds = Lists.newArrayListWithCapacity(ranges.size());
        for (final TimeRange range : ranges) {
          final Timespan.Builder builder = Timespan.newBuilder(query.getTime());
          if (query.getTime().endTime().msEpoch() <= range.start_long) {
            builder.setEnd(Long.toString(query.getTime().endTime().msEpoch()));
          } else {
            builder.setEnd(range.start);
          }
          
          if (query.getTime().startTime().msEpoch() > range.end_long) {
            builder.setStart(Long.toString(query.getTime().startTime().msEpoch()));
          } else {
            builder.setStart(range.end);
          }
          
          final QueryExecutor<T> executor = executors.get(range.executorId);
          if (executor == null) {
            // TODO - WTF!?!?!
          }
          
          executions[i] = executor.executeQuery(context, 
              TimeSeriesQuery.newBuilder(query)
                .setTime(builder)
                .build(), 
              tracer_span);
          deferreds.add(executions[i].deferred());
          // TODO - fail-fast and/or/failover
          i++;
        }
        
        Deferred.group(deferreds)
          .addCallback(new CompletedCB())
          .addErrback(new ErrorCB());
      } catch (Exception e) {
        try {
          callback(e, 
              TsdbTrace.exceptionTags(e),
              TsdbTrace.exceptionAnnotation(e));
        } catch (IllegalStateException ex) { 
          // Safe to ignore as it's already been called.
        } catch (Exception ex) {
          LOG.error("Unexpected exception when handling exception for " + this, ex);
        }
        outstanding_executions.remove(LocalExecution.this);
      }
    }
    
    /**
     * Overall callback used when we need to hit 2 or more executors. Merges
     * the results using the configured planner.
     */
    class CompletedCB implements Callback<Object, ArrayList<T>> {

      @Override
      public Object call(ArrayList<T> results) throws Exception {
        try {
          // Note: The merge expects series to be in ascending order but the
          // way we sort tsds is descending so we need to flip the order.
          Collections.reverse(results);
          final T merged = planner.mergeSlicedResults(results);
          
          final StringBuilder buf = new StringBuilder();
          for (int i = 0; i < ranges.size(); i++) {
            if (i > 0) {
              buf.append(",");
            }
            buf.append(ranges.get(i).executorId);
          }
          callback(merged,
              TsdbTrace.successfulTags("downstreams", buf.toString()));
        } catch (Exception e) {
          try {
            callback(e, 
                TsdbTrace.exceptionTags(e),
                TsdbTrace.exceptionAnnotation(e));
          } catch (IllegalStateException ex) { 
            // Safe to ignore as it's already been called.
          } catch (Exception ex) {
            LOG.error("Unexpected exception handling success callback for " 
                + this, ex);
          }
        } finally {
          outstanding_executions.remove(LocalExecution.this);
        }
        return null;
      }
      
    }
    
    /**
     * Error handler for the grouped downstreams.
     */
    class ErrorCB implements Callback<Object, Exception> {
      @Override
      public Object call(final Exception e) throws Exception {
        try {
          callback(e, 
              TsdbTrace.exceptionTags(e),
              TsdbTrace.exceptionAnnotation(e));
        } catch (IllegalStateException ex) { 
          // Safe to ignore as it's already been called.
        } catch (Exception ex) {
          LOG.error("Unexpected exception when handling exception for " + this, ex);
        }
        outstanding_executions.remove(LocalExecution.this);
        return null;
      }
    }
    
    @Override
    public void cancel() {
      if (!completed.get()) {
        try {
          final Exception e = new QueryExecutionCanceled(
              "Query was cancelled upstream: " + this, 400, query.getOrder());
          callback(e, TsdbTrace.canceledTags(e));
        } catch (IllegalArgumentException ex) {
          // lost race, no prob.
        } catch (Exception ex) {
          LOG.warn("Failed to complete callback due to unexepcted "
              + "exception: " + this, ex);
        }
      }
      outstanding_executions.remove(LocalExecution.this);
      
      if (executions != null) {
        for (final QueryExecution<T> execution : executions) {
          try {
            execution.cancel();
          } catch (Exception e) {
            LOG.warn("Failed canceling execution: " + execution, e);
          }
        }
      }
    }
    
  }
  
  /** The configuration class for this executor. */
  @JsonInclude(Include.NON_NULL)
  @JsonDeserialize(builder = Config.Builder.class)
  public static class Config extends BaseQueryNodeConfig {
    private List<TimeRange> times;
    private String planner_id;
    private List<TimeRange> caches;
    private List<TimeRange> tsds;
    
    protected Config(Builder builder) {
      super(builder);
      times = builder.times;
      planner_id = builder.plannerId;
      if (Strings.isNullOrEmpty(planner_id)) {
        throw new IllegalArgumentException("Cannot have an empty planner ID.");
      }
      if (times == null || times.isEmpty()) {
        throw new IllegalArgumentException(
            "Time settings cannot be null or empty.");
      }
      
      for (final TimeRange range : times) {
        if (range.is_relative) {
          if (caches == null) {
            caches = Lists.newArrayListWithCapacity(1);
          }
          caches.add(range);
        } else {
          if (tsds == null) {
            tsds = Lists.newArrayListWithCapacity(1);
          }
          tsds.add(range);
        }
      }
      
      if (caches != null) {
        Collections.sort(caches, new TimeRange.TimeSorting());
      }
      if (tsds != null) {
        Collections.sort(tsds, new TimeRange.TimeSorting());
      }
      // TODO - validation of overlaps
    }
    
    /** @return The list of time ranges. */
    public List<TimeRange> getTimes() {
      return Collections.unmodifiableList(times);
    }

    /** @return The planner ID. */
    public String getPlannerId() {
      return planner_id;
    }
    
    @Override
    public String getId() {
      // TODO Auto-generated method stub
      return null;
    }
    
    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final Config config = (Config) o;
      return Objects.equal(id, config.id)
          && Objects.equal(planner_id, config.planner_id)
          && Objects.equal(times, config.times);
    }

    @Override
    public int hashCode() {
      return buildHashCode().asInt();
    }

    @Override
    public HashCode buildHashCode() {
      final List<HashCode> hashes = Lists.newArrayListWithCapacity(
          times.size() + 1);
      hashes.add(Const.HASH_FUNCTION().newHasher()
        .putString(Strings.nullToEmpty(id), Const.UTF8_CHARSET)
        .putString(Strings.nullToEmpty(planner_id), Const.UTF8_CHARSET)
        .hash());
      for (final TimeRange range : times) {
        hashes.add(range.buildHashCode());
      }
      return Hashing.combineOrdered(hashes);
    }

    @Override
    public int compareTo(QueryNodeConfig config) {
      return ComparisonChain.start()
          .compare(id, config.getId(), Ordering.natural().nullsFirst())
          .compare(planner_id, ((Config) config).planner_id, 
              Ordering.natural().nullsFirst())
          .compare(times, ((Config) config).times,
              Ordering.<TimeRange>natural().lexicographical().nullsFirst())
          .result();
    }
    
    public static Builder newBuilder() {
      return new Builder();
    }
    
    public static class Builder extends BaseQueryNodeConfig.Builder {
      @JsonProperty
      private List<TimeRange> times;
      @JsonProperty
      private String plannerId = "IteratorGroupsSlicePlanner";
      
      /** @param times The non-null and non-empty list of time ranges. */
      public Builder setTimes(final List<TimeRange> times) {
        this.times = times;
        if (times != null) {
          Collections.sort(this.times);
        }
        return this;
      }
      
      /** @param plannerId The non-null and non-empty planner ID */ 
      public Builder setPlannerId(final String plannerId) {
        this.plannerId = plannerId;
        return this;
      }
      
      @Override
      public Config build() {
        return new Config(this);
      }
      
    }
    
  }
  
  /** A single time range object. It's validated on build. */
  @JsonInclude(Include.NON_NULL)
  @JsonDeserialize(builder = TimeRange.Builder.class)
  public static class TimeRange implements Comparable<TimeRange> {
    private String start;
    private String end;
    private String executorId;
    private boolean is_relative;
    private long start_long;
    private long end_long;
    
    protected TimeRange(final Builder builder) {
      start = Strings.isNullOrEmpty(builder.start) ? null : builder.start;
      end = Strings.isNullOrEmpty(builder.end) ? null : builder.end;
      if (Strings.isNullOrEmpty(builder.executorId)) {
        throw new IllegalArgumentException("Executor ID is missing.");
      }
      executorId = builder.executorId;
      if (start == null && end == null) {
        throw new IllegalArgumentException("Start and/or end must be provided.");
      }
      if (DateTime.isRelativeDate(end)) {
        end_long = DateTime.parseDuration(
            end.substring(0, end.length() - "-ago".length()));
        is_relative = true;
      } else {
        end_long = DateTime.parseDateTimeString(end, null);
      }
      
      if (start != null) {
        if (is_relative) {
          throw new IllegalArgumentException("Start time cannot be given in "
              + "relative ranges.");
        }
        start_long = DateTime.parseDateTimeString(start, null);
      } 
    }
    
    /** @return The start time as given by the user. May be null. */
    public String getStart() {
      return start;
    }
    
    /** @return The end time as given by the user. May be null. */
    public String getEnd() {
      return end;
    }
    
    /** @return The executor ID to use downstream. */
    public String getExecutorId() {
      return executorId;
    }
    
    /** Helper used for sorting caches vs tsdbs. */
    static class TimeSorting implements Comparator<TimeRange> {

      @Override
      public int compare(final TimeRange o1, final TimeRange o2) {
        long diff = o1.is_relative ?
            o1.end_long - o2.end_long :
            o2.end_long - o1.end_long;
          if (diff == 0) {
            return 0;
          } else if (diff < 1) {
            return -1;
          }
          return 1;
      }
      
    }
    
    @Override
    public int compareTo(final TimeRange o) {
      return ComparisonChain.start()
          .compare(executorId, o.executorId, Ordering.natural().nullsFirst())
          .compare(start, o.start, Ordering.natural().nullsFirst())
          .compare(end, o.end, Ordering.natural().nullsFirst())
          .result();
    }
    
    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final TimeRange range = (TimeRange) o;
      return Objects.equal(start, range.start)
          && Objects.equal(end, range.end)
          && Objects.equal(executorId, range.executorId);
    }

    @Override
    public int hashCode() {
      return buildHashCode().asInt();
    }
    
    public HashCode buildHashCode() {
      return Const.HASH_FUNCTION().newHasher()
          .putString(Strings.nullToEmpty(start), Const.UTF8_CHARSET)
          .putString(Strings.nullToEmpty(end), Const.UTF8_CHARSET)
          .putString(executorId, Const.UTF8_CHARSET)
          .hash();
    }
    
    public static Builder newBuilder() {
      return new Builder();
    }
    
    public static class Builder {
      @JsonProperty
      private String start;
      @JsonProperty
      private String end;
      @JsonProperty
      private String executorId;
      
      /** @param start The start time. May be null or an absolute timestamp. */
      public Builder setStart(final String start) {
        this.start = start;
        return this;
      }
      
      /** @param end The end time. May be null or an absolute or relative timestamp. */
      public Builder setEnd(final String end) {
        this.end = end;
        return this;
      }
      
      /** param executorId The name of the downstream executor. */
      public Builder setExecutorId(final String executorId) {
        this.executorId = executorId;
        return this;
      }
      
      public TimeRange build() {
        return new TimeRange(this);
      }
    }
    
  }
}
