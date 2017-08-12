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
package net.opentsdb.core;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.hbase.async.BinaryPrefixComparator;
import org.hbase.async.Bytes;
import org.hbase.async.CompareFilter;
import org.hbase.async.FilterList;
import org.hbase.async.HBaseException;
import org.hbase.async.QualifierFilter;
import org.hbase.async.ScanFilter;
import org.hbase.async.Scanner;
import org.hbase.async.Bytes.ByteMap;
import org.hbase.async.FilterList.Operator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.DeferredGroupException;

import net.opentsdb.query.QueryUtil;
import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.query.filter.TagVLiteralOrFilter;
import net.opentsdb.rollup.NoSuchRollupForIntervalException;
import net.opentsdb.rollup.RollupInterval;
import net.opentsdb.rollup.RollupQuery;
import net.opentsdb.rollup.RollupUtils;
import net.opentsdb.stats.Histogram;
import net.opentsdb.stats.QueryStats;
import net.opentsdb.stats.QueryStats.QueryStat;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.ByteSet;
import net.opentsdb.utils.DateTime;

/**
 * Non-synchronized implementation of {@link Query}.
 */
final class TsdbQuery implements Query {

  private static final Logger LOG = LoggerFactory.getLogger(TsdbQuery.class);

  /** Used whenever there are no results. */
  private static final DataPoints[] NO_RESULT = new DataPoints[0];

  /**
   * Keep track of the latency we perceive when doing Scans on HBase.
   * We want buckets up to 16s, with 2 ms interval between each bucket up to
   * 100 ms after we which we switch to exponential buckets.
   */
  static final Histogram scanlatency = new Histogram(16000, (short) 2, 100);

  /**
   * Charset to use with our server-side row-filter.
   * We use this one because it preserves every possible byte unchanged.
   */
  private static final Charset CHARSET = Charset.forName("ISO-8859-1");

  /** The TSDB we belong to. */
  private final TSDB tsdb;
  
  /** The time, in ns, when we start scanning for data **/
  private long scan_start_time;
  
  /** Whether or not the query has any results. */
  private Boolean no_results;

  /** Value used for timestamps that are uninitialized.  */
  private static final int UNSET = -1;

  /** Start time (UNIX timestamp in seconds) on 32 bits ("unsigned" int). */
  private long start_time = UNSET;

  /** End time (UNIX timestamp in seconds) on 32 bits ("unsigned" int). */
  private long end_time = UNSET;
  
  /** Whether or not to delete the queried data */
  private boolean delete;

  /** ID of the metric being looked up. */
  private byte[] metric;
  
  /** Row key regex to pass to HBase if we have tags or TSUIDs */
  private String regex;
  
  /** Whether or not to enable the fuzzy row filter for Hbase */
  private boolean enable_fuzzy_filter;
  
  /** Whether or not the user wants to use the fuzzy filter */
  private boolean override_fuzzy_filter;
  
  /**
   * Tags by which we must group the results.
   * Each element is a tag ID.
   * Invariant: an element cannot be both in this array and in {@code tags}.
   */
  private ArrayList<byte[]> group_bys;

  /**
   * Tag key and values to use in the row key filter, all pre-sorted
   */
  private ByteMap<byte[][]> row_key_literals;
  private List<ByteMap<byte[][]>> row_key_literals_list;

  /** If true, use rate of change instead of actual values. */
  private boolean rate;

  /** Specifies the various options for rate calculations */
  private RateOptions rate_options;
  
  /** Aggregator function to use. */
  private Aggregator aggregator;

  /** Downsampling specification to use, if any (can be {@code null}). */
  private DownsamplingSpecification downsampler;

  /** Rollup interval and aggregator, null if not applicable. */
  private RollupQuery rollup_query;
  
  /** Map of RollupInterval objects in the order of next best match
   * like 1d, 1h, 10m, 1m, for rollup of 1d. */
  private List<RollupInterval> best_match_rollups;
  
  /** How to use the rollup data */
  private ROLLUP_USAGE rollup_usage = ROLLUP_USAGE.ROLLUP_NOFALLBACK;
  
  /** Search the query on pre-aggregated table directly instead of post fetch 
   * aggregation. */
  private boolean pre_aggregate;
  
  /** Optional list of TSUIDs to fetch and aggregate instead of a metric */
  private List<String> tsuids;
  
  /** An index that links this query to the original sub query */
  private int query_index;
  
  /** Tag value filters to apply post scan */
  private List<TagVFilter> filters;
  
  /** An object for storing stats in regarding the query. May be null */
  private QueryStats query_stats;
  
  /** Whether or not to match series with ONLY the given tags */
  private boolean explicit_tags;
  
  private List<Float> percentiles;
  
  private boolean show_histogram_buckets;
  
  /** Set at filter resolution time to determine if we can use multi-gets */
  private boolean use_multi_gets;

  /** Set by the user if they want to bypass multi-gets */
  private boolean override_multi_get;
  
  /** Whether or not to use the search plugin for multi-get resolution. */
  private boolean multiget_with_search;
  
  /** Whether or not to fall back on query failure. */
  private boolean search_query_failure;
  
  /** The maximum number of bytes allowed per query. */
  private long max_bytes = 0;
  
  /** The maximum number of data points allowed per query. */
  private long max_data_points = 0;
  
  /**
   * Enum for rollup fallback control.
   * @since 2.4
   */
  public static enum ROLLUP_USAGE {
    ROLLUP_RAW, //Don't use rollup data, instead use raw data
    ROLLUP_NOFALLBACK, //Use rollup data, and don't fallback on no data
    ROLLUP_FALLBACK, //Use rollup data and fallback to next best match on data
    ROLLUP_FALLBACK_RAW; //Use rollup data and fallback to raw on no data
    
    /**
     * Parse and transform a string to ROLLUP_USAGE object
     * @param str String to be parsed
     * @return enum param tells how to use the rollup data
     */
    public static ROLLUP_USAGE parse(String str) {
      ROLLUP_USAGE def = ROLLUP_NOFALLBACK;
      
      if (str != null) {
        try {
          def = ROLLUP_USAGE.valueOf(str.toUpperCase());
        }
        catch(IllegalArgumentException ex) {
          LOG.warn("Unknown rollup usage, " + str + ", use default usage - which"
                  + "uses raw data but don't fallback on no data");
        }
      }
      
      return def;
    }
    
    /**
     * Whether to fallback to next best match or raw
     * @return true means fall back else false
     */
    public boolean fallback() {
      return this == ROLLUP_FALLBACK || this == ROLLUP_FALLBACK_RAW;
    }
  }
  
  /** Constructor. */
  public TsdbQuery(final TSDB tsdb) {
    this.tsdb = tsdb;
    this.downsampler = DownsamplingSpecification.NO_DOWNSAMPLER;
    enable_fuzzy_filter = tsdb.getConfig()
        .getBoolean("tsd.query.enable_fuzzy_filter");
    use_multi_gets = tsdb.getConfig().getBoolean("tsd.query.multi_get.enable");
  }

  /** Which rollup table it scanned to get the final result.
   * @since 2.4 */
  public String getRollupTable() {
    if (RollupQuery.isValidQuery(rollup_query)) {
      return rollup_query.getRollupInterval().getInterval();
    }
    else {
      return "raw";
    }
  }
  
  /** Search the query on pre-aggregated table directly instead of post fetch 
   * aggregation. 
   * @since 2.4 
   */
  public boolean isPreAggregate() {
      return this.pre_aggregate;
  }
  
  /**
   * Sets the start time for the query
   * @param timestamp Unix epoch timestamp in seconds or milliseconds
   * @throws IllegalArgumentException if the timestamp is invalid or greater
   * than the end time (if set)
   */
  @Override
  public void setStartTime(final long timestamp) {
    if (timestamp < 0 || ((timestamp & Const.SECOND_MASK) != 0 && 
        timestamp > 9999999999999L)) {
      throw new IllegalArgumentException("Invalid timestamp: " + timestamp);
    } else if (end_time != UNSET && timestamp >= getEndTime()) {
      throw new IllegalArgumentException("new start time (" + timestamp
          + ") is greater than or equal to end time: " + getEndTime());
    }
    start_time = timestamp;
  }

  /**
   * @return the start time for the query
   * @throws IllegalStateException if the start time hasn't been set yet
   */
  @Override
  public long getStartTime() {
    if (start_time == UNSET) {
      throw new IllegalStateException("setStartTime was never called!");
    }
    return start_time;
  }

  /**
   * Sets the end time for the query. If this isn't set, the system time will be
   * used when the query is executed or {@link #getEndTime} is called
   * @param timestamp Unix epoch timestamp in seconds or milliseconds
   * @throws IllegalArgumentException if the timestamp is invalid or less
   * than the start time (if set)
   */
  @Override
  public void setEndTime(final long timestamp) {
    if (timestamp < 0 || ((timestamp & Const.SECOND_MASK) != 0 && 
        timestamp > 9999999999999L)) {
      throw new IllegalArgumentException("Invalid timestamp: " + timestamp);
    } else if (start_time != UNSET && timestamp <= getStartTime()) {
      throw new IllegalArgumentException("new end time (" + timestamp
          + ") is less than or equal to start time: " + getStartTime());
    }
    end_time = timestamp;
  }

  /** @return the configured end time. If the end time hasn't been set, the
   * current system time will be stored and returned.
   */
  @Override
  public long getEndTime() {
    if (end_time == UNSET) {
      setEndTime(DateTime.currentTimeMillis());
    }
    return end_time;
  }
  
  @Override
  public void setDelete(boolean delete) {
    this.delete = delete;
  }
  
  @Override
  public boolean getDelete() {
    return delete;
  }
  
  @Override
  public void setPercentiles(List<Float> percentiles) {
    this.percentiles = percentiles;
  }
  
  @Override
  public void setTimeSeries(final String metric,
      final Map<String, String> tags,
      final Aggregator function,
      final boolean rate) throws NoSuchUniqueName {
    setTimeSeries(metric, tags, function, rate, new RateOptions());
  }
  
  @Override
  public void setTimeSeries(final String metric,
        final Map<String, String> tags,
        final Aggregator function,
        final boolean rate,
        final RateOptions rate_options)
  throws NoSuchUniqueName {
    if (filters == null) {
      filters = new ArrayList<TagVFilter>(tags.size());
    }
    TagVFilter.tagsToFilters(tags, filters);
    
    try {
      for (final TagVFilter filter : this.filters) {
        filter.resolveTagkName(tsdb).join();
      }
    } catch (final InterruptedException e) {
      LOG.warn("Interrupted", e);
      Thread.currentThread().interrupt();
    } catch (final NoSuchUniqueName e) {
      throw e;
    } catch (final Exception e) {
      if (e instanceof DeferredGroupException) {
        // rollback to the actual case. The DGE missdirects
        Throwable ex = e.getCause();
        while(ex != null && ex instanceof DeferredGroupException) {
          ex = ex.getCause();
        }
        if (ex != null) {
          throw (RuntimeException)ex;
        }
      }
      LOG.error("Unexpected exception processing group bys", e);
      throw new RuntimeException(e);
    }
    
    findGroupBys();
    this.metric = tsdb.metrics.getId(metric);
    aggregator = function;
    this.rate = rate;
    this.rate_options = rate_options;
  }

  @Override
  public void setTimeSeries(final List<String> tsuids,
      final Aggregator function, final boolean rate) {
    setTimeSeries(tsuids, function, rate, new RateOptions());
  }
  
  @Override
  public void setTimeSeries(final List<String> tsuids,
      final Aggregator function, final boolean rate, 
      final RateOptions rate_options) {
    if (tsuids == null || tsuids.isEmpty()) {
      throw new IllegalArgumentException(
          "Empty or missing TSUID list not allowed");
    }
    
    String first_metric = "";
    for (final String tsuid : tsuids) {
      if (first_metric.isEmpty()) {
        first_metric = tsuid.substring(0, TSDB.metrics_width() * 2)
          .toUpperCase();
        continue;
      }
      
      final String metric = tsuid.substring(0, TSDB.metrics_width() * 2)
        .toUpperCase();
      if (!first_metric.equals(metric)) {
        throw new IllegalArgumentException(
          "One or more TSUIDs did not share the same metric");
      }
    }
    
    // the metric will be set with the scanner is configured 
    this.tsuids = tsuids;
    aggregator = function;
    this.rate = rate;
    this.rate_options = rate_options;
  }
  
  /**
   * @param explicit_tags Whether or not to match only on the given tags
   * @since 2.3
   */
  public void setExplicitTags(final boolean explicit_tags) {
    this.explicit_tags = explicit_tags;
  }
  
  @Override
  public Deferred<Object> configureFromQuery(final TSQuery query, 
      final int index) {
    if (query.getQueries() == null || query.getQueries().isEmpty()) {
      throw new IllegalArgumentException("Missing sub queries");
    }
    if (index < 0 || index > query.getQueries().size()) {
      throw new IllegalArgumentException("Query index was out of range");
    }
    
    final TSSubQuery sub_query = query.getQueries().get(index);
    setStartTime(query.startTime());
    setEndTime(query.endTime());
    setDelete(query.getDelete());
    query_index = index;
    query_stats = query.getQueryStats();
    
    // set common options
    aggregator = sub_query.aggregator();
    rate = sub_query.getRate();
    rate_options = sub_query.getRateOptions();
    if (rate_options == null) {
      rate_options = new RateOptions();
    }
    downsampler = sub_query.downsamplingSpecification();
    pre_aggregate = sub_query.isPreAggregate();
    rollup_usage = sub_query.getRollupUsage();
    filters = sub_query.getFilters();
    explicit_tags = sub_query.getExplicitTags();
    override_fuzzy_filter = sub_query.getUseFuzzyFilter();
    override_multi_get = sub_query.getUseMultiGets();
    
    max_bytes = tsdb.getQueryByteLimits().getByteLimit(sub_query.getMetric());
    if (tsdb.getConfig().getBoolean("tsd.query.limits.bytes.allow_override") && 
        query.overrideByteLimit()) {
      max_bytes = 0;
    }
    max_data_points = tsdb.getQueryByteLimits().getDataPointLimit(sub_query.getMetric());
    if (tsdb.getConfig().getBoolean("tsd.query.limits.data_points.allow_override") &&
        query.overrideDataPointLimit()) {
      max_data_points = 0;
    }
    
    // set percentile options
    percentiles = sub_query.getPercentiles();
    show_histogram_buckets = sub_query.getShowHistogramBuckets();
    
    if (rollup_usage != ROLLUP_USAGE.ROLLUP_RAW) {
      //Check whether the down sampler is set and rollup is enabled
      transformDownSamplerToRollupQuery(aggregator, sub_query.getDownsample());
    }
    sub_query.setTsdbQuery(this);
    
    if (use_multi_gets && override_multi_get && multiget_with_search) {
      row_key_literals_list = Lists.newArrayList();
    }
    
    // if we have tsuids set, that takes precedence
    if (sub_query.getTsuids() != null && !sub_query.getTsuids().isEmpty()) {
      tsuids = new ArrayList<String>(sub_query.getTsuids());
      String first_metric = "";
      for (final String tsuid : tsuids) {
        if (first_metric.isEmpty()) {
          first_metric = tsuid.substring(0, TSDB.metrics_width() * 2)
            .toUpperCase();
          continue;
        }
        
        final String metric = tsuid.substring(0, TSDB.metrics_width() * 2)
          .toUpperCase();
        if (!first_metric.equals(metric)) {
          throw new IllegalArgumentException(
            "One or more TSUIDs did not share the same metric [" + first_metric + 
            "] [" + metric + "]");
        }
      }
      return Deferred.fromResult(null);
    } else {
      /** Triggers the group by resolution if we had filters to resolve */
      class FilterCB implements Callback<Object, ArrayList<byte[]>> {
        @Override
        public Object call(final ArrayList<byte[]> results) throws Exception {
          findGroupBys();
          return null;
        }
      }

      /** Resolve and group by tags after resolving the metric */
      class MetricCB implements Callback<Deferred<Object>, byte[]> {
        @Override
        public Deferred<Object> call(final byte[] uid) throws Exception {
          metric = uid;
          if (filters != null) {
            if (use_multi_gets && override_multi_get && multiget_with_search) {
              class ErrorCB implements Callback<Deferred<Object>, Exception> {
                @Override
                public Deferred<Object> call(Exception arg) throws Exception {
                 LOG.info("Doing scans because meta query is failed", arg);
                 if (explicit_tags) {
                   search_query_failure = true; 
                 } else {
                   override_multi_get = false;
                   use_multi_gets = false;
                 }
                 return Deferred.group(resolveTagFilters()).addCallback(new FilterCB());
                }
              }
              
              class SuccessCB implements Callback<Deferred<Object>, List<ByteMap<byte[][]>>> {
                @Override
                public Deferred<Object> call(final List<ByteMap<byte[][]>> results) throws Exception {
                  row_key_literals_list.addAll(results);
                  return Deferred.fromResult(null);
                }
              }
              
              tsdb.getSearchPlugin().resolveTSQuery(query, index)
                .addCallbackDeferring(new SuccessCB())
                .addErrback(new ErrorCB());
            }
            
            return Deferred.group(resolveTagFilters()).addCallback(new FilterCB());
          } else {
            return Deferred.fromResult(null);
          }
        }
        
        private List<Deferred<byte[]>> resolveTagFilters() {
          final List<Deferred<byte[]>> deferreds = 
              new ArrayList<Deferred<byte[]>>(filters.size());
          for (final TagVFilter filter : filters) {
            // determine if the user is asking for pre-agg data
            if (filter instanceof TagVLiteralOrFilter && tsdb.getAggTagKey() != null) {
              if (filter.getTagk().equals(tsdb.getAggTagKey())) {
                if (tsdb.getRawTagValue() != null && 
                    !filter.getFilter().equals(tsdb.getRawTagValue())) {
                  pre_aggregate = true;
                }
              }
            }
            
            deferreds.add(filter.resolveTagkName(tsdb));
          }
          return deferreds;
        }
      }
      
      // fire off the callback chain by resolving the metric first
      return tsdb.metrics.getIdAsync(sub_query.getMetric())
          .addCallbackDeferring(new MetricCB());
    }
  }
  
  @Override
  public void downsample(final long interval, final Aggregator downsampler,
      final FillPolicy fill_policy) {
    this.downsampler = new DownsamplingSpecification(
        interval, downsampler,fill_policy);
  }

  /**
   * Sets an optional downsampling function with interpolation on this query.
   * @param interval The interval, in milliseconds to rollup data points
   * @param downsampler An aggregation function to use when rolling up data points
   * @throws NullPointerException if the aggregation function is null
   * @throws IllegalArgumentException if the interval is not greater than 0
   */
  @Override
  public void downsample(final long interval, final Aggregator downsampler) {
    if (downsampler == Aggregators.NONE) {
      throw new IllegalArgumentException("cannot use the NONE "
          + "aggregator for downsampling");
    }
    downsample(interval, downsampler, FillPolicy.NONE);
  }

  /**
   * Populates the {@link #group_bys} and {@link #row_key_literals}'s with 
   * values pulled from the filters. 
   */
  private void findGroupBys() {
    if (filters == null || filters.isEmpty()) {
      return;
    }
    
    if ((use_multi_gets && override_multi_get) && !search_query_failure) {
      
    }
    
    row_key_literals = new ByteMap<byte[][]>();
    final int expansion_limit = tsdb.getConfig().getInt(
        "tsd.query.filter.expansion_limit");
    
    Collections.sort(filters);
    final Iterator<TagVFilter> current_iterator = filters.iterator();
    final Iterator<TagVFilter> look_ahead = filters.iterator();
    byte[] tagk = null;
    TagVFilter next = look_ahead.hasNext() ? look_ahead.next() : null;
    int row_key_literals_count = 0;
    while (current_iterator.hasNext()) {
      next = look_ahead.hasNext() ? look_ahead.next() : null;
      int gbs = 0;
      // sorted!
      final ByteMap<Void> literals = new ByteMap<Void>();
      final List<TagVFilter> literal_filters = new ArrayList<TagVFilter>();
      TagVFilter current = null;
      do { // yeah, I'm breakin out the do!!!
        current = current_iterator.next();
        if (tagk == null) {
          tagk = new byte[TSDB.tagk_width()];
          System.arraycopy(current.getTagkBytes(), 0, tagk, 0, TSDB.tagk_width());
        }
        
        if (current.isGroupBy()) {
          gbs++;
        }
        if (!current.getTagVUids().isEmpty()) {
          for (final byte[] uid : current.getTagVUids()) {
            literals.put(uid, null);
          }
          literal_filters.add(current);
        }

        if (next != null && Bytes.memcmp(tagk, next.getTagkBytes()) != 0) {
          break;
        }
        next = look_ahead.hasNext() ? look_ahead.next() : null;
      } while (current_iterator.hasNext() && 
          Bytes.memcmp(tagk, current.getTagkBytes()) == 0);

      if (gbs > 0) {
        if (group_bys == null) {
          group_bys = new ArrayList<byte[]>();
        }
        group_bys.add(current.getTagkBytes());
      }
      
      if (literals.size() > 0) {
        if (literals.size() + row_key_literals_count > expansion_limit) {
          LOG.debug("Skipping literals for " + current.getTagk() + 
              " as it exceedes the limit");
          //has_filter_cannot_use_get = true;
        } else {
          final byte[][] values = new byte[literals.size()][];
          literals.keySet().toArray(values);
          row_key_literals.put(current.getTagkBytes(), values);
          row_key_literals_count += values.length;
          
          for (final TagVFilter filter : literal_filters) {
            filter.setPostScan(false);
          }
        }
      } else {
        row_key_literals.put(current.getTagkBytes(), null);
        // no literal values, just keys, so we can't multi-get
        if (search_query_failure) {
          use_multi_gets = false;
        }
      }
      
      // make sure the multi-get cardinality doesn't exceed our limit (or disable
      // multi-gets)
      if ((use_multi_gets && override_multi_get)) {
        int multi_get_limit = tsdb.getConfig().getInt("tsd.query.multi_get.limit");
        int cardinality = filters.size() * row_key_literals_count;
        if (cardinality > multi_get_limit) {
          use_multi_gets = false;
        } else if (search_query_failure) {
          row_key_literals_list.add(row_key_literals);
        }
        // TODO - account for time as well
      }
    }
  }
  /**
   * Executes the query.
   * NOTE: Do not run the same query multiple times. Construct a new query with
   * the same parameters again if needed
   * TODO(cl) There are some strange occurrences when unit testing where the end
   * time, if not set, can change between calls to run()
   * @return An array of data points with one time series per array value
   */
  @Override
  public DataPoints[] run() throws HBaseException {
    try {
      return runAsync().joinUninterruptibly();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Should never be here", e);
    }
  }
  
  @Override
  public DataPoints[] runHistogram() throws HBaseException {
    if (!isHistogramQuery()) {
      throw new RuntimeException("Should never be here");
    }
    
    try {
      return runHistogramAsync().joinUninterruptibly();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Should never be here", e);
    }
  }
  
  @Override
  public Deferred<DataPoints[]> runAsync() throws HBaseException {
    Deferred<DataPoints[]> result = null;
    if (use_multi_gets && override_multi_get) {
      result = this.findSpansWithMultiGetter().addCallback(new GroupByAndAggregateCB());
    } else {
      result = findSpans().addCallback(new GroupByAndAggregateCB());
    }

    if (rollup_usage != null && rollup_usage.fallback()) {
      result.addCallback(new FallbackRollupOnEmptyResult());
    }
    
    return result;
  }

  @Override
  public Deferred<DataPoints[]> runHistogramAsync() throws HBaseException {
    if (!isHistogramQuery()) {
      throw new RuntimeException("Should never be here");
    }
    
    Deferred<DataPoints[]> result = null;
    if (use_multi_gets && override_multi_get) {
      result = findHistogramSpansWithMultiGetter()
          .addCallback(new HistogramGroupByAndAggregateCB());
    } else {
      result = findHistogramSpans()
          .addCallback(new HistogramGroupByAndAggregateCB());
    }
        
    return result;
  }
  
  @Override
  public boolean isHistogramQuery() {
    if ((this.percentiles != null && this.percentiles.size() > 0) || show_histogram_buckets) {
      return true;
    }
    
    return false;
  }
  
  /**
   * Finds all the {@link Span}s that match this query.
   * This is what actually scans the HBase table and loads the data into
   * {@link Span}s.
   * @return A map from HBase row key to the {@link Span} for that row key.
   * Since a {@link Span} actually contains multiple HBase rows, the row key
   * stored in the map has its timestamp zero'ed out.
   * @throws HBaseException if there was a problem communicating with HBase to
   * perform the search.
   * @throws IllegalArgumentException if bad data was retrieved from HBase.
   */
  private Deferred<TreeMap<byte[], Span>> findSpans() throws HBaseException {
    final short metric_width = tsdb.metrics.width();
    final TreeMap<byte[], Span> spans = // The key is a row key from HBase.
      new TreeMap<byte[], Span>(new SpanCmp(
          (short)(Const.SALT_WIDTH() + metric_width)));
    
    // Copy only the filters that should trigger a tag resolution. If this list
    // is empty due to literals or a wildcard star, then we'll save a TON of
    // UID lookups
    final List<TagVFilter> scanner_filters;
    if (filters != null) {   
      scanner_filters = new ArrayList<TagVFilter>(filters.size());
      for (final TagVFilter filter : filters) {
        if (filter.postScan()) {
          scanner_filters.add(filter);
        }
      }
    } else {
      scanner_filters = null;
    }
    
    if (Const.SALT_WIDTH() > 0) {
      final List<Scanner> scanners = new ArrayList<Scanner>(Const.SALT_BUCKETS());
      for (int i = 0; i < Const.SALT_BUCKETS(); i++) {
        scanners.add(getScanner(i));
      }
      scan_start_time = DateTime.nanoTime();
      return new SaltScanner(tsdb, metric, scanners, spans, scanner_filters,
          delete, rollup_query, query_stats, query_index, null, 
          max_bytes, max_data_points).scan();
    } else {
      final List<Scanner> scanners = new ArrayList<Scanner>(1);
      scanners.add(getScanner(0));
      scan_start_time = DateTime.nanoTime();
      return new SaltScanner(tsdb, metric, scanners, spans, scanner_filters,
          delete, rollup_query, query_stats, query_index, null, max_bytes, 
          max_data_points).scan();
    }
  }
  
  private Deferred<TreeMap<byte[], Span>> findSpansWithMultiGetter() throws HBaseException {
    final short metric_width = tsdb.metrics.width();
    final TreeMap<byte[], Span> spans = // The key is a row key from HBase.
    new TreeMap<byte[], Span>(new SpanCmp(metric_width));

    scan_start_time = System.nanoTime();
    
    return new MultiGetQuery(tsdb, this, metric, row_key_literals_list, 
        getScanStartTimeSeconds(), getScanEndTimeSeconds(),
        tableToBeScanned(), spans, null, 0, rollup_query, query_stats, query_index, 0,
        false, search_query_failure).fetch();
  }
  
  /**
   * Finds all the {@link HistogramSpan}s that match this query.
   * This is what actually scans the HBase table and loads the data into
   * {@link HistogramSpan}s.
   * 
   * @return A map from HBase row key to the {@link HistogramSpan} for that row key.
   * Since a {@link HistogramSpan} actually contains multiple HBase rows, the row key
   * stored in the map has its timestamp zero'ed out.
   * 
   * @throws HBaseException if there was a problem communicating with HBase to
   * perform the search.
   * @throws IllegalArgumentException if bad data was retreived from HBase.
   */
  private Deferred<TreeMap<byte[], HistogramSpan>> findHistogramSpans() throws HBaseException {
    final short metric_width = tsdb.metrics.width();
    final TreeMap<byte[], HistogramSpan> histSpans = new TreeMap<byte[], HistogramSpan>(new SpanCmp(metric_width));
    
    // Copy only the filters that should trigger a tag resolution. If this list
    // is empty due to literals or a wildcard star, then we'll save a TON of
    // UID lookups
    final List<TagVFilter> scanner_filters;
    if (filters != null) {
      scanner_filters = new ArrayList<TagVFilter>(filters.size());
      for (final TagVFilter filter : filters) {
        if (filter.postScan()) {
          scanner_filters.add(filter);
        }
      }
    } else {
      scanner_filters = null;
    }

    scan_start_time = System.nanoTime();
    final List<Scanner> scanners;
    if (Const.SALT_WIDTH() > 0) {
      scanners = new ArrayList<Scanner>(Const.SALT_BUCKETS());
      for (int i = 0; i < Const.SALT_BUCKETS(); i++) {
        scanners.add(getScanner(i));
      }
      scan_start_time = DateTime.nanoTime();
      return new SaltScanner(tsdb, metric, scanners, null, scanner_filters, 
          delete, rollup_query, query_stats, query_index, histSpans, 
          max_bytes, max_data_points).scanHistogram();
    } else {
      scanners = Lists.newArrayList(getScanner());
      scan_start_time = DateTime.nanoTime();
      return new SaltScanner(tsdb, metric, scanners, null, scanner_filters, 
          delete, rollup_query, query_stats, query_index, histSpans, 
          max_bytes, max_data_points).scanHistogram();
    }
  }
  
  private Deferred<TreeMap<byte[], HistogramSpan>> findHistogramSpansWithMultiGetter() throws HBaseException {
    final short metric_width = tsdb.metrics.width();
    // The key is a row key from HBase
    final TreeMap<byte[], HistogramSpan> histSpans = new TreeMap<byte[], HistogramSpan>(new SpanCmp(metric_width));

    scan_start_time = System.nanoTime();
    return new MultiGetQuery(tsdb, this, metric, row_key_literals_list, 
        getScanStartTimeSeconds(), getScanEndTimeSeconds(),
        tableToBeScanned(), null, histSpans, 0, rollup_query, query_stats, query_index, 0,
        false, search_query_failure).fetchHistogram();
  }
  
  /**
   * Callback that should be attached the the output of
   * {@link TsdbQuery#findSpans} to group and sort the results.
   */
  private class GroupByAndAggregateCB implements 
    Callback<DataPoints[], TreeMap<byte[], Span>>{
    
    /**
    * Creates the {@link SpanGroup}s to form the final results of this query.
    * @param spans The {@link Span}s found for this query ({@link #findSpans}).
    * Can be {@code null}, in which case the array returned will be empty.
    * @return A possibly empty array of {@link SpanGroup}s built according to
    * any 'GROUP BY' formulated in this query.
    */
    @Override
    public DataPoints[] call(final TreeMap<byte[], Span> spans) throws Exception {
      if (query_stats != null) {
        query_stats.addStat(query_index, QueryStat.QUERY_SCAN_TIME, 
                (System.nanoTime() - TsdbQuery.this.scan_start_time));
      }
      
      if (spans == null || spans.size() <= 0) {
        if (query_stats != null) {
          query_stats.addStat(query_index, QueryStat.GROUP_BY_TIME, 0);
        }
        return NO_RESULT;
      }
      
      // The raw aggregator skips group bys and ignores downsampling
      if (aggregator == Aggregators.NONE) {
        final SpanGroup[] groups = new SpanGroup[spans.size()];
        int i = 0;
        for (final Span span : spans.values()) {
          final SpanGroup group = new SpanGroup(
              tsdb, 
              getScanStartTimeSeconds(),
              getScanEndTimeSeconds(),
              null, 
              rate, 
              rate_options,
              aggregator,
              downsampler,
              getStartTime(), 
              getEndTime(),
              query_index,
              rollup_query);
          group.add(span);
          groups[i++] = group;
        }
        return groups;
      }
      
      if (group_bys == null) {
        // We haven't been asked to find groups, so let's put all the spans
        // together in the same group.
        final SpanGroup group = new SpanGroup(tsdb,
                                              getScanStartTimeSeconds(),
                                              getScanEndTimeSeconds(),
                                              spans.values(),
                                              rate, rate_options,
                                              aggregator,
                                              downsampler,
                                              getStartTime(), 
                                              getEndTime(),
                                              query_index,
                                              rollup_query);
        if (query_stats != null) {
          query_stats.addStat(query_index, QueryStat.GROUP_BY_TIME, 0);
        }
        return new SpanGroup[] { group };
      }
  
      // Maps group value IDs to the SpanGroup for those values. Say we've
      // been asked to group by two things: foo=* bar=* Then the keys in this
      // map will contain all the value IDs combinations we've seen. If the
      // name IDs for `foo' and `bar' are respectively [0, 0, 7] and [0, 0, 2]
      // then we'll have group_bys=[[0, 0, 2], [0, 0, 7]] (notice it's sorted
      // by ID, so bar is first) and say we find foo=LOL bar=OMG as well as
      // foo=LOL bar=WTF and that the IDs of the tag values are:
      // LOL=[0, 0, 1] OMG=[0, 0, 4] WTF=[0, 0, 3]
      // then the map will have two keys:
      // - one for the LOL-OMG combination: [0, 0, 1, 0, 0, 4] and,
      // - one for the LOL-WTF combination: [0, 0, 1, 0, 0, 3].
      final ByteMap<SpanGroup> groups = new ByteMap<SpanGroup>();
      final short value_width = tsdb.tag_values.width();
      final byte[] group = new byte[group_bys.size() * value_width];
      for (final Map.Entry<byte[], Span> entry : spans.entrySet()) {
        final byte[] row = entry.getKey();
        byte[] value_id = null;
        int i = 0;
        // TODO(tsuna): The following loop has a quadratic behavior. We can
        // make it much better since both the row key and group_bys are sorted.
        for (final byte[] tag_id : group_bys) {
          value_id = Tags.getValueId(tsdb, row, tag_id);
          if (value_id == null) {
            break;
          }
          System.arraycopy(value_id, 0, group, i, value_width);
          i += value_width;
        }
        if (value_id == null) {
          LOG.error("WTF? Dropping span for row " + Arrays.toString(row)
                   + " as it had no matching tag from the requested groups,"
                   + " which is unexpected. Query=" + this);
          continue;
        }
        //LOG.info("Span belongs to group " + Arrays.toString(group) + ": " + Arrays.toString(row));
        SpanGroup thegroup = groups.get(group);
        if (thegroup == null) {
          thegroup = new SpanGroup(tsdb, getScanStartTimeSeconds(),
                                   getScanEndTimeSeconds(),
                                   null, rate, rate_options, aggregator,
                                   downsampler,
                                   getStartTime(), 
                                   getEndTime(),
                                   query_index,
                                   rollup_query);
          // Copy the array because we're going to keep `group' and overwrite
          // its contents. So we want the collection to have an immutable copy.
          final byte[] group_copy = new byte[group.length];
          System.arraycopy(group, 0, group_copy, 0, group.length);
          groups.put(group_copy, thegroup);
        }
        thegroup.add(entry.getValue());
      }
      //for (final Map.Entry<byte[], SpanGroup> entry : groups) {
      // LOG.info("group for " + Arrays.toString(entry.getKey()) + ": " + entry.getValue());
      //}
      if (query_stats != null) {
        query_stats.addStat(query_index, QueryStat.GROUP_BY_TIME, 0);
      }
      return groups.values().toArray(new SpanGroup[groups.size()]);
    }
  }

  /**
   * Callback that should be attached the the output of
   * {@link TsdbQuery#findHistogramSpans} to group and sort the results.
   */
   private class HistogramGroupByAndAggregateCB implements 
     Callback<DataPoints[], TreeMap<byte[], HistogramSpan>>{

     /**
     * Creates the {@link HistogramSpanGroup}s to form the final results of this query.
     * @param spans The {@link HistogramSpan}s found for this query ({@link #findHistogramSpans}).
     * Can be {@code null}, in which case the array returned will be empty.
     * @return A possibly empty array of {@link HistogramSpanGroup}s built according to
     * any 'GROUP BY' formulated in this query.
     */
     public DataPoints[] call(final TreeMap<byte[], HistogramSpan> spans) throws Exception {
       if (query_stats != null) {
         query_stats.addStat(query_index, QueryStat.QUERY_SCAN_TIME, 
                 (System.nanoTime() - TsdbQuery.this.scan_start_time));
       }
       
       final long group_build = System.nanoTime();
       if (spans == null || spans.size() <= 0) {
         if (query_stats != null) {
           query_stats.addStat(query_index, QueryStat.GROUP_BY_TIME, 0);
         }
         return NO_RESULT;
       }
       final ByteSet query_tags = null;
       // TODO
//       if (agg_tag_promotion && group_bys != null && !group_bys.isEmpty()) {
//         query_tags = new ByteSet();
//         query_tags.addAll(group_bys);
//       } else {
//         query_tags = null;
//       }
       
       final ArrayList<DataPoints> result_dp_groups = new ArrayList<DataPoints>();
       // The raw aggregator skips group bys and ignores downsampling
       if (aggregator == Aggregators.NONE) {
         for (final HistogramSpan span : spans.values()) {
           final HistogramSpanGroup group = new HistogramSpanGroup(tsdb, 
                                                 getScanStartTimeSeconds(),
                                                 getScanEndTimeSeconds(),
                                                 null,
                                                 null,
                                                 downsampler,
                                                 getStartTime(), 
                                                 getEndTime(),
                                                 query_index,
                                                 RollupQuery.isValidQuery(rollup_query),
                                                 query_tags);
           group.add(span);
           
           // create histogram data points to data points adaptor for each percentile calculation
           if (null != percentiles && percentiles.size() > 0) {
             List<DataPoints> percentile_datapoints_list = generateHistogramPercentileDataPoints(group);
             if (null != percentile_datapoints_list && percentile_datapoints_list.size() > 0)
               result_dp_groups.addAll(percentile_datapoints_list);
           }
           
           
           // create bucket metric 
           if (show_histogram_buckets) {
             List<DataPoints> bucket_datapoints_list = generateHistogramBucketDataPoints(group);
             if (null != bucket_datapoints_list && bucket_datapoints_list.size() > 0) {
               result_dp_groups.addAll(bucket_datapoints_list);
             }
           }
         } // end for
         
         int i = 0;
         DataPoints[] result = new DataPoints[result_dp_groups.size()];
         for (DataPoints item : result_dp_groups) {
           result[i++] = item;
         }
         return result;
       }
       
       if (group_bys == null) {
         // We haven't been asked to find groups, so let's put all the spans
         // together in the same group.
         final HistogramSpanGroup group = new HistogramSpanGroup(tsdb,
                                               getScanStartTimeSeconds(),
                                               getScanEndTimeSeconds(),
                                               spans.values(),
                                               HistogramAggregation.SUM, // only SUM is applicable for histogram metric
                                               downsampler,
                                               getStartTime(),
                                               getEndTime(),
                                               query_index,
                                               RollupQuery.isValidQuery(rollup_query),
                                               query_tags);
         if (query_stats != null) {
           query_stats.addStat(query_index, QueryStat.GROUP_BY_TIME, 
               (System.nanoTime() - group_build));
         }
         
         // create histogram data points to data points adaptor for each percentile calculation
         if (null != percentiles && percentiles.size() > 0) {
           List<DataPoints> percentile_datapoints_list = generateHistogramPercentileDataPoints(group);
           if (null != percentile_datapoints_list && percentile_datapoints_list.size() > 0)
             result_dp_groups.addAll(percentile_datapoints_list);
         }
         
         // create bucket metric 
         if (show_histogram_buckets) {
           List<DataPoints> bucket_datapoints_list = generateHistogramBucketDataPoints(group);
           if (null != bucket_datapoints_list && bucket_datapoints_list.size() > 0) {
             result_dp_groups.addAll(bucket_datapoints_list);
           }
         }
         
         int i = 0;
         DataPoints[] result = new DataPoints[result_dp_groups.size()];
         for (DataPoints item : result_dp_groups) {
           result[i++] = item;
         }
         return result;
       }
   
       // Maps group value IDs to the SpanGroup for those values. Say we've
       // been asked to group by two things: foo=* bar=* Then the keys in this
       // map will contain all the value IDs combinations we've seen. If the
       // name IDs for `foo' and `bar' are respectively [0, 0, 7] and [0, 0, 2]
       // then we'll have group_bys=[[0, 0, 2], [0, 0, 7]] (notice it's sorted
       // by ID, so bar is first) and say we find foo=LOL bar=OMG as well as
       // foo=LOL bar=WTF and that the IDs of the tag values are:
       // LOL=[0, 0, 1] OMG=[0, 0, 4] WTF=[0, 0, 3]
       // then the map will have two keys:
       // - one for the LOL-OMG combination: [0, 0, 1, 0, 0, 4] and,
       // - one for the LOL-WTF combination: [0, 0, 1, 0, 0, 3].
       final ByteMap<HistogramSpanGroup> groups = new ByteMap<HistogramSpanGroup>();
       final short value_width = tsdb.tag_values.width();
       final byte[] group = new byte[group_bys.size() * value_width];
       for (final Map.Entry<byte[], HistogramSpan> entry : spans.entrySet()) {
         final byte[] row = entry.getKey();
         byte[] value_id = null;
         int i = 0;
         // TODO(tsuna): The following loop has a quadratic behavior. We can
         // make it much better since both the row key and group_bys are sorted.
         for (final byte[] tag_id : group_bys) {
           value_id = Tags.getValueId(tsdb, row, tag_id);
           if (value_id == null) {
             break;
           }
           System.arraycopy(value_id, 0, group, i, value_width);
           i += value_width;
         }
         if (value_id == null) {
           LOG.error("WTF? Dropping span for row " + Arrays.toString(row)
                    + " as it had no matching tag from the requested groups,"
                    + " which is unexpected. Query=" + this);
           continue;
         }
         
         //LOG.info("Span belongs to group " + Arrays.toString(group) + ": " + Arrays.toString(row));
         HistogramSpanGroup thegroup = groups.get(group);
         if (thegroup == null) {
           thegroup = new HistogramSpanGroup(tsdb, 
                                             getScanStartTimeSeconds(),
                                             getScanEndTimeSeconds(),
                                             null,
                                             HistogramAggregation.SUM, // only SUM is applicable for histogram metric
                                             downsampler, 
                                             getStartTime(), 
                                             getEndTime(),
                                             query_index,
                                             RollupQuery.isValidQuery(rollup_query),
                                             query_tags);
           
           // Copy the array because we're going to keep `group' and overwrite
           // its contents. So we want the collection to have an immutable copy.
           final byte[] group_copy = new byte[group.length];
           System.arraycopy(group, 0, group_copy, 0, group.length);
           groups.put(group_copy, thegroup);
         }
         thegroup.add(entry.getValue());
       }
       
       if (query_stats != null) {
         query_stats.addStat(query_index, QueryStat.GROUP_BY_TIME, 
             (System.nanoTime() - group_build));
       }
       
       
       for (final Map.Entry<byte[], HistogramSpanGroup> entry : groups.entrySet()) {
         // create histogram data points to data points adaptor for each percentile calculation
         if (null != percentiles && percentiles.size() > 0) {
           List<DataPoints> percentile_datapoints_list = generateHistogramPercentileDataPoints(entry.getValue());
           if (null != percentile_datapoints_list && percentile_datapoints_list.size() > 0)
             result_dp_groups.addAll(percentile_datapoints_list);
         }
         
         // create bucket metric 
         if (show_histogram_buckets) {
           List<DataPoints> bucket_datapoints_list = generateHistogramBucketDataPoints(entry.getValue());
           if (null != bucket_datapoints_list && bucket_datapoints_list.size() > 0) {
             result_dp_groups.addAll(bucket_datapoints_list);
           }
         }
       } // end for
      
       int i = 0;
       DataPoints[] result = new DataPoints[result_dp_groups.size()];
       for (DataPoints item : result_dp_groups) {
         result[i++] = item;
       }
       return result;
     }

    private List<DataPoints> generateHistogramPercentileDataPoints(final HistogramSpanGroup group) {
      ArrayList<DataPoints> result_dp_groups = new ArrayList<DataPoints>();
      for (final Float percentil : percentiles) {
        final HistogramDataPointsToDataPointsAdaptor dp_adaptor = new HistogramDataPointsToDataPointsAdaptor(group,
            percentil.floatValue());
        result_dp_groups.add(dp_adaptor);
      } // end for

      return result_dp_groups;
    }

    private List<DataPoints> generateHistogramBucketDataPoints(final HistogramSpanGroup group) {
      ArrayList<DataPoints> result_dp_groups = new ArrayList<DataPoints>();
      try {
        HistogramSeekableView seek_view = group.iterator();
        if (seek_view.hasNext()) {
          HistogramDataPoint hdp = seek_view.next();
          Map<HistogramDataPoint.HistogramBucket, Long> buckets = hdp.getHistogramBucketsIfHas();
          if (null != buckets) {
            for (Map.Entry<HistogramDataPoint.HistogramBucket, Long> bucket : buckets.entrySet()) {
              final HistogramBucketDataPointsAdaptor dp_bucket_adaptor = new HistogramBucketDataPointsAdaptor(group, bucket.getKey());
              result_dp_groups.add(dp_bucket_adaptor);
            } // end for
          } // end if
        }
      } catch (UnsupportedOperationException e) {
        // Just Ignore
      }

      return result_dp_groups;
    }
   }
  
  /**
   * Scan the tables again with the next best rollup match, on empty result set
   */
  private class FallbackRollupOnEmptyResult implements 
     Callback<Deferred<DataPoints[]>, DataPoints[]>{

     /**
     * Creates the {@link SpanGroup}s to form the final results of this query.
     * @param spans The {@link Span}s found for this query ({@link #findSpans}).
     * Can be {@code null}, in which case the array returned will be empty.
     * @return A possibly empty array of {@link SpanGroup}s built according to
     * any 'GROUP BY' formulated in this query.
     */
     public Deferred<DataPoints[]> call(final DataPoints[] datapoints) throws Exception {
      //TODO review this logic during spatial aggregation implementation
       
      if (datapoints == NO_RESULT && RollupQuery.isValidQuery(rollup_query)) {
        //There are no datapoints for this query and it is a rollup query
        //but not the default interval (default interval means raw).
        //This will prevent redundant scan on raw data on the presense of
        //default rollup interval
        
        //If the rollup usage is to fallback directly to raw data
        //then nullyfy the rollup query, so that the recursive scan will use
        //raw data and this will not called again because of isValida check
        //If the rollup usage is to fallback to next best match then pupup
        //next best match and attach that to the rollup query
        if (rollup_usage == ROLLUP_USAGE.ROLLUP_FALLBACK_RAW) {
          transformRollupQueryToDownSampler();
          return runAsync();
        }
        else if (best_match_rollups != null && best_match_rollups.size() > 0) {
          RollupInterval interval = best_match_rollups.remove(0);
          
          if (interval.isDefaultInterval()) {
            transformRollupQueryToDownSampler();
          }
          else {
            rollup_query = new RollupQuery(interval, 
                  rollup_query.getRollupAgg(),
                  rollup_query.getSampleIntervalInMS(),
                  aggregator);
            //Here the requested sampling rate will be higher than
            //resulted result. So downsample it
            if (!rollup_query.isLowerSamplingRate()) {
//               sample_interval_ms = rollup_query.getSampleIntervalInMS();
//               downsampler = rollup_query.getRollupAgg();
              // TODO - default fill
              downsampler = new DownsamplingSpecification(
                  rollup_query.getSampleIntervalInMS(), 
                  rollup_query.getRollupAgg(),
                  (downsampler != null ? downsampler.getFillPolicy() : 
                    FillPolicy.ZERO));
            }
          }
          
          return runAsync();
        }
        return Deferred.fromResult(NO_RESULT);
      }
      else {
        return Deferred.fromResult(datapoints);
      }
    }
  }
  
  /**
   * Returns a scanner set for the given metric (from {@link #metric} or from
   * the first TSUID in the {@link #tsuids}s list. If one or more tags are 
   * provided, it calls into {@link #createAndSetFilter} to setup a row key 
   * filter. If one or more TSUIDs have been provided, it calls into
   * {@link #createAndSetTSUIDFilter} to setup a row key filter.
   * @return A scanner to use for fetching data points
   */
  protected Scanner getScanner() throws HBaseException {
    return getScanner(0);
  }
  
  /**
   * Returns a scanner set for the given metric (from {@link #metric} or from
   * the first TSUID in the {@link #tsuids}s list. If one or more tags are 
   * provided, it calls into {@link #createAndSetFilter} to setup a row key 
   * filter. If one or more TSUIDs have been provided, it calls into
   * {@link #createAndSetTSUIDFilter} to setup a row key filter.
   * @param salt_bucket The salt bucket to scan over when salting is enabled.
   * @return A scanner to use for fetching data points
   */
  protected Scanner getScanner(final int salt_bucket) throws HBaseException {
    final short metric_width = tsdb.metrics.width();
    
    // set the metric UID based on the TSUIDs if given, or the metric UID
    if (tsuids != null && !tsuids.isEmpty()) {
      final String tsuid = tsuids.get(0);
      final String metric_uid = tsuid.substring(0, metric_width * 2);
      metric = UniqueId.stringToUid(metric_uid);
    }
    
    final boolean is_rollup = RollupQuery.isValidQuery(rollup_query);
    
    // We search at least one row before and one row after the start & end
    // time we've been given as it's quite likely that the exact timestamp
    // we're looking for is in the middle of a row.  Plus, a number of things
    // rely on having a few extra data points before & after the exact start
    // & end dates in order to do proper rate calculation or downsampling near
    // the "edges" of the graph.
    final Scanner scanner = QueryUtil.getMetricScanner(tsdb, salt_bucket, metric, 
        (int) getScanStartTimeSeconds(), end_time == UNSET
        ? -1  // Will scan until the end (0xFFF...).
        : (int) getScanEndTimeSeconds(), 
        tableToBeScanned(), 
        TSDB.FAMILY());
    if(tsdb.getConfig().use_otsdb_timestamp()) {
      long stTime = (getScanStartTimeSeconds() * 1000);
      long endTime = end_time == UNSET ? -1 : (getScanEndTimeSeconds() * 1000);
      scanner.setTimeRange(stTime, endTime);
    }
    if (tsuids != null && !tsuids.isEmpty()) {
      createAndSetTSUIDFilter(scanner);
    } else if (filters.size() > 0) {
      createAndSetFilter(scanner);
    }

    if (is_rollup) {
      ScanFilter existing = scanner.getFilter();
      // TODO - need some UTs around this!
      // Set the Scanners column qualifier pattern with rollup aggregator
      // HBase allows only a single filter so if we have a row key filter, keep
      // it. If not, then we can do this
      if (!rollup_query.getGroupBy().toString().equals("avg")) {
        if (existing != null) {
          final List<ScanFilter> filters = new ArrayList<ScanFilter>(3);
          filters.add(existing);
          filters.add(new QualifierFilter(CompareFilter.CompareOp.EQUAL,
              new BinaryPrefixComparator(rollup_query.getGroupBy().toString()
                      .getBytes(Const.ASCII_CHARSET))));
          filters.add(new QualifierFilter(CompareFilter.CompareOp.EQUAL,
              new BinaryPrefixComparator(new byte[] { 
                  (byte) tsdb.getRollupConfig().getIdForAggregator(
                      rollup_query.getRollupAgg().toString())
              })));
          scanner.setFilter(new FilterList(filters, Operator.MUST_PASS_ONE));
        } else {
          final List<ScanFilter> filters = new ArrayList<ScanFilter>(2);
          filters.add(new QualifierFilter(CompareFilter.CompareOp.EQUAL,
              new BinaryPrefixComparator(rollup_query.getRollupAgg().toString()
                  .getBytes(Const.ASCII_CHARSET))));
          filters.add(new QualifierFilter(CompareFilter.CompareOp.EQUAL,
              new BinaryPrefixComparator(new byte[] { 
                  (byte) tsdb.getRollupConfig().getIdForAggregator(
                      rollup_query.getRollupAgg().toString())
              })));
          scanner.setFilter(new FilterList(filters, Operator.MUST_PASS_ONE));
        }
      } else {
        final List<ScanFilter> filters = new ArrayList<ScanFilter>(4);
        filters.add(new QualifierFilter(CompareFilter.CompareOp.EQUAL,
            new BinaryPrefixComparator("sum".getBytes())));
        filters.add(new QualifierFilter(CompareFilter.CompareOp.EQUAL,
            new BinaryPrefixComparator("count".getBytes())));
        filters.add(new QualifierFilter(CompareFilter.CompareOp.EQUAL,
            new BinaryPrefixComparator(new byte[] { 
                (byte) tsdb.getRollupConfig().getIdForAggregator("sum")
            })));
        filters.add(new QualifierFilter(CompareFilter.CompareOp.EQUAL,
            new BinaryPrefixComparator(new byte[] { 
                (byte) tsdb.getRollupConfig().getIdForAggregator("count")
            })));
        
        if (existing != null) {
          final List<ScanFilter> combined = new ArrayList<ScanFilter>(2);
          combined.add(existing);
          combined.add(new FilterList(filters, Operator.MUST_PASS_ONE));
          scanner.setFilter(new FilterList(combined, Operator.MUST_PASS_ALL));
        } else {
          scanner.setFilter(new FilterList(filters, Operator.MUST_PASS_ONE));
        }
      }
    }
    return scanner;
  }

  /**
   * Identify the table to be scanned based on the roll up and pre-aggregate 
   * query parameters
   * @return table name as byte array
   * @since 2.4
   */
  private byte[] tableToBeScanned() {
    final byte[] tableName;
    
    if (RollupQuery.isValidQuery(rollup_query)) {
      if (pre_aggregate) {
        tableName= rollup_query.getRollupInterval().getGroupbyTable();
      }
      else {
        tableName= rollup_query.getRollupInterval().getTemporalTable();
      }
    }
    else if (pre_aggregate) {
      tableName = tsdb.getDefaultInterval().getGroupbyTable();
    }
    else {
      tableName = tsdb.dataTable();
    }
    
    return tableName;
  }
  
  /** Returns the UNIX timestamp from which we must start scanning.  */
  private long getScanStartTimeSeconds() {
    // Begin with the raw query start time.
    long start = getStartTime();

    // Convert to seconds if we have a query in ms.
    if ((start & Const.SECOND_MASK) != 0L) {
      start /= 1000L;
    }
    
    // if we have a rollup query, we have different row key start times so find
    // the base time from which we need to search
    if (rollup_query != null) {
      long base_time = RollupUtils.getRollupBasetime(start, 
          rollup_query.getRollupInterval());
      if (rate) {
        // scan one row back so we can get the first rate value.
        base_time = RollupUtils.getRollupBasetime(base_time - 1, 
            rollup_query.getRollupInterval());
      }
      return base_time;
    }

    // First, we align the start timestamp to its representative value for the
    // interval in which it appears, if downsampling.
    long interval_aligned_ts = start;
    if (downsampler != null && downsampler.getInterval() > 0) {
      // Downsampling enabled.
      // TODO - calendar interval
      final long interval_offset = (1000L * start) % downsampler.getInterval();
      interval_aligned_ts -= interval_offset / 1000L;
    }

    // Then snap that timestamp back to its representative value for the
    // timespan in which it appears.
    final long timespan_offset = interval_aligned_ts % Const.MAX_TIMESPAN;
    final long timespan_aligned_ts = interval_aligned_ts - timespan_offset;

    // Don't return negative numbers.
    return timespan_aligned_ts > 0L ? timespan_aligned_ts : 0L;
  }

  /** Returns the UNIX timestamp at which we must stop scanning.  */
  private long getScanEndTimeSeconds() {
    // Begin with the raw query end time.
    long end = getEndTime();

    // Convert to seconds if we have a query in ms.
    if ((end & Const.SECOND_MASK) != 0L) {
      end /= 1000L;
      if (end - (end * 1000) < 1) {
        // handle an edge case where a user may request a ms time between
        // 0 and 1 seconds. Just bump it a second.
        end++;
      }
    }

    // The calculation depends on whether we're downsampling.
    if (downsampler != null && downsampler.getInterval() > 0) {
      // Downsampling enabled.
      //
      // First, we align the end timestamp to its representative value for the
      // interval FOLLOWING the one in which it appears.
      //
      // OpenTSDB's query bounds are inclusive, but HBase scan bounds are half-
      // open. The user may have provided an end bound that is already
      // interval-aligned (i.e., its interval offset is zero). If so, the user
      // wishes for that interval to appear in the output. In that case, we
      // skip forward an entire extra interval.
      //
      // This can be accomplished by simply not testing for zero offset.
      final long interval_offset = (1000L * end) % downsampler.getInterval();
      final long interval_aligned_ts = end +
        (downsampler.getInterval() - interval_offset) / 1000L;

      // Then, if we're now aligned on a timespan boundary, then we need no
      // further adjustment: we are guaranteed to have always moved the end time
      // forward, so the scan will find the data we need.
      //
      // Otherwise, we need to align to the NEXT timespan to ensure that we scan
      // the needed data.
      final long timespan_offset = interval_aligned_ts % Const.MAX_TIMESPAN;
      return (0L == timespan_offset) ?
        interval_aligned_ts :
        interval_aligned_ts + (Const.MAX_TIMESPAN - timespan_offset);
    } else {
      // Not downsampling.
      //
      // Regardless of the end timestamp's position within the current timespan,
      // we must always align to the beginning of the next timespan. This is
      // true even if it's already aligned on a timespan boundary. Again, the
      // reason for this is OpenTSDB's closed interval vs. HBase's half-open.
      final long timespan_offset = end % Const.MAX_TIMESPAN;
      return end + (Const.MAX_TIMESPAN - timespan_offset);
    }
  }

  /**
   * Sets the server-side regexp filter on the scanner.
   * In order to find the rows with the relevant tags, we use a
   * server-side filter that matches a regular expression on the row key.
   * @param scanner The scanner on which to add the filter.
   */
  private void createAndSetFilter(final Scanner scanner) {
    QueryUtil.setDataTableScanFilter(scanner, group_bys, row_key_literals, 
        explicit_tags, enable_fuzzy_filter, 
        (end_time == UNSET
        ? -1  // Will scan until the end (0xFFF...).
        : (int) getScanEndTimeSeconds()));
  }
  
  /**
   * Sets the server-side regexp filter on the scanner.
   * This will compile a list of the tagk/v pairs for the TSUIDs to prevent
   * storage from returning irrelevant rows.
   * @param scanner The scanner on which to add the filter.
   * @since 2.0
   */
  private void createAndSetTSUIDFilter(final Scanner scanner) {
    if (regex == null) {
      regex = QueryUtil.getRowKeyTSUIDRegex(tsuids);
    }
    scanner.setKeyRegexp(regex, CHARSET);
  }
  
  /**
   * Return the query index that maps this datapoints to the original subquery 
   * @return index of the query in the TSQuery class
   * @since 2.4
   */
  @Override
  public int getQueryIdx() {
    return query_index;
  }
  
  /**
   * set the index that link this query to the original index.
   * @param idx query index idx
   * @since 2.4
   */
  public void setQueryIdx(int idx) {
    query_index = idx;
  }
  
  /**
   * Transform downsampler properties to rollup properties, if the rollup
   * is enabled at configuration level and down sampler is set.
   * It falls back to raw data and down sampling if there is no 
   * RollupInterval is configured against this down sample interval
   * @param group_by The group by aggregator.
   * @param str_interval String representation of the  interval, for logging
   * @since 2.4
   */
  public void transformDownSamplerToRollupQuery(final Aggregator group_by, 
      final String str_interval)  {
    
    if (downsampler != null && downsampler.getInterval() > 0) {
      if (tsdb.getRollupConfig() != null) {
        try {
          best_match_rollups = tsdb.getRollupConfig().
              getRollupInterval(downsampler.getInterval() / 1000, str_interval);
          //It is thread safe as each thread will be working on unique 
          // TsdbQuery object
          //RollupConfig.getRollupInterval guarantees that, 
          //  it always return a non-empty list
          // TODO
          rollup_query = new RollupQuery(best_match_rollups.remove(0), 
                  downsampler.getFunction(), downsampler.getInterval(),
                  group_by);
          if (group_by == Aggregators.COUNT) {
            aggregator = Aggregators.SUM;
          }
        }
        catch (NoSuchRollupForIntervalException nre) {
          LOG.error("There is no such rollup for the downsample interval "
            + str_interval + ". So fall back to the  default tsdb down"
            + " sampling approach and it requires raw data scan." );
          //nullify the rollup_query if this api is called explicitly
          rollup_query = null;
          return;
        }
        
        if (rollup_query.getRollupInterval().isDefaultInterval()) {
          //Anyways it is a scan on raw data
          rollup_query = null;
        }
      }       
    }
  }
  
  /**
   * Transform rollup query to downsampler
   * It is mainly useful when it scan on raw data on fallback.
   * @since 2.4
   */
  private void transformRollupQueryToDownSampler()  {
    
    if (rollup_query != null) {
      // TODO - clean up and handle fill
      downsampler = new DownsamplingSpecification(
          rollup_query.getRollupInterval().getIntervalSeconds() * 1000, 
          rollup_query.getRollupAgg(),
          (downsampler != null ? downsampler.getFillPolicy() : 
            FillPolicy.ZERO));
      rollup_query = null;
    }
  }
  
  @Override
  public String toString() {
    final StringBuilder buf = new StringBuilder();
    buf.append("TsdbQuery(start_time=")
       .append(getStartTime())
       .append(", end_time=")
       .append(getEndTime());
    if (tsuids != null && !tsuids.isEmpty()) {
      buf.append(", tsuids=");
      for (final String tsuid : tsuids) {
        buf.append(tsuid).append(",");
      }
    } else {
      buf.append(", metric=").append(Arrays.toString(metric));
      buf.append(", filters=[");
      for (final Iterator<TagVFilter> it = filters.iterator(); it.hasNext(); ) {
        buf.append(it.next());
        if (it.hasNext()) {
          buf.append(',');
        }
      }
      buf.append("], rate=").append(rate)
        .append(", aggregator=").append(aggregator)
        .append(", group_bys=(");
      if (group_bys != null) {
        for (final byte[] tag_id : group_bys) {
          try {
            buf.append(tsdb.tag_names.getName(tag_id));
          } catch (NoSuchUniqueId e) {
            buf.append('<').append(e.getMessage()).append('>');
          }
          buf.append(' ')
             .append(Arrays.toString(tag_id));
          if (row_key_literals != null) {
            final byte[][] value_ids = row_key_literals.get(tag_id);
            if (value_ids == null) {
              continue;
            }
            buf.append("={");
            for (final byte[] value_id : value_ids) {
              try {
                if (value_id != null) {
                  buf.append(tsdb.tag_values.getName(value_id));
                } else {
                  buf.append("null");
                }
              } catch (NoSuchUniqueId e) {
                buf.append('<').append(e.getMessage()).append('>');
              }
              buf.append(' ')
                 .append(Arrays.toString(value_id))
                 .append(", ");
            }
            buf.append('}');
          }
          buf.append(", ");
        }
      }
    }
    buf.append(")")
     .append(", rollup=")
     .append(RollupQuery.isValidQuery(rollup_query))
     .append("))");
    return buf.toString();
  }
  
  public Boolean getNoResults() {
    return no_results;
  }

  public void setNoResults(Boolean noResults) {
    this.no_results = noResults;
  }
  
  /**
   * Comparator that ignores timestamps in row keys.
   */
  static final class SpanCmp implements Comparator<byte[]> {

    private final short metric_width;

    public SpanCmp(final short metric_width) {
      this.metric_width = metric_width;
    }

    @Override
    public int compare(final byte[] a, final byte[] b) {
      final int length = Math.min(a.length, b.length);
      if (a == b) {  // Do this after accessing a.length and b.length
        return 0;    // in order to NPE if either a or b is null.
      }
      int i;
      // First compare the metric ID.
      for (i = 0; i < metric_width; i++) {
        if (a[i] != b[i]) {
          return (a[i] & 0xFF) - (b[i] & 0xFF);  // "promote" to unsigned.
        }
      }
      // Then skip the timestamp and compare the rest.
      for (i += Const.TIMESTAMP_BYTES; i < length; i++) {
        if (a[i] != b[i]) {
          return (a[i] & 0xFF) - (b[i] & 0xFF);  // "promote" to unsigned.
        }
      }
      return a.length - b.length;
    }

  }

  /** Helps unit tests inspect private methods. */
  @VisibleForTesting
  static class ForTesting {

    /** @return the start time of the HBase scan for unit tests. */
    static long getScanStartTimeSeconds(final TsdbQuery query) {
      return query.getScanStartTimeSeconds();
    }

    /** @return the end time of the HBase scan for unit tests. */
    static long getScanEndTimeSeconds(final TsdbQuery query) {
      return query.getScanEndTimeSeconds();
    }

    /** @return the downsampling interval for unit tests. */
    static long getDownsampleIntervalMs(final TsdbQuery query) {
      return query.downsampler.getInterval();
    }
  
    static byte[] getMetric(final TsdbQuery query) {
      return query.metric;
    }
    
    static RateOptions getRateOptions(final TsdbQuery query) {
      return query.rate_options;
    }
    
    static List<TagVFilter> getFilters(final TsdbQuery query) {
      return query.filters;
    }
    
    static ArrayList<byte[]> getGroupBys(final TsdbQuery query) {
      return query.group_bys;
    }
    
    static ByteMap<byte[][]> getRowKeyLiterals(final TsdbQuery query) {
      return query.row_key_literals;
    }
  
    static long maxBytes(final TsdbQuery query) {
      return query.max_bytes;
    }
    
    static long maxDataPoints(final TsdbQuery query) {
      return query.max_data_points;
    }
    
  }
}
