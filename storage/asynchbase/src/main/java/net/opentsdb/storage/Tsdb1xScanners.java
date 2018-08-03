// This file is part of OpenTSDB.
// Copyright (C) 2010-2018  The OpenTSDB Authors.
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
package net.opentsdb.storage;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.hbase.async.Bytes.ByteMap;
import org.hbase.async.FilterList.Operator;
import org.hbase.async.KeyRegexpFilter;
import org.hbase.async.BinaryPrefixComparator;
import org.hbase.async.CompareFilter;
import org.hbase.async.FilterList;
import org.hbase.async.FuzzyRowFilter;
import org.hbase.async.QualifierFilter;
import org.hbase.async.ScanFilter;
import org.hbase.async.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.stumbleupon.async.Callback;

import net.opentsdb.configuration.Configuration;
import net.opentsdb.core.Const;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QuerySourceConfig;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.filter.ExplicitTagsFilter;
import net.opentsdb.query.filter.NotFilter;
import net.opentsdb.query.filter.QueryFilter;
import net.opentsdb.query.filter.TagValueFilter;
import net.opentsdb.query.filter.TagValueLiteralOrFilter;
import net.opentsdb.query.filter.TagValueRegexFilter;
import net.opentsdb.query.filter.TagValueWildcardFilter;
import net.opentsdb.query.processor.rate.Rate;
import net.opentsdb.rollup.RollupInterval;
import net.opentsdb.rollup.RollupUtils;
import net.opentsdb.rollup.RollupUtils.RollupUsage;
import net.opentsdb.stats.Span;
import net.opentsdb.storage.schemas.tsdb1x.ResolvedChainFilter;
import net.opentsdb.storage.schemas.tsdb1x.ResolvedPassThroughFilter;
import net.opentsdb.storage.schemas.tsdb1x.ResolvedQueryFilter;
import net.opentsdb.storage.schemas.tsdb1x.ResolvedTagValueFilter;
import net.opentsdb.storage.schemas.tsdb1x.Schema;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.uid.UniqueIdType;
import net.opentsdb.utils.ByteSet;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.DateTime;

/**
 * The owner/container for one or more HBase scanners used to execute a
 * query for a single metric and optional filter. This used to be the 
 * {@code TsdbQuery} class in TSDB 1/2x.
 * <p>
 * The class is responsible for converting the metric and optional filters
 * to their assigned UIDs. Then it will setup the {@link Scanner} with the 
 * appropriate filters and setup a {@link Tsdb1xScanner} for each HBase 
 * scanner.
 * <p>
 * To fetch data, call {@link #fetchNext(Tsdb1xQueryResult, Span)} and it
 * will perform the initialization on the first call. 
 * <b>Note:</b> Subsequent calls to {@link #fetchNext(Tsdb1xQueryResult, Span)}
 * should only be made after this scanner has responded with a result. 
 * Only one {@link Tsdb1xQueryNode} can be filled at a time.
 * <p>
 * The class also handles rollup queries with fallback when so configured.
 * Currently fallback is limited to trying the next higher resolution 
 * interval when the result from the lower resolution scan returned an
 * empty time series set.
 * TODO - handle downsampling of higher resolution data
 * 
 * @since 3.0
 */
public class Tsdb1xScanners implements HBaseExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(Tsdb1xScanners.class);
  
  /** The upstream query node that owns this scanner set. */
  protected final Tsdb1xQueryNode node;
  
  /** The data source config. */
  protected final QuerySourceConfig source_config;
  
  /** Search the query on pre-aggregated table directly instead of post fetch 
   * aggregation. */
  protected final boolean pre_aggregate;
  
  /** Whether or not to skip NoSuchUniqueName errors for tag keys on resolution. */
  protected final boolean skip_nsun_tagks;
  
  /** Whether or not to skip NoSuchUniqueName errors for tag values on resolution. */
  protected final boolean skip_nsun_tagvs;

  /** The limit on literal tag value expansion when crafting the scanner
   * filter to send to HBase. */
  protected final int expansion_limit;
  
  /** The number of rows to scan per call to {@link Scanner#nextRows()} */
  protected final int rows_per_scan;
  
  /** Whether or not to enable the fuzzy filter. */
  protected final boolean enable_fuzzy_filter;
  
  /** Whether or not we're scanning in reverse. */
  protected final boolean reverse_scan;
  
  /** The maximum cardinality to allow in determining if we can switch to
   * multi-gets. */
  protected final int max_multi_get_cardinality;
  
  /** Whether or not the scanners have been initialized. */
  protected volatile boolean initialized;
  
  /** The scanners configured post initialization. If only the raw table is
   * scanned, the list will have a size of 1 with a {@link Tsdb1xScanner} 
   * per salt bucket. If rollups are enabled, the list will have scanners
   * configured for rollups starting with the lowest resolution at 0 and
   * working up to the raw table if fallback was configured. 
   */
  protected List<Tsdb1xScanner[]> scanners;
  
  /** The current index used for fetching data within the 
   * {@link #scanners} list. */
  protected int scanner_index;
  
  /** The filter callback class instantiated when the query had filters
   * and used to pull out variables after initialization. */
  protected FilterCB filter_cb; 
  
  /** How many scanners have checked in with results post {@link #scanNext(Span)}
   * calls. <b>WARNING</b> Must be synchronized!. */
  protected volatile int scanners_done;
  
  /** The current result set by {@link #fetchNext(Tsdb1xQueryResult, Span)}. */
  protected Tsdb1xQueryResult current_result;
  
  /** Tag key and values to use in the row key filter, all pre-sorted */
  protected ByteMap<List<byte[]>> row_key_literals;
  
  /** Whether or not the scanner set is in a failed state and children 
   * should close. */
  protected volatile boolean has_failed;
  
  /**
   * Default ctor.
   * @param node A non-null parent node.
   * @param source_config A non-null query with a single metric and optional filter
   * matching the metric.
   * @throws IllegalArgumentException if the node or query were null.
   */
  public Tsdb1xScanners(final Tsdb1xQueryNode node, 
                        final QuerySourceConfig source_config) {
    if (node == null) {
      throw new IllegalArgumentException("Node cannot be null.");
    }
    if (source_config == null) {
      throw new IllegalArgumentException("Config cannot be null.");
    }
    this.node = node;
    this.source_config = source_config;
    
    final Configuration config = node.parent()
        .tsdb().getConfig();
    if (source_config.hasKey(Tsdb1xHBaseDataStore.EXPANSION_LIMIT_KEY)) {
      expansion_limit = source_config.getInt(config, 
          Tsdb1xHBaseDataStore.EXPANSION_LIMIT_KEY);
    } else {
      expansion_limit = node.parent()
          .dynamicInt(Tsdb1xHBaseDataStore.EXPANSION_LIMIT_KEY);
    }
    if (source_config.hasKey(Tsdb1xHBaseDataStore.ROWS_PER_SCAN_KEY)) {
      rows_per_scan = source_config.getInt(config, 
          Tsdb1xHBaseDataStore.ROWS_PER_SCAN_KEY);
    } else {
      rows_per_scan = node.parent()
          .dynamicInt(Tsdb1xHBaseDataStore.ROWS_PER_SCAN_KEY);
    }
    if (source_config.hasKey(Tsdb1xHBaseDataStore.SKIP_NSUN_TAGK_KEY)) {
      skip_nsun_tagks = source_config.getBoolean(config, 
          Tsdb1xHBaseDataStore.SKIP_NSUN_TAGK_KEY);
    } else {
      skip_nsun_tagks = node.parent()
          .dynamicBoolean(Tsdb1xHBaseDataStore.SKIP_NSUN_TAGK_KEY);
    }
    if (source_config.hasKey(Tsdb1xHBaseDataStore.SKIP_NSUN_TAGV_KEY)) {
      skip_nsun_tagvs = source_config.getBoolean(config, 
          Tsdb1xHBaseDataStore.SKIP_NSUN_TAGV_KEY);
    } else {
      skip_nsun_tagvs = node.parent()
          .dynamicBoolean(Tsdb1xHBaseDataStore.SKIP_NSUN_TAGV_KEY);
    }
    if (source_config.hasKey(Tsdb1xHBaseDataStore.PRE_AGG_KEY)) {
      pre_aggregate = source_config.getBoolean(config, 
          Tsdb1xHBaseDataStore.PRE_AGG_KEY);
    } else {
      pre_aggregate = false;
    }
    if (source_config.hasKey(Tsdb1xHBaseDataStore.FUZZY_FILTER_KEY)) {
      enable_fuzzy_filter = source_config.getBoolean(config, 
          Tsdb1xHBaseDataStore.FUZZY_FILTER_KEY);
    } else {
      enable_fuzzy_filter = node.parent()
          .dynamicBoolean(Tsdb1xHBaseDataStore.FUZZY_FILTER_KEY);
    }
    if (source_config.hasKey(Schema.QUERY_REVERSE_KEY)) {
      reverse_scan = source_config.getBoolean(config, 
          Schema.QUERY_REVERSE_KEY);
    } else {
      reverse_scan = node.parent()
          .dynamicBoolean(Schema.QUERY_REVERSE_KEY);
    }
    if (source_config.hasKey(Tsdb1xHBaseDataStore.MAX_MG_CARDINALITY_KEY)) {
      max_multi_get_cardinality = source_config.getInt(config, 
          Tsdb1xHBaseDataStore.MAX_MG_CARDINALITY_KEY);
    } else {
      max_multi_get_cardinality = node.parent()
          .dynamicInt(Tsdb1xHBaseDataStore.MAX_MG_CARDINALITY_KEY);
    }
  }
  
  /**
   * Call to fetch the next set of data from the scanners. 
   * <b>WARNING:</b> Do not call this from the parent node without receiving
   * a response first. Only one call can be outstanding at a time.
   * 
   * @param result A non-null result.
   * @param span An optional span.
   * @throws IllegalArgumentException if the result is null.
   */
  public void fetchNext(final Tsdb1xQueryResult result, final Span span) {
    if (result == null) {
      throw new IllegalArgumentException("Result must be initialized");
    }
    
    synchronized (this) {
      if (current_result != null) {
        throw new IllegalStateException("Query result must have been null "
            + "to start another query!");
      }
      current_result = result;
    }
    // just extra safe locking. Shouldn't ever happen.
    if (!initialized) {
      synchronized (this) {
        if (!initialized) {
          initialize(span);
          return;
        } else {
          throw new IllegalStateException("Lost initialization race. "
              + "Who called me? This shouldn't happen");
        }
      }
    }
    
    scanner_index = 0;
    scanNext(span);
  }
  
  /** @return Whether or not a child scanner or config has thrown an 
   * exception. */
  boolean hasException() {
    return has_failed;
  }
  
  /** Called by a child when the scanner has finished it's current run. */
  void scannerDone() {
    boolean send_upstream = false;
    synchronized (this) {
      scanners_done++;
      if (scanners_done >= scanners.get(scanner_index).length) {
        if (current_result == null) {
          throw new IllegalStateException("Current result was null but "
              + "all scanners were finished.");
        }
        send_upstream = true;
      }
    }
    
    if (send_upstream) {
      try {
        scanners_done = 0;
        if (scanners.size() == 1 || scanner_index + 1 >= scanners.size()) {
          // swap and null
          final Tsdb1xQueryResult result;
          synchronized (this) {
            result = current_result;
            current_result = null;
          }
          node.onNext(result);
        } else {
          if ((current_result.timeSeries() == null || 
              current_result.timeSeries().isEmpty()) && 
              scanner_index + 1 < scanners.size()) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Scanner index at [" + scanner_index 
                  + "] returned an empty set, falling back.");
            }
            // fall back!
            scanner_index++;
            scanners_done = 0;
            scanNext(null /** TODO - span */);
          } else {
            final Tsdb1xQueryResult result;
            synchronized (this) {
              result = current_result;
              current_result = null;
            }
            node.onNext(result);
          }
        }
      } catch (Exception e) {
        LOG.error("Unexpected exception handling scanner complete", e);
        node.onError(e);
      }
    }
  }
  
  /** @return Whether or not to filter during scans. */
  public boolean filterDuringScan() {
    return filter_cb == null ? false : filter_cb.filter_during_scans;
  }
  
  /** @return Whether or not we could use multi-gets instead. */
  public boolean couldMultiGet() {
    return filter_cb == null ? false : filter_cb.could_multi_get;
  }
  
  /**
   * Called by children when they encounter an exception. Only passes
   * the first exception upstream. Subsequent exceptions are logged at
   * debug.
   * 
   * @param t A non-null exception.
   * @throws IllegalArgumentException if the exception was null.
   */
  public synchronized void exception(final Throwable t) {
    if (t == null) {
      throw new IllegalArgumentException("Throwable cannot be null.");
    }
    
    if (has_failed) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Exception received after having been marked as failed", t);
      }
      return;
    }
    
    has_failed = true;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Exception from downstream", t);
    }
    node.onError(t);
  }

  @Override
  public void close() {
    if (scanners != null) {
      for (final Tsdb1xScanner[] scnrs : scanners) {
        for (final Tsdb1xScanner scanner : scnrs) {
          try {
            scanner.close();
          } catch (Exception e) {
            LOG.warn("Failed to close scanner: " + scanner, e);
          }
        }
      }
    }
  }
  
  @Override
  public State state() {
    if (!initialized && scanners == null) {
      return State.CONTINUE;
    }
    for (final Tsdb1xScanner scanner : scanners.get(scanner_index)) {
      if (scanner.state() == State.CONTINUE) {
        return State.CONTINUE;
      } else if (scanner.state() == State.EXCEPTION) {
        return State.EXCEPTION;
      }
    }
    return State.COMPLETE;
  }
  
  /**
   * Configures the start row key for a scanner with room for salt.
   * @param metric A non-null and non-empty metric UID.
   * @param rollup_interval An optional rollup interval.
   * @param fuzzy_key An optional fuzzy row key when enabled.
   * @return A non-null and non-empty byte array.
   */
  byte[] setStartKey(final byte[] metric, 
                     final RollupInterval rollup_interval,
                     final byte[] fuzzy_key) {
    long start = ((SemanticQuery) source_config.getQuery()).startTime().epoch();
    
    final Collection<QueryNode> rates = 
        node.pipelineContext().upstreamOfType(node, Rate.class);
    if (rollup_interval != null) {
      if (!rates.isEmpty()) {
        start = RollupUtils.getRollupBasetime(start - 1, rollup_interval);
      } else {
        start = RollupUtils.getRollupBasetime(start, rollup_interval);
      }
    } else {
      // First, we align the start timestamp to its representative value for the
      // interval in which it appears, if downsampling.
      
      // TODO - doesn't account for calendaring, etc.
      if (node.downsampleConfig() != null) {
        final long interval = DateTime.parseDuration(
            node.downsampleConfig().intervalAsString());
        if (interval > 0) {
          final long interval_offset = (1000L * start) % interval;
          start -= interval_offset / 1000L;
        }
      }
      
      // Then snap that timestamp back to its representative value for the
      // timespan in which it appears.
      final long timespan_offset = start % Schema.MAX_RAW_TIMESPAN;
      start -= timespan_offset;
    }
    
    // Don't return negative numbers.
    start = start > 0L ? start : 0L;
    
    final byte[] start_key;
    if (fuzzy_key != null) {
      start_key = Arrays.copyOf(fuzzy_key, fuzzy_key.length);
    } else {
      start_key = new byte[node.schema().saltWidth() + 
                           node.schema().metricWidth() +
                           Schema.TIMESTAMP_BYTES];
    }
    System.arraycopy(metric, 0, start_key, node.schema().saltWidth(), metric.length);
    Bytes.setInt(start_key, (int) start, (node.schema().saltWidth() + 
                                          node.schema().metricWidth()));
    return start_key;
  }

  /**
   * Configures the stop row key for a scanner with room for salt.
   * @param metric A non-null and non-empty metric UID.
   * @param rollup_interval An optional rollup interval.
   * @return A non-null and non-empty byte array.
   */
  byte[] setStopKey(final byte[] metric, final RollupInterval rollup_interval) {
    long end = ((SemanticQuery) source_config.getQuery()).endTime().epoch();
    
    if (rollup_interval != null) {
      // TODO - need rollup end time here
      end = RollupUtils.getRollupBasetime(end + 
          (rollup_interval.getIntervalSeconds() * rollup_interval.getIntervals()), 
            rollup_interval);
    } else {
      long interval = 0;
      if (node.downsampleConfig() != null) {
        interval = DateTime.parseDuration(
            node.downsampleConfig().intervalAsString());
      }

      if (interval > 0) {
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
        final long interval_offset = (1000L * end) % interval;
        final long interval_aligned_ts = end +
          (interval - interval_offset) / 1000L;
     
        // Then, if we're now aligned on a timespan boundary, then we need no
        // further adjustment: we are guaranteed to have always moved the end time
        // forward, so the scan will find the data we need.
        //
        // Otherwise, we need to align to the NEXT timespan to ensure that we scan
        // the needed data.
        final long timespan_offset = interval_aligned_ts % Schema.MAX_RAW_TIMESPAN;
        end = (0L == timespan_offset) ?
          interval_aligned_ts :
          interval_aligned_ts + (Schema.MAX_RAW_TIMESPAN - timespan_offset);
      } else {
        // Not downsampling.
        //
        // Regardless of the end timestamp's position within the current timespan,
        // we must always align to the beginning of the next timespan. This is
        // true even if it's already aligned on a timespan boundary. Again, the
        // reason for this is OpenTSDB's closed interval vs. HBase's half-open.
        final long timespan_offset = end % Schema.MAX_RAW_TIMESPAN;
        end += (Schema.MAX_RAW_TIMESPAN - timespan_offset);
      }
    }
    
    final byte[] end_key = new byte[node.schema().saltWidth() + 
                                      node.schema().metricWidth() +
                                      Schema.TIMESTAMP_BYTES];
    System.arraycopy(metric, 0, end_key, node.schema().saltWidth(), metric.length);
    Bytes.setInt(end_key, (int) end, (node.schema().saltWidth() + 
                                      node.schema().metricWidth()));
    return end_key;
  }

  /**
   * Initializes the scanners on the first call to 
   * {@link #fetchNext(Tsdb1xQueryResult, Span)}. Starts with resolving
   * the metric to a UID and filters.
   * @param span An optional span.
   */
  void initialize(final Span span) {
    final Span child;
    if (span != null && span.isDebug()) {
      child = span.newChild(getClass().getName() + ".initialize")
                  .withTag("query", source_config.toString())
                  .start();
    } else {
      child = span;
    }
    
    class ErrorCB implements Callback<Object, Exception> {
      @Override
      public Object call(final Exception ex) throws Exception {
        if (child != null) {
          child.setErrorTags(ex)
               .finish();
        }
        node.onError(ex);
        has_failed = true;
        return null;
      }
    }
    
    // resolve metric name
    class MetricCB implements Callback<Object, byte[]> {
      @Override
      public Object call(final byte[] metric) throws Exception {
        if (metric == null) {
          final NoSuchUniqueName ex = new NoSuchUniqueName(Schema.METRIC_TYPE, 
              source_config.getMetric().metric());
          if (child != null) {
            child.setErrorTags(ex)
                 .finish();
          }
          node.onError(ex);
          has_failed = true;
          return null;
        }
        
        if (!Strings.isNullOrEmpty(source_config.getFilterId())) {
          final QueryFilter filter;
          if (source_config.getQuery() instanceof SemanticQuery) {
            filter = ((SemanticQuery) source_config.getQuery())
                .getFilter(source_config.getFilterId());
          } else {
            throw new UnsupportedOperationException("We don't support " 
                + source_config.getQuery().getClass() + " yet");
          }
          if (filter == null) {
            throw new IllegalStateException("No filter was found for: " 
                + source_config.getFilterId());
          }
          
          filter_cb = new FilterCB(metric, child);
          node.schema().resolveUids(filter, child)
            .addCallback(filter_cb)
            .addErrback(new ErrorCB());
        } else if (source_config.getFilter() != null) {
          filter_cb = new FilterCB(metric, child);
          node.schema().resolveUids(source_config.getFilter(), child)
            .addCallback(filter_cb)
            .addErrback(new ErrorCB());
        } else {
          setupScanners(metric, child);
          if (child != null) {
            child.setSuccessTags()
                 .finish();
          }
        }
        return null;
      }
    }
    
    try {
      node.schema().getId(UniqueIdType.METRIC, source_config.getMetric().metric(), 
          child)
        .addCallback(new MetricCB())
        .addErrback(new ErrorCB());
    } catch (Exception e) {
      LOG.error("Unexpected exception", e);
      if (child != null) {
        child.setErrorTags(e)
             .finish();
      }
      node.onError(e);
    }
  }
  
  /**
   * Called post UID resolution to setup the scanners.
   * @param metric A non-null and non-empty metric UID.
   * @param span An optional tracer.
   */
  void setupScanners(final byte[] metric, final Span span) {
    final Span child;
    if (span != null && span.isDebug()) {
      child = span.newChild(getClass().getName() + ".setupScanners")
                  .start();
    } else {
      child = null;
    }
    
    try {
      int size = node.rollupIntervals() == null ? 
          1 : node.rollupIntervals().size() + 1;
      scanners = Lists.newArrayListWithCapacity(size);
      final byte[] fuzzy_key;
      final byte[] fuzzy_mask;
      final String regex;
      if (row_key_literals != null) {
        if (filter_cb != null && filter_cb.explicit_tags && enable_fuzzy_filter) {
          fuzzy_key = new byte[node.schema().saltWidth() 
                               + node.schema().metricWidth() 
                               + Schema.TIMESTAMP_BYTES 
                               + (row_key_literals.size() * 
                                   (node.schema().tagkWidth() 
                                       + node.schema().tagvWidth()))];
          // copy the metric UID into the fuzzy key.
          System.arraycopy(metric, 0, fuzzy_key, 
              node.schema().saltWidth(), metric.length);
          fuzzy_mask = new byte[fuzzy_key.length];
        } else {
          fuzzy_key = null;
          fuzzy_mask = null;
        }
        
        regex = QueryUtil.getRowKeyUIDRegex(
            node.schema(),
            row_key_literals, 
            filter_cb != null ? filter_cb.explicit_tags : false, 
            fuzzy_key, 
            fuzzy_mask);
        if (Strings.isNullOrEmpty(regex)) {
          throw new RuntimeException("WTF????");
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Scanner regular expression: " + 
              QueryUtil.byteRegexToString(node.schema(), regex));
        }
      } else {
        fuzzy_key = null;
        fuzzy_mask = null;
        regex = null;
      }
      
      final ScanFilter rollup_filter;
      if (node.rollupIntervals() != null && 
          !node.rollupIntervals().isEmpty() && 
          node.rollupUsage() != RollupUsage.ROLLUP_RAW) {
        
        // set qualifier filters
        if (node.rollupAggregation() != null && 
            node.rollupAggregation().equals("avg")) {
          // old and new schemas with literal agg names or prefixes.
          final List<ScanFilter> filters = Lists.newArrayListWithCapacity(4);
          filters.add(new QualifierFilter(CompareFilter.CompareOp.EQUAL,
              new BinaryPrefixComparator("sum".getBytes(Const.ASCII_CHARSET))));
          filters.add(new QualifierFilter(CompareFilter.CompareOp.EQUAL,
              new BinaryPrefixComparator("count".getBytes(Const.ASCII_CHARSET))));
          filters.add(new QualifierFilter(CompareFilter.CompareOp.EQUAL,
              new BinaryPrefixComparator(new byte[] { 
                  (byte) node.schema().rollupConfig().getIdForAggregator("sum")
              })));
          filters.add(new QualifierFilter(CompareFilter.CompareOp.EQUAL,
              new BinaryPrefixComparator(new byte[] { 
                  (byte) node.schema().rollupConfig().getIdForAggregator("count")
              })));
          
          rollup_filter = new FilterList(filters, Operator.MUST_PASS_ONE);
        } else {
          // it's another aggregation
          final List<ScanFilter> filters = Lists.newArrayListWithCapacity(2);
          filters.add(new QualifierFilter(CompareFilter.CompareOp.EQUAL,
              new BinaryPrefixComparator(node.rollupAggregation()
                  .getBytes(Const.ASCII_CHARSET))));
          filters.add(new QualifierFilter(CompareFilter.CompareOp.EQUAL,
              new BinaryPrefixComparator(new byte[] { 
                  (byte) node.schema().rollupConfig()
                    .getIdForAggregator(node.rollupAggregation())
              })));
          
          rollup_filter = new FilterList(filters, Operator.MUST_PASS_ONE);
        }
      } else {
        rollup_filter = null;
      }
      
      int idx = 0;
      if (node.rollupIntervals() != null && 
          !node.rollupIntervals().isEmpty() && 
          node.rollupUsage() != RollupUsage.ROLLUP_RAW) {
        
        for (int i = 0; i < node.rollupIntervals().size(); i++) {
          final RollupInterval interval = node.rollupIntervals().get(idx);
          final Tsdb1xScanner[] array = new Tsdb1xScanner[node.schema().saltWidth() > 0 ? 
              node.schema().saltBuckets() : 1];
          scanners.add(array);
          final byte[] start_key = setStartKey(metric, interval, fuzzy_key);
          final byte[] stop_key = setStopKey(metric, interval);
          
          for (int x = 0; x < array.length; x++) {
            final Scanner scanner = node.parent()
                .client().newScanner(pre_aggregate ? 
                    interval.getGroupbyTable() : interval.getTemporalTable());
            
            scanner.setFamily(Tsdb1xHBaseDataStore.DATA_FAMILY);
            scanner.setMaxNumRows(rows_per_scan);
            scanner.setReversed(reverse_scan);
            
            if (node.schema().saltWidth() > 0) {
              final byte[] start_clone = Arrays.copyOf(start_key, start_key.length);
              final byte[] stop_clone = Arrays.copyOf(stop_key, stop_key.length);
              node.schema().prefixKeyWithSalt(start_clone, x);
              node.schema().prefixKeyWithSalt(stop_clone, x);
              scanner.setStartKey(start_clone);
              scanner.setStopKey(stop_clone);
            } else {
              // no copying needed, just dump em in
              scanner.setStartKey(start_key);
              scanner.setStopKey(stop_key);
            }
            
            setScannerFilter(scanner, x, regex, fuzzy_key, fuzzy_mask, rollup_filter);
            
            if (LOG.isDebugEnabled()) {
              LOG.debug("Instantiating rollup: " + scanner);
            }
            
            array[x] = new Tsdb1xScanner(this, scanner, x, interval);
          }
          idx++;
          
          // bail out
          if (node.rollupUsage() == RollupUsage.ROLLUP_NOFALLBACK && idx > 0) {
            break;
          }
        }
      }
  
      // raw scanner here if applicable
      if (node.rollupIntervals() == null || 
          node.rollupIntervals().isEmpty() || 
          node.rollupUsage() != RollupUsage.ROLLUP_NOFALLBACK) {
        
        final Tsdb1xScanner[] array = new Tsdb1xScanner[node.schema().saltWidth() > 0 ? 
            node.schema().saltBuckets() : 1];
        scanners.add(array);
        
        final byte[] start_key = setStartKey(metric, null, fuzzy_key);
        final byte[] stop_key = setStopKey(metric, null);
        
        for (int i = 0; i < array.length; i++) {
          final Scanner scanner = node.parent()
              .client().newScanner(node.parent().dataTable());
          
          scanner.setFamily(Tsdb1xHBaseDataStore.DATA_FAMILY);
          scanner.setMaxNumRows(rows_per_scan);
          scanner.setReversed(reverse_scan);
          
          if (node.schema().saltWidth() > 0) {
            final byte[] start_clone = Arrays.copyOf(start_key, start_key.length);
            final byte[] stop_clone = Arrays.copyOf(stop_key, stop_key.length);
            node.schema().prefixKeyWithSalt(start_clone, i);
            node.schema().prefixKeyWithSalt(stop_clone, i);
            scanner.setStartKey(start_clone);
            scanner.setStopKey(stop_clone);
          } else {
            // no copying needed, just dump em in
            scanner.setStartKey(start_key);
            scanner.setStopKey(stop_key);
          }
          
          setScannerFilter(scanner, i, regex, fuzzy_key, fuzzy_mask, null);
          
          if (LOG.isDebugEnabled()) {
            LOG.debug("Instantiating raw table scanner: " + scanner);
          }
          
          array[i] = new Tsdb1xScanner(this, scanner, i, null);
        }
      }
      initialized = true;
    } catch (Exception e) {
      if (child != null) {
        child.setErrorTags(e)
             .finish();
      }
      e.printStackTrace();
      throw e;
    }
    
    if (child != null) {
      child.setSuccessTags()
           .finish();
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Configured " + scanners.size() + " scanner sets with " 
          + scanners.get(0).length + " scanners per set.");
    }
    scanNext(span);
  }
  
  /**
   * Compiles the filter list to add to a scanner when applicable.
   * @param scanner A non-null scanner to add the filters to.
   * @param salt_bucket An optional salt bucket
   * @param regex An optional regular expression to match.
   * @param fuzzy_key An optional fuzzy row key filter.
   * @param fuzzy_mask An optional mask for fuzzy matching. Can't be null
   * if the fuzzy_key was set.
   * @param rollup_filter An optional rollup filter.
   */
  void setScannerFilter(final Scanner scanner, 
                        final int salt_bucket, 
                        final String regex, 
                        final byte[] fuzzy_key, 
                        final byte[] fuzzy_mask, 
                        final ScanFilter rollup_filter) {
    if (regex == null && fuzzy_key == null && rollup_filter == null) {
      return;
    }
    
    List<ScanFilter> filters = Lists.newArrayListWithCapacity(3);
    if (fuzzy_key != null) {
      final byte[] key = node.schema().saltWidth() < 1 ? 
          fuzzy_key : Arrays.copyOf(fuzzy_key, fuzzy_key.length);
      if (node.schema().saltWidth() > 0) {
        node.schema().prefixKeyWithSalt(key, salt_bucket);
      }
      filters.add(new FuzzyRowFilter(
              new FuzzyRowFilter.FuzzyFilterPair(key, fuzzy_mask)));
    }
    
    if (regex != null) {
      filters.add(new KeyRegexpFilter(regex, Const.ASCII_CHARSET));
    }
    
    if (rollup_filter != null) {
      filters.add(rollup_filter);
    }
    
    if (filters.size() == 1) {
      scanner.setFilter(filters.get(0));
    } else {
      scanner.setFilter(new FilterList(filters, Operator.MUST_PASS_ALL));
    }
  }
  
  /**
   * Called from {@link #fetchNext(Tsdb1xQueryResult, Span)} to iterate
   * over the current scanner index set and call 
   * {@link Tsdb1xScanner#fetchNext(Tsdb1xQueryResult, Span)}.
   * @param span An optional tracer.
   */
  void scanNext(final Span span) {
    // TODO - figure out how to downsample on higher resolution data
    final Tsdb1xScanner[] scnrs = scanners.get(scanner_index);
    for (final Tsdb1xScanner scanner : scnrs) {
      if (scanner.state() == State.CONTINUE) {
        try {
          scanner.fetchNext(current_result, span);
        } catch (Exception e) {
          LOG.error("Failed to execute query on scanner: " + scanner, e);
          node.onError(e);
          throw e;
        }
      } else {
        scannerDone();
      }
    }
    
    if (scanners_done == scnrs.length) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("All scanners were already complete! That was unexpected.");
      }
      
    }
  }
  
  /** A callback class to parse the query filter UID resolution callback. */
  class FilterCB implements Callback<Object, ResolvedQueryFilter> {
    final byte[] metric;
    final Span span;
    
    boolean filter_during_scans = false;
    boolean could_multi_get = true;
    boolean explicit_tags = false;
    ResolvedQueryFilter resolved;
    int total_expansion = 0;
    int cardinality = 1;
    
    FilterCB(final byte[] metric, final Span span) {
      this.metric = metric;
      this.span = span;
    }
    
    /**
     * Recursive function for walking the filter tree and building the
     * tag key UIDs to add to the HBase filter.
     * @param resolved The current non-null resolved query filter.
     */
    void processFilter(final ResolvedQueryFilter resolved) {
      if (resolved instanceof ResolvedTagValueFilter) {
        final ResolvedTagValueFilter filter = (ResolvedTagValueFilter) resolved;
        if (Bytes.isNullOrEmpty(filter.getTagKey())) {
          if (!skip_nsun_tagks || explicit_tags) {
            final NoSuchUniqueName ex = 
                new NoSuchUniqueName(Schema.TAGK_TYPE, 
                    ((TagValueFilter) filter.filter()).tagKey());
            throw ex;
          }
          
          if (LOG.isDebugEnabled()) {
            LOG.debug("Skipping tag key without an ID: " + 
                ((TagValueFilter) filter.filter()).tagKey());
          }
          return;
        }
        
        // Literal or filter
        if (resolved.filter() instanceof TagValueLiteralOrFilter) {
          final List<byte[]> tag_values = Lists.newArrayListWithCapacity(
            filter.getTagValues().size());
          for (int i = 0; i < filter.getTagValues().size(); i++) {
            final byte[] tagv = filter.getTagValues().get(i);
            if (Bytes.isNullOrEmpty(tagv)) {
              if (!skip_nsun_tagvs) {
                final NoSuchUniqueName ex = new NoSuchUniqueName(Schema.TAGV_TYPE, 
                        ((TagValueLiteralOrFilter) filter.filter()).literals().get(i));
                throw ex;
              }
              if (LOG.isDebugEnabled()) {
                LOG.debug("Dropping tag value without an ID: " 
                    + ((TagValueLiteralOrFilter) filter.filter()).literals().get(i));
              }
            } else {
              tag_values.add(tagv);
            }
          }
          
          // similar to the above, if all of the values were null we have
          // a bad query.
          if (tag_values.isEmpty()) {
            final NoSuchUniqueName ex = new NoSuchUniqueName(Schema.TAGV_TYPE, 
                    ((TagValueLiteralOrFilter) filter.filter()).literals().get(0));
            throw ex;
          }
          
          if (total_expansion + tag_values.size() > expansion_limit) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Too many literals in the row filter. Switching "
                  + "to post filtering.");
            }
            // too big to store in the scanner filter so we go slow
            putFilter(filter.getTagKey(), null);
            filter_during_scans = true;
            could_multi_get = false;
            return;
          }
          
          Collections.sort(tag_values, Bytes.MEMCMP);
          putFilter(filter.getTagKey(), tag_values);
          // discard this filter since we put it all in the strings
          total_expansion += tag_values.size();
          cardinality *= tag_values.size();
        } else {
          putFilter(filter.getTagKey(), null);
          // for match-alls, we don't need to filter
          if ((resolved.filter() instanceof TagValueWildcardFilter &&
              ((TagValueWildcardFilter) resolved.filter()).matchesAll()) ||
              (resolved.filter() instanceof TagValueRegexFilter && 
              ((TagValueRegexFilter) resolved.filter()).matchesAll())) {
            // don't filter during scans!!
          } else {
            filter_during_scans = true;
          }
          could_multi_get = false;
        }
      } else if (resolved instanceof ResolvedPassThroughFilter) {
        if (resolved.filter() instanceof NotFilter) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Skipping filters behind the NotFilter");
          }
          if (hasTagFilter(((ResolvedPassThroughFilter) resolved).resolved())) {
            filter_during_scans = true;
            could_multi_get = false;
          }
          return;
        } else if (resolved.filter() instanceof ExplicitTagsFilter) {
          // TODO - if we don't enforce explicit tags as the top-level 
          // filter then we'll run into some odd behavior.
          explicit_tags = true;
        }
        processFilter(((ResolvedPassThroughFilter) resolved).resolved());
      } else if (resolved instanceof ResolvedChainFilter) {
        for (final ResolvedQueryFilter filter : 
          ((ResolvedChainFilter) resolved).resolved()) {
          processFilter(filter);
        }
      }
    }
    
    /**
     * Helper that is used to determine if we have a tag filter behind
     * a NOT filter and need to run the filter at scan time.
     * @param resolved The non-null 
     * @return
     */
    boolean hasTagFilter(final ResolvedQueryFilter resolved) {
      if (resolved instanceof ResolvedTagValueFilter) {
        return true;
      } else if (resolved instanceof ResolvedPassThroughFilter) {
        return hasTagFilter(((ResolvedPassThroughFilter) resolved).resolved());
      } else if (resolved instanceof ResolvedChainFilter) {
        for (final ResolvedQueryFilter filter : 
          ((ResolvedChainFilter) resolved).resolved()) {
          if (hasTagFilter(filter)) {
            return true;
          }
        }
      }
      return false;
    }
    
    @Override
    public Object call(final ResolvedQueryFilter resolved) throws Exception {
      this.resolved = resolved;
      final Span child;
      if (span != null && span.isDebug()) {
        child = span.newChild(getClass().getName() + ".call")
                    .start();
      } else {
        child = span;
      }
      
      try {
        row_key_literals = new ByteMap<List<byte[]>>();
        processFilter(resolved);
        
        if (cardinality > max_multi_get_cardinality) {
          could_multi_get = false;
        }
        
        // now that we have our filters sorted out, create the scanner(s).
        setupScanners(metric, child);
        if (child != null) {
          child.setSuccessTags()
               .finish();
        }
      } catch (Exception e) {
        if (child != null) {
          child.setErrorTags(e)
               .finish();
        }
        throw e;
      }
      return null;
    }
    
    /**
     * Determines how to write the tag key and optional literals to the
     * map when there are more than one filters sharing the same tag key.
     * 
     * @param tagk A non-null and non-empty tag key.
     * @param literals An optional list of literal tag values.
     */
    void putFilter(final byte[] tagk, final List<byte[]> literals) {
      if (row_key_literals.containsKey(tagk)) {
        // literals win
        List<byte[]> extant = row_key_literals.get(tagk);
        if (literals != null && extant != null) {
          // merge
          extant.addAll(literals);
          if (extant.size() > 0) {
            final ByteSet dedupe = new ByteSet();
            dedupe.addAll(extant);
            extant.clear();
            extant.addAll(dedupe);
          }
          Collections.sort(extant, Bytes.MEMCMP);
        } else if (literals != null && extant == null) {
          if (literals.size() > 0) {
            final ByteSet dedupe = new ByteSet();
            dedupe.addAll(literals);
            literals.clear();
            literals.addAll(dedupe);
          }
          Collections.sort(literals, Bytes.MEMCMP);
          row_key_literals.put(tagk, literals);
        }
      } else {
        if (literals != null && literals.size() > 0) {
          final ByteSet dedupe = new ByteSet();
          dedupe.addAll(literals);
          literals.clear();
          literals.addAll(dedupe);
          Collections.sort(literals, Bytes.MEMCMP);
        }
        row_key_literals.put(tagk, literals);
      }
    }
  }
  
  /** @return The parent node. */
  Tsdb1xQueryNode node() {
    return node;
  }
}