// This file is part of OpenTSDB.
// Copyright (C) 2015-2017  The OpenTSDB Authors.
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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import net.opentsdb.meta.Annotation;
import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.rollup.RollupQuery;
import net.opentsdb.rollup.RollupSpan;
import net.opentsdb.stats.QueryStats;
import net.opentsdb.stats.QueryStats.QueryStat;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.JSON;

import org.hbase.async.Bytes.ByteMap;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.hbase.async.Bytes;
import org.hbase.async.DeleteRequest;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

/**
 * A class that handles coordinating the various scanners created for each 
 * salt bucket when salting is enabled. Each scanner stores it's results in 
 * local maps and once everyone has reported in, then the maps are parsed and
 * combined into a proper set of spans to return to the {@link TsdbQuery} class.
 * 
 * Note that if one or more of the scanners throws an exception, then that 
 * exception will be returned to the caller in the deferred. Unfortunately we
 * don't have a good way to cancel a scan in progress so the first scanner with
 * an error will store it, then we wait for all of the other scanners to
 * complete.
 * 
 * Concurrency is important in this class as the scanners are executing
 * asynchronously and can modify variables at any time.
 * 
 * @since 2.2
 */
public class SaltScanner {
  private static final Logger LOG = LoggerFactory.getLogger(SaltScanner.class);
  
  /** This is a map that the caller must supply. We'll fill it with data.
   * WARNING: The salted row comparator should be applied to this map. */
  private final SortedMap<byte[], Span> spans;
  
  private final SortedMap<byte[], HistogramSpan> histSpans;
  
  /** The list of pre-configured scanners. One scanner should be created per
   * salt bucket. */
  private final List<Scanner> scanners;
  
  /** Stores the compacted columns from each scanner as it completes. After all
   * scanners are done, we process this into the span map above. */
  private final Map<Integer, List<KeyValue>> kv_map = 
          new ConcurrentHashMap<Integer, List<KeyValue>>();
  
  /** Stores annotations from each scanner as it completes */
  private final Map<byte[], List<Annotation>> annotation_map = 
          Collections.synchronizedMap(
              new TreeMap<byte[], List<Annotation>>(new RowKey.SaltCmp()));
  
  private final Map<Integer, List<SimpleEntry<byte[], List<HistogramDataPoint>>>> 
    histMap = new ConcurrentHashMap<Integer, List<SimpleEntry<byte[], 
    List<HistogramDataPoint>>>>();
  
  /** A deferred to call with the spans on completion */
  private final Deferred<SortedMap<byte[], Span>> results =
          new Deferred<SortedMap<byte[], Span>>();
  
  private final Deferred<SortedMap<byte[], HistogramSpan>> histogramResults =
      new Deferred<SortedMap<byte[], HistogramSpan>>();
  
  /** The metric this scanner set is dealing with. If a row comes in with a 
   * different metric we toss an exception. This shouldn't happen though. */
  private final byte[] metric;
  
  /** The TSDB to which we belong */
  private final TSDB tsdb;
  
  /** A stats object associated with the sub query used for storing stats
   * about scanner operations. */
  private final QueryStats query_stats;
  
  /** Index of the sub query in the main query list */
  private final int query_index;
  private final boolean is_rollup;
  private final int rollup_agg_id;
  private final int rollup_count_id;
  
  /** Settings and counters to determine when we need to cancel a query. */
  private final AtomicLong num_data_points;  
  private final AtomicBoolean max_data_points_flag;
  private AtomicLong bytes_fetched = new AtomicLong();
  private final long max_data_points;
  private final long max_bytes;
  
  /** A latch used to determine how many scanners are still running */
  private final CountDownLatch countdown;
  
  /** When the scanning started. We store the scan latency once all scanners
   * are done.*/
  private long start_time; // milliseconds.

  /** Whether or not to delete the queried data */
  private final boolean delete;
  
  /** A rollup query configuration if scanning for rolled up data. */
  private final RollupQuery rollup_query;
  
  /** A list of filters to iterate over when processing rows */
  private final List<TagVFilter> filters;
  
  /** A holder for storing the first exception thrown by a scanner if something
   * goes pear shaped. Make sure to synchronize on this object when checking
   * for null or assigning from a scanner's callback. */
  private volatile Exception exception;
  
  /**
   * Default ctor that performs some validation. Call {@link scan} after 
   * construction to actually start fetching data.
   * @param tsdb The TSDB to which we belong
   * @param metric The metric we're expecting to fetch
   * @param scanners A list of HBase scanners, one for each bucket
   * @param spans The span map to store results in
   * @param filters A list of filters for processing
   * @throws IllegalArgumentException if any required data was missing or
   * we had invalid parameters.
   */
  public SaltScanner(final TSDB tsdb, final byte[] metric, 
                                      final List<Scanner> scanners, 
                                      final TreeMap<byte[], Span> spans,
                                      final List<TagVFilter> filters) {
    this(tsdb, metric, scanners, spans, filters, false, null, null, 0, null, 0, 0);
  }
  
  /**
   * Default ctor that performs some validation. Call {@link scan} after 
   * construction to actually start fetching data.
   * @param tsdb The TSDB to which we belong
   * @param metric The metric we're expecting to fetch
   * @param scanners A list of HBase scanners, one for each bucket
   * @param spans The span map to store results in
   * @param delete Whether or not to delete the queried data
   * @param rollup_query An optional rollup query config. May be null.
   * @param filters A list of filters for processing
   * @param query_stats A stats object for tracking timing
   * @param query_index The index of the sub query in the main query list
   * @param histogramSpans The histo map to populate.
   * @param max_bytes The maximum number of bytes pulled out from all scanners 
   * combined.
   * @param max_data_points The maximum number of data points pulled out from all
   * scanners (estimated).
   * @throws IllegalArgumentException if any required data was missing or
   * we had invalid parameters.
   */
  public SaltScanner(final TSDB tsdb, final byte[] metric, 
                                      final List<Scanner> scanners, 
                                      final TreeMap<byte[], Span> spans,
                                      final List<TagVFilter> filters,
                                      final boolean delete,
                                      final RollupQuery rollup_query,
                                      final QueryStats query_stats,
                                      final int query_index,
                                      final TreeMap<byte[], HistogramSpan> histogramSpans,
                                      final long max_bytes,
                                      final long max_data_points) {
    if (tsdb == null) {
      throw new IllegalArgumentException("The TSDB argument was null.");
    }
    if (spans == null && histogramSpans == null) {
      throw new IllegalArgumentException("Both Span map and HistogramSpan map were null.");
    }
    if (spans != null && !spans.isEmpty()) {
      throw new IllegalArgumentException("The span map should be empty.");
    }
    if (histogramSpans != null && !histogramSpans.isEmpty()) {
      throw new IllegalArgumentException("The histogram span map should be empty.");
    }
    if (scanners == null || scanners.isEmpty()) {
      throw new IllegalArgumentException("Missing or empty scanners list. "
          + "Please provide a list of scanners for each salt.");
    }
    if (Const.SALT_WIDTH() > 0 && scanners.size() != Const.SALT_BUCKETS()) {
      throw new IllegalArgumentException("Not enough or too many scanners " + 
          scanners.size() + " when the salt bucket count is " + 
          Const.SALT_BUCKETS());
    } else if (Const.SALT_WIDTH() <= 0 && scanners.size() > 1) {
      throw new IllegalArgumentException("Not enough or too many scanners " + 
          scanners.size() + " when the salting is disabled.");
    }
    if (metric == null) {
      throw new IllegalArgumentException("The metric array was null.");
    }
    if (metric.length != TSDB.metrics_width()) {
      throw new IllegalArgumentException("The metric was too short. It must be " 
          + TSDB.metrics_width() + "bytes wide.");
    }
    
    this.scanners = scanners;
    this.spans = spans != null ? Collections.synchronizedSortedMap(spans) : null;
    this.histSpans = histogramSpans != null ? Collections.synchronizedSortedMap(histogramSpans) : null;
    this.metric = metric;
    this.tsdb = tsdb;
    this.filters = filters;
    this.delete = delete;
    this.rollup_query = rollup_query;
    this.query_stats = query_stats;
    this.query_index = query_index;
    countdown = new CountDownLatch(scanners.size());
    if (rollup_query != null && RollupQuery.isValidQuery(rollup_query)) {
      is_rollup = true;
      if (rollup_query.getRollupAgg() == Aggregators.AVG) {
        rollup_agg_id = tsdb.getRollupConfig().getIdForAggregator("sum");
        rollup_count_id = tsdb.getRollupConfig().getIdForAggregator("count");
      } else {
        rollup_agg_id = tsdb.getRollupConfig().getIdForAggregator(
            rollup_query.getRollupAgg().toString());
        rollup_count_id = -1;
      }
    } else {
      is_rollup = false;
      rollup_agg_id = rollup_count_id = -1;
    }
    this.max_bytes = max_bytes;
    this.max_data_points = max_data_points;
    num_data_points = new AtomicLong();
    bytes_fetched = new AtomicLong();
    max_data_points_flag = new AtomicBoolean();
  }

  /**
   * Starts all of the scanners asynchronously and returns the data fetched
   * once all of the scanners have completed. Note that the result may be an
   * exception if one or more of the scanners encountered an exception. The 
   * first error will be returned, others will be logged. 
   * @return A deferred to wait on for results.
   */
  public Deferred<SortedMap<byte[], Span>> scan() {
    start_time = System.currentTimeMillis();
    int i = 0;
    for (final Scanner scanner: scanners) {
      new ScannerCB(scanner, i++).scan();
    }
    return results; 
  }

  public Deferred<SortedMap<byte[], HistogramSpan>> scanHistogram() {
    start_time = DateTime.currentTimeMillis();

    int index = 0;
    for (Scanner scanner: scanners) {
      ScannerCB scnr = new ScannerCB(scanner, index++);
      scnr.scan();
    }

    return histogramResults;
  }
  
  /**
   * Called once all of the scanners have reported back in to record our
   * latency and merge the results into the spans map. If there was an exception
   * stored then we'll return that instead.
   */
  private void mergeAndReturnResults() {
    final long hbase_time = DateTime.currentTimeMillis();
    TsdbQuery.scanlatency.add((int)(hbase_time - start_time));
    
    if (exception != null) {
      LOG.error("After all of the scanners finished, at "
          + "least one threw an exception", exception);
      results.callback(exception);
      return;
    }
    
    final long merge_start = DateTime.nanoTime();
    //Merge sorted spans together
    if (!isHistogramScan()) {
      mergeDataPoints();
    } else {
      // Merge histogram data points
      mergeHistogramDataPoints();
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("It took " + (DateTime.currentTimeMillis() - hbase_time) + " ms, "
            + " to merge and sort the rows into a tree map");
    }

    if (query_stats != null) {
      query_stats.addStat(query_index, QueryStat.SCANNER_MERGE_TIME, 
          (DateTime.nanoTime() - merge_start));
    }

    if (!isHistogramScan()) {
      results.callback(spans);
    } else {
      histogramResults.callback(histSpans);
    }
  }
  
  private boolean isHistogramScan() {
    return histSpans != null;
  }

  private void mergeHistogramDataPoints() {
    if (histSpans != null) {
      for (List<SimpleEntry<byte[], List<HistogramDataPoint>>> rows : histMap.values()) {
        if (null == rows || rows.isEmpty()) {
          LOG.error("Found a histogram rows list that was null or empty");
          continue;
        }
        
        // for all the rows with the same salt in the scann order - timestamp order
        for (final SimpleEntry<byte[], List<HistogramDataPoint>> row : rows) {
          if (null == row) {
            LOG.error("Found a histogram row item that was null");
            continue;
          }
          
          HistogramSpan histSpan = null;
          try {
            histSpan = histSpans.get(row.getKey());
          } catch (RuntimeException e) {
            LOG.error("Failed to fetch the histogram span", e);
          }
          
          if (histSpan == null) {
            histSpan = new HistogramSpan(tsdb);
            histSpans.put(row.getKey(), histSpan);
          }

          if (annotation_map.containsKey(row.getKey())) {
            histSpan.getAnnotations().addAll(annotation_map.get(row.getKey()));
            annotation_map.remove(row.getKey());
          }
          
          try {
            histSpan.addRow(row.getKey(), row.getValue());
          } catch (RuntimeException e) {
            LOG.error("Exception adding row to histogram span", e);
          }
        } // end for
      } // end for
      
      histMap.clear();

      for (byte[] key : annotation_map.keySet()) {
        HistogramSpan histSpan = histSpans.get(key);

        if (histSpan == null) {
          histSpan = new HistogramSpan(tsdb);
          histSpans.put(key, histSpan);
        }

        histSpan.getAnnotations().addAll(annotation_map.get(key));
      }

      annotation_map.clear();
    }
  }
  
  /**
   * Called once all of the scanners have reported back in to record our
   * latency and merge the results into the spans map. If there was an exception
   * stored then we'll return that instead.
   */
  private void mergeDataPoints() {
    // Merge sorted spans together
    final long merge_start = DateTime.nanoTime();
    for (final List<KeyValue> kvs : kv_map.values()) {
      if (kvs == null || kvs.isEmpty()) {
        LOG.warn("Found a key value list that was null or empty");
        continue;
      }
      
      for (final KeyValue kv : kvs) {
        if (kv == null) {
          LOG.warn("Found a key value item that was null");
          continue;
        }
        if (kv.key() == null) {
          LOG.warn("A key for a kv was null");
          continue;
        }

        Span datapoints = spans.get(kv.key());
        if (datapoints == null) {
          datapoints = RollupQuery.isValidQuery(rollup_query) ?
              new RollupSpan(tsdb, this.rollup_query) : new Span(tsdb);
          spans.put(kv.key(), datapoints);
        }

        if (annotation_map.containsKey(kv.key())) {
          for (final Annotation note: annotation_map.get(kv.key())) {
            datapoints.getAnnotations().add(note);
          }
          annotation_map.remove(kv.key());
        }
        try {  
          datapoints.addRow(kv);
        } catch (RuntimeException e) {
          LOG.error("Exception adding row to span", e);
          throw e;
        }
      }
    }
     
    kv_map.clear();

    for (final byte[] key : annotation_map.keySet()) {
      Span datapoints = spans.get(key);
      if (datapoints == null) {
        datapoints = new Span(tsdb);
        spans.put(key, datapoints);
      }

      for (final Annotation note: annotation_map.get(key)) {
        datapoints.getAnnotations().add(note);
      }
    }
    
    annotation_map.clear();
  }

  /**
  * Scanner callback executed recursively each time we get a set of data
  * from storage. This is responsible for determining what columns are
  * returned and issuing requests to load leaf objects.
  * When the scanner returns a null set of rows, the method initiates the
  * final callback.
  */
  final class ScannerCB implements Callback<Object, 
    ArrayList<ArrayList<KeyValue>>> {
    private final Scanner scanner;
    private final int index;
    private final List<KeyValue> kvs = new ArrayList<KeyValue>();
    private final ByteMap<List<Annotation>> annotations = 
            new ByteMap<List<Annotation>>();
    private final Set<String> skips = Collections.newSetFromMap(
        new ConcurrentHashMap<String, Boolean>());
    private final Set<String> keepers = Collections.newSetFromMap(
        new ConcurrentHashMap<String, Boolean>());
 
    // use list here because we want to keep the rows in the scan order - 
    // timestamp order. I don't want to define an additional class to store the 
    // information of the row key and the histogram data points in the row, 
    // then use {@link SimpleEntry}
    private List<SimpleEntry<byte[], List<HistogramDataPoint>>> histograms = 
        Collections.synchronizedList(Lists.<SimpleEntry<byte[], 
            List<HistogramDataPoint>>>newArrayList());
    
    private long scanner_start = -1;
    /** nanosecond timestamps */
    private long fetch_start = 0;      // reset each time we send an RPC to HBase
    private long fetch_time = 0;       // cumulation of time waiting on HBase
    private long uid_resolve_time = 0; // cumulation of time resolving UIDs
    private long uids_resolved = 0; 
    private long compaction_time = 0;  // cumulation of time compacting
    private long dps_pre_filter = 0;
    private long rows_pre_filter = 0;
    private long dps_post_filter = 0;
    private long rows_post_filter = 0;
    
    public ScannerCB(final Scanner scanner, final int index) {
      this.scanner = scanner;
      this.index = index;
      if (query_stats != null) {
        query_stats.addScannerId(query_index, index, scanner.toString());
      }
    }
    
    /** Error callback that will capture an exception from AsyncHBase and store
     * it so we can bubble it up to the caller.
     */
    class ErrorCb implements Callback<Object, Exception> {
      @Override
      public Object call(final Exception e) throws Exception {
        LOG.error("Scanner " + scanner + " threw an exception", e);
        close(false);
        handleException(e);
        return null;
      }
    }
    
    /**
    * Starts the scanner and is called recursively to fetch the next set of
    * rows from the scanner.
    * @return The map of spans if loaded successfully, null if no data was
    * found
    */
    public Object scan() {
      if (scanner_start < 0) {
        scanner_start = DateTime.nanoTime();
      }
      fetch_start = DateTime.nanoTime();
      return scanner.nextRows().addCallback(this).addErrback(new ErrorCb());
    }

    /**
    * Iterate through each row of the scanner results, parses out data
    * points (and optional meta data).
    * @return null if no rows were found, otherwise the SortedMap with spans
    */
    @Override
    public Object call(final ArrayList<ArrayList<KeyValue>> rows) 
            throws Exception {
      try {
        fetch_time += DateTime.nanoTime() - fetch_start;
        if (rows == null) {
          close(true);
          return null;
        } else if (exception != null) {
          close(false);
          // don't need to handleException here as it's already taken care of
          // due to the fact that exception was set.
          if (LOG.isDebugEnabled()) {
            LOG.debug("Closing scanner as there was an exception: " + scanner);
          }
          return null;
        }

        // used for UID resolution if a filter is involved
        final List<Deferred<Object>> lookups = 
            filters != null && !filters.isEmpty() ? 
                new ArrayList<Deferred<Object>>(rows.size()) : null;
        
        // validation checking before processing the next set of results. It's 
        // kinda funky but we want to allow queries to sneak through that were
        // just a *tad* over the limits so that's why we don't check at the 
        // end of a scan call.
        if (max_data_points > 0 && num_data_points.get() >= max_data_points) {
          max_data_points_flag.getAndSet(true);
          try {
            close(false);
            handleException(
                new QueryException(HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE,
                "Sorry, you have attempted to fetch more than our limit of " 
                    + max_data_points + " data points. Please try filtering "
                    + "using more tags or decrease your time range."));
            return false;
          } catch (Exception e) {
            LOG.error("Sorry, Scanner is closed: " + scanner, e);
            return false;
          }
        }
        
        if (max_bytes > 0 && bytes_fetched.get() > max_bytes) {
          max_data_points_flag.getAndSet(true);
          try {
            close(false);
            handleException(
                new QueryException(HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE,
                "Sorry, you have attempted to fetch more than our maximum "
                    + "amount of " + (max_bytes / 1024 / 1024) + "MB from storage. " 
                    + "Please try filtering using more tags or decrease your time range."));
            return false;
          } catch (Exception e) {
            LOG.error("Sorry, Scanner is closed: " + scanner, e);
            return false;
          }
        }
                
        rows_pre_filter += rows.size();
        for (final ArrayList<KeyValue> row : rows) {
          final byte[] key = row.get(0).key();
          num_data_points.addAndGet(row.size());
          if (RowKey.rowKeyContainsMetric(metric, key) != 0) {
            close(false);
            handleException(new IllegalDataException(
                   "HBase returned a row that doesn't match"
                   + " our scanner (" + scanner + ")! " + row + " does not start"
                   + " with " + Arrays.toString(metric) + " on scanner " + this));
            return null;
          }

          // calculate estimated data point count. We don't want to deserialize
          // the byte arrays so we'll just get a rough estimate of compacted
          // columns.
          long bytes = 0;
          for (final KeyValue kv : row) {
            // rough estimate of the # of bytes returned from storage.
            bytes += key.length + kv.qualifier().length | kv.value().length;
            bytes_fetched.addAndGet(bytes);
            
            if (kv.qualifier().length % 2 == 0) {
              if (kv.qualifier().length == 2 || kv.qualifier().length == 4) {
                ++dps_pre_filter;
              } else {
                // for now we'll assume that all compacted columns are of the 
                // same precision. This is likely incorrect.
                if (Internal.inMilliseconds(kv.qualifier())) {
                  dps_pre_filter += (kv.qualifier().length / 4);
                } else {
                  dps_pre_filter += (kv.qualifier().length / 2);
                }
              }
            } else if (kv.qualifier()[0] == AppendDataPoints.APPEND_COLUMN_PREFIX) {
              // with appends we don't have a good rough estimate as the length
              // can vary widely with the value length variability. Therefore we
              // have to iterate.
              int idx = 0;
              int qlength = 0;
              while (idx < kv.value().length) {
                qlength = Internal.getQualifierLength(kv.value(), idx);
                idx += qlength + Internal.getValueLengthFromQualifier(kv.value(), idx);
                ++dps_pre_filter;
              }
            }
          }
          
          // If any filters have made it this far then we need to resolve
          // the row key UIDs to their names for string comparison. We'll
          // try to avoid the resolution with some sets but we may dupe
          // resolve a few times.
          // TODO - more efficient resolution
          // TODO - byte set instead of a string for the uid may be faster
          if (filters != null && !filters.isEmpty()) {
            lookups.clear();
            final String tsuid = 
                UniqueId.uidToString(UniqueId.getTSUIDFromKey(key, 
                TSDB.metrics_width(), Const.TIMESTAMP_BYTES));
            if (skips.contains(tsuid)) {
              continue;
            }
            if (!keepers.contains(tsuid)) {
              final long uid_start = DateTime.nanoTime();
              
              /** CB to called after all of the UIDs have been resolved */
              class MatchCB implements Callback<Object, ArrayList<Boolean>> {
                @Override
                public Object call(final ArrayList<Boolean> matches) 
                    throws Exception {
                  for (final boolean matched : matches) {
                    if (!matched) {
                      skips.add(tsuid);
                      return null;
                    }
                  }
                  // matched all, good data
                  keepers.add(tsuid);
                  processRow(key, row);
                  return null;
                }
              }

              /** Resolves all of the row key UIDs to their strings for filtering */
              class GetTagsCB implements
                  Callback<Deferred<ArrayList<Boolean>>, Map<String, String>> {
                @Override
                public Deferred<ArrayList<Boolean>> call(
                    final Map<String, String> tags) throws Exception {
                  uid_resolve_time += (DateTime.nanoTime() - uid_start);
                  uids_resolved += tags.size();
                  final List<Deferred<Boolean>> matches =
                      new ArrayList<Deferred<Boolean>>(filters.size());

                  for (final TagVFilter filter : filters) {
                    matches.add(filter.match(tags));
                  }
                  
                  return Deferred.group(matches);
                }
              }
 
              lookups.add(Tags.getTagsAsync(tsdb, key)
                  .addCallbackDeferring(new GetTagsCB())
                  .addBoth(new MatchCB()));
            } else {
              processRow(key, row);
            }
          } else {
            processRow(key, row);
          }
        }
           
        // either we need to wait on the UID resolutions or we can go ahead
        // if we don't have filters.
        if (lookups != null && lookups.size() > 0) {
          class GroupCB implements Callback<Object, ArrayList<Object>> {
            @Override
            public Object call(final ArrayList<Object> group) throws Exception {
              return scan();
            }
          }
          return Deferred.group(lookups).addCallback(new GroupCB());
        } else {
          return scan();
        }
      } catch (final RuntimeException e) {
        LOG.error("Unexpected exception on scanner " + this, e);
        close(false);
        handleException(e);
        return null;
      }
    }
    
    /**
     * Finds or creates the span for this row, compacts it and stores it. Also
     * fires off a delete request for the row if told to.
     * @param key The row key to use for fetching the span
     * @param row The row to add
     */
    void processRow(final byte[] key, final ArrayList<KeyValue> row) {
      ++rows_post_filter;
      if (delete) {
        final DeleteRequest del = new DeleteRequest(tsdb.dataTable(), key);
        tsdb.getClient().delete(del);
      }
      
      List<HistogramDataPoint> hists = new ArrayList<HistogramDataPoint>();
      
      //TODO rollup doesn't use the column qualifier prefix right now
      //Please move this logic to @CompactionQueue.compact API, if the 
      //qualifier prefix is set for rollup. Right now there is no way to
      //identify whether a cell belong to rollup or default data table
      //from the KeyValue/Hbase cell object
      if (RollupQuery.isValidQuery(rollup_query)) {
        //It is the rollup search result and rollup cells will not be 
        //compacted, so don't need to worry about complex or trivial 
        //compactions. It just need to consider the cells are different key 
        //values
        for (KeyValue kv:row) {
          final byte[] qual = kv.qualifier();
          
          if (qual.length > 0) {
            // TODO - allow rollups for annotations and histos? Probably will
            // want to encode those on 4 bytes or something
            if (!is_rollup && qual[0] == Annotation.PREFIX()) {
              // This could be a row with only an annotation in it
              final Annotation note = JSON.parseToObject(kv.value(),
                      Annotation.class);
              synchronized (annotations) {
                List<Annotation> map_notes = annotations.get(key);
                if (map_notes == null) {
                  map_notes = new ArrayList<Annotation>();
                  annotations.put(key, map_notes);
                }
                map_notes.add(note);
              }
            } else if (!is_rollup && qual[0] == HistogramDataPoint.PREFIX) {
              try {
                HistogramDataPoint histogram = 
                    Internal.decodeHistogramDataPoint(tsdb, kv);
                hists.add(histogram);
              } catch (Throwable t) {
                LOG.error("Failed to decode histogram data point", t);
              }
            } else {
              if (rollup_query.getRollupAgg() == Aggregators.AVG || 
                  rollup_query.getRollupAgg() == Aggregators.DEV) {
                if (qual[0] == (byte) rollup_agg_id ||
                    qual[0] == (byte) rollup_count_id ||
                    Bytes.memcmp(RollupQuery.SUM, qual, 0, RollupQuery.SUM.length) == 0 ||
                    Bytes.memcmp(RollupQuery.COUNT, qual, 0, RollupQuery.COUNT.length) == 0) {
                  kvs.add(kv);
                }
              } else if (qual[0] == (byte) rollup_agg_id ||
                  Bytes.memcmp(rollup_query.getRollupAggPrefix(), 
                    qual, 0, rollup_query.getRollupAggPrefix().length) == 0) {
                kvs.add(kv);
              }
            }
          }
        } // end for
        
        // histogram row
        if (hists.size() > 0) {
          this.histograms.add(new SimpleEntry<byte[], List<HistogramDataPoint>>(key, hists));
        }
      } else {
        // calculate estimated data point count. We don't want to deserialize
        // the byte arrays so we'll just get a rough estimate of compacted
        // columns.
        for (final KeyValue kv : row) {
          if (kv.qualifier().length % 2 == 0) {
            if (kv.qualifier().length == 2 || kv.qualifier().length == 4) {
              ++dps_post_filter;
            } else {
              // for now we'll assume that all compacted columns are of the 
              // same precision. This is likely incorrect.
              if (Internal.inMilliseconds(kv.qualifier())) {
                dps_post_filter += (kv.qualifier().length / 4);
              } else {
                dps_post_filter += (kv.qualifier().length / 2);
              }
            }
          } else if (kv.qualifier()[0] == AppendDataPoints.APPEND_COLUMN_PREFIX) {
            // with appends we don't have a good rough estimate as the length
            // can vary widely with the value length variability. Therefore we
            // have to iterate.
            int idx = 0;
            int qlength = 0;
            while (idx < kv.value().length) {
              qlength = Internal.getQualifierLength(kv.value(), idx);
              idx += qlength + Internal.getValueLengthFromQualifier(kv.value(), idx);
              ++dps_post_filter;
            }
          }
        }
        
        final KeyValue compacted;
        // let IllegalDataExceptions bubble up so the handler above can close
        // the scanner
        final long compaction_start = DateTime.nanoTime();
        try {
          final List<Annotation> notes = Lists.newArrayList();
          compacted = tsdb.compact(row, notes, hists);
          
          // histogram row
          if (hists.size() > 0) {
            this.histograms.add(new SimpleEntry<byte[], List<HistogramDataPoint>>(key, hists));
          }
          
          if (!notes.isEmpty()) {
            synchronized (annotations) {
              List<Annotation> map_notes = annotations.get(key);
              if (map_notes == null) {
                annotations.put(key, notes);
              } else {
                map_notes.addAll(notes);
              }
            }
          }
        } catch (IllegalDataException idex) {
          compaction_time += (DateTime.nanoTime() - compaction_start);
          close(false);
          handleException(idex);
          return;
        }
        compaction_time += (DateTime.nanoTime() - compaction_start);
        if (compacted != null) { // Can be null if we ignored all KVs.
          kvs.add(compacted);
        }
      }
    }
  
    /**
     * Closes the scanner and sets the various stats after filtering
     * @param ok Whether or not the scanner closed with an exception or 
     * closed due to natural causes (e.g. ran out of data or we wanted to stop
     * it early)
     */
    void close(final boolean ok) {
      scanner.close();
      
      if (query_stats != null) {
        query_stats.addScannerStat(query_index, index, QueryStat.SCANNER_TIME, 
            DateTime.nanoTime() - scanner_start);

        // Scanner Stats
        /* Uncomment when AsyncHBase has this feature:
        query_stats.addScannerStat(query_index, index, 
            QueryStat.ROWS_FROM_STORAGE, scanner.getRowsFetched());
        query_stats.addScannerStat(query_index, index, 
            QueryStat.COLUMNS_FROM_STORAGE, scanner.getColumnsFetched());
        query_stats.addScannerStat(query_index, index, 
            QueryStat.BYTES_FROM_STORAGE, scanner.getBytesFetched()); */
        query_stats.addScannerStat(query_index, index, 
            QueryStat.HBASE_TIME, fetch_time);
        query_stats.addScannerStat(query_index, index, 
            QueryStat.SUCCESSFUL_SCAN, ok ? 1 : 0);
        
        // Post Scan stats
        query_stats.addScannerStat(query_index, index, 
            QueryStat.ROWS_PRE_FILTER, rows_pre_filter);
        query_stats.addScannerStat(query_index, index,
            QueryStat.DPS_PRE_FILTER, dps_pre_filter);
        query_stats.addScannerStat(query_index, index, 
            QueryStat.ROWS_POST_FILTER, rows_post_filter);
        query_stats.addScannerStat(query_index, index,
            QueryStat.DPS_POST_FILTER, dps_post_filter);
        query_stats.addScannerStat(query_index, index, 
            QueryStat.SCANNER_UID_TO_STRING_TIME, uid_resolve_time);
        query_stats.addScannerStat(query_index, index, 
            QueryStat.UID_PAIRS_RESOLVED, uids_resolved);
        query_stats.addScannerStat(query_index, index, 
            QueryStat.COMPACTION_TIME, compaction_time);
      }
      
      if (ok && exception == null) {
        validateAndTriggerCallback(kvs, annotations, histograms);
      } else {
        countdown.countDown();
      }
    }
  }
  
  /**
   * Called each time a scanner completes with valid or empty data.
   * @param kvs The compacted columns fetched by the scanner
   * @param annotations The annotations fetched by the scanners
   */
  private void validateAndTriggerCallback(
      final List<KeyValue> kvs, 
      final Map<byte[], List<Annotation>> annotations,
      final List<SimpleEntry<byte[], List<HistogramDataPoint>>> histograms) {

    countdown.countDown();
    final long count = countdown.getCount();
    if (kvs.size() > 0) {
      kv_map.put((int) count, kvs);
    }
    
    for (final byte[] key : annotations.keySet()) {
      final List<Annotation> notes = annotations.get(key);
      if (notes.size() > 0) {
        // Optimistic write, expecting unique row keys
        annotation_map.put(key, notes);
      }
    }
    
    if (histograms.size() > 0) {
      histMap.put((int) count, histograms);
    }
    
    if (countdown.getCount() <= 0) {
      try {
        mergeAndReturnResults();
      } catch (final Exception ex) {
        results.callback(ex);
      }
    }
  }

  /**
   * If one or more of the scanners throws an exception then we should close it
   * and pass the exception here so that we can catch and return it to the
   * caller. If all of the scanners have finished, this will callback to the 
   * caller immediately.
   * @param e The exception to store.
   */
  private void handleException(final Exception e) {
    // make sure only one scanner can set the exception
    countdown.countDown();
    if (exception == null) {
      synchronized (this) {
        if (exception == null) {
          exception = e;
          // fail once and fast on the first scanner to throw an exception
          try {
            mergeAndReturnResults();
          } catch (Exception ex) {
            LOG.error("Failed merging and returning results, "
                + "calling back with exception", ex);
            results.callback(ex);
          }
        } else {
          // TODO - it would be nice to close and cancel the other scanners but
          // for now we have to wait for them to finish and/or throw exceptions.
          LOG.error("Another scanner threw an exception", e);
        }
      }
    }
  }
}
