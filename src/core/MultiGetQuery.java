// This file is part of OpenTSDB.
// Copyright (C) 2016  The OpenTSDB Authors.
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
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.hbase.async.Bytes.ByteMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import org.hbase.async.Bytes;
import org.hbase.async.GetRequest;
import org.hbase.async.GetResultOrException;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.hbase.async.KeyValue;

import net.opentsdb.meta.Annotation;
import net.opentsdb.rollup.RollupInterval;
import net.opentsdb.rollup.RollupQuery;
import net.opentsdb.rollup.RollupSpan;
import net.opentsdb.rollup.RollupUtils;
import net.opentsdb.stats.QueryStats;
import net.opentsdb.stats.QueryStats.QueryStat;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.ByteSet;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.JSON;

/**
 * Class that handles fetching TSDB data from storage using GetRequests instead
 * of scanning for the data. This is only applicable if the query is using the
 * "explicit_tags" feature and specified literal filters for exact matching.
 * It also works best when used for data that has high cardinality but the query
 * is fetching a small subset of that data.
 *  
 * @since 2.4
 */
public class MultiGetQuery {
  private static final Logger LOG = LoggerFactory.getLogger(MultiGetQuery.class);
  private final TSDB tsdb;
  private final byte[] metric;
  private final List<ByteMap<byte[][]>> tags;
  private final long start_row_time; // in sec
  private final long end_row_time; // in sec
  private final byte[] table_to_fetch;
  private final TreeMap<byte[], Span> spans;
  private final TreeMap<byte[], HistogramSpan> histogramSpans;
  private final RollupQuery rollup_query;
  private final QueryStats query_stats;
  private final int query_index;

  private int multi_get_wait_cnt;
  
  private int multi_get_num_get_requests;

  private final Map<Integer, List<KeyValue>> kvsmap = 
      new ConcurrentHashMap<Integer, List<KeyValue>>();

  private final Map<byte[], List<Annotation>> annotMap = Collections
      .synchronizedMap(new TreeMap<byte[], List<Annotation>>(new RowKey.SaltCmp()));

  private final Map<Integer, List<SimpleEntry<byte[], 
  List<HistogramDataPoint>>>> histMap = Maps.newConcurrentMap();

  private final Deferred<TreeMap<byte[], Span>> results = 
      new Deferred<TreeMap<byte[], Span>>();

  private final Deferred<TreeMap<byte[], HistogramSpan>> histogramResults = 
      new Deferred<TreeMap<byte[], HistogramSpan>>();

  private final ArrayList<List<MultiGetTask>> multi_get_tasks;
  private final ArrayList<AtomicInteger> multi_get_indexs;

  private long prepare_multi_get_start_time;

  private long prepare_multi_get_end_time;

  // the timestamp of starting fetching data
  private long fetch_start_time;

  // the number of data point fetched
  private AtomicLong number_pre_filter_data_point;

  private AtomicLong num_post_filter_data_points;

  // the byte size of the data fetched
  private AtomicLong byte_size_fetched;

  // the finished multi get number
  private AtomicInteger finished_multi_get_cnt;

  private AtomicInteger multi_get_seq_id;

  private final int concurrency_multi_get;

  private final int batch_size;

  /** Whether or not to fetch all possible salts for the rows in case the 
   * salting has changed during the TSD's run. */
  private final boolean get_all_salts;

  /** A holder for storing the first exception thrown by a scanner if something
   * goes pear shaped. Make sure to synchronize on this object when checking
   * for null or assigning from a scanner's callback. */
  private volatile Exception exception;
  
  private long max_bytes;
  
  private boolean multiget_no_meta;
  
  private AtomicLong number_byte_fetched;

  private final boolean is_rollup;
  private final int rollup_agg_id;
  private final int rollup_count_id;
  
  public MultiGetQuery(final TSDB tsdb, 
      final TsdbQuery query,
      final byte[] metric, 
      final List<ByteMap<byte[][]>> tags, 
      final long start_row_time,
      final long end_row_time, 
      final byte[] table_to_fetch, 
      final TreeMap<byte[], Span> spans,
      final TreeMap<byte[], HistogramSpan> histogramSpans, 
      final long timeout, 
      final RollupQuery rollup_query,
      final QueryStats query_stats, 
      final int query_index, 
      final long max_bytes, 
      final boolean override_count_limit,
      final boolean multiget_no_meta) {
    this.tsdb = tsdb;
    this.metric = metric;
    this.tags = tags;
    this.start_row_time = start_row_time;
    this.end_row_time = end_row_time;
    this.table_to_fetch = table_to_fetch;
    this.spans = spans;
    this.histogramSpans = histogramSpans;
    this.rollup_query = rollup_query;
    this.query_stats = query_stats;
    this.query_index = query_index;
    this.multiget_no_meta = multiget_no_meta;

    if (tags == null) {
      throw new IllegalArgumentException("Tags list cannot be null or empty");
    }
     if (tags.isEmpty()) {
       query.setNoResults(true);
     }
    if (end_row_time <= start_row_time) {
      throw new IllegalArgumentException("Start time cannot be later or "
          + "equal to the end time");
    }

    concurrency_multi_get = tsdb.config
        .getInt("tsd.query.multi_get.concurrent");
    batch_size = tsdb.config.getInt("tsd.query.multi_get.batch_size");
    get_all_salts = tsdb.config.getBoolean("tsd.query.multi_get.get_all_salts");
    multi_get_tasks = new ArrayList<List<MultiGetTask>>(concurrency_multi_get);
    multi_get_indexs = new ArrayList<AtomicInteger>(concurrency_multi_get);
    for (int i = 0; i < concurrency_multi_get; ++i) {
      multi_get_tasks.add(new ArrayList<MultiGetTask>());
      multi_get_indexs.add(new AtomicInteger(-1));
    }

    number_pre_filter_data_point = new AtomicLong(0);
    num_post_filter_data_points = new AtomicLong(0);
    byte_size_fetched = new AtomicLong(0);
    finished_multi_get_cnt = new AtomicInteger(0);
    multi_get_seq_id = new AtomicInteger(-1);
    number_byte_fetched = new AtomicLong(0);
    this.max_bytes = max_bytes;
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
  }

  /**
   * Helper container class to store a set of TSUIDs and GetRequests in the same
   * object.
   */
  final static class MultiGetTask {
    private final Set<byte[]> tsuids;
    private final List<GetRequest> gets;

    /**
     * Default Ctor
     * @param tsuids Non-null set of TSUIDs. May be empty.
     * @param gets Non-null list of GetRequests. May be empty.
     */
    public MultiGetTask(final Set<byte[]> tsuids, final List<GetRequest> gets) {
      this.tsuids = tsuids;
      this.gets = gets;
    }

    public Set<byte[]> getTSUIDs() {
      return tsuids;
    }

    public List<GetRequest> getGets() {
      return gets;
    }
  }

  //////////////////////////////////////////////////////////////////////////////////////////
  //// Call back to handle result
  /////////////////////////////////////////////////////////////////////////////////////////
  final class MulGetCB implements Callback<Object, List<GetResultOrException>> {
    private final int concurrency_index;
    private final Set<byte[]> tsuids;
    private final List<GetRequest> gets;
    private final int seq_id;

    private List<KeyValue> keyValues = new ArrayList<KeyValue>();
    private final Map<byte[], List<Annotation>> annotations = 
        new ConcurrentHashMap<byte[], List<Annotation>>();

    // use list here because we want to keep the rows in the scan order -
    // timestamp order.
    // i don't want to define an additional class to store the information of
    // the row key and the histogram data points in the row, then use {@link SimpleEntry}
    private List<SimpleEntry<byte[], List<HistogramDataPoint>>> histograms = 
        new ArrayList<SimpleEntry<byte[], List<HistogramDataPoint>>>();

    ///////////////////////////////////////////////////////////////////////////
    // nanosecond times - trace response metrics                             //
    ///////////////////////////////////////////////////////////////////////////

    // the time to start the multi get request
    private long mul_get_start_time = -1;

    // cumulation of time waiting on HBase
    private long mul_get_time = 0; 

    // cumulation of time resolving uid
    private long mul_get_uid_resolved_time = 0; 

    // how many uids is resolved
    private long mul_get_uids_resolved = 0;

    // cumulation of time compacting
    private long mul_get_compaction_time = 0; 

    // how many data points after filtering
    private long mul_get_dps_post_filter = 0;

    // how many rows after filtering
    private long mul_get_rows_post_filter = 0;

    // how many rows fetched from hbase
    private long mul_get_number_row_fetched = 0;

    // how many cells fetched from hbase
    private long mul_get_number_column_fetched = 0;

    // how many bytes fetched from hbase
    private long mul_get_number_byte_fetched = 0;

    /** The exception thrown by this get request set */
    private Exception get_exception;

    public MulGetCB(final int concurrency_index, final Set<byte[]> tsuids, 
        final List<GetRequest> gets) {
      this.concurrency_index = concurrency_index;
      this.tsuids = tsuids;
      this.gets = gets;

      if (query_stats != null) {
        seq_id = multi_get_seq_id.incrementAndGet();
        StringBuilder sb = new StringBuilder();
        sb.append("Mulget_").append(concurrency_index).append("_").append(seq_id);
        query_stats.addScannerId(query_index, seq_id, sb.toString());
      } else {
        seq_id = 0;
      }
    }

    /** Error callback that will capture an exception from AsyncHBase and store
     * it so we can bubble it up to the caller.
     */
    class ErrorCb implements Callback<Object, Exception> {
      @Override
      public Object call(final Exception e) throws Exception {
        LOG.error("Multi get threw an exception: " + this, e);
        MulGetCB.this.get_exception = e;
        close(false);
        return null;
      }
    }

    public Object fetch() {
     
      mul_get_start_time = DateTime.nanoTime();

      if (LOG.isDebugEnabled()) {
        LOG.debug("Trying to fetch data for concurrency index: " 
            + concurrency_index + "; with " + gets.size() + " gets");
      }
      return tsdb.client.get(gets)
          .addCallback(this)
          .addErrback(new ErrorCb());
    }

    /**
     * Iterate through each row of the multi get results, parses out data
     * points (and optional meta data).
     * @return null if no rows were found, otherwise the TreeMap with spans
     */
    @Override
    public Object call(final List<GetResultOrException> results) throws Exception {
      mul_get_time = (DateTime.nanoTime() - mul_get_start_time);

      try {
        for (final GetResultOrException result : results) {
          // handle an exception
          if (result.getException() != null) {
            get_exception = result.getException();
            handleException(get_exception);
            close(false);
          }
          if (null != result.getCells()) {
            final ArrayList<KeyValue> row = result.getCells();
            if (row.isEmpty()) {
              continue;
            }
            
            number_pre_filter_data_point.addAndGet(row.size());
            ++mul_get_number_row_fetched;
            mul_get_number_column_fetched += row.size();

            final byte[] key = row.get(0).key();
            final byte[] tsuid_key = UniqueId.getTSUIDFromKey(key, 
                TSDB.metrics_width(), Const.TIMESTAMP_BYTES);

            if (!tsuids.contains(tsuid_key)) {
              LOG.error("Multi getter fetched the wrong row " + result 
                  + " when fetching metric: " + Bytes.pretty(metric));
              continue;
            }
            process(key, row);
          } else {
            // TODO we don't get cells for some get requests. This could be an
            // error or the database just didn't have data. Gotta look into it.
          }
        } // end for
      } catch (Exception e) {
        get_exception = e;
        close(false); 
        return null;
      }

      close(true);
      return null;
    }

    /**
     * Handles processing of row of data into the proper list
     * @param key The row key, possibly mutated
     * @param row The row of KVs to process
     * @return True if processing should continue, false if an exception occurred
     * or the scanner was already closed (possibly due to another scanner error)
     */
    boolean process(final byte[] key, final ArrayList<KeyValue> row) {
      ++mul_get_rows_post_filter;
      num_post_filter_data_points.addAndGet(row.size());

      List<Annotation> notes = null;
      if (annotMap != null) {
        notes = annotations.get(key);

        if (notes == null) {
          notes = new ArrayList<Annotation>();
          annotations.put(key, notes);
        }
      }

      List<HistogramDataPoint> hists = new ArrayList<HistogramDataPoint>();
      if (RollupQuery.isValidQuery(rollup_query)) {
        processRollupQuery(key, row, notes, hists);
      } else {
        processNotRollupQuery(key, row, notes, hists);
      }

      return true;
    }

    private void processNotRollupQuery(final byte[] key, 
        final ArrayList<KeyValue> row, 
        List<Annotation> notes,
        List<HistogramDataPoint> hists) {
      KeyValue compacted = null;
      try {
        final long compaction_start = DateTime.nanoTime();
        compacted = tsdb.compact(row, notes, hists);
        
        // histogram row
        if (hists.size() > 0) {
          histograms.add(new SimpleEntry<byte[], List<HistogramDataPoint>>(key, hists));
          mul_get_dps_post_filter += hists.size();
        }

        mul_get_compaction_time += (DateTime.nanoTime() - compaction_start);
        if (compacted != null) {
          final byte[] compact_value = compacted.value();
          final byte[] compact_qualifier = compacted.qualifier();
          mul_get_number_byte_fetched = mul_get_number_byte_fetched + 
              compacted.value().length + compacted.key().length;
          number_byte_fetched.addAndGet(compacted.value().length + compacted.key().length);
          if (number_byte_fetched.get() > max_bytes) {
            handleException(
                new QueryException(HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE,
                "Sorry, you have attempted to fetch more than our maximum "
                    + "amount of " + (max_bytes / 1024 / 1024) + "MB from storage. " 
                    + "Please try reducing your time range or adjust the query filters."));
            close(false);
            return;
          }
          if (compact_qualifier.length % 2 == 0) {
            // The length of the qualifier is even so this is a put type
            // so the size of the data is the length of the qualifier by 2
            if (compact_value[compact_value.length - 1] == 0) {
              // LOG.debug("All data points we have here are either in seconds
              // or Ms");
              if (Internal.inMilliseconds(compact_qualifier[0])) {
                mul_get_dps_post_filter += compact_qualifier.length / 4;
              } else {
                mul_get_dps_post_filter += compact_qualifier.length / 2;
              }
            } else {
              // LOG.debug("Data Points we have here are stored in second and Ms
              // precision");
              // We wil make a estimate here as iterating over each qualifer
              // could be expensive.
              // We will just divide the qualifier by 3 to estimate the value
              mul_get_dps_post_filter += compact_qualifier.length / 3;
            }
          }
        }
      } catch (IllegalDataException idex) {
        LOG.error("Caught IllegalDataException exception while parsing the " + "row " + key + ", skipping index", idex);
      }

      if (compacted != null) { // Can be null if we ignored all KVs.
        keyValues.add(compacted);
      }
    }

    private void processRollupQuery(final byte[] key, 
        final ArrayList<KeyValue> row, 
        List<Annotation> notes,
        final List<HistogramDataPoint> hists) {
      for (KeyValue kv : row) {
        mul_get_number_byte_fetched = mul_get_number_byte_fetched + 
            kv.value().length + kv.key().length;
        number_byte_fetched.addAndGet(kv.value().length + kv.key().length);
        if (number_byte_fetched.get() > max_bytes) {
          handleException(
              new QueryException(HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE,
              "Sorry, you have attempted to fetch more than our maximum "
                  + "amount of " + (max_bytes / 1024 / 1024) + "MB from storage. " 
                  + "Please try reducing your time range or adjust the query filters."));
          close(false);
          return;
        }
        final byte[] qual = kv.qualifier();

        if (qual.length > 0) {
          // Todo: Bug! Here we shouldn't use the first byte to check the type
          // of this row
          // Instead should parse the byte array to find the suffix and
          // determine the actual type
          if (qual[0] == Annotation.PREFIX()) {
            // This could be a row with only an annotation in it
            final Annotation note = JSON.parseToObject(kv.value(), Annotation.class);
            notes.add(note);
          } else if (qual[0] == HistogramDataPoint.PREFIX) {
            try {
              HistogramDataPoint histogram = Internal.decodeHistogramDataPoint(tsdb, kv);
              hists.add(histogram);
            } catch (Throwable t) {
              LOG.error("Failed to decode histogram data point", t);
            }
          } else {
            if (qual[0] == (byte) rollup_agg_id ||
                qual[0] == (byte) rollup_count_id ||
                rollup_query.getRollupAgg() == Aggregators.AVG || 
                rollup_query.getRollupAgg() == Aggregators.DEV) {
              if (Bytes.memcmp(RollupQuery.SUM, qual, 0, RollupQuery.SUM.length) == 0
                  || Bytes.memcmp(RollupQuery.COUNT, qual, 0, RollupQuery.COUNT.length) == 0) {
                keyValues.add(kv);
              }
            } else if (Bytes.memcmp(rollup_query.getRollupAggPrefix(), qual, 0,
                rollup_query.getRollupAggPrefix().length) == 0) {
              keyValues.add(kv);
            }
          }
        }
      } // end for

      // histogram row
      if (hists.size() > 0) {
        histograms.add(new SimpleEntry<byte[], List<HistogramDataPoint>>(key, hists));
      }
    }

    void close(final boolean ok) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Finished multiget on concurrency index: " + concurrency_index 
            + ", seq id: " + seq_id + ". Fetched rows: " + mul_get_number_row_fetched 
            + ", cells: " + mul_get_number_column_fetched + ", mget time(ms): " 
            + mul_get_time / 1000000 + " and " + ok);
      }
  
      if (query_stats != null) {
        query_stats.addScannerStat(query_index, seq_id, QueryStat.SCANNER_TIME,
            DateTime.nanoTime() - mul_get_start_time);

        // Scanner Stats
        query_stats.addScannerStat(query_index, seq_id, QueryStat.ROWS_FROM_STORAGE, 
            mul_get_number_row_fetched);

        query_stats.addScannerStat(query_index, seq_id, QueryStat.COLUMNS_FROM_STORAGE,
            mul_get_number_column_fetched);

        query_stats.addScannerStat(query_index, seq_id, QueryStat.BYTES_FROM_STORAGE, 
            mul_get_number_byte_fetched);

        query_stats.addScannerStat(query_index, seq_id, QueryStat.HBASE_TIME, 
            mul_get_time);
        query_stats.addScannerStat(query_index, seq_id, QueryStat.SUCCESSFUL_SCAN, 
            ok ? 1 : 0);

        // Post Scan stats
        query_stats.addScannerStat(query_index, seq_id, QueryStat.ROWS_POST_FILTER, 
            mul_get_rows_post_filter);
        query_stats.addScannerStat(query_index, seq_id, QueryStat.DPS_POST_FILTER, 
            mul_get_dps_post_filter);
        query_stats.addScannerStat(query_index, seq_id, QueryStat.SCANNER_UID_TO_STRING_TIME,
            mul_get_uid_resolved_time);
        query_stats.addScannerStat(query_index, seq_id, QueryStat.UID_PAIRS_RESOLVED, 
            mul_get_uids_resolved);
        query_stats.addScannerStat(query_index, seq_id, QueryStat.COMPACTION_TIME, 
            mul_get_compaction_time);
      }

      if (ok) {
        validateMultigetData(keyValues, annotations, histograms);
      } 
      else {
        finished_multi_get_cnt.incrementAndGet();
      }

      // check we have finished all the multi get
      if (!checkAllFinishAndTriggerCallback()) {
        // check to fire a new multi get in this concurrency bucket
        List<MultiGetTask> salt_mul_get_tasks = multi_get_tasks.get(concurrency_index);
        int task_index = multi_get_indexs.get(concurrency_index).incrementAndGet();
        if (task_index < salt_mul_get_tasks.size()) {
          MultiGetTask task = salt_mul_get_tasks.get(task_index);
          MulGetCB mgcb = new MulGetCB(concurrency_index, task.getTSUIDs(), task.getGets());
          mgcb.fetch();
        }
      }
    }
  }

  /**
   * Initiate the get requests and return the tree map of results.
   * @return A non-null tree map of results (may be empty)
   */
  public Deferred<TreeMap<byte[], Span>> fetch() {
    if(tags.isEmpty()) {
      return Deferred.fromResult(null);
    }
    startFetch();
    return results;
  }

  /**
   * Initiate the get requests and return the tree map of results.
   * @return A non-null tree map of results (may be empty)
   */
  public Deferred<TreeMap<byte[], HistogramSpan>> fetchHistogram() {
    startFetch();
    return histogramResults;
  }

  /**
   * Start the work of firing up X concurrent get requests.
   */
  private void startFetch() {
    prepareConcurrentMultiGetTasks();

    // set the time of starting
    fetch_start_time = System.currentTimeMillis();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Start to fetch data using multiget, there will be " + multi_get_wait_cnt
          + " multigets to call");
    }

    for (int con_idx = 0; con_idx < concurrency_multi_get; ++con_idx) {
      final List<MultiGetTask> con_mul_get_tasks = multi_get_tasks.get(con_idx);
      final int task_index = multi_get_indexs.get(con_idx).incrementAndGet();

      if (task_index < con_mul_get_tasks.size()) {
        final MultiGetTask task = con_mul_get_tasks.get(task_index);
        final MulGetCB mgcb = new MulGetCB(con_idx, task.getTSUIDs(), task.getGets());
        mgcb.fetch();
      }
    } // end for
  }
  
  /**
   * Compiles the list of TSUIDs and GetRequests to send to execute against
   * storage. Each batch will only have requests for one salt, i.e a batch
   * will not have requests with multiple salts.
   */
  @VisibleForTesting
  void prepareConcurrentMultiGetTasks() {
    multi_get_wait_cnt = 0;
    prepare_multi_get_start_time = DateTime.currentTimeMillis();


    final List<Long> row_base_time_list;
    if (RollupQuery.isValidQuery(rollup_query)) {
      row_base_time_list = prepareRowBaseTimesRollup();
    } else {
      row_base_time_list = prepareRowBaseTimes();
    }

    int next_concurrency_index = 0;
    List<GetRequest> gets_to_prepare = new ArrayList<GetRequest>(batch_size);
    Set<byte[]> tsuids = new ByteSet();
    
    final ByteMap<ByteMap<List<GetRequest>>> all_tsuids_gets;
    if (multiget_no_meta) {
      // prepare the tagvs combinations and base time list
      
      final List<byte[][]> tagv_compinations = prepareAllTagvCompounds();
      all_tsuids_gets = prepareRequestsNoMeta(tagv_compinations, row_base_time_list);
    } else {
      all_tsuids_gets = prepareRequests(row_base_time_list, tags);

    }
    // Iterate over all salts
    for (final Entry<byte[], ByteMap<List<GetRequest>>> salts_entry : all_tsuids_gets.entrySet()) {
      if (gets_to_prepare.size() > 0) { // if we have any gets_to_prepare for previous salt, create a 
                                        // request out of it.
        final MultiGetTask task = new MultiGetTask(tsuids, gets_to_prepare);
        final List<MultiGetTask> mulget_task_list = 
            multi_get_tasks.get((next_concurrency_index++) % concurrency_multi_get);
        mulget_task_list.add(task);
        ++multi_get_wait_cnt;
        multi_get_num_get_requests = multi_get_num_get_requests + gets_to_prepare.size();
        gets_to_prepare = new ArrayList<GetRequest>(batch_size);
        tsuids = new ByteSet();
      }
      byte[] curr_salt = salts_entry.getKey();
      // Iterate over all tsuid's in curr_salt and add them to gets_to_prepare
      for (final Entry<byte[], List<GetRequest>> gets_entry : salts_entry.getValue()) {
        byte[] tsuid = gets_entry.getKey();
      
          for (GetRequest request : gets_entry.getValue()) { 
            if (gets_to_prepare.size() >= batch_size) { // close batch and create a MultiGetTask
              final MultiGetTask task = new MultiGetTask(tsuids, gets_to_prepare);
              final List<MultiGetTask> mulget_task_list = 
                  multi_get_tasks.get((next_concurrency_index++) % concurrency_multi_get);
              mulget_task_list.add(task);
              ++multi_get_wait_cnt;
              multi_get_num_get_requests = multi_get_num_get_requests + gets_to_prepare.size();
              if (LOG.isDebugEnabled()) {
                LOG.debug("Finished preparing MultiGetRequest with " + gets_to_prepare.size() + " requests for salt " + curr_salt + 
                 " for tsuid " + Bytes.pretty(tsuid));
              }
              // prepare a new task list and tsuids
              gets_to_prepare = new ArrayList<GetRequest>(batch_size);
              tsuids = new ByteSet();
            }             
            gets_to_prepare.add(request);
            tsuids.add(gets_entry.getKey());
          }
          tsuids.add(gets_entry.getKey());
      }
    }
   
  
    if (gets_to_prepare.size() > 0) {
      
      final MultiGetTask task = new MultiGetTask(tsuids, gets_to_prepare);
      final List<MultiGetTask> mulget_task_list = 
          multi_get_tasks.get((next_concurrency_index++) % concurrency_multi_get);
      mulget_task_list.add(task);
      ++multi_get_wait_cnt;
      multi_get_num_get_requests = multi_get_num_get_requests + gets_to_prepare.size();
      LOG.debug("Finished preparing MultiGetRequest with " + gets_to_prepare.size());
      gets_to_prepare = new ArrayList<GetRequest>(batch_size);
      tsuids = new ByteSet();
    }

    prepare_multi_get_end_time = DateTime.currentTimeMillis();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Finished preparing concurrency multi get task with " 
          + multi_get_wait_cnt + " tasks using "
          + (prepare_multi_get_end_time - prepare_multi_get_start_time) + "ms");
    }

  }

  private void validateMultigetData(List<KeyValue> kvs, 
      Map<byte[], List<Annotation>> annotations,
      List<SimpleEntry<byte[], List<HistogramDataPoint>>> histograms) {
    int tasks = finished_multi_get_cnt.incrementAndGet();

    if (kvs.size() > 0) {
      kvsmap.put(tasks, kvs);
    }

    if (annotMap != null) {
      for (byte[] key : annotations.keySet()) {
        List<Annotation> notes = annotations.get(key);

        if (notes.size() > 0) {
          annotMap.put(key, notes);
        }
      }
    }

    if (histograms.size() > 0) {
      histMap.put(tasks, histograms);
    }
  }

  private boolean checkAllFinishAndTriggerCallback() {
    if (multi_get_wait_cnt == finished_multi_get_cnt.get()) {
      try {
        if (exception == null) {
          mergeAndReturnResults();
        }
      } catch (Exception ex) {
        LOG.error("Failed merging and returning results, calling back with "
            + "exception", ex);

        if (!isHistogramScan()) {
          results.callback(ex);
        } else {
          histogramResults.callback(ex);
        }
      }
      return true;
    } else {
      return false;
    }
  }

  private void mergeAndReturnResults() throws Exception {
    final long hbase_time = DateTime.currentTimeMillis();
    TsdbQuery.scanlatency.add((int) (hbase_time - fetch_start_time));
    if (LOG.isDebugEnabled()) {
      LOG.debug("Finished fetching data for metric: " + Bytes.pretty(metric)
      + " using " + (hbase_time - fetch_start_time) + "ms");
    }
    LOG.info("Finished fetching data for metric: " + Bytes.pretty(metric)
    + " using " + (hbase_time - fetch_start_time) + "ms");
    if (exception != null) {
      LOG.error("After all of the multi-gets finished, at "
          + "least one threw an exception", exception);
      throw exception;
    }

    final long merge_start = DateTime.nanoTime();

    // Merge sorted spans together
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
      histogramResults.callback(histogramSpans);
    }
  }

  private boolean isHistogramScan() {
    return histogramSpans != null;
  }

  private void mergeHistogramDataPoints() {
    if (histogramSpans != null) {
      for (List<SimpleEntry<byte[], List<HistogramDataPoint>>> rows : histMap.values()) {
        if (null == rows || rows.isEmpty()) {
          LOG.error("Found a histogram rows list that was null or empty");
          continue;
        }

        // for all the rows with the same salt in the timestamp order
        for (final SimpleEntry<byte[], List<HistogramDataPoint>> row : rows) {
          if (null == row) {
            LOG.error("Found a histogram row item that was null");
            continue;
          }

          HistogramSpan histSpan = null;
          try {
            histSpan = histogramSpans.get(row.getKey());
          } catch (RuntimeException e) {
            LOG.error("Failed to fetch the histogram span", e);
          }

          if (histSpan == null) {
            histSpan = new HistogramSpan(tsdb);
            histogramSpans.put(row.getKey(), histSpan);
          }

          if (annotMap.containsKey(row.getKey())) {
            histSpan.getAnnotations().addAll(annotMap.get(row.getKey()));
            annotMap.remove(row.getKey());
          }

          try {
            histSpan.addRow(row.getKey(), row.getValue());
          } catch (RuntimeException e) {
            LOG.error("Exception adding row to histogram span", e);
          }
        } // end for
      } // end for

      histMap.clear();

      for (byte[] key : annotMap.keySet()) {
        HistogramSpan histSpan = histogramSpans.get(key);

        if (histSpan == null) {
          histSpan = new HistogramSpan(tsdb);
          histogramSpans.put(key, histSpan);
        }

        histSpan.getAnnotations().addAll(annotMap.get(key));
      }

      annotMap.clear();
    }
  }

  private void mergeDataPoints() {
    for (List<KeyValue> kvs : kvsmap.values()) {
      if (kvs == null || kvs.isEmpty()) {
        LOG.error("Found a key value list that was null or empty");
        continue;
      }
      for (final KeyValue kv : kvs) {

        if (kv == null) {
          LOG.error("Found a key value item that was null");
          continue;
        }
        if (kv.key() == null) {
          LOG.error("A key for a kv was null");
          continue;
        }

        Span datapoints = null;
        try {
          datapoints = spans.get(kv.key());
        } catch (RuntimeException e) {
          LOG.error("Failed to fetch the span", e);
        }

        // If this tsdb follows append logic, then there will not be any
        // duplicates here. But if it is not, then there can be multiple
        // non-compcated or out of order rows here
        if (datapoints == null) {
          datapoints = RollupQuery.isValidQuery(rollup_query) 
              ? new RollupSpan(tsdb, rollup_query)
                  : new Span(tsdb);
              spans.put(kv.key(), datapoints);
        }

        if (annotMap.containsKey(kv.key())) {
          for (Annotation note : annotMap.get(kv.key())) {
            datapoints.getAnnotations().add(note);
          }
          annotMap.remove(kv.key());
        }
        try {
          datapoints.addRow(kv);
        } catch (RuntimeException e) {
          LOG.error("Exception adding row to span", e);
        }
      }
    }

    kvsmap.clear();

    for (byte[] key : annotMap.keySet()) {
      Span datapoints = (Span) spans.get(key);

      if (datapoints == null) {
        datapoints = new Span(tsdb);
        spans.put(key, datapoints);
      }

      for (Annotation note : annotMap.get(key)) {
        datapoints.getAnnotations().add(note);
      }
    }

    annotMap.clear();
  }
  boolean exception1;
  /**
   * If one or more of the scanners throws an exception then we should close it
   * and pass the exception here so that we can catch and return it to the
   * caller. If all of the scanners have finished, this will callback to the 
   * caller immediately.
   * @param e The exception to store.
   */
  private void handleException(final Exception e) {
    // make sure only one scanner can set the exception
    finished_multi_get_cnt.incrementAndGet();
    if (exception == null) {
      synchronized (this) {
        if (exception == null) {
          exception = e;
          // fail once and fast on the first scanner to throw an exception
          try {
            if (exception != null) {
              mergeAndReturnResults();
            }
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

  /**
   * Generates a list of Unix Epoch Timestamps for the row key base times given
   * the start and end times of the query.
   * @return A non-null list of at least one base row.
   */
  @VisibleForTesting
  List<Long> prepareRowBaseTimes() {
    final ArrayList<Long> row_base_time_list = new ArrayList<Long>(
        (int) ((end_row_time - start_row_time) / Const.MAX_TIMESPAN));
    // NOTE: inclusive end here
    long ts = (start_row_time - (start_row_time % Const.MAX_TIMESPAN));
    while (ts <= end_row_time) {
      row_base_time_list.add(ts);
      ts += Const.MAX_TIMESPAN;
    }
    return row_base_time_list;
  }

  /**
   * Generates a list of Unix Epoch Timestamps for the row key base times given
   * the start and end times of the query and rollup interval given.
   * @return A non-null list of at least one base row.
   */
  @VisibleForTesting
  List<Long> prepareRowBaseTimesRollup() {
    final RollupInterval interval = rollup_query.getRollupInterval();

    // standard TSDB table format, i.e. we're using the default table and schema
    if (interval.getUnits() == 'h') {
      return prepareRowBaseTimes();
    } else {
      final List<Long> row_base_times = new ArrayList<Long>(
          (int) ((end_row_time - start_row_time) / interval.getIntervals()));

      long ts = RollupUtils.getRollupBasetime(start_row_time, interval);
      while (ts <= end_row_time) {
        row_base_times.add(ts);
        // TODO - possible this could overshoot in some cases. It shouldn't
        // if the rollups are properly configured, but... you know. Check it.
        ts = RollupUtils.getRollupBasetime(ts + 
            (interval.getIntervalSeconds() * interval.getIntervals()), interval);
      }
      return row_base_times;
    }
  }

  /**
   * We have multiple tagks and each tagk may has multiple possible values. 
   * This routine generates all the possible tagv compounds basing on the
   * present sequence of the tagks. Each compound will be used to generate 
   * the final tsuid.
   * For example, if there are two tag keys where the first tag key has 4 values
   * and the second tagk has 2 values then the resulting list will have 6
   * entries, one for each permutation.
   * 
   * @return a non-null list that contains all the possible compounds 
   */
  @VisibleForTesting
  List<byte[][]> prepareAllTagvCompounds() {
    List<byte[][]> pre_phase_tags = new LinkedList<byte[][]>();
    pre_phase_tags.add(new byte[tags.get(0).size()][TSDB.tagv_width()]);

    List<byte[][]> next_phase_tags = new LinkedList<byte[][]>();
    int next_append_index = 0;

    for (final Map.Entry<byte[], byte[][]> tag : tags.get(0)) {
      byte[][] tagv = tag.getValue();
      for (int i = 0; i < tagv.length; ++i) {
        for (byte[][] pre_phase_tag : pre_phase_tags) {
          final byte[][] next_phase_tag = 
              new byte[tags.get(0).size()][tsdb.tag_values.width()];

          // copy the tagv from index 0 ~ next_append_index - 1
          for (int k = 0; k < next_append_index; ++k) { 
            System.arraycopy(pre_phase_tag[k], 0, next_phase_tag[k], 0, 
                tsdb.tag_values.width());
          }

          // copy the tagv in next_append_index
          System.arraycopy(tagv[i], 0, next_phase_tag[next_append_index], 0, 
              tsdb.tag_values.width());
          next_phase_tags.add(next_phase_tag);
        }
      } // end for

      ++next_append_index;
      pre_phase_tags = next_phase_tags;
      next_phase_tags = new LinkedList<byte[][]>();
    } // end for
    return pre_phase_tags;
  }
    
    /**
     * Generates a map of TSUIDs to get requests given the tag permutations.
     * If all salts is enabled, each TSUID will have Const.SALT_BUCKETS() number
     * of entries. Otherwise each TSUID will have one row key.
     * @param tagv_compounds The permutations of tag key and value combinations to
     * search for.
     * @param base_time_list The list of base timestamps.
     * @return A non-null map of TSUIDs to lists of get requests to send to HBase.
     */
    @VisibleForTesting
    ByteMap<ByteMap<List<GetRequest>>> prepareRequestsNoMeta(final List<byte[][]> tagv_compounds, 
        final List<Long> base_time_list) {
      
      final int row_size = (Const.SALT_WIDTH() + tsdb.metrics.width() 
          + Const.TIMESTAMP_BYTES
          + (tsdb.tag_names.width() * tags.get(0).size()) 
          + (tsdb.tag_values.width() * tags.get(0).size()));

      final ByteMap<List<GetRequest>> tsuid_rows = new ByteMap<List<GetRequest>>();
      for (final byte[][] tagvs : tagv_compounds) {
        // TSUID's don't have salts
        // TODO: we reallly don't have to allocate tsuid's here.
        // we can use the row_key array to fetch the tsuid.
        // This will just double the memory utilization per time series.
        final byte[] tsuid = new byte[tsdb.metrics.width() 
            + (tags.get(0).size() * tsdb.tag_names.width())
            + tags.get(0).size() * tsdb.tag_values.width()];
        final byte[] row_key = new byte[row_size];
        
        // metric
        System.arraycopy(metric, 0, row_key, Const.SALT_WIDTH(), tsdb.metrics.width());
        System.arraycopy(metric, 0, tsuid, 0, tsdb.metrics.width());
        
        final List<GetRequest> rows = 
            new ArrayList<GetRequest>(base_time_list.size());
        
        // copy tagks and tagvs to the row key
        int tag_index = 0;
        int row_key_copy_offset = Const.SALT_WIDTH() + tsdb.metrics.width() 
          + Const.TIMESTAMP_BYTES;
        int tsuid_copy_offset = tsdb.metrics.width();
        for (Map.Entry<byte[], byte[][]> tag : tags.get(0)) {
          // tagk
          byte[] tagk = tag.getKey();
          System.arraycopy(tagk, 0, row_key, row_key_copy_offset, tsdb.tag_names.width());
          System.arraycopy(tagk, 0, tsuid, tsuid_copy_offset, tsdb.tag_names.width());
          row_key_copy_offset += tsdb.tag_names.width();
          tsuid_copy_offset += tsdb.tag_names.width();

          // tagv
          System.arraycopy(tagvs[tag_index], 0, row_key, row_key_copy_offset, 
              tsdb.tag_values.width());
          System.arraycopy(tagvs[tag_index], 0, tsuid, tsuid_copy_offset, 
              tsdb.tag_values.width());
          row_key_copy_offset += tsdb.tag_values.width();
          tsuid_copy_offset += tsdb.tag_values.width();

          // move to the next tag
          ++tag_index;
        }
        
        // iterate for each timestamp, making a copy of the key and tweaking it's 
        // timestamp.
        for (final long row_base_time : base_time_list) {
          final byte[] key_copy = Arrays.copyOf(row_key, row_key.length);
          
          // base time
          Internal.setBaseTime(key_copy, (int) row_base_time);
          
          if (get_all_salts) {
            for (int i = 0; i < Const.SALT_BUCKETS(); i++) {
              final byte[] copy = Arrays.copyOf(key_copy, key_copy.length);
              // TODO - handle multi byte salts
              copy[0] = (byte) i;
              rows.add(new GetRequest(table_to_fetch, copy, TSDB.FAMILY));
            }
          } else {
            // salt
            RowKey.prefixKeyWithSalt(key_copy);
            rows.add(new GetRequest(table_to_fetch, key_copy, TSDB.FAMILY));
          }
        } // end for
        
        tsuid_rows.put(tsuid, rows);
      } // end for
      ByteMap<ByteMap<List<GetRequest>>> return_obj = new ByteMap<ByteMap<List<GetRequest>>>();
   //   byte[] salt = new byte[Const.SALT_WIDTH()];
     // System.arraycopy("1".getBytes(), 0, salt, 0, Const.SALT_WIDTH());
      return_obj.put("0".getBytes(), tsuid_rows);
       return return_obj;
    }

  /**
   * Generates a map of TSUIDs per salt to get requests given the tag permutations.
   * If all salts is enabled, each TSUID will have Const.SALT_BUCKETS() number
   * of entries. Otherwise each TSUID will have one row key.
   * @param tagv_compounds The cardinality of tag key and value combinations to
   * search for.
   * @param base_time_list The list of base timestamps.
   * @return A non-null map of TSUIDs to lists of get requests to send to HBase.
   */
  @VisibleForTesting
  ByteMap<ByteMap<List<GetRequest>>> prepareRequests(final List<Long> base_time_list, List<ByteMap<byte[][]>> tags) {
   
    final ByteMap<ByteMap<List<GetRequest>>> tsuid_rows = new ByteMap<ByteMap<List<GetRequest>>>();
    // TSUID's don't have salts
    //    final byte[] tsuid = new byte[tsdb.metrics.width() 
    //                                  + (tags.size() * tsdb.tag_names.width())
    //                                  + tags.size() * tsdb.tag_values.width()];
    //    final byte[] row_key = new byte[row_size];

    
    // copy tagks and tagvs to the row key
    for (ByteMap<byte[][]> each_row_key : tags) {
      
      final int row_size = (Const.SALT_WIDTH() + tsdb.metrics.width() 
        + Const.TIMESTAMP_BYTES
        + (tsdb.tag_names.width() * each_row_key.size()) 
        + (tsdb.tag_values.width() * each_row_key.size()));
       byte[] tsuid = new byte[tsdb.metrics.width() 
                                    + (each_row_key.size() * tsdb.tag_names.width())
                                    + each_row_key.size() * tsdb.tag_values.width()];
       byte[] row_key = new byte[row_size];
       int row_key_copy_offset = Const.SALT_WIDTH() + tsdb.metrics.width() 
       + Const.TIMESTAMP_BYTES;
       int tsuid_copy_offset = tsdb.metrics.width();

       // metric
      System.arraycopy(metric, 0, row_key, Const.SALT_WIDTH(), tsdb.metrics.width());
      System.arraycopy(metric, 0, tsuid, 0, tsdb.metrics.width());
      
      final List<GetRequest> rows = 
          new ArrayList<GetRequest>(base_time_list.size());
      for (Map.Entry<byte[], byte[][]> tag_arr : each_row_key.entrySet()) {
        byte[] tagv = tag_arr.getValue()[0]; 
        // tagk
        byte[] tagk = tag_arr.getKey();

        System.arraycopy(tagk, 0, row_key, row_key_copy_offset, tsdb.tag_names.width());
        System.arraycopy(tagk, 0, tsuid, tsuid_copy_offset, tsdb.tag_names.width());
        row_key_copy_offset += tsdb.tag_names.width();
        tsuid_copy_offset += tsdb.tag_names.width();

        // tagv
        System.arraycopy(tagv, 0, row_key, row_key_copy_offset, 
            tsdb.tag_values.width());
        System.arraycopy(tagv, 0, tsuid, tsuid_copy_offset, 
            tsdb.tag_values.width());
        row_key_copy_offset += tsdb.tag_values.width();
        tsuid_copy_offset += tsdb.tag_values.width();
        
      } 
     
    // iterate for each timestamp, making a copy of the key and tweaking it's 
    // timestamp.
    for (final long row_base_time : base_time_list) {
      final byte[] key_copy = Arrays.copyOf(row_key, row_key.length);

      // base time
      Internal.setBaseTime(key_copy, (int) row_base_time);

      if (get_all_salts) {
        for (int i = 0; i < Const.SALT_BUCKETS(); i++) {
          byte[] salt = RowKey.getSaltBytes(i);
          final byte[] copy = Arrays.copyOf(key_copy, key_copy.length);
          System.arraycopy(salt, 0, copy, 0, Const.SALT_WIDTH());
          rows.add(new GetRequest(table_to_fetch, copy, TSDB.FAMILY));
        }
      } else {
        // salt
        RowKey.prefixKeyWithSalt(key_copy);
        rows.add(new GetRequest(table_to_fetch, key_copy, TSDB.FAMILY));
      }
    } // end for
    for (GetRequest request : rows) {
      byte[] salt = new byte[Const.SALT_WIDTH()];
      System.arraycopy(request.key(), 0, salt, 0, Const.SALT_WIDTH());
      if (tsuid_rows.containsKey(salt)) {
         ByteMap<List<GetRequest>> map = tsuid_rows.get(salt);
         if (map.containsKey(tsuid)) {
           List<GetRequest> list = map.get(tsuid);
           list.add(request);
         } else {
           List<GetRequest> list = new ArrayList<GetRequest>();
           list.add(request);
           map.put(tsuid, list);
         }
      } else {
        ByteMap<List<GetRequest>> map = new ByteMap<List<GetRequest>>();
        List<GetRequest> list = new ArrayList<GetRequest>();
        list.add(request);
        map.put(tsuid, list);
        tsuid_rows.put(salt, map);
      }
    }
    }
    
    return tsuid_rows;
  }

  @VisibleForTesting
  List<List<MultiGetTask>> getMultiGetTasks() {
    return multi_get_tasks;
  }
}
