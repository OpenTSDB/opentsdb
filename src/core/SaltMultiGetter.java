// This file is part of OpenTSDB.
// Copyright (C) 20156  The OpenTSDB Authors.
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
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.hbase.async.Bytes.ByteMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import org.hbase.async.Bytes;
import org.hbase.async.GetRequest;
import org.hbase.async.GetResultOrException;
import org.hbase.async.KeyValue;

import net.opentsdb.meta.Annotation;
import net.opentsdb.rollup.RollupInterval;
import net.opentsdb.rollup.RollupQuery;
import net.opentsdb.rollup.RollupSpan;
import net.opentsdb.stats.QueryStats;
import net.opentsdb.stats.QueryStats.QueryStat;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.JSON;

public class SaltMultiGetter {
  private static final Logger LOG = LoggerFactory.getLogger(SaltMultiGetter.class);
  private final TSDB tsdb;
  private final byte[] metric;
  private final ByteMap<byte[][]> tags;
  private final long start_row_time; // in sec
  private final long end_row_time; // in sec
  private final byte[] table_to_fetch;
  private final TreeMap<byte[], Span> spans;
  private final long timeout;
  private final RollupQuery rollup_query;
  private final QueryStats query_stats;
  private final int query_index;
  private final long max_bytes;

  private long max_pre_filter_dps;
  private long max_dps;
  private int mulget_wait_cnt;
  
  ////////////////////////////////////////////////////////////////////////////////////////
  private final Map<Integer, List<KeyValue>> kvsmap = new ConcurrentHashMap<Integer, List<KeyValue>>();

  private final Map<byte[], List<Annotation>> annotMap = Collections
      .synchronizedMap(new TreeMap<byte[], List<Annotation>>(new RowKey.SaltCmp()));

  private final Deferred<TreeMap<byte[], Span>> results = new Deferred<TreeMap<byte[], Span>>();
  
  private final ArrayList<List<MulgetTask>> mul_get_tasks;
  private final ArrayList<AtomicInteger> mul_get_indexs;

  ////////////////////////////////////////////////////////////////////////////////////////
  private long prepare_multi_get_start_time;
  
  private long prepare_multi_get_end_time;
  
  // the timestamp of starting fetching data
  private long fetch_start_time;
  
  // the number of data point fetched
  private AtomicLong number_pre_filter_data_point;
  
  private AtomicLong num_post_filter_data_points;
  
  // the byte size of the data fetched
  private AtomicLong byte_size_feted;
  
  // the finished multi get number
  private AtomicInteger finished_mulget_cnt;
  
  private AtomicInteger multi_get_seq_id;

  public SaltMultiGetter(TSDB tsdb, 
                        byte[] metric, 
                        ByteMap<byte[][]> tags, 
                        final long start_row_time,
                        final long end_row_time, 
                        final byte[] table_to_fetch, 
                        TreeMap<byte[], Span> spans,
                        final long timeout, 
                        final RollupQuery rollup_query,
                        final QueryStats query_stats, 
                        final int query_index, 
                        final long max_bytes, 
                        final boolean override_count_limit) {
    this.tsdb = tsdb;
    this.metric = metric;
    this.tags = tags;
    this.start_row_time = start_row_time;
    this.end_row_time = end_row_time;
    this.table_to_fetch = table_to_fetch;
    this.spans = spans;
    this.timeout = timeout;
    this.rollup_query = rollup_query;
    this.query_stats = query_stats;
    this.query_index = query_index;
    this.max_bytes = max_bytes;

    if (override_count_limit) {
      this.max_pre_filter_dps = 0;
      this.max_dps = 0;
    } else {
      // TODO
      //this.max_pre_filter_dps = tsdb.getConfig().getLong("tsd.core.scanner.max_pre_filter_dps");
      //this.max_dps = tsdb.getConfig().max_data_points();
    }
   
    int concurrency_multi_get = tsdb.config.mul_get_concurrency_number();
    mul_get_tasks = new ArrayList<List<MulgetTask>>(concurrency_multi_get);
    mul_get_indexs = new ArrayList<AtomicInteger>(concurrency_multi_get);
    for (int i = 0; i < concurrency_multi_get; ++i) {
      mul_get_tasks.add(new ArrayList<MulgetTask>());
      mul_get_indexs.add(new AtomicInteger(-1));
    }
    
    number_pre_filter_data_point = new AtomicLong(0);
    num_post_filter_data_points = new AtomicLong(0);
    byte_size_feted = new AtomicLong(0);
    finished_mulget_cnt = new AtomicInteger(0);
    multi_get_seq_id = new AtomicInteger(-1);
  }
  
  final class TSUIDComparator implements Comparator<byte[]> {

    @Override
    public int compare(byte[] left, byte[] right) {
      for (int i = 0, j = 0; i < left.length && j < right.length; i++, j++) {
        int a = (left[i] & 0xff);
        int b = (right[j] & 0xff);
        if (a != b) {
          return a - b;
        }
      }
      return left.length - right.length;
    }
  }
  
  final class MulgetTask {
    private final Set<byte[]> tsuids;
    private final List<GetRequest> gets;
   
    public MulgetTask(final Set<byte[]> tsuids, final List<GetRequest> gets) {
      this.tsuids = tsuids;
      this.gets = gets;
    }
    
    public Set<byte[]> getTSUIDs() {
      return this.tsuids;
    }
    
    public List<GetRequest> getGets() {
      return this.gets;
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
    private final Map<byte[], List<Annotation>> annotations = new ConcurrentHashMap<byte[], List<Annotation>>();
    
    /////////////////////////////////////////////////////////////////////////////////////////
    // nanosecond times - trace response metrics                                          //
    ////////////////////////////////////////////////////////////////////////////////////////
    
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

    public MulGetCB(final int concurrency_index, final Set<byte[]> tsuids, final List<GetRequest> gets) {
      this.concurrency_index = concurrency_index;
      this.tsuids = tsuids;
      this.gets = gets;

      if (query_stats != null) {
        seq_id = multi_get_seq_id.incrementAndGet();
        StringBuilder sb = new StringBuilder();
        sb.append("Mulget_").append(this.concurrency_index).append("_").append(seq_id);
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
        LOG.error("Multi get threw an exception : ", e);
        close(false);
        return null;
      }
    }
    
    public Object fetch() {
      mul_get_start_time = DateTime.nanoTime();
      
      if (LOG.isDebugEnabled()) {
        LOG.debug("Try to fetch data for concurrency index: " 
            + this.concurrency_index + "; with " + this.gets.size() + " gets");
      }
      return tsdb.client.get(this.gets)
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
      mul_get_time = (DateTime.nanoTime() - this.mul_get_start_time);
      
      try {
        for (final GetResultOrException result : results) {
          if (null != result.getCells()) {
            ArrayList<KeyValue> row = result.getCells();
            if (row.size() == 0) {
              continue;
            }
            
            number_pre_filter_data_point.addAndGet(row.size());
            ++mul_get_number_row_fetched;
            mul_get_number_column_fetched += row.size();

            final byte[] key = row.get(0).key();
            final byte[] tsuid_key = UniqueId.getTSUIDFromKey(key, 
                TSDB.metrics_width(), Const.TIMESTAMP_BYTES);

            if (!this.tsuids.contains(tsuid_key)) {
              LOG.error("Multi geter fetched the wrong row " + result + " when fetching metric: " + Bytes.pretty(metric));
              continue;
            }
            
            process(key, row);
          } else {
            // TODO we don't get cells for some get requests
          }
        } // end for
      } catch (Exception e) {
        close(true); 
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

      if (RollupQuery.isValidQuery(rollup_query)) {
        processRollupQuery(key, row, notes);
      } else {
        processNotRollupQuery(key, row, notes);
      }
      
      return true;
    }

    private void processNotRollupQuery(final byte[] key, 
                                       final ArrayList<KeyValue> row, 
                                       List<Annotation> notes) {
      KeyValue compacted = null;
      try {
        final long compaction_start = DateTime.nanoTime();
        compacted = tsdb.compact(row, notes);

        mul_get_compaction_time += (DateTime.nanoTime() - compaction_start);
        if (compacted != null) {
          final byte[] compact_value = compacted.value();
          final byte[] compact_qualifier = compacted.qualifier();

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

    private void processRollupQuery(final byte[] key, final ArrayList<KeyValue> row, List<Annotation> notes) {
      for (KeyValue kv : row) {
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
          } else {
            if (rollup_query.getRollupAgg() == Aggregators.AVG || rollup_query.getRollupAgg() == Aggregators.DEV) {
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
    }
  
    void close(final boolean ok) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Finished multiget on concurrency index: " + this.concurrency_index + ", seq id: " + this.seq_id
            + ". Fetched rows: " + this.mul_get_number_row_fetched + ", cells: " + this.mul_get_number_column_fetched
            + ", mget time(ms): " + this.mul_get_time / 1000000);
      }
      
      if (query_stats != null) {
        query_stats.addScannerStat(query_index, seq_id, QueryStat.SCANNER_TIME,
            DateTime.nanoTime() - mul_get_start_time);

        // Scanner Stats
        query_stats.addScannerStat(query_index, seq_id, QueryStat.ROWS_FROM_STORAGE, this.mul_get_number_row_fetched);

        query_stats.addScannerStat(query_index, seq_id, QueryStat.COLUMNS_FROM_STORAGE,
            this.mul_get_number_column_fetched);

        query_stats.addScannerStat(query_index, seq_id, QueryStat.BYTES_FROM_STORAGE, this.mul_get_number_byte_fetched);

        query_stats.addScannerStat(query_index, seq_id, QueryStat.HBASE_TIME, mul_get_time);
        query_stats.addScannerStat(query_index, seq_id, QueryStat.SUCCESSFUL_SCAN, ok ? 1 : 0);

        // Post Scan stats
        query_stats.addScannerStat(query_index, seq_id, QueryStat.ROWS_POST_FILTER, mul_get_rows_post_filter);
        query_stats.addScannerStat(query_index, seq_id, QueryStat.DPS_POST_FILTER, mul_get_dps_post_filter);
        query_stats.addScannerStat(query_index, seq_id, QueryStat.SCANNER_UID_TO_STRING_TIME,
            mul_get_uid_resolved_time);
        query_stats.addScannerStat(query_index, seq_id, QueryStat.UID_PAIRS_RESOLVED, mul_get_uids_resolved);
        query_stats.addScannerStat(query_index, seq_id, QueryStat.COMPACTION_TIME, mul_get_compaction_time);
      }

      if (ok) {
        validateMultigetData(keyValues, annotations);
      } else {
        finished_mulget_cnt.incrementAndGet();
      }
      
      // check we have finished all the multi get
      if (!checkAllFinishAndTriggerCallback()) {
        // check to fire a new multi get in this concurrency bucket
        List<MulgetTask> salt_mul_get_tasks = mul_get_tasks.get(this.concurrency_index);
        int task_index = mul_get_indexs.get(this.concurrency_index).incrementAndGet();
        if (task_index < salt_mul_get_tasks.size()) {
          MulgetTask task = salt_mul_get_tasks.get(task_index);
          MulGetCB mgcb = new MulGetCB(this.concurrency_index, task.getTSUIDs(), task.getGets());
          mgcb.fetch();
        }
      }
    }
  }
  
  public Deferred<TreeMap<byte[], Span>> fetch() {
    startFetch();
    return this.results;
  }
  
  private void startFetch() {
    prepareConcurrentMultiGetTasks();
    int concurrency_number = tsdb.config.mul_get_concurrency_number();

    // set the time of starting
    fetch_start_time = System.currentTimeMillis();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Start to fetch data using multiget, there will be " + mulget_wait_cnt
          + " multigets to call");
    }
    
    for (int con_idx = 0; con_idx < concurrency_number; ++con_idx) {
      List<MulgetTask> con_mul_get_tasks = mul_get_tasks.get(con_idx);
      int task_index = this.mul_get_indexs.get(con_idx).incrementAndGet();

      if (task_index < con_mul_get_tasks.size()) {
        MulgetTask task = con_mul_get_tasks.get(task_index);
        MulGetCB mgcb = new MulGetCB(con_idx, task.getTSUIDs(), task.getGets());
        mgcb.fetch();
      }
    } // end for
  }

  private void prepareConcurrentMultiGetTasks() {
    int batch_size = tsdb.config.mul_get_batch_size();
    int concurrency_number = tsdb.config.mul_get_concurrency_number();

    mulget_wait_cnt = 0;
    prepare_multi_get_start_time = System.currentTimeMillis();

    // prepare the tagvs combinations and base time list
    List<byte[][]> tagv_compinations = prepareAllTagvCompounds();
    List<Long> row_base_time_list = null;
    if (RollupQuery.isValidQuery(rollup_query)) {
      row_base_time_list = prepareRowBaseTimesRollup();
    } else {
      row_base_time_list = prepareRowBaseTimesNotRollup();
    }
    
    int next_concurrency_index = 0;
    List<GetRequest> gets_to_prepare = new ArrayList<GetRequest>(batch_size);
    Set<byte[]> tsuids = new TreeSet<byte[]>(new TSUIDComparator());
    ByteMap<List<GetRequest>> all_tsuids_gets = prepareGets(tagv_compinations, row_base_time_list);
    
    for (Map.Entry<byte[], List<GetRequest>> gets_entry : all_tsuids_gets) {
      byte[] tsuid = gets_entry.getKey();
      List<GetRequest> gets = gets_entry.getValue();

      for (int slice_offset = 0; slice_offset < gets.size();) {
        int cur_sz = gets_to_prepare.size();
        int need_sz = batch_size - cur_sz;
        int left_sz = gets.size() - slice_offset;
        int slice_sz = (left_sz > need_sz ? need_sz : left_sz);
        gets_to_prepare.addAll(gets.subList(slice_offset, slice_offset + slice_sz));
        tsuids.add(tsuid);

        // move the offset
        slice_offset += slice_sz;

        // a new task is ready, add it to next concurrency task list
        if (gets_to_prepare.size() == batch_size) {
          MulgetTask task = new MulgetTask(tsuids, gets_to_prepare);
          List<MulgetTask> mulget_task_list = mul_get_tasks.get((next_concurrency_index++) % concurrency_number);
          mulget_task_list.add(task);
          ++mulget_wait_cnt;

          // prepare a new task list and tsuids
          gets_to_prepare = new ArrayList<GetRequest>(batch_size);
          tsuids = new TreeSet<byte[]>(new TSUIDComparator());
        } // end if
      } // end for (int slice_offset)
    } // end for (Map.Entry)

    
    // add the uncompleted one
    if (gets_to_prepare.size() > 0) {
      MulgetTask task = new MulgetTask(tsuids, gets_to_prepare);
      List<MulgetTask> mulget_task_list = mul_get_tasks.get((next_concurrency_index++) % concurrency_number);
      mulget_task_list.add(task);
      ++mulget_wait_cnt;
    }

    prepare_multi_get_end_time = System.currentTimeMillis();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Finished preparing concurrency multi get task with " + mulget_wait_cnt + " tasks using "
          + (prepare_multi_get_end_time - prepare_multi_get_start_time) + "ms");
    }
  }
  
  private void validateMultigetData(List<KeyValue> kvs, 
                                    Map<byte[], List<Annotation>> annotations) {
    int tasks = finished_mulget_cnt.incrementAndGet();

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
  }
  
  private boolean checkAllFinishAndTriggerCallback() {
    if (this.mulget_wait_cnt == finished_mulget_cnt.get()) {
      try {
        mergeAndReturnResults();
      } catch (Exception ex) {
        LOG.error("Failed merging and returning results, " + "calling back with exception", ex);
        
        this.results.callback(ex);
      }
      
      return true;
    } else {
      return false;
    }
  }

  private void mergeAndReturnResults() {
    final long hbase_time = DateTime.currentTimeMillis();
    TsdbQuery.scanlatency.add((int) (hbase_time - this.fetch_start_time));
    if (LOG.isDebugEnabled()) {
      LOG.debug("Finished to fetch data for metric: " + Bytes.pretty(this.metric)
        + " using " + (hbase_time - this.fetch_start_time) + "ms");
    }

    final long merge_start = DateTime.nanoTime();

    mergeDataPoints();

    if (LOG.isDebugEnabled()) {
      LOG.debug("It took " + (DateTime.currentTimeMillis() - hbase_time) + " ms, "
          + " to merge and sort the rows into a tree map");
    }

    if (query_stats != null) {
      query_stats.addStat(query_index, QueryStat.SCANNER_MERGE_TIME, (DateTime.nanoTime() - merge_start));
    }

    results.callback(this.spans);
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
          datapoints = RollupQuery.isValidQuery(rollup_query) ? new RollupSpan(tsdb, this.rollup_query)
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

  protected ByteMap<List<GetRequest>> prepareGets(final List<byte[][]> tagv_compounds,
      final List<Long> row_base_time_list) {
    ByteMap<List<byte[]>> tsuids_rows = prepareTsuidRowKeys(tags, tagv_compounds, row_base_time_list);
    ByteMap<List<GetRequest>> tsuids_gets = new ByteMap<List<GetRequest>>();
    for (Map.Entry<byte[], List<byte[]>> tsuid_rows : tsuids_rows) {
      byte[] tsuid = tsuid_rows.getKey();
      List<byte[]> rows = tsuid_rows.getValue();
      List<GetRequest> rows_gets = new ArrayList<GetRequest>();

      for (byte[] row : rows) {
        GetRequest get = new GetRequest(this.table_to_fetch, row, TSDB.FAMILY);
        rows_gets.add(get);
      } // end for

      tsuids_gets.put(tsuid, rows_gets);
    } // end for

    return tsuids_gets;
  }
  
  private List<Long> prepareRowBaseTimesNotRollup() {
    ArrayList<Long> row_base_time_list = new ArrayList<Long>();
    for (long row_base_time = start_row_time; row_base_time <= end_row_time; row_base_time += Const.MAX_TIMESPAN) {
      row_base_time_list.add(row_base_time - row_base_time % Const.MAX_TIMESPAN);
    } // end for
    
    return row_base_time_list;
  }
  
  private List<Long> prepareRowBaseTimesRollup() {
    RollupInterval interval = rollup_query.getRollupInterval();
    List<Long> rows_base_times = new ArrayList<Long>();
    
    if (interval.getUnits() == 'h') {
      int modulo = Const.MAX_TIMESPAN;
      if (interval.getUnitMultiplier() > 1) {
        modulo = interval.getUnitMultiplier() * 60 * 60;
      }
      
      for (long row_base_time = this.start_row_time; row_base_time <= this.end_row_time; row_base_time += modulo) {
        rows_base_times.add(row_base_time - row_base_time % modulo);
      } // end for
    } else {
      Calendar pre_calendar = Calendar.getInstance(Const.UTC_TZ);
      pre_calendar.setTimeInMillis(this.start_row_time * 1000);
      rows_base_times.add(this.start_row_time);
      
      while(true) {
        final Calendar calendar = Calendar.getInstance(Const.UTC_TZ);
        calendar.setTimeInMillis(pre_calendar.getTimeInMillis());
        
        // zero out the hour, minutes, seconds
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        
        switch (interval.getUnits()) {
        case 'd':
          int day_of_month = pre_calendar.get(Calendar.DAY_OF_MONTH);
          calendar.set(Calendar.DAY_OF_MONTH, ++day_of_month);
          break;
        case 'm':
          calendar.set(Calendar.DAY_OF_MONTH, 1);
          int month = pre_calendar.get(Calendar.MONTH);
          calendar.set(Calendar.MONTH, ++month);
          break;
        case 'y':
          calendar.set(Calendar.DAY_OF_MONTH, 1);
          calendar.set(Calendar.MONTH, 0); // 0 for January
          int year = pre_calendar.get(Calendar.YEAR);
          calendar.set(Calendar.YEAR, ++year);
          break;
        default:
          throw new IllegalArgumentException("Unrecogznied span: " + interval);
        }
        
        long base_time = (long)(calendar.getTimeInMillis() / 1000);
        if (base_time <= this.end_row_time) {
          rows_base_times.add(base_time);
          
          // current calendar becomes the baseline for next one
          pre_calendar = calendar;
        } else {
          break;
        }
      } // end while
    }
    
    return rows_base_times;
  }

  /**
   * We have multiple tagks and each tagk may has multiple possible values. 
   * This routine generates all the possible tagv compounds basing on the
   * presence sequence of the tagks. Each compound will be used to generate 
   * the final tsuid.
   * 
   * @return a list contains all the possible compounds 
   */
  private List<byte[][]> prepareAllTagvCompounds() {
    List<byte[][]> pre_phase_tags = new LinkedList<byte[][]>();
    pre_phase_tags.add(new byte[tags.size()][tsdb.tag_values.width()]);

    List<byte[][]> next_phase_tags = new LinkedList<byte[][]>();
    int next_append_index = 0;

    for (Map.Entry<byte[], byte[][]> tag : tags) {
      byte[][] tagv = tag.getValue();

      for (int i = 0; i < tagv.length; ++i) {
        for (byte[][] pre_phase_tag : pre_phase_tags) {
          byte[][] next_phase_tag = new byte[tags.size()][tsdb.tag_values.width()];

          // copy the tagv from index 0 ~ next_append_index - 1
          for (int k = 0; k < next_append_index; ++k) { 
            System.arraycopy(pre_phase_tag[k], 0, next_phase_tag[k], 0, tsdb.tag_values.width());
          }

          // copy the tagv in next_append_index
          System.arraycopy(tagv[i], 0, next_phase_tag[next_append_index], 0, tsdb.tag_values.width());
          next_phase_tags.add(next_phase_tag);
        }
      } // end for

      ++next_append_index;
      pre_phase_tags = next_phase_tags;
      next_phase_tags = new LinkedList<byte[][]>();
    } // end for
    return pre_phase_tags;
  }

  private ByteMap<List<byte[]>> prepareTsuidRowKeys(final ByteMap<byte[][]> tags,
      final List<byte[][]> tagv_compounds, final List<Long> base_time_list) {

    int row_size = (Const.SALT_WIDTH() + tsdb.metrics.width() + Const.TIMESTAMP_BYTES
        + tsdb.tag_names.width() * tags.size() + tsdb.tag_values.width() * tags.size());

    ByteMap<List<byte[]>> tsuid_rows = new ByteMap<List<byte[]>>();
    for (byte[][] tagvs : tagv_compounds) {
      byte[] tsuid = new byte[tsdb.metrics.width() + tags.size() * tsdb.tag_names.width()
          + tags.size() * tsdb.tag_values.width()];
      List<byte[]> rows = new ArrayList<byte[]>();
      
      for (Long row_base_time : base_time_list) {
        byte[] row_key = new byte[row_size];
        // salt will prefix basing the hash value of the other part

        // metric
        System.arraycopy(metric, 0, row_key, Const.SALT_WIDTH(), tsdb.metrics.width());
        System.arraycopy(metric, 0, tsuid, 0, tsdb.metrics.width());

        // base time
        Internal.setBaseTime(row_key, row_base_time.intValue());
        
        // copy tagks and tagvs to the row key
        int tag_index = 0;
        int row_key_copy_offset = Const.SALT_WIDTH() + tsdb.metrics.width() + Const.TIMESTAMP_BYTES;
        int tsuid_copy_offset = tsdb.metrics.width();
        for (Map.Entry<byte[], byte[][]> tag : tags) {
          // tagk
          byte[] tagk = tag.getKey();
          System.arraycopy(tagk, 0, row_key, row_key_copy_offset, tsdb.tag_names.width());
          System.arraycopy(tagk, 0, tsuid, tsuid_copy_offset, tsdb.tag_names.width());
          row_key_copy_offset += tsdb.tag_names.width();
          tsuid_copy_offset += tsdb.tag_names.width();

          // tagv
          System.arraycopy(tagvs[tag_index], 0, row_key, row_key_copy_offset, tsdb.tag_values.width());
          System.arraycopy(tagvs[tag_index], 0, tsuid, tsuid_copy_offset, tsdb.tag_values.width());
          row_key_copy_offset += tsdb.tag_values.width();
          tsuid_copy_offset += tsdb.tag_values.width();

          // move to the next tag
          ++tag_index;
        }
        
        // salt
        RowKey.prefixKeyWithSalt(row_key);
        
        rows.add(row_key);
      } // end for
      
      tsuid_rows.put(tsuid, rows);
    } // end for

    return tsuid_rows;
  }
}
