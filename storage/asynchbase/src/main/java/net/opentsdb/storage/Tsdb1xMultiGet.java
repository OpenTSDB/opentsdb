// This file is part of OpenTSDB.
// Copyright (C) 2016-2018  The OpenTSDB Authors.
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

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.hbase.async.BinaryPrefixComparator;
import org.hbase.async.CompareFilter;
import org.hbase.async.FilterList;
import org.hbase.async.GetRequest;
import org.hbase.async.GetResultOrException;
import org.hbase.async.QualifierFilter;
import org.hbase.async.ScanFilter;
import org.hbase.async.FilterList.Operator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.configuration.Configuration;
import net.opentsdb.core.Const;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.Op;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.pojo.Downsampler;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.rollup.RollupInterval;
import net.opentsdb.rollup.RollupUtils;
import net.opentsdb.rollup.RollupUtils.RollupUsage;
import net.opentsdb.stats.Span;
import net.opentsdb.storage.schemas.tsdb1x.Schema;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.DateTime;

/**
 * Class that handles fetching TSDB data from storage using GetRequests 
 * instead of scanning for the data. This is only applicable if the 
 * query is using the "explicit_tags" feature and specified literal 
 * filters for exact matching or if the TSUIDs were resolved via a query
 * to meta-data.
 * It also works best when used for data that has high cardinality but 
 * the query is fetching a small subset of that data.
 * <p>
 * Note that this class is designed to asynchronously fire off 
 * {@link #concurrency_multi_get} number of get requests of size
 * {@link #batch_size}. Before issuing queries against HBase we sort the
 * TSUIDs so that we have a greater chance of sending a batch of requests
 * to the same region and region server. This isn't perfect though
 * unless we (TODO) interface with the region list.
 * <p>
 * The class also supports fallback when querying rollup data. If the 
 * first pass at a table yields no data, another run will be executed
 * against the next table and so on until at least one time series is
 * discovered.
 * <p>
 * Note that there is a lot of synchronization in this class. Only one
 * query can be outstanding at a time (e.g. calls to 
 * {@link #fetchNext(Tsdb1xQueryResult, Span)}) but there can be many
 * batches executing at any time, potentially in different threads with
 * different region servers.
 * <p>
 * TODO - room for optimization here. E.g. we expect a decent number of
 * TSUIDs. If it's small then we'd potentially have only one or two 
 * GetRequests per batch.
 * 
 * @since 2.4
 */
public class Tsdb1xMultiGet implements HBaseExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(Tsdb1xMultiGet.class);
  
  /** The upstream query node that owns this scanner set. */
  protected final Tsdb1xQueryNode node;
  
  /** The single metric query from the node. */
  protected final TimeSeriesQuery query;
  
  /** The list of TSUIDs to work one. */
  protected final List<byte[]> tsuids;
  
  /** The rollup group by aggregation name. */
  protected final String rollup_group_by;
  
  /** The rollup downsampling aggregation by name. */
  protected final String rollup_aggregation;
  
  /** The number of get requests batches allowed to be outstanding at
   * any given time. */
  protected final int concurrency_multi_get;
  
  /** Whether or not we're fetching rows in reverse. */
  protected final boolean reversed;
  
  /** The number of get requests in each batch. */
  protected final int batch_size;
  
  /** A rollup column filter when fetching rollup data. */
  protected final ScanFilter filter;
  
  /** Whether or not rollups were enabled for this query. */
  protected final boolean rollups_enabled;
  
  /** The list of rollup/pre-agg and raw tables we need to query. */
  protected final List<byte[]> tables;
  
  /** Search the query on pre-aggregated table directly instead of post fetch 
   * aggregation. */
  protected final boolean pre_aggregate;

  /** The current index in the TSUID list we're working on. */
  protected volatile int tsuid_idx;
  
  /** The current timestamp for the lowest resolution data we're querying. */
  protected volatile TimeStamp timestamp;
  
  /** The current fallback timestamp for the next highest resolution data
   * we're querying when falling back. May be null. */
  protected volatile TimeStamp fallback_timestamp;
  
  /** Index into the {@link Tsdb1xQueryNode#rollupIntervals()} that we're
   * working on. -1 means we're query raw only, 0 or more means we're
   * working with rollups. A value >= the rollup intervals size means we've
   * fallen back to the raw table.
   */
  protected volatile int rollup_index;
  
  /** The number of outstanding batches inflight. */
  protected volatile int outstanding;
  
  /** Whether or not the query has failed, i.e. a batch returned an
   * exception. */
  protected volatile boolean has_failed;
  
  /** The current result to write data into. */
  protected volatile Tsdb1xQueryResult current_result;
  
  /** An optional tracing child. */
  protected volatile Span child;
  
  /** The state of this executor. */
  protected volatile State state;
  
  /**
   * Default ctor that parses the query, sets up rollups and sorts (and
   * optionally salts) the TSUIDs.
   * @param node A non-null query node that owns this getter.
   * @param query A non-null query to parse.
   * @param tsuids A non-null and non-empty list of TSUIDs.
   * @throws IllegalArgumentException if the params were null or empty.
   */
  public Tsdb1xMultiGet(final Tsdb1xQueryNode node, 
                  final TimeSeriesQuery query, 
                  final List<byte[]> tsuids) {
    if (node == null) {
      throw new IllegalArgumentException("Node cannot be null.");
    }
    if (query == null) {
      throw new IllegalArgumentException("Query cannot be null.");
    }
    if (tsuids == null || tsuids.isEmpty()) {
      throw new IllegalArgumentException("TSUIDs cannot be empty.");
    }
    this.node = node;
    this.query = query;
    
    // prepare with salt if needed and sort
    if (node.schema().saltWidth() > 0) {
      if (node.schema().timelessSalting()) {
        // time is not incorporated in the salt (OSS release) so we can
        // prepare the row keys with an empty space for the timestamp and
        // sort them by salts.
        this.tsuids = Lists.newArrayListWithCapacity(tsuids.size());
        for (final byte[] tsuid : tsuids) {
          final byte[] row = new byte[node.schema().saltWidth() + 
                                      Schema.TIMESTAMP_BYTES + 
                                      tsuid.length];
          System.arraycopy(tsuid, 0, row, node.schema().saltWidth(), 
              node.schema().metricWidth());
          System.arraycopy(tsuid, node.schema().metricWidth(), row, 
              node.schema().saltWidth() + node.schema().metricWidth() + 
                Schema.TIMESTAMP_BYTES, 
              tsuid.length - node.schema().metricWidth());
          node.schema().prefixKeyWithSalt(row);
          this.tsuids.add(row);
        }
        Collections.sort(this.tsuids, Bytes.MEMCMP);
      } else {
        // TODO - Maybe there is something we can do to optimize this
        // without wasting an immense amount of space but I dunno.
        this.tsuids = tsuids;
        Collections.sort(tsuids, Bytes.MEMCMP);
      }
    } else {
      this.tsuids = tsuids;
      Collections.sort(tsuids, Bytes.MEMCMP);
    }
    
    final Configuration config = node.parent()
        .tsdb().getConfig();
    
    if (query.hasKey(Tsdb1xHBaseDataStore.MULTI_GET_CONCURRENT_KEY)) {
      concurrency_multi_get = query.getInt(config, 
          Tsdb1xHBaseDataStore.MULTI_GET_CONCURRENT_KEY);
    } else {
      concurrency_multi_get = node.parent()
          .dynamicInt(Tsdb1xHBaseDataStore.MULTI_GET_CONCURRENT_KEY);
    }
    if (query.hasKey(Tsdb1xHBaseDataStore.MULTI_GET_BATCH_KEY)) {
      batch_size = query.getInt(config, 
          Tsdb1xHBaseDataStore.MULTI_GET_BATCH_KEY);
    } else {
      batch_size = node.parent()
          .dynamicInt(Tsdb1xHBaseDataStore.MULTI_GET_BATCH_KEY);
    }
    if (query.hasKey(Schema.QUERY_REVERSE_KEY)) {
      reversed = query.getBoolean(config, 
          Schema.QUERY_REVERSE_KEY);
    } else {
      reversed = node.parent()
          .dynamicBoolean(Schema.QUERY_REVERSE_KEY);
    }
    if (query.hasKey(Tsdb1xHBaseDataStore.PRE_AGG_KEY)) {
      pre_aggregate = query.getBoolean(config, 
          Tsdb1xHBaseDataStore.PRE_AGG_KEY);
    } else {
      pre_aggregate = false;
    }
    
    if (node.schema().rollupConfig() != null && 
        node.rollupUsage() != RollupUsage.ROLLUP_RAW) {
      Downsampler ds = query.getMetrics().get(0).getDownsampler();
      if (ds == null) {
        ds = query.getTime().getDownsampler();
      }
      
      if (ds != null) {
        rollup_aggregation = ds.getAggregator();
      } else {
        rollup_aggregation = null;
      }
    } else {
      rollup_aggregation = null;
    }
    
    if (Strings.isNullOrEmpty(query.getMetrics().get(0).getAggregator()) ) {
      rollup_group_by = query.getTime().getAggregator();
    } else {
      rollup_group_by = query.getMetrics().get(0).getAggregator();
    }
    
    if (node.rollupIntervals() != null && 
        !node.rollupIntervals().isEmpty() && 
        node.rollupUsage() != RollupUsage.ROLLUP_RAW) {
      rollups_enabled = true;
      rollup_index = 0;
      if (rollup_group_by != null && rollup_group_by.equals("avg")) {
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
        filter = new FilterList(filters, Operator.MUST_PASS_ONE);
      } else {
        // it's another aggregation
        final List<ScanFilter> filters = Lists.newArrayListWithCapacity(2);
        filters.add(new QualifierFilter(CompareFilter.CompareOp.EQUAL,
            new BinaryPrefixComparator(rollup_group_by
                .getBytes(Const.ASCII_CHARSET))));
        filters.add(new QualifierFilter(CompareFilter.CompareOp.EQUAL,
            new BinaryPrefixComparator(new byte[] { 
                (byte) node.schema().rollupConfig().getIdForAggregator(rollup_group_by)
            })));
        filter = new FilterList(filters, Operator.MUST_PASS_ONE);
      }
    } else {
      rollup_index = -1;
      rollups_enabled = false;
      filter = null;
    }
    
    // sentinel
    tsuid_idx = -1;
    timestamp = getInitialTimestamp(rollup_index);
    
    if (rollups_enabled) {
      tables = Lists.newArrayListWithCapacity(node.rollupIntervals().size() + 1);
      for (final RollupInterval interval : node.rollupIntervals()) {
        if (pre_aggregate) {
          tables.add(interval.getGroupbyTable());
        } else {
          tables.add(interval.getTemporalTable());
        }
      }
      tables.add(node.parent().dataTable());
    } else {
      tables = Lists.newArrayList(node.parent().dataTable());
    }
    state = State.CONTINUE;
  }
  
  @Override
  public synchronized void fetchNext(final Tsdb1xQueryResult result, 
                                     final Span span) {
    if (result == null) {
      throw new IllegalArgumentException("Result cannot be null.");
    }
    if (current_result != null) {
      throw new IllegalStateException("Result cannot be set!");
    }
    current_result = result;
    if (span != null && span.isDebug()) {
      child = span.newChild(getClass().getName() + ".fetchNext")
                  .start();
    }
    
    while (outstanding < concurrency_multi_get && !advance() && !has_failed) {
      outstanding++;
      nextBatch(tsuid_idx, (int) timestamp.epoch(), child);
    }
    
    // see if we're all done
    if (outstanding == 0 && current_result != null && !has_failed) {
      final Tsdb1xQueryResult temp = current_result;
      current_result = null;
      node.onNext(temp);
    }
  }
  
  @Override
  public void close() {
    // no-op
  }
  
  @Override
  public State state() {
    return state;
  }
  
  /**
   * Increments the timestamp(s) and/or {@link #tsuid_idx}.
   * @return True if there is more data to fetch, false if we should 
   * stop due to either reaching the end of the query or the end of
   * the segment.
   */
  @VisibleForTesting
  boolean advance() {
    TimeStamp ts;
    if (rollups_enabled && rollup_index > 0) {
      ts = fallback_timestamp;
    } else {
      // raw
      ts = timestamp;
    }
    
    if (node.sequenceEnd() != null) {
      if (ts != null && ts.compare((reversed ? Op.LT : Op.GT), 
          node.sequenceEnd())) {
        tsuid_idx = -1;
        // DONE with segment
        return true;
      }
    }
    if (ts != null && (reversed ? 
        ts.compare(Op.LT, query.getTime().startTime()) : 
        ts.compare(Op.GT, query.getTime().endTime()))) {
      // DONE with query!
      return true;
    }
    
    if (tsuid_idx >= 0 && tsuid_idx + batch_size >= tsuids.size()) {
      tsuid_idx = 0;
      
      incrementTimestamp();
      if (ts == null) {
        ts = fallback_timestamp;
      }
      
      if (node.sequenceEnd() != null) {
        if (ts.compare((reversed ? Op.LT : Op.GT), 
            node.sequenceEnd())) {
          tsuid_idx = -1;
          // DONE with segment
          return true;
        }
      }
      if (reversed ? 
          ts.compare(Op.LT, query.getTime().startTime()) : 
          ts.compare(Op.GT, query.getTime().endTime())) {
        // DONE with query!
        return true;
      }
    } else if (tsuid_idx >= 0) {
      tsuid_idx += batch_size;
    } else {
      tsuid_idx = 0;
    }
    
    return false;
  }
  
  /**
   * Called when a batch encountered or threw an exception.
   * @param t A non-null exception.
   */
  @VisibleForTesting
  synchronized void onError(final Throwable t) {
    if (!has_failed) {
      has_failed = true;
      if (child != null) {
        child.setErrorTags(t)
             .finish();
      }
      state = State.EXCEPTION;
      node.onError(t);
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Exception from followup get", t);
      }
    }
  }
  
  /**
   * Called when a batch has completed successfully (possibly empty).
   * This method may start another batch or send the results upstream. It
   * also handles fallback to higher resolution data.
   */
  @VisibleForTesting
  synchronized void onComplete() {
    if (has_failed || current_result == null) {
      // nothing to do if we already failed or called upstream.
      return;
    }
    
    if (current_result.isFull()) {
      if (outstanding <= 0) {
        final QueryResult result = current_result;
        current_result = null;
        if (child != null) {
          child.setSuccessTags().finish();
        }
        state = State.COMPLETE;
        node.onNext(result);
      }
      
      // if we sent upstream, good. If not then we need to wait for the
      // remaining runs to complete
      return;
    }
    
    // if there are more things to run then do it
    if (outstanding < concurrency_multi_get && !advance()) {
      final int ts;
      if (rollups_enabled) {
        ts = (int) fallback_timestamp.epoch();
      } else {
        ts = (int) timestamp.epoch();
      }
      final int idx = tsuid_idx;
      outstanding++;
      nextBatch(idx, ts, child);
      return;
    }
    
    if (outstanding <= 0) {
      // we're done. Possibly....
      if ((current_result.timeSeries() == null || 
          current_result.timeSeries().isEmpty()) && 
          rollups_enabled && node.rollupUsage() != RollupUsage.ROLLUP_NOFALLBACK) {
        // we can fallback!
        tsuid_idx = 0;
        fallback_timestamp = null;
        if (node.rollupUsage() == RollupUsage.ROLLUP_FALLBACK_RAW) {
          rollup_index = node.rollupIntervals().size();
        } else {
          rollup_index++;
        }
        while (outstanding < concurrency_multi_get && !advance() && !has_failed) {
          outstanding++;
          nextBatch(tsuid_idx, (int) fallback_timestamp.epoch(), child);
        }
      } else {
        // no fallback, we're done.
        final QueryResult result = current_result;
        current_result = null;
        if (child != null) {
          child.setSuccessTags().finish();
        }
        state = State.COMPLETE;
        node.onNext(result);
        return;
      }
    }
    
    // otherwise don't fire another one off till more have completed.
  }

  /**
   * A callback attached to the multi-gets that parses the results and
   * stores them in the current results. Calls {@link MultiGet#onComplete()}.
   * <p>
   * While it would be nice to attach a tracer here, we can avoid a lot
   * of object overhead by using a singleton here.
   */
  class ResponseCB implements Callback<Void, List<GetResultOrException>> {
    final Span span;
    
    ResponseCB(final Span span) {
      this.span = span;
    }
    
    @Override
    public Void call(final List<GetResultOrException> results)
        throws Exception {
      synchronized (Tsdb1xMultiGet.this) {
        outstanding--;
      }
      if (has_failed) {
        return null;
      }
      
      for (final GetResultOrException result : results) {
        if (result.getException() != null) {
          if (span != null) {
            span.setErrorTags().finish();
          }
          onError(result.getException());
          return null;
        }
        
        if (result.getCells() == null || result.getCells().isEmpty()) {
          continue;
        }
        
        if (current_result != null) {
          current_result.decode(result.getCells(), 
              (rollup_index < 0 || 
               rollup_index >= node.rollupIntervals().size() 
                 ? null : node.rollupIntervals().get(rollup_index)));
        }
      }
      
      if (span != null) {
        span.setSuccessTags().finish();
      }
      onComplete();
      return null;
    }
  }
  final ResponseCB response_cb = new ResponseCB(null);
  
  /**
   * A callback attached to the multi-gets to catch exceptions and call
   * {@link Tsdb1xMultiGet#onError(Throwable)}.
   * <p>
   * While it would be nice to attach a tracer here, we can avoid a lot
   * of object overhead by using a singleton here.
   */
  class ErrorCB implements Callback<Object, Exception> {
    final Span span;
    
    ErrorCB(final Span span) {
      this.span = span;
    }
    
    @Override
    public Object call(final Exception ex) throws Exception {
      synchronized (Tsdb1xMultiGet.this) {
        outstanding--;
      }
      
      if (span != null) {
        span.setErrorTags(ex).finish();
      }
      onError(ex);
      return null;
    }
  }
  final ErrorCB error_cb = new ErrorCB(null);
  
  /**
   * Creates a batch of {@link GetRequest}s and sends them to the HBase
   * client.
   * @param tsuid_idx The TSUID index to start at.
   * @param timestamp The timestamp for each row key.
   */
  @VisibleForTesting
  void nextBatch(final int tsuid_idx, 
                 final int timestamp, 
                 final Span span) {
    final Span child;
    if (span != null) {
      child = span.newChild(getClass() + "nextBatch()")
          .withTag("batchSize", batch_size)
          .withTag("startTsuidIdx", tsuid_idx)
          .withTag("startTimestamp", timestamp)
          .start();
    } else {
      child = null;
    }
    final List<GetRequest> requests = Lists.newArrayListWithCapacity(batch_size);
    final byte[] table = rollups_enabled ? tables.get(rollup_index) : tables.get(0);
    
    // TODO - it would be extra nice to know the region splits so we 
    // can batch better. In this case we may have some split between regions.
    for (int i = tsuid_idx; i < tsuid_idx + batch_size && i < tsuids.size(); i++) {
      final byte[] tsuid = tsuids.get(i);
      
      final byte[] key;
      if (node.schema().saltWidth() > 0 && node.schema().timelessSalting()) {
        key = Arrays.copyOf(tsuid, tsuid.length);
        node.schema().setBaseTime(key, timestamp);
      } else {
        key = new byte[tsuid.length + node.schema().saltWidth() + 
                                      Schema.TIMESTAMP_BYTES];
        
        System.arraycopy(tsuid, 0, key, node.schema().saltWidth(), 
            node.schema().metricWidth());
        System.arraycopy(Bytes.fromInt((int) timestamp), 0, key, 
            node.schema().saltWidth() + node.schema().metricWidth(), 
              Schema.TIMESTAMP_BYTES);
        System.arraycopy(tsuid, node.schema().metricWidth(), key, 
            node.schema().saltWidth() + node.schema().metricWidth() + 
              Schema.TIMESTAMP_BYTES, 
            tsuid.length - node.schema().metricWidth());
        node.schema().prefixKeyWithSalt(key);
      }
      
      final GetRequest request = new GetRequest(table, key, 
          Tsdb1xHBaseDataStore.DATA_FAMILY);
      if (rollup_index > -1 && 
          rollup_index < node.rollupIntervals().size() && 
          filter != null) {
        request.setFilter(filter);
      }
      requests.add(request);
    }
    
    try {
      final Deferred<List<GetResultOrException>> deferred = 
          node.parent().client().get(requests);
      if (child == null || !child.isDebug()) {
        deferred.addCallback(response_cb)
                .addErrback(error_cb);
      } else {
        deferred.addCallback(new ResponseCB(child))
        .addErrback(new ErrorCB(child));
      }
    } catch (Exception e) {
      LOG.error("Unexpected exception", e);
      onError(e);
    }
  }
  
  /**
   * Increments the main timestamp or the fallback timestamp depending
   * on the {@link #rollup_index}.
   */
  @VisibleForTesting
  void incrementTimestamp() {
    if (rollups_enabled) {
      if (rollup_index == 0) {
        final RollupInterval interval = 
            node.rollupIntervals().get(rollup_index);
        if (reversed) {
          timestamp.updateEpoch((long) RollupUtils.getRollupBasetime(
              timestamp.epoch() - 
              (interval.getIntervalSeconds() * interval.getIntervals()), interval));
        } else {
          timestamp.updateEpoch((long) RollupUtils.getRollupBasetime(
              timestamp.epoch() + 
              (interval.getIntervalSeconds() * interval.getIntervals()), interval));
        }
      } else {
        if (fallback_timestamp == null) {
          // initialize the timestamp
          fallback_timestamp = getInitialTimestamp(rollup_index);
        } else if (rollup_index >= node.rollupIntervals().size()) {
          // it's raw
          fallback_timestamp.add(Duration.of(
              (reversed ? - 1 : 1), ChronoUnit.HOURS));
        } else {
          final RollupInterval interval = 
              node.rollupIntervals().get(rollup_index);
          if (reversed) {
            fallback_timestamp.updateEpoch((long) RollupUtils.getRollupBasetime(
                fallback_timestamp.epoch() - 
                (interval.getIntervalSeconds() * interval.getIntervals()), interval));
          } else {
            fallback_timestamp.updateEpoch((long) RollupUtils.getRollupBasetime(
                fallback_timestamp.epoch() + 
                (interval.getIntervalSeconds() * interval.getIntervals()), interval));
          }
        }
      }
    } else {
      timestamp.add(Duration.of(
          (reversed ? -1 : 1), ChronoUnit.HOURS));
    }
  }
  
  /**
   * Generates the initial timestamp based on the query and aligned to
   * either the raw table or a rollup table.
   * @param rollup_index An index determining if we work on the raw or
   * rollup table.
   * @return The base timestamp.
   */
  @VisibleForTesting
  TimeStamp getInitialTimestamp(final int rollup_index) {
    if (rollups_enabled && rollup_index >= 0 && 
        rollup_index < node.rollupIntervals().size()) {
      final RollupInterval interval = node.rollupIntervals().get(0);
      if (query.getTime().isRate()) {
        return new MillisecondTimeStamp((long) RollupUtils.getRollupBasetime(
            (reversed ? query.getTime().endTime().epoch() + 1 : 
              query.getTime().startTime().epoch() - 1), interval) * 1000L);      
      } else {
        return new MillisecondTimeStamp((long) RollupUtils.getRollupBasetime(
            (reversed ? query.getTime().endTime().epoch() : 
              query.getTime().startTime().epoch()), interval) * 1000L);
      }
    } else {
      long ts = reversed ? query.getTime().endTime().epoch() : 
        query.getTime().startTime().epoch();
      if (query.getMetrics().get(0).getDownsampler() != null) {
        long interval = DateTime.parseDuration(
            query.getMetrics().get(0).getDownsampler().getInterval());
        if (interval > 0) {
          final long interval_offset = (1000L * ts) % interval;
          ts -= interval_offset / 1000L;
        }
      } else if (query.getTime().getDownsampler() != null) {
        long interval = DateTime.parseDuration(
            query.getTime().getDownsampler().getInterval());
        if (interval > 0) {
          final long interval_offset = (1000L * ts) % interval;
          ts -= interval_offset / 1000L;
        }
      }
      
      // Then snap that timestamp back to its representative value for the
      // timespan in which it appears.
      final long timespan_offset = ts % Schema.MAX_RAW_TIMESPAN;
      ts -= timespan_offset;
      
      // Don't return negative numbers.
      ts = ts > 0L ? ts : 0L;
      return new MillisecondTimeStamp(ts * 1000);
    }
  }
}
