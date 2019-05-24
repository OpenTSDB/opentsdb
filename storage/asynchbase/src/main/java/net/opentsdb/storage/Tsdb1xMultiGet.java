// This file is part of OpenTSDB.
// Copyright (C) 2016-2019  The OpenTSDB Authors.
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
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.hbase.async.BinaryPrefixComparator;
import org.hbase.async.CompareFilter;
import org.hbase.async.FilterList;
import org.hbase.async.GetRequest;
import org.hbase.async.GetResultOrException;
import org.hbase.async.KeyValue;
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

import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import net.openhft.hashing.LongHashFunction;
import net.opentsdb.configuration.Configuration;
import net.opentsdb.core.Const;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.Op;
import net.opentsdb.data.types.numeric.NumericByteArraySummaryType;
import net.opentsdb.data.types.numeric.NumericLongArrayType;
import net.opentsdb.pools.CloseablePooledObject;
import net.opentsdb.pools.PooledObject;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.processor.rate.Rate;
import net.opentsdb.rollup.RollupInterval;
import net.opentsdb.rollup.RollupUtils;
import net.opentsdb.rollup.RollupUtils.RollupUsage;
import net.opentsdb.stats.Span;
import net.opentsdb.storage.schemas.tsdb1x.Schema;
import net.opentsdb.storage.schemas.tsdb1x.TSUID;
import net.opentsdb.storage.schemas.tsdb1x.Tsdb1xPartialTimeSeries;
import net.opentsdb.storage.schemas.tsdb1x.Tsdb1xPartialTimeSeriesSet;
import net.opentsdb.storage.schemas.tsdb1x.Tsdb1xPartialTimeSeriesSetPool;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.Pair;

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
 * TODO - fix push for reverse.
 * TODO - fix segment start/end
 * 
 * @since 2.4
 */
public class Tsdb1xMultiGet implements HBaseExecutor, CloseablePooledObject {
  private static final Logger LOG = LoggerFactory.getLogger(Tsdb1xMultiGet.class);
  
  /** Reference to the Object pool for this instance. */
  protected PooledObject pooled_object;
  
  /** The upstream query node that owns this scanner set. */
  protected Tsdb1xHBaseQueryNode node;
  
  /** The query config from the node. */
  protected TimeSeriesDataSourceConfig source_config;
  
  /** The list of TSUIDs to work one. */
  protected List<byte[]> tsuids;
  
  /** The number of get requests batches allowed to be outstanding at
   * any given time. */
  protected int concurrency_multi_get;
  
  /** Whether or not we're fetching rows in reverse. */
  protected boolean reversed;
  
  /** The number of get requests in each batch. */
  protected int batch_size;
  
  /** A rollup column filter when fetching rollup data. */
  protected ScanFilter filter;
    
  /** The list of rollup/pre-agg and raw tables we need to query. */
  protected List<byte[]> tables;
  
  /** Search the query on pre-aggregated table directly instead of post fetch 
   * aggregation. */
  protected boolean pre_aggregate;

  /** The current index in the TSUID list we're working on. */
  protected volatile int tsuid_idx;
  
  /** The current timestamp for the lowest resolution data we're querying. */
  protected final TimeStamp timestamp;
  
  /** The timestamp with optional padding. */
  protected final TimeStamp end_timestamp;
  
  /** Index into the {@link Tsdb1xHBaseQueryNode#rollupIntervals()} that we're
   * working on. -1 means we're query raw only, 0 or more means we're
   * working with rollups. A value >= the rollup intervals size means we've
   * fallen back to the raw table.
   */
  protected volatile int rollup_index;
  
  /** The number of outstanding batches inflight. */
  protected final AtomicInteger outstanding;
  
  /** Whether or not the query has failed, i.e. a batch returned an
   * exception. */
  protected final AtomicBoolean has_failed;
  
  /** The current result to write data into. */
  protected volatile Tsdb1xQueryResult current_result;
  
  /** An optional tracing child. */
  protected volatile Span child;
  
  /** The state of this executor. */
  protected volatile State state;
  
  /** Tracking time. */
  protected long fetch_start;
  
  /** A flag to determine whether or not all of the batches were sent for this
   * request. */
  protected final AtomicBoolean all_batches_sent;
  
  /** The aligned start timestamp of the query, used for indices calculations. */
  protected volatile TimeStamp start_ts;
  
  /** The interval used for indices calculations. */
  protected volatile long interval;
  
  /** The array of sets. */
  protected volatile AtomicReferenceArray<Tsdb1xPartialTimeSeriesSet> sets;
  
  /** How many batches were sent per set. Used to determine if we've seen all
   * of the results for a set and whether or not we should fill. */
  protected volatile AtomicIntegerArray batches_per_set;
  
  /** How many batches have responded. */
  protected volatile AtomicIntegerArray finished_batches_per_set;
  
  /** A map of hashes to time series IDs for the sets. */
  protected volatile TLongObjectMap<TimeSeriesId> ts_ids;
  
  /**
   * Default ctor.
   */
  public Tsdb1xMultiGet() {
    start_ts = new SecondTimeStamp(-1);
    timestamp = new SecondTimeStamp(-1);
    end_timestamp = new SecondTimeStamp(-1);
    tables = Lists.newArrayList();
    has_failed = new AtomicBoolean();
    all_batches_sent = new AtomicBoolean();
    outstanding = new AtomicInteger();
    ts_ids = new TLongObjectHashMap<TimeSeriesId>();
  }
  
  /**
   * Reset that parses the query, sets up rollups and sorts (and optionally 
   * salts) the TSUIDs.
   * @param node A non-null query node that owns this getter.
   * @param source_config A non-null source config.
   * @param tsuids A non-null and non-empty list of TSUIDs.
   * @throws IllegalArgumentException if the params were null or empty.
   */
  public void reset(final Tsdb1xHBaseQueryNode node, 
                    final TimeSeriesDataSourceConfig source_config, 
                    final List<byte[]> tsuids) {
    if (node == null) {
      throw new IllegalArgumentException("Node cannot be null.");
    }
    if (source_config == null) {
      throw new IllegalArgumentException("Config cannot be null.");
    }
    if (tsuids == null || tsuids.isEmpty()) {
      throw new IllegalArgumentException("TSUIDs cannot be empty.");
    }
    this.node = node;
    this.source_config = source_config;
    
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
    
    if (source_config.hasKey(Tsdb1xHBaseDataStore.MULTI_GET_CONCURRENT_KEY)) {
      concurrency_multi_get = source_config.getInt(config, 
          Tsdb1xHBaseDataStore.MULTI_GET_CONCURRENT_KEY);
    } else {
      concurrency_multi_get = node.parent()
          .dynamicInt(Tsdb1xHBaseDataStore.MULTI_GET_CONCURRENT_KEY);
    }
    if (source_config.hasKey(Tsdb1xHBaseDataStore.MULTI_GET_BATCH_KEY)) {
      batch_size = source_config.getInt(config, 
          Tsdb1xHBaseDataStore.MULTI_GET_BATCH_KEY);
    } else {
      batch_size = node.parent()
          .dynamicInt(Tsdb1xHBaseDataStore.MULTI_GET_BATCH_KEY);
    }
    if (source_config.hasKey(Schema.QUERY_REVERSE_KEY)) {
      reversed = source_config.getBoolean(config, 
          Schema.QUERY_REVERSE_KEY);
    } else {
      reversed = node.parent()
          .dynamicBoolean(Schema.QUERY_REVERSE_KEY);
    }
    if (source_config.hasKey(Tsdb1xHBaseDataStore.PRE_AGG_KEY)) {
      pre_aggregate = source_config.getBoolean(config, 
          Tsdb1xHBaseDataStore.PRE_AGG_KEY);
    } else {
      pre_aggregate = false;
    }
    
    if (node.rollupIntervals() != null && 
        !node.rollupIntervals().isEmpty() && 
        node.rollupUsage() != RollupUsage.ROLLUP_RAW) {
      rollup_index = 0;
      
      final List<ScanFilter> filters = Lists.newArrayListWithCapacity(
          source_config.getSummaryAggregations().size());
      for (final String agg : source_config.getSummaryAggregations()) {
        filters.add(new QualifierFilter(CompareFilter.CompareOp.EQUAL,
            new BinaryPrefixComparator(
                agg.toLowerCase().getBytes(Const.ASCII_CHARSET))));
        filters.add(new QualifierFilter(CompareFilter.CompareOp.EQUAL,
            new BinaryPrefixComparator(new byte[] { 
                (byte) node.schema().rollupConfig().getIdForAggregator(
                    agg.toLowerCase())
            })));
      }
      filter = new FilterList(filters, Operator.MUST_PASS_ONE);
      if (node.pipelineContext().query().isTraceEnabled()) {
        node.pipelineContext().queryContext().logTrace(node, 
            "Enabling rollup queries with filter: " + filter);
      }
    } else {
      rollup_index = -1;
      filter = null;
    }
    
    has_failed.set(false);
    outstanding.set(0);
    tsuid_idx = 0;
    setInitialTimestamp();
    setEndTimestamps();
    
    if (node.push()) {
      setupSets();
    }
    
    if (rollup_index >= 0) {
      for (final RollupInterval interval : node.rollupIntervals()) {
        if (pre_aggregate) {
          tables.add(interval.getGroupbyTable());
        } else {
          tables.add(interval.getTemporalTable());
        }
      }
      tables.add(node.parent().dataTable());
    } else {
      tables.add(node.parent().dataTable());
    }
    state = State.CONTINUE;
  }
  
  @Override
  public void fetchNext(final Tsdb1xQueryResult result, 
                        final Span span) {
    if (fetch_start == 0) {
      fetch_start = DateTime.nanoTime();
    }
    if (!node.push() && result == null) {
      throw new IllegalArgumentException("Result cannot be null.");
    }
    if (!node.push() && current_result != null) {
      throw new IllegalStateException("Result cannot be set!");
    }
    current_result = result;
    if (span != null && span.isDebug()) {
      child = span.newChild(getClass().getName() + ".fetchNext")
                  .start();
    }
    
    if (node.pipelineContext().query().isTraceEnabled()) {
      node.pipelineContext().queryContext().logTrace(node, "Executing multiget query with " 
          + tsuids.size() + " TSUIDs and a concurrency of " + concurrency_multi_get);
    }
    
    while (outstanding.get() < concurrency_multi_get && 
           !all_batches_sent.get() && 
           !has_failed.get()) {
      nextBatch(child);
    }
  }
  
  @Override
  public void close() {
    node = null;
    source_config = null;
    tsuids = null;
    filter = null;
    tables.clear();
    current_result = null;
    timestamp.updateEpoch(-1);
    end_timestamp.updateEpoch(-1);
    all_batches_sent.set(false);
    has_failed.set(false);
    child = null;
    outstanding.set(0);
    tsuid_idx = -1;
    if (sets != null) {
      for (int i = 0; i < sets.length(); i++) {
        final Tsdb1xPartialTimeSeriesSet set = sets.get(i);
        if (set != null) {
          try {
            set.close();
          } catch (Exception e) {
            LOG.error("Failed to close out set", e);
          }
        }
      }
    }
    release();
  }
  
  @Override
  public State state() {
    return state;
  }
  
  @Override
  public Object object() {
    return this;
  }
  
  @Override
  public void setPooledObject(final PooledObject pooled_object) {
    this.pooled_object = pooled_object;
  }
  
  @Override
  public void release() {
    if (pooled_object != null) {
      pooled_object.release();
    }
  }
  
  /**
   * Called when a batch encountered or threw an exception.
   * @param t A non-null exception.
   */
  @VisibleForTesting
  void onError(final Throwable t) {
    if (has_failed.compareAndSet(false, true)) {
      if (child != null) {
        child.setErrorTags(t)
             .finish();
      }
      LOG.error("Unexpected exception", t);
      node.pipelineContext().queryContext().logError(node, 
          "Multiget query failed with exception: " + t.getMessage());
      state = State.EXCEPTION;
        if (!node.push()) {
        current_result.setException(t);
        final QueryResult result = current_result;
        current_result = null;
        outstanding.set(0);
        node.onNext(result);
      } else {
        node.onError(t);
      }
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Exception from follow up get", t);
      }
    }
  }
  
  /**
   * Called when a batch has completed successfully (possibly empty).
   * This method may start another batch or send the results upstream. It
   * also handles fallback to higher resolution data.
   */
  @VisibleForTesting
  void onComplete(final int index) {
    try {
      if (has_failed.get() || (!node.push() && current_result == null)) {
        // nothing to do if we already failed or called upstream.
        return;
      }
      
      final int outstanding = (index >= 0 || 
          (!node.push() && index == -1)) ? 
          this.outstanding.decrementAndGet() : 
            this.outstanding.get();
      if (!node.push() && current_result.isFull()) {
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
      if (outstanding < concurrency_multi_get && !all_batches_sent.get()) {
        nextBatch(child);
        
        // Make sure this happens AFTER we send another batch as we want to
        // avoid the race to inc and dec.
        if (node.push() && index >= 0) {
          if (batches_per_set.get(index) == 
              finished_batches_per_set.incrementAndGet(index)) {
            getSet(index).setCompleteAndEmpty(rollup_index < 0);
          }
        }
        return;
      }
      
      if (outstanding <= 0 && all_batches_sent.get()) {
        // if no data, see if we can fallback
        if (!haveData() && 
            rollup_index >= 0 && 
            node.rollupUsage() != RollupUsage.ROLLUP_NOFALLBACK) {
          fallback();
        } else if (!has_failed.get()) {
          // all done
          if (node.push()) {
            for (int i = 0; i < sets.length(); i++) {
              final Tsdb1xPartialTimeSeriesSet set = getSet(i);
              if (!set.complete()) {
                set.setCompleteAndEmpty(true);
              }
            }
            if (node.pipelineContext().query().isTraceEnabled()) {
              node.pipelineContext().queryContext().logTrace(node, 
                  "Finished multi-get query in: " 
                      + DateTime.msFromNanoDiff(DateTime.nanoTime(), fetch_start));
            }
            state = State.COMPLETE;
          } else {
            // no fallback, we're done.
            final QueryResult result = current_result;
            current_result = null;
            if (child != null) {
              child.setSuccessTags().finish();
            }
            if (node.pipelineContext().query().isTraceEnabled()) {
              node.pipelineContext().queryContext().logTrace(node, 
                  "Finished multi-get query in: " 
                      + DateTime.msFromNanoDiff(DateTime.nanoTime(), fetch_start));
            }
            state = State.COMPLETE;
            node.onNext(result);
          }
        }
      }
      // otherwise don't fire another one off till more have completed.
    } catch (Throwable t) {
      LOG.error("Unexpected exception completing multiget", t);
      onError(t);
    }
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
    final int index;
    
    ResponseCB(final int index, final Span span) {
      this.index = index;
      this.span = span;
    }
    
    @Override
    public Void call(final List<GetResultOrException> results)
        throws Exception {
      if (has_failed.get()) {
        return null;
      }
      
      TimeStamp base_ts = new SecondTimeStamp(0);
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
        
        if (node.push()) {
          if (base_ts.epoch() == 0) {
            node.schema().baseTimestamp(result.getCells().get(0).key(), base_ts);
          }
          processPushRow(result.getCells(), index, base_ts);
        } else {
          if (current_result != null) {
            current_result.decode(result.getCells(), 
                (rollup_index < 0 || 
                 rollup_index >= node.rollupIntervals().size() 
                   ? null : node.rollupIntervals().get(rollup_index)));
          } else {
            LOG.error("Results for a multiget were nulled but we had valid "
                + "data to process.");
          }
        }
      }
      
      if (span != null) {
        span.setSuccessTags().finish();
      }
      onComplete(index);
      return null;
    }
  }
  
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
        outstanding.decrementAndGet();
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
  void nextBatch(final Span span) {
    if (all_batches_sent.get()) {
      LOG.warn("Multiget query was already completed but nextBatch was still called.");
      return;
    }
    
    int idx;
    int local_ts;
    synchronized (this) {
      if (tsuid_idx < 0) {
        // sentinel to increment the timestamp
        incrementTimestamp();
        tsuid_idx = 0;
        if (timestamp.compare(Op.GT, end_timestamp)) {
          // finished the whole query
          all_batches_sent.set(true);
          tsuid_idx = -1;
          onComplete(-2); // ugh, ugly sentinel till we get rid of QueryResults
          return;
        }
        
        if (node.sequenceEnd() != null && 
            timestamp.compare(Op.EQ, node.sequenceEnd())) {
          // finished a sequence so don't launch any more.
          return;
        }
      }
      
      idx = tsuid_idx;
      if (tsuid_idx + batch_size >= tsuids.size()) {
        // this is the last set of TSUIDs for this timestamp.
        tsuid_idx = -1;
      } else {
        tsuid_idx += batch_size;
      }
      local_ts = (int) timestamp.epoch();
    }
    
    final Span child;
    if (span != null) {
      child = span.newChild(getClass() + "nextBatch()")
          .withTag("batchSize", batch_size)
          .withTag("startTsuidIdx", idx)
          .withTag("startTimestamp", timestamp.epoch())
          .start();
    } else {
      child = null;
    }
    final List<GetRequest> requests = Lists.newArrayListWithCapacity(batch_size);
    final byte[] table = rollup_index >= 0 ? tables.get(rollup_index) : 
      tables.get(tables.size() - 1);
    // TODO - it would be extra nice to know the region splits so we 
    // can batch better. In this case we may have some split between regions.
    for (int i = idx; i < idx + batch_size && i < tsuids.size(); i++) {
      final byte[] tsuid = tsuids.get(i);
      
      final byte[] key;
      if (node.schema().saltWidth() > 0 && node.schema().timelessSalting()) {
        key = Arrays.copyOf(tsuid, tsuid.length);
        node.schema().setBaseTime(key, local_ts);
      } else {
        key = new byte[tsuid.length + node.schema().saltWidth() + 
                                      Schema.TIMESTAMP_BYTES];
        
        System.arraycopy(tsuid, 0, key, node.schema().saltWidth(), 
            node.schema().metricWidth());
        System.arraycopy(Bytes.fromInt(local_ts), 0, key, 
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
    
    outstanding.incrementAndGet();
    int index = -1;
    if (node.push()) {
      index = (int) ((local_ts - start_ts.epoch()) / interval);
      batches_per_set.incrementAndGet(index);
    }
    
    try {
      final Deferred<List<GetResultOrException>> deferred = 
          node.parent().client().get(requests);
      if (child == null || !child.isDebug()) {
        deferred.addCallback(new ResponseCB(index, child))
                .addErrback(error_cb);
      } else {
        deferred.addCallback(new ResponseCB(index, child))
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
    if (rollup_index >= 0) {
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
      timestamp.add(Duration.of(
          (reversed ? - 1 : 1), ChronoUnit.HOURS));
    }
  }
  
  /**
   * Generates the initial timestamp based on the query and aligned to
   * either the raw table or a rollup table.
   */
  @VisibleForTesting
  void setInitialTimestamp() {
    if (reversed) {
      timestamp.update(node.pipelineContext().query().endTime());
    } else {
      timestamp.update(node.pipelineContext().query().startTime());
    }
    
    if (source_config.timeShifts() != null && 
        source_config.timeShifts().containsKey(source_config.getId())) {
      final Pair<Boolean, TemporalAmount> pair = 
          source_config.timeShifts().get(source_config.getId());
      if (pair.getKey()) {
        timestamp.subtract(pair.getValue());
      } else {
        timestamp.add(pair.getValue());
      }
    }
    
    if (rollup_index >= 0 && 
        rollup_index < node.rollupIntervals().size()) {
      final Collection<QueryNode> rates = node.pipelineContext()
          .upstreamOfType(node, Rate.class);
      final RollupInterval interval = node.rollupIntervals().get(0);
      if (!rates.isEmpty()) {
        timestamp.updateEpoch(RollupUtils.getRollupBasetime(
            (reversed ? timestamp.epoch() + 1 : timestamp.epoch() - 1), interval));     
      } else {
        timestamp.updateEpoch((long) RollupUtils.getRollupBasetime(
            (reversed ? node.pipelineContext().query().endTime().epoch() : 
              node.pipelineContext().query().startTime().epoch()), interval));
      }
    } else {
      long ts = timestamp.epoch();
      if (!(Strings.isNullOrEmpty(source_config.getPrePadding()))) {
        final long padding = DateTime.parseDuration(source_config.getPrePadding());
        if (padding > 0) {
          ts -= padding / 1000L;
        }
      }
      
      // Then snap that timestamp back to its representative value for the
      // timespan in which it appears.
      ts -= ts % Schema.MAX_RAW_TIMESPAN;
      
      // Don't return negative numbers.
      ts = ts > 0L ? ts : 0L;
      timestamp.updateEpoch(ts);
    }
  }
  
  /**
   * Method to fallback to the next highest resolution and/or raw table.
   */
  private void fallback() {
    // we can fallback!
    if (node.pipelineContext().query().isDebugEnabled()) {
      node.pipelineContext().queryContext().logDebug(node,
          "Falling back to next highest resolution.");
    }
    
    if (++rollup_index >= node.rollupIntervals().size()) {
      rollup_index = -1;
    }
    setInitialTimestamp();
    setEndTimestamps();
    tsuid_idx = 0;
    all_batches_sent.set(false);
    
    if (node.pipelineContext().query().isTraceEnabled()) {
      node.pipelineContext().queryContext().logTrace(node, "Falling back to get "
          + "data from " + new String(
              rollup_index >= 0 ? tables.get(rollup_index) : tables.get(0)));
      if (LOG.isTraceEnabled()) {
        LOG.trace("Falling back to get "
          + "data from " + new String(
              rollup_index >= 0 ? tables.get(rollup_index) : tables.get(0)));
      }
    }
    
    if (node.push()) {
      for (int i = 0; i < sets.length(); i++) {
        try {
          if (sets.get(i) != null) {
            sets.get(i).close();
          }
        } catch (Exception e) {
          LOG.error("Unexpected exception closing set", e);
        }
      }
      
      setupSets();
    }
    
    while (outstanding.get() < concurrency_multi_get && 
           !all_batches_sent.get() &&
           !has_failed.get()) {
      nextBatch(child);
    }
  }
  
  /**
   * Processes data for a push row.
   * @param row The non-null row to process.
   * @param index The computed set index.
   * @param base_ts The base timestamp from the row key.
   */
  private void processPushRow(final ArrayList<KeyValue> row, 
                              final int index, 
                              final TimeStamp base_ts) {
    try {
      final byte[] tsuid = node.schema().getTSUID(row.get(0).key());
      final long hash = LongHashFunction.xx_r39().hashBytes(tsuid);
      
      synchronized (ts_ids) {
        if (!ts_ids.containsKey(hash)) {
          ts_ids.put(hash, new TSUID(tsuid, node.schema()));
        }
      }
      final RollupInterval interval = rollup_index >= 0 ? 
          node.rollupIntervals().get(rollup_index) : null;
      
      Tsdb1xPartialTimeSeriesSet set = getSet(index);
      Tsdb1xPartialTimeSeries pts = null;
      for (final KeyValue column : row) {
        if (rollup_index < 0 && (column.qualifier().length & 1) == 0) {
          // it's a NumericDataType
          if (!node.fetchDataType((byte) 0)) {
            // filter doesn't want #'s
            // TODO - dropped counters
            continue;
          }
          if (pts == null) {
            pts = node.schema().newSeries(
                NumericLongArrayType.TYPE, 
                base_ts, 
                hash, 
                set, 
                interval);
          }
          pts.addColumn((byte) 0,
                        column.qualifier(), 
                        column.value());
        } else if (rollup_index < 0) {
          final byte prefix = column.qualifier()[0];
          
          if (prefix == Schema.APPENDS_PREFIX) {
            if (!node.fetchDataType((byte) 1)) {
              // filter doesn't want #'s
              continue;
            } else {
              if (pts == null) {
                pts = node.schema().newSeries(
                    NumericLongArrayType.TYPE, 
                    base_ts, 
                    hash, 
                    set, 
                    null);
              }
              pts.addColumn(Schema.APPENDS_PREFIX,
                            column.qualifier(), 
                            column.value());
            }
          } else if (node.fetchDataType(prefix)) {
            // TODO - find the right type
          } else {
            // TODO - log drop
          }
        } else {
          // Only numerics are rolled up right now. And we shouldn't have
          // a rollup query if the user doesn't want rolled-up data.
          if (pts == null) {
            pts = node.schema().newSeries(
                NumericByteArraySummaryType.TYPE, 
                base_ts, 
                hash, 
                set, 
                interval);
          }
          pts.addColumn((byte) 0,
                        column.qualifier(), 
                        column.value());
        }
      }
      set.increment(pts, false);
    } catch (Throwable t) {
      LOG.error("Unexpected exception processing a row", t);
      throw t;
    }
  }
  
  /** @return Whether or not data was found during the gets. */
  private boolean haveData() {
    if (node.push()) {
      return node.sentData();
    } else {
      return current_result.timeSeries() != null && 
             !current_result.timeSeries().isEmpty();
    }
  }
  
  /**
   * Fetches or creates the proper set at the given index.
   * @param index The index of the set.
   * @return A set.
   */
  private Tsdb1xPartialTimeSeriesSet getSet(final int index) {
    Tsdb1xPartialTimeSeriesSet set = sets.get(index);
    if (set == null) {
      TimeStamp start = new SecondTimeStamp(start_ts.epoch() + (interval * index));
      TimeStamp end = new SecondTimeStamp(start_ts.epoch() + (interval * index) + interval);
      set = (Tsdb1xPartialTimeSeriesSet) 
          node.parent().tsdb().getRegistry()
          .getObjectPool(Tsdb1xPartialTimeSeriesSetPool.TYPE)
          .claim().object();
      set.reset(node, 
          start, 
          end, 
          node.rollupUsage(),
          1, 
          sets.length(), 
          ts_ids);
      if (!sets.compareAndSet(index, null, set)) {
        // lost the race
        try {
          set.close();
        } catch (Exception e) {
          LOG.error("Failed to close a set.", e);
        }
        set = sets.get(index);
      }
    }
    return set;
  }
  
  /**
   * When pushing data, computes the size of arrays and snaps the end timestamp
   * to the proper value.
   */
  private void setupSets() {
    if (reversed) {
      throw new UnsupportedOperationException("Reverse not supported yet.");
    }
    start_ts.update(timestamp);
    // snap end
    if (rollup_index >= 0) {
      final RollupInterval rollup_interval = node.rollupIntervals().get(rollup_index);
      interval = rollup_interval.getIntervalSeconds() * 
          rollup_interval.getIntervals();
      end_timestamp.updateEpoch(RollupUtils.getRollupBasetime(
          end_timestamp.epoch(), rollup_interval));
    } else {
      interval = Schema.MAX_RAW_TIMESPAN;
      long end = end_timestamp.epoch();
      end_timestamp.updateEpoch(end - (end % Schema.MAX_RAW_TIMESPAN));
    }
    
    final long indices = ((end_timestamp.epoch() - start_ts.epoch()) / interval) + 1;
    sets = new AtomicReferenceArray<Tsdb1xPartialTimeSeriesSet>((int) indices);
    batches_per_set = new AtomicIntegerArray((int) indices);
    finished_batches_per_set = new AtomicIntegerArray((int) indices);
  }
  
  /**
   * Helper to set the end timestamp.
   */
  private void setEndTimestamps() {
    if (reversed) {
      end_timestamp.update(node.pipelineContext().query().startTime());
    } else {
      end_timestamp.update(node.pipelineContext().query().endTime());
    }
    if (source_config.timeShifts() != null && 
        source_config.timeShifts().containsKey(source_config.getId())) {
      final Pair<Boolean, TemporalAmount> pair = 
          source_config.timeShifts().get(source_config.getId());
      if (pair.getKey()) {
        end_timestamp.subtract(pair.getValue());
      } else {
        end_timestamp.add(pair.getValue());
      }
    }
    
    if (!Strings.isNullOrEmpty(source_config.getPostPadding())) {
      end_timestamp.add(DateTime.parseDuration2(source_config.getPostPadding()));
    }
  }
}
