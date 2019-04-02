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
import java.time.temporal.TemporalAmount;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.Row;
import com.google.bigtable.v2.RowFilter;
import com.google.bigtable.v2.RowSet;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.protobuf.UnsafeByteOperations;

import net.opentsdb.common.Const;
import net.opentsdb.configuration.Configuration;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.Op;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.processor.rate.Rate;
import net.opentsdb.rollup.RollupInterval;
import net.opentsdb.rollup.RollupUtils;
import net.opentsdb.rollup.RollupUtils.RollupUsage;
import net.opentsdb.stats.Span;
import net.opentsdb.storage.schemas.tsdb1x.Schema;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.Pair;

/**
 * Class that handles fetching TSDB data from storage using reads 
 * instead of scanning for the data. This is only applicable if the 
 * query is using the "explicit_tags" feature and specified literal 
 * filters for exact matching or if the TSUIDs were resolved via a query
 * to meta-data.
 * It also works best when used for data that has high cardinality but 
 * the query is fetching a small subset of that data.
 * <p>
 * Note that this class is designed to asynchronously fire off 
 * {@link #concurrency_multi_get} number of get requests of size
 * {@link #batch_size}. Before issuing queries against Bigtable we sort the
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
 * {@link #fetchNext(Tsdb1xBigtableQueryResult, Span)}) but there can be many
 * batches executing at any time, potentially in different threads with
 * different region servers.
 * <p>
 * TODO - room for optimization here. E.g. we expect a decent number of
 * TSUIDs. If it's small then we'd potentially have only one or two 
 * requests per batch.
 * 
 * @since 2.4
 */
public class Tsdb1xBigtableMultiGet implements BigtableExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(Tsdb1xBigtableMultiGet.class);
  
  /** The upstream query node that owns this scanner set. */
  protected final Tsdb1xBigtableQueryNode node;
  
  /** The query config from the node. */
  protected final TimeSeriesDataSourceConfig source_config;
  
  /** The list of TSUIDs to work one. */
  protected final List<byte[]> tsuids;
  
  /** The number of get requests batches allowed to be outstanding at
   * any given time. */
  protected final int concurrency_multi_get;
  
  /** Whether or not we're fetching rows in reverse. */
  protected final boolean reversed;
  
  /** The number of get requests in each batch. */
  protected final int batch_size;
  
  /** A rollup column filter when fetching rollup data. */
  protected final RowFilter filter;
  
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
  
  /** The final timestamp with optional padding. */
  protected volatile TimeStamp end_timestamp;
  
  /** The current fallback timestamp for the next highest resolution data
   * we're querying when falling back. May be null. */
  protected volatile TimeStamp fallback_timestamp;
  
  /** Index into the {@link Tsdb1xBigtableQueryNode#rollupIntervals()} that we're
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
  protected volatile Tsdb1xBigtableQueryResult current_result;
  
  /** An optional tracing child. */
  protected volatile Span child;
  
  /** The state of this executor. */
  protected volatile State state;
  
  /**
   * Default ctor that parses the query, sets up rollups and sorts (and
   * optionally salts) the TSUIDs.
   * @param node A non-null query node that owns this getter.
   * @param source_config A non-null source config.
   * @param tsuids A non-null and non-empty list of TSUIDs.
   * @throws IllegalArgumentException if the params were null or empty.
   */
  public Tsdb1xBigtableMultiGet(final Tsdb1xBigtableQueryNode node, 
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
    
    if (source_config.hasKey(Tsdb1xBigtableDataStore.MULTI_GET_CONCURRENT_KEY)) {
      concurrency_multi_get = source_config.getInt(config, 
          Tsdb1xBigtableDataStore.MULTI_GET_CONCURRENT_KEY);
    } else {
      concurrency_multi_get = node.parent()
          .dynamicInt(Tsdb1xBigtableDataStore.MULTI_GET_CONCURRENT_KEY);
    }
    if (source_config.hasKey(Tsdb1xBigtableDataStore.MULTI_GET_BATCH_KEY)) {
      batch_size = source_config.getInt(config, 
          Tsdb1xBigtableDataStore.MULTI_GET_BATCH_KEY);
    } else {
      batch_size = node.parent()
          .dynamicInt(Tsdb1xBigtableDataStore.MULTI_GET_BATCH_KEY);
    }
    if (source_config.hasKey(Schema.QUERY_REVERSE_KEY)) {
      reversed = source_config.getBoolean(config, 
          Schema.QUERY_REVERSE_KEY);
    } else {
      reversed = node.parent()
          .dynamicBoolean(Schema.QUERY_REVERSE_KEY);
    }
    if (source_config.hasKey(Tsdb1xBigtableDataStore.PRE_AGG_KEY)) {
      pre_aggregate = source_config.getBoolean(config, 
          Tsdb1xBigtableDataStore.PRE_AGG_KEY);
    } else {
      pre_aggregate = false;
    }
    
    if (node.rollupIntervals() != null && 
        !node.rollupIntervals().isEmpty() && 
        node.rollupUsage() != RollupUsage.ROLLUP_RAW) {
      rollups_enabled = true;
      rollup_index = 0;
      
      RowFilter.Interleave.Builder builder = RowFilter.Interleave.newBuilder();
      for (final String agg : source_config.getSummaryAggregations()) {
        builder.addFilters(RowFilter.newBuilder()
            .setColumnQualifierRegexFilter(UnsafeByteOperations.unsafeWrap(
                agg.toLowerCase().getBytes(Const.ASCII_US_CHARSET))));
        builder.addFilters(RowFilter.newBuilder()
            .setColumnQualifierRegexFilter(UnsafeByteOperations.unsafeWrap(new byte[] { 
                (byte) node.schema().rollupConfig().getIdForAggregator(
                    agg.toLowerCase())
            })));
      }
      filter = RowFilter.newBuilder()
          .setChain(RowFilter.Chain.newBuilder()
              .addFilters(RowFilter.newBuilder()
                  .setFamilyNameRegexFilterBytes(
                      UnsafeByteOperations.unsafeWrap(Tsdb1xBigtableDataStore.DATA_FAMILY)))
              .addFilters(RowFilter.newBuilder()
                  .setInterleave(builder)))
          .build();
    } else {
      rollup_index = -1;
      rollups_enabled = false;
      filter = RowFilter.newBuilder()
          .setFamilyNameRegexFilterBytes(
              UnsafeByteOperations.unsafeWrap(Tsdb1xBigtableDataStore.DATA_FAMILY))
          .build();
    }
    
    // sentinel
    tsuid_idx = -1;
    timestamp = getInitialTimestamp(rollup_index);
    end_timestamp = reversed ? node.pipelineContext().query().startTime().getCopy() :
        node.pipelineContext().query().endTime().getCopy();
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
    
    if (rollups_enabled) {
      tables = Lists.newArrayListWithCapacity(node.rollupIntervals().size() + 1);
      for (final RollupInterval interval : node.rollupIntervals()) {
        if (pre_aggregate) {
          tables.add(node.parent().tableNamer().toTableNameStr(
              new String(interval.getGroupbyTable(), Const.ASCII_US_CHARSET))
                .getBytes(Const.ASCII_US_CHARSET));
        } else {
          tables.add(node.parent().tableNamer().toTableNameStr(
              new String(interval.getTemporalTable(), Const.ASCII_US_CHARSET))
                .getBytes(Const.ASCII_US_CHARSET));
        }
      }
      // already bigtablified
      tables.add(node.parent().dataTable());
    } else {
      // already bigtablified
      tables = Lists.newArrayList(node.parent().dataTable());
    }
    state = State.CONTINUE;
  }
  
  @Override
  public synchronized void fetchNext(final Tsdb1xBigtableQueryResult result, 
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
      final Tsdb1xBigtableQueryResult temp = current_result;
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
        ts.compare(Op.LT, end_timestamp) : 
        ts.compare(Op.GT, end_timestamp))) {
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
          ts.compare(Op.LT, end_timestamp) : 
          ts.compare(Op.GT, end_timestamp)) {
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
      current_result.setException(t);
      final QueryResult result = current_result;
      current_result = null;
      outstanding = 0;
      node.onNext(result);
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
        
        if (rollup_index >= tables.size()) {
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
  class ResponseCB implements FutureCallback<List<Row>> {
    final Span span;
    
    ResponseCB(final Span span) {
      this.span = span;
    }
    

    @Override
    public void onSuccess(final List<Row> results) {
      synchronized (Tsdb1xBigtableMultiGet.this) {
        outstanding--;
      }
      if (has_failed) {
        return;
      }
      
      try {
        for (final Row row : results) {
          if (row.getFamiliesCount() < 1 || row.getFamilies(0).getColumnsCount() < 1) {
            continue;
          }
          
          if (current_result != null) {
            current_result.decode(row, 
                (rollup_index < 0 || 
                 rollup_index >= node.rollupIntervals().size() 
                   ? null : node.rollupIntervals().get(rollup_index)));
          }
        }
        
        if (span != null) {
          span.setSuccessTags().finish();
        }
      } catch (Exception e) {
        onFailure(e);
        return;
      }
      onComplete();
    }

    @Override
    public void onFailure(final Throwable t) {
      synchronized (Tsdb1xBigtableMultiGet.this) {
        outstanding--;
      }
      
      if (span != null) {
        span.setErrorTags(t).finish();
      }
      onError(t);
    }
  }
  
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
    final RowSet.Builder rows = RowSet.newBuilder();
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
      rows.addRowKeys(UnsafeByteOperations.unsafeWrap(key));
    }
    
    try {
      ReadRowsRequest request = ReadRowsRequest.newBuilder()
          .setTableNameBytes(UnsafeByteOperations.unsafeWrap(table))
          .setRows(rows)
          .setFilter(filter)
          .build();
      
      Futures.addCallback(node.parent().executor().readRowsAsync(request), 
          new ResponseCB(child), node.parent().pool());
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
    final TimeStamp timestamp = reversed ? 
        node.pipelineContext().query().endTime().getCopy() : 
          node.pipelineContext().query().startTime().getCopy();
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
    
    if (rollups_enabled && rollup_index >= 0 && 
        rollup_index < node.rollupIntervals().size()) {
      final Collection<QueryNode> rates = node.pipelineContext()
          .upstreamOfType(node, Rate.class);
      final RollupInterval interval = node.rollupIntervals().get(0);
      if (!rates.isEmpty()) {
        return new MillisecondTimeStamp((long) RollupUtils.getRollupBasetime(
            (reversed ? timestamp.epoch() + 1 : timestamp.epoch() - 1), interval) * 1000L);      
      } else {
        return new MillisecondTimeStamp((long) RollupUtils.getRollupBasetime(
            (reversed ? node.pipelineContext().query().endTime().epoch() : 
              node.pipelineContext().query().startTime().epoch()), interval) * 1000L);
      }
    } else {
      long ts = timestamp.epoch();
      if (!(Strings.isNullOrEmpty(source_config.getPrePadding()))) {
        final long interval = DateTime.parseDuration(source_config.getPrePadding());
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
