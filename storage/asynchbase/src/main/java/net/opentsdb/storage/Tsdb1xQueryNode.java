// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.DeferredGroupException;

import net.opentsdb.common.Const;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.exceptions.IllegalDataException;
import net.opentsdb.exceptions.QueryDownstreamException;
import net.opentsdb.exceptions.QueryUpstreamException;
import net.opentsdb.meta.MetaDataStorageResult;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QuerySourceConfig;
import net.opentsdb.query.processor.downsample.Downsample;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.rollup.RollupInterval;
import net.opentsdb.rollup.RollupUtils.RollupUsage;
import net.opentsdb.stats.Span;
import net.opentsdb.storage.HBaseExecutor.State;
import net.opentsdb.storage.schemas.tsdb1x.Schema;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.uid.UniqueIdType;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.Bytes.ByteMap;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.Exceptions;

/**
 * A query node implementation for the V1 schema from OpenTSDB. If the 
 * schema was loaded with a meta-data store, the node will query meta
 * first. If the meta results were empty and fallback is enabled, or if
 * meta is not enabled, we'll perform scans.
 * 
 * @since 3.0
 */
public class Tsdb1xQueryNode implements SourceNode {
  private static final Logger LOG = LoggerFactory.getLogger(
      Tsdb1xQueryNode.class);

  /** A reference to the parent of this node. */
  protected final Tsdb1xHBaseDataStore parent;
  
  /** The name of this node. */
  protected final String id;
  
  /** The pipeline context. */
  protected final QueryPipelineContext context;
  
  /** The upstream query nodes. */
  protected Collection<QueryNode> upstream;
  
  /** The downstream query nodes. */
  protected Collection<QueryNode> downstream;
  
  /** The downstream source nodes. */
  protected Collection<TimeSeriesDataSource> downstream_sources;
  
  /** The query source config. */
  protected final QuerySourceConfig config;
  
  /** The sequence ID counter. */
  protected final AtomicLong sequence_id;
  
  /** Whether the node has been initialized. Initialization starts with
   * the call to {@link #fetchNext(Span)}. */
  protected final AtomicBoolean initialized;
  
  /** Whether or not the node is initializing. This is a block on calling
   * {@link #fetchNext(Span)} multiple times. */
  protected final AtomicBoolean initializing;
  
  /** The executor for this node. */
  protected HBaseExecutor executor;
  
  /** Whether or not to skip NoSuchUniqueName errors for tag keys on resolution. */
  protected final boolean skip_nsun_tagks;
  
  /** Whether or not to skip NoSuchUniqueName errors for tag values on resolution. */
  protected final boolean skip_nsun_tagvs;
  
  /** Whether or not to skip name-less IDs when received from HBase. */
  protected final boolean skip_nsui;
  
  /** Whether or not to delete the data found by this query. */
  protected final boolean delete;
  
  /** Rollup fallback mode. */
  protected final RollupUsage rollup_usage;
  
  /** The rollup downsampling aggregation by name. */
  protected String rollup_aggregation;
  
  /** Rollup intervals matching the query downsampler if applicable. */
  protected List<RollupInterval> rollup_intervals;
  
  /** An optional downsample config from upstream. */
  protected DownsampleConfig ds_config;
  
  /**
   * Default ctor.
   * @param factory The Tsdb1xHBaseDataStore that instantiated this node.
   * @param context A non-null query pipeline context.
   * @param id An ID for the node.
   * @param config A non-null config.
   */
  public Tsdb1xQueryNode(final Tsdb1xHBaseDataStore parent, 
                         final QueryPipelineContext context,
                         final String id,
                         final QuerySourceConfig config) {
    if (parent == null) {
      throw new IllegalArgumentException("Parent cannot be null.");
    }
    if (context == null) {
      throw new IllegalArgumentException("Context cannot be null.");
    }
    if (config == null) {
      throw new IllegalArgumentException("Configuration cannot be null.");
    }
    if (context.tsdb().getConfig() == null) {
      throw new IllegalArgumentException("Can't execute a query without "
          + "a configuration in the source config!");
    }
    this.parent = parent;
    this.context = context;
    this.id = id;
    this.config = config;
    
    sequence_id = new AtomicLong();
    initialized = new AtomicBoolean();
    initializing = new AtomicBoolean();
    
    if (config.hasKey(Tsdb1xHBaseDataStore.SKIP_NSUN_TAGK_KEY)) {
      skip_nsun_tagks = config.getBoolean(context.tsdb().getConfig(), 
          Tsdb1xHBaseDataStore.SKIP_NSUN_TAGK_KEY);
    } else {
      skip_nsun_tagks = parent
          .dynamicBoolean(Tsdb1xHBaseDataStore.SKIP_NSUN_TAGK_KEY);
    }
    if (config.hasKey(Tsdb1xHBaseDataStore.SKIP_NSUN_TAGV_KEY)) {
      skip_nsun_tagvs = config.getBoolean(context.tsdb().getConfig(), 
          Tsdb1xHBaseDataStore.SKIP_NSUN_TAGV_KEY);
    } else {
      skip_nsun_tagvs = parent
          .dynamicBoolean(Tsdb1xHBaseDataStore.SKIP_NSUN_TAGV_KEY);
    }
    if (config.hasKey(Tsdb1xHBaseDataStore.SKIP_NSUI_KEY)) {
      skip_nsui = config.getBoolean(context.tsdb().getConfig(), 
          Tsdb1xHBaseDataStore.SKIP_NSUI_KEY);
    } else {
      skip_nsui = parent
          .dynamicBoolean(Tsdb1xHBaseDataStore.SKIP_NSUI_KEY);
    }
    if (config.hasKey(Tsdb1xHBaseDataStore.DELETE_KEY)) {
      delete = config.getBoolean(context.tsdb().getConfig(), 
          Tsdb1xHBaseDataStore.DELETE_KEY);
    } else {
      delete = parent
          .dynamicBoolean(Tsdb1xHBaseDataStore.DELETE_KEY);
    }
    if (config.hasKey(Tsdb1xHBaseDataStore.ROLLUP_USAGE_KEY)) {
      rollup_usage = RollupUsage.parse(config.getString(context.tsdb().getConfig(),
          Tsdb1xHBaseDataStore.ROLLUP_USAGE_KEY));
    } else {
      rollup_usage = RollupUsage.parse(parent
          .dynamicString(Tsdb1xHBaseDataStore.ROLLUP_USAGE_KEY));
    }
  }

  @Override
  public QueryNodeConfig config() {
    return config;
  }

  @Override
  public String id() {
    return config.getId();
  }

  @Override
  public void close() {
    if (executor != null) {
      executor.close();
    }
  }

  @Override
  public void fetchNext(final Span span) {
    // TODO - how do I determine if we have an outstanding request and 
    // should queue or block another fetch? hmmm.
    if (!initialized.get()) {
      if (initializing.compareAndSet(false, true)) {
        setup(span);
        return;
      } else {
        throw new IllegalStateException("Don't call me until I'm "
            + "finished setting up!");
      }
    }

    executor.fetchNext(new Tsdb1xQueryResult(
          sequence_id.getAndIncrement(), 
          Tsdb1xQueryNode.this, 
          parent.schema()), 
    span);

  }
  
  @Override
  public void onComplete(final QueryNode downstream, 
                         final long final_sequence,
                         final long total_sequences) {
    completeUpstream(final_sequence, total_sequences);
  }

  @Override
  public void onNext(final QueryResult next) {
    sendUpstream(next);
    if (executor.state() == State.COMPLETE) {
      completeUpstream(sequence_id.get(), sequence_id.get());
    }
  }

  @Override
  public void onError(final Throwable t) {
    sendUpstream(t);
  }

  @Override
  public TimeStamp sequenceEnd() {
    // TODO implement when the query has this information.
    return null;
  }

  @Override
  public Schema schema() {
    return parent.schema();
  }
  
  @Override
  public void initialize(final Span span) {
    final Span child;
    if (span != null) {
      child = span.newChild(getClass() + ".initialize()").start();
    } else {
      child = null;
    }

    if (parent.schema().rollupConfig() != null && 
        rollup_usage != RollupUsage.ROLLUP_RAW) {
      Collection<QueryNode> downsamplers = context.upstreamOfType(this, Downsample.class);
      if (!downsamplers.isEmpty()) {
        // TODO - find the lowest-common resolution if possible.
        ds_config = (DownsampleConfig) downsamplers.iterator().next().config();
        rollup_intervals = parent.schema()
            .rollupConfig().getRollupIntervals(
                DateTime.parseDuration(ds_config.intervalAsString()) / 1000, 
                ds_config.intervalAsString(), 
                true);
        rollup_aggregation = ds_config.aggregator();
      } else {
        rollup_intervals = null;
        rollup_aggregation = null;
      }
    } else {
      rollup_intervals = null;
      rollup_aggregation = null;
    }
    
    upstream = context.upstream(this);
    downstream = context.downstream(this);
    downstream_sources = context.downstreamSources(this);
    if (child != null) {
      child.setSuccessTags().finish();
    }
  }
  
  @Override
  public QueryPipelineContext pipelineContext() {
    return context;
  }
  
  /**
   * Calls {@link #fetchNext(Span)} on all of the downstream nodes.
   * @param span An optional tracing span.
   */
  protected void fetchDownstream(final Span span) {
    for (final TimeSeriesDataSource source : downstream_sources) {
      source.fetchNext(span);
    }
  }
  
  /**
   * Sends the result to each of the upstream subscribers.
   * 
   * @param result A non-null result.
   * @throws QueryUpstreamException if the upstream 
   * {@link #onNext(QueryResult)} handler throws an exception. I hate
   * checked exceptions but each node needs to be able to handle this
   * ideally by cancelling the query.
   * @throws IllegalArgumentException if the result was null.
   */
  protected void sendUpstream(final QueryResult result) 
        throws QueryUpstreamException {
    if (result == null) {
      throw new IllegalArgumentException("Result cannot be null.");
    }
    
    for (final QueryNode node : upstream) {
      try {
        node.onNext(result);
      } catch (Exception e) {
        throw new QueryUpstreamException("Failed to send results "
            + "upstream to node: " + node, e);
      }
    }
  }
  
  /**
   * Sends the throwable upstream to each of the subscribing nodes. If 
   * one or more upstream consumers throw an exception, it's caught and
   * logged as a warning.
   * 
   * @param t A non-null throwable.
   * @throws IllegalArgumentException if the throwable was null.
   */
  protected void sendUpstream(final Throwable t) {
    if (t == null) {
      throw new IllegalArgumentException("Throwable cannot be null.");
    }
    
    for (final QueryNode node : upstream) {
      try {
        node.onError(t);
      } catch (Exception e) {
        LOG.warn("Failed to send exception upstream to node: " + node, e);
      }
    }
  }
  
  /**
   * Passes the sequence info upstream to all subscribers. If one or 
   * more upstream consumers throw an exception, it's caught and logged 
   * as a warning.
   * 
   * @param final_sequence The final sequence number to pass.
   * @param total_sequences The total sequence count to pass.
   */
  protected void completeUpstream(final long final_sequence,
                                  final long total_sequences) {
    for (final QueryNode node : upstream) {
      try {
        node.onComplete(this, final_sequence, total_sequences);
      } catch (Exception e) {
        LOG.warn("Failed to mark upstream node complete: " + node, e);
      }
    }
  }
  
  @Override
  public int hashCode() {
    return (getClass().getCanonicalName() + id).hashCode();
  }

  /** @return The parent for this node. */
  Tsdb1xHBaseDataStore parent() {
    return parent;
  }
  
  /** @return Whether or not to skip name-less UIDs found in storage. */
  boolean skipNSUI() {
    return skip_nsui;
  }
  
  /**
   * @param prefix A 1 to 254 prefix for a data type.
   * @return True if the type should be included, false if we're filtering
   * out that data type.
   */
  boolean fetchDataType(final byte prefix) {
    // TODO - implement
    return true;
  }
  
  /** @return Whether or not to delete the found data. */
  boolean deleteData() {
    return delete;
  }

  /** @return A list of applicable rollup intervals. May be null. */
  List<RollupInterval> rollupIntervals() {
    return rollup_intervals;
  }
  
  /** @return The rollup usage mode. */
  RollupUsage rollupUsage() {
    return rollup_usage;
  }

  /** @return The optional rollup aggregation. May be null. */
  String rollupAggregation() {
    return rollup_aggregation;
  }
  
  /** @return The optional downsample config. May be null. */
  DownsampleConfig downsampleConfig() {
    return ds_config;
  }
  
  /**
   * Initializes the query, either calling meta or setting up the scanner.
   * @param span An optional tracing span.
   */
  @VisibleForTesting
  void setup(final Span span) {
    if (parent.schema().metaSchema() != null) {
      parent.schema().metaSchema().runQuery(config, span)
          .addCallback(new MetaCB(span))
          .addErrback(new MetaErrorCB(span));
    } else {
      synchronized (this) {
        executor = new Tsdb1xScanners(Tsdb1xQueryNode.this, config);
        if (initialized.compareAndSet(false, true)) {
          executor.fetchNext(new Tsdb1xQueryResult(
              sequence_id.incrementAndGet(), 
              Tsdb1xQueryNode.this, 
              parent.schema()), 
          span);
        } else {
          LOG.error("WTF? We lost an initialization race??");
        }
      }
    }
  }
  
  /**
   * A class to catch exceptions fetching data from meta.
   */
  class MetaErrorCB implements Callback<Object, Exception> {
    final Span span;
    
    MetaErrorCB(final Span span) {
      this.span = span;
    }
    
    @Override
    public Object call(final Exception ex) throws Exception {
      if (span != null) {
        span.setErrorTags(ex)
            .finish();
      }
      sendUpstream(ex);
      return null;
    }
    
  }
  
  /**
   * Handles the logic of what to do based on the results of a meta call
   * e.g. continue with meta if we have data, stop without data or fallback
   * to scans.
   */
  class MetaCB implements Callback<Object, MetaDataStorageResult> {
    final Span span;
    
    MetaCB(final Span span) {
      this.span = span;
    }
    
    @Override
    public Object call(final MetaDataStorageResult result) throws Exception {
      if (span != null) {
        span.setSuccessTags()
            .setTag("metaResponse", result.result().toString())
            .finish();
      }
      
      switch (result.result()) {
      case DATA:
        if (LOG.isDebugEnabled()) {
          LOG.debug("Received results from meta store, setting up "
              + "multi-gets.");
        }
        resolveMeta(result, span);
        return null;
      case NO_DATA:
        if (LOG.isDebugEnabled()) {
          LOG.debug("No data returned from meta store.");
        }
        initialized.compareAndSet(false, true);
        sendUpstream(new Tsdb1xQueryResult(0, Tsdb1xQueryNode.this, 
            parent.schema()));
        completeUpstream(0, 0);
        return null;
      case EXCEPTION:
        LOG.warn("Unrecoverable exception from meta store: ", 
            result.exception());
        initialized.compareAndSet(false, true);
        sendUpstream(result.exception());
        return null;
      case NO_DATA_FALLBACK:
        if (LOG.isDebugEnabled()) {
          LOG.debug("No data returned from meta store." 
              + " Falling back to scans.");
        }
        break; // fall through to scans
      case EXCEPTION_FALLBACK:
        LOG.warn("Exception from meta store, falling back", 
            result.exception());
        break;
      default: // fall through to scans
        final QueryDownstreamException ex = new QueryDownstreamException(
            "Unhandled meta result type: " + result.result());
        LOG.error("WTF? Shouldn't happen.", ex);
        initialized.compareAndSet(false, true);
        sendUpstream(ex);
        return null;
      }
      
      synchronized (Tsdb1xQueryNode.this) {
        executor = new Tsdb1xScanners(Tsdb1xQueryNode.this, config);
        if (initialized.compareAndSet(false, true)) {
          executor.fetchNext(new Tsdb1xQueryResult(
              sequence_id.incrementAndGet(), 
              Tsdb1xQueryNode.this, 
              parent.schema()), 
          span);
        } else {
          LOG.error("WTF? We lost an initialization race??");
        }
      }
      return null;
    }
    
  }
  
  /**
   * Processes the list of TSUIDs from the meta data store, resolving 
   * strings to UIDs.
   * @param result A non-null result with the 
   * {@link MetaDataStorageResult#timeSeries()} populated. 
   * @param span An optional tracing span.
   */
  @VisibleForTesting
  void resolveMeta(final MetaDataStorageResult result, final Span span) {
    final Span child;
    if (span != null) {
      child = span.newChild(getClass().getName() + ".resolveMeta").start();
    } else {
      child = span;
    }
    
    final int metric_width = parent.schema().metricWidth();
    final int tagk_width = parent.schema().tagkWidth();
    final int tagv_width = parent.schema().tagvWidth();
    
    if (result.idType() == Const.TS_BYTE_ID) {
      // easy! Just flatten the bytes.
      final List<byte[]> tsuids = Lists.newArrayListWithExpectedSize(
          result.timeSeries().size());
      final byte[] metric = ((TimeSeriesByteId) result.timeSeries()
          .get(0)).metric();
      for (final TimeSeriesId raw_id : result.timeSeries()) {
        final TimeSeriesByteId id = (TimeSeriesByteId) raw_id;
        if (Bytes.memcmp(metric, id.metric()) != 0) {
          throw new IllegalDataException("Meta returned two or more "
              + "metrics. The initial metric was " + Bytes.pretty(metric) 
              + " and another was " + Bytes.pretty(id.metric()));
        }
        final byte[] tsuid = new byte[metric_width + 
                                      (id.tags().size() * tagk_width) + 
                                      (id.tags().size() * tagv_width)
                                      ];
        System.arraycopy(id.metric(), 0, tsuid, 0, metric_width);
        int idx = metric_width;
        // no need to sort since the id specifies a ByteMap, already sorted!
        for (final Entry<byte[], byte[]> entry : id.tags().entrySet()) {
          System.arraycopy(entry.getKey(), 0, tsuid, idx, tagk_width);
          idx += tagk_width;
          System.arraycopy(entry.getValue(), 0, tsuid, idx, tagv_width);
          idx += tagv_width;
        }
        
        tsuids.add(tsuid);
      }
      
      synchronized (this) {
        executor = new Tsdb1xMultiGet(Tsdb1xQueryNode.this, config, tsuids);
        if (initialized.compareAndSet(false, true)) {
          if (child != null) {
            child.setSuccessTags()
                 .finish();
          }
          executor.fetchNext(new Tsdb1xQueryResult(
              sequence_id.incrementAndGet(), 
              Tsdb1xQueryNode.this, 
              parent.schema()), 
          span);
        } else {
          LOG.error("WTF? We lost an initialization race??");
        }
      }
    } else {
      final String metric = ((TimeSeriesStringId) 
          result.timeSeries().get(0)).metric();
      Set<String> dedupe_tagks = Sets.newHashSet();
      Set<String> dedupe_tagvs = Sets.newHashSet();
      // since it's quite possible that a result would share a number of 
      // common tag keys and values, we dedupe into maps then resolve those 
      // and compile the TSUIDs from them. 
      for (final TimeSeriesId raw_id : result.timeSeries()) {
        final TimeSeriesStringId id = (TimeSeriesStringId) raw_id;
        if (metric != null && !metric.equals(id.metric())) {
          throw new IllegalDataException("Meta returned two or more "
              + "metrics. The initial metric was " + metric 
              + " and another was " + id.metric());
        }
        
        for (final Entry<String, String> entry : id.tags().entrySet()) {
          dedupe_tagks.add(entry.getKey());
          dedupe_tagvs.add(entry.getValue());
        }
      }
      
      // now resolve
      final List<String> tagks = Lists.newArrayList(dedupe_tagks);
      final List<String> tagvs = Lists.newArrayList(dedupe_tagvs);
      final byte[] metric_uid = new byte[parent
                                         .schema().metricWidth()];
      final Map<String, byte[]> tagk_map = 
          Maps.newHashMapWithExpectedSize(tagks.size());
      final Map<String, byte[]> tagv_map = 
          Maps.newHashMapWithExpectedSize(tagvs.size());
      final List<byte[]> tsuids = Lists.newArrayListWithExpectedSize(
          result.timeSeries().size());
      
      /** Catches and passes errors upstream. */
      class ErrorCB implements Callback<Object, Exception> {
        @Override
        public Object call(final Exception ex) throws Exception {
          if (ex instanceof DeferredGroupException) {
            if (child != null) {
              child.setErrorTags(Exceptions.getCause((DeferredGroupException) ex))
                   .finish();
            }
            sendUpstream(Exceptions.getCause((DeferredGroupException) ex));
          } else {
            if (child != null) {
              child.setErrorTags(ex)
                     .finish();
            }
            sendUpstream(ex);
          }
          return null;
        }
      }
      
      /** Handles copying the resolved metric. */
      class MetricCB implements Callback<Object, byte[]> {
        @Override
        public Object call(final byte[] uid) throws Exception {
          if (uid == null) {
            final NoSuchUniqueName ex = 
                new NoSuchUniqueName(Schema.METRIC_TYPE, metric);
            if (child != null) {
              child.setErrorTags(ex)
                   .finish();
            }
            throw ex;
          }
          
          for (int i = 0; i < uid.length; i++) {
            metric_uid[i] = uid[i];
          }
          return null;
        }
      }
      
      /** Populates the tag to UID maps. */
      class TagCB implements Callback<Object, List<byte[]>> {
        final boolean is_tagvs;
        
        TagCB(final boolean is_tagvs) {
          this.is_tagvs = is_tagvs;
        }

        @Override
        public Object call(final List<byte[]> uids) throws Exception {
          if (is_tagvs) {
            for (int i = 0; i < uids.size(); i++) {
              if (uids.get(i) == null) {
                if (skip_nsun_tagvs) {
                  if (LOG.isDebugEnabled()) {
                    LOG.debug("Dropping tag value without an ID: " 
                        + tagvs.get(i));
                  }
                  continue;
                }
                
                final NoSuchUniqueName ex = 
                    new NoSuchUniqueName(Schema.TAGV_TYPE, tagvs.get(i));
                if (child != null) {
                  child.setErrorTags(ex)
                       .finish();
                }
                throw ex;
              }
              
              tagv_map.put(tagvs.get(i), uids.get(i));
            }
          } else {
            for (int i = 0; i < uids.size(); i++) {
              if (uids.get(i) == null) {
                if (skip_nsun_tagks) {
                  if (LOG.isDebugEnabled()) {
                    LOG.debug("Dropping tag key without an ID: " 
                        + tagks.get(i));
                  }
                  continue;
                }
                
                final NoSuchUniqueName ex = 
                    new NoSuchUniqueName(Schema.TAGK_TYPE, tagks.get(i));
                if (child != null) {
                  child.setErrorTags(ex)
                       .finish();
                }
                throw ex;
              }
              
              tagk_map.put(tagks.get(i), uids.get(i));
            }
          }
          
          return null;
        }
      }

      /** The final callback that creates the TSUIDs. */
      class GroupCB implements Callback<Object, ArrayList<Object>> {
        @Override
        public Object call(final ArrayList<Object> ignored) throws Exception {
          // TODO - maybe a better way but the TSUIDs have to be sorted
          // on the key values.
          final ByteMap<byte[]> sorter = new ByteMap<byte[]>();
          for (final TimeSeriesId raw_id : result.timeSeries()) {
            final TimeSeriesStringId id = (TimeSeriesStringId) raw_id;
            sorter.clear();
            
            boolean keep_goin = true;
            for (final Entry<String, String> entry : id.tags().entrySet()) {
              final byte[] tagk = tagk_map.get(entry.getKey());
              final byte[] tagv = tagv_map.get(entry.getValue());
              if (tagk == null || tagv == null) {
                keep_goin = false;
                break;
              }
              sorter.put(tagk, tagv);
            }
            
            if (!keep_goin) {
              // dropping due to a NSUN tagk or tagv
              continue;
            }
            
            final byte[] tsuid = new byte[metric_width + 
                                          (id.tags().size() * tagk_width) + 
                                          (id.tags().size() * tagv_width)
                                          ];
            System.arraycopy(metric_uid, 0, tsuid, 0, metric_width);
            int idx = metric_width;
            for (final Entry<byte[], byte[]> entry : sorter.entrySet()) {
              System.arraycopy(entry.getKey(), 0, tsuid, idx, tagk_width);
              idx += tagk_width;
              System.arraycopy(entry.getValue(), 0, tsuid, idx, tagv_width);
              idx += tagv_width;
            }
            
            tsuids.add(tsuid);
          }
          
          // TODO - what happens if we didn't resolve anything???
          synchronized (this) {
            executor = new Tsdb1xMultiGet(
                Tsdb1xQueryNode.this, 
                config, 
                tsuids);
            if (initialized.compareAndSet(false, true)) {
              if (child != null) {
                child.setSuccessTags()
                     .finish();
              }
              executor.fetchNext(new Tsdb1xQueryResult(
                  sequence_id.incrementAndGet(), 
                  Tsdb1xQueryNode.this, 
                  parent.schema()), 
              span);
            } else {
              LOG.error("WTF? We lost an initialization race??");
            }
          }
          
          return null;
        }
      }
      
      final List<Deferred<Object>> deferreds = Lists.newArrayListWithCapacity(3);
      deferreds.add(parent.schema()
          .getId(UniqueIdType.METRIC, metric, span)
            .addCallbacks(new MetricCB(), new ErrorCB()));
      deferreds.add(parent.schema()
          .getIds(UniqueIdType.TAGK, tagks, span)
            .addCallbacks(new TagCB(false), new ErrorCB()));
      deferreds.add(parent.schema()
          .getIds(UniqueIdType.TAGV, tagvs, span)
            .addCallbacks(new TagCB(true), new ErrorCB()));
      Deferred.group(deferreds).addCallbacks(new GroupCB(), new ErrorCB());
    }
  }
}
