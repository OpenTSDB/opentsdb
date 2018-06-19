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
package net.opentsdb.storage;

import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.common.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.iterators.SlicedTimeSeries;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericMillisecondShard;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.AbstractQueryNode;
import net.opentsdb.query.QueryMode;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QuerySourceConfig;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.query.pojo.Filter;
import net.opentsdb.query.pojo.Metric;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.stats.Span;
import net.opentsdb.utils.DateTime;

/**
 * A simple store that generates a set of time series to query as well as stores
 * new values (must be written in time order) all in memory. It's meant for
 * testing pipelines and benchmarking.
 * 
 * @since 3.0
 */
public class MockDataStore implements TimeSeriesDataStore {
  private static final Logger LOG = LoggerFactory.getLogger(MockDataStore.class);
  
  public static final long ROW_WIDTH = 3600000;
  public static final long HOSTS = 4;
  public static final long INTERVAL = 60000;
  public static final long HOURS = 24;
  public static final List<String> DATACENTERS = Lists.newArrayList(
      "PHX", "LGA", "LAX", "DEN");
  public static final List<String> METRICS = Lists.newArrayList(
      "sys.cpu.user", "sys.if.out", "sys.if.in", "web.requests");

  private final TSDB tsdb;
  private final String id;
  
  /** The super inefficient and thread unsafe in-memory db. */
  private Map<TimeSeriesStringId, MockSpan> database;
  
  /** Thread pool used by the executions. */
  private ExecutorService thread_pool;
  
  public MockDataStore(final TSDB tsdb, final String id) {
    this.tsdb = tsdb;
    this.id = id;
    
    database = Maps.newHashMap();
    generateMockData();
    if (tsdb.getConfig().hasProperty("MockDataStore.threadpool.enable") && 
        tsdb.getConfig().getBoolean("MockDataStore.threadpool.enable")) {
      thread_pool = Executors.newCachedThreadPool();
    }
  }
  
  @Override
  public QueryNode newNode(final QueryPipelineContext context,
                           final String id,
                           final QueryNodeConfig config) {
    return new LocalNode(context, id, (QuerySourceConfig) config);
  }
  
  @Override
  public QueryNode newNode(QueryPipelineContext context, String id) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Class<? extends QueryNodeConfig> nodeConfigClass() {
    // TODO Auto-generated method stub
    return null;
  }
  
  @Override
  public Deferred<List<byte[]>> encodeJoinKeys(final List<String> join_keys, final Span span) {
    return Deferred.fromResult(null);
  }
  
  @Override
  public Deferred<Object> shutdown() {
    if (thread_pool != null) {
      thread_pool.shutdownNow();
    }
    return Deferred.fromResult(null);
  }
  
  @Override
  public Deferred<Object> write(final TimeSeriesStringId id,
                                final TimeSeriesValue<?> value, 
                                final Span span) {    
    MockSpan data_span = database.get(id);
    if (data_span == null) {
      data_span = new MockSpan(id);
      database.put(id, data_span);
    }
    
    data_span.addValue(value);
    return Deferred.fromResult(null);
  }
  
  @Override
  public String id() {
    return "MockDataStore";
  }
  
  class MockSpan {
    private List<MockRow> rows = Lists.newArrayList();
    private final TimeSeriesStringId id;
    
    public MockSpan(final TimeSeriesStringId id) {
      this.id = id;
    }
    
    public void addValue(TimeSeriesValue<?> value) {
      
      long base_time = value.timestamp().msEpoch() - 
          (value.timestamp().msEpoch() % ROW_WIDTH);
      
      for (final MockRow row : rows) {
        if (row.base_timestamp == base_time) {
          row.addValue(value);
          return;
        }
      }
      
      final MockRow row = new MockRow(id, value);
      rows.add(row);
    }
  
    List<MockRow> rows() {
      return rows;
    }
  }
  
  class MockRow implements TimeSeries {
    private TimeSeriesStringId id;
    public long base_timestamp;
    public Map<TypeToken<?>, TimeSeries> sources;
    
    public MockRow(final TimeSeriesStringId id, 
                   final TimeSeriesValue<?> value) {
      this.id = id;
      base_timestamp = value.timestamp().msEpoch() - 
          (value.timestamp().msEpoch() % ROW_WIDTH);
      sources = Maps.newHashMap();
      // TODO - other types
      if (value.type() == NumericType.TYPE) {
        sources.put(NumericType.TYPE, new NumericMillisecondShard(
            id,
            new MillisecondTimeStamp(base_timestamp), 
            new MillisecondTimeStamp(base_timestamp + ROW_WIDTH)));
        addValue(value);
      }
    }
    
    public void addValue(final TimeSeriesValue<?> value) {
      // TODO - other types
      if (value.type() == NumericType.TYPE) {
        NumericMillisecondShard shard = 
            (NumericMillisecondShard) sources.get(NumericType.TYPE);
        if (shard == null) {
          shard = new NumericMillisecondShard(
              id,
              new MillisecondTimeStamp(base_timestamp), 
              new MillisecondTimeStamp(base_timestamp + ROW_WIDTH));
          sources.put(NumericType.TYPE, shard);
        }
        if (((TimeSeriesValue<NumericType>) value).value().isInteger()) {
          shard.add(value.timestamp().msEpoch(), 
              ((TimeSeriesValue<NumericType>) value).value().longValue());
        } else {
          shard.add(value.timestamp().msEpoch(), 
              ((TimeSeriesValue<NumericType>) value).value().doubleValue());
        }
      }
    }

    @Override
    public TimeSeriesStringId id() {
      return id;
    }

    @Override
    public Optional<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> iterator(
        TypeToken<?> type) {
      // TODO - other types
      if (type == NumericType.TYPE) {
        return Optional.of(((NumericMillisecondShard) 
            sources.get(NumericType.TYPE)).iterator());
      }
      return Optional.empty();
    }

    @Override
    public Collection<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> iterators() {
      // TODO - other types
      final List<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> its = 
          Lists.newArrayListWithCapacity(1);
      its.add(((NumericMillisecondShard) sources.get(NumericType.TYPE)).iterator());
      return its;
    }

    @Override
    public Collection<TypeToken<?>> types() {
      return sources.keySet();
    }

    @Override
    public void close() {
      // TODO Auto-generated method stub
    }
    
  }
  
  private void generateMockData() {
    long start_timestamp = DateTime.currentTimeMillis() - 2 * ROW_WIDTH;
    start_timestamp = start_timestamp - start_timestamp % ROW_WIDTH;
    if (tsdb.getConfig().hasProperty("MockDataStore.timestamp")) {
      start_timestamp = tsdb.getConfig().getLong("MockDataStore.timestamp");
    }
    
    long hours = HOURS;
    if (tsdb.getConfig().hasProperty("MockDataStore.hours")) {
      hours = tsdb.getConfig().getLong("MockDataStore.hours");
    }
    
    long hosts = HOSTS;
    if (tsdb.getConfig().hasProperty("MockDataStore.hosts")) {
      hosts = tsdb.getConfig().getLong("MockDataStore.hosts");
    }
    
    long interval = INTERVAL;
    if (tsdb.getConfig().hasProperty("MockDataStore.interval")) {
      interval = tsdb.getConfig().getLong("MockDataStore.interval");
      if (interval <= 0) {
        throw new IllegalStateException("Interval can't be 0 or less.");
      }
    }
    
    for (int t = 0; t < hours; t++) {
      for (final String metric : METRICS) {
        for (final String dc : DATACENTERS) {
          for (int h = 0; h < hosts; h++) {
            TimeSeriesStringId id = BaseTimeSeriesStringId.newBuilder()
                .setMetric(metric)
                .addTags("dc", dc)
                .addTags("host", String.format("web%02d", h + 1))
                .build();
            MutableNumericValue dp = new MutableNumericValue();
            TimeStamp ts = new MillisecondTimeStamp(0);
            for (long i = 0; i < (ROW_WIDTH / interval); i++) {
              ts.updateMsEpoch(start_timestamp + (i * interval) + (t * ROW_WIDTH));
              dp.reset(ts, t + h + i);
              write(id, dp, null);
            }
          }
        }

      }
    }
  }

  Map<TimeSeriesStringId, MockSpan> getDatabase() {
    return database;
  }
  
  /**
   * An instance of a node spawned by this "db".
   */
  class LocalNode extends AbstractQueryNode implements TimeSeriesDataSource {
    private AtomicInteger sequence_id = new AtomicInteger();
    private AtomicBoolean completed = new AtomicBoolean();
    private QuerySourceConfig config;
    private final net.opentsdb.stats.Span trace_span;
    
    public LocalNode(final QueryPipelineContext context,
                     final String id,
                     final QuerySourceConfig config) {
      super(null, context, id);
      this.config = config;
      if (context.queryContext().stats() != null && 
          context.queryContext().stats().trace() != null) {
        trace_span = context.queryContext().stats().trace().firstSpan()
            .newChild("MockDataStore")
            .start();
      } else {
        trace_span = null;
      }
    }
    
    @Override
    public void fetchNext(final Span span) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Fetching next set of data.");
      }
      try {
        if (completed.get()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Already completed by 'fetchNext()' wsa called.");
          }
          return;
        }
        
        final LocalResult result = new LocalResult(context, this, config, 
            sequence_id.getAndIncrement(), trace_span);
        
        if (thread_pool == null) {
          result.run();
        } else {
          thread_pool.submit(result);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    
    @Override
    public String id() {
      return config.getId();
    }
    
    @Override
    public void close() {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Closing pipeline.");
      }
      if (completed.compareAndSet(false, true)) {
        for (final QueryNode node : upstream) {
          node.onComplete(this, sequence_id.get() - 1, sequence_id.get());
        }
      }
      if (trace_span != null) {
        trace_span.setSuccessTags().finish();
      }
    }

    @Override
    public QueryPipelineContext pipelineContext() {
      return context;
    }
    
    Collection<QueryNode> upstream() {
      return upstream;
    }
    
    @Override
    public void onComplete(final QueryNode downstream, 
                           final long final_sequence,
                           final long total_sequences) {
      throw new UnsupportedOperationException("This is a source node!");
    }

    @Override
    public void onNext(final QueryResult next) {
      throw new UnsupportedOperationException("This is a source node!");
    }

    @Override
    public void onError(final Throwable t) {
      throw new UnsupportedOperationException("This is a source node!");
    }

    @Override
    public QueryNodeConfig config() {
      return config;
    }
  }
  
  class LocalResult implements QueryResult, Runnable {
    final QuerySourceConfig config;
    final QueryPipelineContext context;
    final LocalNode pipeline;
    final long sequence_id;
    final List<TimeSeries> matched_series;
    final net.opentsdb.stats.Span trace_span;
    
    LocalResult(final QueryPipelineContext context, 
                final LocalNode pipeline, 
                final QuerySourceConfig config, 
                final long sequence_id,
                final net.opentsdb.stats.Span trace_span) {
      this.context = context;
      this.pipeline = pipeline;
      this.config = config;
      this.sequence_id = sequence_id;
      matched_series = Lists.newArrayList();
      if (trace_span != null) {
        this.trace_span = pipeline.pipelineContext().queryContext().stats()
            .trace().newSpanWithThread("MockDataStore Query")
            .asChildOf(trace_span)
            .withTag("sequence", sequence_id)
            .start();
      } else {
        this.trace_span = null;
      }
    }
    
    @Override
    public TimeSpecification timeSpecification() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Collection<TimeSeries> timeSeries() {
      return matched_series;
    }
    
    @Override
    public long sequenceId() {
      return sequence_id;
    }
    
    @Override
    public void run() {
      try {
        if (!hasNext(sequence_id)) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Over the end time. done: " + this.pipeline);
          }
          if (pipeline.completed.compareAndSet(false, true)) {
            for (QueryNode node : pipeline.upstream()) {
              node.onComplete(pipeline, sequence_id - 1, sequence_id);
            }
          } else if (LOG.isDebugEnabled()) {
            LOG.debug("ALREADY MARKED COMPLETE!!");
          }
          return;
        }
        
        long start_ts = context.queryContext().mode() == QueryMode.SINGLE ? 
            config.startTime().msEpoch() : 
              config.endTime().msEpoch() - ((sequence_id + 1) * ROW_WIDTH);
        long end_ts = context.queryContext().mode() == QueryMode.SINGLE ? 
            config.endTime().msEpoch() : 
              config.endTime().msEpoch() - (sequence_id * ROW_WIDTH);
  
        if (LOG.isDebugEnabled()) {
          LOG.debug("Running the filter: " + config);
        }
        
        final Filter filter;
        if (config.getQuery() instanceof SemanticQuery) {
          filter = ((SemanticQuery) config.getQuery())
              .getFilter(config.getFilterId());
        } else if (config.getQuery() instanceof TimeSeriesQuery) {
          filter = ((TimeSeriesQuery) config.getQuery())
              .getFilter(config.getFilterId());
        } else {
          throw new UnsupportedOperationException("We don't support " 
              + config.getQuery().getClass() + " yet");
        }
        
        for (final Entry<TimeSeriesStringId, MockSpan> entry : database.entrySet()) {
          if (!config.getMetric().equals(entry.getKey().metric())) {
            continue;
          }
          
          if (filter != null) {
            boolean matched = true;
            for (final TagVFilter tf : filter.getTags()) {
              String tagv = entry.getKey().tags().get(tf.getTagk());
              if (tagv == null) {
                matched = false;
                break;
              }
              
              try {
                if (!tf.match(ImmutableMap.of(tf.getTagk(), tagv)).join()) {
                  matched = false;
                  break;
                }
              } catch (Exception e) {
                throw new RuntimeException("WTF?", e);
              }
            }
            
            if (!matched) {
              continue;
            }
          }
          
          // matched the filters
          TimeSeries iterator = context.queryContext().mode() == 
              QueryMode.SINGLE ? new SlicedTimeSeries() : null;
          int rows = 0;
          for (final MockRow row : entry.getValue().rows) {
            if (row.base_timestamp >= start_ts && 
                row.base_timestamp < end_ts) {
              ++rows;
              if (context.queryContext().mode() == QueryMode.SINGLE) {
                ((SlicedTimeSeries) iterator).addSource(row);
              } else {
                iterator = row;
                break;
              }
            }
          }
          
          if (rows > 0) {
            matched_series.add(iterator);
          }
        }
        
        if (LOG.isDebugEnabled()) {
          LOG.debug("DONE with filtering. " + pipeline + "  Results: " 
              + matched_series.size());
        }
        if (pipeline.completed.get()) {
          return;
        }
        
        switch(context.queryContext().mode()) {
        case SINGLE:
          for (QueryNode node : pipeline.upstream()) {
            try {
              node.onNext(this);
            } catch (Throwable t) {
              LOG.error("Failed to pass results upstream to node: " + node, t);
            }
            
            if (LOG.isDebugEnabled()) {
              LOG.debug("No more data, marking complete");
            }
            if (pipeline.completed.compareAndSet(false, true)) {
              node.onComplete(pipeline, sequence_id, sequence_id + 1);
            }
          }
          break;
        case BOUNDED_CLIENT_STREAM:
          for (QueryNode node : pipeline.upstream()) {
            try {
              node.onNext(this);
            } catch (Throwable t) {
              LOG.error("Failed to pass results upstream to node: " + node, t);
            }
          }
          if (!hasNext(sequence_id + 1)) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Next query wouldn't have any data: " + pipeline);
            }
            if (pipeline.completed.compareAndSet(false, true)) {
              for (QueryNode node : pipeline.upstream()) {
                node.onComplete(pipeline, sequence_id, sequence_id + 1);
              }
            }
          }
          break;
        case CONTINOUS_CLIENT_STREAM:
        case BOUNDED_SERVER_SYNC_STREAM:
        case CONTINOUS_SERVER_SYNC_STREAM:
          for (QueryNode node : pipeline.upstream()) {
            try {
              node.onNext(this);
            } catch (Throwable t) {
              LOG.error("Failed to pass results upstream to node: " + node, t);
            }
          }
          // fetching next or complete handled by the {@link QueryNode#close()} 
          // method or fetched by the client.
          break;
        case BOUNDED_SERVER_ASYNC_STREAM:
          for (QueryNode node : pipeline.upstream()) {
            try {
              node.onNext(this);
            } catch (Throwable t) {
              LOG.error("Failed to pass results upstream to node: " + node, t);
            }
          }
          
          if (!hasNext(sequence_id + 1)) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Next query wouldn't have any data: " + pipeline);
            }
            for (QueryNode node : pipeline.upstream()) {
              node.onComplete(pipeline, sequence_id, sequence_id + 1);
            }
          } else {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Fetching next Seq: " + (sequence_id  + 1) 
                  + " as there is more data: " + pipeline);
            }
            pipeline.fetchNext(null /* TODO */);
          }
          break;
        case CONTINOUS_SERVER_ASYNC_STREAM:
          for (QueryNode node : pipeline.upstream()) {
            try {
              node.onNext(this);
            } catch (Throwable t) {
              LOG.error("Failed to pass results upstream to node: " + node, t);
            }
          }
          
          if (!hasNext(sequence_id + 1)) {
            // Wait for data.
          } else {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Fetching next Seq: " + (sequence_id  + 1) 
                  + " as there is more data: " + pipeline);
            }
            pipeline.fetchNext(null /* TODO */);
          }
        }
      } catch (Exception e) {
        LOG.error("WTF? Shouldn't be here", e);
      }
    }

    boolean hasNext(final long seqid) {
      long end_ts = context.queryContext().mode() == QueryMode.SINGLE ? 
          config.endTime().msEpoch() : 
            config.endTime().msEpoch() - (seqid * ROW_WIDTH);
      
      if (end_ts <= config.startTime().msEpoch()) {
        return false;
      }
      return true;
    }
    
    @Override
    public QueryNode source() {
      return pipeline;
    }
    
    @Override
    public TypeToken<? extends TimeSeriesId> idType() {
      return Const.TS_STRING_ID;
    }
    
    @Override
    public ChronoUnit resolution() {
      return ChronoUnit.MILLIS;
    }
    
    @Override
    public RollupConfig rollupConfig() {
      // TODO Auto-generated method stub
      return null;
    }
    
    @Override
    public void close() {
      if (trace_span != null) {
        trace_span.setSuccessTags().finish();
      }
      if (context.queryContext().mode() == QueryMode.BOUNDED_SERVER_SYNC_STREAM || 
          context.queryContext().mode() == QueryMode.CONTINOUS_SERVER_SYNC_STREAM) {
        if (!hasNext(sequence_id + 1)) {
          if (context.queryContext().mode() == QueryMode.BOUNDED_SERVER_SYNC_STREAM) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Next query wouldn't have any data: " + pipeline);
            }
            for (QueryNode node : pipeline.upstream()) {
              node.onComplete(pipeline, sequence_id, sequence_id + 1);
            }
          }
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Result closed, firing the next!: " + pipeline);
          }
          pipeline.fetchNext(null /* TODO */);
        }
      }
    }
  }
  
  @Override
  public Deferred<TimeSeriesStringId> resolveByteId(final TimeSeriesByteId id,
                                                    final Span span) {
    return Deferred.fromError(new UnsupportedOperationException());
  }

  
}
