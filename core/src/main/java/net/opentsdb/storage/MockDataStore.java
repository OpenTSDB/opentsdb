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

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import net.opentsdb.data.TypedTimeSeriesIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.auth.AuthState;
import net.opentsdb.common.Const;
import net.opentsdb.configuration.ConfigurationException;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.PartialTimeSeries;
import net.opentsdb.data.PartialTimeSeriesSet;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesSharedTagsAndTimeData;
import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.data.BaseTimeSeriesDatumStringId;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesDatum;
import net.opentsdb.data.TimeSeriesDatumStringId;
import net.opentsdb.data.TimeSeriesDatumStringWrapperId;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.Op;
import net.opentsdb.data.iterators.SlicedTimeSeries;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericLongArrayType;
import net.opentsdb.data.types.numeric.NumericMillisecondShard;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.pools.BaseObjectPoolAllocator;
import net.opentsdb.pools.CloseablePooledObject;
import net.opentsdb.pools.DefaultObjectPoolConfig;
import net.opentsdb.pools.LongArrayPool;
import net.opentsdb.pools.ObjectPool;
import net.opentsdb.pools.ObjectPoolConfig;
import net.opentsdb.pools.PooledObject;
import net.opentsdb.query.AbstractQueryNode;
import net.opentsdb.query.QueryMode;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.TimeSeriesQuery.LogLevel;
import net.opentsdb.query.filter.FilterUtils;
import net.opentsdb.query.filter.QueryFilter;
import net.opentsdb.rollup.DefaultRollupConfig;
import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.stats.Span;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.JSON;
import net.opentsdb.utils.Pair;

/**
 * A simple store that generates a set of time series to query as well as stores
 * new values (must be written in time order) all in memory. It's meant for
 * testing pipelines and benchmarking.
 * 
 * @since 3.0
 */
public class MockDataStore implements WritableTimeSeriesDataStore {
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
  private final RollupConfig rollup_config;
  
  /** The super inefficient and thread unsafe in-memory db. */
  private Map<TimeSeriesDatumStringId, MockSpan> database;
  
  /** Thread pool used by the executions. */
  private final ExecutorService thread_pool;
  
  public MockDataStore(final TSDB tsdb, final String id) {
    this.tsdb = tsdb;
    this.id = id;
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("Intantiating mock data store with ID: " + this.id + "@" + System.identityHashCode(this));
    }
    
    database = Maps.newHashMap();
    generateMockData();
    if (!tsdb.getConfig().hasProperty("MockDataStore.threadpool.enable")) {
      tsdb.getConfig().register("MockDataStore.threadpool.enable", false, 
          false, "Whether or not to execute results in an asynchronous "
              + "thread pool or not.");
    }
    if (tsdb.getConfig().getBoolean("MockDataStore.threadpool.enable")) {
      thread_pool = Executors.newCachedThreadPool();
    } else {
      thread_pool = null;
    }
    if (!tsdb.getConfig().hasProperty("MockDataStore.partials.enable")) {
      tsdb.getConfig().register("MockDataStore.partials.enable", false, 
          false, "Whether or not send the newer PartialTimeSeries upstream.");
    }
    
    // TODO - we may not need this enable flag any more but we do want
    // a configurable mapping between IDs and agg names.
    String key = configKey("rollups.enable");
    if (!tsdb.getConfig().hasProperty(key)) {
      tsdb.getConfig().register(key, false, false, 
          "Whether or not rollups are enabled for this schema.");
    }
    
    key = configKey("rollups.config");
    if (!tsdb.getConfig().hasProperty(key)) {
      tsdb.getConfig().register(key, null, false, 
          "The path to a JSON file containing the rollup configuration.");
    }
    
    key = configKey("rollups.enable");
    final boolean rollups_enabled = tsdb.getConfig().getBoolean(key);
    if (rollups_enabled) {
      key = configKey("rollups.config");
      String value = tsdb.getConfig().getString(key);
      if (Strings.isNullOrEmpty(value)) { 
        throw new ConfigurationException("Null value for config key: " + key);
      }
      
      if (value.endsWith(".json")) {
        try {
          value = Files.toString(new File(value), Const.UTF8_CHARSET);
        } catch (IOException e) {
          throw new IllegalArgumentException("Failed to open conf file: " 
              + value, e);
        }
      }
      rollup_config = JSON.parseToObject(value, DefaultRollupConfig.class);
    } else {
      rollup_config = null;
    }
  }
  
  public Deferred<WriteStatus> write(final AuthState state, 
                                     final TimeSeriesDatum datum, 
                                     final Span span) {
    MockSpan data_span = database.get((TimeSeriesDatumStringId) datum.id());
    if (data_span == null) {
      data_span = new MockSpan((TimeSeriesDatumStringId) datum.id());
      database.put((TimeSeriesDatumStringId) datum.id(), data_span);
    }
    data_span.addValue(datum.value());
    return Deferred.fromResult(WriteStatus.OK);
  }
  
  public Deferred<List<WriteStatus>> write(
      final AuthState state, 
      final TimeSeriesSharedTagsAndTimeData data, 
      final Span span) {
    int i = 0;
    for (final TimeSeriesDatum datum : data) {
      MockSpan data_span = database.get(datum.id());
      if (data_span == null) {
        data_span = new MockSpan((TimeSeriesDatumStringId) datum.id());
        database.put((TimeSeriesDatumStringId) datum.id(), data_span);
      }
      data_span.addValue(datum.value());
      i++;
    }
    final List<WriteStatus> states = Lists.newArrayListWithExpectedSize(i);
    for (int x = 0; x < i; x++) {
      states.add(WriteStatus.OK);
    }
    return Deferred.fromResult(states);
  }
  
  class MockSpan {
    private List<MockRow> rows = Lists.newArrayList();
    private final TimeSeriesDatumStringId id;
    
    public MockSpan(final TimeSeriesDatumStringId id) {
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
    public Map<TypeToken<? extends TimeSeriesDataType>, TimeSeries> sources;
    
    public MockRow(final TimeSeriesDatumStringId id, 
                   final TimeSeriesValue<?> value) {
      this.id = TimeSeriesDatumStringWrapperId.wrap((TimeSeriesDatumStringId) id);
      base_timestamp = value.timestamp().msEpoch() - 
          (value.timestamp().msEpoch() % ROW_WIDTH);
      sources = Maps.newHashMap();
      // TODO - other types
      if (value.type() == NumericType.TYPE) {
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
    public Optional<TypedTimeSeriesIterator> iterator(
        final TypeToken<? extends TimeSeriesDataType> type) {
      // TODO - other types
      if (type == NumericType.TYPE) {
        return Optional.of(sources.get(NumericType.TYPE).iterator(NumericType.TYPE).get());
      }
      return Optional.empty();
    }

    @Override
    public Collection<TypedTimeSeriesIterator> iterators() {
      // TODO - other types
      final List<TypedTimeSeriesIterator> its =
          Lists.newArrayListWithCapacity(1);
      its.add(sources.get(NumericType.TYPE).iterator(NumericType.TYPE).get());
      return its;
    }

    @Override
    public Collection<TypeToken<? extends TimeSeriesDataType>> types() {
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
            TimeSeriesDatumStringId id = BaseTimeSeriesDatumStringId.newBuilder()
                .setMetric(metric)
                .addTags("dc", dc)
                .addTags("host", String.format("web%02d", h + 1))
                .build();
            MutableNumericValue dp = new MutableNumericValue();
            TimeStamp ts = new MillisecondTimeStamp(0);
            for (long i = 0; i < (ROW_WIDTH / interval); i++) {
              ts.updateMsEpoch(start_timestamp + (i * interval) + (t * ROW_WIDTH));
              dp.reset(ts, t + h + i);
              write(null, new TimeSeriesDatum() {
                
                @Override
                public TimeSeriesDatumStringId id() {
                  return id;
                }

                @Override
                public TimeSeriesValue<? extends TimeSeriesDataType> value() {
                  return dp;
                }
                
              }, null);
            }
          }
        }
      }
    }
  }

  Map<TimeSeriesDatumStringId, MockSpan> getDatabase() {
    return database;
  }
  
  /**
   * An instance of a node spawned by this "db".
   */
  class LocalNode extends AbstractQueryNode implements TimeSeriesDataSource {
    private AtomicInteger sequence_id = new AtomicInteger();
    private AtomicBoolean completed = new AtomicBoolean();
    private TimeSeriesDataSourceConfig config;
    private final net.opentsdb.stats.Span trace_span;
    private final ObjectPool pool;
    
    public LocalNode(final QueryPipelineContext context,
                     final TimeSeriesDataSourceConfig config) {
      super(null, context);
      this.config = config;
      if (context.queryContext().stats() != null && 
          context.queryContext().stats().trace() != null) {
        trace_span = context.queryContext().stats().trace().firstSpan()
            .newChild("MockDataStore")
            .start();
      } else {
        trace_span = null;
      }
      pool = tsdb.getRegistry().getObjectPool(PooledMockPTSPool.TYPE);
    }
    
    @Override
    public void fetchNext(final Span span) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Fetching next set of data: " + config.getId());
      }
      try {
        if (completed.get()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Already completed by 'fetchNext()' wsa called.");
          }
          return;
        }
        
        if (tsdb.getConfig().getBoolean("MockDataStore.partials.enable")) {
          runPartials();
        } else {
          final LocalResult result = new LocalResult(context, this, config, 
              sequence_id.getAndIncrement(), trace_span);
          
          if (thread_pool == null) {
            result.run();
          } else {
            thread_pool.submit(result);
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
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
    
    void runPartials() {
      final TimeStamp st = context.query().startTime().getCopy();
      st.snapToPreviousInterval(3600, ChronoUnit.SECONDS);
      TimeStamp e = context.query().endTime().getCopy();
      e.snapToPreviousInterval(3600, ChronoUnit.SECONDS);
      e.add(Duration.ofSeconds(3600));
      final int total = (int) (e.epoch() - st.epoch()) / 3600;
      
      final ObjectPool array_pool = tsdb.getRegistry().getObjectPool(LongArrayPool.TYPE);
      
      QueryFilter filter = config.getFilter();
      if (filter == null && !Strings.isNullOrEmpty(config.getFilterId())) {
        filter = context.query().getFilter(config.getFilterId());
      }
      
      if (LOG.isDebugEnabled()) {
        LOG.debug("Running the filter: " + filter);
      }
      
      final Map<Long, TimeSeriesId> ids = Maps.newHashMap();
      final Map<Long, Iterator<MockRow>> iterators = Maps.newHashMap();
      final Map<Long, MockRow> rows = Maps.newHashMap();
      
      for (final Entry<TimeSeriesDatumStringId, MockSpan> entry : database.entrySet()) {
        // TODO - handle filter types
        if (!config.getMetric().matches(entry.getKey().metric())) {
          continue;
        }
        
        if (filter != null) {
          if (!FilterUtils.matchesTags(filter, entry.getKey().tags(), Sets.newHashSet())) {
            continue;
          }
        }
        
        // matched the filters
        MockRow row = null;
        Iterator<MockRow> iterator = entry.getValue().rows.iterator();
        while (iterator.hasNext()) {
          row = iterator.next();
          if (row.base_timestamp >= st.msEpoch()) {
            break;
          }
        }
        if (row == null) {
          continue;
        }
        
        if (row.base_timestamp > e.msEpoch()) {
          continue;
        }
        
        final long id_hash = row.id().buildHashCode();
        ids.putIfAbsent(id_hash, row.id());
        
        iterators.put(id_hash, iterator);
        rows.put(id_hash, row);
      }
      
      if (ids.isEmpty()) {
        // nothing found!
        MockPTSSet set = new MockPTSSet(1, 0, ids);
        set.complete = true;
        final PooledMockPTS ppts = (PooledMockPTS) pool.claim().object();
        ppts.setData(null, null, 0, set);
        if (thread_pool != null) {
          thread_pool.submit(new Runnable() {
            @Override
            public void run() {
              sendUpstream(ppts);
            }
          });
        } else {
          sendUpstream(ppts);
        }
        return;
      }
      
      // one hour at a time. Gross huh? That's what happens in a store allocated
      // by time series instead of time.
      int idx = 0;
      long ts = st.msEpoch();
      while (ts < e.msEpoch()) {
        MockPTSSet set = new MockPTSSet(total, idx++, ids);
        Iterator<Entry<Long, Iterator<MockRow>>> iterator = iterators.entrySet().iterator();
        final PooledMockPTS[] last_ppts = new PooledMockPTS[1];
        
        while (iterator.hasNext()) {
          final Entry<Long, Iterator<MockRow>> its = iterator.next();
          MockRow row = rows.get(its.getKey());
          if (row == null) {
            // done
            continue;
          }
          
          if (row.base_timestamp == ts) {
            PooledMockPTS ppts = (PooledMockPTS) pool.claim().object();
            ppts.setData(row, array_pool, its.getKey(), set);
            
            if (last_ppts[0] != null) {
              if (thread_pool != null) {
                set.increment(false);
                final PooledMockPTS pmpts = last_ppts[0];
                thread_pool.submit(new Runnable() {
                  @Override
                  public void run() {
                    sendUpstream(pmpts);
                  }
                });
              } else {
                set.increment(false);
                sendUpstream(last_ppts[0]);
              }
            }
            last_ppts[0] = ppts;
          }
          
          if (its.getValue().hasNext()) {
            rows.put(its.getKey(), its.getValue().next());
          }
        }
        
        if (last_ppts[0] != null) {
          // SUPER IMPORTANT we set it as finished here.
          set.increment(true);
          
          if (thread_pool != null) {
            final PooledMockPTS pmpts = last_ppts[0];
            thread_pool.submit(new Runnable() {
              @Override
              public void run() {
                sendUpstream(pmpts);
              }
            });
          } else {
            sendUpstream(last_ppts[0]);
          }
        }
        
        ts += 3600_000;
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Finished sending data.");
      }
    }
    
    class MockPTSSet implements PartialTimeSeriesSet {
      // Note these two have to be set in the same barrier block.
      private volatile int count;
      private volatile boolean complete;
      
      private final TimeStamp start;
      private TimeStamp end;
      private Map<Long, TimeSeriesId> ids;
      private final int total;
      
      MockPTSSet(final int total, 
                 final long start, 
                 final Map<Long, TimeSeriesId> ids) {
        this.total = total;
        this.start = new SecondTimeStamp(start);
        this.ids = ids;
      }
      
      @Override
      public void close() throws Exception {
        // TODO Auto-generated method stub
        
      }

      @Override
      public int totalSets() {
        return total;
      }

      @Override
      public QueryNode node() {
        return LocalNode.this;
      }

      @Override
      public String dataSource() {
        return config.getId();
      }

      @Override
      public TimeStamp start() {
        return start;
      }

      @Override
      public TimeStamp end() {
        if (end == null) {
          synchronized(this) {
            if (end == null) {
              end = start.getCopy();
              end.add(Duration.ofSeconds(3600));
            }
          }
        }
        return end;
      }

      @Override
      public TimeSeriesId id(long hash) {
        return ids.get(hash);
      }

      @Override
      public synchronized int timeSeriesCount() {
        return count;
      }

      @Override
      public TimeSpecification timeSpecification() {
        // TODO Auto-generated method stub
        return null;
      }
      
      @Override
      public synchronized boolean complete() {
        return complete;
      }
      
      /**
       * Note, super important that this 
       * @param complete
       */
      synchronized void increment(final boolean complete) {
        count++;
        if (complete) {
          this.complete = true;
        }
      }
    }
  }
  
  class LocalResult implements QueryResult, Runnable {
    final TimeSeriesDataSourceConfig config;
    final SemanticQuery query;
    final QueryPipelineContext context;
    final LocalNode pipeline;
    final long sequence_id;
    final List<TimeSeries> matched_series;
    final net.opentsdb.stats.Span trace_span;
    
    LocalResult(final QueryPipelineContext context, 
                final LocalNode pipeline, 
                final TimeSeriesDataSourceConfig config, 
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
      query = (SemanticQuery) context.query();
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
    public String error() {
      return null;
    }
    
    @Override
    public Throwable exception() {
      // TODO - implement
      return null;
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
        
        final long start_ts;
        final long end_ts;
        if (config.timeShifts() == null || 
            !config.timeShifts().containsKey(config.getId())) {
          start_ts = context.queryContext().mode() == QueryMode.SINGLE ? 
              query.startTime().msEpoch() : 
                query.endTime().msEpoch() - ((sequence_id + 1) * ROW_WIDTH);
          end_ts = context.queryContext().mode() == QueryMode.SINGLE ? 
              query.endTime().msEpoch() : 
                query.endTime().msEpoch() - (sequence_id * ROW_WIDTH);
        } else {
          TimeStamp ts = query.startTime().getCopy();
          final Pair<Boolean, TemporalAmount> pair = 
              config.timeShifts().get(config.getId());
          if (pair.getKey()) {
            ts.subtract(pair.getValue());
          } else {
            ts.add(pair.getValue());
          }
          start_ts = ts.msEpoch();
          
          ts = query.endTime().getCopy();
          if (pair.getKey()) {
            ts.subtract(pair.getValue());
          } else {
            ts.add(pair.getValue());
          }
          end_ts = ts.msEpoch();
        }
        
        QueryFilter filter = config.getFilter();
        if (filter == null && !Strings.isNullOrEmpty(config.getFilterId())) {
          filter = context.query().getFilter(config.getFilterId());
        }
        
        if (LOG.isDebugEnabled()) {
          LOG.debug("Running the filter: " + filter);
        }
        
        for (final Entry<TimeSeriesDatumStringId, MockSpan> entry : database.entrySet()) {
          // TODO - handle filter types
          if (!config.getMetric().matches(entry.getKey().metric())) {
            continue;
          }
          
          if (filter != null) {
            if (!FilterUtils.matchesTags(filter, entry.getKey().tags(), null)) {
              continue;
            }
          }
          
          // matched the filters
          TimeSeries iterator = context.queryContext().mode() == 
              QueryMode.SINGLE ? 
                  config.getFetchLast() ? new LastTimeSeries(query) : new SlicedTimeSeries() 
                      : null;
          int rows = 0;
          for (final MockRow row : entry.getValue().rows) {
            if ((row.base_timestamp >= start_ts || 
                row.base_timestamp + ROW_WIDTH >= start_ts) && 
                  row.base_timestamp < end_ts) {
              ++rows;
              if (context.queryContext().mode() == QueryMode.SINGLE) {
                if (config.getFetchLast()) {
                  ((LastTimeSeries) iterator).row = row;
                } else {
                  ((SlicedTimeSeries) iterator).addSource(row);
                }
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
          LOG.debug("[" + MockDataStore.this.id + "@" 
              + System.identityHashCode(MockDataStore.this) 
              + "] DONE with filtering. " + pipeline + "  Results: " 
              + matched_series.size());
        }
        if (context.queryContext().query().getLogLevel().ordinal() >= 
            LogLevel.DEBUG.ordinal()) {
          context.queryContext().logDebug(pipeline, "[" + MockDataStore.this.id 
              + "@" + System.identityHashCode(MockDataStore.this) 
              + "] DONE with filtering. " + pipeline + "  Results: " 
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
              LOG.debug("No more data, marking complete: " + config.getId());
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
        LOG.error("Unexpected exception: " + e.getMessage(), e);
      }
    }

    boolean hasNext(final long seqid) {
      // TODO - handle offsets?
      long end_ts = context.queryContext().mode() == QueryMode.SINGLE ? 
          query.endTime().msEpoch() : 
            query.endTime().msEpoch() - (seqid * ROW_WIDTH);
      
      if (end_ts <= query.startTime().msEpoch()) {
        return false;
      }
      return true;
    }
    
    @Override
    public QueryNode source() {
      return pipeline;
    }

    @Override
    public String dataSource() {
      return config.getId();
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
      return rollup_config;
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
  
  class LastTimeSeries implements TimeSeries {
    SemanticQuery query;
    MockRow row;
    
    LastTimeSeries(final SemanticQuery query) {
      this.query = query;
    }
    
    @Override
    public TimeSeriesId id() {
      return row.id();
    }

    @Override
    public Optional<TypedTimeSeriesIterator> iterator(
        TypeToken<? extends TimeSeriesDataType> type) {
      if (row != null && row.types().contains(type)) {
        return Optional.of(new LocalIterator(type));
      }
      return Optional.empty();
    }

    @Override
    public Collection<TypedTimeSeriesIterator> iterators() {
      final List<TypedTimeSeriesIterator> iterators =
          Lists.newArrayListWithCapacity(row == null ? 0 : row.types().size());
      if (row != null) {
        for (final TypeToken<? extends TimeSeriesDataType> type : row.types()) {
          iterators.add(new LocalIterator(type));
        }
      }
      return iterators;
    }

    @Override
    public Collection<TypeToken<? extends TimeSeriesDataType>> types() {
      return row != null ? row.types() : Collections.emptyList();
    }

    @Override
    public void close() {
    }
    
    class LocalIterator implements TypedTimeSeriesIterator {
      private final TypeToken<? extends TimeSeriesDataType> type;
      boolean has_next = false;
      TimeSeriesValue<?> value;
      
      LocalIterator(final TypeToken<? extends TimeSeriesDataType> type) {
        this.type = type;
        final Optional<TypedTimeSeriesIterator> opt = row.iterator(type);
        if (opt.isPresent()) {
          
          // this is super ugly but it's one way to get the last value
          // before the end time without having to clone values.
          int inc = 0;
          Iterator<TimeSeriesValue<?>> it = opt.get();
          while (it.hasNext()) {
            value = it.next();
            // Offsets don't affect last point right now.
            if (value.timestamp().compare(Op.LTE, query.startTime())) {
              inc++;
            } else {
              break;
            }
          }
          if (value != null) {
            has_next = true;
            it = row.iterator(type).get();
            for (int i = 0; i < inc; i++) {
              value = it.next();
            }
          }
        }
      }
      
      @Override
      public boolean hasNext() {
        return has_next;
      }

      @Override
      public TimeSeriesValue<?> next() {
        has_next = false;
        return value;
      }
      
      @Override
      public TypeToken<? extends TimeSeriesDataType> getType() {
        return type;
      }
    }
  }

  String configKey(final String suffix) {
    return "tsd.storage." + (Strings.isNullOrEmpty(id) || 
        id.equals(MockDataStoreFactory.TYPE) ? "" : id + ".")
          + suffix;
  }

  public static class PooledMockPTS implements PartialTimeSeries, 
      CloseablePooledObject {
    private PooledObject pooled_object;
    private long id_hash;
    private PartialTimeSeriesSet set;
    private AtomicInteger counter;
    private PooledObject pooled_array;
    
    public PooledMockPTS() {
      counter = new AtomicInteger();
    }
    
    @Override
    public void close() throws Exception {
      release();
    }

    @Override
    public Object object() {
      return this;
    }

    @Override
    public void release() {
      if (counter.decrementAndGet() == 0) {
        if (pooled_array != null) {
          pooled_array.release();
          pooled_array = null;
        }
        if (pooled_object != null) {
          pooled_object.release();
          //pooled_object = null;
        }
      }
    }
    
    void setData(final MockRow row, 
                 final ObjectPool long_array_pool, 
                 final long id_hash, 
                 final PartialTimeSeriesSet set) {
      this.id_hash = id_hash;
      this.set = set;
      if (row == null) {
        return;
      }
      
      // TODO - store values in this format
      pooled_array = long_array_pool.claim();
      if (pooled_array == null) {
        throw new IllegalStateException("The pooled array was null!");
      }
      long[] array = (long[]) pooled_array.object();
      int idx = 0;
      Iterator<TimeSeriesValue<?>> it = row.iterator(NumericType.TYPE).get();
      while (it.hasNext()) {
        TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
        if (v.value() == null) {
          continue;
        }
        
        // TODO length check
        if (idx + 2 >= array.length) {
          LOG.error("TODO - need to grow these!");
          throw new UnsupportedOperationException("TODO - mock data "
              + "store needs to be able to grow the arrays.");
        }
        array[idx] = v.timestamp().msEpoch();
        // TODO - maybe a check here to make sure the header isn't set!
        array[idx] |= NumericLongArrayType.MILLISECOND_FLAG;
        if (v.value().isInteger()) {
          idx++;
          array[idx++] = v.value().longValue();
        } else {
          array[idx++] |= NumericLongArrayType.FLOAT_FLAG;
          array[idx++] = Double.doubleToRawLongBits(v.value().doubleValue());
        }
      }
      
      if (idx + 1 >= array.length) {
        LOG.error("TODO - need to grow these!");
        throw new UnsupportedOperationException("TODO - mock data "
            + "store needs to be able to grow the arrays.");
      }
      array[idx] = NumericLongArrayType.TERIMNAL_FLAG;
    }

    @Override
    public void setPooledObject(final PooledObject pooled_object) {
      this.pooled_object = pooled_object;
    }

    @Override
    public long idHash() {
      return id_hash;
    }

    @Override
    public PartialTimeSeriesSet set() {
      return set;
    }

    @Override
    public TypeToken<? extends TimeSeriesDataType> getType() {
      return NumericType.TYPE;
    }
    
    @Override
    public Object data() {
      if (pooled_array != null) {
        counter.incrementAndGet();
        return pooled_array.object();
      } else {
        return null;
      }
    }
    
  }

  public static class PooledMockPTSPool extends BaseObjectPoolAllocator {
    public static final String TYPE = "PooledMockPTS";
    @Override
    public Object allocate() {
      return new PooledMockPTS();
    }

    @Override
    public TypeToken<?> dataType() {
      return TypeToken.of(PooledMockPTS.class);
    }

    @Override
    public String type() {
      return TYPE;
    }

    @Override
    public Deferred<Object> initialize(final TSDB tsdb, final String id) {
      if (Strings.isNullOrEmpty(id)) {
        this.id = TYPE;
      } else {
        this.id = id;
      }
      final ObjectPoolConfig config = DefaultObjectPoolConfig.newBuilder()
          .setAllocator(this)
          .setInitialCount(tsdb.getConfig().getInt(configKey(COUNT_KEY, TYPE)))
          .setMaxCount(tsdb.getConfig().getInt(configKey(COUNT_KEY, TYPE)))
          .setId(this.id)
          .build();
      try {
        createAndRegisterPool(tsdb, config, TYPE);
        return Deferred.fromResult(null);
      } catch (Exception e) {
        return Deferred.fromError(e);
      }
    }
    
  }
}
