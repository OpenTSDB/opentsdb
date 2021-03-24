// Copyright (C) 2017-2020  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.groupby;

import java.util.Collection;
import java.util.Map;
import java.util.function.Predicate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.configuration.Configuration;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.pools.ArrayObjectPool;
import net.opentsdb.pools.DoubleArrayPool;
import net.opentsdb.pools.LongArrayPool;
import net.opentsdb.pools.ObjectPool;
import net.opentsdb.query.QueryIteratorFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.plan.QueryPlanner;
import net.opentsdb.query.processor.BaseQueryNodeFactory;
import net.opentsdb.utils.BigSmallLinkedBlockingQueue;
import net.opentsdb.utils.TSDBQueryQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating GroupBy iterators, aggregating multiple time series into
 * one.
 * 
 * @since 3.0
 */
public class GroupByFactory extends BaseQueryNodeFactory<GroupByConfig, GroupBy> {

  private static final Logger LOG = LoggerFactory.getLogger(GroupByFactory.class);

  public static final String TYPE = "GroupBy";
  public static final String GROUPBY_QUEUE_THRESHOLD_KEY = "groupby.queue.threshold";
  public static final int DEFAULT_GROUPBY_QUEUE_THRESHOLD = 10_000;

  public static final String GROUPBY_TIMESERIES_PER_JOB_KEY = "groupby.timeseries.perjob";
  public static final int DEFAULT_GROUPBY_TIMESERIES_PER_JOB = 512;

  public static final String GROUPBY_THREAD_COUNT_KEY = "groupby.thread.count";
  public static final int DEFAULT_GROUPBY_THREAD_COUNT = 8;
  
  public static final String GROUPBY_USE_REFS = "groupby.timeseries.references";

  protected ObjectPool job_pool;
  protected ArrayObjectPool long_pool;
  protected ArrayObjectPool double_pool;
  private Configuration configuration;
  protected Predicate<GroupByJob> bigJobPredicate;
  private BigSmallLinkedBlockingQueue<GroupByJob> queue;
  private int threadCount;
  private Thread[] threads;

  public interface GroupByJob extends Runnable {
    public int totalTsCount();
  }

  /**
   * Default ctor. Registers the numeric iterator.
   */
  public GroupByFactory() {
    super();
    registerIteratorFactory(NumericType.TYPE, 
        new NumericIteratorFactory());
    registerIteratorFactory(NumericSummaryType.TYPE, 
        new NumericSummaryIteratorFactory());
    registerIteratorFactory(NumericArrayType.TYPE, 
        new NumericArrayIteratorFactory());
  }
  
  @Override
  public String type() {
    return TYPE;
  }

  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
    this.tsdb = tsdb;
    
    configuration = tsdb.getConfig();
    if (!configuration.hasProperty(GROUPBY_QUEUE_THRESHOLD_KEY)) {
      configuration.register(
          GROUPBY_QUEUE_THRESHOLD_KEY,
          DEFAULT_GROUPBY_QUEUE_THRESHOLD,
          true,
          "threshold to chose small or big queue");

      configuration.register(
          GROUPBY_THREAD_COUNT_KEY,
          DEFAULT_GROUPBY_THREAD_COUNT,
          false,
          "group by worker thread count");
  
      configuration.register(
          GROUPBY_TIMESERIES_PER_JOB_KEY,
          DEFAULT_GROUPBY_TIMESERIES_PER_JOB,
          true,
          "maximum number of timeseries per group by job");
      
      configuration.register(GROUPBY_USE_REFS,
          false,
          true,
          "Whether or not to use references into the time series list to reduce "
          + "the number of objects outstanding. Testing this setting.");
    }
    
    // look up the configuration object every time for hot deployment
    bigJobPredicate =
        groupByJob -> groupByJob.totalTsCount() > 
        configuration.getInt(GROUPBY_QUEUE_THRESHOLD_KEY);

    queue = new BigSmallLinkedBlockingQueue<>(tsdb, "groupby", bigJobPredicate);
    threadCount = configuration.getInt(GROUPBY_THREAD_COUNT_KEY);
    threads = new Thread[threadCount];

    LOG.info(
        "Initialized Group by factory {}: {} {}: {} {}: {}",
        GROUPBY_QUEUE_THRESHOLD_KEY,
        configuration.getInt(GROUPBY_QUEUE_THRESHOLD_KEY),
        GROUPBY_THREAD_COUNT_KEY,
        threadCount,
        GROUPBY_TIMESERIES_PER_JOB_KEY,
        configuration.getInt(GROUPBY_TIMESERIES_PER_JOB_KEY));

    for (int i = 0; i < threads.length; i++) {
      Thread thread = new Thread(() -> {
        while (true) {
          try {
            queue.take().run();
          } catch (InterruptedException ignored) {
            LOG.error("GroupBy thread interrupted", ignored);
          }catch (Throwable throwable) {
            LOG.error("Error running GroupBy job", throwable);
          }
        }
      }, "GroupBy thread: " + (i + 1));
      thread.start();
      threads[i] = thread;
    }

    long_pool = (ArrayObjectPool) tsdb.getRegistry().getObjectPool(
        LongArrayPool.TYPE);
    double_pool = (ArrayObjectPool) tsdb.getRegistry().getObjectPool(
        DoubleArrayPool.TYPE);
    return Deferred.fromResult(null);
  }

  @Override
  public GroupBy newNode(final QueryPipelineContext context,
                           final GroupByConfig config) {
    if (config == null) {
      throw new IllegalArgumentException("Config cannot be null.");
    }
    return new GroupBy(this, context, config);
  }
  
  @Override
  public GroupBy newNode(final QueryPipelineContext context) {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public GroupByConfig parseConfig(final ObjectMapper mapper,
                                     final TSDB tsdb,
                                     final JsonNode node) {
    return GroupByConfig.parse(mapper, tsdb, node);
  }
  
  @Override
  public void setupGraph(final QueryPipelineContext context, 
                         final GroupByConfig config,
                         final QueryPlanner plan) {
    // TODO Auto-generated method stub
  }

  protected <T extends TimeSeriesDataType> TypedTimeSeriesIterator newTypedIterator(
      final TypeToken<T> type,
      final GroupBy node,
      final QueryResult result,
      final int[] sources,
      final int sources_length) {
    if (type == NumericArrayType.TYPE) {
      return new GroupByNumericArrayIterator(
          node, 
          result, 
          sources, 
          sources_length,
          configuration.getInt(GROUPBY_QUEUE_THRESHOLD_KEY), 
          configuration.getInt(GROUPBY_TIMESERIES_PER_JOB_KEY), 
          threadCount);
    }
    throw new UnsupportedOperationException();
  }
  
  /**
   * The default numeric iterator factory.
   */
  protected class NumericIteratorFactory implements QueryIteratorFactory<GroupBy, NumericType> {

    @Override
    public TypedTimeSeriesIterator newIterator(final GroupBy node,
                                               final QueryResult result,
                                               final Collection<TimeSeries> sources,
                                               final TypeToken<? extends TimeSeriesDataType> type) {
      return new GroupByNumericIterator(node, result, sources);
    }

    @Override
    public TypedTimeSeriesIterator newIterator(final GroupBy node,
                                               final QueryResult result,
                                               final Map<String, TimeSeries> sources,
                                               final TypeToken<? extends TimeSeriesDataType> type) {
      return new GroupByNumericIterator(node, result, sources);
    }

    @Override
    public Collection<TypeToken<? extends TimeSeriesDataType>> types() {
      return NumericType.SINGLE_LIST;
    }
    
  }

  /**
   * Factory for summary iterators.
   */
  protected class NumericSummaryIteratorFactory implements QueryIteratorFactory<GroupBy, NumericSummaryType> {

    @Override
    public TypedTimeSeriesIterator newIterator(final GroupBy node,
                                               final QueryResult result,
                                               final Collection<TimeSeries> sources,
                                               final TypeToken<? extends TimeSeriesDataType> type) {
      if (type == NumericSummaryType.TYPE && node.getDownsampleConfig() != null) {
        return new GroupByNumericSummaryParallelIterator(
            node, 
            result, 
            sources, 
            configuration.getInt(GROUPBY_QUEUE_THRESHOLD_KEY), 
            configuration.getInt(GROUPBY_TIMESERIES_PER_JOB_KEY), 
            threadCount);
      }
      return new GroupByNumericSummaryIterator(node, result, sources);
    }

    @Override
    public TypedTimeSeriesIterator newIterator(final GroupBy node,
                                               final QueryResult result,
                                               final Map<String, TimeSeries> sources,
                                               final TypeToken<? extends TimeSeriesDataType> type) {
      if (type == NumericSummaryType.TYPE && node.getDownsampleConfig() != null) {
        return new GroupByNumericSummaryParallelIterator(
            node, 
            result, 
            sources, 
            configuration.getInt(GROUPBY_QUEUE_THRESHOLD_KEY), 
            configuration.getInt(GROUPBY_TIMESERIES_PER_JOB_KEY), 
            threadCount);
      }
      return new GroupByNumericSummaryIterator(node, result, sources);
    }
    
    @Override
    public Collection<TypeToken<? extends TimeSeriesDataType>> types() {
      return NumericSummaryType.SINGLE_LIST;
    }
  }

  /**
   * Handles array numerics.
   */
  protected class NumericArrayIteratorFactory implements QueryIteratorFactory<GroupBy, NumericArrayType> {

    @Override
    public TypedTimeSeriesIterator newIterator(final GroupBy node,
                                               final QueryResult result,
                                               final Collection<TimeSeries> sources,
                                               final TypeToken<? extends TimeSeriesDataType> type) {
      return new GroupByNumericArrayIterator(
          node, 
          result, 
          sources, 
          configuration.getInt(GROUPBY_QUEUE_THRESHOLD_KEY), 
          configuration.getInt(GROUPBY_TIMESERIES_PER_JOB_KEY), 
          threadCount);
    }

    @Override
    public TypedTimeSeriesIterator newIterator(final GroupBy node,
                                               final QueryResult result,
                                               final Map<String, TimeSeries> sources,
                                               final TypeToken<? extends TimeSeriesDataType> type) {
      return new GroupByNumericArrayIterator(
          node, 
          result, 
          sources, 
          configuration.getInt(GROUPBY_QUEUE_THRESHOLD_KEY), 
          configuration.getInt(GROUPBY_TIMESERIES_PER_JOB_KEY), 
          threadCount);
    }

    @Override
    public Collection<TypeToken<? extends TimeSeriesDataType>> types() {
      return NumericArrayType.SINGLE_LIST;
    }
    
  }

  protected TSDBQueryQueue<GroupByJob> getQueue() {
    return queue;
  }
  
  Predicate<GroupByJob> predicate() {
    return bigJobPredicate;
  }
  
  TSDB tsdb() {
    return tsdb;
  }
  
  ArrayObjectPool longPool() {
    return long_pool;
  }
  
  ArrayObjectPool doublePool() {
    return double_pool;
  }

}
