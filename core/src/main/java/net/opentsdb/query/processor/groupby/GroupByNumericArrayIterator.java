// This file is part of OpenTSDB.
// Copyright (C) 2019-2020  The OpenTSDB Authors.
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

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import net.opentsdb.utils.BigSmallLinkedBlockingQueue;
import net.opentsdb.utils.TSDBQueryQueue;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.ArrayAggregatorConfig;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.Op;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.aggregators.DefaultArrayAggregatorConfig;
import net.opentsdb.data.types.numeric.aggregators.NumericArrayAggregator;
import net.opentsdb.data.types.numeric.aggregators.NumericArrayAggregatorFactory;
import net.opentsdb.exceptions.QueryDownstreamException;
import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.pools.PooledObject;
import net.opentsdb.query.QueryIterator;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.query.processor.groupby.GroupByFactory.Accumulator;
import net.opentsdb.query.processor.groupby.GroupByFactory.GroupByJob;
import net.opentsdb.stats.StatsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * An iterator for grouping arrays. This should be much faster for numerics than the regular
 * iterative method for arrays, being able to take advantage of the L2 cache.
 *
 * @since 3.0
 */
public class GroupByNumericArrayIterator
    implements QueryIterator, TimeSeriesValue<NumericArrayType>, Accumulator {
  private static final Logger LOG = LoggerFactory.getLogger(
      GroupByNumericArrayIterator.class);

  protected static final int NUM_THREADS = 8;

  protected static ExecutorService executorService =
      new ThreadPoolExecutor(
          NUM_THREADS, NUM_THREADS, 1L, TimeUnit.SECONDS, new LinkedBlockingQueue<>());

  /** The result we belong to. */
  private final GroupByResult result;

  /** The aggregator. */
  private final NumericArrayAggregator aggregator;

  /**
   * Whether or not another real value is present. True while at least one of
   * the time series has a real value.
   */
  private volatile boolean has_next = false;

  private StatsCollector statsCollector;

  private TSDBQueryQueue<GroupByFactory.GroupByJob> blockingQueue;
  private GroupByFactory groupByFactory;
  private int timeSeriesPerJob;

  /**
   * Default ctor.
   *
   * @param node The non-null node this iterator belongs to.
   * @param result The result this source is a part of.
   * @param sources The non-null and non-empty map of sources.
   * @throws IllegalArgumentException if a required parameter or config is not present.
   */
  public GroupByNumericArrayIterator(
      final QueryNode node,
      final QueryResult result,
      final Map<String, TimeSeries> sources,
      final int queueThreshold,
      final int timeSeriesPerJob,
      final int threadCount) {
    this(
        node,
        result,
        sources == null ? null : Lists.newArrayList(sources.values()),
        queueThreshold,
        timeSeriesPerJob,
        threadCount
    );
  }

  /**
   * Ctor with a collection of source time series.
   *
   * @param node The non-null node this iterator belongs to.
   * @param result The result this source is a part of.
   * @param sources The non-null and non-empty collection or sources.
   * @throws IllegalArgumentException if a required parameter or config is not present.
   */
  public GroupByNumericArrayIterator(
      final QueryNode node, 
      final QueryResult result, 
      final Collection<TimeSeries> sources, 
      final int queueThreshold, 
      final int timeSeriesPerJob, 
      final int threadCount) {
    if (node == null) {
      throw new IllegalArgumentException("Query node cannot be null.");
    }
    if (sources == null) {
      throw new IllegalArgumentException("Sources cannot be null.");
    }
    if (sources.isEmpty()) {
      throw new IllegalArgumentException("Sources cannot be empty.");
    }
    if (Strings.isNullOrEmpty(((GroupByConfig) node.config()).getAggregator())) {
      throw new IllegalArgumentException("Aggregator cannot be null or empty.");
    }

    try {
      TSDB tsdb = node.pipelineContext().tsdb();
      this.groupByFactory = (GroupByFactory) ((GroupBy) node).factory();
      this.blockingQueue = groupByFactory.getQueue();
      this.timeSeriesPerJob = timeSeriesPerJob;
      this.result = (GroupByResult) result;
      final NumericArrayAggregatorFactory factory =
          tsdb
              .getRegistry()
              .getPlugin(
                  NumericArrayAggregatorFactory.class,
                  ((GroupByConfig) node.config()).getAggregator());
      if (factory == null) {
        throw new IllegalArgumentException(
            "No aggregator factory found of type: "
                + ((GroupByConfig) node.config()).getAggregator());
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("Group by queue threshold {}", queueThreshold);
      }

      int size;
      DownsampleConfig downsampleConfig = ((GroupBy) node).getDownsampleConfig();
      if (null == downsampleConfig) {
        TemporalAmount temporalAmount = this.result.downstreamResult().timeSpecification().interval();
        TimeStamp ts = node.pipelineContext().query().startTime().getCopy();
        int intervals = 0;
        while (ts.compare(Op.LT, node.pipelineContext().query().endTime())) {
          intervals++;
          ts.add(temporalAmount);
        }
        size = intervals;
      } else {
        size = downsampleConfig.intervals();
      }
      
      ArrayAggregatorConfig agg_config = ((GroupBy) node).aggregatorConfig();
      if (agg_config == null) {
        agg_config = DefaultArrayAggregatorConfig.newBuilder()
            .setArraySize(size)
            .setInfectiousNaN(((GroupByConfig) node.config()).getInfectiousNan())
            .build();
      }
      
      aggregator = (NumericArrayAggregator) factory.newAggregator(agg_config);
      if (aggregator == null) {
        throw new IllegalArgumentException(
            "No aggregator found of type: " + ((GroupByConfig) node.config()).getAggregator());
      }

      // TODO: Need to check if it makes sense to make this threshold configurable
      final int aggrCount = Math.min(sources.size(), threadCount);
      NumericArrayAggregator[] valuesCombiner = new NumericArrayAggregator[aggrCount];
      PooledObject[] pooled_arrays = new PooledObject[aggrCount];
      for (int i = 0; i < valuesCombiner.length; i++) {
        valuesCombiner[i] = createAggregator(node, factory, size, agg_config, pooled_arrays, i);
      }

      this.statsCollector = tsdb.getStatsCollector();

      if (this.result.isSourceProcessInParallel()) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Accumulate in parallel, source size {}", sources.size());
        }
        if (sources instanceof List) {
          accumulateInParallel((List) sources, valuesCombiner, pooled_arrays);
        } else {
          LOG.debug("Accumulation of type {}", sources.getClass().getName());
          accumulateInParallel(sources, valuesCombiner, pooled_arrays);
        }
      } else {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Accumulate in sequence, source size {}", sources.size());
        }
        for (TimeSeries source : sources) {
          accumulate(source, null);
        }
        
        if (pooled_arrays != null) {
          for (int x = 0; x < pooled_arrays.length; x++) {
            if (pooled_arrays[x] != null) {
              pooled_arrays[x].release();
            }
          }
        }
      }
    } catch (Throwable throwable) {
      LOG.error("Error constructing the GroupByNumericArrayIterator", throwable);
      throw new IllegalArgumentException(throwable);
    }
  }

  /**
   * Instantiate an aggregator with pooled arrays if possible.
   * @param node The group by node.
   * @param factory The agg factory to use.
   * @param size The size of the array we need.
   * @param pooled_arrays The pooled object array to set if used.
   * @param index The index into the pooled array.
   * @return The instantiated aggregator.
   */
  protected NumericArrayAggregator createAggregator(
      final QueryNode node, 
      final NumericArrayAggregatorFactory factory, 
      final int size,
      final ArrayAggregatorConfig agg_config,
      final PooledObject[] pooled_arrays,
      final int index) {
    NumericArrayAggregator aggregator = (NumericArrayAggregator)
        factory.newAggregator(agg_config);
    if (aggregator == null) {
      throw new IllegalArgumentException(
          "No aggregator found of type: " + ((GroupByConfig) node.config()).getAggregator());
    }
    
    final double[] nans;
    if (((GroupByFactory) ((GroupBy) node).factory()).doublePool() != null) {
      pooled_arrays[index] = ((GroupByFactory) ((GroupBy) node).factory()).doublePool().claim(size);
      nans = (double[]) pooled_arrays[index].object();
    } else {
      nans = new double[size];
    }
    return aggregator;
  }

  private void accumulateInParallel(final Collection<TimeSeries> sources, 
                                    final NumericArrayAggregator[] combiners,
                                    final PooledObject[] pooled_arrays) {
    final List<Future<Void>> futures = new ArrayList<>(sources.size());
    int i = 0;

    final long start = System.currentTimeMillis();
    int threadCount = combiners.length;
    for (TimeSeries timeSeries : sources) {
      int index = (i++) % threadCount;
      NumericArrayAggregator combiner = combiners[index];

      final long s = System.nanoTime();
      Future<Void> future =
          executorService.submit(
              () -> {
                statsCollector.addTime("groupby.queue.wait.time", System.nanoTime() - s, ChronoUnit.NANOS);
                accumulate(timeSeries, combiner);
                return null;
              });

      futures.add(future);
      has_next = true;
    }

    statsCollector.setGauge("groupby.timeseries.count", sources.size());

    for (i = 0; i < futures.size(); i++) {
      try {
        futures.get(i).get(); // get will block until the future is done
      } catch (InterruptedException e) {
        LOG.error("Unable to get the status of a task", e);
        throw new QueryDownstreamException(e.getMessage(), e);
      } catch (ExecutionException e) {
        LOG.error("Unable to get status of the task", e.getCause());
        throw new QueryDownstreamException(e.getMessage(), e);
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "Parallel downsample time for {} timeseries is {} ms",
          sources.size(),
          System.currentTimeMillis() - start);
    }

    for (NumericArrayAggregator combiner : combiners) {
      aggregator.combine(combiner);
    }

    if (pooled_arrays != null) {
      for (int x = 0; x < pooled_arrays.length; x++) {
        if (pooled_arrays[x] != null) {
          pooled_arrays[x].release();
        }
      }
    }
  }

  private void accumulateInParallel(
      final List<TimeSeries> tsList, 
      final NumericArrayAggregator[] combiners, 
      final PooledObject[] pooled_arrays) {

    final int tsCount = tsList.size();
    final int jobCount = (int) Math.ceil((double) tsCount / timeSeriesPerJob);

    final long start = System.currentTimeMillis();
    final int totalTsCount = this.result.timeSeries().size();

    final PooledObject[] jobs = new PooledObject[jobCount];
    final CountDownLatch doneSignal = new CountDownLatch(jobCount);
    for (int jobIndex = 0; jobIndex < jobCount; jobIndex++) {
      NumericArrayAggregator combiner = combiners[jobIndex % combiners.length];
      final int startIndex = jobIndex * timeSeriesPerJob; // inclusive
      final int endIndex; // exclusive
      if (jobIndex == jobCount - 1) {
        // last job
        endIndex = tsCount;
      } else {
        endIndex = startIndex + timeSeriesPerJob;
      }

      jobs[jobIndex] = groupByFactory.jobPool().claim();
      final GroupByJob job = (GroupByJob) jobs[jobIndex].object();
      job.reset(tsList, this, totalTsCount, startIndex, endIndex, combiner, doneSignal);
      blockingQueue.put(job);
      has_next = true;
    }

    if (blockingQueue instanceof BigSmallLinkedBlockingQueue) {
      // TODO - maybe not record on every submission, instead track in the queue itself.
      statsCollector.setGauge("groupby.queue.big.job", 
          ((BigSmallLinkedBlockingQueue) blockingQueue).bigQSize());
      statsCollector.setGauge("groupby.queue.small.job", 
          ((BigSmallLinkedBlockingQueue) blockingQueue).smallQSize());
    }
    statsCollector.setGauge("groupby.timeseries.count", totalTsCount);

    try {
      doneSignal.await();
    } catch (InterruptedException e) {
      LOG.error("GroupBy interrupted", e);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "Parallel downsample time for {} timeseries is {} ms",
          tsCount,
          System.currentTimeMillis() - start);
    }
    
    // release the jobs.
    for (int i = 0; i < jobs.length; i++) {
      jobs[i].release();
    }

    for (int i = 0; i < combiners.length; i++) {
      aggregator.combine(combiners[i]);
      try {
        combiners[i].close();
      } catch (IOException e) {
        LOG.error("Failed to close combiner, shouldn't happen", e);
      }
    }
    
    if (pooled_arrays != null) {
      for (int x = 0; x < pooled_arrays.length; x++) {
        if (pooled_arrays[x] != null) {
          pooled_arrays[x].release();
        }
      }
    }
    if (aggregator.end() < 1) {
      has_next = false;
    }
  }

  @Override
  public void accumulate(final TimeSeries source,
                         final NumericArrayAggregator aggregator) {
    try {
      if (source == null) {
        throw new IllegalArgumentException("Null time series are not " 
            + "allowed in the sources.");
      }
      
      final Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> optional =
          source.iterator(NumericArrayType.TYPE);
      if (optional.isPresent()) {
        // auto-close it.
        try (final TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator = 
            optional.get()) {
          if (iterator.hasNext()) {
            has_next = true;
            if (aggregator != null) {
              iterator.nextPool(aggregator);
            } else {
              final TimeSeriesValue<NumericArrayType> array =
                  (TimeSeriesValue<NumericArrayType>) iterator.next();
              accumulate(array);
            }
          }
        }
      }
    } catch (Throwable t) {
      LOG.error("Unable to accumulate for a timeseries", t);
      throw new QueryExecutionException(t.getMessage(), 0, t);
    }
  }

  private void accumulate(final TimeSeriesValue<NumericArrayType> array) {
    if (array.value().end() - array.value().offset() > 0) {
      if (array.value().isInteger()) {
        if (array.value().longArray().length > 0) {
          aggregator.accumulate(array.value().longArray(), array.value().offset(),
              array.value().end());
        }
      } else if (array.value().doubleArray().length > 0) {
        aggregator.accumulate(array.value().doubleArray(), array.value().offset(),
            array.value().end());
      }
    }
  }

  @Override
  public boolean hasNext() {
    return has_next;
  }

  @Override
  public TimeSeriesValue<? extends TimeSeriesDataType> next() {
    has_next = false;
    return this;
  }

  @Override
  public TypeToken<? extends TimeSeriesDataType> getType() {
    return NumericArrayType.TYPE;
  }

  @Override
  public TimeStamp timestamp() {
    return result.downstreamResult().timeSpecification().start();
  }

  @Override
  public NumericArrayType value() {
    return aggregator;
  }

  @Override
  public TypeToken<NumericArrayType> type() {
    return NumericArrayType.TYPE;
  }
  
  @Override
  public void close() {
    try {
      aggregator.close();
    } catch (IOException e) {
      // don't bother logging.
      e.printStackTrace();
    }
  }
  
}