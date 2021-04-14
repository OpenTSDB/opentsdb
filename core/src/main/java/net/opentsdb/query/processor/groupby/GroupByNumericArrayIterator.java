// This file is part of OpenTSDB.
// Copyright (C) 2019-2021  The OpenTSDB Authors.
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

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import net.opentsdb.utils.BigSmallLinkedBlockingQueue;
import net.opentsdb.utils.DateTime;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.AggregatingTypedTimeSeriesIterator;
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
import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.pools.PooledObject;
import net.opentsdb.query.QueryIterator;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.query.processor.groupby.GroupByFactory.GroupByJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An iterator for grouping arrays. This should be much faster for numerics than
 * the regular iterative method for arrays, being able to take advantage of the
 * L2 cache.
 *
 * TODO - don't block the thread with the doneSignal.await() call. Use a
 * deferred and handle it asynchronously.
 *
 * @since 3.0
 */
public class GroupByNumericArrayIterator
    implements QueryIterator, TimeSeriesValue<NumericArrayType>/* , Accumulator */ {
  protected static final Logger LOG = LoggerFactory.getLogger(GroupByNumericArrayIterator.class);
  protected static final String BYPASS_QUEUE = "groupby.nai.bypassQueue";
  protected static final String JOBS = "groupby.nai.jobs";
  protected static final String TIME_TAKEN = "groupby.totalTime";
  protected static final String SERIAL_SOURCE = "groupby.nai.serialSource";
  protected static final String THREADS_USED = "groupby.nai.threadsUsed";
  protected static final String TIME_SERIES_COUNT = "groupby.nai.timeseries.count";

  /** The result we belong to. */
  protected final GroupByResult result;

  /** The aggregator. */
  protected NumericArrayAggregator aggregator;

  /**
   * Whether or not another real value is present. True while at least one of the
   * time series has a real value.
   */
  protected volatile boolean has_next = false;

  protected BigSmallLinkedBlockingQueue<GroupByFactory.GroupByJob> blockingQueue;
  protected GroupByFactory groupByFactory;
  protected int timeSeriesPerJob;
  protected final int threadCount;
  
  protected final GroupBy node;
  protected final int[] arraySources;
  protected final int sources_length;
  protected final List<TimeSeries> sources;

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
    this.node = (GroupBy) node;
    this.result = (GroupByResult) result;
    this.sources = sources instanceof List ? (List) sources : Lists.newArrayList(sources);
    this.sources_length = 0;
    this.arraySources = null;
    this.timeSeriesPerJob = timeSeriesPerJob;
    this.threadCount = threadCount;
    this.groupByFactory = (GroupByFactory) ((GroupBy) node).factory();
    this.blockingQueue = (BigSmallLinkedBlockingQueue) groupByFactory.getQueue();
    process();
  }

  public GroupByNumericArrayIterator(final QueryNode node, 
                                     final QueryResult result, 
                                     final int[] sources,
                                     final int sources_length, 
                                     final int queueThreshold, 
                                     final int timeSeriesPerJob, 
                                     final int threadCount) {
    try {
      this.node = (GroupBy) node;
      this.result = (GroupByResult) result;
      this.arraySources = sources;
      this.sources_length = sources_length;
      this.sources = null;
      this.timeSeriesPerJob = timeSeriesPerJob;
      this.threadCount = threadCount;
      this.groupByFactory = (GroupByFactory) ((GroupBy) node).factory();
      this.blockingQueue = (BigSmallLinkedBlockingQueue<GroupByJob>) groupByFactory.getQueue();
      process();
    } catch (Throwable throwable) {
      LOG.error("Error constructing the GroupByNumericArrayIterator", throwable);
      throw new IllegalArgumentException(throwable);
    }
  }
  
  void process() {
    try {
      TSDB tsdb = node.pipelineContext().tsdb();
      final NumericArrayAggregatorFactory factory = tsdb.getRegistry()
              .getPlugin(NumericArrayAggregatorFactory.class,
                      ((GroupByConfig) node.config()).getAggregator());
      if (factory == null) {
        throw new IllegalArgumentException(
                "No aggregator factory found of type: "
                        + ((GroupByConfig) node.config()).getAggregator());
      }

      int size;
      DownsampleConfig downsampleConfig = ((GroupBy) node).getDownsampleConfig();
      if (null == downsampleConfig) {
        TemporalAmount temporalAmount = this.result.downstreamResult()
                .timeSpecification().interval();
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
        agg_config = DefaultArrayAggregatorConfig.newBuilder().setArraySize(size)
                .setInfectiousNaN(((GroupByConfig) node.config()).getInfectiousNan()).build();
      }

      aggregator = (NumericArrayAggregator) factory.newAggregator(agg_config);
      if (aggregator == null) {
        throw new IllegalArgumentException(
                "No aggregator found of type: " + ((GroupByConfig) node.config()).getAggregator());
      }

      int series_count = sources != null ? sources.size() : sources_length;
      // CASE 1 - A single series in our group.
      if (series_count == 1) {
        accumulate(sources != null ? sources.get(0) :
                result.downstreamResult().timeSeries().get(arraySources[0]), aggregator);
        tsdb.getStatsCollector().incrementCounter(BYPASS_QUEUE);
        if (aggregator.end() <= aggregator.offset()) {
          has_next = false;
        } else {
          has_next = true;
        }
        return;
      }

      // CASE 2 - The source feeding us is not able to be accessed in parallel.
      // it _SHOULD_ be but may not be.
      if (!this.result.isSourceProcessInParallel()) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Accumulate in sequence, source size {}", series_count);
        }
        if (sources != null) {
          for (int i = 0; i < sources.size(); i++) {
            accumulate(sources.get(i), aggregator);
          }
        } else {
          for (int i = 0; i < sources_length; i++) {
            accumulate(((GroupByResult) result).downstreamResult().timeSeries().get(i), aggregator);
          }
        }
        tsdb.getStatsCollector().incrementCounter(SERIAL_SOURCE);
        if (aggregator.end() <= aggregator.offset()) {
          has_next = false;
        } else {
          has_next = true;
        }
        return;
      }

      // CASE 3 - We can parallelize so we'll break this up into jobs.
      int job_count = (int) Math.ceil((double) series_count / (double) timeSeriesPerJob);
      final int aggrCount = Math.min(job_count, threadCount);

      NumericArrayAggregator[] valuesCombiner = new NumericArrayAggregator[aggrCount];
      PooledObject[] pooled_arrays = new PooledObject[aggrCount];
      for (int i = 0; i < valuesCombiner.length; i++) {
        valuesCombiner[i] = createAggregator(node, factory, size, agg_config, pooled_arrays, i);
      }

      if (LOG.isTraceEnabled()) {
        LOG.trace("Accumulate in parallel, source size {}", series_count);
      }
      tsdb.getStatsCollector().setGauge(THREADS_USED, aggrCount);
      accumulateInParallel(job_count, valuesCombiner, pooled_arrays);
      if (aggregator.end() <= aggregator.offset()) {
        has_next = false;
      } else {
        has_next = true;
      }
    } catch (Exception e) {
      LOG.error("Unexpected exception", e);
      node.onError(e);
    }
  }

  /**
   * Instantiate an aggregator with pooled arrays if possible.
   * 
   * @param node          The group by node.
   * @param factory       The agg factory to use.
   * @param size          The size of the array we need.
   * @param pooled_arrays The pooled object array to set if used.
   * @param index         The index into the pooled array.
   * @return The instantiated aggregator.
   */
  protected NumericArrayAggregator createAggregator(
      final QueryNode node, 
      final NumericArrayAggregatorFactory factory,
      final int size, 
      final ArrayAggregatorConfig agg_config, 
      final PooledObject[] pooled_arrays, 
      final int index) {
    final NumericArrayAggregator aggregator = (NumericArrayAggregator) factory.newAggregator(agg_config);
    if (aggregator == null) {
      throw new IllegalArgumentException(
          "No aggregator found of type: " + ((GroupByConfig) node.config()).getAggregator());
    }

    if (((GroupByFactory) ((GroupBy) node).factory()).doublePool() != null) {
      pooled_arrays[index] = ((GroupByFactory) ((GroupBy) node).factory()).doublePool().claim(size);
    }
    return aggregator;
  }

  protected void accumulateInParallel(final int jobs,
                                      final NumericArrayAggregator[] combiners,
                                      final PooledObject[] pooled_arrays) {
    final long overall_start = DateTime.nanoTime();
    final int jobCount = Math.min(threadCount, jobs);
    final CountDownLatch doneSignal = new CountDownLatch(jobCount);
    final int length = sources != null ? sources.size() : sources_length;
    final AtomicInteger total_jobs = new AtomicInteger();
    final AtomicReference<Throwable> exception = new AtomicReference<Throwable>();
    
    class GB implements GroupByJob {
      int aggIndex;
      int startIndex;
      int endIndex;
      long timerStart = DateTime.nanoTime();
      int jobs;

      GB(int aggIndex, int startIndex, int endIndex) {
        this.aggIndex = aggIndex;
        this.startIndex = startIndex;
        this.endIndex = endIndex;
      }

      @Override
      public void run() {
        try {
          if (node.pipelineContext().queryContext().isClosed()) {
            doneSignal.countDown();
            return;
          }

          ++jobs;
          boolean big_queue = groupByFactory.predicate().test(this);
          if (big_queue) {
            blockingQueue.recordBigQueueWaitTime(timerStart);
          } else {
            blockingQueue.recordSmallQueueWaitTime(timerStart);
          }

          int end = Math.min(startIndex + timeSeriesPerJob, endIndex);
          for (; startIndex < end; startIndex++) {
            // AGG
            try {
              final TimeSeries source = sources != null ? sources.get(startIndex) :
                result.downstreamResult().timeSeries().get(startIndex);
              if (source == null) {
                exception.compareAndSet(null, new IllegalArgumentException(
                        "Null time series are not allowed in the sources."));
                doneSignal.countDown();
                return;
              }

              final Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> optional = source
                  .iterator(NumericArrayType.TYPE);
              if (optional.isPresent()) {
                // auto-close it.
                try (final TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator = optional.get()) {
                  if (iterator.hasNext()) {
                    if (iterator instanceof AggregatingTypedTimeSeriesIterator) {
                      ((AggregatingTypedTimeSeriesIterator) iterator).next(combiners[aggIndex]);
                    } else {
                      final TimeSeriesValue<NumericArrayType> array = (TimeSeriesValue<NumericArrayType>) iterator
                          .next();
                      if (array.value().end() - array.value().offset() > 0) {
                        if (array.value().isInteger() && array.value().longArray().length > 0) {
                          combiners[aggIndex].accumulate(array.value().longArray(), array.value().offset(),
                              array.value().end());
                        } else if (array.value().doubleArray().length > 0) {
                          combiners[aggIndex].accumulate(array.value().doubleArray(), array.value().offset(),
                              array.value().end());
                        }
                      }
                    }
                  }
                }
              } else {
                LOG.warn("No numeric array at " + startIndex + " for " + source);
              }
            } catch (Throwable t) {
              LOG.error("Unable to accumulate for a timeseries", t);
              node.onError(new QueryExecutionException(t.getMessage(), 0, t));
            }
          }

          startIndex = end;
          if (startIndex < endIndex) {
            timerStart = DateTime.nanoTime();
            blockingQueue.put(this);
          } else {
            total_jobs.addAndGet(jobs);
            doneSignal.countDown();
          }
        } catch (Throwable t) {
          LOG.error("Failure in Group by job.", t);
          exception.compareAndSet(null, t);
          doneSignal.countDown();
        }
      }

      @Override
      public int totalTsCount() {
        return length;
      }

    }

    final int totalTsCount = this.result.getTsCountInQuery();

    try {
      // Here we want to balance the number of threads we eat up vs the amount of
      // work to do. So if we have 8 threads total and 3 jobs, just use the 3.
      // But if we have 16 jobs, we'll wind up creating 8 and let each job schedule
      // the next run after it hits timeSeriesPerJob to allow other jobs to
      // squeak through.
      if (jobCount < threadCount) {
        for (int i = 0; i < jobCount; i++) {
          final int startIndex = i * timeSeriesPerJob; // inclusive
          final int endIndex; // exclusive
          if (i == jobCount - 1) {
            // last job
            endIndex = length;
          } else {
            endIndex = startIndex + timeSeriesPerJob;
          }

          GB job = new GB(i, startIndex, endIndex);
          blockingQueue.put(job);
        }
      } else {
        int startIndex = 0;
        int perJob = length / threadCount;
        for (int i = 0; i < jobCount; i++) {
          final int endIndex;
          if (i + 1 == jobCount) {
            // last one
            endIndex = length;
          } else {
            endIndex = startIndex + perJob;
          }
          GB job = new GB(i, startIndex, endIndex);
          blockingQueue.put(job);
          startIndex += perJob;
        }
      }

      try {
        doneSignal.await();
      } catch (InterruptedException e) {
        LOG.error("GroupBy interrupted", e);
        return;
      }

      Throwable t = exception.get();
      if (t != null) {
        node.onError(t);
        return;
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

      if (LOG.isDebugEnabled()) {
        LOG.debug("Parallel GB and Downsample time for {} timeseries is {} ms", length,
                DateTime.msFromNanoDiff(DateTime.nanoTime(), overall_start));
      }
      node.pipelineContext().tsdb().getStatsCollector().setGauge(TIME_SERIES_COUNT, totalTsCount);
      node.pipelineContext().tsdb().getStatsCollector().incrementCounter(JOBS, total_jobs.get());
      node.pipelineContext().tsdb().getStatsCollector().addTime(TIME_TAKEN,
              DateTime.nanoTime() - overall_start, ChronoUnit.NANOS);
    } catch (Exception e) {
      LOG.error("Unexpected exception", e);
      node.onError(e);
    }
  }

  public void accumulate(final TimeSeries source,
                         final NumericArrayAggregator aggregator) {
    try {
      if (source == null) {
        throw new IllegalArgumentException("Null time series are not "
                + "allowed in the sources.");
      }

      final Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> optional = source
          .iterator(NumericArrayType.TYPE);
      if (optional.isPresent()) {
        // auto-close it.
        try (final TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator = optional.get()) {
          if (iterator.hasNext()) {
            has_next = true;
            if (aggregator != null && iterator instanceof AggregatingTypedTimeSeriesIterator) {
              ((AggregatingTypedTimeSeriesIterator) iterator).next(aggregator);
            } else {
              final TimeSeriesValue<NumericArrayType> array = (TimeSeriesValue<NumericArrayType>) iterator.next();
              if (array.value().end() - array.value().offset() > 0) {
                if (array.value().isInteger()) {
                  if (array.value().longArray().length > 0) {
                    aggregator.accumulate(array.value().longArray(), array.value().offset(), array.value().end());
                  }
                } else if (array.value().doubleArray().length > 0) {
                  aggregator.accumulate(array.value().doubleArray(), array.value().offset(), array.value().end());
                }
              }
            }
          }
        }
      }
    } catch (Throwable t) {
      LOG.error("Unable to accumulate for a timeseries", t);
      throw new QueryExecutionException(t.getMessage(), 0, t);
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