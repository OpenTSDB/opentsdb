// This file is part of OpenTSDB.
// Copyright (C) 2019  The OpenTSDB Authors.
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
import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.Op;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.aggregators.NumericArrayAggregator;
import net.opentsdb.data.types.numeric.aggregators.NumericArrayAggregatorFactory;
import net.opentsdb.exceptions.QueryDownstreamException;
import net.opentsdb.query.QueryIterator;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.stats.StatsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.Arrays;
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
import java.util.function.Predicate;

/**
 * An iterator for grouping arrays. This should be much faster for numerics than the regular
 * iterative method for arrays, being able to take advantage of the L2 cache.
 *
 * @since 3.0
 */
public class GroupByNumericArrayIterator
    implements QueryIterator, TimeSeriesValue<NumericArrayType> {

  private static final Logger logger = LoggerFactory.getLogger(
      GroupByNumericArrayIterator.class);

  public static abstract class GroupByJob<Combiner> implements Runnable {
    private int totalTsCount;
    private final List<TimeSeries> tsList;
    private final int startIndex;
    private final int endIndex;
    private final Combiner combiner;
    private final CountDownLatch doneSignal;
    private StatsCollector statsCollector;
    private final long s = System.nanoTime();

    public GroupByJob(
        int totalTsCount,
        List<TimeSeries> tsList,
        int startIndex,
        int endIndex,
        Combiner combiner,
        CountDownLatch doneSignal,
        StatsCollector statsCollector) {
      this.totalTsCount = totalTsCount;
      this.tsList = tsList;
      this.startIndex = startIndex;
      this.endIndex = endIndex;
      this.combiner = combiner;
      this.doneSignal = doneSignal;
      this.statsCollector = statsCollector;
    }

    @Override
    public void run() {
      boolean isBig = bigJobPredicate.test(this);
      if(isBig) {
        statsCollector.addTime("groupby.queue.big.wait.time", System.nanoTime() - s, ChronoUnit.NANOS);
      } else {
        statsCollector.addTime("groupby.queue.small.wait.time", System.nanoTime() - s, ChronoUnit.NANOS);
      }
      for (int i = startIndex; i < endIndex; i++) {
        doRun(tsList.get(i), combiner);
      }
      doneSignal.countDown();
    }

    /**
     * The aggregating class would have to provide the implementation.
     *
     * @param timeSeries the time series being aggregated.
     * @param combiner is the partial aggregator.
     */
    public abstract void doRun(TimeSeries timeSeries, Combiner combiner);
  }

  //TODO make it configurable
  protected static final int NUM_THREADS = 16;

  //TODO make it configurable
  protected static final int MAX_TS_PER_JOB = 10_000;

  protected static ExecutorService executorService =
      new ThreadPoolExecutor(
          NUM_THREADS, NUM_THREADS, 1L, TimeUnit.SECONDS, new LinkedBlockingQueue<>());

  protected static Thread[] threads = new Thread[NUM_THREADS];

  private  static Predicate<GroupByJob> bigJobPredicate = groupByJob -> groupByJob.totalTsCount > MAX_TS_PER_JOB;
  protected static BigSmallLinkedBlockingQueue<GroupByJob> blockingQueue = new BigSmallLinkedBlockingQueue<>(bigJobPredicate);

  static {
    for (int i = 0; i < threads.length; i++) {
      Thread thread = new Thread(() -> {
        while (true) {
          try {
            blockingQueue.take().run();
          } catch (InterruptedException ignored) {
            logger.error("GroupBy thread interrupted", ignored);
          }catch (Throwable throwable) {
            logger.error("Error running GroupBy job", throwable);
          }
        }
      }, "Group by thread: " + (i + 1));
      thread.setDaemon(true);
      thread.start();
      threads[i] = thread;
    }
  }

  /** The result we belong to. */
  private final GroupByResult result;

  /** The aggregator. */
  private final NumericArrayAggregator aggregator;

  /**
   * Whether or not another real value is present. True while at least one of
   * the time series has a real value.
   */
  private volatile boolean has_next = false;

  protected ExecutorService executor;

  private StatsCollector statsCollector;

  /**
   * Default ctor.
   *
   * @param node The non-null node this iterator belongs to.
   * @param result The result this source is a part of.
   * @param sources The non-null and non-empty map of sources.
   * @throws IllegalArgumentException if a required parameter or config is not present.
   */
  public GroupByNumericArrayIterator(
      final QueryNode node, final QueryResult result, final Map<String, TimeSeries> sources) {
    this(node, result, sources == null ? null : Lists.newArrayList(sources.values()));
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
      final QueryNode node, final QueryResult result, final Collection<TimeSeries> sources) {

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
      executor = tsdb.quickWorkPool();

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
      
      aggregator =
          factory.newAggregator(((GroupByConfig) node.config()).getInfectiousNan());
      if (aggregator == null) {
        throw new IllegalArgumentException(
            "No aggregator found of type: " + ((GroupByConfig) node.config()).getAggregator());
      }

      // TODO: Need to check if it makes sense to make this threshold configurable
      final int threadCount = Math.min(NUM_THREADS, sources.size());
      NumericArrayAggregator[] valuesCombiner = new NumericArrayAggregator[threadCount];
      for (int i = 0; i < valuesCombiner.length; i++) {
        valuesCombiner[i] = createAggregator(node, factory, size);
      }

      this.statsCollector = tsdb.getStatsCollector();

      if (this.result.isSourceProcessInParallel()) {
        if (logger.isTraceEnabled()) {
          logger.trace("Accumulate in parallel, source size {}", sources.size());
        }
        if (sources instanceof List) {
          accumulateInParallel((List) sources, valuesCombiner);
        } else {
          logger.debug("Accumulation of type {}", sources.getClass().getName());
          accumulateInParallel(sources, valuesCombiner);
        }
      } else {
        if (logger.isTraceEnabled()) {
          logger.trace("Accumulate in sequence, source size {}", sources.size());
        }
        for (TimeSeries source : sources) {
          accumulate(source, null);
        }
      }
    } catch (Throwable throwable) {
      logger.error("Error constructing the GroupByNumericArrayIterator", throwable);
      throw new IllegalArgumentException(throwable);
    }
  }

  private NumericArrayAggregator createAggregator(
      final QueryNode node, final NumericArrayAggregatorFactory factory, final int size) {
    NumericArrayAggregator aggregator =
        factory.newAggregator(((GroupByConfig) node.config()).getInfectiousNan());
    if (aggregator == null) {
      throw new IllegalArgumentException(
          "No aggregator found of type: " + ((GroupByConfig) node.config()).getAggregator());
    }
    if(aggregator.isInteger()) {
      aggregator.accumulate(new long[size]);
    } else {
      double[] nans = new double[size];
      Arrays.fill(nans, Double.NaN);
      aggregator.accumulate(nans);
    }
    return aggregator;
  }

  private void accumulateInParallel(final Collection<TimeSeries> sources, 
                                    final NumericArrayAggregator[] combiners) {
    List<Future<TimeSeriesValue<NumericArrayType>>> futures = new ArrayList<>(sources.size());
    int i = 0;

    final long start = System.currentTimeMillis();
    for (TimeSeries timeSeries : sources) {
      int index = (i++) % NUM_THREADS;
      NumericArrayAggregator combiner = combiners[index];

      final long s = System.nanoTime();
      Future<TimeSeriesValue<NumericArrayType>> future =
          executorService.submit(
              () -> {
                statsCollector.addTime("groupby.queue.wait.time", System.nanoTime() - s, ChronoUnit.NANOS);
                return accumulate(timeSeries, combiner);
              });

      futures.add(future);
      has_next = true;
    }

    statsCollector.setGauge("groupby.timeseries.count", sources.size());

    for (Future<TimeSeriesValue<NumericArrayType>> future : futures) {
      try {
        future.get(); // get will block until the future is done
      } catch (InterruptedException e) {
        logger.error("Unable to get the status of a task", e);
        throw new QueryDownstreamException(e.getMessage(), e);
      } catch (ExecutionException e) {
        logger.error("Unable to get status of the task", e.getCause());
        throw new QueryDownstreamException(e.getMessage(), e);
      }
    }

    if (logger.isDebugEnabled()) {
      logger.debug(
          "Parallel downsample time for {} timeseries is {} ms",
          sources.size(),
          System.currentTimeMillis() - start);
    }

    for (NumericArrayAggregator combiner : combiners) {
      aggregator.combine(combiner);
    }

  }

  private void accumulateInParallel(
      final List<TimeSeries> tsList, final NumericArrayAggregator[] combiners) {

    final int tsCount = tsList.size();
    final int threadCount = combiners.length;
    int tsPerJob = tsCount / threadCount;
    if(tsPerJob > MAX_TS_PER_JOB) {
      tsPerJob = MAX_TS_PER_JOB;
    }
    final int jobCount = tsCount / tsPerJob;
    final long start = System.currentTimeMillis();
    final int totalTsCount = this.result.timeSeries().size();

    final CountDownLatch doneSignal = new CountDownLatch(jobCount);
    for (int jobIndex = 0; jobIndex < jobCount; jobIndex++) {
      NumericArrayAggregator combiner = combiners[jobIndex % threadCount];
      final int startIndex = jobIndex * tsPerJob; // inclusive
      final int endIndex; // exclusive
      if (jobIndex == jobCount - 1) {
        // last job
        endIndex = tsCount;
      } else {
        endIndex = startIndex + tsPerJob;
      }

      blockingQueue.put(new GroupByJob<NumericArrayAggregator>(totalTsCount, tsList, startIndex, endIndex, combiner, doneSignal, statsCollector) {
        @Override
        public void doRun(TimeSeries timeSeries, NumericArrayAggregator combiner) {
          accumulate(timeSeries, combiner);
        }
      });
      has_next = true;
    }

    statsCollector.setGauge("groupby.queue.big.job", blockingQueue.bigQSize());
    statsCollector.setGauge("groupby.queue.small.job", blockingQueue.smallQSize());

    try {
      doneSignal.await();
    } catch (InterruptedException e) {
      logger.error("GroupBy interrupted", e);
    }

    if (logger.isDebugEnabled()) {
      logger.debug(
          "Parallel downsample time for {} timeseries is {} ms",
          tsCount,
          System.currentTimeMillis() - start);
    }

    for (NumericArrayAggregator combiner : combiners) {
      aggregator.combine(combiner);
    }
  }

  private TimeSeriesValue<NumericArrayType> accumulate(TimeSeries source,
      NumericArrayAggregator aggregator) {
    try {
      if (source == null) {
        throw new IllegalArgumentException("Null time series are not " + "allowed in the sources.");
      }

      final Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> optional =
          source.iterator(NumericArrayType.TYPE);
      if (optional.isPresent()) {
        final TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator = optional.get();
        if (iterator.hasNext()) {
          has_next = true;
          if (aggregator != null) {
            return (TimeSeriesValue<NumericArrayType>) iterator.nextPool(aggregator);
          } else {
            TimeSeriesValue<NumericArrayType> array =
                (TimeSeriesValue<NumericArrayType>) iterator.next();
            accumulate(array);
            return array;
          }
        }
      }
    } catch (Throwable t) {
      logger.error("Unable to accumulate for a timeseries", t);
    }
    return null;
  }

  private void accumulate(TimeSeriesValue<NumericArrayType> array) {
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
}
