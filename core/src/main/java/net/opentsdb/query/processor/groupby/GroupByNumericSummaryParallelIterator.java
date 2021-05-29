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
import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.MutableNumericSummaryValue;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.query.QueryIterator;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.utils.BigSmallLinkedBlockingQueue;
import net.opentsdb.utils.DateTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * TODO - longs!
 * TODO - pluggable aggs.
 * TODO - interpolators
 *
 */
public class GroupByNumericSummaryParallelIterator implements QueryIterator {
  private static final Logger LOG = LoggerFactory.getLogger(
      GroupByNumericSummaryParallelIterator.class);

  /** Whether or not NaNs are sentinels or real values. */
  private final boolean infectious_nan;
  
  /** [max_threads][interval]*/
  private final Acc[] accumulators;
  private int timeSeriesPerJob;

  // TEMP
  enum AggEnum {
    sum, zimsum, count, min, mimmin, max, mimmax, last, avg;
  }

  private final QueryNode node;
  private final int intervals;
  private final long interval_in_seconds;
  private final TemporalAmount interval;
  private final boolean expect_sums_and_counts;
  private final int start_epoch;
  private int expected_summary;
  private final AggEnum agg;
  private double[] results;
  private final MutableNumericSummaryValue dp;
  private int idx;
  private final TimeStamp ts;
  private final GroupByResult result;
  private final int threadCount;
  private GroupByFactory groupByFactory;
  private BigSmallLinkedBlockingQueue<GroupByFactory.GroupByJob> blockingQueue;
  private int queueThreshold;

  /**
   * Default ctor from a map of time series.
   *
   * @param node The non-null owner.
   * @param result A query result to pull the rollup config from.
   * @param sources A non-null map of sources.
   */
  public GroupByNumericSummaryParallelIterator(
      final QueryNode node,
      final QueryResult result,
      final Map<String, TimeSeries> sources,
      final int queueThreshold,
      final int timeSeriesPerJob,
      final int threadCount) {
    this(node, result, sources == null ? null : Lists.newArrayList(sources.values()), queueThreshold, timeSeriesPerJob, threadCount);
  }

  /**
   * Alternate ctor with a collection of sources.
   * @param node The non-null owner.
   * @param result A query result to pull the rollup config from.
   * @param sources A non-null collection of sources.
   */
  public GroupByNumericSummaryParallelIterator(
      final QueryNode node, 
      final QueryResult result,
      final Collection<TimeSeries> sources,
      final int queueThreshold,
      final int timeSeriesPerJob,
      final int threadCount) {
    expected_summary = -1;
    this.node = node;
    // TODO
    expect_sums_and_counts = false;

    this.result = (GroupByResult) result;

    TSDB tsdb = node.pipelineContext().tsdb();

    this.groupByFactory = (GroupByFactory) ((GroupBy) node).factory();
    this.blockingQueue = (BigSmallLinkedBlockingQueue<GroupByFactory.GroupByJob>) groupByFactory.getQueue();
    this.queueThreshold = queueThreshold;
    this.timeSeriesPerJob = timeSeriesPerJob;

    DownsampleConfig downsampleConfig = ((GroupBy) node).getDownsampleConfig();
    if (null == downsampleConfig) {
      throw new IllegalStateException("Shouldn't be here if the downsample was null.");
    }
    intervals = downsampleConfig.intervals();
    interval = downsampleConfig.interval();
    interval_in_seconds = DateTime.parseDuration(downsampleConfig.getInterval()) / 1000;
    start_epoch = (int) downsampleConfig.startTime().epoch();
    ts = downsampleConfig.startTime().getCopy();
    agg = AggEnum.valueOf(((GroupByConfig) node.config()).getAggregator().toLowerCase());
    infectious_nan = ((GroupByConfig) node.config()).getInfectiousNan();
    dp = new MutableNumericSummaryValue();
    this.threadCount = threadCount;

    // CASE 1 - A single series in our group.
    if (sources.size() == 1) {
      accumulators = new Acc[] { new Acc(0) };
      accumulate(sources);
      return;
    }

    // CASE 2 - The source feeding us is not able to be accessed in parallel.
    // it _SHOULD_ be but may not be.
    if (!this.result.isSourceProcessInParallel()) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Accumulate in sequence, source size {}", sources.size());
      }
      accumulators = new Acc[] { new Acc(0) };
      accumulate(sources);
      return;
    }

    // CASE 3 - We can parallelize so we'll break this up into jobs.
    int job_count = (int) Math.ceil((double) sources.size() / (double) timeSeriesPerJob);
    final int aggrCount = Math.min(job_count, threadCount);
    accumulators = new Acc[aggrCount];
    for (int i = 0; i < accumulators.length; i++) {
      accumulators[i] = new Acc(i);
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("Accumulate in parallel, source size {}", sources.size());
    }
    tsdb.getStatsCollector().setGauge(GroupByNumericArrayIterator.THREADS_USED, aggrCount);
    if (sources instanceof List) {
      accumulateInParallel(aggrCount, (List) sources);
    } else {
      accumulateInParallel(aggrCount, Lists.newArrayList(sources));
    }
  }

  private void accumulateInParallel(final int jobs, final List<TimeSeries> sources) {
    final long overall_start = DateTime.nanoTime();
    final int tsCount = sources.size();
    final int jobCount = Math.min(threadCount, jobs);
    final CountDownLatch doneSignal = new CountDownLatch(jobCount);
    final int totalTsCount = this.result.timeSeries().size();
    final AtomicInteger total_jobs = new AtomicInteger();

    class GB implements GroupByFactory.GroupByJob {
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
              final TimeSeries source = sources.get(startIndex);
              if (source == null) {
                node.onError(new IllegalArgumentException("Null time series are not " +
                        "allowed in the sources."));
                return;
              }

              accumulators[aggIndex].accumulate(source);
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
          doneSignal.countDown();
        }
      }

      @Override
      public int totalTsCount() {
        return tsCount;
      }

    }

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
          endIndex = tsCount;
        } else {
          endIndex = startIndex + timeSeriesPerJob;
        }

        GB job = new GB(i, startIndex, endIndex);
        blockingQueue.put(job);
      }
    } else {
      int startIndex = 0;
      int perJob = tsCount / threadCount;
      for (int i = 0; i < jobCount; i++) {
        final int endIndex;
        if (i + 1 == jobCount) {
          // last one
          endIndex = tsCount;
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
      LOG.error("GroupBy Summary interrupted", e);
    }

    combine();

    if (LOG.isDebugEnabled()) {
      LOG.debug("Parallel GB and Downsample time for {} timeseries is {} ms", tsCount,
              DateTime.msFromNanoDiff(DateTime.nanoTime(), overall_start));
    }

    node.pipelineContext().tsdb().getStatsCollector().setGauge(GroupByNumericArrayIterator.TIME_SERIES_COUNT, totalTsCount);
    node.pipelineContext().tsdb().getStatsCollector().incrementCounter(GroupByNumericArrayIterator.JOBS, total_jobs.get());
    node.pipelineContext().tsdb().getStatsCollector().addTime(GroupByNumericArrayIterator.TIME_TAKEN,
            DateTime.nanoTime() - overall_start, ChronoUnit.NANOS);
  }

  private void accumulate(final Collection<TimeSeries> sources) {
    Iterator<TimeSeries> iterator = sources.iterator();
    while (iterator.hasNext()) {
      TimeSeries series = iterator.next();
      accumulators[0].accumulate(series);
    }

    combine();
  }

  @Override
  public boolean hasNext() {
    return results != null && idx < results.length;
  }

  @Override
  public TimeSeriesValue<? extends TimeSeriesDataType> next() {
    dp.clear();
    dp.resetTimestamp(ts);
    if (Double.isInfinite(results[idx])) {
      dp.resetValue(expected_summary, Double.NaN);
      idx++;
    } else {
      dp.resetValue(expected_summary, results[idx++]);
    }
    ts.add(interval);
    return dp;
  }

  @Override
  public TypeToken<? extends TimeSeriesDataType> getType() {
    return NumericSummaryType.TYPE;
  }
  
  @Override
  public void close() throws IOException {
    // no-op for now
  }
  
  void combine() {
    results = new double[intervals];
    Arrays.fill(results, Double.POSITIVE_INFINITY); // sentinel
    
    long[] counts = null;
    if (agg == AggEnum.avg) {
      counts = new long[intervals];
    }
    boolean had_data = false;
    for (int i = 0; i < accumulators.length; i++) {
      if (!accumulators[i].had_data) {
        continue;
      }

      had_data = true;
      switch (agg) {
      case sum:
      case zimsum:
      case count:
        for (int x = 0; x < intervals; x++) {
          if (Double.isInfinite(accumulators[i].accumulator[x])) {
            continue;
          }
          if (Double.isInfinite(results[x])) {
            results[x] = accumulators[i].accumulator[x];
          } else {
            results[x] += accumulators[i].accumulator[x];
          }
        }
        break;
      case min:
      case mimmin:
        for (int x = 0; x < intervals; x++) {
          if (Double.isInfinite(accumulators[i].accumulator[x])) {
            continue;
          }
          if (Double.isInfinite(results[x])) {
            results[x] = accumulators[i].accumulator[x];
          } else {
            if (accumulators[i].accumulator[x] > results[x]) {
              results[x] = accumulators[i].accumulator[x];
            }
          }
        }
        break;
      case max:
      case mimmax:
      case last: // WARNING: Last has no meaning in group by.
        for (int x = 0; x < intervals; x++) {
          if (Double.isInfinite(accumulators[i].accumulator[x])) {
            continue;
          }
          if (Double.isInfinite(results[x])) {
            results[x] = accumulators[i].accumulator[x];
          } else {
            if (accumulators[i].accumulator[x] < results[x]) {
              results[x] = accumulators[i].accumulator[x];
            }
          }
        }
        break;
      case avg:
        for (int x = 0; x < intervals; x++) {
          if (Double.isInfinite(accumulators[i].accumulator[x])) {
            continue;
          }
          if (Double.isInfinite(results[x])) {
            results[x] = accumulators[i].accumulator[x];
          } else {
            results[x] += accumulators[i].accumulator[x];
          }
          counts[x] += accumulators[i].counts[x];
        }
      }

      accumulators[i] = null; // GC me please!
    }
    
    if (agg == AggEnum.avg) {
      for (int i = 0; i < intervals; i++) {
        if (Double.isInfinite(results[i])) {
          results[i] = Double.NaN;
        } else {
          results[i] = results[i] / counts[i];
        }
      }
    } else {
      for (int i = 0; i < intervals; i++) {
        if (Double.isInfinite(results[i])) {
          results[i] = Double.NaN;
        }
      }
    }
    
    if (!had_data) {
      results = null;
    }
  }

  class Acc {
    final int index;
    double[] accumulator;
    long[] counts;
    boolean had_data;

    Acc(final int index) {
      this.index = index;
      accumulator = new double[intervals];
      Arrays.fill(accumulator, Double.POSITIVE_INFINITY);

      // TODO - nan fill
      if (expect_sums_and_counts || agg == AggEnum.avg) {
        counts = new long[intervals];
      }
    }

    public void accumulate(final TimeSeries source) {
      final Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> op =
          source.iterator(NumericSummaryType.TYPE);
      if (!op.isPresent()) {
        return;
      }

      final TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator = op.get();
      NumericType v;
      while (iterator.hasNext()) {
        final TimeSeriesValue<NumericSummaryType> value =
            (TimeSeriesValue<NumericSummaryType>) iterator.next();
        if (value.value() == null) {
          continue;
        }

        final int bucket = (int) ((value.timestamp().epoch() - start_epoch) / interval_in_seconds);
        if (expect_sums_and_counts) {
          // TODO - no hardcody!
          v = value.value().value(0);
          if (v != null) {
            if (Double.isNaN(v.toDouble())) {
              if (infectious_nan) {
                accumulator[bucket] = Double.NaN;
              }
              continue;
            }

            accumulator[bucket] += v.toDouble();
          }
          // TODO - no hardcody
          v = value.value().value(2);
          if (v != null && v.isInteger()) {
            counts[bucket] += v.longValue();
          }
          had_data = true;
        } else {
          if (expected_summary < 0) {
            expected_summary = value.value().summariesAvailable().iterator().next();
          }

          v = value.value().value(expected_summary);
          if (v != null) {
            if (Double.isNaN(v.toDouble()) && !infectious_nan) {
              continue;
            }

            switch (agg) {
            case sum:
            case zimsum:
              if (Double.isInfinite(accumulator[bucket])) {
                accumulator[bucket] = v.toDouble();
              } else {
                accumulator[bucket] += v.toDouble();
              }
              break;
            case count:
              if (!Double.isNaN(v.toDouble())) {
                if (Double.isInfinite(accumulator[bucket])) {
                  accumulator[bucket] = 1;
                } else {
                  accumulator[bucket]++;
                }
              }
              break;
            case min:
            case mimmin:
              if (Double.isInfinite(accumulator[bucket])) {
                accumulator[bucket] = v.toDouble();
              } else {
                if (v.toDouble() < accumulator[bucket]) {
                  accumulator[bucket] = v.toDouble();
                }
              }
              break;
            case max:
            case mimmax:
            case last:
              if (Double.isInfinite(accumulator[bucket])) {
                accumulator[bucket] = v.toDouble();
              } else {
                if (v.toDouble() > accumulator[bucket]) {
                  accumulator[bucket] = v.toDouble();
                }
              }
              break;
            case avg:
              if (Double.isInfinite(accumulator[bucket])) {
                accumulator[bucket] = v.toDouble();
              } else {
                accumulator[bucket] += v.toDouble();
              }
              counts[bucket]++;
            }
            had_data = true;
          }
        }
      }
    }

  }
  
}