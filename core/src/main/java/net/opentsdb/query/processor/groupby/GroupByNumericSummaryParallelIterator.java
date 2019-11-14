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

import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.MutableNumericSummaryValue;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.exceptions.QueryDownstreamException;
import net.opentsdb.query.QueryIterator;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.utils.DateTime;

/**
 * TODO - longs!
 * TODO - pluggable aggs.
 * TODO - interpolators
 *
 */
public class GroupByNumericSummaryParallelIterator implements QueryIterator {

  /** Whether or not NaNs are sentinels or real values. */
  private final boolean infectious_nan;
  
  /** [max_threads][interval]*/
  private final Accumulator[] accumulators;
  
  // TEMP
  static enum AggEnum {
    sum, count, min, max, last, avg;
  }
  
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
  
  /**
   * Default ctor from a map of time series.
   * @param node The non-null owner.
   * @param result A query result to pull the rollup config from.
   * @param sources A non-null map of sources.
   */
  public GroupByNumericSummaryParallelIterator(
      final QueryNode node, 
      final QueryResult result,
      final Map<String, TimeSeries> sources) {
    this(node, result, sources == null ? null : sources.values());
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
      final Collection<TimeSeries> sources) {
    expected_summary = -1;
    
    // TODO
    expect_sums_and_counts = false;
    
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
    accumulators = new Accumulator[GroupByNumericArrayIterator.NUM_THREADS];
    for (int i = 0; i < accumulators.length; i++) {
      accumulators[i] = new Accumulator(i);
    }

    List<Future<Void>> futures = 
        new ArrayList<>(sources.size());
    int i = 0;
    for (final TimeSeries source : sources) {
      int index = (i++) % GroupByNumericArrayIterator.NUM_THREADS;
      final ExecutorService executorService = 
          GroupByNumericArrayIterator.executorList.get(index);
      
      final long s = System.nanoTime();
      Future<Void> future =
          executorService.submit(
              () -> {
                //statsCollector.addTime("groupby.queue.wait.time", System.nanoTime() - s, ChronoUnit.NANOS);
                accumulators[index].accumulate(source);
                return null;
              });

      futures.add(future);
    }
    
    for (Future<Void> future : futures) {
      try {
        future.get(); // get will block until the future is done
      } catch (InterruptedException e) {
        throw new QueryDownstreamException(e.getMessage(), e);
      } catch (ExecutionException e) {
        throw new QueryDownstreamException(e.getMessage(), e);
      }
    }
    
    combine();
  }
  
  @Override
  public boolean hasNext() {
    return idx < results.length;
  }

  @Override
  public TimeSeriesValue<? extends TimeSeriesDataType> next() {
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
  
  void combine() {
    results = new double[intervals];
    Arrays.fill(results, Double.POSITIVE_INFINITY); // sentinel
    
    long[] counts = null;
    if (agg == AggEnum.avg) {
      counts = new long[intervals];
    }
    for (int i = 0; i < accumulators.length; i++) {
      switch (agg) {
      case sum:
      case count:
        for (int x = 0; x < intervals; x++) {
          if (Double.isInfinite(results[x])) {
            results[x] = accumulators[i].accumulator[x];
          } else {
            results[x] += accumulators[i].accumulator[x];
          }
        }
        break;
      case min:
        for (int x = 0; x < intervals; x++) {
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
      case last: // WARNING: Last has no meaning in group by.
        for (int x = 0; x < intervals; x++) {
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
        results[i] = results[i] / counts[i];
      }
    }
  }
  
  class Accumulator {
    final int index;
    double[] accumulator;
    long[] counts;
    
    Accumulator(final int index) {
      this.index = index;
      accumulator = new double[intervals];
      Arrays.fill(accumulator, Double.POSITIVE_INFINITY);
      
      // TODO - nan fill
      if (expect_sums_and_counts || agg == AggEnum.avg) {
        counts = new long[intervals];
      }
    }
    
    void accumulate(final TimeSeries source) {
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
              if (Double.isInfinite(accumulator[bucket])) {
                accumulator[bucket] = v.toDouble();
              } else {
                if (v.toDouble() < accumulator[bucket]) {
                  accumulator[bucket] = v.toDouble();
                }
              }
              break;
            case max:
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
          }
        }
      }
    }
    
  }
  
}