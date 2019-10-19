// This file is part of OpenTSDB.
// Copyright (C) 2019 The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.query.processor.downsample;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.Aggregator;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.Op;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.data.types.numeric.aggregators.ArrayCountFactory.ArrayCount;
import net.opentsdb.data.types.numeric.aggregators.NumericArrayAggregator;
import net.opentsdb.data.types.numeric.aggregators.NumericArrayAggregatorFactory;
import net.opentsdb.query.QueryIterator;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.processor.downsample.Downsample.DownsampleResult;

import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Date;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Iterator that downsamples data points using an {@link net.opentsdb.data.Aggregator} following
 * various rules:
 * <ul>
 * <li>If {@link DownsampleConfig#getFill()} is enabled, then a value is emitted
 * for every timestamp between {@link DownsampleConfig#start()} and 
 * {@link DownsampleConfig#end()} inclusive. Otherwise only values that are 
 * not null or {@link Double#isNaN()} will be emitted.</li>
 * <li>If the source time series does not have any real values (non-null or filled)
 * or the values are outside of the query bounds set in the config, then the 
 * iterator will false for {@link #hasNext()} even if filling is enabled.</li>
 * <li>Values emitted from this iterator are inclusive of the config 
 * {@link DownsampleConfig#start()} and {@link DownsampleConfig#end()} timestamps.
 * <li> This iterator uses Thread Local data structures to reuse the objects across
 * all the queries in the same thread.
 * <li> {@link #nextPool()} will iterate over all the source data points and 
 * create an array of downsamples based on the aggregator function.
 * <li> The Iterator that calls this downsample iterator will have to pass a {@link Aggregator}
 * to acommodate the partial aggregates from the downsample.
 * <li>Value timestamps emitted from the iterator are aligned to the <b>top</b>
 * of the interval. E.g. if the interval is set to 1 day, then the timestamp will
 * always be the midnight hour at the start of the day and includes values from
 * [midnight of day, midnight of next day). This implies:
 * <ul>
 * <li>If a source timestamp is earlier than the {@link DownsampleConfig#start()}
 * it will not be included in the results even though the query may have a start
 * timestamp earlier than {@link DownsampleConfig#start()} (due to the fact that
 * the config will snap to the earliest interval greater than or equal to the
 * query start timestamp.</li>
 * <li>If a source timestamp is later than the {@link DownsampleConfig#end()}
 * time but is within the interval defined by {@link DownsampleConfig#end()},
 * it <b>will</b> be included in the results.</li>
 * </ul></li>
 * </ul>
 * <p>
 * @since 3.0
 */
public class DownsampleNumericToNumericArrayIterator implements QueryIterator, TimeSeriesValue<NumericArrayType> {

  private static final Logger LOG =
      LoggerFactory.getLogger(DownsampleNumericToNumericArrayIterator.class);
  /** The result we belong to. */
  private final DownsampleResult result;

  /** The downsampler config. */
  private final DownsampleConfig config;

  /** Whether or not the iterator has another real or filled value. */
  private boolean has_next;

  /** The current interval start timestamp. */
  private TimeStamp interval_start;

  /** The current interval end timestamp. */
  private TimeStamp interval_end;

  /** The iterator pulled from the source. */
  private TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator;

  /** The partial aggregation defaults for double values */
  private static final double[] partial_doubles =
      {0.0, 0.0, Double.MAX_VALUE, Double.MIN_VALUE, 0.0, -1.0};

  /** The partial aggregation defaults for long values */
  private static final long[] partial_longs = {0, 0, Long.MAX_VALUE, Long.MIN_VALUE, 0, -1};

  /** The thread local accumulator to hold the raw data points for partial aggregations */
  private static final ThreadLocal<Accumulator> threadLocalAccs =
      ThreadLocal.withInitial(() -> new Accumulator());

  /** The DS to contain partial aggregates based on the aggregation function for double values */
  private static final ThreadLocal<double[]> threadLocalDoubleAggs =
      ThreadLocal.withInitial(() -> Arrays.copyOf(partial_doubles, partial_doubles.length));

  /** The DS to contain partial aggregates based on the aggregation function for long values */
  private static final ThreadLocal<long[]> threadLocalLongAggs =
      ThreadLocal.withInitial(() -> Arrays.copyOf(partial_longs, partial_longs.length));

  /** The downsample query node */
  private QueryNode node;

  /** Number of downsample intervals */
  protected int intervals;

  /**
   * Enums for aggregation functions
   *
   */
  protected static enum AggEnum {
    sum, count, min, max, last, avg;
  }

  /** Enum identifying the aggregation function */
  private final AggEnum aggEnum;

  /** Downsample interval unit */
  private int intervalPart;
  
  /** Numeric aggregator */
  private NumericArrayAggregator agg;

  /**
   * Default ctor. This will seek to the proper source timestamp.
   * 
   * @param node A non-null query node to pull the config from.
   * @param result The result this source is a part of.
   * @param source A non-null source to pull numeric iterators from.
   * @throws IllegalArgumentException if a required argument is missing.
   */
  public DownsampleNumericToNumericArrayIterator(@SuppressWarnings("rawtypes") final QueryNode node,
      final QueryResult result, final TimeSeries source) {
    if (node == null) {
      throw new IllegalArgumentException("Query node cannot be null.");
    }
    if (result == null) {
      throw new IllegalArgumentException("Result cannot be null.");
    }
    if (source == null) {
      throw new IllegalArgumentException("Source cannot be null.");
    }
    if (node.config() == null) {
      throw new IllegalArgumentException("Node config cannot be null.");
    }
    this.result = (DownsampleResult) result;
    this.node = node;

    config = (DownsampleConfig) node.config();

    this.aggEnum = AggEnum.valueOf(config.getAggregator().toLowerCase());
    interval_start = this.result.start().getCopy();
    interval_end = this.result.end().getCopy();
    intervals = ((DownsampleConfig) node.config()).intervals();
    intervalPart = (int) ((DownsampleConfig) node.config()).interval().get(ChronoUnit.SECONDS);

    final Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> optional =
        source.iterator(NumericType.TYPE);
    if (optional.isPresent()) {
      iterator = optional.get();
    } else {
      iterator = null;
    }

    has_next = iterator != null ? iterator.hasNext() : false;

  }

  @Override
  public boolean hasNext() {
    return has_next;
  }

  /**
   * The pre computed result in array aggregator
   * 
   * @param aggregator
   * @return
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  public TimeSeriesValue<NumericArrayType> nextPool(Aggregator aggregator) {

    has_next = false;

    NumericArrayAggregator numericArrayAggregator = (NumericArrayAggregator) aggregator; // group by
                                                                                         // aggregator
    Accumulator accumulator = DownsampleNumericToNumericArrayIterator.threadLocalAccs.get();
    accumulator.reset();

    double[] localDoubleAggs = DownsampleNumericToNumericArrayIterator.threadLocalDoubleAggs.get();
    long[] localLongAggs = DownsampleNumericToNumericArrayIterator.threadLocalLongAggs.get();

    resetLocalAggs(localDoubleAggs, localLongAggs);

    TimeStamp nextTs = null;

    boolean flush = false;
    int index = 0;
    while (iterator.hasNext()) {
      TimeSeriesValue<NumericType> next = (TimeSeriesValue<NumericType>) iterator.next();

      if (next.timestamp().compare(Op.GTE, this.interval_start)
          && next.timestamp().compare(Op.LT, this.interval_end)) {

        if (nextTs == null) {
          // Use the first valid dp timestamp as the first Timestamp.
          nextTs = next.timestamp().getCopy();
          long nextRoundoffTs =
              next.timestamp().epoch() - (next.timestamp().epoch() % intervalPart) + intervalPart;
          nextTs.updateEpoch(nextRoundoffTs);
        }

        if (next.timestamp().compare(Op.GTE, nextTs)) {
          // flush previous interval once new interval starts
          flush = true;

          // find the index to update in the Aggregator
          index = (int) (((nextTs.epoch() - interval_start.epoch() - intervalPart) / intervalPart)
              % intervals);

          // get the next timestamp in the downsample
          long nextTsRoundoff =
              next.timestamp().epoch() - (next.timestamp().epoch() % intervalPart) + intervalPart;
          nextTs.updateEpoch(nextTsRoundoff);
        }
        
        if (accumulator.isFull() || flush) {
          accumulateDoubles(accumulator, localLongAggs, localDoubleAggs);
          accumulateLongs(accumulator, localLongAggs, localDoubleAggs);

          accumulator.reset();
        }
        if (next.value().isInteger()) {
          accumulator.add(next.value().longValue());
        } else {
          accumulator.add(next.value().doubleValue());
        }

        if (flush) {

          // increment interval in the groupby aggregator
          double v = getAggValue(localDoubleAggs, localLongAggs);

          if (numericArrayAggregator instanceof ArrayCount && this.aggEnum == AggEnum.count) {
            ((ArrayCount) numericArrayAggregator).accumulate(v, index, true);
          } else {
            numericArrayAggregator.accumulate(v, index);
          }
          // reset Local aggregates
          resetLocalAggs(localDoubleAggs, localLongAggs);
          flush = false;
        }

      }
    }
    if (nextTs != null) {
      // capture the last entry in agg interval
      accumulateDoubles(accumulator, localLongAggs, localDoubleAggs);
      accumulateLongs(accumulator, localLongAggs, localDoubleAggs);
      index = (int) (((nextTs.epoch() - interval_start.epoch() - intervalPart) / intervalPart)
          % intervals);
      double v = getAggValue(localDoubleAggs, localLongAggs);
      if (numericArrayAggregator instanceof ArrayCount && this.aggEnum == AggEnum.count) {
        ((ArrayCount) numericArrayAggregator).accumulate(v, index, true);
      } else {
        numericArrayAggregator.accumulate(v, index);
      }
    }

    return null;
  }

  private double getAggValue(double[] localDoubleAggs, long[] localLongAggs) {
    double v = Double.NaN;
    switch (this.aggEnum) {
      case sum:

        // condition in each switch statement instead of a boolean variable to save that additional
        // boolean value storage
        if (localDoubleAggs[5] == 0 && localLongAggs[5] == 0) {

          v = localDoubleAggs[0] + localLongAggs[0];
        } else if (localLongAggs[5] == 0) {
          v = localLongAggs[0];
        } else if (localDoubleAggs[5] == 0) {
          v = localDoubleAggs[0];
        }

        break;
      case count:

        if (localDoubleAggs[5] == 0 && localLongAggs[5] == 0) {
          v = localDoubleAggs[1] + localLongAggs[1];
        } else if (localLongAggs[5] == 0) {
          v = localLongAggs[1];
        } else if (localDoubleAggs[5] == 0) {
          v = localDoubleAggs[1];
        }

        break;
      case min:

        if (localDoubleAggs[5] == 0 && localLongAggs[5] == 0) {
          v = localDoubleAggs[2] < localLongAggs[2] ? localDoubleAggs[2] : localLongAggs[2];
        } else if (localLongAggs[5] == 0) {
          v = localLongAggs[2];
        } else if (localDoubleAggs[5] == 0) {
          v = localDoubleAggs[2];
        }

        break;
      case max:

        if (localDoubleAggs[5] == 0 && localLongAggs[5] == 0) {
          v = localDoubleAggs[3] > localLongAggs[3] ? localDoubleAggs[3] : localLongAggs[3];
        } else if (localLongAggs[5] == 0) {
          v = localLongAggs[3];
        } else if (localDoubleAggs[5] == 0) {
          v = localDoubleAggs[3];
        }

        break;
      case last:

        if (localDoubleAggs[5] == 0 && localLongAggs[5] == 0) {
          // Shouldn't be here
          LOG.error("Both long and double arrays are set, data might be wrong");
          v = localDoubleAggs[4];
        } else if (localLongAggs[5] == 0) {
          v = localLongAggs[4];
        } else if (localDoubleAggs[5] == 0) {
          v = localDoubleAggs[4];
        }

        break;
      case avg:

        if (localDoubleAggs[5] == 0 && localLongAggs[5] == 0) {
          v = (localDoubleAggs[0] + localLongAggs[0]) / (localDoubleAggs[1] + localLongAggs[1]);
        } else if (localLongAggs[5] == 0) {
          v = (double) localLongAggs[0] / (double) localLongAggs[1];
        } else if (localDoubleAggs[5] == 0) {
          v = localDoubleAggs[0] / localDoubleAggs[1];
        }

        break;

      default:
        throw new UnsupportedOperationException(
            "Unsupported aggregator function: " + this.aggEnum.name());
    }
    return v;
  }
  
  private void resetLocalAggs(double[] localDoubleAggs, long[] localLongAggs) {
    for (int i = 0; i < localDoubleAggs.length; i++) {
      localDoubleAggs[i] = partial_doubles[i];
      localLongAggs[i] = partial_longs[i];
    }
  }

  private void accumulateLongs(Accumulator accumulator, long[] localLongAggs,
      double[] localDoubleAggs) {
    if (!accumulator.hasLongValue) {
      return;
    }
    // run accumulator for longs
    for (int i = 0; i < accumulator.longIndex; i++) {

      long v = accumulator.longValues[i];

      switch (this.aggEnum) {
        case sum:
          localLongAggs[0] += v;
          localLongAggs[5] = 0; // dirty flag
          break;
        case count:
          localLongAggs[1]++;
          localLongAggs[5] = 0; // dirty flag
          break;
        case min:
          if (v < localLongAggs[2]) {
            localLongAggs[2] = v;
          }
          localLongAggs[5] = 0; // dirty flag
          break;
        case max:
          if (v > localLongAggs[3]) {
            localLongAggs[3] = v;
          }
          localLongAggs[5] = 0; // dirty flag
          break;
        case last:
          localLongAggs[4] = v;
          localLongAggs[5] = 0; // dirty flag
          localDoubleAggs[5] = -1; // Reset double dirty flag.
          break;
        case avg:
          localLongAggs[0] += v;
          localLongAggs[1]++;
          localLongAggs[5] = 0; // dirty flag
          break;

        default:
          throw new UnsupportedOperationException(
              "Unsupported aggregator function: " + this.aggEnum.name());
      }
    }
  }

  private void accumulateDoubles(Accumulator accumulator, long[] localLongAggs,
      double[] localDoubleAggs) {
    if (!accumulator.hasDoubleValue) {
      return;
    }
    // run accumulator for doubles
    for (int i = 0; i < accumulator.doubleIndex; i++) {

      double v = accumulator.doubleValues[i];

      // Ignore NaNs in dowmsampling
      if (Double.isNaN(v)) {
        continue;
      }

      switch (this.aggEnum) {
        case sum:
          localDoubleAggs[0] += v;
          localDoubleAggs[5] = 0; // dirty flag
          break;
        case count:
          localDoubleAggs[1]++;
          localDoubleAggs[5] = 0; // dirty flag

          break;
        case min:
          if (v < localDoubleAggs[2]) {
            localDoubleAggs[2] = v;
          }
          localDoubleAggs[5] = 0; // dirty flag
          break;
        case max:
          if (v > localDoubleAggs[3]) {
            localDoubleAggs[3] = v;
          }
          localDoubleAggs[5] = 0; // dirty flag
          break;
        case last:
          localDoubleAggs[4] = v;
          localDoubleAggs[5] = 0; // dirty flag
          localLongAggs[5] = -1; // Reset long agg
          break;
        case avg:
          localDoubleAggs[0] += v;
          localDoubleAggs[1]++;
          localDoubleAggs[5] = 0; // dirty flag
          break;

        default:
          throw new UnsupportedOperationException(
              "Unsupported aggregator function: " + this.aggEnum.name());
      }
    }
  }

  static class Accumulator {
    private final double[] doubleValues;
    private final long[] longValues;
    private final int size;
    private boolean hasDoubleValue = false;
    private boolean hasLongValue = false;
    private int doubleIndex = 0;
    private int longIndex = 0;

    Accumulator(int size) {
      this.size = size;
      this.doubleValues = new double[size];
      this.longValues = new long[size];
    }

    Accumulator() {
      this.size = 30; // TODO: config this
      this.doubleValues = new double[this.size];
      this.longValues = new long[this.size];
    }

    void add(double value) {
      if (!hasDoubleValue) {
        hasDoubleValue = true;
      }
      doubleValues[doubleIndex++] = value;
    }

    void add(long value) {
      if (!hasLongValue) {
        hasLongValue = true;
      }
      longValues[longIndex++] = value;
    }

    boolean isFull() {
      return longIndex >= size || doubleIndex >= size;
    }

    void reset() {
      hasDoubleValue = false;
      hasLongValue = false;
      doubleIndex = 0;
      longIndex = 0;
    }
  }

  @Override
  public TimeSeriesValue<? extends TimeSeriesDataType> next() {

    final NumericArrayAggregatorFactory factory = node.pipelineContext().tsdb().getRegistry()
        .getPlugin(NumericArrayAggregatorFactory.class, this.aggEnum.name());
    if (factory == null) {
      throw new IllegalArgumentException(
          "No numeric array aggregator factory found for type: " + this.aggEnum.name());
    }
    agg = factory.newAggregator(config.getInfectiousNan());

    double[] nans = new double[config.intervals()];
    Arrays.fill(nans, Double.NaN);
    agg.accumulate(nans);

    nextPool(agg);

    return this;

  }
  
  @Override
  public TypeToken<? extends TimeSeriesDataType> getType() {
    return NumericArrayType.TYPE;
  }

  @Override
  public TimeStamp timestamp() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public NumericArrayType value() {
    return agg;
  }

  @Override
  public TypeToken<NumericArrayType> type() {
    return NumericArrayType.TYPE;
  }

}
