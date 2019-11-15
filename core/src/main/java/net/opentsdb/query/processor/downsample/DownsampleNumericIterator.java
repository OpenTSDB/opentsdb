// This file is part of OpenTSDB.
// Copyright (C) 2014-2018  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.downsample;

import com.google.common.reflect.TypeToken;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.Op;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.data.types.numeric.aggregators.NumericAggregator;
import net.opentsdb.data.types.numeric.aggregators.NumericAggregatorFactory;
import net.opentsdb.query.QueryIterator;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.interpolation.QueryInterpolator;
import net.opentsdb.query.interpolation.QueryInterpolatorConfig;
import net.opentsdb.query.interpolation.QueryInterpolatorFactory;
import net.opentsdb.query.processor.downsample.Downsample.DownsampleResult;

import java.util.Optional;

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
 * Note that in order to optimistically take advantage of special instruction
 * sets on CPUs, we dump values into an array as we downsample and grow the 
 * array as needed, never shrinking it or deleting it. We assume that values
 * are longs until we encounter a double at which point we switch to an alternate
 * array and copy the longs over. So there is potential here that two big
 * arrays could be created but in generally there should only be a few of these
 * iterators instantiated at any time for a query.
 * <p>
 * This combines the old filling downsampler and downsampler classes from 
 * OpenTSDB 2.x.
 * <p>
 * <b>WARNING:</b> For now, the arrays grow by doubling. That means there's a 
 * potential for eating up a ton of heap if there are massive amounts of values
 * (e.g. nano second data points) in an interval. 
 * TODO - look at a better way of growing the arrays.
 * @since 3.0
 */
public class DownsampleNumericIterator implements QueryIterator {

  /** The result we belong to. */
  private final DownsampleResult result;
  
  /** The downsampler config. */
  private final DownsampleConfig config;
  
  /** The aggregator. */
  private final NumericAggregator aggregator;
  
  /** The source to pull an iterator from. */
  private final TimeSeries source;
  
  /** The interpolator to use for filling missing intervals. */
  private final QueryInterpolator<NumericType> interpolator;
  
  /** The current interval timestamp marking the start of the interval. */
  private TimeStamp interval_ts;
  
  /** Whether or not we're using an expected reporting interval. */
  protected final boolean reporting_average;
  
  /** Whether or not the iterator has another real or filled value. */
  private boolean has_next;
  
  /** The current source value to return on the next call to {@link #next()} */
  private TimeSeriesValue<NumericType> value;
  
  /** The value we'll actually return to a caller. */
  private MutableNumericValue response;
  
  /**
   * Default ctor. This will seek to the proper source timestamp.
   * 
   * @param node A non-null query node to pull the config from.
   * @param result The result this source is a part of.
   * @param source A non-null source to pull numeric iterators from. 
   * @throws IllegalArgumentException if a required argument is missing.
   */
  @SuppressWarnings("unchecked")
  public DownsampleNumericIterator(final QueryNode node, 
                                   final QueryResult result,
                                   final TimeSeries source) {
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
    this.source = source;
    config = (DownsampleConfig) node.config();
    final QueryInterpolatorConfig interpolator_config = config.interpolatorConfig(NumericType.TYPE);
    if (interpolator_config == null) {
      throw new IllegalArgumentException("No interpolator config found for type");
    }
    
    String agg = config.getAggregator();
    if (agg.equalsIgnoreCase("AVG") && config.dpsInInterval() > 0) {
      reporting_average = true;
      agg = "sum";
    } else {
      reporting_average = false;
    }
    
    NumericAggregatorFactory agg_factory = node.pipelineContext().tsdb()
        .getRegistry().getPlugin(NumericAggregatorFactory.class, agg);
    if (agg_factory == null) {
      throw new IllegalArgumentException("No aggregator found for type: " 
          + ((DownsampleConfig) node.config()).getAggregator());
    }
    aggregator = agg_factory.newAggregator(
        ((DownsampleConfig) node.config()).getInfectiousNan());
    
    
    final QueryInterpolatorFactory factory = node.pipelineContext()
        .tsdb().getRegistry().getPlugin(QueryInterpolatorFactory.class, 
                                        interpolator_config.getType());
    if (factory == null) {
      throw new IllegalArgumentException("No interpolator factory found for: " + 
          interpolator_config.getDataType() == null ? "Default" : interpolator_config.getDataType());
    }
    
    final QueryInterpolator<?> interp = factory.newInterpolator(
        NumericType.TYPE, 
        (TypedTimeSeriesIterator<? extends TimeSeriesDataType>) new Downsampler(),
        interpolator_config);
    if (interp == null) {
      throw new IllegalArgumentException("No interpolator implementation found for: " + 
          interpolator_config.getDataType() == null ? "Default" : interpolator_config.getDataType());
    }
    interpolator = (QueryInterpolator<NumericType>) interp;
    interval_ts = this.result.start().getCopy();
    
    if (config.getFill() && !config.getRunAll()) {
      if (!interpolator.hasNext()) {
        has_next = false;
      } else {
        // make sure there is a value within our query interval
        TimeStamp interval_before_last = this.result.end().getCopy();
        this.result.nextTimestamp(interval_before_last);
        if (interpolator.nextReal().compare(Op.GTE, interval_before_last) ||
            interpolator.nextReal().compare(Op.LT, this.result.start())) {
          has_next = false;
        } else {
          value = interpolator.next(interval_ts);
          has_next = true;
        }
      }
    } else {
      if (interpolator.hasNext()) {
        value = interpolator.next(interval_ts);
        while (value != null && (value.value() == null || 
            (!value.value().isInteger() && Double.isNaN(value.value().doubleValue())))) {
          if (interpolator.hasNext()) {
            this.result.nextTimestamp(interval_ts);
            if (interval_ts.compare(Op.GT, this.result.end())) {
              value = null;
              break;
            }
            value = interpolator.next(interval_ts);
          } else {
            value = null;
          }
        }
        
        if (value != null) {
          has_next = true;
        }
      }
    }
    response = new MutableNumericValue();
  }

  @Override
  public boolean hasNext() {
    return has_next;
  }
  
  @Override
  public TimeSeriesValue<? extends TimeSeriesDataType> next() {
    has_next = false;

    response.reset(value);
    result.nextTimestamp(interval_ts);
    
    if (config.getFill() && !config.getRunAll()) {
      value = interpolator.next(interval_ts);
      if (interval_ts.compare(Op.LTE, result.end())) {
        has_next = true;
      }
    } else if (interpolator.hasNext()) {
      value = interpolator.next(interval_ts);
      while (value != null && (value.value() == null || 
          (!value.value().isInteger() && Double.isNaN(value.value().doubleValue())))) {
        if (interpolator.hasNext()) {
          result.nextTimestamp(interval_ts);
          if (interval_ts.compare(Op.GT, result.end())) {
            value = null;
            break;
          }
          value = interpolator.next(interval_ts);
        } else {
          value = null;
        }
      }
      
      if (value != null) {
        has_next = true;
      }
    }
    
    return response;
  }
  
  @Override
  public TypeToken<? extends TimeSeriesDataType> getType() {
    return NumericType.TYPE;
  }
  
  /**
   * A class that actually performs the downsampling calculation on real
   * values from the source timeseries. It's a child class so we share the same
   * reference for the config and source.
   */
  private class Downsampler implements TypedTimeSeriesIterator {
    /** The last data point extracted from the source. */
    private TimeSeriesValue<NumericType> next_dp = null;
    
    /** The data point set and returned by the iterator. */
    private final MutableNumericValue dp;
    
    /** Whether or not we're using the longs array. */
    private boolean longs;
    
    /** An array of long values used when all sources return longs. */
    private long[] long_values;
    
    /** An array of double values used when one or more sources return a double. */
    private double[] double_values;
    
    /** Used as an index into the value arrays at any given iteration. */
    private int value_idx;
    
    /** Whether or not another real value is present. True while at least one 
     * of the time series has a real value. */
    private boolean has_next = false;
    
    /** The current interval start timestamp. */
    private TimeStamp interval_start;
    
    /** The current interval end timestamp. */
    private TimeStamp interval_end;
    
    /** The iterator pulled from the source. */
    private final TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator;
    
    /**
     * Default ctor.
     */
    @SuppressWarnings("unchecked")
    Downsampler() {
      interval_start = result.start().getCopy();
      if (config.getRunAll()) {
        interval_end = result.end().getCopy();
      } else {
        interval_end = result.start().getCopy();
        result.nextTimestamp(interval_end);
      }
      
      final Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> optional =
          source.iterator(NumericType.TYPE);
      if (optional.isPresent()) {
        iterator = optional.get();
      } else {
        iterator = null;
      }
      if (iterator.hasNext()) {
        next_dp = (TimeSeriesValue<NumericType>) iterator.next();
      }
      
      dp = new MutableNumericValue();
      has_next = iterator.hasNext();
      long_values = new long[2];
      
      // blow out anything earlier than the first timestamp
      if (next_dp != null) {
        // out of bounds
        if (next_dp.timestamp().compare(Op.GT, result.end())) {
          next_dp = null;
        }
        
        while (next_dp != null && next_dp.value() != null && 
            next_dp.timestamp().compare(Op.LT, interval_start)) {
          if (iterator.hasNext()) {
            next_dp = (TimeSeriesValue<NumericType>) iterator.next();
          } else {
            next_dp = null;
          }
        }
      }
      
      has_next = next_dp != null;
    }
    
    /**
     * Helper that expands the array as necessary.
     * @param value A value to store.
     */
    private void add(final long value) {
      if (!longs) {
        add((double) value);
        return;
      }
      
      if (value_idx >= long_values.length) {
        final long[] temp = new long[long_values.length * 2];
        System.arraycopy(long_values, 0, temp, 0, long_values.length);
        long_values = temp;
      }
      long_values[value_idx++] = value;
    }
    
    /**
     * Helper that expands the array as necessary.
     * @param value
     */
    private void add(final double value) {
      if (longs) {
        if (double_values == null) {
          double_values = new double[long_values.length];
        } else if (long_values.length > double_values.length) {
          double_values = new double[long_values.length];
        }
        for (int i = 0; i < long_values.length; i++) {
          double_values[i] = (double) long_values[i];
        }
        longs = false;
      }
      
      if (value_idx >= double_values.length) {
        final double[] temp = new double[double_values.length * 2];
        System.arraycopy(double_values, 0, temp, 0, value_idx - 1);
        double_values = temp;
      }
      double_values[value_idx++] = value;
    }
    
    @Override
    public boolean hasNext() {
      return has_next;
    }

    @SuppressWarnings("unchecked")
    @Override
    public TimeSeriesValue<NumericType> next() {
      if (!has_next) {
        throw new RuntimeException("FAIL! NO more data");
      }
      longs = true;
      has_next = false;
      value_idx = 0;
      
      // we only return reals here, so skip empty intervals. Those are handled by
      // the interpolator.
      while (true) {
        if (next_dp == null) {
          break;
        }
        
        if (config.getRunAll() || 
            next_dp.timestamp().compare(Op.LT, interval_end)) {
          // when running through all the dps, make sure we don't go over the 
          // end timestamp of the query.
          if (config.getRunAll() && 
              next_dp.timestamp().compare(Op.GT, interval_end)) {
            next_dp = null;
            break;
          }
          
          if (next_dp.value() != null && !next_dp.value().isInteger() && 
              Double.isNaN(next_dp.value().doubleValue())) {
            if (config.getInfectiousNan()) {
              add(Double.NaN);
            }
          } else if (next_dp.value() != null) {
            if (next_dp.value().isInteger() && longs) {
              add(next_dp.value().longValue());
            } else {
              add(next_dp.value().toDouble());
            }
          }
          
          if (iterator.hasNext()) {
            next_dp = (TimeSeriesValue<NumericType>) iterator.next();
          } else {
            next_dp = null;
          }
        } else if (value_idx == 0) {
          result.nextTimestamp(interval_start);
          result.nextTimestamp(interval_end);
          if (interval_start.compare(Op.GT, result.end())) {
            next_dp = null;
            break;
          }
        } else {
          // we've reached the end of an interval and have data.
          break;
        }
      }
      
      if (value_idx < 1) {
        dp.resetNull(interval_start);
      } else if (longs) {
        dp.resetTimestamp(interval_start);
        aggregator.run(long_values, 0, value_idx, dp);
      } else {
        dp.resetTimestamp(interval_start);
        aggregator.run(double_values, 0, value_idx, false/* TODO -!! */, dp);
      }
      
      if (reporting_average) {
        int count = 0;
        if (long_values != null) {
          count = value_idx;
        } else {
          for (int i = 0; i < value_idx; i++) {
            if (!Double.isNaN(double_values[i])) {
              count++;
            }
          }
        }
        dp.resetValue((double) dp.toDouble() / 
            Math.max(config.dpsInInterval(), count));
      }
      
      result.nextTimestamp(interval_start);
      result.nextTimestamp(interval_end);
      if (interval_start.compare(Op.GT, result.end())) {
        next_dp = null;
      }
      has_next = next_dp != null;
      return dp;
    }
  
    @Override
    public TypeToken<? extends TimeSeriesDataType> getType() {
      return NumericType.TYPE;
    }
  }
}
