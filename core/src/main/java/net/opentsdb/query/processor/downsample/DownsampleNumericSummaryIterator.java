// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
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

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.Op;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.MutableNumericSummaryValue;
import net.opentsdb.data.types.numeric.NumericAccumulator;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.data.types.numeric.aggregators.NumericAggregator;
import net.opentsdb.data.types.numeric.aggregators.NumericAggregatorFactory;
import net.opentsdb.query.interpolation.QueryInterpolatorFactory;
import net.opentsdb.query.QueryIterator;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.interpolation.QueryInterpolator;
import net.opentsdb.query.interpolation.QueryInterpolatorConfig;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.interpolation.types.numeric.NumericSummaryInterpolatorConfig;
import net.opentsdb.query.processor.downsample.Downsample.DownsampleResult;
import net.opentsdb.rollup.DefaultRollupConfig;

/**
 * Iterator that handles summary values. Note that when the 
 * @since 3.0
 */
public class DownsampleNumericSummaryIterator implements QueryIterator {
  
  /** The result we belong to. */
  private final DownsampleResult result;
  
  /** The downsampler config. */
  private final DownsampleConfig config;
    
  /** The aggregator. */
  private final NumericAggregator aggregator;
  
  /** The source to pull an iterator from. */
  private final TimeSeries source;
  
  /** The interpolator to use for filling missing intervals. */
  private final QueryInterpolator<NumericSummaryType> interpolator;
  
  /** The current interval timestamp marking the start of the interval. */
  private TimeStamp interval_ts;
  
  /** Whether or not the iterator has another real or filled value. */
  private boolean has_next;
  
  /** The config for the interpolator. */
  private final NumericSummaryInterpolatorConfig interpolator_config;
  
  /** The data point returned by this iterator. */
  private MutableNumericSummaryValue dp;
  
  /**
   * Default ctor. This will seek to the proper source timestamp.
   * 
   * @param node A non-null query node to pull the config from.
   * @param result The result this source is a part of.
   * @param source A non-null source to pull numeric iterators from. 
   * @throws IllegalArgumentException if a required argument is missing.
   */
  @SuppressWarnings("unchecked")
  public DownsampleNumericSummaryIterator(final QueryNode node, 
                                          final QueryResult result,
                                          final TimeSeries source) {
    if (node == null) {
      throw new IllegalArgumentException("Query node cannot be null.");
    }
    if (source == null) {
      throw new IllegalArgumentException("Source cannot be null.");
    }
    if (node.config() == null) {
      throw new IllegalArgumentException("Node config cannot be null.");
    }
    this.result = (DownsampleResult) result;
    this.source = source;
    NumericAggregatorFactory agg_factory = node.pipelineContext().tsdb()
        .getRegistry().getPlugin(NumericAggregatorFactory.class, 
            ((DownsampleConfig) node.config()).getAggregator());
    if (agg_factory == null) {
      throw new IllegalArgumentException("No aggregator found for type: " 
          + ((DownsampleConfig) node.config()).getAggregator());
    }
    aggregator = agg_factory.newAggregator(
        ((DownsampleConfig) node.config()).getInfectiousNan());
    config = (DownsampleConfig) node.config();
    
    QueryInterpolatorConfig interpolator_config = config.interpolatorConfig(NumericSummaryType.TYPE);
    if (interpolator_config == null) {
      interpolator_config = config.interpolatorConfig(NumericType.TYPE);
      if (interpolator_config == null) {
        throw new IllegalArgumentException("No interpolator config found for type");
      }
      
      NumericSummaryInterpolatorConfig.Builder nsic = 
          NumericSummaryInterpolatorConfig.newBuilder()
          .setDefaultFillPolicy(((NumericInterpolatorConfig) interpolator_config).getFillPolicy())
          .setDefaultRealFillPolicy(((NumericInterpolatorConfig) interpolator_config).getRealFillPolicy());
      if (config.getAggregator().toLowerCase().equals("avg")) {
        nsic.addExpectedSummary(result.rollupConfig().getIdForAggregator("avg"))
          .setSync(true)
          .setComponentAggregator(
              node.pipelineContext().tsdb()
              .getRegistry().getPlugin(NumericAggregatorFactory.class, 
                  "sum").newAggregator(
                      ((DownsampleConfig) node.config()).getInfectiousNan()));
      } else {
        nsic.addExpectedSummary(result.rollupConfig().getIdForAggregator(
            DefaultRollupConfig.queryToRollupAggregation(config.getAggregator())));
      }
      interpolator_config = nsic
          .setDataType(NumericSummaryType.TYPE.toString())
          .build();
    }
    this.interpolator_config = (NumericSummaryInterpolatorConfig) interpolator_config;
    
    QueryInterpolatorFactory factory = node.pipelineContext().tsdb().getRegistry().getPlugin(QueryInterpolatorFactory.class, 
        interpolator_config.getType());
    if (factory == null) {
      throw new IllegalArgumentException("No interpolator factory found for: " + 
          interpolator_config.getDataType() == null ? "Default" : interpolator_config.getDataType());
    }
    
    QueryInterpolator<?> interp = factory.newInterpolator(
        NumericSummaryType.TYPE, 
        (Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>) new Downsampler(),
        interpolator_config);
    if (interp == null) {
      throw new IllegalArgumentException("No interpolator implementation found for: " + 
          interpolator_config.getDataType() == null ? "Default" : interpolator_config.getDataType());
    }
    interpolator = (QueryInterpolator<NumericSummaryType>) interp;
    interval_ts = this.result.start().getCopy();
    
    // check bounds
    if (interpolator.hasNext()) {
      if (interpolator.nextReal().compare(Op.GTE, this.result.start()) && 
          interpolator.nextReal().compare(Op.LT, this.result.end())) {
        has_next = true;
      }
      
      if (!config.getFill()) {
        // advance to the first real value
        while (interpolator.nextReal().compare(Op.GT, interval_ts)) {
          this.result.nextTimestamp(interval_ts);
        }
      }
    }
    
    dp = new MutableNumericSummaryValue();
  }

  @Override
  public boolean hasNext() {
    return has_next;
  }
  
  @Override
  public TimeSeriesValue<? extends TimeSeriesDataType> next() {
    dp.reset(interpolator.next(interval_ts));
    result.nextTimestamp(interval_ts);
    has_next = false;
    if (config.getFill() && !config.getRunAll()) {
      if (interval_ts.compare(Op.GTE, result.end())) {
        has_next = false;
      } else {
        has_next = true;
      }
    } else if (interpolator.hasNext()) {
      if (interpolator.nextReal().compare(Op.GTE, result.start()) && 
          interpolator.nextReal().compare(Op.LT, result.end())) {
        while (interpolator.nextReal().compare(Op.GT, interval_ts)) {
          result.nextTimestamp(interval_ts);
        }
        has_next = true;
      }
    }
    return dp;
  }
  
  @Override
  public TypeToken<? extends TimeSeriesDataType> getType() {
    return NumericSummaryType.TYPE;
  }
  
  /**
   * A class that actually performs the downsampling calculation on real
   * values from the source timeseries. It's a child class so we share the same
   * reference for the config and source.
   */
  protected class Downsampler implements TypedTimeSeriesIterator {
    /** The last data point extracted from the source. */
    private TimeSeriesValue<NumericSummaryType> next_dp = null;
    
    /** The data point set and returned by the iterator. */
    private final MutableNumericSummaryValue dp;
    
    /** Various accumulators for the different summaries. */
    private final Map<Integer, NumericAccumulator> accumulators;
    
    /** Whether or not another real value is present. True while at least one 
     * of the time series has a real value. */
    private boolean has_next = false;
    
    /** The current interval start timestamp. */
    private TimeStamp interval_start;
    
    /** The current interval end timestamp. */
    private TimeStamp interval_end;
    
    /** The iterator pulled from the source. */
    private final Iterator<TimeSeriesValue<?>> iterator;

    /** IDs cached to avoid lookups per value. */
    private final int sum_id;
    private final int count_id;
    private final int avg_id;
    
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
      sum_id = result.rollupConfig().getIdForAggregator("sum");
      count_id = result.rollupConfig().getIdForAggregator("count");
      avg_id = result.rollupConfig().getIdForAggregator("avg");
      
      final Optional<TypedTimeSeriesIterator> optional = 
          source.iterator(NumericSummaryType.TYPE);
      if (optional.isPresent()) {
        iterator = optional.get();
      } else {
        iterator = null;
        dp = null;
        has_next = false;
        accumulators = null;
        return;
      }
      
      dp = new MutableNumericSummaryValue();

      while (iterator.hasNext()) {
        next_dp = (TimeSeriesValue<NumericSummaryType>) iterator.next();
        if (next_dp != null && 
            next_dp.timestamp().compare(Op.GTE, interval_start) && 
            next_dp.value() != null && 
            next_dp.value().summariesAvailable().size() > 0 &&
            next_dp.timestamp().compare(Op.LT, result.end())) {
          break;
        } else {
          next_dp = null;
        }
      }
      
      has_next = next_dp != null;
      accumulators = Maps.newHashMapWithExpectedSize(2);
    }
    
    @Override
    public boolean hasNext() {
      return has_next;
    }

    @SuppressWarnings("unchecked")
    @Override
    public TimeSeriesValue<NumericSummaryType> next() {
      if (!has_next) {
        throw new RuntimeException("FAIL! NO more data");
      }
      has_next = false;
      resetIndices();
      
      // we only return reals here, so skip empty intervals. Those are handled by
      // the interpolator.
      boolean data_in_iteration = false;
      while (true) {
        if (next_dp == null) {
          break;
        }
        
        if (config.getRunAll() || 
            next_dp.timestamp().compare(config.getRunAll() ? 
                Op.LTE : Op.LT, interval_end)) {
          // when running through all the dps, make sure we don't go over the 
          // end timestamp of the query.
          if (config.getRunAll() && 
              next_dp.timestamp().compare(config.getRunAll() ? 
                  Op.GT : Op.GTE, interval_end)) {
            next_dp = null;
            break;
          }
          
          if (next_dp.value() != null) {
            for (final int summary : next_dp.value().summariesAvailable()) {
              final NumericType value = next_dp.value().value(summary);
              if (value == null) {
                continue;
              }
              
              NumericAccumulator accumulator = accumulators.get(summary);
              if (accumulator == null) {
                accumulator = new NumericAccumulator();
                accumulators.put(summary, accumulator);
              }
             
              if (!value.isInteger() && 
                  Double.isNaN(value.doubleValue())) {
                if (config.getInfectiousNan()) {
                  accumulator.add(Double.NaN);
                }
              } else if (value != null) {
                if (value.isInteger()) {
                  accumulator.add(value.longValue());
                } else {
                  accumulator.add(value.toDouble());
                }
              }
              data_in_iteration = true;
            }
          }
          
          if (iterator.hasNext()) {
            while (iterator.hasNext()) {
              next_dp = (TimeSeriesValue<NumericSummaryType>) iterator.next();
              if (next_dp != null && 
                  next_dp.value() != null && 
                  next_dp.value().summariesAvailable().size() > 0) {
                break;
              } else {
                next_dp = null;
              }
            }
          } else {
            next_dp = null;
          }
        } else if (!data_in_iteration) {
          result.nextTimestamp(interval_start);
          result.nextTimestamp(interval_end);
          if (interval_start.compare(config.getRunAll()? 
              Op.GT : Op.GTE, result.end())) {
            next_dp = null;
            break;
          }
        } else {
          // we've reached the end of an interval and have data.
          break;
        }
      }
      
      if (aggregator.name().toLowerCase().equals("avg")) {
        for (final Entry<Integer, NumericAccumulator> entry : 
            accumulators.entrySet()) {
          final NumericAccumulator accumulator = entry.getValue();
          if (accumulator.valueIndex() > 0) {
            accumulator.run(interpolator_config.componentAggregator() != null ? 
                interpolator_config.componentAggregator() : aggregator, false /* TODO */);
            dp.resetValue(entry.getKey(), (NumericType) accumulator.dp());
          }
        }
        
        // TODO - this is an ugly old hard-coding!!! Make it flexible somehow
        final NumericType sum = dp.value(sum_id);
        final NumericType count = dp.value(count_id);
        dp.clear();
        if (sum == null || count == null) {
          // no-op since one is missing
          // TODO log or count as a metric
        } else {
          dp.resetValue(avg_id, (sum.toDouble() / count.toDouble()));
        }
        dp.resetTimestamp(interval_start);
      } else if (aggregator.name().equals("count")) {
        for (final Entry<Integer, NumericAccumulator> entry : 
            accumulators.entrySet()) {
          final NumericAccumulator accumulator = entry.getValue();
          if (accumulator.valueIndex() > 0) {
            dp.resetValue(entry.getKey(), (NumericType) accumulator.dp());
          }
        }
        dp.resetTimestamp(interval_start);
      } else {
        for (final Entry<Integer, NumericAccumulator> entry : 
            accumulators.entrySet()) {
          final NumericAccumulator accumulator = entry.getValue();
          if (accumulator.valueIndex() < 1) {
            dp.nullSummary(entry.getKey());
          } else {
            if (aggregator.name().equals("count") && 
                entry.getKey() == count_id) {
              accumulator.run(interpolator_config.componentAggregator() != null ? 
                  interpolator_config.componentAggregator() : aggregator, false /* TODO */);
            } else {
              accumulator.run(aggregator, false /** TODO */);
            }
            dp.resetValue(entry.getKey(), (NumericType) accumulator.dp());
          }
        }
        dp.resetTimestamp(interval_start);
      }
      
      result.nextTimestamp(interval_start);
      result.nextTimestamp(interval_end);
      if (interval_start.compare(config.getRunAll() ? 
          Op.GT : Op.GTE, result.end())) {
        next_dp = null;
      }
      has_next = next_dp != null;
      return dp;
    }

    @Override
    public TypeToken<? extends TimeSeriesDataType> getType() {
      return NumericSummaryType.TYPE;
    }
    
    private void resetIndices() {
      for (final NumericAccumulator accumulator : accumulators.values()) {
        accumulator.reset();
      }
      dp.clear();
    }
  }
}