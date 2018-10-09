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
package net.opentsdb.query.processor.merge;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.Op;
import net.opentsdb.data.types.numeric.Aggregators;
import net.opentsdb.data.types.numeric.MutableNumericSummaryValue;
import net.opentsdb.data.types.numeric.NumericAccumulator;
import net.opentsdb.data.types.numeric.NumericAggregator;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.interpolation.QueryInterpolatorFactory;
import net.opentsdb.query.QueryIterator;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.interpolation.QueryInterpolator;
import net.opentsdb.query.interpolation.QueryInterpolatorConfig;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.interpolation.types.numeric.NumericSummaryInterpolatorConfig;
import net.opentsdb.rollup.DefaultRollupConfig;

/**
 * A group by iterator for summary data. Note that for the special case
 * of calculating averages, the sum and count are required.
 * 
 * @since 3.0
 */
public class MergerNumericSummaryIterator implements QueryIterator, 
  TimeSeriesValue<NumericSummaryType>{
  
  /** The config. */
  private final MergerConfig config;
  
  /** The aggregator. */
  private final NumericAggregator aggregator;
  
  /** The next timestamp to return. */
  private final TimeStamp next_ts = new MillisecondTimeStamp(0);
  
  /** The next timestamp evaulated when returning the next value. */
  private final TimeStamp next_next_ts = new MillisecondTimeStamp(0);
  
  /** The data point set and returned by the iterator. */
  private final MutableNumericSummaryValue dp;
  
  /** The list of interpolators containing the real sources. */
  private final QueryInterpolator<NumericSummaryType>[] interpolators;
  
  /** Accumulators for each summary. */
  private final Map<Integer, NumericAccumulator> accumulators;
  
  /** The interpolator config. */
  private final NumericSummaryInterpolatorConfig interpolator_config;
  
  /** An index in the sources array used when pulling numeric iterators from the
   * sources. Must be less than or equal to the number of sources. */
  private int iterator_max;
  
  /** Whether or not another real value is present. True while at least one 
   * of the time series has a real value. */
  private boolean has_next = false;
  
  /**
   * Default ctor from a map of time series.
   * @param node The non-null owner.
   * @param result A query result to pull the rollup config from.
   * @param sources A non-null map of sources.
   */
  public MergerNumericSummaryIterator(
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
  public MergerNumericSummaryIterator(
      final QueryNode node, 
      final QueryResult result,
      final Collection<TimeSeries> sources) {
    if (node == null) {
      throw new IllegalArgumentException("Query node cannot be null.");
    }
    if (sources == null) {
      throw new IllegalArgumentException("Sources cannot be null.");
    }
    if (sources.isEmpty()) {
      throw new IllegalArgumentException("Sources cannot be empty.");
    }
    dp = new MutableNumericSummaryValue();
    next_ts.setMax();
    config = (MergerConfig) node.config();
    // TODO - better way of supporting aggregators
    aggregator = Aggregators.get(config.getAggregator());
    interpolators = new QueryInterpolator[sources.size()];
    
    QueryInterpolatorConfig interpolator_config = 
        config.interpolatorConfig(NumericSummaryType.TYPE);
    if (interpolator_config == null) {
      interpolator_config = config.interpolatorConfig(NumericType.TYPE);
      if (interpolator_config == null) {
        throw new IllegalArgumentException("No interpolator config found for type");
      }
      
      NumericSummaryInterpolatorConfig.Builder nsic = 
          NumericSummaryInterpolatorConfig.newBuilder()
          .setDefaultFillPolicy(((NumericInterpolatorConfig) interpolator_config).getFillPolicy())
          .setDefaultRealFillPolicy(((NumericInterpolatorConfig) interpolator_config).getRealFillPolicy());
      interpolator_config = nsic
          .setDataType(NumericSummaryType.TYPE.toString())
          .setType(null).build();
    }
    this.interpolator_config = (NumericSummaryInterpolatorConfig) interpolator_config;
    
    QueryInterpolatorFactory factory = node.pipelineContext().tsdb()
        .getRegistry().getPlugin(QueryInterpolatorFactory.class, 
            interpolator_config.getType());
    if (factory == null) {
      throw new IllegalArgumentException("No interpolator factory found for: " + 
          interpolator_config.getDataType() == null ? "Default" : 
            interpolator_config.getDataType());
    }
    
    for (final TimeSeries source : sources) {
      if (source == null) {
        throw new IllegalArgumentException("Null time series are not "
            + "allowed in the sources.");
      }
      interpolators[iterator_max] = (QueryInterpolator<NumericSummaryType>) 
          factory.newInterpolator(NumericSummaryType.TYPE, source, interpolator_config);
      
      if (interpolators[iterator_max].hasNext()) {
        has_next = true;
        if (interpolators[iterator_max].nextReal().compare(Op.LT, next_ts)) {
          next_ts.update(interpolators[iterator_max].nextReal());
        }
      }
      iterator_max++;
    }
    
    accumulators = Maps.newHashMapWithExpectedSize(1);
    for (final int summary : this.interpolator_config.getExpectedSummaries()) {
      accumulators.put(summary, new NumericAccumulator());
    }
  }
  
  @Override
  public boolean hasNext() {
    return has_next;
  }

  @Override
  public TimeSeriesValue<? extends TimeSeriesDataType> next() {
    has_next = false;
    next_next_ts.setMax();
    dp.clear();
    resetIndices();
    
    boolean had_nan = false;
    for (int i = 0; i < iterator_max; i++) {
      final TimeSeriesValue<NumericSummaryType> v = 
          interpolators[i].next(next_ts);
      if (v == null || v.value() == null) {
        // skip it
      } else {
        for (final int summary : v.value().summariesAvailable()) {
          final NumericType value = v.value().value(summary);
          if (value == null) {
            continue;
          }
          
          NumericAccumulator accumulator = accumulators.get(summary);
          if (accumulator == null) {
            accumulator = new NumericAccumulator();
            accumulators.put(summary, accumulator);
          }
          
          if (value.isInteger()) {
            accumulator.add(value.longValue());
          } else {
            if (Double.isNaN(value.doubleValue())) {
              if (config.getInfectiousNan()) {
                accumulator.add(value.doubleValue());
              }
              had_nan = true;
              // skip non-infectious nans.
            } else {
              accumulator.add(value.doubleValue());
            }
          }
        }
      }
      
      if (interpolators[i].hasNext()) {
        has_next = true;
        if (interpolators[i].nextReal().compare(Op.LT, next_next_ts)) {
          next_next_ts.update(interpolators[i].nextReal());
        }
      }
    }
    
    for (final Entry<Integer, NumericAccumulator> entry : 
        accumulators.entrySet()) {
      final NumericAccumulator accumulator = entry.getValue();
      if (accumulator.valueIndex() > 0) {
        accumulator.run(aggregator, config.getInfectiousNan());
        dp.resetValue(entry.getKey(), (NumericType) accumulator.dp());
      }
    }
    if (dp.summariesAvailable().isEmpty() && had_nan) {
      for (int summary : interpolator_config.getExpectedSummaries()) {
        dp.resetValue(summary, Double.NaN);
      }
    }
    dp.resetTimestamp(next_ts);
    
    next_ts.update(next_next_ts);
    return this;
  }

  @Override
  public TypeToken<? extends TimeSeriesDataType> getType() {
    return NumericSummaryType.TYPE;
  }
  
  @Override
  public TimeStamp timestamp() {
    return dp.timestamp();
  }

  @Override
  public NumericSummaryType value() {
    return dp.value();
  }

  @Override
  public TypeToken<NumericSummaryType> type() {
    return NumericSummaryType.TYPE;
  }

  private void resetIndices() {
    for (final NumericAccumulator accumulator : accumulators.values()) {
      accumulator.reset();
    }
  }
}