// This file is part of OpenTSDB.
// Copyright (C) 2017-2018  The OpenTSDB Authors.
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

import com.google.common.base.Strings;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.Op;
import net.opentsdb.data.types.numeric.Aggregators;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericAggregator;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryIterator;
import net.opentsdb.query.interpolation.QueryInterpolatorFactory;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.interpolation.QueryInterpolator;
import net.opentsdb.query.interpolation.QueryInterpolatorConfig;
import net.opentsdb.query.processor.groupby.GroupByConfig;

/**
 * An iterator for group-by operations wherein multiple time series are 
 * aggregated into a single time series using an aggregation function and 
 * interpolators to fill missing or unaligned values.
 * <p>
 * Since most aggregations include multiple source time series, values are
 * written to arrays of longs (first attempt to preserve precision when all 
 * values are longs) or doubles (if one or more values are doubles). This allows
 * the JVM to use SIMD processing if possible. 
 * 
 * @since 3.0
 */
public class GroupByNumericIterator implements QueryIterator, 
    TimeSeriesValue<NumericType> {
  /** Whether or not NaNs are sentinels or real values. */
  private final boolean infectious_nan;
  
  /** The aggregator. */
  private final NumericAggregator aggregator;
  
  /** The next timestamp to return. */
  private final TimeStamp next_ts = new MillisecondTimeStamp(0);
  
  /** The next timestamp evaluated when returning the next value. */
  private final TimeStamp next_next_ts = new MillisecondTimeStamp(0);
  
  /** The data point set and returned by the iterator. */
  private final MutableNumericValue dp;
  
  /** The list of interpolators containing the real sources. */
  private final QueryInterpolator<NumericType>[] interpolators;
  
  /** An array of long values used when all sources return longs. */
  private long[] long_values;
  
  /** An array of double values used when one or more sources return a double. */
  private double[] double_values;

  /** An index in the sources array used when pulling numeric iterators from the
   * sources. Must be less than or equal to the number of sources. */
  private int iterator_max;
  
  /** Used as an index into the value arrays at any given iteration. */
  private int value_idx;
  
  /** Whether or not another real value is present. True while at least one 
   * of the time series has a real value. */
  private boolean has_next = false;
  
  /**
   * Default ctor.
   * @param node The non-null node this iterator belongs to.
   * @param result The result this source is a part of.
   * @param sources The non-null and non-empty map of sources.
   * @throws IllegalArgumentException if a required parameter or config is 
   * not present.
   */
  public GroupByNumericIterator(final QueryNode node, 
                                final QueryResult result,
                                final Map<String, TimeSeries> sources) {
    this(node, result, sources == null ? null : sources.values());
  }
  
  /**
   * Ctor with a collection of source time series.
   * @param node The non-null node this iterator belongs to.
   * @param result The result this source is a part of.
   * @param sources The non-null and non-empty collection or sources.
   * @throws IllegalArgumentException if a required parameter or config is 
   * not present.
   */
  @SuppressWarnings("unchecked")
  public GroupByNumericIterator(final QueryNode node, 
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
    if (Strings.isNullOrEmpty(((GroupByConfig) node.config()).getAggregator())) {
      throw new IllegalArgumentException("Aggregator cannot be null or empty."); 
    }
    
    dp = new MutableNumericValue();
    next_ts.setMax();
    dp.resetNull(next_ts);
    // TODO - better way of supporting aggregators
    aggregator = Aggregators.get(((GroupByConfig) node.config()).getAggregator());
    infectious_nan = ((GroupByConfig) node.config()).getInfectiousNan();
    interpolators = new QueryInterpolator[sources.size()];
    
    QueryInterpolatorConfig interpolator_config = ((GroupByConfig) node.config()).interpolatorConfig(NumericType.TYPE);
    if (interpolator_config == null) {
      throw new IllegalArgumentException("No interpolator config found for type");
    }
    
    QueryInterpolatorFactory factory = node.pipelineContext().tsdb()
        .getRegistry().getPlugin(QueryInterpolatorFactory.class, 
            interpolator_config.id());
    if (factory == null) {
      throw new IllegalArgumentException("No interpolator factory found for: " + 
          interpolator_config.id() == null ? "Default" : interpolator_config.id());
    }
    
    for (final TimeSeries source : sources) {
      if (source == null) {
        throw new IllegalArgumentException("Null time series are not "
            + "allowed in the sources.");
      }
      interpolators[iterator_max] = (QueryInterpolator<NumericType>) 
          factory.newInterpolator(NumericType.TYPE, source, interpolator_config);
      if (interpolators[iterator_max].hasNext()) {
        has_next = true;
        if (interpolators[iterator_max].nextReal().compare(Op.LT, next_ts)) {
          next_ts.update(interpolators[iterator_max].nextReal());
        }
      }
      iterator_max++;
    }
    long_values = new long[sources.size()];
  }

  @Override
  public TimeStamp timestamp() {
    return dp.timestamp();
  }

  @Override
  public NumericType value() {
    return dp.value();
  }

  @Override
  public TypeToken<NumericType> type() {
    return NumericType.TYPE;
  }

  @Override
  public boolean hasNext() {
    return has_next;
  }
  
  @Override
  public TimeSeriesValue<?> next() {
    has_next = false;
    next_next_ts.setMax();
    value_idx = 0;
    boolean longs = true;
    boolean had_nan = false;
    for (int i = 0; i < iterator_max; i++) {
      final TimeSeriesValue<NumericType> v = interpolators[i].next(next_ts);
      if (v == null || v.value() == null) {
        // skip it
      } else if (!v.value().isInteger() && Double.isNaN(v.value().doubleValue())) {
        if (infectious_nan) {
          if (longs) {
            longs = false;
            shiftToDouble();
          }
          double_values[value_idx++] = Double.NaN;
        }
        had_nan = true;
      } else {
        if (v.value().isInteger() && longs) {
          long_values[value_idx++] = v.value().longValue();
        } else {
          if (longs) {
            longs = false;
            shiftToDouble();
          }
          double_values[value_idx++] = v.value().toDouble();
        }
      }
      
      if (interpolators[i].hasNext()) {
        has_next = true;
        if (interpolators[i].nextReal().compare(Op.LT, next_next_ts)) {
          next_next_ts.update(interpolators[i].nextReal());
        }
      }
    }
    
    // sum it
    if (value_idx < 1) {
      if (had_nan) {
        dp.reset(next_ts, Double.NaN);
      } else 
      if (interpolators[0].fillPolicy().fill() == null) {
        dp.resetNull(next_ts);
      } else {
        dp.reset(next_ts, interpolators[0].fillPolicy().fill());
      }
    } else {
      if (longs) {
        dp.resetTimestamp(next_ts);
        aggregator.run(long_values, value_idx, dp);
      } else {
        dp.resetTimestamp(next_ts);
        aggregator.run(double_values, value_idx, infectious_nan, dp);
      }
    }

    next_ts.update(next_next_ts);

    return this;
  }
  
  /**
   * Helper that moves all of the longs to the doubles array.
   */
  private void shiftToDouble() {
    if (double_values == null) {
      double_values = new double[interpolators.length];
    }
    if (value_idx == 0) {
      return;
    }
    for (int i = 0; i < value_idx; i++) {
      double_values[i] = (double) long_values[i];
    }
  }
}
