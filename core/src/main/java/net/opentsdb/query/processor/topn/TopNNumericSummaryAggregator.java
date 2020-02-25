// This file is part of OpenTSDB.
// Copyright (C) 2018-2020  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.topn;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.data.types.numeric.aggregators.NumericAggregator;
import net.opentsdb.data.types.numeric.aggregators.NumericAggregatorFactory;
import net.opentsdb.exceptions.QueryDownstreamException;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.rollup.DefaultRollupConfig;

import java.io.IOException;
import java.util.Optional;

/**
 * Aggregates an entire numeric series into a single value for the 
 * summary given in the aggregation function.
 * <p>
 * <b>NOTE:</b> If the aggregator is "count" then the count summary values
 * are summed.
 * <b>NOTE:</b> If the aggregator is "avg" and the "avg" summary is not 
 * available then we'll sum the "sum" and "counts" then avg.
 * 
 * @since 3.0
 */
public class TopNNumericSummaryAggregator {

  /** The aggregator. */
  private final NumericAggregator aggregator;
  
  /** The parent node. */
  protected final TopN node;
  
  /** The series we'll pull from. */
  protected final TimeSeries series;
  
  /** Summary IDs. */
  protected final int summary;
  protected final int sum_id;
  protected final int count_id;
  
  /**
   * Package private ctor.
   * @param node The non-null node.
   * @param result The non-null result.
   * @param source The non-null source.
   */
  TopNNumericSummaryAggregator(final QueryNode node, 
                               final QueryResult result,
                               final TimeSeries source) {
    this.node = (TopN) node;
    this.series = source;
    if (((TopNConfig) node.config()).getAggregator().toLowerCase().equals("count")) {
      NumericAggregatorFactory agg_factory = node.pipelineContext().tsdb()
          .getRegistry().getPlugin(NumericAggregatorFactory.class, "sum");
      if (agg_factory == null) {
        throw new IllegalArgumentException("No aggregator found for type: " + "sum");
      }
      aggregator = agg_factory.newAggregator(
          ((TopNConfig) node.config()).getInfectiousNan());
    } else {
      NumericAggregatorFactory agg_factory = node.pipelineContext().tsdb()
          .getRegistry().getPlugin(NumericAggregatorFactory.class, 
              ((TopNConfig) node.config()).getAggregator());
      if (agg_factory == null) {
        throw new IllegalArgumentException("No aggregator found for type: " 
            + ((TopNConfig) node.config()).getAggregator());
      }
      aggregator = agg_factory.newAggregator(
          ((TopNConfig) node.config()).getInfectiousNan());
    }
    
    summary = result.rollupConfig().getIdForAggregator(
        DefaultRollupConfig.queryToRollupAggregation(
            ((TopNConfig) node.config()).getAggregator()));
    if (((TopNConfig) node.config()).getAggregator().toLowerCase().equals("avg")) {
      sum_id = result.rollupConfig().getIdForAggregator("sum");
      count_id = result.rollupConfig().getIdForAggregator("count");
    } else {
      sum_id = -1;
      count_id = -1;
    }
  }
  
  /** @return Perform the aggregation. If no data is present, return null. */
  NumericType run() {
    final Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> optional =
        series.iterator(NumericSummaryType.TYPE);
    if (!optional.isPresent()) {
      return null;
    }
    try (final TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator = 
        optional.get()) {
      long[] long_values = sum_id >= 0 ? null : new long[16];
      double[] double_values = sum_id >= 0 ? new double[16] : null;
      double[] counts = null;
      int idx = 0;
      int count_idx = 0;
      
      while(iterator.hasNext()) {
        @SuppressWarnings("unchecked")
        final TimeSeriesValue<NumericSummaryType> value = 
            (TimeSeriesValue<NumericSummaryType>) iterator.next();
        if (value.value() == null) {
          continue;
        }
        
        NumericType v = value.value().value(summary);
        if (sum_id >= 0 && (v == null || count_idx > 0)) {
          final NumericType sum = value.value().value(sum_id);
          if (sum == null) {
            continue;
          }
          final NumericType count = value.value().value(count_id);
          if (count == null) {
            continue;
          }
  
          // roll back any that snuck in.
          if (idx > count_idx) {
            idx = count_idx;
          }
          
          if (idx >= double_values.length) {
            // grow
            double[] temp = new double[double_values.length + 
                                       (double_values.length >= 1024 ? 32 : double_values.length)];
            System.arraycopy(double_values, 0, temp, 0, double_values.length);
            double_values = temp;
          }
          double_values[idx++] = sum.toDouble();
          
          if (counts == null) {
            counts = new double[16];
          }
          if (count_idx >= counts.length) {
            // grow
            double[] temp = new double[counts.length + 
                                       (counts.length >= 1024 ? 32 : counts.length)];
            System.arraycopy(counts, 0, temp, 0, counts.length);
            counts = temp;
          }
          counts[count_idx++] = count.toDouble();
          
          continue;
        }
        
        if (v.isInteger() && long_values != null) {
          if (idx >= long_values.length) {
            // grow
            long[] temp = new long[long_values.length + 
                                   (long_values.length >= 1024 ? 32 : long_values.length)];
            System.arraycopy(long_values, 0, temp, 0, long_values.length);
            long_values = temp;
          }
          long_values[idx++] = v.longValue();
        } else {
          if (double_values == null) {
            // shift
            double_values = new double[long_values.length];
            for (int i = 0; i < idx; i++) {
              double_values[i] = (double) long_values[i];
            }
            long_values = null;
          }
          
          if (idx >= double_values.length) {
            // grow
            double[] temp = new double[double_values.length + 
                                       (double_values.length >= 1024 ? 32 : double_values.length)];
            System.arraycopy(double_values, 0, temp, 0, double_values.length);
            double_values = temp;
          }
          double_values[idx++] = v.toDouble();
        }
      }
      
      if (idx <= 0) {
        return null;
      }
      
      final MutableNumericValue dp = new MutableNumericValue();
      if (count_idx > 0) {
        final MutableNumericValue sums = new MutableNumericValue();
        aggregator.run(double_values, 0, idx, 
            ((TopNConfig) node.config()).getInfectiousNan(), sums);
        final MutableNumericValue count_sum = new MutableNumericValue();
        aggregator.run(counts, 0, count_idx, 
            ((TopNConfig) node.config()).getInfectiousNan(), count_sum);
        dp.resetValue(sums.toDouble() / count_sum.toDouble());
      } else {
        if (long_values != null) {
          aggregator.run(long_values, 0, idx, dp);
        } else {
          aggregator.run(double_values, 0, idx, 
              ((TopNConfig) node.config()).getInfectiousNan(), dp);
        }
      }
      return dp.value();
    } catch (IOException e) {
      throw new QueryDownstreamException(e.getMessage(), e);
    }
  }
  
}
