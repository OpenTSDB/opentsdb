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
package net.opentsdb.query.processor.downsample;

import java.io.IOException;
import java.util.Optional;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.Op;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.MutableNumericSummaryValue;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.aggregators.NumericAggregator;
import net.opentsdb.data.types.numeric.aggregators.NumericAggregatorFactory;
import net.opentsdb.query.QueryIterator;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.processor.downsample.Downsample.DownsampleResult;

/**
 * Iterator that handles summary values. Note that when the 
 * @since 3.0
 */
public class DownsampleNumericSummaryIterator implements QueryIterator {

  private static final ThreadLocal<double[][]> agg_arrays = new ThreadLocal<double[][]>() {
    @Override
    protected double[][] initialValue() {
      double[][] outer = new double[2][];
      outer[0] = new double[1024];
      outer[1] = new double[1024];
      return outer;
    }
  };
  
  /** The result we belong to. */
  private final DownsampleResult result;
  
  /** The downsampler config. */
  private final DownsampleConfig config;
    
  /** The aggregator. */
  private final NumericAggregator aggregator;
  
  /** The iterator. */
  private TypedTimeSeriesIterator<NumericSummaryType> iterator;
  
  /** The current interval timestamp marking the start of the interval. */
  private TimeStamp interval_ts;
  
  /** Whether or not the iterator has another real or filled value. */
  private boolean has_next;
    
  /** The data point returned by this iterator. */
  private MutableNumericSummaryValue dp;
  
  MutableNumericValue temp_dp;
  
  /** The next value to populate. */
  private TimeSeriesValue<NumericSummaryType> next_value;
  
  private int summary;
  
  private final int sum_id;
  private final int count_id;
  private final int avg_id;
  
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
    config = (DownsampleConfig) node.config();
    
    final Optional<TypedTimeSeriesIterator<?>> op = source.iterator(NumericSummaryType.TYPE);
    if (!op.isPresent()) {
      // save some cycles;
      aggregator = null;
      sum_id = count_id = avg_id = 0;
      return;
    }
    iterator = (TypedTimeSeriesIterator<NumericSummaryType>) op.get();
    if (!iterator.hasNext()) {
      // save some cycles;
      aggregator = null;
      sum_id = count_id = avg_id = 0;
      return;
    }
    
    sum_id = result.rollupConfig().getIdForAggregator("sum");
    count_id = result.rollupConfig().getIdForAggregator("count");
    avg_id = result.rollupConfig().getIdForAggregator("avg");
    
    dp = new MutableNumericSummaryValue();
    NumericAggregatorFactory agg_factory;
    if (config.getAggregator().equalsIgnoreCase("avg")) {
      agg_factory = node.pipelineContext().tsdb()
          .getRegistry().getPlugin(NumericAggregatorFactory.class, 
              "sum");
      summary = -1;
      dp.resetValue(avg_id, Double.NaN);
    } else {
      agg_factory = node.pipelineContext().tsdb()
          .getRegistry().getPlugin(NumericAggregatorFactory.class, 
              ((DownsampleConfig) node.config()).getAggregator());
      summary = result.rollupConfig().getIdForAggregator(
          ((DownsampleConfig) node.config()).getAggregator());
      dp.resetValue(summary, Double.NaN);
    }
    if (agg_factory == null) {
      throw new IllegalArgumentException("No aggregator found for type: " 
          + ((DownsampleConfig) node.config()).getAggregator());
    }
    aggregator = agg_factory.newAggregator(
        ((DownsampleConfig) node.config()).getInfectiousNan());
    interval_ts = this.result.start().getCopy();
    
    String agg = config.getAggregator();
    if (agg.equalsIgnoreCase("AVG") && config.dpsInInterval() > 0) {
      agg = "sum";
    }
    
    temp_dp = new MutableNumericValue();
    // search
    while (iterator.hasNext()) {
      next_value = iterator.next();
      if (next_value.timestamp().compare(Op.GTE, config.startTime())) {
        has_next = true;
        break;
      }
    }
    if (next_value.timestamp().compare(Op.GTE, config.endTime())) {
      has_next = false;
      return;
    }
    
    if (has_next && !config.getFill() && !config.getRunAll()) {
      // advance
      TimeStamp end = interval_ts.getCopy();
      end.add(config.interval());
      while (true) {
        if (interval_ts.compare(Op.LTE, next_value.timestamp()) &&
            end.compare(Op.GT, next_value.timestamp())) {
          break;
        }
        
        if (next_value.timestamp().compare(Op.GTE, config.endTime())) {
          has_next = false;
          return;
        }
        
        interval_ts.add(config.interval());
        end.add(config.interval());
      }
    }
    
  }

  @Override
  public boolean hasNext() {
    return has_next;
  }
  
  @Override
  public TimeSeriesValue<? extends TimeSeriesDataType> next() {
    if (next_value.timestamp().compare(Op.LT, interval_ts) && 
        !iterator.hasNext() && 
        config.getFill()) {
      dp.resetTimestamp(interval_ts);
      for (int summary : dp.summariesAvailable()) {
        dp.resetValue(summary, Double.NaN);
      }
      interval_ts.add(config.interval());
      has_next = interval_ts.compare(Op.LT, config.endTime());
      return dp;
    }
    
    if (next_value.timestamp().compare(Op.GT, interval_ts) && 
        config.getFill()) {
      dp.resetTimestamp(interval_ts);
      for (int summary : dp.summariesAvailable()) {
        dp.resetValue(summary, Double.NaN);
      }
      interval_ts.add(config.interval());
      has_next = interval_ts.compare(Op.LT, config.endTime());
      return dp;
    }
    
    double[][] aggs = agg_arrays.get();
    int i = 0;
    dp.resetTimestamp(interval_ts);
    if (config.getRunAll()) {
      interval_ts.update(config.endTime());
    } else {
      interval_ts.add(config.interval());
    }
    
    if (summary < 0) {
      if (next_value.value() == null ||
          next_value.value().value(sum_id) == null) {
        aggs[0][i] = Double.NaN;
      } else {
        aggs[0][i] = next_value.value().value(sum_id).toDouble();
      }
      if (next_value.value() == null ||
          next_value.value().value(count_id) == null) {
        aggs[1][i++] = Double.NaN;
      } else {
        aggs[1][i++] = next_value.value().value(count_id).toDouble();
      }
    } else {
      if (next_value.value() == null ||
          next_value.value().value(summary) == null) {
        aggs[0][i++] = Double.NaN;
      } else {
        aggs[0][i++] = next_value.value().value(summary).toDouble();
      }
    }
    
    while (iterator.hasNext()) {
      next_value = iterator.next();
      if (next_value.timestamp().compare(Op.GTE, interval_ts)) {
        break;
      }
      
      if (i + 1 >= aggs[0].length) {
        // grow
        double[][] temp = new double[2][];
        temp[0] = new double[aggs[0].length * 2];
        System.arraycopy(aggs[0], 0, temp[0], 0, i);
        
        temp[1] = new double[aggs[0].length * 2];
        System.arraycopy(aggs[1], 0, temp[1], 0, i);
        aggs = temp;
      }
      
      if (summary < 0) {
        if (next_value.value() == null ||
            next_value.value().value(sum_id) == null) {
          aggs[0][i] = Double.NaN;
        } else {
          aggs[0][i] = next_value.value().value(sum_id).toDouble();
        }
        if (next_value.value() == null ||
            next_value.value().value(count_id) == null) {
          aggs[1][i++] = Double.NaN;
        } else {
          aggs[1][i++] = next_value.value().value(count_id).toDouble();
        }
      } else {
        if (next_value.value() == null ||
            next_value.value().value(summary) == null) {
          aggs[0][i++] = Double.NaN;
        } else {
          aggs[0][i++] = next_value.value().value(summary).toDouble();
        }
      }
    }
    
    if (config.getRunAll()) {
      has_next = false;
    } else if (config.getFill()) {
      has_next = interval_ts.compare(Op.LT, config.endTime());
    } else {
      // advance if we have a gap
      TimeStamp end = interval_ts.getCopy();
      end.add(config.interval());
      boolean oob = false;
      while (true) {
        if (interval_ts.compare(Op.LTE, next_value.timestamp()) &&
            end.compare(Op.GT, next_value.timestamp())) {
          break;
        }
        
        if (next_value.timestamp().compare(Op.GTE, config.endTime()) ||
            interval_ts.compare(Op.GTE, config.endTime())) {
          oob = true;
          break;
        }
        
        interval_ts.add(config.interval());
        end.add(config.interval());
      }
      if (oob) {
        has_next = false;
      } else {
        has_next = !(next_value.timestamp().compare(Op.LT, interval_ts) && 
            !iterator.hasNext());
      }
    }
    
    if (summary < 0) {
      aggregator.run(aggs[0], 0, i, config.getInfectiousNan(), temp_dp);
      double sum = temp_dp.toDouble();
      aggregator.run(aggs[1], 0, i, config.getInfectiousNan(), temp_dp);
      double count = Math.max(config.dpsInInterval(), temp_dp.toDouble());
      dp.resetValue(avg_id, (sum / count));
    } else {
      aggregator.run(aggs[0], 0, i, config.getInfectiousNan(), temp_dp);
      dp.resetValue(summary, temp_dp.value());
    }
    
    // nother check if we have a funky interval
    if (interval_ts.compare(Op.GTE, config.endTime())) {
      has_next = false;
    }
    return dp;
  }
  
  @Override
  public TypeToken<? extends TimeSeriesDataType> getType() {
    return NumericSummaryType.TYPE;
  }
  
  @Override
  public void close() {
    if (iterator != null) {
      try {
        iterator.close();
      } catch (IOException e) {
        // don't bother logging.
        e.printStackTrace();
      }
    }

    try {
      aggregator.close();
    } catch (IOException e) {
      // don't bother logging.
      e.printStackTrace();
    }
  }
  
}