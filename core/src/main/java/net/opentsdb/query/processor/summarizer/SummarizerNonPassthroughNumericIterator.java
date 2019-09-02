// This file is part of OpenTSDB.
// Copyright (C) 2018-2019  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.summarizer;

import com.google.common.reflect.TypeToken;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.MutableNumericSummaryValue;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.data.types.numeric.aggregators.NumericAggregator;
import net.opentsdb.query.QueryIterator;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;

import java.util.Map.Entry;

/**
 * The iterator that handles summarizing arrays, numerics and other
 * summaries.
 * 
 * @since 3.0
 */
public class SummarizerNonPassthroughNumericIterator implements QueryIterator {

  /** The node we belong to. */
  private final QueryNode node;
  
  /** The results we came from. */
  private final QueryResult result;
  
  /** Rollup IDs for sum and count. */
  private final int sum;
  private final int count;
  
  /** Whether or not the iterator has another real or filled value. */
  private boolean has_next;
  
  /** The source iterator. */
  private TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator;
  
  /** The type of data pulled from the iterator. */
  private TypeToken<?> type;
  
  /** Results from the source. */
  private long[] long_values;
  private double[] double_values;
  
  /** The index into the results. */
  private int idx;
  
  /** The data point returned by this iterator. */
  private MutableNumericSummaryValue dp;
  
  /**
   * The default ctor.
   * @param node The non-null query node we belong to.
   * @param result The non-null results.
   * @param source The non-null source.
   */
  SummarizerNonPassthroughNumericIterator(final QueryNode node,
                                          final QueryResult result, 
                                          final TimeSeries source) {
    this.node = node;
    this.result = result;
    // pick one and only one
    // TODO - what if we have more than one??
    if (source.types().contains(NumericArrayType.TYPE)) {
      iterator = source.iterator(NumericArrayType.TYPE).get();
      type = NumericArrayType.TYPE;
    } else if (source.types().contains(NumericType.TYPE)) {
      iterator = source.iterator(NumericType.TYPE).get();
      type = NumericType.TYPE;
    } else if (source.types().contains(NumericSummaryType.TYPE)) {
      iterator = source.iterator(NumericSummaryType.TYPE).get();
      type = NumericSummaryType.TYPE;
    } else {
      // nothing to do here.
    }
    
    if (iterator != null) {
      has_next = iterator.hasNext();
      dp = new MutableNumericSummaryValue();
      sum = result.rollupConfig().getIdForAggregator("sum");
      count = result.rollupConfig().getIdForAggregator("count");
    } else {
      sum = 0;
      count = 0;
    }
  }
  
  @Override
  public boolean hasNext() {
    return has_next;
  }
  
  @Override
  public TimeSeriesValue<? extends TimeSeriesDataType> next() {
    int offset = 0;
    if (type == NumericArrayType.TYPE) {
      // easiest!
      // TODO - handle multiple nexts
      final TimeSeriesValue<NumericArrayType> value = 
          (TimeSeriesValue<NumericArrayType>) iterator.next();
      dp.resetTimestamp(value.timestamp());
      if (value.value() != null) {
        if (value.value().isInteger()) {
          long_values = value.value().longArray();
        } else {
          double_values = value.value().doubleArray();
        }
        offset = value.value().offset();
        idx = value.value().end();
      }
    } else if (type == NumericType.TYPE) {
      long_values = new long[8];
      boolean got_timestamp = false;
      while (iterator.hasNext()) {
        final TimeSeriesValue<NumericType> value = 
            (TimeSeriesValue<NumericType>) iterator.next();
        if (!got_timestamp) {
          dp.resetTimestamp(value.timestamp());
          got_timestamp = true;
        }
        if (value.value() != null) {
          if (value.value().isInteger()) {
            store(value.value().longValue());
          } else {
            store(value.value().doubleValue());
          }
        }
      }
    } else if (type == NumericSummaryType.TYPE) {
      long_values = new long[8];
      boolean got_timestamp = false;
      while (iterator.hasNext()) {
        final TimeSeriesValue<NumericSummaryType> value =
            (TimeSeriesValue<NumericSummaryType>) iterator.next();

        if (!got_timestamp) {
          dp.resetTimestamp(value.timestamp());
          got_timestamp = true;
        }

        if (value.value() != null) {
          if (value.value().summariesAvailable().size() == 1) {
            NumericType val = value.value().value(
                value.value().summariesAvailable().iterator().next());
            if (val.isInteger()) {
              store(val.longValue());
            } else {
              store(val.doubleValue());
            }
          } else if (value.value().summariesAvailable().size() == 2 && 
                     value.value().summariesAvailable().contains(sum) && 
                     value.value().summariesAvailable().contains(count)) {
            // in this case we got the sum and count so we want the average.
            NumericType sum = value.value().value(this.sum);
            NumericType count = value.value().value(this.count);
            if (sum == null || count == null) {
              dp.resetNull(iterator.next().timestamp());
              return dp;
            }
            store(sum.toDouble() / count.toDouble());
          } else {
            // TODO
            dp.resetNull(iterator.next().timestamp());
            has_next = false;
            return dp;
          }
        }
      }
    }
    
    final MutableNumericValue number = new MutableNumericValue();
    for (Entry<String, NumericAggregator> entry : 
        ((Summarizer) node).aggregators().entrySet()) {
      if (long_values != null) {
        entry.getValue().run(long_values, offset, idx, number);
      } else {
        entry.getValue().run(double_values, offset, idx, 
            ((SummarizerConfig) result.source().config()).getInfectiousNan(), 
            number);
      }
      
      dp.resetValue(result.rollupConfig().getIdForAggregator(entry.getKey()), number.value());
    }
    
    has_next = false;
    return dp;
  }
  
  @Override
  public TypeToken<? extends TimeSeriesDataType> getType() {
    return NumericSummaryType.TYPE;
  }
  
  /**
   * Stores a long.
   * @param value The value.
   */
  void store(final long value) {
    if (long_values == null) {
      store((double) value);
      return;
    }
    
    if (idx >= long_values.length) {
      long[] temp = new long[long_values.length + 8];
      for (int i = 0; i < long_values.length; i++) {
        temp[i] = long_values[i];
      }
      long_values = temp;
    }
    long_values[idx++] = value;
  }
  
  /**
   * Stores a double.
   * @param value The value.
   */
  void store(final double value) {
    if (long_values != null) {
      double_values = new double[long_values.length];
      for (int i = 0; i < long_values.length; i++) {
        double_values[i] = long_values[i];
      }
      long_values = null;
    }
    
    if (double_values == null) {
      double_values = new double[8];
    }
    
    if (idx >= double_values.length) {
      double[] temp = new double[double_values.length + 8];
      for (int i = 0; i < double_values.length; i++) {
        temp[i] = double_values[i];
      }
      double_values = temp;
    }
    double_values[idx++] = value;
  }
}
