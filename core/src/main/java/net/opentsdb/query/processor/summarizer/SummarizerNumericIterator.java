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
package net.opentsdb.query.processor.summarizer;

import java.util.Iterator;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.types.numeric.Aggregators;
import net.opentsdb.data.types.numeric.MutableNumericSummaryValue;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericAggregator;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryIterator;
import net.opentsdb.query.QueryResult;

/**
 * The iterator that handles summarizing arrays, numerics and other
 * summaries. TODO!
 * 
 * @since 3.0
 */
public class SummarizerNumericIterator implements QueryIterator {

  /** The results we came from. */
  private final QueryResult result;
  
  /** Whether or not the iterator has another real or filled value. */
  private boolean has_next;
  
  /** The source iterator. */
  private Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> iterator;
  
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
   * @param result The non-null results.
   * @param source The non-null source.
   */
  SummarizerNumericIterator(final QueryResult result, 
                            final TimeSeries source) {
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
      // TODO !
      // hopefully it's a rollup series, not a summarizer
      //iterator = source.iterator(NumericSummaryType.TYPE).get();
      //type = NumericSummaryType.TYPE;
    } else {
      // nothing to do here.
    }
    
    if (iterator != null) {
      has_next = iterator.hasNext();
      dp = new MutableNumericSummaryValue();
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
      // TODO!!!
    }
    
    final MutableNumericValue number = new MutableNumericValue();
    for (final String summary : 
        ((SummarizerConfig) result.source().config()).getSummaries()) {
      final NumericAggregator agg = Aggregators.get(summary);
      if (long_values != null) {
        agg.run(long_values, offset, idx, number);
      } else {
        agg.run(double_values, offset, idx, 
            ((SummarizerConfig) result.source().config()).getInfectiousNan(), 
            number);
      }
      
      dp.resetValue(result.rollupConfig().getIdForAggregator(summary), number.value());
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
