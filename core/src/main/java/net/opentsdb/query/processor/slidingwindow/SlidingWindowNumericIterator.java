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
package net.opentsdb.query.processor.slidingwindow;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.Op;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.Aggregators;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericAggregator;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryIterator;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.SemanticQuery;

/**
 * An iterator for simple numeric series. It populates arrays to perform
 * the aggregation, growing as needed and shifting when we can to avoid
 * growing.
 * 
 * @since 3.0
 */
public class SlidingWindowNumericIterator implements QueryIterator, 
    TimeSeriesValue<NumericType> {

  /** The owner. */
  private final SlidingWindow node;
  
  /** The aggregator. */
  private final NumericAggregator aggregator;
  
  /** The data point set and returned by the iterator. */
  private final MutableNumericValue dp;
  
  /** An array of timestamps. */
  private TimeStamp[] timestamps;
  
  /** The long values. */
  private long[] long_values;
  
  /** The double values. */
  private double[] double_values;
  
  /** The next timestamp to return. */
  private TimeStamp next_ts;
  
  /** The starting offset into the arrays. */
  private int offset;
  
  /** Used as an index into the value arrays at any given iteration. */
  private int value_idx;
  
  /** Whether or not another real value is present. True while at least one 
   * of the time series has a real value. */
  private boolean has_next = false;
  
  /** The source iterator. */
  private Iterator<TimeSeriesValue<?>> iterator;
  
  /**
   * Ctor for maps.
   * @param node The non-null query node.
   * @param result The non-null result.
   * @param sources The non-null map of sources.
   */
  public SlidingWindowNumericIterator(final QueryNode node, 
                                      final QueryResult result,
                                      final Map<String, TimeSeries> sources) {
    this(node, result, sources == null ? null : sources.values());
  }
  

  /**
   * Ctor for collections.
   * @param node The non-null query node.
   * @param result The non-null result.
   * @param sources The non-null collection of sources.
   */
  public SlidingWindowNumericIterator(final QueryNode node, 
                                      final QueryResult result,
                                      final Collection<TimeSeries> sources) {
    next_ts = new MillisecondTimeStamp(0);
    this.node = (SlidingWindow) node;
    aggregator = Aggregators.get(((SlidingWindowConfig) node.config()).getAggregator());
    dp = new MutableNumericValue();
    final Optional<TypedTimeSeriesIterator> opt = 
        sources.iterator().next().iterator(NumericType.TYPE);
    if (opt.isPresent()) {
      iterator = opt.get();
      init();
    } else {
      iterator = null;
      has_next = false;
    }
  }
  
  @Override
  public boolean hasNext() {
    return has_next;
  }

  @Override
  public TimeSeriesValue<? extends TimeSeriesDataType> next() {
    has_next = false;
    if (long_values != null) {
      aggregator.run(long_values, offset, value_idx, dp);
    } else {
      aggregator.run(double_values, offset, value_idx, 
          ((SlidingWindowConfig) node.config()).getInfectiousNan(), dp);
    }
    dp.resetTimestamp(next_ts);
    advance();
    return dp;
  }

  @Override
  public TypeToken<? extends TimeSeriesDataType> getType() {
    return NumericType.TYPE;
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

  /**
   * Advances to the next window.
   */
  private void advance() {
    if (!iterator.hasNext()) {
      return;
    }
    
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    while (value.value() == null) {
      if (!iterator.hasNext()) {
        return;
      } else {
        value = (TimeSeriesValue<NumericType>) iterator.next();
      }
    }
    
    addValue(value);
    if (value_idx > 1) {
      next_ts.update(timestamps[value_idx - 1]);
      final TimeStamp start_window = timestamps[value_idx - 1].getCopy();
      start_window.subtract(((SlidingWindowConfig) node.config()).window());
      setOffset(start_window);
    }
    
    if (offset < value_idx) {
      has_next = true;
    }
  }
  
  /**
   * Initializes the arrays and loads the first window in the query range.
   */
  private void init() {
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    timestamps = new TimeStamp[8];
    long_values = new long[8];
    
    TimeStamp start = ((SemanticQuery) node.pipelineContext().query()).startTime();
    while (value.timestamp().compare(Op.LT, start)) {
      // skip nulls
      if (value.value() == null) {
        if (iterator.hasNext()) {
          value = (TimeSeriesValue<NumericType>) iterator.next();
          continue;
        } else {
          has_next = false;
          iterator = null;
          return;
        }
      }
      
      addValue(value);
      value = (TimeSeriesValue<NumericType>) iterator.next();
    }
    
    if (value.timestamp().compare(Op.GTE, start) && value.value() != null) {
      addValue(value);
    } else if (iterator.hasNext()) {
      value = (TimeSeriesValue<NumericType>) iterator.next();
      while (value.value() == null && iterator.hasNext()) {
        value = (TimeSeriesValue<NumericType>) iterator.next();
      }
      if (value.value() == null) {
        has_next = false;
        return;
      }
      addValue(value);
    } else {
      has_next = false;
      return;
    }
    
    next_ts.update(timestamps[value_idx - 1]);
    final TimeStamp start_window = timestamps[value_idx - 1].getCopy();
    start_window.subtract(((SlidingWindowConfig) node.config()).window());
    setOffset(start_window);
    has_next = true;
  }
  
  /**
   * Handles resizing/shifting the arrays when adding data.
   * @param value A non-null value to add.
   */
  private void addValue(final TimeSeriesValue<NumericType> value) {
    if (value_idx >= timestamps.length) {
      if (offset >= 16) {
        // shift!
        for (int i = 0; i < (value_idx - offset); i++) {
          timestamps[i] = timestamps[offset + i];
          if (long_values == null) {
            double_values[i] = double_values[offset + i];
          } else {
            long_values[i] = long_values[offset + i];
          }
        }
        value_idx -= offset;
        offset = 0;
      } else {
        // EXPAND timestamps and values
        TimeStamp[] clone = new TimeStamp[timestamps.length < 1024 ? 
            timestamps.length * 2 : timestamps.length + 16];
        for (int i = 0; i < timestamps.length; i++) {
          clone[i] = timestamps[i];
        }
        timestamps = clone;
        
        if (long_values != null) {
          long[] values = new long[long_values.length < 1024 ? 
              long_values.length * 2 : long_values.length + 16];
          for (int i = 0; i < long_values.length; i++) {
            values[i] = long_values[i];
          }
          long_values = values;
        } else {
          double[] values = new double[double_values.length < 1024 ? 
              double_values.length * 2 : double_values.length + 16];
          for (int i = 0; i < double_values.length; i++) {
            values[i] = double_values[i];
          }
          double_values = values;
        }
      }
    }
    
    timestamps[value_idx] = value.timestamp().getCopy();
    if (value.value().isInteger() && long_values != null) {
      long_values[value_idx] = value.value().longValue();
    } else {
      if (double_values == null) {
        shiftToDouble();
      }
      double_values[value_idx] = value.value().toDouble();
    }
    value_idx++;
  }
  
  /**
   * Finds the next offset (start of the window)
   * @param window_start The non-null start of the window.
   */
  private void setOffset(final TimeStamp window_start) {
    for (int i = offset; i < timestamps.length; i++) {
      if (timestamps[i].compare(Op.LTE, window_start)) {
        offset++;
      } else {
        break;
      }
    }
  }
  
  /**
   * Helper that moves all of the longs to the doubles array.
   */
  private void shiftToDouble() {
    if (double_values == null) {
      double_values = new double[long_values.length];
    }
    if (value_idx == 0) {
      long_values = null;
      return;
    }
    for (int i = 0; i < value_idx; i++) {
      double_values[i] = (double) long_values[i];
    }
    long_values = null;
  }
  
  @VisibleForTesting
  int arrayLength() {
    if (long_values == null) {
      return double_values.length;
    }
    return long_values.length;
  }
}
