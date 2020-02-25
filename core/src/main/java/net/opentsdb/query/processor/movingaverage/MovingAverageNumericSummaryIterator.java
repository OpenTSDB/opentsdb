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
package net.opentsdb.query.processor.movingaverage;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.Op;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.MutableNumericSummaryType;
import net.opentsdb.data.types.numeric.MutableNumericSummaryValue;
import net.opentsdb.data.types.numeric.NumericAccumulator;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.data.types.numeric.aggregators.NumericAggregator;
import net.opentsdb.query.QueryIterator;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.SemanticQuery;

/**
 * An iterator for numeric summary series.
 * 
 * TODO - tons of optimizations to be had here.
 * 
 * @since 3.0
 */
public class MovingAverageNumericSummaryIterator implements QueryIterator, 
    TimeSeriesValue<NumericSummaryType> {

  /** The node we belong to. */
  private final MovingAverage node;
  
  /** The aggregator. */
  private final NumericAggregator aggregator;
  
  /** The value to populate on each iteration. */
  private final MutableNumericSummaryValue dp;

  /** Whether or not we're in windowing mode. */
  private final boolean windowed;
  
  /** The timestamps array. */
  private TimeStamp[] timestamps;
  
  /** The values array. */
  private MutableNumericSummaryType[] values;

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
  private TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator;
  
  /** A map of summary accumulators to populate on each run. */
  private Map<Integer, NumericAccumulator> accumulators;
  
  /** Whether or not we're computing with samples or intervals. */
  private int samples;
  
  /**
   * Ctor for maps.
   * @param node The non-null query node.
   * @param result The non-null result.
   * @param sources The non-null map of sources.
   */
  public MovingAverageNumericSummaryIterator(final QueryNode node, 
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
  public MovingAverageNumericSummaryIterator(final QueryNode node, 
                                             final QueryResult result,
                                             final Collection<TimeSeries> sources) {
    next_ts = new MillisecondTimeStamp(0);
    this.node = (MovingAverage) node;
    final MovingAverageConfig config = (MovingAverageConfig) node.config();
    samples = config.getSamples();
    aggregator = this.node.getAggregator();
    dp = new MutableNumericSummaryValue();
    
    final Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> opt =
        sources.iterator().next().iterator(NumericSummaryType.TYPE);
    if (Strings.isNullOrEmpty(((MovingAverageConfig) node.config()).getInterval())) {
      windowed = false;
    } else {
      windowed = true;
    }
    
    if (opt.isPresent()) {
      iterator = opt.get();
      accumulators = Maps.newHashMap();
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
    dp.clear();
    
    // reset extant accumulators
    for (final NumericAccumulator accumulator : accumulators.values()) {
      accumulator.reset();
    }
    
    // this could be optimized a bunch by creating offset based arrays
    // for each type. For now we create arrays and repopulate them
    // on each pass.
    for (int i = offset; i < value_idx; i++) {
      final MutableNumericSummaryType value = values[i];
      for (final int summary : value.summariesAvailable()) {
        final NumericType v = value.value(summary);
        if (v == null) {
          continue;
        }
        
        NumericAccumulator accumulator = accumulators.get(summary);
        if (accumulator == null) {
          accumulator = new NumericAccumulator();
          accumulators.put(summary, accumulator);
        }
        if (v.isInteger()) {
          accumulator.add(v.longValue());
        } else {
          accumulator.add(v.doubleValue());
        }
      }
    }
    
    for (final Entry<Integer, NumericAccumulator> entry : accumulators.entrySet()) {
      if (entry.getValue().valueIndex() < 1) {
        continue;
      }
      
      entry.getValue().run(aggregator, 
          ((MovingAverageConfig) node.config()).getInfectiousNan());
      dp.resetValue(entry.getKey(), entry.getValue().dp().value());
    }
    dp.resetTimestamp(next_ts);
    advance();
    return dp;
  }

  @Override
  public TypeToken<? extends TimeSeriesDataType> getType() {
    return NumericSummaryType.TYPE;
  }
  
  @Override
  public void close() {
    try {
      iterator.close();
    } catch (IOException e) {
      // Don't bother logging.
      e.printStackTrace();
    }
    iterator = null;
    
    try {
      aggregator.close();
    } catch (IOException e) {
      // don't bother logging.
      e.printStackTrace();
    }
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

  /**
   * Advances to the next window.
   */
  private void advance() {
    if (!iterator.hasNext()) {
      return;
    }
    
    if (!windowed && (value_idx - offset) >= samples) {
      offset++;
    }
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    while (value.value() == null) {
      if (!iterator.hasNext()) {
        return;
      } else {
        value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
      }
    }
    
    addValue(value);
    if (value_idx > 1 && windowed) {
      next_ts.update(timestamps[value_idx - 1]);
      final TimeStamp start_window = timestamps[value_idx - 1].getCopy();
      start_window.subtract(((MovingAverageConfig) node.config()).interval());
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
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    values = new MutableNumericSummaryType[8];
    if (windowed) {
      timestamps = new TimeStamp[8];
    }
    
    TimeStamp start = ((SemanticQuery) node.pipelineContext().query()).startTime();
    while (value.timestamp().compare(Op.LT, start)) {
      // skip nulls
      if (value.value() == null) {
        if (iterator.hasNext()) {
          value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
          continue;
        } else {
          has_next = false;
          iterator = null;
          return;
        }
      }
      
      if (addValue(value) && !windowed && 
          (value_idx - offset) >= samples) {
        // drop really early values.
        offset++;
      }
      value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    }
    
    if (value.timestamp().compare(Op.GTE, start) && value.value() != null) {
      addValue(value);
    } else if (iterator.hasNext()) {
      value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
      while (value.value() == null && iterator.hasNext()) {
        value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
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
    
    if (windowed) {
      next_ts.update(timestamps[value_idx - 1]);
      final TimeStamp start_window = timestamps[value_idx - 1].getCopy();
      start_window.subtract(((MovingAverageConfig) node.config()).interval());
      setOffset(start_window);
    }
    has_next = true;
  }
  
  /**
   * Handles resizing/shifting the arrays when adding data.
   * @param value A non-null value to add.
   * @return True if we've hit the count limit, false if not.
   */
  private boolean addValue(final TimeSeriesValue<NumericSummaryType> value) {
    if (value_idx >= values.length) {
      if (offset >= 16) {
        // shift!
        for (int i = 0; i < (value_idx - offset); i++) {
          if (windowed) {
            timestamps[i] = timestamps[offset + i];
          }
          values[i] = values[offset + i];
        }
        value_idx -= offset;
        offset = 0;
      } else {
        // EXPAND timestamps and values
        if (windowed) {
          TimeStamp[] clone = new TimeStamp[timestamps.length < 1024 ? 
              timestamps.length * 2 : timestamps.length + 16];
          for (int i = 0; i < timestamps.length; i++) {
            clone[i] = timestamps[i];
          }
          timestamps = clone;
        }
        
        MutableNumericSummaryType[] temp = new MutableNumericSummaryType[
            values.length < 1024 ? values.length * 2 : 16];
        for (int i = 0; i < values.length; i++) {
          temp[i] = values[i];
        }
        values = temp;
      }
    }
    
    if (windowed) {
      timestamps[value_idx] = value.timestamp().getCopy();
    } else {
      next_ts.update(value.timestamp());
    }
    values[value_idx] = new MutableNumericSummaryType(value.value());
    value_idx++;
    if (value_idx - offset >= samples) {
      return true;
    }
    return false;
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
  
  @VisibleForTesting
  int arrayLength() {
    return values.length;
  }
}
