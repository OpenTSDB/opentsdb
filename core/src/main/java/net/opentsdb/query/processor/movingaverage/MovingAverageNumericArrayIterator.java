// This file is part of OpenTSDB.
// Copyright (C) 2019  The OpenTSDB Authors.
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

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

import com.google.common.base.Strings;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.Op;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.data.types.numeric.aggregators.NumericAggregator;
import net.opentsdb.query.QueryIterator;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;

/**
 * An iterator for simple numeric series. It populates arrays to perform
 * the aggregation, growing as needed and shifting when we can to avoid
 * growing.
 * 
 * TODO - we need to make sure that upstream we validate whether or not
 * the window could properly fit the downsampled result window.
 * 
 * TODO - also we want to make sure the downsample would cover the first
 * window so it needs to return data previous to the query start time.
 * 
 * @since 3.0
 */
public class MovingAverageNumericArrayIterator implements QueryIterator, 
    TimeSeriesValue<NumericArrayType>,
    NumericArrayType {

  /** The owner. */
  private final MovingAverage node;
  
  /** The query result so we can get the time spec. */
  private final QueryResult result;
  
  /** Whether or not we're in windowing mode. */
  private final boolean windowed;
  
  /** The aggregator. */
  private final NumericAggregator aggregator;

  /** The current timestamp. */
  private TimeStamp timestamp;
  
  /** The long values. */
  private long[] long_values;
  
  /** The double values. */
  private double[] double_values;
  
  /** Whether or not another real value is present. True while at least one 
   * of the time series has a real value. */
  private boolean has_next = false;
  
  /** The source iterator. */
  private TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator;

  /** Whether or not we're computing with samples or intervals. */
  private int samples;
  
  /**
   * Ctor for maps.
   * @param node The non-null query node.
   * @param result The non-null result.
   * @param sources The non-null map of sources.
   */
  public MovingAverageNumericArrayIterator(final QueryNode node, 
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
  public MovingAverageNumericArrayIterator(final QueryNode node, 
                                           final QueryResult result,
                                           final Collection<TimeSeries> sources) {
    this.node = (MovingAverage) node;
    this.result = result;
    
    final MovingAverageConfig config = (MovingAverageConfig) node.config();
    samples = config.getSamples();
    aggregator = this.node.getAggregator();
    
    final Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> opt =
        sources.iterator().next().iterator(NumericArrayType.TYPE);
    if (Strings.isNullOrEmpty(((MovingAverageConfig) node.config()).getInterval())) {
      windowed = false;
    } else {
      windowed = true;
    }
    
    if (opt.isPresent()) {
      iterator = opt.get();
      has_next = true;
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
    final TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    timestamp = value.timestamp();
    long[] long_source = null;
    double[] double_source = null;
    if (value.value().isInteger()) {
      long_values = new long[value.value().end() - value.value().offset()];
      long_source = value.value().longArray();
    } else {
      double_values = new double[value.value().end() - value.value().offset()];
      double_source = value.value().doubleArray();
    }
    
    final MutableNumericValue dp = new MutableNumericValue();
    int offset = value.value().offset();
    int end = value.value().offset() + 1;
    int offset_start_value = 0;
    
    if (windowed) {      
      TimeStamp current = timestamp.getCopy();
      TimeStamp offset_calc = current.getCopy();
      offset_calc.add(((MovingAverageConfig) node.config()).interval());
      while (current.compare(Op.LTE, offset_calc)) {
        current.add(result.timeSpecification().interval());
        offset_start_value++;
      }
    } else {
      offset_start_value = samples + 1;
    }
    
    while (end <= value.value().end()) {
      if (long_source != null) {
        aggregator.run(long_source, offset, end, dp);
        add(end - 1, dp);
      } else {
        aggregator.run(double_source, offset, end, 
            ((MovingAverageConfig) node.config()).getInfectiousNan(), dp);
        add(end - 1, dp);
      }
      end++;

      if (end >= offset_start_value) {
        offset++;
      }
    }
    return this;
  }

  @Override
  public TypeToken<? extends TimeSeriesDataType> getType() {
    return NumericArrayType.TYPE;
  }
  
  @Override
  public TimeStamp timestamp() {
    return timestamp;
  }

  @Override
  public NumericArrayType value() {
    return this;
  }

  @Override
  public TypeToken<NumericArrayType> type() {
    return NumericArrayType.TYPE;
  }
  
  @Override
  public int offset() {
    return 0;
  }

  @Override
  public int end() {
    return long_values != null ? long_values.length : double_values.length;
  }

  @Override
  public boolean isInteger() {
    // TODO Auto-generated method stub
    return long_values != null;
  }

  @Override
  public long[] longArray() {
    return long_values;
  }

  @Override
  public double[] doubleArray() {
    return double_values;
  }
  
  void add(final int idx, final NumericType value) {
    if (value.isInteger()) {
      if (long_values != null) {
        long_values[idx] = value.longValue();
      } else {
        double_values[idx] = value.longValue();
      }
    } else {
      if (double_values == null) {
        double_values = new double[long_values.length];
        for (int i = 0; i < double_values.length; i++) {
          double_values[i] = long_values[i];
        }
        long_values = null;
      }
      double_values[idx] = value.doubleValue();
    }
  }
  
  private String arraysToString(final long[] src, final int offset, final int end) {
    StringBuilder buf = new StringBuilder();
    buf.append("[");
    for (int i = offset; i < end; i++) {
      if (i != offset) {
        buf.append(", ");
      }
      buf.append(src[i]);
    }
    buf.append("]");
    return buf.toString();
  }
  
  private String arraysToString(final double[] src, final int offset, final int end) {
    StringBuilder buf = new StringBuilder();
    buf.append("[");
    for (int i = offset; i < end; i++) {
      if (i != offset) {
        buf.append(", ");
      }
      buf.append(src[i]);
    }
    buf.append("]");
    return buf.toString();
  }
}
