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
import java.util.Optional;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.Op;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.Aggregators;
import net.opentsdb.data.types.numeric.NumericAccumulator;
import net.opentsdb.data.types.numeric.NumericAggregator;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryIterator;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.processor.downsample.Downsample.DownsampleResult;

/**
 * A downsampler working over a numeric source array. If the source is 
 * the same length then we just pass it through without modifications to
 * save some cycles.
 * 
 * TODO - Implement an interpolator and/or fills other than NaN.
 * 
 * @since 3.0
 */
public class DownsampleNumericArrayIterator implements QueryIterator, 
    TimeSeriesValue<NumericArrayType>, 
    NumericArrayType {

  /** The aggregator. */
  protected final NumericAggregator aggregator;
  
  /** The node we belong to. */
  protected final QueryNode node;
  
  /** The parent result. */
  protected final DownsampleResult result;
  
  /** The calculated intervals. */
  protected int intervals;
  
  /** The source iterator. */
  protected final Iterator<TimeSeriesValue<?>> iterator;
  
  /** Values to populate. */
  protected long[] long_values;
  protected double[] double_values;
  
  /**
   * Ctor with a collection of source time series.
   * @param node The non-null node this iterator belongs to.
   * @param result The result this source is a part of.
   * @param source A non-null source to pull numeric iterators from. 
   * @throws IllegalArgumentException if a required parameter or config is 
   * not present.
   */
  public DownsampleNumericArrayIterator(final QueryNode node, 
                                        final QueryResult result,
                                        final TimeSeries source) {
    if (node == null) {
      throw new IllegalArgumentException("Query node cannot be null.");
    }
    if (source == null) {
      throw new IllegalArgumentException("Source cannot be null.");
    }
    if (result == null) {
      throw new IllegalArgumentException("Result cannot be null.");
    }
    this.node = node;
    this.result = (DownsampleResult) result;
    
    // TODO - better way of supporting aggregators
    aggregator = Aggregators.get(((DownsampleConfig) node.config()).getAggregator());
    final Optional<TypedTimeSeriesIterator> optional = 
        source.iterator(NumericArrayType.TYPE);
    if (optional.isPresent()) {
      iterator = optional.get();
      intervals = ((DownsampleConfig) node.config()).intervals();
    } else {
      iterator = null;
    }
  }
  
  @Override
  public boolean hasNext() {
    return iterator == null ? false : iterator.hasNext();
  }

  @Override
  public TimeSeriesValue<? extends TimeSeriesDataType> next() {
    final TimeSeriesValue<NumericArrayType> value =
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    // short-circuit in case we have the same downsample interval as our 
    // source.
    if (value.value().end() - value.value().offset() == intervals) {
      if (value.value().isInteger()) {
        long_values = value.value().longArray();
      } else {
        double_values = value.value().doubleArray();
      }
      return this;
    }
    
    long[] long_source = null;
    double[] double_source = null;
    
    NumericAccumulator accumulator = new NumericAccumulator();
    
    if (value.value().isInteger()) {
      long_values = new long[intervals];
      long_source = value.value().longArray();
    } else {
      double_values = new double[intervals];
      double_source = value.value().doubleArray();
    }
    
    final TimeStamp source_ts = 
        result.downstreamResult().timeSpecification().start().getCopy();
    final TimeStamp end_of_interval = 
        ((DownsampleConfig) node.config()).startTime().getCopy();
    end_of_interval.add(((DownsampleConfig) node.config()).interval());
    
    int source_idx = 0;
    
    // iterate and fill
    for (int i = 0; i < intervals; i++) {
      while (source_ts.compare(Op.LT, ((DownsampleConfig) node.config()).startTime())) {
        source_ts.add(result.downstreamResult().timeSpecification().interval());
        source_idx++;
      }
      
      while (source_ts.compare(Op.LT, end_of_interval)) {
        if (long_source != null) {
          if (source_idx < long_source.length) {
            accumulator.add(long_source[source_idx]);
          }
        } else {
          if (source_idx < double_source.length) {
            accumulator.add(double_source[source_idx]);
          }
        }
        source_ts.add(result.downstreamResult().timeSpecification().interval());
        source_idx++;
      }
      
      if (accumulator.valueIndex() > 0) {
        accumulator.run(aggregator, ((DownsampleConfig) node.config()).getInfectiousNan());
        if (accumulator.dp().value().isInteger()) {
          if (long_values != null) {
            long_values[i] = accumulator.dp().value().longValue();
          } else {
            double_values[i] = accumulator.dp().value().toDouble();
          }
        } else {
          if (long_values != null) {
            shift();
          }
          double_values[i] = accumulator.dp().value().toDouble();
        }
        accumulator.reset();
      } else {
        fill(i);
      }
      end_of_interval.add(((DownsampleConfig) node.config()).interval());
    }

    return this;
  }

  @Override
  public TypeToken<? extends TimeSeriesDataType> getType() {
    return NumericArrayType.TYPE;
  }
  
  @Override
  public TimeStamp timestamp() {
    return result.start();
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
  public boolean isInteger() {
    return long_values == null ? false : true;
  }

  @Override
  public long[] longArray() {
    return long_values;
  }

  @Override
  public double[] doubleArray() {
    return double_values;
  }

  @Override
  public int offset() {
    return 0;
  }

  @Override
  public int end() {
    return intervals;
  }

  /**
   * Helper to fill a value.
   * TODO - for now it's just a NaN.
   * @param idx
   */
  private void fill(final int idx) {
    if (long_values != null) {
      shift();
    }
    double_values[idx] = Double.NaN;
  }
  
  /**
   * Copies longs into the double array.
   */
  private void shift() {
    double_values = new double[long_values.length];
    for (int x = 0; x < long_values.length; x++) {
      double_values[x] = long_values[x];
    }
    long_values = null;
  }
}