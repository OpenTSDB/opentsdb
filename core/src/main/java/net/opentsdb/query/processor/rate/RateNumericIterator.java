// This file is part of OpenTSDB.
// Copyright (C) 2014-2017 The OpenTSDB Authors.
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
package net.opentsdb.query.processor.rate;

import java.util.Iterator;
import java.util.Optional;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp.RelationalOperator;
import net.opentsdb.data.types.numeric.MutableNumericType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryIterator;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.pojo.RateOptions;

/**
 * Iterator that generates rates from a sequence of adjacent data points.
 * 
 * @since 2.0
 */
public class RateNumericIterator implements QueryIterator {
  
  /** A sequence of data points to compute rates. */
  private final Iterator<TimeSeriesValue<?>> source;
  
  /** Options for calculating rates. */
  private final RateOptions options;
  
  /** The previous raw value to calculate the rate. */
  private MutableNumericType prev_data;
  
  /** The rate that will be returned at the {@link #next} call. */
  private final MutableNumericType next_rate = new MutableNumericType();
  
  /** Users see this rate after they called next. */
  private final MutableNumericType prev_rate = new MutableNumericType();
  
  /** Whether or not the iterator has another real or filled value. */
  private boolean has_next;
  
  /**
   * Constructs a {@link RateNumericIterator} instance.
   * @param source The iterator to access the underlying data.
   * @param options Options for calculating rates.
   */
  public RateNumericIterator(final QueryNode node, final TimeSeries source) {
    if (node == null) {
      throw new IllegalArgumentException("Query node cannot be null.");
    }
    if (source == null) {
      throw new IllegalArgumentException("Source cannot be null.");
    }
    if (node.config() == null) {
      throw new IllegalArgumentException("Node config cannot be null.");
    }
    options = (RateOptions) node.config();
    final Optional<Iterator<TimeSeriesValue<?>>> optional = 
        source.iterator(NumericType.TYPE);
    if (optional.isPresent()) {
      this.source = optional.get();
      populateNextRate();
    } else {
      this.source = null;
    }
  }

  /** @return True if there is a valid next value. */
  @Override
  public boolean hasNext() {
    return has_next;
  }
  
  @Override
  public TimeSeriesValue<?> next() {
    prev_rate.reset(next_rate);
    populateNextRate();
    return prev_rate;
  }
  
  /**
   * Populate the next rate.
   */
  private void populateNextRate() {
    has_next = false;
    
    while (source.hasNext()) {
      final TimeSeriesValue<NumericType> next = 
          (TimeSeriesValue<NumericType>) source.next();
      if (next.value() == null || (!next.value().isInteger() && 
          (Double.isNaN(next.value().doubleValue())))) {
        continue;
      }
      
      if (prev_data == null) {
        prev_data = new MutableNumericType(next);
        continue;
      }
      
      // validation similar to TSDB 2.x
      if (next.timestamp().compare(RelationalOperator.LTE, prev_data.timestamp())) {
        throw new IllegalStateException("Next timestamp [" + next.timestamp() 
          + " ] cannot be less than or equal to the previous [" + 
            prev_data.timestamp() + "] timestamp.");
      }
      
      long prev_epoch = prev_data.timestamp().epoch();
      long prev_nanos = prev_data.timestamp().nanos();
      
      long next_epoch = next.timestamp().epoch();
      long next_nanos = next.timestamp().nanos();
      
      if (next_nanos < prev_nanos) {
        next_nanos *= 1000000000L;
        next_epoch--;
      }
      
      long diff = ((next_epoch - prev_epoch) * 1000000000) + (next_nanos - prev_nanos);
      double time_delta = (double) diff / (double) options.duration().toNanos();
      
      // got a rate!
      if (prev_data.value().isInteger() && next.value().isInteger()) {
        // longs
        long value_delta = next.value().longValue() - prev_data.longValue();
        if (options.isCounter() && value_delta < 0) {
          if (options.getDropResets()) {
            prev_data.reset(next);
            continue;
          }
          
          value_delta = options.getCounterMax() - prev_data.longValue() +
              next.value().longValue();
          
          final double rate = (double) value_delta / time_delta;
          if (options.getResetValue() > RateOptions.DEFAULT_RESET_VALUE
            && rate > options.getResetValue()) {
            next_rate.reset(next.timestamp(), 0.0D);
          } else {
            next_rate.reset(next.timestamp(), rate);
          }
        } else {
          next_rate.reset(next.timestamp(), (double) value_delta / time_delta);
        }
      } else {
        double value_delta = next.value().toDouble() - prev_data.toDouble();
        if (options.isCounter() && value_delta < 0) {
          if (options.getDropResets()) {
            prev_data.reset(next);
            continue;
          }
          
          value_delta = options.getCounterMax() - prev_data.toDouble() +
              next.value().toDouble();
          
          final double rate = value_delta / time_delta;
          if (options.getResetValue() > RateOptions.DEFAULT_RESET_VALUE
            && rate > options.getResetValue()) {
            next_rate.reset(next.timestamp(), 0.0D);
          } else {
            next_rate.reset(next.timestamp(), rate);
          }
        } else {
          next_rate.reset(next.timestamp(), value_delta / time_delta);
        }
      }
      
      prev_data.reset(next);
      has_next = true;
      break;
    }
  }

  @Override
  public String toString() {
    final StringBuilder buf = new StringBuilder();
    buf.append("RateSpan: ")
       .append(", options=").append(options)
       .append(", prev_data=[").append(prev_data)
       .append("], next_rate=[").append(next_rate)
       .append("], prev_rate=[").append(prev_rate)
       .append("], source=[").append(source).append("]");
    return buf.toString();
  }
}
