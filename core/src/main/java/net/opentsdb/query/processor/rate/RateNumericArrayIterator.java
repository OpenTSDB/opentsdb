// This file is part of OpenTSDB.
// Copyright (C) 2018 The OpenTSDB Authors.
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

import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.query.QueryIterator;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.pojo.RateOptions;

/**
 * Iterator that generates rates from a sequence of adjacent data points.
 * 
 * TODO - proper interval conversion. May not work for > 1hr
 * 
 * @since 3.0
 */
public class RateNumericArrayIterator implements QueryIterator, 
    TimeSeriesValue<NumericArrayType>, 
    NumericArrayType {
  
  /** A sequence of data points to compute rates. */
  private final Iterator<TimeSeriesValue<?>> source;
  
  /** Options for calculating rates. */
  private final RateConfig config;
  
  /** The query result with timespec. */
  private final QueryResult result;
  
  /** Whether or not the iterator has another real or filled value. */
  private boolean has_next;
  
  /** The value timestamp. */
  private TimeStamp timestamp;
  
  /** The long or double values. */
  private long[] long_values;
  private double[] double_values;
  
  /**
   * Constructs a {@link RateNumericArrayIterator} instance.
   * @param node The non-null query node.
   * @param result The non-null result.
   * @param sources The non-null map of sources.
   */
  public RateNumericArrayIterator(final QueryNode node, 
                                  final QueryResult result,
                                  final Map<String, TimeSeries> sources) {
    this(node, result, sources == null ? null : sources.values());
  }
  
  /**
   * Constructs a {@link RateNumericArrayIterator} instance.
   * @param node The non-null query node.
   * @param result The non-null result.
   * @param sources The non-null collection of sources.
   */
  public RateNumericArrayIterator(final QueryNode node, 
                                  final QueryResult result,
                                  final Collection<TimeSeries> sources) {
    if (node == null) {
      throw new IllegalArgumentException("Query node cannot be null.");
    }
    if (sources == null) {
      throw new IllegalArgumentException("Sources cannot be null.");
    }
    if (node.config() == null) {
      throw new IllegalArgumentException("Node config cannot be null.");
    }
    this.result = result;
    config = (RateConfig) node.config();
    final Optional<TypedTimeSeriesIterator> optional = 
        sources.iterator().next().iterator(NumericArrayType.TYPE);
    if (optional.isPresent()) {
      this.source = optional.get();
      has_next = true;
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
    has_next = false;
    final TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) source.next();
    timestamp = value.timestamp();
    int idx = value.value().offset() + 1;
    int write_idx = 1;
    
    // handle the delta only case.
    if (config.getDeltaOnly()) {
      if (value.value().isInteger()) {
        long_values = new long[value.value().end() - value.value().offset()];
        long[] source = value.value().longArray();
        // TODO - look at the rest values
        while (idx < value.value().end()) {
          long delta = source[idx] - source[idx - 1];
          if (config.getDropResets() && delta < 0) {
            long_values[write_idx++] = 0;
            idx++;
            continue;
          }
          
          long_values[write_idx++] = delta;
          idx++;
        }
      } else {
        double[] source = value.value().doubleArray();
        double_values = new double[value.value().end() - value.value().offset()];
        while (idx < value.value().end()) {
          double delta = source[idx] - source[idx - 1];
          if (config.getDropResets() && delta < 0) {
            double_values[write_idx++] = 0;
            idx++;
            continue;
          }
          
          double_values[write_idx++] = delta;
          idx++;
        }
      }
      return this;
    }
    
    // init doubles.
    double_values = new double[value.value().end() - value.value().offset()];
    double_values[0] = Double.NaN;
    
    // calculate the denom
    double denom = 
        (double) result.timeSpecification().interval().get(ChronoUnit.SECONDS) * 1000000000L /
        (double) config.duration().toNanos();
    
    double delta;
    if (value.value().isInteger()) {
      long[] source = value.value().longArray();
      while (idx < value.value().end()) {
        delta = (double) source[idx] - (double) source[idx - 1];
        if (config.isCounter() && delta < 0) {
          if (config.getDropResets()) {
            double_values[write_idx] = 0;
            write_idx++;
            idx++;
            continue;
          }
          
          delta = config.getCounterMax() + (double) source[idx] - (double) source[idx - 1];
          double_values[write_idx] = delta / denom;
          
          if (config.getResetValue() > RateOptions.DEFAULT_RESET_VALUE
            && double_values[write_idx] > config.getResetValue()) {
            double_values[write_idx] = 0;
          }
          write_idx++;
          idx++;
          continue;
        }
        
        double_values[write_idx] = delta / denom;
        
        if (config.getResetValue() > RateOptions.DEFAULT_RESET_VALUE
          && double_values[write_idx] > config.getResetValue()) {
          double_values[write_idx] = 0;
        }
        write_idx++;
        idx++;
      }
    } else {
      double[] source = value.value().doubleArray();
      while (idx < value.value().end()) {
        delta = source[idx] - source[idx - 1];
        if (config.isCounter() && delta < 0) {
          if (config.getDropResets()) {
            double_values[write_idx] = 0;
            write_idx++;
            idx++;
            continue;
          }
          
          delta = config.getCounterMax() + source[idx] - source[idx - 1];
          double_values[write_idx] = delta / denom;
          
          if (config.getResetValue() > RateOptions.DEFAULT_RESET_VALUE
            && double_values[write_idx] > config.getResetValue()) {
            double_values[write_idx] = 0;
          }
          write_idx++;
          idx++;
          continue;
        }
        
        double_values[write_idx] = delta / denom;
        
        if (config.getResetValue() > RateOptions.DEFAULT_RESET_VALUE
          && double_values[write_idx] > config.getResetValue()) {
          double_values[write_idx] = 0;
        }
        write_idx++;
        idx++;
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
}
