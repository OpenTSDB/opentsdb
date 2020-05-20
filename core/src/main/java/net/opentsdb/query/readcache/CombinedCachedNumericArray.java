// This file is part of OpenTSDB.
// Copyright (C) 2019-2020  The OpenTSDB Authors.
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
package net.opentsdb.query.readcache;

import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.util.Arrays;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.Op;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericArrayType;

/**
 * Handles splicing multiple cached arrays into one by allocating a new
 * array of the proper length, filling with NaNs when necessary and running
 * the copy in the ctor.
 * TODO - other fills.
 * 
 * <b>NOTE:</b>
 * This class assumes that all source arrays have the same length.
 *
 * @since 3.0
 */
public class CombinedCachedNumericArray implements 
    TypedTimeSeriesIterator<NumericArrayType>, 
    TimeSeriesValue<NumericArrayType>, 
    NumericArrayType {
  private static final Logger LOG = LoggerFactory.getLogger(
      CombinedCachedNumericArray.class);

  /** Write index into the array and used as the end. */
  protected int idx = 0;
  
  /** Whether or not the iterator was called. */
  protected boolean called = false;
  
  /** The two arrays, one of which will be null at any time. */
  protected long[] long_array;
  protected double[] double_array;
  
  /** A ref to the cache result. */
  protected final CombinedCachedResult result;
  
  /**
   * Default ctor.
   * @param result The non-null result.
   * @param series The non-null list of series.
   */
  CombinedCachedNumericArray(final CombinedCachedResult result, 
                             final TimeSeries[] series) {
    this.result = result;
    try {
      final int array_length = (int) ((result.timeSpecification().end().epoch() - 
          result.timeSpecification().start().epoch()) /
          result.timeSpecification().interval().get(ChronoUnit.SECONDS));
      
      final TemporalAmount duration = result.timeSpecification().interval();
      TimeStamp merged_ts = result.timeSpecification().start().getCopy();
      TimeStamp cache_ts;
      int idx = 0;
      
      // NOTE: We're assuming the incoming series are sorted in order (with possible
      // chunks missing) and that all underlying entries have the same interval.
      // Also that all series have array values.
      // TODO - Use longs vs doubles, etc.
      double_array = new double[array_length];
      Arrays.fill(double_array, Double.NaN);
      
      // assume alignment and arrays coming in
      for (int i = 0; i < series.length; i++) {
        if (series[i] == null) {
          continue;
        }
        
        final TimeSpecification series_spec = result.results()[i].timeSpecification();
        Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> optional = 
          series[i].iterator(NumericArrayType.TYPE);
        if (!optional.isPresent()) {
          continue;
        }
        
        final TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator = 
            (TypedTimeSeriesIterator<NumericArrayType>) optional.get();
        if (!iterator.hasNext()) {
          continue;
        }
        
        final TimeSeriesValue<NumericArrayType> v = 
            (TimeSeriesValue<NumericArrayType>) iterator.next();
        cache_ts = series_spec.start().getCopy();
        int cache_idx = v.value().offset();
        while (cache_ts.compare(Op.LT, merged_ts)) {
          cache_idx++;
          cache_ts.add(duration);
        }
        
        if (cache_idx >= v.value().end()) {
          continue;
        }
        
        if (cache_ts.compare(Op.GT, result.timeSpecification().end())) {
          break;
        }
        
        // valid data
        if (cache_ts.compare(Op.GT, merged_ts)) {
          while (merged_ts.compare(Op.LT, cache_ts)) {
            idx++;
            merged_ts.add(duration);
          }
        }
        
        while (cache_idx < v.value().end() && 
            cache_ts.compare(Op.LT, result.timeSpecification().end())) {
          if (v.value().isInteger()) {
            double_array[idx++] = v.value().longArray()[cache_idx++];
          } else {
            double_array[idx++] = v.value().doubleArray()[cache_idx++];
          }
          cache_ts.add(duration);
          merged_ts.add(duration);
        }
        
        series[i].close();
      } // end loop
      
      // catchup to the end of the result if needed.
      while (merged_ts.compare(Op.LT, result.timeSpecification().end())) {
        merged_ts.add(duration);
        idx++;
      }
      
      this.idx = idx;
    } catch (Throwable t) {
      LOG.error("Unexpected exception building combined cache result.", t);
      this.idx = 0;
      called = true;
    }
  }

  @Override
  public boolean hasNext() {
    return !called;
  }

  @Override
  public TimeSeriesValue<NumericArrayType> next() {
    called = true;
    return this;
  }

  @Override
  public TypeToken<NumericArrayType> getType() {
    return NumericArrayType.TYPE;
  }

  @Override
  public void close() {
    // no-op for now
  }
  
  @Override
  public int offset() {
    return 0;
  }

  @Override
  public int end() {
    return idx;
  }

  @Override
  public boolean isInteger() {
    return long_array != null;
  }

  @Override
  public long[] longArray() {
    return long_array;
  }

  @Override
  public double[] doubleArray() {
    return double_array;
  }

  @Override
  public TypeToken<NumericArrayType> type() {
    return NumericArrayType.TYPE;
  }

  @Override
  public TimeStamp timestamp() {
    return result.timeSpecification().start();
  }

  @Override
  public NumericArrayType value() {
    return this;
  }
}
