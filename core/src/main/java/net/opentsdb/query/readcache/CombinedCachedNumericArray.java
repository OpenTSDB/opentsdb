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
package net.opentsdb.query.readcache;

import java.time.temporal.ChronoUnit;
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
import net.opentsdb.data.types.numeric.NumericType;

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
    final int array_length = (int) ((result.timeSpecification().end().epoch() - 
        result.timeSpecification().start().epoch()) /
        result.timeSpecification().interval().get(ChronoUnit.SECONDS));
    
    TimeStamp timestamp = result.timeSpecification().start().getCopy();
    timestamp.snapToPreviousInterval(result.resultInterval(), result.resultUnits());
    
    final long interval_in_seconds = 
        result.timeSpecification().interval().get(ChronoUnit.SECONDS);
    long next_epoch = timestamp.epoch();
    for (int i = 0; i < series.length; i++) {
      if (series[i] == null) {
        continue;
      }
      
      final TimeSpecification series_spec = result.results()[i].timeSpecification();
      // ok, so this is temporary and ugly cause we convert numerictype to arrays
      // in serdes sometimes since we haven't implemented every agg as an array
      // aggregator. So we can have arrays OR NumericTypes here.
      Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> optional = 
          series[i].iterator(NumericArrayType.TYPE);
      if (!optional.isPresent()) {
        optional = series[i].iterator(NumericType.TYPE);
      }
      
      final TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator = 
          (TypedTimeSeriesIterator<NumericArrayType>) optional.get();
      if (!iterator.hasNext()) {
        continue;
      }
      
      final TimeSeriesValue<? extends TimeSeriesDataType> v = iterator.next();
      while (series_spec.start().epoch() > next_epoch && 
             series_spec.end().epoch() > next_epoch) {
        // fill
        if (double_array == null) {
          double_array = new double[array_length];
          Arrays.fill(double_array, Double.NaN);
          if (long_array != null) {
            // flip
            for (int x = 0; x < idx; x++) {
              double_array[x] = long_array[x];
            }
            long_array = null;
          }
        }
        
        // edge case if the query is not aligned to the cache interval
        if (result.timeSpecification().start().epoch() > next_epoch) {
          idx += (((next_epoch + (series_spec.end().epoch() - 
                series_spec.start().epoch())) - 
                result.timeSpecification().start().epoch()) / 
              interval_in_seconds);
        } else {
          idx += ((series_spec.end().epoch() - series_spec.start().epoch()) / 
              interval_in_seconds);
        }
        next_epoch += series_spec.end().epoch() - series_spec.start().epoch();
      }
      
      /** --------- ARRAY TYPE ------------ */
      if (iterator.getType() == NumericArrayType.TYPE) {
        final TimeSeriesValue<NumericArrayType> value = (TimeSeriesValue<NumericArrayType>) v;

        int start_offset;
        if (next_epoch > series_spec.start().epoch()) {
          start_offset = (int) ((next_epoch - series_spec.start().epoch()) / interval_in_seconds);
        } else {
          start_offset = result.timeSpecification().start().epoch() > 
              series_spec.start().epoch() ?
              (int) (value.value().offset() + (result.timeSpecification().start().epoch() - 
                  series_spec.start().epoch()) / interval_in_seconds)
              : value.value().offset();
        }
        int end = series_spec.end().compare(Op.GT, result.timeSpecification().end()) ?
            (int) (value.value().end() - (series_spec.end().epoch() - 
                  result.timeSpecification().end().epoch()) / 
                interval_in_seconds) - start_offset
            : value.value().end() - start_offset;
        
        // we have some data to write.
        if (value.value().isInteger()) {
          if (long_array == null && double_array == null) {
            if (start_offset > 0) {
              // we're offset so we need to fill
              double_array = new double[array_length];
              Arrays.fill(double_array, Double.NaN);
              for (int x = start_offset; x < end; x++) {
                if (idx > double_array.length) {
                  LOG.warn("Coding bug 1 wherein the index " + idx 
                      + " was greater than the double array " + double_array.length);
                  break;
                }
                double_array[idx++] = value.value().longArray()[x];
              }
              long_array = null;
            } else {
              // start with a long array
              long_array = new long[array_length];
              if (idx + (end - start_offset) > long_array.length) {
                LOG.warn("Coding bug 1 wherein the index " + (idx + end - start_offset) 
                    + " was greater than the long array " + long_array.length);
                end -= (idx + end - start_offset - long_array.length);
              }
              System.arraycopy(value.value().longArray(), start_offset, 
                  long_array, idx, end);
              idx += end;
            }
          } else if (double_array != null) {
            for (int x = start_offset; x < end; x++) {
              if (idx > double_array.length) {
                LOG.warn("Coding bug 2 wherein the index " + idx 
                    + " was greater than the double array " + double_array.length);
              }
              double_array[idx++] = value.value().longArray()[x];
            }
          } else {
            if (idx + (end - start_offset) > long_array.length) {
              LOG.warn("Coding bug 2 wherein the index " + (idx + end - start_offset) 
                  + " was greater than the long array " + long_array.length);
              end -= (idx + end - start_offset - long_array.length);
            }
            System.arraycopy(value.value().longArray(), start_offset, 
                long_array, idx, end);
            idx += end;
          }
        } else {
          if (double_array == null) {
            // flip
            double_array = new double[array_length];
            Arrays.fill(double_array, Double.NaN);
            for (int x = 0; x < idx; x++) {
              double_array[x] = long_array[x];
            }
            long_array = null;
          }
          if (idx + end > double_array.length) {
            LOG.warn("Coding bug 3 wherein the index " + (idx + end) 
                + " was greater than the double array " + double_array.length);
            end -= (idx + end - double_array.length);
          } if (end > value.value().doubleArray().length) {
            LOG.warn("Coding but 6 wherein the end " + end + " is greater than "
                + "the source array len: " + value.value().doubleArray().length);
            end = value.value().doubleArray().length - start_offset;
          }
          
          System.arraycopy(value.value().doubleArray(), start_offset, 
              double_array, idx, end);
          idx += end;
        }
      } else {
        /** --------- NUMERIC TYPE ------------ */
        TimeSeriesValue<NumericType> value = (TimeSeriesValue<NumericType>) v;
        // handle early cache expirations
        while (value != null && value.timestamp().epoch() < next_epoch) {
          if (iterator.hasNext()) {
            value = (TimeSeriesValue<NumericType>) iterator.next(); 
          } else {
            value = null;
          }
        }
        
        do {
          if (value == null) {
            // could happen if we fast-forwarded
            break;
          }
          
          if (value.value() == null) {
            if (double_array == null) {
              // flip
              double_array = new double[array_length];
              Arrays.fill(double_array, Double.NaN);
              for (int x = 0; x < idx; x++) {
                double_array[x] = long_array[x];
              }
              long_array = null;
            }
            idx++;
          } else if (value.value().isInteger()) {
            if (long_array == null) {
              if (idx > double_array.length) {
                LOG.warn("Coding bug 4 wherein the numeric type index " + idx 
                    + " was greater than the double array " + double_array.length);
              } else {
                double_array[idx++] = value.value().toDouble();
              }
            } else {
              if (idx > long_array.length) {
                LOG.warn("Coding bug 4 wherein the numeric type index " + idx 
                    + " was greater than the long array " + long_array.length);
              } else {
                long_array[idx++] = value.value().longValue();
              }
            }
          } else {
            if (double_array == null) {
              // flip
              double_array = new double[array_length];
              Arrays.fill(double_array, Double.NaN);
              for (int x = 0; x < idx; x++) {
                double_array[x++] = long_array[x];
              }
              long_array = null;
            }
            if (idx >= double_array.length) {
              LOG.warn("Coding bug 5 wherein the numeric type index " + idx 
                  + " was greater than the double array " + double_array.length + " => " + value);
              break;
            } else {
              double_array[idx++] = value.value().toDouble();
            }
          }
          
          if (iterator.hasNext()) {
            value = (TimeSeriesValue<NumericType>) iterator.next();
            if (value.timestamp().compare(Op.GTE, result.timeSpecification().end()) ||
                value.timestamp().compare(Op.GTE, series_spec.end())) {
              value = null;
            }
          } else {
            value = null;
          }
        } while (value != null);
      }
      
      series[i].close();
      next_epoch += series_spec.end().epoch() - series_spec.start().epoch();
    }
    
    // adjust in case we were missing data at the end of the interval.
    if (long_array != null && idx < long_array.length) {
      // we missed some data at the end so we flip to Double and fill.
      double_array = new double[array_length];
      for (int i = 0; i < idx; i++) {
        double_array[i] = long_array[i];
      }
      long_array = null;
      Arrays.fill(double_array, idx, array_length, Double.NaN);
    }
    idx = array_length;
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
