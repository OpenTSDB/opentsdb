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

import java.time.Duration;
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
    
    final TemporalAmount duration = Duration.of(result.resultInterval(), result.resultUnits());
    TimeStamp timestamp = result.timeSpecification().start().getCopy();
    timestamp.snapToPreviousInterval(result.resultInterval(), result.resultUnits());
    TimeStamp next_ts = timestamp.getCopy();
    next_ts.add(duration);
    final long interval_in_seconds = 
        result.timeSpecification().interval().get(ChronoUnit.SECONDS);
    final int intervals = (int) ((next_ts.epoch() - timestamp.epoch()) / interval_in_seconds);
    
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
      while (series_spec.start().epoch() > timestamp.epoch() && 
             series_spec.end().epoch() > timestamp.epoch()) {
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
        if (result.timeSpecification().start().epoch() > timestamp.epoch()) {
          idx += (((timestamp.epoch() + (series_spec.end().epoch() - 
                series_spec.start().epoch())) - 
                result.timeSpecification().start().epoch()) / 
              interval_in_seconds);
        } else {
          idx += ((series_spec.end().epoch() - series_spec.start().epoch()) / 
              interval_in_seconds);
        }
        timestamp.add(duration);
        next_ts.add(duration);
      }
      
      /** --------- ARRAY TYPE ------------ */
      if (iterator.getType() == NumericArrayType.TYPE) {
        int start_offset = 0;
        int length = 0;
        final TimeSeriesValue<NumericArrayType> value = (TimeSeriesValue<NumericArrayType>) v;
        
        try {  
          if (timestamp.epoch() > series_spec.start().epoch()) {
            start_offset = (int) ((timestamp.epoch() - series_spec.start().epoch()) 
                / interval_in_seconds);
          } else {
            start_offset = result.timeSpecification().start().epoch() > 
                series_spec.start().epoch() ?
                  (int) (value.value().offset() + 
                      (result.timeSpecification().start().epoch() - 
                          series_spec.start().epoch()) / interval_in_seconds)
                : value.value().offset();
          }
          
          length = Math.min(intervals, value.value().end() - start_offset);
          
          // we have some data to write.
          if (value.value().isInteger()) {
            if (long_array == null && double_array == null) {
              if (start_offset > 0) {
                // we're offset so we need to fill
                double_array = new double[array_length];
                Arrays.fill(double_array, Double.NaN);
                for (int x = start_offset; x < length; x++) {
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
                if (idx + (length - start_offset) > long_array.length) {
                  LOG.warn("Coding bug 1 wherein the index " + (idx + length - start_offset) 
                      + " was greater than the long array " + long_array.length);
                  length -= (idx + length - start_offset - long_array.length);
                }
                System.arraycopy(value.value().longArray(), start_offset, 
                    long_array, idx, length);
                idx += length;
              }
            } else if (double_array != null) {
              for (int x = start_offset; x < length; x++) {
                if (idx > double_array.length) {
                  LOG.warn("Coding bug 2 wherein the index " + idx 
                      + " was greater than the double array " + double_array.length);
                }
                double_array[idx++] = value.value().longArray()[x];
              }
            } else {
              if (idx + (length - start_offset) > long_array.length) {
                LOG.warn("Coding bug 2 wherein the index " + (idx + length - start_offset) 
                    + " was greater than the long array " + long_array.length);
                length -= (idx + length - start_offset - long_array.length);
              }
              System.arraycopy(value.value().longArray(), start_offset, 
                  long_array, idx, length);
              idx += length;
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
            if (idx + length > double_array.length) {
              LOG.warn("Coding bug 3 wherein the index " + (idx + length) 
                  + " was greater than the double array " + double_array.length);
              length -= (idx + length - double_array.length);
            } 
            if (length > value.value().end()) {
              LOG.warn("Coding but 6 wherein the end " + length + " is greater than "
                  + "the source array len: " + value.value().doubleArray().length);
              length = value.value().end() - start_offset;
            }
            
            // can happen if we have a tiny bit of data at the start of a segment
            // that is overlapping but not enough to add to the array.
            //if (end > start_offset) {
            System.arraycopy(value.value().doubleArray(), start_offset, 
                  double_array, idx, length);
              idx += length;
          }
        } catch (Throwable t) {
          StringBuilder buf = new StringBuilder()
              .append("[CCNA FAILURE] idx=")
              .append(idx)
              .append(", SO=")
              .append(start_offset)
              .append(", end=")
              .append(length)
              .append(", nextEpoch=")
              .append(timestamp.epoch())
              .append(", CCResultTimeSpec={ start=")
              .append(result.timeSpecification().start().epoch())
              .append(", end=")
              .append(result.timeSpecification().end().epoch())
              .append(", interval=")
              .append(result.timeSpecification().interval().toString())
              .append("}, IncomingTimeSpec={ start=")
              .append(series_spec.start().epoch())
              .append(", end=")
              .append(series_spec.end().epoch())
              .append(", interval=")
              .append(series_spec.interval().toString())
              .append("} ")
              .append("\n  LOCAL ARRAY=");
          if (long_array != null) {
            buf.append(Arrays.toString(long_array));
          } else {
            buf.append(Arrays.toString(double_array));
          }
          buf.append("\n, INCOMING=");
          if (value.value().isInteger()) {
            buf.append(Arrays.toString(value.value().longArray()));
          } else {
            buf.append(Arrays.toString(value.value().doubleArray()));
          }
          LOG.error(buf.toString(), t);
        }
      } else {
        /** --------- NUMERIC TYPE ------------ */
        TimeSeriesValue<NumericType> value = (TimeSeriesValue<NumericType>) v;
        // handle early cache expirations
        while (value != null && value.timestamp().epoch() < timestamp.epoch()) {
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
      timestamp.add(duration);
      next_ts.add(duration);
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
