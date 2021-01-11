// This file is part of OpenTSDB.
// Copyright (C) 2021  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.timedifference;

import java.io.IOException;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collection;
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

/**
 * Computes the time delta on numeric arrays.
 * 
 * @since 3.0
 */
public class TimeDifferenceNumericArrayIterator implements QueryIterator,
    TimeSeriesValue<NumericArrayType>, NumericArrayType {
  
  private final ChronoUnit resolution;
  
  private final TimeStamp timestamp;
  private boolean has_next;
  private double[] double_values;

  TimeDifferenceNumericArrayIterator(final QueryNode node, 
                                     final QueryResult result,
                                     final Map<String, TimeSeries> sources) {
    this(node, result, sources == null ? null : sources.values());
  }
  
  TimeDifferenceNumericArrayIterator(final QueryNode node, 
                                     final QueryResult result,
                                     final Collection<TimeSeries> sources) {
    resolution = ((TimeDifferenceConfig) node.config()).getResolution();
    timestamp = result.timeSpecification().start();
    final Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> op = 
        sources.iterator().next().iterator(NumericArrayType.TYPE);
    if (op.isPresent()) {
      final TypedTimeSeriesIterator<NumericArrayType> iterator = 
          (TypedTimeSeriesIterator<NumericArrayType>) op.get();
      if (iterator.hasNext()) {
        final TimeSeriesValue<NumericArrayType> value = 
            iterator.next();
        // may as well compute here.
        // Note the result is always a double as we have a NaN as our first 
        // data point always for now
        // TODO - padding! If we can pad we can use longs or doubles and not
        // return the darn nan.
        // TODO - pool
        double_values = new double[value.value().end() - value.value().offset()];
        final TimeStamp last_ts = result.timeSpecification().start().getCopy();
        last_ts.updateEpoch(-1L);
        final TimeStamp next_ts = result.timeSpecification().start().getCopy();
        int write_idx = 0;
        double_values[write_idx++] = Double.NaN;
        
        long delta = -1;
        if (value.value().isInteger()) {
          last_ts.update(next_ts); // first dp is valid.
          next_ts.add(result.timeSpecification().interval());
          // we can just fill and update the first again since we have values 
          // all the way through the long aray.
          double fill = Double.NaN;
          switch (resolution) {
          case HOURS:
            fill = next_ts.epoch() - last_ts.epoch();
            fill /= (60d * 60d);
            break;
          case MINUTES:
            fill = next_ts.epoch() - last_ts.epoch();
            fill /= 60d;
            break;
          case SECONDS:
            fill = next_ts.epoch() - last_ts.epoch();
            break;
          case MILLIS:
            fill = next_ts.msEpoch() - last_ts.msEpoch();
            break;
          case NANOS:
            fill = next_ts.epoch() - last_ts.epoch();
            fill *= 1000 * 1000 * 1000;
            fill += (next_ts.nanos() - last_ts.nanos());
            break;
          default:
            throw new IllegalStateException("Shouldn't be here! Bad resolution: " 
                + resolution);
          }
          
          Arrays.fill(double_values, fill);
          double_values[0] = Double.NaN;
        } else {
          // double
          double[] v = value.value().doubleArray();
          if (!Double.isNaN(v[value.value().offset()])) {
            last_ts.update(result.timeSpecification().start());
          }
          
          for (int i = value.value().offset() + 1; i < value.value().end(); i++) {
            next_ts.add(result.timeSpecification().interval());
            if (Double.isNaN(v[i])) {
              double_values[write_idx++] = Double.NaN;
              continue;
            }
            
            if (last_ts.epoch() < 0) {
              last_ts.update(next_ts);
              double_values[write_idx++] = Double.NaN;
              continue;
            }
            
            switch (resolution) {
            case HOURS:
              double d = next_ts.epoch() - last_ts.epoch();
              d /= 60d / 60d;
              double_values[write_idx++] = d;
              break;
            case MINUTES:
              double dm = next_ts.epoch() - last_ts.epoch();
              dm /= 60d;
              double_values[write_idx++] = dm;
              break;
            case SECONDS:
              delta = next_ts.epoch() - last_ts.epoch();
              break;
            case MILLIS:
              delta = next_ts.msEpoch() - last_ts.msEpoch();
              break;
            case NANOS:
              delta = next_ts.epoch() - last_ts.epoch();
              delta *= 1000 * 1000 * 1000;
              delta += (next_ts.nanos() - last_ts.nanos());
              break;
            default:
              throw new IllegalStateException("Shouldn't be here! Bad resolution: " 
                  + resolution);
            }
            if (delta >= 0) {
              double_values[write_idx++] = delta;
            }
            last_ts.update(next_ts);
          }
        }
        
        has_next = true;
      }
    }
  }
  
  @Override
  public TypeToken getType() {
    return NumericArrayType.TYPE;
  }

  @Override
  public boolean hasNext() {
    return has_next;
  }

  @Override
  public TimeSeriesValue<NumericArrayType> next() {
    has_next = false;
    return this;
  }

  @Override
  public void close() throws IOException {
    // no-op here till we pool.
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
    return double_values.length;
  }

  @Override
  public boolean isInteger() {
    return false;
  }

  @Override
  public long[] longArray() {
    return null;
  }

  @Override
  public double[] doubleArray() {
    return double_values;
  }

}
