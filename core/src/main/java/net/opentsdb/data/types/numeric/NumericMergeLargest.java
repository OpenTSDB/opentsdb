// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
package net.opentsdb.data.types.numeric;

import java.util.List;

import com.google.common.reflect.TypeToken;

import io.opentracing.Span;
import net.opentsdb.data.DataShardMergeStrategy;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.RelationalOperator;
import net.opentsdb.data.iterators.IteratorStatus;
import net.opentsdb.data.iterators.TimeSeriesIterator;
import net.opentsdb.query.context.QueryContext;

/**
 * Chooses the largest value from amongst the numeric values regardless of
 * the real value counts. If any values are {@link Double#NaN} or 
 * {@link Double#POSITIVE_INFINITY} or {@link Double#NEGATIVE_INFINITY} then the
 * real values are preferred and infinities dropped.
 * <p>
 * If all values for a timestamp are {@link Double#NaN} then the result will be
 * {@link Double#NaN}. If all of the values are infinities, they are converted
 * to a {@link Double#NaN}. 
 * TODO - add an option to handle infinities differently as well as possibly
 * overriding with the real counts.
 * 
 * @since 3.0
 */
public class NumericMergeLargest implements DataShardMergeStrategy<NumericType> {

  @Override
  public TypeToken<NumericType> type() {
    return NumericType.TYPE;
  }

  @SuppressWarnings("unchecked")
  @Override
  public TimeSeriesIterator<NumericType> merge(final TimeSeriesStringId id, 
                                      final List<TimeSeriesIterator<?>> shards,
                                      final QueryContext context,
                                      final Span tracer_span) {
    if (id == null) {
      throw new IllegalArgumentException("The ID cannot be null.");
    }
    if (shards == null) {
      throw new IllegalArgumentException("Shards list cannot be null.");
    }
    if (context == null) {
      throw new IllegalArgumentException("Query context cannot be null.");
    }
    final NumericMillisecondShard shard = 
        new NumericMillisecondShard(id, 
        shards.get(0).startTime(), shards.get(0).endTime(),
        shards.get(0).order());
    
    final TimeSeriesValue<NumericType>[] values = 
        new TimeSeriesValue[shards.size()];
    final TimeSeriesIterator<NumericType>[] iterators = 
        new TimeSeriesIterator[shards.size()];
    TimeStamp last_ts = null;
    
    for (int i = 0; i < shards.size(); i++) {
      if (shards.get(i) == null) {
        throw new IllegalArgumentException("One shard in the list was null.");
      }
      if (!shards.get(i).type().equals(NumericType.TYPE)) {
        throw new IllegalArgumentException("One of the shards had the wrong "
            + "type: " + shards.get(i).type());
      }
      iterators[i] = (TimeSeriesIterator<NumericType>) shards.get(i);
      if (iterators[i].status() == IteratorStatus.HAS_DATA) {
        values[i] = iterators[i].next();
        if (last_ts == null) {
          last_ts = new MillisecondTimeStamp(values[i].timestamp().msEpoch());
        } else {
          if (values[i].timestamp().compare(RelationalOperator.LT, last_ts)) {
            last_ts.update(values[i].timestamp());
          }
        }
      }
    }
    
    if (last_ts == null) {
      // no data was present. Return the shard
      if (tracer_span != null) {
        tracer_span.setTag("totalValues", 0);
        //tracer_span.setTag("totalValues", 0);
      }
      //return shard;
      return null;
    }
    
    // counters for tracing
    int total_values = 0;
    int non_finites = 0;
    int differences = 0;
    
    final MutableNumericValue v = new MutableNumericValue();
    final TimeStamp next = new MillisecondTimeStamp(Long.MAX_VALUE);
    // loop till all the values are nulled out.
    while (true) {
      next.setMax();
      int had_value = 0;
      v.reset(last_ts, Double.MIN_VALUE);
      for (int i = 0; i < values.length; i++) {
        if (values[i] == null) {
          continue;
        }
        ++total_values;
        had_value++;
        if (values[i].timestamp().compare(RelationalOperator.EQ, last_ts)) {
          if (had_value == 1) {
            // start with the first value.
            v.reset(values[i]);
            if (!values[i].value().isInteger() && 
                !Double.isFinite(values[i].value().doubleValue())) {
              ++non_finites;
            }
          } else if (Double.isFinite(values[i].value().toDouble()) && 
              values[i].value().toDouble() > v.toDouble()) {
            v.reset(values[i]);
            ++differences;
          }
          
          if (iterators[i].status() == IteratorStatus.HAS_DATA) {
            values[i] = iterators[i].next();
            if (values[i].timestamp().compare(RelationalOperator.LT, next)) {
              next.update(values[i].timestamp());
            }
          } else {
            values[i] = null;
          }
        } else if (values[i].timestamp().compare(RelationalOperator.LT, next)) {
          next.update(values[i].timestamp());
        }
      }
      
      if (had_value < 1) {
        break;
      }
      
      last_ts.update(next);
      if (v.isInteger()) {
        shard.add(v.timestamp().msEpoch(), v.longValue());
      } else if (Double.isFinite(v.doubleValue())) {
        shard.add(v.timestamp().msEpoch(), v.doubleValue());
      } else {
        shard.add(v.timestamp().msEpoch(), Double.NaN);
      }
    }
    
    if (tracer_span != null) {
      tracer_span.setTag("totalValues", total_values);
      tracer_span.setTag("nonFiniteValues", non_finites);
      tracer_span.setTag("differentValues", differences);
    }
    //return shard;
    return null;
  }

}
