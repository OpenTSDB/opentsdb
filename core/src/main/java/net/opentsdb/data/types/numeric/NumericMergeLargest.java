// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.data.types.numeric;

import java.util.List;
import java.util.NoSuchElementException;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.DataShard;
import net.opentsdb.data.DataShardMergeStrategy;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.TimeStampComparator;
import net.opentsdb.data.iterators.TimeSeriesIterator;

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
  public DataShard<NumericType> merge(final TimeSeriesId id, 
      final List<DataShard<?>> shards) {
    if (id == null) {
      throw new IllegalArgumentException("The ID cannot be null.");
    }
    if (shards == null) {
      throw new IllegalArgumentException("Shards list cannot be null.");
    }
    final NumericMillisecondShard shard = 
        new NumericMillisecondShard(id, 
        shards.get(0).startTime(), shards.get(0).endTime());
    
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
      iterators[i] = (TimeSeriesIterator<NumericType>) shards.get(i).iterator();
      try {
        values[i] = iterators[i].next();
        if (last_ts == null) {
          last_ts = new MillisecondTimeStamp(values[i].timestamp().msEpoch());
        } else {
          if (values[i].timestamp().compare(TimeStampComparator.LT, last_ts)) {
            last_ts.update(values[i].timestamp());
          }
        }
      } catch (NoSuchElementException e) { }
    }
    
    if (last_ts == null) {
      // no data was present. Return the shard
      return shard;
    }
    
    final MutableNumericType v = new MutableNumericType(shard.id());
    final TimeStamp next = new MillisecondTimeStamp(Long.MAX_VALUE);
    // loop till all the values are nulled out.
    while (true) {
      next.setMax();
      int had_value = 0;
      v.reset(last_ts, Double.MIN_VALUE, 0);
      for (int i = 0; i < values.length; i++) {
        if (values[i] == null) {
          continue;
        }
        had_value++;
        if (values[i].timestamp().compare(TimeStampComparator.EQ, last_ts)) {
          if (had_value == 1) {
            // start with the first value.
            v.reset(values[i]);
          } else if (Double.isFinite(values[i].value().toDouble()) && 
              values[i].value().toDouble() > v.toDouble()) {
            v.reset(values[i]);
          }
          
          try {
            values[i] = iterators[i].next();
            if (values[i].timestamp().compare(TimeStampComparator.LT, next)) {
              next.update(values[i].timestamp());
            }
          } catch (NoSuchElementException e) {
            values[i] = null;
          }
        } else if (values[i].timestamp().compare(TimeStampComparator.LT, next)) {
          next.update(values[i].timestamp());
        }
      }
      
      if (had_value < 1) {
        break;
      }
      
      last_ts.update(next);
      if (v.isInteger()) {
        shard.add(v.timestamp().msEpoch(), v.longValue(), v.realCount());
      } else if (Double.isFinite(v.doubleValue())) {
        shard.add(v.timestamp().msEpoch(), v.doubleValue(), v.realCount());
      } else {
        shard.add(v.timestamp().msEpoch(), Double.NaN, v.realCount());
      }
    }
    
    return shard;
  }

}
