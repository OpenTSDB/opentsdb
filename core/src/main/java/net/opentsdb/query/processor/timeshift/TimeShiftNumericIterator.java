// This file is part of OpenTSDB.
// Copyright (C) 2015-2019  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.timeshift;

import java.util.Iterator;
import java.util.Optional;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryIterator;
import net.opentsdb.query.QueryResult;

/**
 * Shifts a numeric time series by the appropriate amount of time.
 * TODO - handle calendars.
 * 
 * @since 3.0
 */
public class TimeShiftNumericIterator implements QueryIterator {
  /** The iterator. */
  private Iterator<TimeSeriesValue<?>> iterator;
  
  /** The DP we'll update. */
  private MutableNumericValue dp;
  
  /** The result we'll use to get the offset. */
  private TimeShiftResult result;
  
  TimeShiftNumericIterator(final QueryResult result, 
                           final TimeSeries source) {
    this.result = (TimeShiftResult) result;
    final Optional<TypedTimeSeriesIterator> optional = 
        source.iterator(NumericType.TYPE);
    if (optional.isPresent()) {
      iterator = optional.get();
      dp = new MutableNumericValue();
    }    
  }
  
  @Override
  public boolean hasNext() {
    return iterator == null ? false : iterator.hasNext();
  }
  
  @Override
  public TimeSeriesValue<? extends TimeSeriesDataType> next() {
    final TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    dp.reset(value);
    if (result.isPrevious()) {
      dp.timestamp().add(result.amount());
    } else {
      dp.timestamp().subtract(result.amount());
    }
    
    return dp;
  }

  @Override
  public TypeToken<? extends TimeSeriesDataType> getType() {
    return NumericType.TYPE;
  }
  
}
