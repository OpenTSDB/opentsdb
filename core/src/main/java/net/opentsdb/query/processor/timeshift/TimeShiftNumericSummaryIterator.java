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
package net.opentsdb.query.processor.timeshift;

import java.util.Optional;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.MutableNumericSummaryValue;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.query.QueryIterator;
import net.opentsdb.query.QueryResult;

public class TimeShiftNumericSummaryIterator implements QueryIterator {
  /** The iterator. */
  private TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator;
  
  /** The DP we'll update. */
  private MutableNumericSummaryValue dp;
  
  /** The result we'll use to get the offset. */
  private TimeShiftResult result;
  
  TimeShiftNumericSummaryIterator(final QueryResult result, 
                                  final TimeSeries source) {
    this.result = (TimeShiftResult) result;
    final Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> optional =
        source.iterator(NumericSummaryType.TYPE);
    if (optional.isPresent()) {
      iterator = optional.get();
      dp = new MutableNumericSummaryValue();
    }    
  }
  
  @Override
  public boolean hasNext() {
    return iterator == null ? false : iterator.hasNext();
  }
  
  @Override
  public TimeSeriesValue<? extends TimeSeriesDataType> next() {
    final TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
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
    return NumericSummaryType.TYPE;
  }
  
}
