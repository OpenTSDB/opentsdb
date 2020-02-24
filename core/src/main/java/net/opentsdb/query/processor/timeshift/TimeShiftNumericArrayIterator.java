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
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryIterator;
import net.opentsdb.query.QueryResult;

/**
 * Shifts a numeric array time series by the appropriate amount of time.
 * TODO - handle calendars.
 *
 * @since 3.0
 */
public class TimeShiftNumericArrayIterator implements QueryIterator,
    TimeSeriesValue<NumericArrayType> {
  /** The iterator. */
  private TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator;

  /** The result we'll use to get the offset. */
  private TimeShiftResult result;

  /** The NumericArray value of the timeseries. */
  private TimeSeriesValue value;

  TimeShiftNumericArrayIterator(final QueryResult result,
      final TimeSeries source) {
    this.result = (TimeShiftResult) result;
    final Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> optional =
        source.iterator(NumericArrayType.TYPE);
    if (optional.isPresent()) {
      iterator = optional.get();
    }
  }

  @Override
  public boolean hasNext() {
    return iterator == null ? false : iterator.hasNext();
  }

  @Override
  public TimeSeriesValue<? extends TimeSeriesDataType> next() {
    value =
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    return this;
  }

  @Override
  public TypeToken<? extends TimeSeriesDataType> getType() {
    return NumericArrayType.TYPE;
  }

  @Override
  public void close() {
    // no-op for now
  }
  
  @Override
  public TimeStamp timestamp() {
    return result.timeSpecification().start();
  }

  @Override
  public NumericArrayType value() {
    return (NumericArrayType) value.value();
  }

  @Override
  public TypeToken<NumericArrayType> type() {
    return value.type();
  }
}
