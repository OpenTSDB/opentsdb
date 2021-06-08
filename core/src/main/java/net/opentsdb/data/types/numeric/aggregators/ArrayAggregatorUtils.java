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
package net.opentsdb.data.types.numeric.aggregators;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericType;

import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.util.Optional;

/**
 * Utilities for numeric array aggregators.
 *
 * @since 3.0
 */
public class ArrayAggregatorUtils {

  private ArrayAggregatorUtils() {
    // you shall not instantiate!
  }

  /**
   * The state of the call into an accumulate method.
   */
  public enum AccumulateState {
    /** The iterator of the requested typewas not present for the given time
     * series. E.g. if {@link #accumulateInAggregatorArray(NumericArrayAggregator, TimeStamp, TimeStamp, TemporalAmount, TimeSeries)}
     * was called but no numeric array type iterator was present, this is thrown. */
    NOT_PRESENT,

    /** The iterator was present but didn't have a value when hasNext() was
     * called. */
    NO_VALUE,

    /** There was data but it was outside of the aggregator bounds. */
    OUT_OF_BOUNDS,

    /** At least one value was written within the boundary. */
    SUCCESS
  }

  /**
   * Writes the given time series into the proper location of the aggregation
   * array given that the time series has a {@link NumericArrayType} iterator.
   * The location in the aggregator array is determined by the timestamp of the
   * numeric array value so make sure that is correct when using this method.
   * <b>Invariants:</b>
   * <ul>
   *   <li>The aggEnd must be greater than the aggStart.</li>
   *   <li>AggEnd must be at least one interval greater than aggStart.</li>
   *   <li>The time series array iterator must have the same interval as the
   *   aggregator. Check that before using this method otherwise the aggregator
   *   will have garbage.</li>
   * </ul>
   * @param agg The non-null aggregator to write into.
   * @param aggStart The non-null starting timestamp of the aggregator array.
   * @param aggEnd The non-null ending timestamp of the aggregator array,
   *               exclusive.
   * @param interval The non-null interval between values in the two arrays.
   * @param timeseries The non-null time series to pull an iterator from.
   * @return A non-null state based on the aggregation.
   */
  public static AccumulateState accumulateInAggregatorArray(
          final NumericArrayAggregator agg,
          final TimeStamp aggStart,
          final TimeStamp aggEnd,
          final TemporalAmount interval,
          final TimeSeries timeseries) {

    final Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> op =
            timeseries.iterator(NumericArrayAggregator.TYPE);
    if (!op.isPresent()) {
      return AccumulateState.NOT_PRESENT;
    }
    final TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator = op.get();
    if (!iterator.hasNext()) {
      return AccumulateState.NO_VALUE;
    }

    final TimeSeriesValue<NumericArrayType> value =
            (TimeSeriesValue<NumericArrayType>) iterator.next();
    if (value.timestamp().compare(TimeStamp.Op.GTE, aggEnd)) {
      return AccumulateState.OUT_OF_BOUNDS;
    }

    // TODO - only handling seconds for now. need ms and nanos someday.
    TimeStamp currentTs = value.timestamp().getCopy();
    int aggIndex = (int) ((value.timestamp().epoch() - aggStart.epoch()) /
            interval.get(ChronoUnit.SECONDS));
    int arrayIndex = value.value().offset();
    // make sure we move to the start of the agg index if we have data before
    // the interval.
    while (aggIndex < 0 && arrayIndex < value.value().end() &&
            currentTs.compare(TimeStamp.Op.LT, aggEnd)) {
      currentTs.add(interval);
      ++aggIndex;
      ++arrayIndex;
    }
    if (aggIndex < 0) {
      return AccumulateState.OUT_OF_BOUNDS;
    }

    int wrote = 0;
    if (value.value().isInteger()) {
      long[] array = value.value().longArray();
      while (arrayIndex < value.value().end() &&
             currentTs.compare(TimeStamp.Op.LT, aggEnd)) {
        agg.accumulate(array[arrayIndex++], aggIndex++);
        currentTs.add(interval);
        ++wrote;
      }
    } else {
      double[] array = value.value().doubleArray();
      while (arrayIndex < value.value().end() &&
             currentTs.compare(TimeStamp.Op.LT, aggEnd)) {
        agg.accumulate(array[arrayIndex++], aggIndex++);
        currentTs.add(interval);
        ++wrote;
      }
    }

    return wrote > 0 ? AccumulateState.SUCCESS : AccumulateState.OUT_OF_BOUNDS;
  }

  /**
   * Writes the given time series into the proper location of the aggregation
   * array given that the time series has a {@link NumericType} iterator.
   * This method <b>does not</b> perform any de-duplication of the incoming
   * values, rather it's meant for series that were downsampled to the same
   * spec as the aggregating array but may be missing some intervals or just
   * didn't convert to a numeric array.
   * <b>Invariants:</b>
   * <ul>
   *   <li>The aggEnd must be greater than the aggStart.</li>
   *   <li>AggEnd must be at least one interval greater than aggStart.</li>
   *   <li>The time series data must have the same interval as the aggregator
   *   and that there are not any duplicate values or values between intervals.
   *   Check that before using this method otherwise the aggregator will have
   *   garbage.</li>
   * </ul>
   * @param agg The non-null aggregator to write into.
   * @param aggStart The non-null starting timestamp of the aggregator array.
   * @param aggEnd The non-null ending timestamp of the aggregator array,
   *               exclusive.
   * @param interval The non-null interval between values in the two arrays.
   * @param timeseries The non-null time series to pull an iterator from.
   * @return A non-null state based on the aggregation.
   */
  public static AccumulateState accumulateInAggregatorNumeric(
          final NumericArrayAggregator agg,
          final TimeStamp aggStart,
          final TimeStamp aggEnd,
          final TemporalAmount interval,
          final TimeSeries timeseries) {

    final Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> op =
            timeseries.iterator(NumericType.TYPE);
    if (!op.isPresent()) {
      return AccumulateState.NOT_PRESENT;
    }
    final TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator = op.get();
    if (!iterator.hasNext()) {
      return AccumulateState.NO_VALUE;
    }

    int wrote = 0;
    final long intervalSeconds = interval.get(ChronoUnit.SECONDS);
    while (iterator.hasNext()) {
      final TimeSeriesValue<NumericType> value =
              (TimeSeriesValue<NumericType>) iterator.next();
      if (value.timestamp().compare(TimeStamp.Op.LT, aggStart)) {
        continue;
      }
      if (value.timestamp().compare(TimeStamp.Op.GTE, aggEnd)) {
        return wrote > 0 ? AccumulateState.SUCCESS :
                AccumulateState.OUT_OF_BOUNDS;
      }
      if (value.value() == null) {
        continue;
      }

      int aggIndex = (int) ((value.timestamp().epoch() - aggStart.epoch()) /
              intervalSeconds);
      if (value.value().isInteger()) {
        agg.accumulate(value.value().longValue(), aggIndex);
      } else {
        agg.accumulate(value.value().doubleValue(), aggIndex);
      }
      ++wrote;
    }
    return wrote > 0 ? AccumulateState.SUCCESS : AccumulateState.OUT_OF_BOUNDS;
  }
}
