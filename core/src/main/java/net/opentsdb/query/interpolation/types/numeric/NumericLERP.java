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
package net.opentsdb.query.interpolation.types.numeric;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.TimeStampComparator;
import net.opentsdb.data.types.numeric.MutableNumericType;
import net.opentsdb.data.types.numeric.NumericType;

/**
 * An interpolator for numeric data points that implements linear 
 * interpolation. This interpolator also supports filling before or after
 * the values within an iterator.
 * <p>
 * If {@link #first_or_last} is set to true, then values for times earlier than
 * the first available data point will be filled with the first data point and
 * values later than the last point are filled with the last point. Otherwise
 * if false, the fill policy is used.
 * <p>
 * Note that if a source is passed in that either does not have a numeric type
 * iterator or the numeric iterator has no data, the fill policy value will 
 * always be returned.
 * 
 * @since 3.0
 */
public class NumericLERP extends NumericInterpolator {
  
  /**
   * Default ctor.
   * @param source A non-null source.
   * @param config The non-null config for the interpolator.
   * @throws IllegalArgumentException if the source or config were null.
   */
  public NumericLERP(final TimeSeries source, 
                     final NumericInterpolatorConfig config) {
    super(source, config);
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public TimeSeriesValue<NumericType> next(final TimeStamp timestamp) {
    // if the iterator is null or it was empty, the next is null and we just 
    // pass the fill value.
    if (iterator == null || next == null) {
      return fill(timestamp);
    }
    
    has_next = false;
    if (timestamp.compare(TimeStampComparator.EQ, next.timestamp())) {
      if (previous == null) {
        previous = new MutableNumericType(next);
      } else {
        previous.reset(next);
      }
      if (iterator.hasNext()) {
        next = (TimeSeriesValue<NumericType>) iterator.next();
        has_next = true;
      } else {
        next = null;
      }
      return previous;
    } else if (timestamp.compare(TimeStampComparator.LT, next.timestamp())) {
      // lerp the lerp!
      if (previous == null) {
        if (next != null || iterator.hasNext()) {
          has_next = true;
        }
        
        // ... except if we don't have a previous value, then we need to fill
        // and advance to the next.
        return fill(timestamp);
      }
      
      final long ts_delta = next.timestamp().msEpoch() - 
          previous.timestamp().msEpoch();
      if (!next.value().isInteger() || !previous.value().isInteger()) {
        // if either values is a double, then we have to use a double.
        final double interpolation = previous.toDouble() + 
            ((timestamp.msEpoch() - previous.timestamp().msEpoch()) * 
                (next.value().toDouble() - previous.value().toDouble()) /
                  (double) ts_delta);
        response.reset(timestamp, interpolation);
      } else {
        // attempt to maintain precision in those rare instances when we can.
        final long interpolation;
        final long value_delta = next.value().longValue() - 
            previous.value().longValue();
        if (ts_delta > value_delta || 
            (((double) value_delta) / (double) ts_delta) % 1 != 0) {
          interpolation = previous.longValue() + ((long)  
              ((timestamp.msEpoch() - previous.timestamp().msEpoch()) * 
                  ((double) value_delta) / (double) ts_delta));
        } else {
          interpolation = previous.longValue() + 
              ((timestamp.msEpoch() - previous.timestamp().msEpoch()) * 
                  ((next.value().longValue() - previous.value().longValue()) / 
                      (next.timestamp().msEpoch() - previous.timestamp().msEpoch())));
        }
        response.reset(timestamp, interpolation);
      }
      if (next != null) {
        has_next = true;
      }
      return response;
    } else {
      if (next != null || iterator.hasNext()) {
        has_next = true;
      }
      
      // we're after the final value in the iterator so fill
      return fill(timestamp);
    }
  }

}
