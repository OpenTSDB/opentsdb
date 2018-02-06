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
package net.opentsdb.query.interpolation.types.numeric;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.RelationalOperator;
import net.opentsdb.data.types.numeric.MutableNumericType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryFillPolicy;
import net.opentsdb.query.QueryIteratorInterpolator;

/**
 * An interpolator for numeric data points that fills with the given 
 * {@link QueryFillPolicy} or the next/last real value when appropriate.
 * <p>
 * If {@link #first_or_last} is set to true, then when a value is missing for a
 * given timestamp, the value is filled with the next real value. For timestamps
 * after the <i>last</i> real value, the last value is used.
 * <p>
 * Note that if a source is passed in that either does not have a numeric type
 * iterator or the numeric iterator has no data, the fill policy value will 
 * always be returned.
 * 
 * @since 3.0
 */
public class NumericInterpolator implements QueryIteratorInterpolator<NumericType> {
  
  /** The config. */
  protected final NumericInterpolatorConfig config;
  
  /** The fill policy to use for timestamps before or after the values within
   * the iterator range if applicable */
  protected final QueryFillPolicy<NumericType> fill_policy;
  
  /** The iterator pulled from the source. May be null. */
  protected final Iterator<TimeSeriesValue<?>> iterator;
  
  /** The previous real value. */
  protected MutableNumericType previous;
  
  /** The next real value. */
  protected TimeSeriesValue<NumericType> next;
  
  /** The value filled when lerping. */
  protected MutableNumericType response;
  
  /** Whether or not the source iterator has more data. */
  protected boolean has_next;
  
  @SuppressWarnings("unchecked")
  public NumericInterpolator(final TimeSeries source, 
                             final NumericInterpolatorConfig config) {
    if (source == null) {
      throw new IllegalArgumentException("Source cannot be null.");
    }
    if (config == null) {
      throw new IllegalArgumentException("Config cannot be null.");
    }
    this.config = config;
    fill_policy = config.queryFill();
    final Optional<Iterator<TimeSeriesValue<?>>> optional = 
        source.iterator(NumericType.TYPE);
    if (optional.isPresent()) {
      iterator = optional.get();
      if (iterator.hasNext()) {
        next = (TimeSeriesValue<NumericType>) iterator.next();
        has_next = true;
      }
    } else {
      iterator = null;
    }
    
    response = new MutableNumericType();
  }
  
  @Override
  public boolean hasNext() {
    return has_next;
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
    if (timestamp.compare(RelationalOperator.EQ, next.timestamp())) {
      response.reset(next);
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
    } else {
      if (next != null) {
        has_next = true;
      }
      return fill(timestamp);
    }
    
    return response;
  }

  @Override
  public TimeStamp nextReal() {
    if (!has_next) {
      throw new NoSuchElementException();
    }
    return next.timestamp();
  }

  @Override
  public QueryFillPolicy<NumericType> fillPolicy() {
    return fill_policy;
  }

  protected TimeSeriesValue<NumericType> fill(final TimeStamp timestamp) {
    switch (config.realFillPolicy()) {
    case PREVIOUS_ONLY:
      if (previous != null) {
        response.reset(timestamp, previous.value());
        return response;
      }
      break;
    case PREFER_PREVIOUS:
      if (previous != null) {
        response.reset(timestamp, previous.value());
        return response;
      }
      if (next != null) {
        response.reset(timestamp, next.value());
        return response;
      }
      break;
    case NEXT_ONLY:
      if (next != null) {
        response.reset(timestamp, next.value());
        return response;
      }
      break;
    case PREFER_NEXT:
      if (next != null) {
        response.reset(timestamp, next.value());
        return response;
      }
      if (previous != null) {
        response.reset(timestamp, previous.value());
        return response;
      }
      break;
    case NONE:
      break;
    }
    
    final NumericType fill = fill_policy.fill();
    if (fill == null) {
      response.resetNull(timestamp);
    } else {
      response.reset(timestamp, fill);
    }
    return response;
  }
  
}
