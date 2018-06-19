// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
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
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Optional;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.Op;
import net.opentsdb.data.types.numeric.MutableNumericSummaryValue;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryFillPolicy;
import net.opentsdb.query.interpolation.QueryInterpolator;
import net.opentsdb.query.interpolation.QueryInterpolatorConfig;

/**
 * An interpolator class for summary values. It can advance through an
 * iterator of summaries when 
 * {@link NumericSummaryInterpolatorConfig#sync()} is set to true in the
 * event that all values for a timestamp in a summary must be present, 
 * for example when calculating the average from summaries of sum and 
 * count. It can also interpolate between individual summaries.
 * 
 * @since 3.0
 */
public class NumericSummaryInterpolator implements 
    QueryInterpolator<NumericSummaryType>{

  /** The interpolator config. */
  protected final NumericSummaryInterpolatorConfig config;
  
  /** The fill policy. */
  protected final QueryFillPolicy<NumericSummaryType> fill_policy;
  
  /** The iterator pulled from the source. May be null. */
  protected final Iterator<TimeSeriesValue<?>> iterator;
  
  /** A map of summary IDs to real value interpolators. */
  protected Map<Integer, ReadAheadNumericInterpolator> data;
  
  /** The next real value. */
  protected TimeSeriesValue<NumericSummaryType> next;
  
  /** The value filled when lerping. */
  protected final MutableNumericSummaryValue response;
  
  /** Whether or not the source iterator has more data. */
  protected boolean has_next;
  
  /**
   * Default ctor.
   * @param source A non-null source to pull data from.
   * @param config A non-null config for the interpolator.
   * @throws IllegalArgumentException if the source or config was null.
   */
  public NumericSummaryInterpolator(final TimeSeries source, 
                                    final QueryInterpolatorConfig config) {
    if (source == null) {
      throw new IllegalArgumentException("Source cannot be null.");
    }
    if (config == null) {
      throw new IllegalArgumentException("Config cannot be null.");
    }
    this.config = (NumericSummaryInterpolatorConfig) config;
    fill_policy = ((NumericSummaryInterpolatorConfig) config).queryFill();
    final Optional<Iterator<TimeSeriesValue<?>>> optional = 
        source.iterator(NumericSummaryType.TYPE);
    if (optional.isPresent()) {
      iterator = optional.get();
    } else {
      iterator = null;
    }
    response = new MutableNumericSummaryValue();
    initialize();
  }
  
  /**
   * Ctor.
   * @param iterator A non-null source to pull data from.
   * @param config A non-null config for the interpolator.
   * @throws IllegalArgumentException if the source or config was null.
   */
  public NumericSummaryInterpolator(
      final Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> iterator, 
      final QueryInterpolatorConfig config) {
    if (iterator == null) {
      throw new IllegalArgumentException("Source cannot be null.");
    }
    if (config == null) {
      throw new IllegalArgumentException("Config cannot be null.");
    }
    this.config = (NumericSummaryInterpolatorConfig) config;
    fill_policy = ((NumericSummaryInterpolatorConfig) config).queryFill();
    this.iterator = iterator;
    response = new MutableNumericSummaryValue();
    initialize();
  }
  
  @Override
  public boolean hasNext() {
    return has_next;
  }

  @SuppressWarnings("unchecked")
  @Override
  public TimeSeriesValue<NumericSummaryType> next(final TimeStamp timestamp) {
    if (iterator == null || next == null) {
      return fill(timestamp);
    }
    
    has_next = false;
    if (next != null && next.timestamp().compare(Op.LTE, timestamp)) {
      if (iterator.hasNext()) {
        if (config.sync()) {
          advanceSynchronized();
        } else {
          next = (TimeSeriesValue<NumericSummaryType>) iterator.next();
          setReadAheads(next);
        }
      } else {
        next = null;
      }
    }

    has_next = next != null;
    return fill(timestamp);
  }

  @Override
  public TimeStamp nextReal() {
    if (next == null || !has_next) {
      throw new NoSuchElementException();
    }
    return next.timestamp();
  }

  @Override
  public QueryFillPolicy<NumericSummaryType> fillPolicy() {
    return fill_policy;
  }

  /**
   * Fills the value for the given timestamp.
   * @param timestamp A non-null timestamp to fill at.
   * @return A non-null summary value.
   */
  @VisibleForTesting
  MutableNumericSummaryValue fill(final TimeStamp timestamp) {
    response.clear();
    response.resetTimestamp(timestamp);
    for (final Entry<Integer, ReadAheadNumericInterpolator> entry : data.entrySet()) {
      response.resetValue(entry.getKey(), entry.getValue().next(timestamp));
    }
    return response;
  }

  /**
   * Bumps the appropriate read-aheads with the next real value.
   * @param value A summary value.
   */
  @VisibleForTesting
  void setReadAheads(final TimeSeriesValue<NumericSummaryType> value) {
    if (value.value() == null) {
      return;
    }
    
    final MutableNumericValue dp = new MutableNumericValue();
    dp.resetTimestamp(value.timestamp());
    for (final int summary : value.value().summariesAvailable()) {
      ReadAheadNumericInterpolator interpolator = data.get(summary);
      if (interpolator == null) {
        // we don't care about this one.
        // TODO - a counter of these drops
        continue;
      }
      final NumericType v = value.value().value(summary);
      if (v == null) {
        dp.resetNull(value.timestamp());
      } else if (v.isInteger()) {
        dp.resetValue(v.longValue());
      } else {
        dp.resetValue(v.doubleValue());
      }
      interpolator.setNext(dp);
    }
  }

  /**
   * Advances to the next summary where all of the expected summaries
   * have a value.
   */
  @SuppressWarnings("unchecked")
  private void advanceSynchronized() {
    // advance to the next real that has values for all summaries
    while (iterator.hasNext()) {
      next = (TimeSeriesValue<NumericSummaryType>) iterator.next();
      if (next.value() == null) {
        continue;
      }
      
      int present = 0;
      for (final int summary : next.value().summariesAvailable()) {
        final NumericType value = next.value().value(summary);
        if (value == null) {
          continue;
        }
        if (!value.isInteger() && Double.isNaN(value.doubleValue())) {
          continue;
        }
        present++;
      }
      
      if (present == config.expectedSummaries().size()) {
        // all there!
        setReadAheads(next);
        has_next = true;
        return;
      }
    }
    
    // we hit the end, nothing left that's in sync.
    next = null;
    has_next = false;
  }
  
  /**
   * Advances to the first real value either synchronized or not.
   */
  @SuppressWarnings("unchecked")
  private void initialize() {
    data = Maps.newHashMapWithExpectedSize(config.expectedSummaries().size());
    for (final int summary : config.expectedSummaries()) {
      data.put(summary, new ReadAheadNumericInterpolator(config.queryFill(summary)));
    }
    if (iterator != null) {
      if (!config.sync()) {
        if (iterator.hasNext()) {
          next = (TimeSeriesValue<NumericSummaryType>) iterator.next();
          setReadAheads(next);
          has_next = true;
        }
      } else {
        advanceSynchronized();
      }
    }
  }
}