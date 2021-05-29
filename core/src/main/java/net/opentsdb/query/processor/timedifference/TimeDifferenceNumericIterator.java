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
import java.util.Collection;
import java.util.Map;
import java.util.Optional;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryIterator;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;

/**
 * An iterator that computes the time delta on numeric data.
 * 
 * @since 3.0
 */
public class TimeDifferenceNumericIterator implements QueryIterator {
  
  private final ChronoUnit resolution;
  
  private TypedTimeSeriesIterator<NumericType> iterator;
  private TimeSeriesValue<NumericType> previous;
  private boolean has_next;
  private MutableNumericValue next_dp;
  private MutableNumericValue dp;

  TimeDifferenceNumericIterator(final QueryNode node, 
                                final QueryResult result,
                                final Map<String, TimeSeries> sources) {
    this(node, result, sources == null ? null : sources.values());
  }
  
  TimeDifferenceNumericIterator(final QueryNode node, 
                                final QueryResult result,
                                final Collection<TimeSeries> sources) {
    resolution = ((TimeDifferenceConfig) node.config()).getResolution();
    final Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> op = 
        sources.iterator().next().iterator(NumericType.TYPE);
    if (op.isPresent()) {
      iterator = (TypedTimeSeriesIterator<NumericType>) op.get();
      advance();
    }
  }
  
  @Override
  public TypeToken getType() {
    return NumericType.TYPE;
  }

  @Override
  public boolean hasNext() {
    return has_next;
  }

  @Override
  public TimeSeriesValue<NumericType> next() {
    if (dp == null) {
      dp = new MutableNumericValue();
    }
    dp.reset(next_dp);
    advance();
    return dp;
  }

  @Override
  public void close() throws IOException {
    if (iterator != null) {
      iterator.close();
    }
  }

  void advance() {
    has_next = false;
    while (iterator.hasNext()) {
      final TimeSeriesValue<NumericType> value = iterator.next();
      if (value.value() == null) {
        continue;
      }
      
      if (!value.value().isInteger() && Double.isNaN(value.value().doubleValue())) {
        continue;
      }
      
      if (previous == null) {
        previous = value;
        continue;
      }
      
      // good
      if (next_dp == null) {
        next_dp = new MutableNumericValue();
      }
      long delta = -1;
      switch (resolution) {
      case HOURS:
        double d = value.timestamp().epoch() - previous.timestamp().epoch();
        d /= (60d * 60d);
        next_dp.reset(value.timestamp(), d);
        break;
      case MINUTES:
        double dm = value.timestamp().epoch() - previous.timestamp().epoch();
        dm /= 60d;
        next_dp.reset(value.timestamp(), dm);
        break;
      case SECONDS:
        delta = value.timestamp().epoch() - previous.timestamp().epoch();
        break;
      case MILLIS:
        delta = value.timestamp().msEpoch() - previous.timestamp().msEpoch();
        break;
      case NANOS:
        delta = value.timestamp().epoch() - previous.timestamp().epoch();
        delta *= 1000 * 1000 * 1000;
        delta += (value.timestamp().nanos() - previous.timestamp().nanos());
        break;
      default:
        throw new IllegalStateException("Shouldn't be here! Bad resolution: " 
            + resolution);
      }
      if (delta >= 0) {
        next_dp.reset(value.timestamp(), delta);
      }
      has_next = true;
      previous = value;
      return;
    }
  }
}
