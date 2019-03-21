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

import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.query.BaseWrappedQueryResult;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;

/**
 * Result for a time shifted set of data that will re-align the timestamps with
 * the original query for comparisson.
 * 
 * @since 3.0
 */
public class TimeShiftResult extends BaseWrappedQueryResult {

  /** The original node. */
  private final TimeShift node;
  
  /** Whether or not we're expecting earlier data and aligning it with later
   * timestamps or vice-versa. */
  private final boolean is_previous;
  
  /** The adjustment amount. */
  private final TemporalAmount amount;
  
  /** List of re-converted time series. */
  private final List<TimeSeries> time_series;
  
  /** Time spec if we're dealing with arrays. */
  private final TimeSpecification time_spec;
  
  public TimeShiftResult(final TimeShift node, 
                         final QueryResult result,
                         final boolean is_previous,
                         final TemporalAmount amount) {
    super(result);
    this.node = node;
    this.is_previous = is_previous;
    this.amount = amount;
    
    if (result.timeSpecification() != null) {
      // WOOT! Downsampled arrays so we just need to tweak the spec and push it
      // up!
      time_spec = new WrappedTimeSpecification(result.timeSpecification());
      time_series = null;
    } else {
      time_series = Lists.newArrayListWithExpectedSize(result.timeSeries().size());
      for (final TimeSeries ts : result.timeSeries()) {
        time_series.add(new TimeShiftTimeSeries(ts));
      }
      time_spec = null;
    }
  }

  @Override
  public QueryNode source() {
    return node;
  }
  
  @Override
  public Collection<TimeSeries> timeSeries() {
    if (time_series != null) {
      return time_series;
    }
    return result.timeSeries();
  }
  
  @Override
  public TimeSpecification timeSpecification() {
    return time_spec;
  }
  
  TemporalAmount amount() {
    return amount;
  }
  
  boolean isPrevious() {
    return is_previous;
  }

  class TimeShiftTimeSeries implements TimeSeries {

    private final TimeSeries source;
    
    TimeShiftTimeSeries(final TimeSeries source) {
      this.source = source;
    }
    
    @Override
    public TimeSeriesId id() {
      return source.id();
    }

    @Override
    public Optional<TypedTimeSeriesIterator> iterator(
        final TypeToken<? extends TimeSeriesDataType> type) {
      final TypedTimeSeriesIterator iterator = 
          ((TimeShiftFactory) node.factory()).newTypedIterator(type, node, 
              TimeShiftResult.this, Lists.newArrayList(source));
      if (iterator == null) {
        return Optional.empty();
      }
      return Optional.of(iterator);
    }

    @Override
    public Collection<TypedTimeSeriesIterator> iterators() {
      List<TypedTimeSeriesIterator> iterators = Lists.newArrayList();
      for (final TypeToken<? extends TimeSeriesDataType> type : source.types()) {
        final TypedTimeSeriesIterator iterator = 
            ((TimeShiftFactory) node.factory()).newTypedIterator(type, node, 
                TimeShiftResult.this, Lists.newArrayList(source));
        if (iterator != null) {
          iterators.add(iterator);
        }
      }
      return iterators;
    }

    @Override
    public Collection<TypeToken<? extends TimeSeriesDataType>> types() {
      return source.types();
    }

    @Override
    public void close() {
      result.close();
    }
    
  }

  class WrappedTimeSpecification implements TimeSpecification {
    private final TimeSpecification time_spec;
    private final TimeStamp start;
    private final TimeStamp end;
    
    WrappedTimeSpecification(final TimeSpecification time_spec) {
      this.time_spec = time_spec;
      start = time_spec.start().getCopy();
      end = time_spec.end().getCopy();
      // TODO - handle calendaring here.
      if (is_previous) {
        start.add(amount);
        end.add(amount);
      } else {
        start.subtract(amount);
        end.subtract(amount);
      }
    }
    
    @Override
    public TimeStamp start() {
      return start;
    }

    @Override
    public TimeStamp end() {
      return end;
    }

    @Override
    public TemporalAmount interval() {
      return time_spec.interval();
    }

    @Override
    public String stringInterval() {
      return time_spec.stringInterval();
    }

    @Override
    public ChronoUnit units() {
      return time_spec.units();
    }

    @Override
    public ZoneId timezone() {
      return time_spec.timezone();
    }

    @Override
    public void updateTimestamp(final int offset, final TimeStamp timestamp) {
      throw new UnsupportedOperationException("Not implemented yet!");
    }

    @Override
    public void nextTimestamp(final TimeStamp timestamp) {
      throw new UnsupportedOperationException("Not implemented yet!");
    }
    
  }
}
