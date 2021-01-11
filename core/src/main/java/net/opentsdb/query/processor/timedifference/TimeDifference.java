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

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.query.AbstractQueryNode;
import net.opentsdb.query.BaseWrappedQueryResult;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;

/**
 * A node that computes the time diference for values within the query window,
 * omitting null or NaN data points.
 * 
 * @since 3.0
 */
public class TimeDifference extends AbstractQueryNode {

  private final TimeDifferenceConfig config;
  
  public TimeDifference(final QueryNodeFactory factory, 
                        final QueryPipelineContext context,
                        final TimeDifferenceConfig config) {
    super(factory, context);
    this.config = config;
  }

  @Override
  public QueryNodeConfig config() {
    return config;
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void onNext(final QueryResult next) {
    sendUpstream(new TimeDifferenceResult(next));
  }

  class TimeDifferenceResult extends BaseWrappedQueryResult {

    /** The new series. */
    private List<TimeSeries> series;
    
    public TimeDifferenceResult(final QueryResult result) {
      super(TimeDifference.this, result);
      series = Lists.newArrayListWithExpectedSize(result.timeSeries().size());
      for (final TimeSeries ts : result.timeSeries()) {
        series.add(new TimeDifferenceTimeSeries(ts));
      }
    }
    
    @Override
    public List<TimeSeries> timeSeries() {
      return series;
    }
    
    class TimeDifferenceTimeSeries implements TimeSeries {
      private final TimeSeries source;
      
      TimeDifferenceTimeSeries(final TimeSeries source) {
        this.source = source;
      }
      
      @Override
      public TimeSeriesId id() {
        return source.id();
      }

      @Override
      public Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterator(
          TypeToken<? extends TimeSeriesDataType> type) {
        if (!source.types().contains(type)) {
          return Optional.empty();
        }
        if (((TimeDifferenceFactory) factory).types().contains(type)) {
          final TypedTimeSeriesIterator iterator = 
              ((TimeDifferenceFactory) factory).newTypedIterator(
                  type,
                  TimeDifference.this,
                  TimeDifferenceResult.this,
                  Lists.newArrayList(source));
          return Optional.of(iterator);
          
        }
        return Optional.empty();
      }

      @Override
      public Collection<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterators() {
        final Collection<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterators =
            Lists.newArrayListWithExpectedSize(source.types().size());
        for (final TypeToken<? extends TimeSeriesDataType> type : source.types()) {
          if (((TimeDifferenceFactory) factory).types().contains(type)) {
            final TypedTimeSeriesIterator iterator = 
                ((TimeDifferenceFactory) factory).newTypedIterator(
                    type,
                    TimeDifference.this,
                    TimeDifferenceResult.this,
                    Lists.newArrayList(source));
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
        source.close();
      }
      
    }
  }
}
