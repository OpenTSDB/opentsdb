// This file is part of OpenTSDB.
// Copyright (C) 2019-2020  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.movingaverage;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.aggregators.AverageFactory;
import net.opentsdb.data.types.numeric.aggregators.ExponentialWeightedMovingAverageConfig;
import net.opentsdb.data.types.numeric.aggregators.ExponentialWeightedMovingAverageFactory;
import net.opentsdb.data.types.numeric.aggregators.MovingMedianFactory;
import net.opentsdb.data.types.numeric.aggregators.NumericAggregator;
import net.opentsdb.data.types.numeric.aggregators.NumericAggregatorFactory;
import net.opentsdb.data.types.numeric.aggregators.WeightedMovingAverageFactory;
import net.opentsdb.query.AbstractQueryNode;
import net.opentsdb.query.BaseWrappedQueryResult;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;

/**
 * A node that computes an aggregation on a window that slides over the
 * data points per iteration. E.g. this can implement a 5 minute 
 * moving average or a 10 minute max function.
 * <p>
 * The first window starts at the first data point equal to or greater 
 * than the timestamp of the query start time. Each data value emitted
 * represents the aggregation of values from the current values timestamp
 * to the time of the window in the past, exclusive of the window start 
 * timestamp. E.g. with the following data:
 * t1, t2, t3, t4, t5, t6
 * and a window of 5 intervals, the value for t6 would include t2 to t6.
 * 
 * @since 3.0
 */
public class MovingAverage extends AbstractQueryNode {

  /** The non-null config. */
  private final MovingAverageConfig config;
  
  /** The aggregator. */
  private volatile NumericAggregator aggregator;
  
  /**
   * Default ctor.
   * @param factory The non-null parent factory.
   * @param context The non-null context.
   * @param config The non-null config.
   */
  public MovingAverage(final QueryNodeFactory factory, 
                       final QueryPipelineContext context,
                       final MovingAverageConfig config) {
    super(factory, context);
    if (config == null) {
      throw new IllegalArgumentException("Config cannot be null.");
    }
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
    sendUpstream(new MovingAverageResult(next));
  }
  
  protected NumericAggregator getAggregator() {
    if (aggregator == null) {
      synchronized (this) {
        if (aggregator == null) {
          if (!config.getMedian() && !config.getWeighted() && !config.getExponential()) {
            // just a basic sliding avg.
            aggregator = pipelineContext().tsdb().getRegistry()
              .getPlugin(NumericAggregatorFactory.class, AverageFactory.TYPE)
                .newAggregator(config.getInfectiousNan());
          } else if (config.getMedian()) {
            aggregator = pipelineContext().tsdb().getRegistry()
                .getPlugin(NumericAggregatorFactory.class, 
                    MovingMedianFactory.TYPE)
                  .newAggregator(config.getInfectiousNan());
          } else if (config.getExponential()) {
            if (config.getAlpha() == 
                  ExponentialWeightedMovingAverageFactory.DEFAULT_CONFIG.alpha() &&
                config.getAverageInitial() == 
                  ExponentialWeightedMovingAverageFactory.DEFAULT_CONFIG.averageInitial()) {
              aggregator = pipelineContext().tsdb().getRegistry()
                  .getPlugin(NumericAggregatorFactory.class, 
                      ExponentialWeightedMovingAverageFactory.TYPE)
                    .newAggregator(config.getInfectiousNan());
            } else {
              final ExponentialWeightedMovingAverageConfig ewma_config = 
                  ExponentialWeightedMovingAverageConfig.newBuilder()
                    .setAlpha(config.getAlpha())
                    .setAverageInitial(config.getAverageInitial())
                    .build();
              aggregator = (NumericAggregator) pipelineContext().tsdb().getRegistry()
                  .getPlugin(NumericAggregatorFactory.class, 
                      ExponentialWeightedMovingAverageFactory.TYPE)
                    .newAggregator(ewma_config);
            }
          } else {
            aggregator = pipelineContext().tsdb().getRegistry()
                .getPlugin(NumericAggregatorFactory.class, 
                    WeightedMovingAverageFactory.TYPE)
                  .newAggregator(config.getInfectiousNan());
          }
        }
      }
    }
    
    return aggregator;
  }
  
  /**
   * Local results.
   */
  class MovingAverageResult extends BaseWrappedQueryResult {
    
    /** The new series. */
    private List<TimeSeries> series;
    
    MovingAverageResult(final QueryResult next) {
      super(MovingAverage.this, next);
      series = Lists.newArrayListWithExpectedSize(next.timeSeries().size());
      for (final TimeSeries ts : next.timeSeries()) {
        series.add(new MovingAverageTimeSeries(ts));
      }
    }
    
    @Override
    public List<TimeSeries> timeSeries() {
      return series;
    }
    
    /**
     * A nested time series class that injects our iterators.
     */
    class MovingAverageTimeSeries implements TimeSeries {
      private final TimeSeries source;
      
      MovingAverageTimeSeries(final TimeSeries source) {
        this.source = source;
      }
      
      @Override
      public TimeSeriesId id() {
        return source.id();
      }

      @Override
      public Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterator(
          final TypeToken<? extends TimeSeriesDataType> type) {
        if (!source.types().contains(type)) {
          return Optional.empty();
        }
        if (((MovingAverageFactory) factory).types().contains(type)) {
          final TypedTimeSeriesIterator iterator = 
              ((MovingAverageFactory) factory).newTypedIterator(
                  type, 
                  MovingAverage.this, 
                  MovingAverageResult.this, 
                  Lists.newArrayList(source));
          return Optional.of(iterator);
        }
        return source.iterator(type);
      }

      @Override
      public Collection<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterators() {
        final Collection<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterators =
            Lists.newArrayListWithExpectedSize(source.types().size());
        
        for (final TypeToken<? extends TimeSeriesDataType> type : source.types()) {
          if (((MovingAverageFactory) factory).types().contains(type)) {
            final TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator =
                ((MovingAverageFactory) factory).newTypedIterator(
                    type, 
                    MovingAverage.this, 
                    MovingAverageResult.this, 
                    Lists.newArrayList(source));
            iterators.add(iterator);
          } else {
            final Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>>  optional =
                source.iterator(type);
            if (optional.isPresent()) {
              iterators.add(optional.get());
            }
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
