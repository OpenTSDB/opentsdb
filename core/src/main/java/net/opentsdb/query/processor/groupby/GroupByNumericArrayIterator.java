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
package net.opentsdb.query.processor.groupby;

import com.google.common.base.Strings;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.aggregators.ArrayLastFactory;
import net.opentsdb.data.types.numeric.aggregators.NumericArrayAggregator;
import net.opentsdb.data.types.numeric.aggregators.NumericArrayAggregatorFactory;
import net.opentsdb.query.QueryIterator;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.utils.DateTime;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An iterator for grouping arrays. This should be much faster for 
 * numerics than the regular iterative method for arrays, being able to 
 * take advantage of the L2 cache.
 * 
 * @since 3.0
 */
public class GroupByNumericArrayIterator implements QueryIterator, 
  TimeSeriesValue<NumericArrayType> {
  
  private static final Logger logger = LoggerFactory.getLogger(GroupByNumericArrayIterator.class);
  
  /** The result we belong to. */
  private final GroupByResult result;
  
  /** The aggregator. */
  private final NumericArrayAggregator aggregator;
  
  /** Whether or not another real value is present. True while at least one 
   * of the time series has a real value. */
  private volatile boolean has_next = false;
  
  ExecutorService executor;
  
  /**
   * Default ctor.
   * @param node The non-null node this iterator belongs to.
   * @param result The result this source is a part of.
   * @param sources The non-null and non-empty map of sources.
   * @throws IllegalArgumentException if a required parameter or config is 
   * not present.
   */
  public GroupByNumericArrayIterator(final QueryNode node, 
                                     final QueryResult result,
                                     final Map<String, TimeSeries> sources) {
    this(node, result, sources == null ? null : sources.values());
  }
  
  /**
   * Ctor with a collection of source time series.
   * @param node The non-null node this iterator belongs to.
   * @param result The result this source is a part of.
   * @param sources The non-null and non-empty collection or sources.
   * @throws IllegalArgumentException if a required parameter or config is 
   * not present.
   */
  public GroupByNumericArrayIterator(final QueryNode node, 
                                     final QueryResult result,
                                     final Collection<TimeSeries> sources) {
    long st = DateTime.nanoTime();
    if (node == null) {
      throw new IllegalArgumentException("Query node cannot be null.");
    }
    if (sources == null) {
      throw new IllegalArgumentException("Sources cannot be null.");
    }
    if (sources.isEmpty()) {
      throw new IllegalArgumentException("Sources cannot be empty.");
    }
    if (Strings.isNullOrEmpty(((GroupByConfig) node.config()).getAggregator())) {
      throw new IllegalArgumentException("Aggregator cannot be null or empty."); 
    }
    
    executor = node.pipelineContext().tsdb().quickWorkPool();
    
    this.result = (GroupByResult) result;
    final NumericArrayAggregatorFactory factory = node.pipelineContext().tsdb()
        .getRegistry().getPlugin(NumericArrayAggregatorFactory.class, 
            ((GroupByConfig) node.config()).getAggregator());
    if (factory == null) {
      throw new IllegalArgumentException("No aggregator factory found of type: " 
          + ((GroupByConfig) node.config()).getAggregator());
    }
    aggregator = factory.newAggregator(((GroupByConfig) node.config()).getInfectiousNan());
    if (aggregator == null) {
      throw new IllegalArgumentException("No aggregator found of type: " 
          + ((GroupByConfig) node.config()).getAggregator());
    }
    
    long startTs = DateTime.nanoTime();

    // TODO: Need to check if it makes sense to make this threshold configurable
    if (sources.size() >= 1000) {

      List<Future<TimeSeriesValue<NumericArrayType>>> futures =
          new ArrayList<Future<TimeSeriesValue<NumericArrayType>>>(sources.size());

      parallelFetch(sources, futures);
      long endTs = DateTime.nanoTime() - startTs;
      logger.info("Total number of paralelly fetched iterator values {} and parallel fetch took {}",
          futures.size(), ((double) endTs) / 1000_000);
    } else {
      for (final TimeSeries source : sources) {
        if (source == null) {
          throw new IllegalArgumentException(
              "Null time series are not " + "allowed in the sources.");
        }

        final Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> optional =
            source.iterator(NumericArrayType.TYPE);
        if (optional.isPresent()) {
          final TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator = optional.get();
          if (iterator.hasNext()) {
            has_next = true;
            final TimeSeriesValue<NumericArrayType> array =
                (TimeSeriesValue<NumericArrayType>) iterator.next();
            accumulate(array);

          }
        }
      }
    }

    double e = ((double) (DateTime.nanoTime() - st)) / 1000_000;
    logger.info("Groupby iterator took {}, timeseries count {}", e, sources.size());
  }

  private void parallelFetch(final Collection<TimeSeries> sources,
      List<Future<TimeSeriesValue<NumericArrayType>>> futures) {
    for (TimeSeries source : sources) {
      has_next = true;
      futures.add(submit(source));
    }

    for (Future<TimeSeriesValue<NumericArrayType>> future : futures) {
      try {

        TimeSeriesValue<NumericArrayType> array = future.get(); // get will block until the future
                                                                // is done
        if (array == null) {
          continue;
        }

        accumulate(array);
      } catch (InterruptedException | ExecutionException e3) {
        logger.error("Unable to get the status of a task", e3);
      }
    }
  }

  private void accumulate(TimeSeriesValue<NumericArrayType> array) {
    if (array.value().end() - array.value().offset() > 0) {
      if (array.value().isInteger()) {
        if (array.value().longArray().length > 0) {
          aggregator.accumulate(array.value().longArray(), array.value().offset(),
              array.value().end());
        }
      } else if (array.value().doubleArray().length > 0) {
        aggregator.accumulate(array.value().doubleArray(), array.value().offset(),
            array.value().end());
      }
    }
  }
  
  public Future<TimeSeriesValue<NumericArrayType>> submit(TimeSeries source) {

    Callable<TimeSeriesValue<NumericArrayType>> c =
        new Callable<TimeSeriesValue<NumericArrayType>>() {

          @Override
          public TimeSeriesValue<NumericArrayType> call() throws Exception {
            if (source == null) {
              throw new IllegalArgumentException(
                  "Null time series are not " + "allowed in the sources.");
            }

            final Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> optional =
                source.iterator(NumericArrayType.TYPE);
            if (optional.isPresent()) {
              final TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator =
                  optional.get();
              if (iterator.hasNext()) {
                return (TimeSeriesValue<NumericArrayType>) iterator.next();
              }
            }
            return null;
          }

        };
        
    return executor.submit(c);
  }
  
  @Override
  public boolean hasNext() {
    return has_next;
  }

  @Override
  public TimeSeriesValue<? extends TimeSeriesDataType> next() {
    has_next = false;
    return this;
  }

  @Override
  public TypeToken<? extends TimeSeriesDataType> getType() {
    return NumericArrayType.TYPE;
  }
  
  @Override
  public TimeStamp timestamp() {
    return result.downstreamResult().timeSpecification().start();
  }

  @Override
  public NumericArrayType value() {
    return aggregator;
  }

  @Override
  public TypeToken<NumericArrayType> type() {
    return NumericArrayType.TYPE;
  }

}