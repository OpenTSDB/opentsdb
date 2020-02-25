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
package net.opentsdb.query.processor.merge;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.aggregators.NumericArrayAggregator;
import net.opentsdb.data.types.numeric.aggregators.NumericArrayAggregatorFactory;
import net.opentsdb.query.QueryIterator;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;

/**
 * An iterator for grouping arrays. This should be much faster for 
 * numerics than the regular iterative method for arrays, being able to 
 * take advantage of the L2 cache.
 * 
 * @since 3.0
 */
public class MergerNumericArrayIterator implements QueryIterator, 
  TimeSeriesValue<NumericArrayType> {
  
  /** The result we belong to. */
  private final MergerResult result;
  
  /** The aggregator. */
  private final NumericArrayAggregator aggregator;
  
  /** Whether or not another real value is present. True while at least one 
   * of the time series has a real value. */
  private boolean has_next = false;
  
  /**
   * Default ctor.
   * @param node The non-null node this iterator belongs to.
   * @param result The result this source is a part of.
   * @param sources The non-null and non-empty map of sources.
   * @throws IllegalArgumentException if a required parameter or config is 
   * not present.
   */
  public MergerNumericArrayIterator(final QueryNode node, 
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
  public MergerNumericArrayIterator(final QueryNode node, 
                                     final QueryResult result,
                                     final Collection<TimeSeries> sources) {
    if (node == null) {
      throw new IllegalArgumentException("Query node cannot be null.");
    }
    if (sources == null) {
      throw new IllegalArgumentException("Sources cannot be null.");
    }
    if (sources.isEmpty()) {
      throw new IllegalArgumentException("Sources cannot be empty.");
    }
    
    this.result = (MergerResult) result;
    final NumericArrayAggregatorFactory factory = node.pipelineContext().tsdb()
        .getRegistry().getPlugin(NumericArrayAggregatorFactory.class, 
            ((MergerConfig) node.config()).getAggregator());
    if (factory == null) {
      throw new IllegalArgumentException("No aggregator factory found of type: " 
          + ((MergerConfig) node.config()).getAggregator());
    }
    aggregator = factory.newAggregator(((MergerConfig) node.config()).getInfectiousNan());
    if (aggregator == null) {
      throw new IllegalArgumentException("No aggregator found of type: " 
          + ((MergerConfig) node.config()).getAggregator());
    }
    
    // gotta do it here otherwise we won't know if there is any data.
    for (final TimeSeries source : sources) {
      if (source == null) {
        throw new IllegalArgumentException("Null time series are not "
            + "allowed in the sources.");
      }
      final Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> optional =
          source.iterator(NumericArrayType.TYPE);
      if (optional.isPresent()) {
        final TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator = optional.get();
        if (iterator.hasNext()) {
          has_next = true;
          final TimeSeriesValue<NumericArrayType> array = 
              (TimeSeriesValue<NumericArrayType>) iterator.next();
          // skip empties.
          if (array.value().end() - array.value().offset() > 0) {
            if (array.value().isInteger()) {
              if (array.value().longArray().length > 0) {
                aggregator.accumulate(array.value().longArray(), 
                    array.value().offset(), array.value().end());
              }
            } else if (array.value().doubleArray().length > 0) {
              aggregator.accumulate(array.value().doubleArray(),
                  array.value().offset(), array.value().end());
            }
          }
        }
      }
    }
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
  public void close() {
    // no-op for now
  }
  
  @Override
  public TimeStamp timestamp() {
    return result.timeSpecification().start();
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