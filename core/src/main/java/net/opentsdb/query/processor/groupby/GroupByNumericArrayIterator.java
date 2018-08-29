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

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
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
public class GroupByNumericArrayIterator implements QueryIterator, 
  TimeSeriesValue<NumericArrayType> {
  
  /** The result we belong to. */
  private final GroupByResult result;
  
  /** The aggregator. */
  private final NumericArrayAggregator aggregator;
  
  /** Whether or not another real value is present. True while at least one 
   * of the time series has a real value. */
  private boolean has_next = false;
  
  /** List of iterators. */
  private final List<Iterator<TimeSeriesValue<?>>> iterators;
  
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
    iterators = Lists.newArrayListWithExpectedSize(sources.size());
    for (final TimeSeries source : sources) {
      if (source == null) {
        throw new IllegalArgumentException("Null time series are not "
            + "allowed in the sources.");
      }
      final Optional<Iterator<TimeSeriesValue<?>>> optional = 
          source.iterator(NumericArrayType.TYPE);
      if (optional.isPresent()) {
        final Iterator<TimeSeriesValue<?>> iterator = optional.get();
        iterators.add(iterator);
        if (iterator.hasNext()) {
          has_next = true;
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
    for (final Iterator<TimeSeriesValue<?>> iterator : iterators) {
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
    return this;
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