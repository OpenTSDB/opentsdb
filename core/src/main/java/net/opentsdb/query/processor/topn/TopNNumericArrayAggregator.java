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
package net.opentsdb.query.processor.topn;

import java.util.Iterator;
import java.util.Optional;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.data.types.numeric.aggregators.NumericAggregator;
import net.opentsdb.data.types.numeric.aggregators.NumericAggregatorFactory;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;

/**
 * Aggregates an entire numeric series into a single value.
 * 
 * @since 3.0
 */
public class TopNNumericArrayAggregator {
  
  /** The aggregator. */
  private final NumericAggregator aggregator;
  
  /** The parent node. */
  protected final TopN node;
  
  /** The series we'll pull from. */
  protected final TimeSeries series;
  
  /**
   * Package private ctor.
   * @param node The non-null node.
   * @param result The non-null result.
   * @param source The non-null source.
   */
  TopNNumericArrayAggregator(final QueryNode node, 
                             final QueryResult result,
                             final TimeSeries source) {
    this.node = (TopN) node;
    this.series = source;
    NumericAggregatorFactory agg_factory = node.pipelineContext().tsdb()
        .getRegistry().getPlugin(NumericAggregatorFactory.class, 
            ((TopNConfig) node.config()).getAggregator());
    if (agg_factory == null) {
      throw new IllegalArgumentException("No aggregator found for type: " 
          + ((TopNConfig) node.config()).getAggregator());
    }
    aggregator = agg_factory.newAggregator(
        ((TopNConfig) node.config()).getInfectiousNan());
  }
  
  /** @return Perform the aggregation. If no data is present, return null. */
  NumericType run() {
    final Optional<TypedTimeSeriesIterator> optional = 
        series.iterator(NumericArrayType.TYPE);
    if (!optional.isPresent()) {
      return null;
    }
    final Iterator<TimeSeriesValue<?>> iterator = optional.get();
    if (!iterator.hasNext()) {
      return null;
    }
    
    final TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    final MutableNumericValue dp = new MutableNumericValue();
    if (value.value().isInteger()) {
      aggregator.run(value.value().longArray(), 
          value.value().offset(), 
          value.value().end(), 
          dp);
    } else {
      aggregator.run(value.value().doubleArray(), 
          value.value().offset(), 
          value.value().end(), 
          ((TopNConfig) node.config()).getInfectiousNan(), dp);
    }
    return dp.value();
  }
  
}
