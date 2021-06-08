/*
 * This file is part of OpenTSDB.
 * Copyright (C) 2021  The OpenTSDB Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.opentsdb.query.processor.merge;

import com.google.common.reflect.TypeToken;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.aggregators.ArrayAggregatorUtils;
import net.opentsdb.data.types.numeric.aggregators.NumericArrayAggregator;
import net.opentsdb.query.QueryIterator;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;

import java.io.IOException;
import java.util.Collection;

public class SplitNumericArrayIterator implements QueryIterator,
        TimeSeriesValue<NumericArrayType> {

  /** The result we belong to. */
  private final MergerResult result;
  private NumericArrayAggregator aggregator;
  private boolean hasNext;

  SplitNumericArrayIterator(final QueryNode node,
                            final QueryResult result,
                            final Collection<TimeSeries> sources) {
    this.result = (MergerResult) result;

    // so, happy fun, we have to merge one or more series into a single array.
    // In theory everything should have been downsampled at this point to a
    // matching downsample SO we should be good. BUT we have to watch out for
    // overlap and take the AGG of those. grr.
    aggregator = ((Merger) node).getNAI();
    for (final TimeSeries ts : sources) {
      // TODO - for now we're assuming it's all arrays in the sources
      final ArrayAggregatorUtils.AccumulateState state =
              ArrayAggregatorUtils.accumulateInAggregatorArray(aggregator,
              result.timeSpecification().start(),
              result.timeSpecification().end(),
              result.timeSpecification().interval(),
              ts);
      if (state == ArrayAggregatorUtils.AccumulateState.SUCCESS) {
        hasNext = true;
      }
    }
  }

  @Override
  public TypeToken getType() {
    return NumericArrayType.TYPE;
  }

  @Override
  public void close() throws IOException {
    if (aggregator != null) {
      aggregator.close();
      aggregator = null;
    }
  }

  @Override
  public boolean hasNext() {
    return hasNext;
  }

  @Override
  public TimeSeriesValue<NumericArrayType> next() {
    hasNext = false;
    return this;
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
