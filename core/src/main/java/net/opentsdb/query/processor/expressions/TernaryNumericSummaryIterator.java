//This file is part of OpenTSDB.
//Copyright (C) 2020  The OpenTSDB Authors.
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.
package net.opentsdb.query.processor.expressions;

import java.util.Map;
import java.util.Optional;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;

/**
 * Iterator for summary data in a ternary expression.
 * 
 * TODO - This needs work and testing.
 * 
 * @since 3.0
 */
public class TernaryNumericSummaryIterator extends ExpressionNumericSummaryIterator {

  /** The condition time series. */
  protected TypedTimeSeriesIterator condition;
  
  /**
   * Default ctor.
   * @param node The non-null ternary expression node.
   * @param result The non-null result we're populating.
   * @param sources The map of sources.
   */
  TernaryNumericSummaryIterator(final QueryNode node, 
                                final QueryResult result, 
                                final Map<String, TimeSeries> sources) {
    super(node, result, sources);
    TimeSeries c = sources.get(ExpressionTimeSeries.CONDITION_KEY);
    if (c == null) {
      // can't do anything
      has_next = false;
      return;
    }
    final Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> op = 
        c.iterator(NumericSummaryType.TYPE);
    if (!op.isPresent()) {
      // can't do anything so leave has_next as false.
      has_next = false;
      return;
    }
    condition = op.get();
    has_next = condition.hasNext();
  }

  @Override
  public TimeSeriesValue<? extends TimeSeriesDataType> next() {
    TimeSeriesValue<NumericSummaryType> c = 
        (TimeSeriesValue<NumericSummaryType>) condition.next();
    has_next = condition.hasNext();
    
    if (c.value() != null) {
      TimeSeriesValue<NumericSummaryType> l = left_interpolator.next(c.timestamp());
      TimeSeriesValue<NumericSummaryType> r = right_interpolator.next(c.timestamp());
      for (int summary : c.value().summariesAvailable()) {
        NumericType v = c.value().value(summary);
        boolean is_true = false;
        if (v.isInteger()) {
          if (v.longValue() > 0) {
            is_true = true;
          } 
        } else if (Double.isFinite(v.doubleValue()) && 
                   v.doubleValue() > 0) {
          is_true = true;
        }
        
        if (is_true) {
          if (l == null) {
            dp.nullSummary(summary);
          } else {
            dp.resetValue(summary, l.value().value(summary));
          }
        } else {
          if (r == null) {
            dp.nullSummary(summary);
          } else {
            dp.resetValue(summary, r.value().value(summary));
          }
        }
      }
    } else {
      dp.resetNull(c.timestamp());
    }
    
    dp.resetTimestamp(c.timestamp());
    return dp;
  }
}