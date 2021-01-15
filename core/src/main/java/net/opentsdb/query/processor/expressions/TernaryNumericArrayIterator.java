//This file is part of OpenTSDB.
//Copyright (C) 2020-2021  The OpenTSDB Authors.
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

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;

/**
 * Iterator for a ternary expression that will return the proper left or right
 * value per the evaluated condition at the given timestamp in the array.
 * 
 * @since 3.0
 */
public class TernaryNumericArrayIterator extends 
    ExpressionNumericArrayIterator {
  
  /** The iterator for the condition. */
  protected final TypedTimeSeriesIterator<?> condition_iterator;
  
  /** The condition time series. */
  protected final TimeSeriesValue<NumericArrayType> condition;
  
  /**
   * Default ctor.
   * @param node The non-null ternary expression node.
   * @param result The non-null result we're populating.
   * @param sources The map of sources.
   */
  TernaryNumericArrayIterator(final QueryNode node, 
                              final QueryResult result,
                              final Map<String, TimeSeries> sources) {
    super(node, result, sources);
    TimeSeries c = sources.get(ExpressionTimeSeries.CONDITION_KEY);
    if (c == null) {
      // can't do anything
      condition_iterator = null;
      condition = null;
      has_next = false;
      return;
    }
    final Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> op = 
        c.iterator(NumericArrayType.TYPE);
    if (!op.isPresent()) {
      // can't do anything so leave has_next as false.
      condition_iterator = null;
      condition = null;
      has_next = false;
      return;
    }
    condition_iterator = op.get();
    if (condition_iterator.hasNext()) {
      condition = (TimeSeriesValue<NumericArrayType>) condition_iterator.next();
    } else {
      condition = null;
    }
    
    if (condition != null && condition.value() != null && 
        condition.value().end() > condition.value().offset()) {
      has_next = true;
    } else {
      has_next = false;
    }
    
    if (left == null && left_literal == null) {
      throw new IllegalStateException("Ternary must have a left hand series "
          + "or literal.");
    }
    if (right == null && right_literal == null) {
      throw new IllegalStateException("Ternary must have a right hand series "
          + "or literal.");
    }
  }
  
  @Override
  public TimeSeriesValue<? extends TimeSeriesDataType> next() {
    has_next = false;
    TimeSeriesValue<NumericArrayType> left_value = 
        left != null ?
        (TimeSeriesValue<NumericArrayType>) left.next() : null;
    TimeSeriesValue<NumericArrayType> right_value = 
        right != null ?
        (TimeSeriesValue<NumericArrayType>) right.next() : null;
    
    // further checks
    if (left_value != null && (left_value.value() == null || 
        left_value.value().end() <= left_value.value().offset())) {
      left_value = null;
    }
    if (right_value != null && (right_value.value() == null ||
        right_value.value().end() <= right_value.value().offset())) {
      right_value = null;
    }
    
    next_ts.update(condition.timestamp());
    
    // TODO - pools
    if (left_value != null && right_value != null) {
      if (left_value.value().isInteger() && right_value.value().isInteger()) {
        long_values = new long[condition.value().end() - 
                               condition.value().offset()];
      } else {
        double_values = new double[condition.value().end() - 
                                   condition.value().offset()];
      }
    } else {
      long_values = new long[condition.value().end() - 
                             condition.value().offset()];
    }
    
    int idx = 0;
    for (int i = condition.value().offset(); i < condition.value().end(); i++) {
      final boolean use_left = condition.value().isInteger() ?
          condition.value().longArray()[i] > 0 :
            condition.value().doubleArray()[i] > 0;
      if (use_left) {
        if (left_value == null) {
          if (left_literal != null) {
            if (left_literal.isInteger() && long_values != null) {
              write(idx, left_literal.longValue());
            } else {
              write(idx, left_literal.toDouble());
            }
          } else {
            write(idx, Double.NaN);
          }
        } else if (left_value.value().isInteger()) {
          write(idx, left_value.value().longArray()[left_value.value().offset() + idx]);
        } else {
          write(idx, left_value.value().doubleArray()[left_value.value().offset() + idx]);
        }
      } else {
        // right side
        if (right_value == null) {
          if (right_literal != null) {
            if (right_literal.isInteger() && long_values != null) {
              write(idx, right_literal.longValue());
            } else {
              write(idx, right_literal.toDouble());
            }
          } else {
            write(idx, Double.NaN);
          }
        } else if (right_value.value().isInteger()) {
          write(idx, right_value.value().longArray()[right_value.value().offset() + idx]);
        } else {
          write(idx, right_value.value().doubleArray()[right_value.value().offset() + idx]);
        }
      }
      
      idx++;
    } // end loop
    close();
    return this;
  }

  @Override
  public void close() {
    if (condition_iterator != null) {
      try {
        condition_iterator.close();
      } catch (IOException e) {
        // don't bother logging.
        e.printStackTrace();
      }
    }
    super.close();
  }

  @Override
  public TimeStamp timestamp() {
    return next_ts;
  }

  @Override
  public NumericArrayType value() {
    return this;
  }

  @Override
  public TypeToken<NumericArrayType> type() {
    return NumericArrayType.TYPE;
  }

  @Override
  public int offset() {
    return 0;
  }

  @Override
  public int end() {
    return long_values != null ? long_values.length : double_values.length;
  }

  @Override
  public boolean isInteger() {
    return long_values != null;
  }

  @Override
  public long[] longArray() {
    return long_values;
  }

  @Override
  public double[] doubleArray() {
    return double_values;
  }

  void write(final int index, final long value) {
    if (long_values != null) {
      long_values[index] = value;
    } else {
      double_values[index] = value;
    }
  }
  
  void write(final int index, final double value) {
    if (double_values == null) {
      double_values = new double[long_values.length];
      for (int x = 0; x < index; x++) {
        double_values[x] = long_values[x];
      }
      long_values = null;
    }
    double_values[index] = value;
  }
  
}