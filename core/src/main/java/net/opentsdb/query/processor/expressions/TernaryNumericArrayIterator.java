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
  
  /** The condition time series. */
  protected TypedTimeSeriesIterator condition;
  
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
      has_next = false;
      return;
    }
    final Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> op = 
        c.iterator(NumericArrayType.TYPE);
    if (!op.isPresent()) {
      // can't do anything so leave has_next as false.
      has_next = false;
      return;
    }
    condition = op.get();
    has_next = condition.hasNext();
    
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
    TimeSeriesValue<NumericArrayType> condition_value = 
        (TimeSeriesValue<NumericArrayType>) condition.next();
    TimeSeriesValue<NumericArrayType> left_value = 
        left != null ?
        (TimeSeriesValue<NumericArrayType>) left.next() : null;
    TimeSeriesValue<NumericArrayType> right_value = 
        right != null ?
        (TimeSeriesValue<NumericArrayType>) right.next() : null;
    
    next_ts.update(condition_value.timestamp());
    
    // TODO - pools
    if (left_value != null && right_value != null) {
      if (left_value.value().isInteger() && right_value.value().isInteger()) {
        long_values = new long[condition_value.value().end() - 
                               condition_value.value().offset()];
      } else {
        double_values = new double[condition_value.value().end() - 
                                   condition_value.value().offset()];
      }
    } else {
      long_values = new long[condition_value.value().end() - 
                             condition_value.value().offset()];
    }
    
    int idx = 0;
    if (condition_value.value().isInteger()) {
      for (int i = condition_value.value().offset(); 
              i < condition_value.value().end(); i++) {
        if (condition_value.value().longArray()[i] > 0) {
          if (left_value == null) {
            // literal or fill
            if (left_literal != null) {
              if (left_literal.isInteger() && long_values != null) {
                long_values[idx] = left_literal.longValue();
              } else {
                if (double_values == null) {
                  // copy
                  double_values = new double[long_values.length];
                  for (int x = 0; x < idx; x++) {
                    double_values[x] = long_values[x];
                  }
                  long_values = null;
                }
                double_values[idx] = left_literal.toDouble();
              }
            } else {
              // TODO Shouldn't be here?
              
            }
          } else if (left_value.value().isInteger() && long_values != null) {
            long_values[idx] = 
                left_value.value().longArray()[left_value.value().offset() + idx];
          } else if (left_value.value().isInteger()) {
            double_values[idx] = 
                left_value.value().longArray()[left_value.value().offset() + idx];
          } else {
            double_values[idx] = 
                left_value.value().doubleArray()[left_value.value().offset() + idx];
          }
        } else {
          if (right_value == null) {
            // literal or fill
            if (right_literal != null) {
              if (right_literal.isInteger() && long_values != null) {
                long_values[idx] = right_literal.longValue();
              } else {
                if (double_values == null) {
                  // copy
                  double_values = new double[long_values.length];
                  for (int x = 0; x < idx; x++) {
                    double_values[x] = long_values[x];
                  }
                  long_values = null;
                }
                double_values[idx] = right_literal.toDouble();
              }
            } else {
              // TODO Shouldn't be here?
              
            }
          } else if (right_value.value().isInteger() && long_values != null) {
            long_values[idx] = 
                right_value.value().longArray()[right_value.value().offset() + idx];
          } else if (right_value.value().isInteger()) {
            double_values[idx] = 
                right_value.value().longArray()[right_value.value().offset() + idx];
          } else {
            double_values[idx] = 
                right_value.value().doubleArray()[right_value.value().offset() + idx];
          }
        }
        idx++;
      }
    } else {
      for (int i = condition_value.value().offset(); 
              i < condition_value.value().end(); i++) {
        // TODO - how _should_ we treat nans?
        if (Double.isFinite(condition_value.value().doubleArray()[i]) && 
            condition_value.value().doubleArray()[i] > 0) {
          if (left_value == null) {
            // literal or fill
            if (left_literal != null) {
              if (left_literal.isInteger() && long_values != null) {
                long_values[idx] = left_literal.longValue();
              } else {
                if (double_values == null) {
                  // copy
                  double_values = new double[long_values.length];
                  for (int x = 0; x < idx; x++) {
                    double_values[x] = long_values[x];
                  }
                  long_values = null;
                }
                double_values[idx] = left_literal.toDouble();
              }
            } else {
              // TODO Shouldn't be here?
              
            }
          } else if (left_value.value().isInteger() && long_values != null) {
            long_values[idx] = 
                left_value.value().longArray()[left_value.value().offset() + idx];
          } else if (left_value.value().isInteger()) {
            double_values[idx] = 
                left_value.value().longArray()[left_value.value().offset() + idx];
          } else {
            double_values[idx] = 
                left_value.value().doubleArray()[left_value.value().offset() + idx];
          }
        } else {
          if (right_value == null) {
            // literal or fill
            if (right_literal != null) {
              if (right_literal.isInteger() && long_values != null) {
                long_values[idx] = right_literal.longValue();
              } else {
                if (double_values == null) {
                  // copy
                  double_values = new double[long_values.length];
                  for (int x = 0; x < idx; x++) {
                    double_values[x] = long_values[x];
                  }
                  long_values = null;
                }
                double_values[idx] = right_literal.toDouble();
              }
            } else {
              // TODO Shouldn't be here?
              
            }
          } else if (right_value.value().isInteger() && long_values != null) {
            long_values[idx] = 
                right_value.value().longArray()[right_value.value().offset() + idx];
          } else if (right_value.value().isInteger()) {
            double_values[idx] = 
                right_value.value().longArray()[right_value.value().offset() + idx];
          } else {
            double_values[idx] = 
                right_value.value().doubleArray()[right_value.value().offset() + idx];
          }
        }
        idx++;
      }
    }
    close();
    return this;
  }

  @Override
  public void close() {
    if (condition != null) {
      try {
        condition.close();
      } catch (IOException e) {
        // don't bother logging.
        e.printStackTrace();
      }
      condition = null;
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

}