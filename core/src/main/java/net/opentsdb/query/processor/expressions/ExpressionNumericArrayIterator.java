//This file is part of OpenTSDB.
//Copyright (C) 2018-2019  The OpenTSDB Authors.
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may getNot use this file except in compliance with the License.
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

import java.util.Arrays;
import java.util.Map;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.exceptions.QueryDownstreamException;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.processor.expressions.ExpressionParseNode.OperandType;

/**
 * An iterator handling {@link NumericArrayType} data values.
 * 
 * @since 3.0
 */
public class ExpressionNumericArrayIterator extends 
    BaseExpressionNumericIterator<NumericArrayType>
  implements NumericArrayType {

  /** The iterators. */
  protected final TypedTimeSeriesIterator left;
  protected final TypedTimeSeriesIterator right;
  
  /** The values, either integers or doubles. */
  protected long[] long_values;
  protected double[] double_values;
  
  /**
   * Package private ctor.
   * @param node The non-null node this belongs to.
   * @param result The result set this belongs to.
   * @param sources The non-null map of results to fetch data from.
   */
  ExpressionNumericArrayIterator(final QueryNode node, 
                                 final QueryResult result,
                                 final Map<String, TimeSeries> sources) {
    super(node, result, sources);
    
    if (sources.get(ExpressionTimeSeries.LEFT_KEY) == null) {
      left = null;
      if (((BinaryExpressionNode) node).config().getLeftType() 
          == OperandType.LITERAL_BOOL ||
        ((BinaryExpressionNode) node).config().getLeftType() 
          == OperandType.LITERAL_NUMERIC ||
          ((BinaryExpressionNode) node).expressionConfig().getSubstituteMissing()) {
        has_next = true;
      }
    } else {
      left = sources.get(ExpressionTimeSeries.LEFT_KEY)
          .iterator(NumericArrayType.TYPE).get();
      has_next = left != null ? left.hasNext() : false;
    }
    
    if (sources.get(ExpressionTimeSeries.RIGHT_KEY) == null) {
      right = null;
      if (((BinaryExpressionNode) node).config().getRightType() 
            != OperandType.LITERAL_BOOL &&
          ((BinaryExpressionNode) node).config().getRightType() 
            != OperandType.LITERAL_NUMERIC &&
            !((BinaryExpressionNode) node).expressionConfig().getSubstituteMissing()) {
        has_next = false;
      }
    } else {
      right = sources.get(ExpressionTimeSeries.RIGHT_KEY)
          .iterator(NumericArrayType.TYPE).get();
      if (has_next) {
        has_next = right != null ? right.hasNext() : false;
      }
    }
    
    // final sanity check
    if (left == null && right == null) {
      throw new IllegalStateException("Must have at least one time "
          + "series in an expression.");
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

    // fills
    if (left == null) {
      left_value = new LiteralArray(
          right_value.value().end() - right_value.value().offset(),
            left_literal);
    } else if (right == null) {
      right_value = new LiteralArray(
          left_value.value().end() - left_value.value().offset(),
            right_literal);
    }
    next_ts.update(left != null ? left_value.timestamp() : right_value.timestamp());

    int left_offset = 0;
    int right_offset = 0;
    if (right_value != null && left_value.value().end() < right_value.value().end()) {
      right_offset = right_value.value().end() - left_value.value().end();
    }  else if (left_value != null && left_value.value().end() > right_value.value().end()) {
      left_offset = left_value.value().end() - right_value.value().end();
    }

    switch (((ExpressionParseNode) node.config()).getOperator()) {
    // logical
    case OR:
      runOr(left_value == null ? new Substitute(right_value.value().end(), 0) : left_value, 
            right_value == null ? new Substitute(left_value.value().end(), 0) : right_value, 
            left_offset, 
            right_offset);
      break;
    case AND:
      runAnd(left_value == null ? new Substitute(right_value.value().end(), 0) : left_value, 
          right_value == null ? new Substitute(left_value.value().end(), 0) : right_value, 
          left_offset, 
          right_offset);
      break;
    // relational
    case EQ:
      runEQ(left_value == null ? new Substitute(right_value.value().end(), 0) : left_value, 
          right_value == null ? new Substitute(left_value.value().end(), 0) : right_value, 
          left_offset, 
          right_offset);
      break;
    case NE:
      runNE(left_value == null ? new Substitute(right_value.value().end(), 0) : left_value, 
          right_value == null ? new Substitute(left_value.value().end(), 0) : right_value, 
          left_offset, 
          right_offset);
      break;
    case LE:
      runLE(left_value == null ? new Substitute(right_value.value().end(), 0) : left_value, 
          right_value == null ? new Substitute(left_value.value().end(), 0) : right_value, 
          left_offset, 
          right_offset);
      break;
    case GE:
      runGE(left_value == null ? new Substitute(right_value.value().end(), 0) : left_value, 
          right_value == null ? new Substitute(left_value.value().end(), 0) : right_value, 
          left_offset, 
          right_offset);
      break;
    case LT:
      runLT(left_value == null ? new Substitute(right_value.value().end(), 0) : left_value, 
          right_value == null ? new Substitute(left_value.value().end(), 0) : right_value, 
          left_offset, 
          right_offset);
      break;
    case GT:
      runGT(left_value == null ? new Substitute(right_value.value().end(), 0) : left_value, 
          right_value == null ? new Substitute(left_value.value().end(), 0) : right_value, 
          left_offset, 
          right_offset);
      break;
    // arithmetic
    case ADD:
      runAdd(left_value == null ? new Substitute(right_value.value().end(), 0) : left_value, 
          right_value == null ? new Substitute(left_value.value().end(), 0) : right_value, 
          left_offset, 
          right_offset);
      break;
    case SUBTRACT:
      runSubtract(left_value == null ? new Substitute(right_value.value().end(), 0) : left_value, 
          right_value == null ? new Substitute(left_value.value().end(), 0) : right_value, 
          left_offset, 
          right_offset);
      break;
    case DIVIDE:
      runDivide(left_value == null ? new Substitute(right_value.value().end(), 0) : left_value, 
          right_value == null ? new Substitute(left_value.value().end(), 1) : right_value, 
          left_offset, 
          right_offset);
      break;
    case MULTIPLY:
      runMultiply(left_value == null ? new Substitute(right_value.value().end(), 1) : left_value, 
          right_value == null ? new Substitute(left_value.value().end(), 1) : right_value, 
          left_offset, 
          right_offset);
      break;
    case MOD:
      runMod(left_value == null ? new Substitute(right_value.value().end(), 0) : left_value, 
          right_value == null ? new Substitute(left_value.value().end(), 1) : right_value, 
          left_offset, 
          right_offset);
      break;
    default:
      throw new QueryDownstreamException("Expression iterator was "
          + "told to handle the unexpected operator: " 
          + ((ExpressionParseNode) node.config()).getOperator());
    }
    
    return this;
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
  public TypeToken<? extends TimeSeriesDataType> getType() {
    return NumericArrayType.TYPE;
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
  
  /**
   * Implements the logical OR comparison of two values. Nulls are 
   * treated as false. Non-finite floating point values are treated as 
   * false except in the case when both are NaN.
   * <p>
   * <b>Note:</b> All values are returned as 1 for true and 0 for false as 
   * integer results.
   * 
   * @param left_value The left operand.
   * @param right_value The right operand.
   */
  void runOr(final TimeSeriesValue<NumericArrayType> left_value, 
             final TimeSeriesValue<NumericArrayType> right_value,
             final int left_offset, final int right_offset) {
    int idx = 0;
    int right_idx = right_offset;
    long_values = new long[left_value.value().end() - left_offset];
    
    if (left_value.value().isInteger() && right_value.value().isInteger()) {
      long[] left = left_value.value().longArray();
      long[] right = right_value.value().longArray();
      if (((ExpressionParseNode) node.config()).getNot()) {
        for (int left_idx = left_offset;
              left_idx < left_value.value().end(); left_idx++) {
          long_values[idx++] = left[left_idx] > 0 || 
              right[right_idx]  > 0 ? 0 : 1;
          right_idx++;
        }
      } else {
        for (int left_idx = left_offset;
              left_idx < left_value.value().end(); left_idx++) {
          long_values[idx++] = left[left_idx] > 0 || 
              right[right_idx] > 0 ? 1 : 0;
          right_idx++;
        }
      }
    } else {
      double[] left = left_value.value().isInteger() ? 
          convert(left_value.value().longArray()) : left_value.value().doubleArray();
      double[] right = right_value.value().isInteger() ?
          convert(right_value.value().longArray()) : right_value.value().doubleArray();
      if (((ExpressionParseNode) node.config()).getNot()) {
        for (int left_idx = left_offset;
              left_idx < left_value.value().end(); left_idx++) {
          if (Double.isNaN(left[left_idx]) && Double.isNaN(right[right_idx])) {
            long_values[idx++] = 0;
            right_idx++;
          } else {
            long_values[idx++] = left[left_idx] > 0 || 
                right[right_idx] > 0 ? 0 : 1;
            right_idx++;
          }
        }
      } else {
        for (int left_idx = left_offset;
              left_idx < left_value.value().end(); left_idx++) {
          if (Double.isNaN(left[left_idx]) && Double.isNaN(right[right_idx])) {
            long_values[idx++] = 1;
            right_idx++;
          } else {
            long_values[idx++] = left[left_idx] > 0 || 
                right[right_idx] > 0 ? 1 : 0;
            right_idx++;
          }
        }
      }
    }
  }
  
  /**
   * Implements the logical AND comparison of two values. Nulls are 
   * treated as false. Non-finite floating point values are treated as 
   * false except in the case when both are NaN.
   * <p>
   * <b>Note:</b> All values are returned as 1 for true and 0 for false as 
   * integer results.
   * 
   * @param left_value The left operand.
   * @param right_value The right operand.
   */
  void runAnd(final TimeSeriesValue<NumericArrayType> left_value, 
              final TimeSeriesValue<NumericArrayType> right_value,
              final int left_offset, final int right_offset) {
    int idx = 0;
    int right_idx = right_offset;
    long_values = new long[left_value.value().end() - left_offset];
    
    if (left_value.value().isInteger() && right_value.value().isInteger()) {
      long[] left = left_value.value().longArray();
      long[] right = right_value.value().longArray();
      if (((ExpressionParseNode) node.config()).getNot()) {
        for (int left_idx = left_offset;
              left_idx < left_value.value().end(); left_idx++) {
          long_values[idx++] = left[left_idx] > 0 && 
              right[right_idx]  > 0 ? 0 : 1;
          right_idx++;
        }
      } else {
        for (int left_idx = left_offset;
              left_idx < left_value.value().end(); left_idx++) {
          long_values[idx++] = left[left_idx] > 0 && 
              right[right_idx] > 0 ? 1 : 0;
          right_idx++;
        }
      }
    } else {
      double[] left = left_value.value().isInteger() ? 
          convert(left_value.value().longArray()) : left_value.value().doubleArray();
      double[] right = right_value.value().isInteger() ?
          convert(right_value.value().longArray()) : right_value.value().doubleArray();
      if (((ExpressionParseNode) node.config()).getNot()) {
        for (int left_idx = left_offset;
              left_idx < left_value.value().end(); left_idx++) {
          if (Double.isNaN(left[left_idx]) && Double.isNaN(right[right_idx])) {
            long_values[idx++] = 0;
            right_idx++;
          } else {
            long_values[idx++] = left[left_idx] > 0 && 
                right[right_idx] > 0 ? 0 : 1;
            right_idx++;
          }
        }
      } else {
        for (int left_idx = left_offset;
              left_idx < left_value.value().end(); left_idx++) {
          if (Double.isNaN(left[left_idx]) && Double.isNaN(right[right_idx])) {
            long_values[idx++] = 1;
            right_idx++;
          } else {
            long_values[idx++] = left[left_idx] > 0 && 
                right[right_idx] > 0 ? 1 : 0;
            right_idx++;
          }
        }
      }
    }
  }
  
  /**
   * Implements the equals comparison of two values. A null on either 
   * side will null the result. We follow IEEE 754 for NaN behavior.
   * <p>
   * <b>Note:</b> All values are returned as 1 for true and 0 for false as 
   * integer results.
   * 
   * @param left_value The left operand.
   * @param right_value The right operand.
   */
  void runEQ(final TimeSeriesValue<NumericArrayType> left_value, 
             final TimeSeriesValue<NumericArrayType> right_value,
             final int left_offset, final int right_offset) {
    int idx = 0;
    int right_idx = right_offset;
    long_values = new long[left_value.value().end() - left_offset];
    
    if (left_value.value().isInteger() && right_value.value().isInteger()) {
      long[] left = left_value.value().longArray();
      long[] right = right_value.value().longArray();
      
      if (((ExpressionParseNode) node.config()).getNot()) {
        for (int left_idx = left_offset;
              left_idx < left_value.value().end(); left_idx++) {
          long_values[idx++] = left[left_idx] == right[right_idx++] ? 0 : 1;
        }
      } else {
        for (int left_idx = left_offset;
              left_idx < left_value.value().end(); left_idx++) {
          long_values[idx++] = left[left_idx] == right[right_idx++] ? 1 : 0;
        }
      }
    } else {
      double[] left = left_value.value().isInteger() ? 
          convert(left_value.value().longArray()) : left_value.value().doubleArray();
      double[] right = right_value.value().isInteger() ?
          convert(right_value.value().longArray()) : right_value.value().doubleArray();
      // let the ieee 754 standard handle NaNs
      if (((ExpressionParseNode) node.config()).getNot()) {
        for (int left_idx = left_offset;
              left_idx < left_value.value().end(); left_idx++) {
          long_values[idx++] = left[left_idx] == right[right_idx++] ? 0 : 1;
        }
      } else {
        for (int left_idx = left_offset;
              left_idx < left_value.value().end(); left_idx++) {
          long_values[idx++] = left[left_idx] == right[right_idx++] ? 1 : 0;
        }
      }
    }
  }
  
  /**
   * Implements the not equal comparison of two values. A null on either 
   * side will null the result. We follow IEEE 754 for NaN behavior.
   * <p>
   * <b>Note:</b> All values are returned as 1 for true and 0 for false as 
   * integer results.
   * 
   * @param left_value The left operand.
   * @param right_value The right operand.
   */
  void runNE(final TimeSeriesValue<NumericArrayType> left_value, 
             final TimeSeriesValue<NumericArrayType> right_value,
             final int left_offset, final int right_offset) {
    int idx = 0;
    int right_idx = right_offset;
    long_values = new long[left_value.value().end() - left_offset];
    
    if (left_value.value().isInteger() && right_value.value().isInteger()) {
      long[] left = left_value.value().longArray();
      long[] right = right_value.value().longArray();
      
      if (((ExpressionParseNode) node.config()).getNot()) {
        for (int left_idx = left_offset;
              left_idx < left_value.value().end(); left_idx++) {
          long_values[idx++] = left[left_idx] != right[right_idx++] ? 0 : 1;
        }
      } else {
        for (int left_idx = left_offset;
              left_idx < left_value.value().end(); left_idx++) {
          long_values[idx++] = left[left_idx] != right[right_idx++] ? 1 : 0;
        }
      }
    } else {
      double[] left = left_value.value().isInteger() ? 
          convert(left_value.value().longArray()) : left_value.value().doubleArray();
      double[] right = right_value.value().isInteger() ?
          convert(right_value.value().longArray()) : right_value.value().doubleArray();
      // let the ieee 754 standard handle NaNs
      if (((ExpressionParseNode) node.config()).getNot()) {
        for (int left_idx = left_offset;
              left_idx < left_value.value().end(); left_idx++) {
          long_values[idx++] = left[left_idx] != right[right_idx++] ? 0 : 1;
        }
      } else {
        for (int left_idx = left_offset;
              left_idx < left_value.value().end(); left_idx++) {
          long_values[idx++] = left[left_idx] != right[right_idx++] ? 1 : 0;
        }
      }
    }
  }
  
  /**
   * Implements the less than or equal to comparison of two values. A 
   * null on either side will null the result. We follow IEEE 754 for 
   * NaN behavior.
   * <p>
   * <b>Note:</b> All values are returned as 1 for true and 0 for false as 
   * integer results.
   * 
   * @param left_value The left operand.
   * @param right_value The right operand.
   */
  void runLE(final TimeSeriesValue<NumericArrayType> left_value, 
             final TimeSeriesValue<NumericArrayType> right_value,
             final int left_offset, final int right_offset) {
    int idx = 0;
    int right_idx = right_offset;
    long_values = new long[left_value.value().end() - left_offset];
    
    if (left_value.value().isInteger() && right_value.value().isInteger()) {
      long[] left = left_value.value().longArray();
      long[] right = right_value.value().longArray();
      
      if (((ExpressionParseNode) node.config()).getNot()) {
        for (int left_idx = left_offset;
              left_idx < left_value.value().end(); left_idx++) {
          long_values[idx++] = left[left_idx] <= right[right_idx++] ? 0 : 1;
        }
      } else {
        for (int left_idx = left_offset;
              left_idx < left_value.value().end(); left_idx++) {
          long_values[idx++] = left[left_idx] <= right[right_idx++] ? 1 : 0;
        }
      }
    } else {
      double[] left = left_value.value().isInteger() ? 
          convert(left_value.value().longArray()) : left_value.value().doubleArray();
      double[] right = right_value.value().isInteger() ?
          convert(right_value.value().longArray()) : right_value.value().doubleArray();
      // let the ieee 754 standard handle NaNs
      if (((ExpressionParseNode) node.config()).getNot()) {
        for (int left_idx = left_offset;
              left_idx < left_value.value().end(); left_idx++) {
          long_values[idx++] = left[left_idx] <= right[right_idx++] ? 0 : 1;
        }
      } else {
        for (int left_idx = left_offset;
              left_idx < left_value.value().end(); left_idx++) {
          long_values[idx++] = left[left_idx] <= right[right_idx++] ? 1 : 0;
        }
      }
    }
  }
  
  /**
   * Implements the greater than or equal to comparison of two values. A 
   * null on either side will null the result. We follow IEEE 754 for 
   * NaN behavior.
   * <p>
   * <b>Note:</b> All values are returned as 1 for true and 0 for false as 
   * integer results.
   * 
   * @param left_value The left operand.
   * @param right_value The right operand.
   */
  void runGE(final TimeSeriesValue<NumericArrayType> left_value, 
             final TimeSeriesValue<NumericArrayType> right_value,
             final int left_offset, final int right_offset) {
    int idx = 0;
    int right_idx = right_offset;
    long_values = new long[left_value.value().end() - left_offset];
    
    if (left_value.value().isInteger() && right_value.value().isInteger()) {
      long[] left = left_value.value().longArray();
      long[] right = right_value.value().longArray();
      
      if (((ExpressionParseNode) node.config()).getNot()) {
        for (int left_idx = left_offset;
              left_idx < left_value.value().end(); left_idx++) {
          long_values[idx++] = left[left_idx] >= right[right_idx++] ? 0 : 1;
        }
      } else {
        for (int left_idx = left_offset;
              left_idx < left_value.value().end(); left_idx++) {
          long_values[idx++] = left[left_idx] >= right[right_idx++] ? 1 : 0;
        }
      }
    } else {
      double[] left = left_value.value().isInteger() ? 
          convert(left_value.value().longArray()) : left_value.value().doubleArray();
      double[] right = right_value.value().isInteger() ?
          convert(right_value.value().longArray()) : right_value.value().doubleArray();
      // let the ieee 754 standard handle NaNs
      if (((ExpressionParseNode) node.config()).getNot()) {
        for (int left_idx = left_offset;
              left_idx < left_value.value().end(); left_idx++) {
          long_values[idx++] = left[left_idx] >= right[right_idx++] ? 0 : 1;
        }
      } else {
        for (int left_idx = left_offset;
              left_idx < left_value.value().end(); left_idx++) {
          long_values[idx++] = left[left_idx] >= right[right_idx++] ? 1 : 0;
        }
      }
    }
  }
  
  /**
   * Implements the less than comparison of two values. A null on either 
   * side will null the result. We follow IEEE 754 for NaN behavior.
   * <p>
   * <b>Note:</b> All values are returned as 1 for true and 0 for false as 
   * integer results.
   * 
   * @param left_value The left operand.
   * @param right_value The right operand.
   */
  void runLT(final TimeSeriesValue<NumericArrayType> left_value, 
             final TimeSeriesValue<NumericArrayType> right_value,
             final int left_offset, final int right_offset) {
    int idx = 0;
    int right_idx = right_offset;
    long_values = new long[left_value.value().end() - left_offset];
    
    if (left_value.value().isInteger() && right_value.value().isInteger()) {
      long[] left = left_value.value().longArray();
      long[] right = right_value.value().longArray();
      
      if (((ExpressionParseNode) node.config()).getNot()) {
        for (int left_idx = left_offset;
              left_idx < left_value.value().end(); left_idx++) {
          long_values[idx++] = left[left_idx] < right[right_idx++] ? 0 : 1;
        }
      } else {
        for (int left_idx = left_offset;
              left_idx < left_value.value().end(); left_idx++) {
          long_values[idx++] = left[left_idx] < right[right_idx++] ? 1 : 0;
        }
      }
    } else {
      double[] left = left_value.value().isInteger() ? 
          convert(left_value.value().longArray()) : left_value.value().doubleArray();
      double[] right = right_value.value().isInteger() ?
          convert(right_value.value().longArray()) : right_value.value().doubleArray();
      // let the ieee 754 standard handle NaNs
      if (((ExpressionParseNode) node.config()).getNot()) {
        for (int left_idx = left_offset;
              left_idx < left_value.value().end(); left_idx++) {
          long_values[idx++] = left[left_idx] < right[right_idx++] ? 0 : 1;
        }
      } else {
        for (int left_idx = left_offset;
              left_idx < left_value.value().end(); left_idx++) {
          long_values[idx++] = left[left_idx] < right[right_idx++] ? 1 : 0;
        }
      }
    }
  }
  
  /**
   * Implements the greater than comparison of two values. A null on either 
   * side will null the result. We follow IEEE 754 for NaN behavior.
   * <p>
   * <b>Note:</b> All values are returned as 1 for true and 0 for false as 
   * integer results.
   * 
   * @param left_value The left operand.
   * @param right_value The right operand.
   */
  void runGT(final TimeSeriesValue<NumericArrayType> left_value, 
             final TimeSeriesValue<NumericArrayType> right_value,
             final int left_offset, final int right_offset) {
    int idx = 0;
    int right_idx = right_offset;
    long_values = new long[left_value.value().end() - left_offset];
    
    if (left_value.value().isInteger() && right_value.value().isInteger()) {
      long[] left = left_value.value().longArray();
      long[] right = right_value.value().longArray();
      
      if (((ExpressionParseNode) node.config()).getNot()) {
        for (int left_idx = left_offset;
              left_idx < left_value.value().end(); left_idx++) {
          long_values[idx++] = left[left_idx] > right[right_idx++] ? 0 : 1;
        }
      } else {
        for (int left_idx = left_offset;
              left_idx < left_value.value().end(); left_idx++) {
          long_values[idx++] = left[left_idx] > right[right_idx++] ? 1 : 0;
        }
      }
    } else {
      double[] left = left_value.value().isInteger() ? 
          convert(left_value.value().longArray()) : left_value.value().doubleArray();
      double[] right = right_value.value().isInteger() ?
          convert(right_value.value().longArray()) : right_value.value().doubleArray();
      // let the ieee 754 standard handle NaNs
      if (((ExpressionParseNode) node.config()).getNot()) {
        for (int left_idx = left_offset;
              left_idx < left_value.value().end(); left_idx++) {
          long_values[idx++] = left[left_idx] > right[right_idx++] ? 0 : 1;
        }
      } else {
        for (int left_idx = left_offset;
              left_idx < left_value.value().end(); left_idx++) {
          long_values[idx++] = left[left_idx] > right[right_idx++] ? 1 : 0;
        }
      }
    }
  }
  
  /**
   * Implements the sum of two values. A null on either side will null 
   * the result. A NaN in both returns NaN. Non-infectious NaN will just 
   * return the other operand.
   * <p>
   * Integer math is used unless a floating point is present in either
   * operand. getNote that values may overflow.
   * 
   * TODO - overflow for addition.
   * 
   * @param left_value The left operand.
   * @param right_value The right operand.
   */
  void runAdd(final TimeSeriesValue<NumericArrayType> left_value, 
              final TimeSeriesValue<NumericArrayType> right_value,
              final int left_offset, final int right_offset) {
    int idx = 0;
    int right_idx = right_offset;

    if (left_value.value().isInteger() && right_value.value().isInteger()) {
      long_values = new long[left_value.value().end() - left_offset];
      long[] left = left_value.value().longArray();
      long[] right = right_value.value().longArray();

      if (((ExpressionParseNode) node.config()).getNegate()) {
        for (int left_idx = left_offset;
              left_idx < left_value.value().end(); left_idx++) {
          long_values[idx++] = -(left[left_idx] + right[right_idx++]);
        }
      } else {
        for (int left_idx = left_offset;
              left_idx < left_value.value().end(); left_idx++) {
          long_values[idx++] = left[left_idx] + right[right_idx++];
        }
      }
      
    } else {
      double_values = new double[left_value.value().end() - left_offset];
      double[] left = left_value.value().isInteger() ? 
          convert(left_value.value().longArray()) : left_value.value().doubleArray();
      double[] right = right_value.value().isInteger() ?
          convert(right_value.value().longArray()) : right_value.value().doubleArray();
      if (((ExpressionParseNode) node.config()).getNegate()) {
        for (int left_idx = left_offset;
              left_idx < left_value.value().end(); left_idx++) {
          if (infectious_nan) {
            double_values[idx++] = -(left[left_idx] + right[right_idx++]);
          } else if (Double.isNaN(left[left_idx])) {
            double_values[idx++] = -right[right_idx++];
          } else if (Double.isNaN(right[right_idx])) {
            double_values[idx++] = -left[left_idx];
          } else {
            double_values[idx++] = -(left[left_idx] + right[right_idx++]);
          }
        }
      } else {
        for (int left_idx = left_offset;
              left_idx < left_value.value().end(); left_idx++) {
          if (infectious_nan) {
            double_values[idx++] = left[left_idx] + right[right_idx++];
          } else if (Double.isNaN(left[left_idx])) {
            double_values[idx++] = right[right_idx++];
          } else if (Double.isNaN(right[right_idx])) {
            double_values[idx++] = left[left_idx];
          } else {
            double_values[idx++] = left[left_idx] + right[right_idx++];
          }
        }
      }
      
    }
  }
  
  /**
   * Implements the difference of two values. A null on either side will 
   * null the result. A NaN in both returns NaN. Non-infectious NaN will 
   * just return the other operand.
   * <p>
   * Integer math is used unless a floating point is present in either
   * operand. getNote that values may overflow.
   * 
   * TODO - overflow for addition.
   * 
   * @param left_value The left operand.
   * @param right_value The right operand.
   */
  void runSubtract(final TimeSeriesValue<NumericArrayType> left_value, 
                   final TimeSeriesValue<NumericArrayType> right_value,
                   final int left_offset, final int right_offset) {
    int idx = 0;
    int right_idx = right_offset;

    if (left_value.value().isInteger() && right_value.value().isInteger()) {
      long_values = new long[left_value.value().end() - left_offset];
      long[] left = left_value.value().longArray();
      long[] right = right_value.value().longArray();

      if (((ExpressionParseNode) node.config()).getNegate()) {
        for (int left_idx = left_offset;
              left_idx < left_value.value().end(); left_idx++) {
          long_values[idx++] = -(left[left_idx] - right[right_idx++]);
        }
      } else {
        for (int left_idx = left_offset;
              left_idx < left_value.value().end(); left_idx++) {
          long_values[idx++] = left[left_idx] - right[right_idx++];
        }
      }
    } else {
      double_values = new double[left_value.value().end() - left_offset];
      double[] left = left_value.value().isInteger() ? 
          convert(left_value.value().longArray()) : left_value.value().doubleArray();
      double[] right = right_value.value().isInteger() ?
          convert(right_value.value().longArray()) : right_value.value().doubleArray();
      if (((ExpressionParseNode) node.config()).getNegate()) {
        for (int left_idx = left_offset;
              left_idx < left_value.value().end(); left_idx++) {
          if (infectious_nan) {
            double_values[idx++] = -(left[left_idx] - right[right_idx++]);
          } else if (Double.isNaN(left[left_idx])) {
            double_values[idx++] = -right[right_idx++];
          } else if (Double.isNaN(right[right_idx])) {
            double_values[idx++] = -left[left_idx];
          } else {
            double_values[idx++] = -(left[left_idx] - right[right_idx++]);
          }
        }
      } else {
        for (int left_idx = left_offset;
              left_idx < left_value.value().end(); left_idx++) {
          if (infectious_nan) {
            double_values[idx++] = left[left_idx] - right[right_idx++];
          } else if (Double.isNaN(left[left_idx])) {
            double_values[idx++] = right[right_idx++];
          } else if (Double.isNaN(right[right_idx])) {
            double_values[idx++] = left[left_idx];
          } else {
            double_values[idx++] = left[left_idx] - right[right_idx++];
          }
        }
      }
    }
  }
  
  /**
   * Implements the quotient of two values. A null on either side will null 
   * the result. A NaN in both returns NaN. Non-infectious NaN will just 
   * return the other operand.
   * <p>
   * <b>Note</b> A divide-by-zero operation will return 0. Likewise a 
   * NaN in the numerator or denominator will return a 0 unless both are
   * NaN or infectious NaN is enabled.
   * <p>
   * Integer math is used unless a floating point is present in either
   * operand OR the quotient would result in a float.
   * 
   * @param left_value The left operand.
   * @param right_value The right operand.
   */
  void runDivide(final TimeSeriesValue<NumericArrayType> left_value, 
                 final TimeSeriesValue<NumericArrayType> right_value,
                 final int left_offset, final int right_offset) {
    int idx = 0;
    int right_idx = right_offset;
    double_values = new double[left_value.value().end() - left_offset];
    double[] left = left_value.value().isInteger() ? 
        convert(left_value.value().longArray()) : left_value.value().doubleArray();
    double[] right = right_value.value().isInteger() ?
        convert(right_value.value().longArray()) : right_value.value().doubleArray();
    if (((ExpressionParseNode) node.config()).getNegate()) {
      for (int left_idx = left_offset;
            left_idx < left_value.value().end(); left_idx++) {
        if (Math.abs(0.0 - right[right_idx]) <= EPSILON) {
          double_values[idx++] = 0;
          right_idx++;
        } else if (infectious_nan) {
          double_values[idx++] = -(left[left_idx] / right[right_idx++]);
        } else if (Double.isNaN(left[left_idx]) || 
                  (Double.isNaN(right[right_idx]))) {
          double_values[idx++] = 0;
          right_idx++;
        } else {
          double_values[idx++] = -(left[left_idx] / right[right_idx++]);
        }
      }
    } else {
      for (int left_idx = left_offset;
            left_idx < left_value.value().end(); left_idx++) {
        if (Math.abs(0.0 - right[right_idx]) <= EPSILON) {
          double_values[idx++] = 0;
          right_idx++;
        } else if (infectious_nan) {
          double_values[idx++] = left[left_idx] / right[right_idx++];
        } else if (Double.isNaN(left[left_idx]) || 
            (Double.isNaN(right[right_idx]))) {
          double_values[idx++] = 0;
          right_idx++;
        } else {
          double_values[idx++] = left[left_idx] / right[right_idx++];
        }
      }
    }
  }
  
  /**
   * Implements the product of two values. A null on either side will null 
   * the result. A NaN in both returns NaN.
   * <p>
   * getNote that if a NaN is present in the left or right (but getNot both) 
   * then a 0 is returned.
   * <p>
   * Integer math is used unless a floating point is present in either
   * operand. getNote that values may overflow.
   * 
   * TODO - overflow.
   * 
   * @param left_value The left operand.
   * @param right_value The right operand.
   */
  void runMultiply(final TimeSeriesValue<NumericArrayType> left_value, 
                   final TimeSeriesValue<NumericArrayType> right_value,
                   final int left_offset, final int right_offset) {
    int idx = 0;
    int right_idx = right_offset;

    if (left_value.value().isInteger() && right_value.value().isInteger()) {
      long_values = new long[left_value.value().end() - left_offset];
      long[] left = left_value.value().longArray();
      long[] right = right_value.value().longArray();

      // TODO - deal with overflow
      if (((ExpressionParseNode) node.config()).getNegate()) {
        for (int left_idx = left_offset;
            left_idx < left_value.value().end(); left_idx++) {
          long_values[idx++] = -(left[left_idx] * right[right_idx++]);
        }
      } else {
        for (int left_idx = left_offset;
            left_idx < left_value.value().end(); left_idx++) {
          long_values[idx++] = left[left_idx] * right[right_idx++];
        }        
      }
    } else {
      double_values = new double[left_value.value().end() - left_offset];
      double[] left = left_value.value().isInteger() ? 
          convert(left_value.value().longArray()) : left_value.value().doubleArray();
      double[] right = right_value.value().isInteger() ?
          convert(right_value.value().longArray()) : right_value.value().doubleArray();
      if (((ExpressionParseNode) node.config()).getNegate()) {
        for (int left_idx = left_offset;
              left_idx < left_value.value().end(); left_idx++) {
          if (infectious_nan) {
            double_values[idx++] = -(left[left_idx] * right[right_idx++]);
          } else if (Double.isNaN(left[left_idx]) || 
              (Double.isNaN(right[right_idx]))) {
            double_values[idx++] = 0;
            right_idx++;
          } else {
            double_values[idx++] = -(left[left_idx] * right[right_idx++]);
          }
        }
      } else {
        for (int left_idx = left_offset;
              left_idx < left_value.value().end(); left_idx++) {
          if (infectious_nan) {
            double_values[idx++] = left[left_idx] * right[right_idx++];
          } else if (Double.isNaN(left[left_idx]) || 
              (Double.isNaN(right[right_idx]))) {
            double_values[idx++] = 0;
            right_idx++;
          } else {
            double_values[idx++] = left[left_idx] * right[right_idx++];
          }
        }
      }
    }
  }
  
  /**
   * Implements the modulus of two values. A null on either side will null 
   * the result. A NaN in both returns NaN.
   * <p>
   * getNote that if a NaN is present in the left or right (but getNot both) then
   * a 0 is returned.
   * <p>
   * Integer math is used unless a floating point is present in either
   * operand.
   * 
   * @param left_value The left operand.
   * @param right_value The right operand.
   */
  void runMod(final TimeSeriesValue<NumericArrayType> left_value, 
              final TimeSeriesValue<NumericArrayType> right_value,
              final int left_offset, final int right_offset) {
    int idx = 0;
    int right_idx = right_offset;

    if (left_value.value().isInteger() && right_value.value().isInteger()) {
      long_values = new long[left_value.value().end() - left_offset];
      long[] left = left_value.value().longArray();
      long[] right = right_value.value().longArray();

      // TODO - deal with overflow
      if (((ExpressionParseNode) node.config()).getNegate()) {
        for (int left_idx = left_offset;
            left_idx < left_value.value().end(); left_idx++) {
          if (right[right_idx] == 0) {
            long_values[idx++] = 0;
            right_idx++;
          } else {
            long_values[idx++] = -(left[left_idx] % right[right_idx++]);
          }
        }
      } else {
        for (int left_idx = left_offset;
            left_idx < left_value.value().end(); left_idx++) {
          if (right[right_idx] == 0) {
            long_values[idx++] = 0;
            right_idx++;
          } else {
            long_values[idx++] = left[left_idx] % right[right_idx++];
          }
        }        
      }
    } else {
      double_values = new double[left_value.value().end() - left_offset];
      double[] left = left_value.value().isInteger() ? 
          convert(left_value.value().longArray()) : left_value.value().doubleArray();
      double[] right = right_value.value().isInteger() ?
          convert(right_value.value().longArray()) : right_value.value().doubleArray();
      if (((ExpressionParseNode) node.config()).getNegate()) {
        for (int left_idx = left_offset;
              left_idx < left_value.value().end(); left_idx++) {
          if (Math.abs(0.0 - right[right_idx]) <= EPSILON) {
            double_values[idx++] = 0;
            right_idx++;
          } else if (infectious_nan) {
            double_values[idx++] = -(left[left_idx] % right[right_idx++]);
          } else if (Double.isNaN(left[left_idx]) || 
              (Double.isNaN(right[right_idx]))) {
            double_values[idx++] = 0;
            right_idx++;
          } else {
            double_values[idx++] = -(left[left_idx] % right[right_idx++]);
          }
        }
      } else {
        for (int left_idx = left_offset;
              left_idx < left_value.value().end(); left_idx++) {
          if (Math.abs(0.0 - right[right_idx]) <= EPSILON) {
            double_values[idx++] = 0;
            right_idx++;
          } else if (infectious_nan) {
            double_values[idx++] = left[left_idx] % right[right_idx++];
          } else if (Double.isNaN(left[left_idx]) || 
              (Double.isNaN(right[right_idx]))) {
            double_values[idx++] = 0;
            right_idx++;
          } else {
            double_values[idx++] = left[left_idx] % right[right_idx++];
          }
        }
      }
    }
  }
  
  /**
   * Ugly way to cast from a long to a double. Note that this doesn't 
   * care about offsets and ends, just copies the full array over so
   * there is potential that it'll do extra work.
   * @param values The non-null longs to convert.
   * @return A double array the same length of the values.
   */
  double[] convert(final long[] values) {
    final double[] converted = new double[values.length];
    for (int i = 0; i < values.length; i++) {
      converted[i] = (double) values[i];
    }
    return converted;
  }

  /**
   * Populates an array of the proper type and length with the fill
   * value.
   */
  class LiteralArray implements TimeSeriesValue<NumericArrayType>,
                                NumericArrayType{
    
    private long[] long_values;
    private double[] double_values;
    
    LiteralArray(final int length, final NumericType literal) {
      if (literal == null) {
        // TODO - dunno if this is right.
        long_values = new long[length];
      } else if (literal.isInteger()) {
        long_values = new long[length];
        Arrays.fill(long_values, literal.longValue());
      } else {
        double_values = new double[length];
        Arrays.fill(double_values, literal.doubleValue());
      }
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
      return long_values != null ? long_values.length : 
        double_values.length;
    }

    @Override
    public boolean isInteger() {
      return long_values != null ? true : false;
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

  /** Helper to substitute a missing time series. */
  class Substitute implements TimeSeriesValue<NumericArrayType>, NumericArrayType {
    final long[] array;
    
    Substitute(final int length, final int value) {
      array = new long[length];
      if (value != 0) {
        Arrays.fill(array, value);
      }
    }
    
    @Override
    public TimeStamp timestamp() {
      // not used here
      return null;
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
      return array.length;
    }

    @Override
    public boolean isInteger() {
      return true;
    }

    @Override
    public long[] longArray() {
      return array;
    }

    @Override
    public double[] doubleArray() {
      return null;
    }
  }
}
