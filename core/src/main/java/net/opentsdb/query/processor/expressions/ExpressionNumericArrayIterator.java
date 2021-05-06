//This file is part of OpenTSDB.
//Copyright (C) 2018-2021  The OpenTSDB Authors.
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

import java.io.IOException;
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

  private static final int STATIC_ARRAY_LEN = 86_400;
  private static final double[] NAN_ARRAY = new double[STATIC_ARRAY_LEN];
  static {
    Arrays.fill(NAN_ARRAY, Double.NaN);
  }
  
  private static ThreadLocal<long[]> LITERAL_LONGS = new ThreadLocal<long[]>() {
    @Override
    protected long[] initialValue() {
      return new long[STATIC_ARRAY_LEN];
    }
  };
  private static ThreadLocal<double[]> LITERAL_DOUBLES = new ThreadLocal<double[]>() {
    @Override
    protected double[] initialValue() {
      return new double[STATIC_ARRAY_LEN];
    }
  };
  
  /** The iterators. */
  protected TypedTimeSeriesIterator<?> left;
  protected TypedTimeSeriesIterator<?> right;

  protected final boolean infectious_nan;
  protected final boolean negate_or_not;
  protected boolean left_is_int;
  protected boolean right_is_int;
  
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
    infectious_nan = ((BinaryExpressionNode) node).expressionConfig().getInfectiousNan();
    if (sources.get(ExpressionTimeSeries.LEFT_KEY) == null) {
      left = null;
      if (((BinaryExpressionNode) node).config().getLeftType() 
          == OperandType.LITERAL_BOOL ||
        ((BinaryExpressionNode) node).config().getLeftType() 
          == OperandType.LITERAL_NUMERIC || !infectious_nan) {
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
            != OperandType.LITERAL_NUMERIC && infectious_nan) {
        has_next = false;
      }
    } else {
      right = sources.get(ExpressionTimeSeries.RIGHT_KEY)
          .iterator(NumericArrayType.TYPE).get();
      if (has_next) {
        has_next = right != null ? right.hasNext() : false;
      }
    }
    
    negate_or_not = ((BinaryExpressionNode) node).config().getNegate() ||
        ((BinaryExpressionNode) node).config().getNot();
    
    // final sanity check
    if (this.getClass().equals(ExpressionNumericArrayIterator.class) &&
        left == null && right == null) {
      // only throw if we're the super class.
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
      ExpressionParseNode config = node.config();
      if (config.getLeftId() != null &&
          config.getRightId() != null &&
          config.getLeftId().equals(config.getRightId())) {
        right_value = left_value;
      } else {
        right_value = new LiteralArray(
                left_value.value().end() - left_value.value().offset(),
                right_literal);
      }
    }
    next_ts.update(left != null ? left_value.timestamp() : right_value.timestamp());

    int left_offset = 0;
    int right_offset = 0;
    if (right_value != null && left_value.value().end() < right_value.value().end()) {
      right_offset = right_value.value().end() - left_value.value().end();
    }  else if (left_value != null && left_value.value().end() > right_value.value().end()) {
      left_offset = left_value.value().end() - right_value.value().end();
    }
    
    left_is_int = left_value != null && 
                  left_value.value() != null && 
                  left_value.value().isInteger();
    right_is_int = right_value != null && 
                   right_value.value() != null && 
                   right_value.value().isInteger();

    switch (((ExpressionParseNode) node.config()).getOperator()) {
    // logical
    case OR:
      runOr(left_value == null ? 
              new Substitute(right_value.value().end()) : left_value, 
          right_value == null ? 
              new Substitute(left_value.value().end()) : right_value, 
          left_offset, 
          right_offset);
      break;
    case AND:
      runAnd(left_value == null? 
              new Substitute(right_value.value().end()) : left_value, 
          right_value == null? 
              new Substitute(left_value.value().end()) : right_value, 
          left_offset, 
          right_offset);
      break;
    // relational
    case EQ:
      runEQ(left_value == null? 
              new Substitute(right_value.value().end()) : left_value, 
          right_value == null? 
              new Substitute(left_value.value().end()) : right_value, 
          left_offset, 
          right_offset);
      break;
    case NE:
      runNE(left_value == null? 
              new Substitute(right_value.value().end()) : left_value, 
          right_value == null? 
              new Substitute(left_value.value().end()) : right_value, 
          left_offset, 
          right_offset);
      break;
    case LE:
      runLE(left_value == null? 
              new Substitute(right_value.value().end()) : left_value, 
          right_value == null? 
              new Substitute(left_value.value().end()) : right_value, 
          left_offset, 
          right_offset);
      break;
    case GE:
      runGE(left_value == null? 
              new Substitute(right_value.value().end()) : left_value, 
          right_value == null? 
              new Substitute(left_value.value().end()) : right_value, 
          left_offset, 
          right_offset);
      break;
    case LT:
      runLT(left_value == null? 
              new Substitute(right_value.value().end()) : left_value, 
          right_value == null? 
              new Substitute(left_value.value().end()) : right_value, 
          left_offset, 
          right_offset);
      break;
    case GT:
      runGT(left_value == null? 
              new Substitute(right_value.value().end()) : left_value, 
          right_value == null? 
              new Substitute(left_value.value().end()) : right_value, 
          left_offset, 
          right_offset);
      break;
    // arithmetic
    case ADD:
      runAdd(left_value == null? 
              new Substitute(right_value.value().end()) : left_value, 
          right_value == null? 
              new Substitute(left_value.value().end()) : right_value, 
          left_offset, 
          right_offset);
      break;
    case SUBTRACT:
      runSubtract(left_value == null? 
              new Substitute(right_value.value().end()) : left_value, 
          right_value == null? 
              new Substitute(left_value.value().end()) : right_value, 
          left_offset, 
          right_offset);
      break;
    case DIVIDE:
      runDivide(left_value == null? 
              new Substitute(right_value.value().end()) : left_value, 
          right_value == null? 
              new Substitute(left_value.value().end()) : right_value, 
          left_offset, 
          right_offset);
      break;
    case MULTIPLY:
      runMultiply(left_value == null? 
              new Substitute(right_value.value().end()) : left_value, 
          right_value == null? 
              new Substitute(left_value.value().end()) : right_value, 
          left_offset, 
          right_offset);
      break;
    case MOD:
      runMod(left_value == null? 
              new Substitute(right_value.value().end()) : left_value, 
          right_value == null? 
              new Substitute(left_value.value().end()) : right_value, 
          left_offset, 
          right_offset);
      break;
    default:
      throw new QueryDownstreamException("Expression iterator was "
          + "told to handle the unexpected operator: " 
          + ((ExpressionParseNode) node.config()).getOperator());
    }
    
    close();
    
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
  public void close() {
    if (left != null) {
      try {
        left.close();
      } catch (IOException e) {
        // don't bother logging.
        e.printStackTrace();
      }
      left = null;
    }
    if (right != null) {
      try {
        right.close();
      } catch (IOException e) {
        // don't bother logging.
        e.printStackTrace();
      }
      right = null;
    }
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
   * Implements the logical OR comparison of two values. Non-finite floating 
   * point values are treated as false except in the case when either side are
   * NaN and infectious NaN is enabled.
   * <p>
   * <b>Note:</b> All values are returned as 1 for true and 0 for false as 
   * integer results (except if infectious NaNs were countered).
   * 
   * @param left_value The left operand.
   * @param right_value The right operand.
   * @param left_offset The offset into the left array.
   * @param right_offset The offset into the right array.
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
      for (int left_idx = left_offset;
          left_idx < left_value.value().end(); left_idx++) {
        if (left[left_idx] > 0 || right[right_idx] > 0) {
          long_values[idx++] = negate_or_not ? 0 : 1;
        } else {
          long_values[idx++] = negate_or_not ? 1 : 0;
        }
        right_idx++;
      }
    } else {
      for (int left_idx = left_offset;
          left_idx < left_value.value().end(); left_idx++) {
        final double left = left_is_int ? left_value.value().longArray()[left_idx] :
          left_value.value().doubleArray()[left_idx];
        final double right = right_is_int ? right_value.value().longArray()[right_idx++] :
          right_value.value().doubleArray()[right_idx++];
        if (infectious_nan && 
            (Double.isNaN(left) || Double.isNaN(right))) {
          writeResult(idx++, Double.NaN);
        } else {
          if (left > 0 || right > 0) {
            writeResult(idx++, negate_or_not ? 0 : 1);
          } else {
            writeResult(idx++, negate_or_not ? 1 : 0);
          }
        }
      }
    }
  }
  
  /**
   * Implements the logical AND comparison of two values. Non-finite floating 
   * point values are treated as false except in the case when either side are
   * NaN and infectious Nan is enabled.
   * <p>
   * <b>Note:</b> All values are returned as 1 for true and 0 for false as 
   * integer results (except if infectious NaNs were countered).
   * 
   * @param left_value The left operand.
   * @param right_value The right operand.
   * @param left_offset The offset into the left array.
   * @param right_offset The offset into the right array.
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
      for (int left_idx = left_offset;
          left_idx < left_value.value().end(); left_idx++) {
        if (left[left_idx] > 0 && right[right_idx] > 0) {
          long_values[idx++] = negate_or_not ? 0 : 1;
        } else {
          long_values[idx++] = negate_or_not ? 1 : 0;
        }
        right_idx++;
      }
    } else {
      for (int left_idx = left_offset;
          left_idx < left_value.value().end(); left_idx++) {
        final double left = left_is_int ? left_value.value().longArray()[left_idx] :
          left_value.value().doubleArray()[left_idx];
        final double right = right_is_int ? right_value.value().longArray()[right_idx++] :
          right_value.value().doubleArray()[right_idx++];
        if (infectious_nan && 
            (Double.isNaN(left) || Double.isNaN(right))) {
          writeResult(idx++, Double.NaN);
        } else {
          if (left > 0 && right > 0) {
            writeResult(idx++, negate_or_not ? 0 : 1);
          } else {
            writeResult(idx++, negate_or_not ? 1 : 0);
          }
        }
      }
    }
  }
  
  /**
   * Implements the equals comparison of two values. We follow IEEE 754 for NaN 
   * behavior when infectious nans is false.
   * <p>
   * <b>Note:</b> All values are returned as 1 for true and 0 for false as 
   * integer results.
   * 
   * @param left_value The left operand.
   * @param right_value The right operand.
   * @param left_offset The offset into the left array.
   * @param right_offset The offset into the right array.
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
      for (int left_idx = left_offset;
          left_idx < left_value.value().end(); left_idx++) {
        if (left[left_idx] == right[right_idx]) {
          long_values[idx++] = negate_or_not ? 0 : 1;
        } else {
          long_values[idx++] = negate_or_not ? 1 : 0;
        }
        right_idx++;
      }
    } else {
      for (int left_idx = left_offset;
          left_idx < left_value.value().end(); left_idx++) {
        final double left = left_is_int ? left_value.value().longArray()[left_idx] :
          left_value.value().doubleArray()[left_idx];
        final double right = right_is_int ? right_value.value().longArray()[right_idx++] :
          right_value.value().doubleArray()[right_idx++];
        if (infectious_nan && 
            (Double.isNaN(left) || Double.isNaN(right))) {
          writeResult(idx++, Double.NaN);
        } else {
          // let the ieee 754 standard handle NaNs when not infectious
          if (left == right) {
            writeResult(idx++, negate_or_not ? 0 : 1);
          } else {
            writeResult(idx++, negate_or_not ? 1 : 0);
          }
        }
      }
    }
  }
  
  /**
   * Implements the not equal comparison of two values. We follow IEEE 754 for 
   * NaN behavior if infectious nans is false.
   * <p>
   * <b>Note:</b> All values are returned as 1 for true and 0 for false as 
   * integer results.
   * 
   * @param left_value The left operand.
   * @param right_value The right operand.
   * @param left_offset The offset into the left array.
   * @param right_offset The offset into the right array.
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
      for (int left_idx = left_offset;
          left_idx < left_value.value().end(); left_idx++) {
        if (left[left_idx] != right[right_idx]) {
          long_values[idx++] = negate_or_not ? 0 : 1;
        } else {
          long_values[idx++] = negate_or_not ? 1 : 0;
        }
        right_idx++;
      }
    } else {
      for (int left_idx = left_offset;
          left_idx < left_value.value().end(); left_idx++) {
        final double left = left_is_int ? left_value.value().longArray()[left_idx] :
          left_value.value().doubleArray()[left_idx];
        final double right = right_is_int ? right_value.value().longArray()[right_idx++] :
          right_value.value().doubleArray()[right_idx++];
        if (infectious_nan && 
            (Double.isNaN(left) || Double.isNaN(right))) {
          writeResult(idx++, Double.NaN);
        } else {
          // let the ieee 754 standard handle NaNs when not infectious
          if (left != right) {
            writeResult(idx++, negate_or_not ? 0 : 1);
          } else {
            writeResult(idx++, negate_or_not ? 1 : 0);
          }
        }
      }
    }
  }
  
  /**
   * Implements the less than or equal to comparison of two values. We follow 
   * IEEE 754 for NaN behavior when infectious nans is false.
   * <p>
   * <b>Note:</b> All values are returned as 1 for true and 0 for false as 
   * integer results.
   * 
   * @param left_value The left operand.
   * @param right_value The right operand.
   * @param left_offset The offset into the left array.
   * @param right_offset The offset into the right array.
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
      for (int left_idx = left_offset;
          left_idx < left_value.value().end(); left_idx++) {
        if (left[left_idx] <= right[right_idx]) {
          long_values[idx++] = negate_or_not ? 0 : 1;
        } else {
          long_values[idx++] = negate_or_not ? 1 : 0;
        }
        right_idx++;
      }
    } else {
      for (int left_idx = left_offset;
          left_idx < left_value.value().end(); left_idx++) {
        final double left = left_is_int ? left_value.value().longArray()[left_idx] :
          left_value.value().doubleArray()[left_idx];
        final double right = right_is_int ? right_value.value().longArray()[right_idx++] :
          right_value.value().doubleArray()[right_idx++];
        if (infectious_nan && 
            (Double.isNaN(left) || Double.isNaN(right))) {
          writeResult(idx++, Double.NaN);
        } else {
          // let the ieee 754 standard handle NaNs when not infectious
          if (left <= right) {
            writeResult(idx++, negate_or_not ? 0 : 1);
          } else {
            writeResult(idx++, negate_or_not ? 1 : 0);
          }
        }
      }
    }
  }
  
  /**
   * Implements the greater than or equal to comparison of two values. We follow 
   * IEEE 754 for NaN behavior when infectious nans is false.
   * <p>
   * <b>Note:</b> All values are returned as 1 for true and 0 for false as 
   * integer results.
   * 
   * @param left_value The left operand.
   * @param right_value The right operand.
   * @param left_offset The offset into the left array.
   * @param right_offset The offset into the right array.
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
      for (int left_idx = left_offset;
          left_idx < left_value.value().end(); left_idx++) {
        if (left[left_idx] >= right[right_idx]) {
          long_values[idx++] = negate_or_not ? 0 : 1;
        } else {
          long_values[idx++] = negate_or_not ? 1 : 0;
        }
        right_idx++;
      }
    } else {
      for (int left_idx = left_offset;
          left_idx < left_value.value().end(); left_idx++) {
        final double left = left_is_int ? left_value.value().longArray()[left_idx] :
          left_value.value().doubleArray()[left_idx];
        final double right = right_is_int ? right_value.value().longArray()[right_idx++] :
          right_value.value().doubleArray()[right_idx++];
        if (infectious_nan && 
            (Double.isNaN(left) || Double.isNaN(right))) {
          writeResult(idx++, Double.NaN);
        } else {
          // let the ieee 754 standard handle NaNs when not infectious
          if (left >= right) {
            writeResult(idx++, negate_or_not ? 0 : 1);
          } else {
            writeResult(idx++, negate_or_not ? 1 : 0);
          }
        }
      }
    }
  }
  
  /**
   * Implements the less than comparison of two values. We follow IEEE 754 for 
   * NaN behavior when infectious NaNs is false.
   * <p>
   * <b>Note:</b> All values are returned as 1 for true and 0 for false as 
   * integer results.
   * 
   * @param left_value The left operand.
   * @param right_value The right operand.
   * @param left_offset The offset into the left array.
   * @param right_offset The offset into the right array.
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
      for (int left_idx = left_offset;
          left_idx < left_value.value().end(); left_idx++) {
        if (left[left_idx] < right[right_idx]) {
          long_values[idx++] = negate_or_not ? 0 : 1;
        } else {
          long_values[idx++] = negate_or_not ? 1 : 0;
        }
        right_idx++;
      }
    } else {
      for (int left_idx = left_offset;
          left_idx < left_value.value().end(); left_idx++) {
        final double left = left_is_int ? left_value.value().longArray()[left_idx] :
          left_value.value().doubleArray()[left_idx];
        final double right = right_is_int ? right_value.value().longArray()[right_idx++] :
          right_value.value().doubleArray()[right_idx++];
        if (infectious_nan && 
            (Double.isNaN(left) || Double.isNaN(right))) {
          writeResult(idx++, Double.NaN);
        } else {
          // let the ieee 754 standard handle NaNs when not infectious
          if (left < right) {
            writeResult(idx++, negate_or_not ? 0 : 1);
          } else {
            writeResult(idx++, negate_or_not ? 1 : 0);
          }
        }
      }
    }
  }
  
  /**
   * Implements the greater than comparison of two values. We follow IEEE 754 
   * for NaN behavior if infectious nans is false.
   * <p>
   * <b>Note:</b> All values are returned as 1 for true and 0 for false as 
   * integer results.
   * 
   * @param left_value The left operand.
   * @param right_value The right operand.
   * @param left_offset The offset into the left array.
   * @param right_offset The offset into the right array.
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
      for (int left_idx = left_offset;
          left_idx < left_value.value().end(); left_idx++) {
        if (left[left_idx] > right[right_idx]) {
          long_values[idx++] = negate_or_not ? 0 : 1;
        } else {
          long_values[idx++] = negate_or_not ? 1 : 0;
        }
        right_idx++;
      }
    } else {
      for (int left_idx = left_offset;
          left_idx < left_value.value().end(); left_idx++) {
        final double left = left_is_int ? left_value.value().longArray()[left_idx] :
          left_value.value().doubleArray()[left_idx];
        final double right = right_is_int ? right_value.value().longArray()[right_idx++] :
          right_value.value().doubleArray()[right_idx++];
        if (infectious_nan && 
            (Double.isNaN(left) || Double.isNaN(right))) {
          writeResult(idx++, Double.NaN);
        } else {
          // let the ieee 754 standard handle NaNs when not infectious
          if (left > right) {
            writeResult(idx++, negate_or_not ? 0 : 1);
          } else {
            writeResult(idx++, negate_or_not ? 1 : 0);
          }
        }
      }
    }
  }
  
  /**
   * Implements the sum of two values. A NaN in both returns NaN.
   * Non-infectious NaN will just return the other operand.
   * <p>
   * Integer math is used unless a floating point is present in either
   * operand. getNote that values may overflow.
   * 
   * TODO - overflow for addition.
   * 
   * @param left_value The left operand.
   * @param right_value The right operand.
   * @param left_offset The offset into the left array.
   * @param right_offset The offset into the right array.
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
      for (int left_idx = left_offset;
          left_idx < left_value.value().end(); left_idx++) {
        long result = left[left_idx] + right[right_idx++];
        long_values[idx++] = negate_or_not ? -result : result;
      }
    } else {
      double_values = new double[left_value.value().end() - left_offset];
      for (int left_idx = left_offset;
          left_idx < left_value.value().end(); left_idx++) {
        final double left = left_is_int ? left_value.value().longArray()[left_idx] :
          left_value.value().doubleArray()[left_idx];
        final double right = right_is_int ? right_value.value().longArray()[right_idx++] :
          right_value.value().doubleArray()[right_idx++];
        final double result;
        if (infectious_nan) {
          result = left + right;
        } else if (Double.isNaN(left)) {
          result = right;
        } else if (Double.isNaN(right)) {
          result = left;
        } else {
          result = left + right;
        }
        double_values[idx++] = negate_or_not ? -result : result;
      }
    }
  }
  
  /**
   * Implements the difference of two values. A NaN in both returns NaN. 
   * Non-infectious NaN will treat the missing operand as 0.
   * <p>
   * Integer math is used unless a floating point is present in either
   * operand. getNote that values may overflow.
   * 
   * TODO - overflow for addition.
   * 
   * @param left_value The left operand.
   * @param right_value The right operand.
   * @param left_offset The offset into the left array.
   * @param right_offset The offset into the right array.
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
      for (int left_idx = left_offset;
          left_idx < left_value.value().end(); left_idx++) {
        long result = left[left_idx] - right[right_idx++];
        long_values[idx++] = negate_or_not ? -result : result;
      }
    } else {
      double_values = new double[left_value.value().end() - left_offset];
      for (int left_idx = left_offset;
          left_idx < left_value.value().end(); left_idx++) {
        final double left = left_is_int ? left_value.value().longArray()[left_idx] :
          left_value.value().doubleArray()[left_idx];
        final double right = right_is_int ? right_value.value().longArray()[right_idx++] :
          right_value.value().doubleArray()[right_idx++];
        final double result;
        if (infectious_nan) {
          result = left - right;
        } else if (Double.isNaN(left)) {
          result = -right;
        } else if (Double.isNaN(right)) {
          result = left;
        } else {
          result = left - right;
        }
        double_values[idx++] = negate_or_not ? -result : result;
      }
    }
  }
  
  /**
   * Implements the quotient of two values. 
   * <p>
   * <b>Note</b> With infectious NaN enabled, a divide-by-zero operation will 
   * return NaN, otherwise it will return 0. Without infection, a NaN in either
   * operand will return a 0.
   * <p>
   * The result is always a double.
   * 
   * @param left_value The left operand.
   * @param right_value The right operand.
   * @param left_offset The offset into the left array.
   * @param right_offset The offset into the right array.
   */
  void runDivide(final TimeSeriesValue<NumericArrayType> left_value, 
                 final TimeSeriesValue<NumericArrayType> right_value,
                 final int left_offset, final int right_offset) {
    int idx = 0;
    int right_idx = right_offset;
    double_values = new double[left_value.value().end() - left_offset];
    for (int left_idx = left_offset;
        left_idx < left_value.value().end(); left_idx++) {
      final double left = left_is_int ? left_value.value().longArray()[left_idx] :
        left_value.value().doubleArray()[left_idx];
      final double right = right_is_int ? right_value.value().longArray()[right_idx++] :
        right_value.value().doubleArray()[right_idx++];
      final double result;
      if (infectious_nan) {
        result = left / (right == 0 ? Double.NaN : right);
      } else if (right == 0) {
        result = 0;
      } else if (Math.abs(0.0 - right) <= EPSILON) {
        result = 0;
      }  else if (Double.isNaN(left) || Double.isNaN(right)) {
        result = 0;
      } else {
        result = left / (right == 0 ? Double.NaN : right);
      }
      
      writeResult(idx++, negate_or_not ? -result : result);
    }
  }
  
  /**
   * Implements the product of two values. Non-infectious NaN treats the 
   * missing value as a 0.
   * <p>
   * Integer math is used unless a floating point is present in either
   * operand. getNote that values may overflow.
   * 
   * TODO - overflow.
   * 
   * @param left_value The left operand.
   * @param right_value The right operand.
   * @param left_offset The offset into the left array.
   * @param right_offset The offset into the right array.
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
      for (int left_idx = left_offset;
          left_idx < left_value.value().end(); left_idx++) {
        final long result = left[left_idx] * right[right_idx++];
        long_values[idx++] = negate_or_not ? -result : result;
      }
    } else {
      double_values = new double[left_value.value().end() - left_offset];
      for (int left_idx = left_offset;
          left_idx < left_value.value().end(); left_idx++) {
        final double left = left_is_int ? left_value.value().longArray()[left_idx] :
          left_value.value().doubleArray()[left_idx];
        final double right = right_is_int ? right_value.value().longArray()[right_idx++] :
          right_value.value().doubleArray()[right_idx++];
        final double result;
        if (infectious_nan) {
          result = left * right;
        } else if (Double.isNaN(left) || Double.isNaN(right)) {
          result = 0;
        } else {
          result = left * right;
        }
        
        writeResult(idx++, negate_or_not ? -result : result);
      }
    }
  }
  
  /**
   * Implements the modulus of two values. 
   * <p>
   * <b>Note</b> With infectious NaN enabled, a divide-by-zero operation will 
   * return NaN, otherwise it will return 0. Without infection, a NaN in either
   * operand will return a 0.
   * <p>
   * Integer math is used unless a floating point is present in either
   * operand.
   * 
   * @param left_value The left operand.
   * @param right_value The right operand.
   * @param left_offset The offset into the left array.
   * @param right_offset The offset into the right array.
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
      for (int left_idx = left_offset;
          left_idx < left_value.value().end(); left_idx++) {
        if (right[right_idx] == 0) {
          writeResult(idx++, Double.NaN);
          right_idx++;
        } else {
          long result = left[left_idx] % right[right_idx++];
          long_values[idx++] = negate_or_not ? -result : result;
        }
      }
    } else {
      double_values = new double[left_value.value().end() - left_offset];
      for (int left_idx = left_offset;
          left_idx < left_value.value().end(); left_idx++) {
        final double left = left_is_int ? left_value.value().longArray()[left_idx] :
          left_value.value().doubleArray()[left_idx];
        final double right = right_is_int ? right_value.value().longArray()[right_idx++] :
          right_value.value().doubleArray()[right_idx++];
        final double result;
        if (infectious_nan) {
          result = left % (right == 0 ? Double.NaN : right);
        } else if (right == 0) {
          result = 0;
        } else if (Math.abs(0.0 - right) <= EPSILON) {
          result = 0;
        } else if (Double.isNaN(left) || Double.isNaN(right)) {
          result = 0;
        } else {
          result = left % (right == 0 ? Double.NaN : right);
        }
        
        writeResult(idx++, negate_or_not ? -result : result);
      }
    }
  }
  
  /**
   * Directs the result to the proper array. Hopefully it'll be inlined.
   * @param idx The index to write into.
   * @param result The result to store.
   */
  void writeResult(final int idx, final long result) {
    if (long_values != null) {
      long_values[idx] = result;
    } else {
      double_values[idx] = result;
    }
  }
  
  /**
   * Directs the result to the proper array, migrating to the double array if
   * appropriate. Hopefully it'll be inlined.
   * @param idx The index to write into.
   * @param result The result to store.
   */
  void writeResult(final int idx, final double result) {
    if (long_values != null) {
      double_values = new double[long_values.length];
      for (int i = 0; i < idx; i++) {
        double_values[i] = long_values[i];
      }
      long_values = null;
    }
    double_values[idx] = result;
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
        if (length > STATIC_ARRAY_LEN) {
          double_values = new double[length];
          Arrays.fill(double_values, Double.NaN);
        } else {
          double_values = NAN_ARRAY;
        }
      } else if (literal.isInteger()) {
        if (length > STATIC_ARRAY_LEN) {
          long_values = new long[length];
        } else {
          long_values = LITERAL_LONGS.get();
        }
        Arrays.fill(long_values, literal.longValue());
      } else {
        if (length > STATIC_ARRAY_LEN) {
          double_values = new double[length];
        } else {
          double_values = LITERAL_DOUBLES.get();
        }
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
    final double[] array;
    
    Substitute(final int length) {
      if (length <= NAN_ARRAY.length) {
        array = NAN_ARRAY;
      } else {
        array = new double[length];
        Arrays.fill(array, Double.NaN);
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
      return false;
    }

    @Override
    public long[] longArray() {
      return null;
    }

    @Override
    public double[] doubleArray() {
      return array;
    }
  }
}
