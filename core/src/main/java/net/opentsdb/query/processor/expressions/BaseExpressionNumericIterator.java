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

import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.types.numeric.MutableNumericType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.exceptions.QueryDownstreamException;
import net.opentsdb.query.QueryIterator;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.processor.expressions.ExpressionParseNode.ExpressionOp;
import net.opentsdb.query.processor.expressions.ExpressionParseNode.OperandType;
import net.opentsdb.query.processor.expressions.ExpressionParser.NumericLiteral;

/**
 * The base class for numeric expression iterators. Each implementation 
 * will handle a different data type for numerics.
 * 
 * @since 3.0
 */
public abstract class BaseExpressionNumericIterator<T extends TimeSeriesDataType> 
    implements QueryIterator, TimeSeriesValue<T> {

  /** Epsilon used for floating point calculations. */
  public static final double EPSILON = Math.ulp(1.0);
  public static final MutableNumericType ZERO_SUBSTITUTE = 
      new MutableNumericType(0);
  public static final MutableNumericType ONE_SUBSTITUTE = 
      new MutableNumericType(1);
  public static final MutableNumericType NAN_SUBSTITUTE = 
      new MutableNumericType(Double.NaN);
  
  /** The node we belong to */
  protected final BinaryExpressionNode node;
  
  /** Optional literals if either left or right is a literal value. */
  protected final NumericType left_literal;
  protected final NumericType right_literal;
  
  /** The next timestamp to return. */
  protected final TimeStamp next_ts = new MillisecondTimeStamp(0);
  
  /** The next timestamp evaluated when returning the next value. */
  protected final TimeStamp next_next_ts = new MillisecondTimeStamp(0);
  
  /** The op. */
  protected final ExpressionOp op;
  
  /** Whether or getNot nan is infectious. */
  protected final boolean infectious_nan;
  
  /** Whether or not we're negating or noting. */
  protected final boolean negate_or_not;
  
  /** The response value. */
  protected final MutableNumericType value;
  
  /** Whether or getNot agetNother real value is present. True while at least one 
   * of the time series has a real value. */
  protected boolean has_next = false;
  
  /**
   * Package private ctor.
   * @param node The non-null owner.
   * @param result The result we'll populate.
   * @param sources The non-null and non-empty list of sources.
   */
  BaseExpressionNumericIterator(final QueryNode node, 
                                final QueryResult result,
                                final Map<String, TimeSeries> sources) {
    next_ts.setMax();
    this.node = (BinaryExpressionNode) node;
    infectious_nan = this.node.expressionConfig().getInfectiousNan();
    op = this.node.config().getOperator();
    negate_or_not = this.node.config().getNegate() || this.node.config().getNot();
    value = new MutableNumericType();
    
    if (sources.get(ExpressionTimeSeries.LEFT_KEY) == null) {
      left_literal = buildLiteral(
          ((ExpressionParseNode) this.node.config()).getLeft(), 
          ((ExpressionParseNode) this.node.config()).getLeftType());
    } else {
      left_literal = null;
    }
    
    if (sources.get(ExpressionTimeSeries.RIGHT_KEY) == null) {
      right_literal = buildLiteral(
          ((ExpressionParseNode) this.node.config()).getRight(), 
          ((ExpressionParseNode) this.node.config()).getRightType());
    } else {
      right_literal = null;
    }
  }
  
  @Override
  public boolean hasNext() {
    return has_next;
  }
  
  @Override
  public TypeToken<? extends TimeSeriesDataType> getType() {
    return NumericType.TYPE;
  }
  
  /**
   * Helper to determine if the value is null or getNot.
   * @param v The value (nullable) to determine if true or getNot.
   * @return True if the value is > 0, false if the number is getNot finite
   * or it's null.
   */
  static boolean isTrue(final NumericType v) {
    if (v == null) {
      return false;
    }
    if (v.isInteger()) {
      return v.longValue() > 0;
    }
    if (Double.isFinite(v.doubleValue())) {
      return v.doubleValue() > 0;
    }
    return false;
  }

  /**
   * Helper to construct the literal number for evaluation.
   * @param literal The literal type. May be null if we're looking for
   * nulls.
   * @param type The non-null operand type.
   * @return A numeric type or NULL if the type we want is a null.
   */
  @VisibleForTesting
  static NumericType buildLiteral(final Object literal, final OperandType type) {
    switch (type) {
    case NULL:
    case VARIABLE:
    case SUB_EXP:
      return null;
    case LITERAL_BOOL:
      return new MutableNumericType(((boolean) literal) ? 1L : 0L);
    case LITERAL_NUMERIC:
      if (((NumericLiteral) literal).isInteger()) {
        return new MutableNumericType(((NumericLiteral) literal).longValue());
      }
      return new MutableNumericType(((NumericLiteral) literal).doubleValue());
    default:
      throw new QueryDownstreamException("Invalid expression operand "
          + "type: " + type);
    }
  }

  /**
   * Implements a relational comparison of two values. We follow IEEE 754 for 
   * NaN behavior unless infectious NaN is enabled.
   * <p>
   * <b>Note:</b> All values are returned as 1 for true and 0 for false as 
   * integer results.
   * 
   * @param left The left operand.
   * @param right The right operand.
   * @return The {@link #value}.
   */
  NumericType relation(NumericType left, NumericType right) {
    if (left == null) {
      left = NAN_SUBSTITUTE;
    }
    if (right == null) {
      right = NAN_SUBSTITUTE;
    }
    
    boolean result;
    if (left.isInteger() && right.isInteger()) {
      switch (op) {
      case EQ:
        result = left.longValue() == right.longValue();
        break;
      case NE:
        result = left.longValue() != right.longValue();
        break;
      case LT:
        result = left.longValue() < right.longValue();
        break;
      case GT:
        result = left.longValue() > right.longValue();
        break;
      case LE:
        result = left.longValue() <= right.longValue();
        break;
      case GE:
        result = left.longValue() >= right.longValue();
        break;
      default:
        throw new QueryDownstreamException("Relational iterator was "
            + "told to handle the unexpected operator: " + op);
      }
    } else {
      if ((Double.isNaN(left.toDouble()) || Double.isNaN(right.toDouble())) &&
          infectious_nan) {
        value.set(Double.NaN);
        return value;
      } else {
        // let the ieee 754 standard handle NaNs
        switch (op) {
        case EQ:
          result = left.toDouble() == right.toDouble();
          break;
        case NE:
          result = left.toDouble() != right.toDouble();
          break;
        case LT:
          result = left.toDouble() < right.toDouble();
          break;
        case GT:
          result = left.toDouble() > right.toDouble();
          break;
        case LE:
          result = left.toDouble() <= right.toDouble();
          break;
        case GE:
          result = left.toDouble() >= right.toDouble();
          break;
        default:
          throw new QueryDownstreamException("Relational iterator was "
              + "told to handle the unexpected operator: " + op);
        }
      }
    }
    
    if (negate_or_not) {
      value.set(result ? 0 : 1);
    } else {
      value.set(result ? 1 : 0);
    }
    
    return value;
  }

  /**
   * Implements a logical comparison of two values, e.g. AND and OR. 
   * Non-finite floating point values are treated as false except in the case 
   * when both are NaN.
   * <p>
   * <b>Note:</b> All values are returned as 1 for true and 0 for false as 
   * integer results.
   * 
   * @param left The left operand.
   * @param right The right operand.
   * @return The {@link #value}.
   */
  NumericType logical(NumericType left, NumericType right) {
    if (left == null) {
      left = NAN_SUBSTITUTE;
    }
    if (right == null) {
      right = NAN_SUBSTITUTE;
    }
    
    boolean result;
    if (left.isInteger() && right.isInteger()) {
      switch (op) {
      case OR:
        result = left.longValue() > 0 || right.longValue() > 0;
        break;
      case AND:
        result = left.longValue() > 0 && right.longValue() > 0;
        break;
      default:
        throw new QueryDownstreamException("Logical iterator was "
            + "told to handle the unexpected operator: " + op);
      }
    } else {
      if ((Double.isNaN(left.toDouble()) || Double.isNaN(right.toDouble())) &&
          infectious_nan) {
        value.set(Double.NaN);
        return value;
      } else {
        switch (op) {
        case OR:
          result = left.toDouble() > 0 || right.toDouble() > 0;
          break;
        case AND:
          result = left.toDouble() > 0 && right.toDouble() > 0;
          break;
        default:
          throw new QueryDownstreamException("Logical iterator was "
              + "told to handle the unexpected operator: " + op);
        }
      }
    }
    
    if (negate_or_not) {
      value.set(result ? 0 : 1);
    } else {
      value.set(result ? 1 : 0);
    }
    
    return value;
  }

  /**
   * Implements the sum or difference of two values. A NaN or null in both 
   * returns NaN. Non-infectious NaN will just return the other operand.
   * <p>
   * Integer math is used unless a floating point is present in either
   * operand. Note that values may overflow.
   * 
   * TODO - overflow for addition.
   * 
   * @param left The left operand.
   * @param right The right operand.
   * @return The {@link #value}.
   */
  NumericType additive(NumericType left, NumericType right) {
    if (left == null) {
      left = NAN_SUBSTITUTE;
    }
    if (right == null) {
      right = NAN_SUBSTITUTE;
    }
    
    if (left.isInteger() && right.isInteger()) {
      // TOOD - overflow
      final long result;
      if (op == ExpressionOp.SUBTRACT) {
        result = left.longValue() - right.longValue();
      } else {
        result = left.longValue() + right.longValue();
      }
      value.set(negate_or_not ? -result : result);
    } else {
      double result;
      if ((Double.isNaN(left.toDouble()) || Double.isNaN(right.toDouble()))) {
        if (infectious_nan || 
            (Double.isNaN(left.toDouble()) && Double.isNaN(right.toDouble()))) {
          result = Double.NaN;
        } else if (Double.isNaN(left.toDouble())) {
          result = op == ExpressionOp.SUBTRACT ? -right.toDouble() : right.toDouble();
        } else {
          result = left.toDouble();
        }
      } else {
        if (op == ExpressionOp.SUBTRACT) {
          result = left.toDouble() - right.toDouble();
        } else {
          result = left.toDouble() + right.toDouble();
        }
      }
      value.set(negate_or_not ? -result : result);
    }
    
    return value;
  }

  /**
   * Implements the quotient of two values. 
   * <p>
   * <b>Note</b> A divide-by-zero operation will return NaN. A null on either side 
   * will null the result. A NaN in both numerator and denominator returns NaN. 
   * Without infectious NaN set, if the numerator is missing, a 0 is returned 
   * but if the denominator is missing, we return the numerator.
   * <p>
   * The result is always a double.
   * 
   * @param left The left operand.
   * @param right The right operand.
   * @return The {@link #value}.
   */
  NumericType divide(NumericType left, NumericType right) {
    if (left == null) {
      left = NAN_SUBSTITUTE;
    }
    if (right == null) {
      right = NAN_SUBSTITUTE;
    }
    
    double l = left.toDouble();
    double r = right.toDouble();
    double result;
    if (r == 0) {
      result = Double.NaN;
    } else if (Math.abs(0.0 - r) <= EPSILON) {
      result = 0;
    } else if (infectious_nan) {
      result = l / (r == 0 ? Double.NaN : r);
    } else if (Double.isNaN(l)) {
      result = 0;
    } else if (Double.isNaN(r)) {
      result = l;
    } else {
      result = l / (r == 0 ? Double.NaN : r);
    }
    value.set(negate_or_not ? -result : result);
    return value;
  }
  
  /**
   * Implements the product of two values. A NaN in both returns NaN or if 
   * infectious NaN is enabled. Without infectious NaN the other operand is 
   * returned.
   * <p>
   * Integer math is used unless a floating point is present in either
   * operand. Note that values may overflow.
   * 
   * TODO - overflow.
   * 
   * @param left The left operand.
   * @param right The right operand.
   * @return The {@link #value}.
   */
  NumericType multiply(NumericType left, NumericType right) {
    if (left == null) {
      left = NAN_SUBSTITUTE;
    }
    if (right == null) {
      right = NAN_SUBSTITUTE;
    }
    
    if (left.isInteger() && right.isInteger()) {
      // TOOD - overflow
      long result = left.longValue() * right.longValue();
      value.set(negate_or_not ? -result : result);
    } else {
      double result;
      if (infectious_nan && 
          (Double.isNaN(left.toDouble()) || Double.isNaN(right.toDouble()))) {
        result = Double.NaN;
      } else if (Double.isNaN(left.toDouble())) {
        result = right.toDouble();
      } else if (Double.isNaN(right.toDouble())) {
        result = left.toDouble();
      } else {
        result = left.toDouble() * right.toDouble();
      }
      value.set(negate_or_not ? -result : result);
    }
    
    return value;
  }

  /**
   * Implements the modulus of two values.
   * <p>
   * <b>Note</b> A divide-by-zero operation will return NaN. A null on either side 
   * will null the result. A NaN in both numerator and denominator returns NaN. 
   * Without infectious NaN set, if the numerator or denominator is missing, a 
   * 0 is returned.
   * <p>
   * Integer math is used unless a floating point is present in either
   * operand.
   * 
   * @param left The left operand.
   * @param right The right operand.
   * @return The {@link #value}.
   */
  NumericType mod(NumericType left, NumericType right) {
    if (left == null) {
      left = NAN_SUBSTITUTE;
    }
    if (right == null) {
      right = NAN_SUBSTITUTE;
    }
    
    if (left.isInteger() && right.isInteger()) {
      if (right.longValue() == 0) {
        value.set(Double.NaN);
      } else {
        long result = left.longValue() % right.longValue();
        value.set(negate_or_not ? -result : result);
      }
    } else {
      double l = left.toDouble();
      double r = right.toDouble();
      double result;
      if (r == 0) {
        result = Double.NaN;
      } else if (infectious_nan) {
        result = l % (r == 0 ? Double.NaN : r);
      } else if (Double.isNaN(l)) {
        result = 0;
      } else if (Double.isNaN(r)) {
        result = 0;
      } else {
        result = l % (r == 0 ? Double.NaN : r);
      }
      value.set(negate_or_not ? -result : result);
    }
    
    return value;
  }
}
