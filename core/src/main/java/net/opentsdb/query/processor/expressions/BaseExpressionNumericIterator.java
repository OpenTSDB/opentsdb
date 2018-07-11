//This file is part of OpenTSDB.
//Copyright (C) 2018  The OpenTSDB Authors.
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

import com.google.common.annotations.VisibleForTesting;

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
  
  /** The node we belong to */
  protected final BinaryExpressionNode node;
  
  /** Optional literals if either left or right is a literal value. */
  protected final NumericType left_literal;
  protected final NumericType right_literal;
  
  /** The next timestamp to return. */
  protected final TimeStamp next_ts = new MillisecondTimeStamp(0);
  
  /** The next timestamp evaluated when returning the next value. */
  protected final TimeStamp next_next_ts = new MillisecondTimeStamp(0);
  
  /** Whether or not nan is infectious. */
  protected final boolean infectious_nan;
  
  protected final MutableNumericType value;
  
  /** Whether or not another real value is present. True while at least one 
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
    infectious_nan = ((ExpressionConfig) node.config()).getInfectiousNan();
    value = new MutableNumericType();
    
    if (sources.get(ExpressionTimeSeries.LEFT_KEY) == null) {
      left_literal = buildLiteral(
          this.node.expressionConfig().left(), 
          this.node.expressionConfig().leftType());
    } else {
      left_literal = null;
    }
    
    if (sources.get(ExpressionTimeSeries.RIGHT_KEY) == null) {
      right_literal = buildLiteral(
          this.node.expressionConfig().right(), 
          this.node.expressionConfig().rightType());
    } else {
      right_literal = null;
    }
  }
  
  @Override
  public boolean hasNext() {
    return has_next;
  }
  
  /**
   * Helper to determine if the value is null or not.
   * @param v The value (nullable) to determine if true or not.
   * @return True if the value is > 0, false if the number is not finite
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
   * Implements the modulus of two values. A null on either side will null 
   * the result. We follow IEEE 754 for NaN behavior.
   * <p>
   * <b>Note:</b> All values are returned as 1 for true and 0 for false as 
   * integer results.
   * 
   * @param left The left operand.
   * @param right The right operand.
   * @return The {@link #value} or null if either operand was null.
   */
  NumericType relation(final NumericType left, final NumericType right) {
    if (left == null || right == null) {
      return null;
    }
    
    if (left.isInteger() && right.isInteger()) {
      switch (node.expressionConfig().operator()) {
      case EQ:
        if (node.expressionConfig().not()) {
          value.set(left.longValue() == right.longValue() ? 0L : 1L);
        } else {
          value.set(left.longValue() == right.longValue() ? 1L : 0L);
        }
        break;
      case NE:
        if (node.expressionConfig().not()) {
          value.set(left.longValue() != right.longValue() ? 0L : 1L);
        } else {
          value.set(left.longValue() != right.longValue() ? 1L : 0L);
        }
        break;
      case LT:
        if (node.expressionConfig().not()) {
          value.set(left.longValue() < right.longValue() ? 0L : 1L);
        } else {
          value.set(left.longValue() < right.longValue() ? 1L : 0L);
        }
        break;
      case GT:
        if (node.expressionConfig().not()) {
          value.set(left.longValue() > right.longValue() ? 0L : 1L);
        } else {
          value.set(left.longValue() > right.longValue() ? 1L : 0L);
        }
        break;
      case LE:
        if (node.expressionConfig().not()) {
          value.set(left.longValue() <= right.longValue() ? 0L : 1L);
        } else {
          value.set(left.longValue() <= right.longValue() ? 1L : 0L);
        }
        break;
      case GE:
        if (node.expressionConfig().not()) {
          value.set(left.longValue() >= right.longValue() ? 0L : 1L); 
        } else {
          value.set(left.longValue() >= right.longValue() ? 1L : 0L);          
        }
        break;
      default:
        throw new QueryDownstreamException("Relational iterator was "
            + "told to handle the unexpected operator: " 
            + node.expressionConfig().operator());
      }
    } else {
      if ((Double.isNaN(left.toDouble()) || Double.isNaN(right.toDouble())) &&
          infectious_nan) {
        value.set(Double.NaN);
      } else {
        // let the ieee 754 standard handle NaNs
        switch (node.expressionConfig().operator()) {
        case EQ:
          if (node.expressionConfig().not()) {
            value.set(left.toDouble() == right.toDouble() ? 0L : 1L);
          } else {
            value.set(left.toDouble() == right.toDouble() ? 1L : 0L);
          }
          break;
        case NE:
          if (node.expressionConfig().not()) {
            value.set(left.toDouble() != right.toDouble() ? 0L : 1L);
          } else {
            value.set(left.toDouble() != right.toDouble() ? 1L : 0L);
          }
          break;
        case LT:
          if (node.expressionConfig().not()) {
            value.set(left.toDouble() < right.toDouble() ? 0L : 1L);
          } else {
            value.set(left.toDouble() < right.toDouble() ? 1L : 0L);
          }
          break;
        case GT:
          if (node.expressionConfig().not()) {
            value.set(left.toDouble() > right.toDouble() ? 0L : 1L);
          } else {
            value.set(left.toDouble() > right.toDouble() ? 1L : 0L);
          }
          break;
        case LE:
          if (node.expressionConfig().not()) {
            value.set(left.toDouble() <= right.toDouble() ? 0L : 1L);
          } else {
            value.set(left.toDouble() <= right.toDouble() ? 1L : 0L);
          }
          break;
        case GE:
          if (node.expressionConfig().not()) {
            value.set(left.toDouble() >= right.toDouble() ? 0L : 1L);
          } else {
            value.set(left.toDouble() >= right.toDouble() ? 1L : 0L);
          }
          break;
        default:
          throw new QueryDownstreamException("Relational iterator was "
              + "told to handle the unexpected operator: " 
              + node.expressionConfig().operator());
        }
      }
    }
    
    return value;
  }

  /**
   * Implements the modulus of two values. Nulls are treated as false. 
   * Non-finite floating point values are treated as false except in the 
   * case when both are NaN.
   * <p>
   * <b>Note:</b> All values are returned as 1 for true and 0 for false as 
   * integer results.
   * 
   * @param left The left operand.
   * @param right The right operand.
   * @return The {@link #value}.
   */
  NumericType logical(final NumericType left, final NumericType right) {
    switch (node.expressionConfig().operator()) {
    case OR:
      if (left == null) {
        if (node.expressionConfig().not()) {
          value.set(isTrue(right) ? 0 : 1);
        } else {
          value.set(isTrue(right) ? 1 : 0);
        }
      } else if (right == null) {
        if (node.expressionConfig().not()) {
          value.set(isTrue(left) ? 0 : 1);
        } else {
          value.set(isTrue(left) ? 1 : 0);
        }
      } else {
        if (!left.isInteger() && !right.isInteger() && 
            Double.isNaN(left.doubleValue()) && 
            Double.isNaN(right.doubleValue())) {
          value.set(node.expressionConfig().not() ? 0L : 1L);
        } else {
          if (node.expressionConfig().not()) {
            value.set(isTrue(left) || isTrue(right) ? 0 : 1);
          } else {
            value.set(isTrue(left) || isTrue(right) ? 1 : 0);
          }
        }
      }
      break;
    case AND:
      if (left == null || right == null) {
        value.set(node.expressionConfig().not() ? 1L : 0L);
      } else {
        if (!left.isInteger() && !right.isInteger() && 
            Double.isNaN(left.doubleValue()) && 
            Double.isNaN(right.doubleValue())) {
          value.set(node.expressionConfig().not() ? 0L : 1L);
        } else {
          if (node.expressionConfig().not()) {
            value.set(isTrue(left) && isTrue(right) ? 0 : 1);
          } else {
            value.set(isTrue(left) && isTrue(right) ? 1 : 0);
          }
        }
      }
      break;
    default:
      throw new QueryDownstreamException("Logical iterator was "
          + "told to handle the unexpected operator: " 
          + node.expressionConfig().operator());
    }
    
    return value;
  }

  /**
   * Implements the sum or difference of two values. A null on either side 
   * will null the result. A NaN in both returns NaN. Non-infectious NaN
   * will just return the other operand.
   * <p>
   * Integer math is used unless a floating point is present in either
   * operand. Note that values may overflow.
   * 
   * TODO - overflow for addition.
   * 
   * @param left The left operand.
   * @param right The right operand.
   * @return The {@link #value} or null if either operand was null.
   */
  NumericType additive(final NumericType left, final NumericType right) {
    if (left == null || right == null) {
      return null;
    }
    
    if (left.isInteger() && right.isInteger()) {
      // TOOD - overflow
      if (node.expressionConfig().operator() == ExpressionOp.SUBTRACT) {
        if (node.expressionConfig().negate()) {
          value.set(-(left.longValue() - right.longValue())); 
        } else {
          value.set(left.longValue() - right.longValue());  
        }
      } else {
        if (node.expressionConfig().negate()) {
          value.set(-(left.longValue() + right.longValue()));
        } else {
          value.set(left.longValue() + right.longValue());
        }
      }
    } else {
      if ((Double.isNaN(left.toDouble()) || Double.isNaN(right.toDouble()))) {
        if (infectious_nan || 
            (Double.isNaN(left.toDouble()) && Double.isNaN(right.toDouble()))) {
          value.set(Double.NaN);
        } else if (Double.isNaN(left.toDouble())) {
          value.set(node.expressionConfig().negate() ? 
              -right.toDouble() : right.toDouble());
        } else {
          value.set(node.expressionConfig().negate() ? 
              -left.toDouble() : left.toDouble());
        }
      } else {
        if (node.expressionConfig().operator() == ExpressionOp.SUBTRACT) {
          if (node.expressionConfig().negate()) {
            value.set(-(left.toDouble() - right.toDouble())); 
          } else {
            value.set(left.toDouble() - right.toDouble());
          }
        } else {
          if (node.expressionConfig().negate()) {
            value.set(-(left.toDouble() + right.toDouble())); 
          } else {
            value.set(left.toDouble() + right.toDouble());
          }
        }
      }
    }
    
    return value;
  }

  /**
   * Implements the quotient of two values. A null on either side will null 
   * the result. A NaN in both returns NaN. Non-infectious NaN will just 
   * return the other operand.
   * <p>
   * <b>NOTE</b> A divide-by-zero operation will return 0. Likewise a 
   * NaN in the numerator or denominator will return a 0 unless both are
   * NaN or infectious NaN is enabled.
   * <p>
   * Integer math is used unless a floating point is present in either
   * operand OR the quotient would result in a float.
   * 
   * @param left The left operand.
   * @param right The right operand.
   * @return The {@link #value} or null if either operand was null.
   */
  NumericType divide(final NumericType left, final NumericType right) {
    if (left == null || right == null) {
      return null;
    }
    
    if (left.isInteger() && right.isInteger() && 
        (right.longValue() != 0 && left.longValue() % right.longValue() == 0)) {
      if (right.longValue() == 0) {
        value.set(0L);
      } else {
        if (node.expressionConfig().negate()) {
          value.set(-(left.longValue() / right.longValue()));
        } else {
          value.set(left.longValue() / right.longValue());
        }
      }
    } else {
      if ((Double.isNaN(left.toDouble()) || Double.isNaN(right.toDouble()))) {
        if (infectious_nan || 
            (Double.isNaN(left.toDouble()) && Double.isNaN(right.toDouble()))) {
          value.set(Double.NaN);
        } else {
          value.set(0.0);
        }
      } else {
        if (Math.abs(0.0 - right.toDouble()) <= EPSILON) {
          value.set(0.0);
        } else {
          if (node.expressionConfig().negate()) {
            value.set(-(left.toDouble() / right.toDouble())); 
          } else {
            value.set(left.toDouble() / right.toDouble());
          }
        }
      }
    }
    
    return value;
  }
  
  /**
   * Implements the product of two values. A null on either side will null 
   * the result. A NaN in both returns NaN.
   * <p>
   * Note that if a NaN is present in the left or right (but not both) then
   * a 0 is returned.
   * <p>
   * Integer math is used unless a floating point is present in either
   * operand. Note that values may overflow.
   * 
   * TODO - overflow.
   * 
   * @param left The left operand.
   * @param right The right operand.
   * @return The {@link #value} or null if either operand was null.
   */
  NumericType multiply(final NumericType left, final NumericType right) {
    if (left == null || right == null) {
      return null;
    }
    
    if (left.isInteger() && right.isInteger()) {
      // TOOD - overflow
      if (node.expressionConfig().negate()) {
        value.set(-(left.longValue() * right.longValue())); 
      } else {
        value.set(left.longValue() * right.longValue());
      }
    } else {
      if ((Double.isNaN(left.toDouble()) || Double.isNaN(right.toDouble()))) {
        if (infectious_nan || 
            (Double.isNaN(left.toDouble()) && Double.isNaN(right.toDouble()))) {
          value.set(Double.NaN);
        } else {
          value.set(0.0);
        }
      } else {
        if (node.expressionConfig().negate()) {
          value.set(-(left.toDouble() * right.toDouble())); 
        } else {
          value.set(left.toDouble() * right.toDouble());
        }
      }
    }
    
    return value;
  }

  /**
   * Implements the modulus of two values. A null on either side will null 
   * the result. A NaN in both returns NaN.
   * <p>
   * Note that if a NaN is present in the left or right (but not both) then
   * a 0 is returned.
   * <p>
   * Integer math is used unless a floating point is present in either
   * operand.
   * 
   * @param left The left operand.
   * @param right The right operand.
   * @return The {@link #value} or null if either operand was null.
   */
  NumericType mod(final NumericType left, final NumericType right) {
    if (left == null || right == null) {
      return null;
    }
    
    if (left.isInteger() && right.isInteger()) {
      if (right.longValue() == 0) {
        value.set(0L);
      } else {
        if (node.expressionConfig().negate()) {
          value.set(-(left.longValue() % right.longValue())); 
        } else {
          value.set(left.longValue() % right.longValue());
        }
      }
    } else {
      if ((Double.isNaN(left.toDouble()) || Double.isNaN(right.toDouble()))) {
        if (infectious_nan || 
            (Double.isNaN(left.toDouble()) && Double.isNaN(right.toDouble()))) {
          value.set(Double.NaN);
        } else {
          value.set(0.0);
        }
      } else {
        if (node.expressionConfig().negate()) {
          value.set(-(left.toDouble() % right.toDouble())); 
        } else {
          value.set(left.toDouble() % right.toDouble());
        }
      }
    }
    
    return value;
  }
}
