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
package net.opentsdb.query.processor.expressions;

import com.google.common.base.Strings;
import com.google.common.hash.HashCode;

import net.opentsdb.query.BaseQueryNodeConfig;
import net.opentsdb.query.QueryNodeConfig;

/**
 * A node populated during parsing of a metric expression.
 * 
 * TODO - hashcodes/equals/compare.
 * 
 * @since 3.0
 */
public class ExpressionParseNode extends BaseQueryNodeConfig {
  
  /**
   * The type of value represented in the left or right operand.
   */
  static enum OperandType {
    VARIABLE,
    SUB_EXP,
    LITERAL_NUMERIC,
    LITERAL_STRING,
    LITERAL_BOOL,
    NULL
  }
  
  /**
   * The operation this expression will execute.
   */
  static enum ExpressionOp {
    OR(new String[] { "||", "OR" }),
    AND(new String[] { "&&", "AND" }),
    EQ(new String[] { "==" }),
    NE(new String[] { "!=" }),
    LT(new String[] { "<" }),
    GT(new String[] { ">" }),
    LE(new String[] { "<=" }),
    GE(new String[] { ">=" }),
    ADD(new String[] { "+" }),
    SUBTRACT(new String[] { "-" }),
    MULTIPLY(new String[] { "*" }),
    DIVIDE(new String[] { "/" }),
    MOD(new String[] { "%" });
    
    private final String[] symbols;
      
    /**
     * Default ctor.
     * @param symbols The non-null and non-empty list of symbols.
     */
    ExpressionOp(final String[] symbols) {
      this.symbols = symbols;
    }
    
    /**
     * Parses the symbol to determine the enum it belongs to.
     * @param symbol A non-null and non-empty symbol.
     * @return The symbol enum if found.
     * @throws IllegalArgumentException if the symbol was null, empty or
     * not found in the enum set.
     */
    static public ExpressionOp parse(String symbol) {
      symbol = symbol == null ? null : symbol.trim();
      if (Strings.isNullOrEmpty(symbol)) {
        throw new IllegalArgumentException("Symbol cannot be null or empty.");
      }
      for (int i = 0; i < values().length; i++) {
        final ExpressionOp op = values()[i];
        for (final String op_symbol : op.symbols) {
          if (op_symbol.equals(symbol)) {
            return op;
          }
        }
      }
      throw new IllegalArgumentException("Unrecognized symbol: " + symbol);
    }
  }
  
  /** ID shadow needed to allow for overrides. */
  private String id;
  
  /** The left operand. */
  private final Object left;
  
  /** The type of the left operand. */
  private final OperandType left_type;
  
  /** The right operand. */
  private final Object right;
  
  /** The type of the right operand. */
  private final OperandType right_type;
  
  /** The expression operator. */
  private final ExpressionOp op;
  
  /** Whether or not we're negating the output. */
  private boolean negate;
  
  /** Whether or not we're "not"ting the output. */
  private boolean not;
  
  /**
   * Protected ctor.
   * @param builder The non-null builder.
   */
  protected ExpressionParseNode(final Builder builder) {
    super(builder);
    this.id = super.getId();
    left = builder.left;
    left_type = builder.left_type;
    right = builder.right;
    right_type = builder.right_type;
    op = builder.op;
    negate = builder.negate;
    not = builder.not;
  }
  
  /** @return The left operand. */
  public Object left() {
    return left;
  }
  
  /** @return The type of the left operand. */
  public OperandType leftType() {
    return left_type;
  }
  
  /** @return The right operand. */
  public Object right() {
    return right;
  }
  
  /** @return The type of the right operand. */
  public OperandType rightType() {
    return right_type;
  }
  
  /** @return The operator. */
  public ExpressionOp operator() {
    return op;
  }
  
  /** @param negate Whether or not to negate the output. */
  public void setNegate(final boolean negate) {
    this.negate = negate;
  }
  
  /** @return Whether or not to negate the output. */
  public boolean negate() {
    return negate;
  }
  
  /** @param not Whether or not to "not" the output. */
  public void setNot(final boolean not) {
    this.not = not;
  }
  
  /** @return Whether or not to "not" the output. */
  public boolean not() {
    return not;
  }
  
  @Override
  public HashCode buildHashCode() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int compareTo(QueryNodeConfig o) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public boolean equals(Object o) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public int hashCode() {
    // TODO Auto-generated method stub
    return 0;
  }
  
  public String toString() {
    return new StringBuilder()
        .append("{id=")
        .append(id)
        .append(", left=")
        .append(left)
        .append(", leftType=")
        .append(left_type)
        .append(", right=")
        .append(right)
        .append(", rightType=")
        .append(right_type)
        .append(", op=")
        .append(op)
        .append(", negate=")
        .append(negate)
        .append(", not=")
        .append(not)
        .append("}")
        .toString();
  }
  
  @Override
  public String getId() {
    return id;
  }
  
  /**
   * Package private method to override the ID
   * @param id A non-null ID to set.
   */
  void overrideId(final String id) {
    this.id = id;
  }
  
  static Builder newBuilder() {
    return new Builder();
  }
  
  static class Builder extends BaseQueryNodeConfig.Builder {
    private Object left;
    private OperandType left_type;
    private Object right;
    private OperandType right_type;
    private ExpressionOp op;
    private boolean negate;
    private boolean not;
    
    public Builder setLeft(final Object left) {
      this.left = left;
      return this;
    }
    
    public Builder setLeftType(final OperandType left_type) {
      this.left_type = left_type;
      return this;
    }
    
    public Builder setRight(final Object right) {
      this.right = right;
      return this;
    }
    
    public Builder setRightType(final OperandType right_type) {
      this.right_type = right_type;
      return this;
    }
    
    public Builder setExpressionOp(final ExpressionOp op) {
      this.op = op;
      return this;
    }
    
    public Builder setNegate(final boolean negate) {
      this.negate = negate;
      return this;
    }
    
    public Builder setNot(final boolean not) {
      this.not = not;
      return this;
    }
    
    @Override
    public QueryNodeConfig build() {
      return new ExpressionParseNode(this);
    }
    
  }
}
