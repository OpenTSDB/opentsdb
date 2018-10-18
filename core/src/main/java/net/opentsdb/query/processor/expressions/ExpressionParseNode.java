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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;

import net.opentsdb.common.Const;
import net.opentsdb.query.BaseQueryNodeConfig;
import net.opentsdb.query.QueryNodeConfig;

/**
 * A node populated during parsing of a metric expression.
 * 
 * TODO - hashcodes/equals/compare.
 * 
 * @since 3.0
 */
@JsonInclude(Include.NON_NULL)
@JsonDeserialize(builder = ExpressionParseNode.Builder.class)
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
  
  /** The output metric name. Defaults to the ID. */
  private String as;
  
  /** The left operand. */
  private Object left;
  
  /** The type of the left operand. */
  private final OperandType left_type;
  
  /** The right operand. */
  private Object right;
  
  /** The type of the right operand. */
  private final OperandType right_type;
  
  /** The expression operator. */
  private final ExpressionOp op;
  
  /** Whether or not we're negating the output. */
  private boolean negate;
  
  /** Whether or not we're "not"ting the output. */
  private boolean not;
  
  /** A link to the original expression config. */
  private final ExpressionConfig expression_config;
  
  /**
   * Protected ctor.
   * @param builder The non-null builder.
   */
  protected ExpressionParseNode(final Builder builder) {
    super(builder);
    if (builder.expressionConfig == null) {
      throw new IllegalArgumentException("Missing parent expression config.");
    }
    this.id = super.getId();
    left = builder.left;
    left_type = builder.leftType;
    right = builder.right;
    right_type = builder.rightType;
    op = builder.op;
    negate = builder.negate;
    not = builder.not;
    as = id;
    expression_config = builder.expressionConfig;
  }
  
  /** @return The name to use for the metric. Defaults to the ID. */
  public String getAs() {
    return as;
  }
  
  /** @return The left operand. */
  public Object getLeft() {
    return left;
  }
  
  /** @return The type of the left operand. */
  public OperandType getLeftType() {
    return left_type;
  }
  
  /** @return The right operand. */
  public Object getRight() {
    return right;
  }
  
  /** @return The type of the right operand. */
  public OperandType getRightType() {
    return right_type;
  }
  
  /** @return The operator. */
  public ExpressionOp getOperator() {
    return op;
  }
  
  /** @param negate Whether or not to negate the output. */
  public void setNegate(final boolean negate) {
    this.negate = negate;
  }
  
  /** @return Whether or not to negate the output. */
  public boolean getNegate() {
    return negate;
  }
  
  /** @param not Whether or not to "not" the output. */
  public void setNot(final boolean not) {
    this.not = not;
  }
  
  /** @return Whether or not to "not" the output. */
  public boolean getNot() {
    return not;
  }
  
  /** @param id The new id to set. */
  public void setLeft(final String id) {
    left = id;
  }
  
  /** @param id The new id to set. */
  public void setRight(final String id) {
    right = id;
  }
  
  /** @return The original expressionConfig. */
  public ExpressionConfig getExpressionConfig() {
    return expression_config;
  }
  
  @Override
  public boolean pushDown() {
    // TODO Auto-generated method stub
    return false;
  }
  
  @Override
  public boolean joins() {
    return true;
  }
  
  @Override
  public HashCode buildHashCode() {
    // TODO Auto-generated method stub
    return Const.HASH_FUNCTION().newHasher()
        .putString(id, Const.UTF8_CHARSET)
        .hash();
  }

  @Override
  public int compareTo(QueryNodeConfig o) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public boolean equals(Object o) {
    // TODO Auto-generated method stub
    if (o == null) {
      return false;
    }
    if (o == this) {
      return true;
    }
    if (!(o instanceof ExpressionParseNode)) {
      return false;
    }
    
    return id.equals(((ExpressionParseNode) o).id);
  }

  @Override
  public int hashCode() {
    return buildHashCode().asInt();
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
  
  void addSource(final String source) {
    // NOTE: since it could be set to Collections.emptyList() we need 
    // to store an actual list.
    if (sources == null || sources.isEmpty()) {
      sources = Lists.newArrayListWithExpectedSize(2);
    }
    sources.add(source);
  }
  
  /**
   * Package private method to override the ID
   * @param id A non-null ID to set.
   */
  void overrideId(final String id) {
    this.id = id;
  }
  
  /**
   * Package private method to override the as string.
   * @param as The non-null as string to set for the new metric.
   */
  void overrideAs(final String as) {
    this.as = as;
  }
  
  static Builder newBuilder() {
    return new Builder();
  }
  
  static class Builder extends BaseQueryNodeConfig.Builder {
    @JsonProperty
    private Object left;
    @JsonProperty
    private OperandType leftType;
    @JsonProperty
    private Object right;
    @JsonProperty
    private OperandType rightType;
    @JsonProperty
    private ExpressionOp op;
    @JsonProperty
    private boolean negate;
    @JsonProperty
    private boolean not;
    @JsonProperty
    private ExpressionConfig expressionConfig;
    
    Builder() {
      setType(BinaryExpressionNodeFactory.TYPE);
    }
    
    public Builder setLeft(final Object left) {
      this.left = left;
      return this;
    }
    
    public Builder setLeftType(final OperandType left_type) {
      this.leftType = left_type;
      return this;
    }
    
    public Builder setRight(final Object right) {
      this.right = right;
      return this;
    }
    
    public Builder setRightType(final OperandType right_type) {
      this.rightType = right_type;
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
    
    public Builder setExpressionConfig(final ExpressionConfig expression_config) {
      this.expressionConfig = expression_config;
      return this;
    }
    
    @Override
    public QueryNodeConfig build() {
      return new ExpressionParseNode(this);
    }
    
  }
}
