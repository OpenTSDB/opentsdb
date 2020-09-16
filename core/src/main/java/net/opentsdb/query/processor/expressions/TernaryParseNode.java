// This file is part of OpenTSDB.
// Copyright (C) 2020  The OpenTSDB Authors.
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
import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;

import net.opentsdb.common.Const;
import net.opentsdb.query.QueryResultId;

/**
 * A Ternary node config populated during parsing of a metric expression.
 * 
 * TODO - hashcodes/equals/compare.
 * 
 * @since 3.0
 */
@JsonInclude(Include.NON_NULL)
@JsonDeserialize(builder = TernaryParseNode.Builder.class)
public class TernaryParseNode extends ExpressionParseNode {
  
  /** The condition. */
  protected Object condition;
  
  /** The type of the left operand. */
  protected final OperandType condition_type;
  
  /** Node IDs for linking results. */
  protected QueryResultId condition_id;
  
  /**
   * Protected ctor.
   * @param builder The non-null builder.
   */
  protected TernaryParseNode(final Builder builder) {
    super(builder);
    if (builder.expressionConfig == null) {
      throw new IllegalArgumentException("Missing parent expression config.");
    }
    condition = builder.condition;
    if (condition == null) {
      throw new RuntimeException("WTF?");
    }
    condition_type = builder.conditionType;
    condition_id = builder.conditionId;
  }
  
  /** @return The condition operand. */
  public Object getCondition() {
    return condition;
  }
  
  /** @return The type of the condition operand. */
  public OperandType getConditionType() {
    return condition_type;
  }
  
  /** @return The condition result source ID, may be null. */
  public QueryResultId getConditionId() {
    return condition_id;
  }
  
  @Override
  public HashCode buildHashCode() {
    // TODO Auto-generated method stub
    return Const.HASH_FUNCTION().newHasher()
        .putString(id, Const.UTF8_CHARSET)
        .hash();
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
    if (!(o instanceof TernaryParseNode)) {
      return false;
    }
    
    return id.equals(((TernaryParseNode) o).id);
  }

  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }
  
  public String toString() {
    final StringBuilder builder = stringBuilder();
    builder.setLength(builder.length() - 1);
    builder.append(", condition=")
           .append(condition)
           .append(", conditionType=")
           .append(condition_type)
           .append(", conditionId=")
           .append(condition_id)
           .append("}");
    return builder.toString();
  }
  
  public Builder toBuilder() {
    return (Builder) new Builder()
        .setCondition(condition)
        .setConditionType(condition_type)
        .setConditionId(condition_id)
        .setExpressionConfig(expression_config)
        .setExpressionOp(op)
        .setLeft(left)
        .setLeftType(left_type)
        .setRight(right)
        .setRightType(right_type)
        .setNegate(negate)
        .setNot(not)
        .setAs(as)
        .setLeftId(left_id)
        .setRightId(right_id)
        .setSources(Lists.newArrayList(sources))
        .setId(id);
  }
  
  public static Builder newBuilder() {
    return new Builder();
  }
  
  public static class Builder extends ExpressionParseNode.Builder {
    @JsonProperty
    private Object condition;
    @JsonProperty
    private OperandType conditionType;
    @JsonProperty
    private QueryResultId conditionId;
    
    Builder() {
      setType(TernaryNodeFactory.TYPE);
    }
    
    public Builder setCondition(final Object condition) {
      this.condition = condition;
      return this;
    }
    
    public Builder setConditionType(final OperandType conditionType) {
      this.conditionType = conditionType;
      return this;
    }
    
    public Object condition() {
      return condition;
    }
    
    public QueryResultId conditionId() {
      return conditionId;
    }
    
    public Builder setConditionId(final QueryResultId conditionId) {
      this.conditionId = conditionId;
      return this;
    }
    
    @Override
    public TernaryParseNode build() {
      return new TernaryParseNode(this);
    }
    
  }
}