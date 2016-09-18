// This file is part of OpenTSDB.
// Copyright (C) 2015  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.query.pojo;

import net.opentsdb.query.expression.ExpressionIterator;
import net.opentsdb.query.expression.NumericFillPolicy;
import net.opentsdb.query.expression.VariableIterator.SetOperator;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.jexl2.Script;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.common.base.Objects;

/**
 * Pojo builder class used for serdes of the expression component of a query
 * @since 2.3
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonDeserialize(builder = Expression.Builder.class)
public class Expression extends Validatable {
  /** An id for this expression for use in output selection or nested expressions */
  private String id;
  
  /** The raw expression as a string */
  private String expr;
  
  /** The joiner operator */
  private Join join;
  
  /** The fill policy to use for ? */
  private NumericFillPolicy fill_policy;
  
  /** Set of unique variables used by this expression. */
  private Set<String> variables;
  
  /** The parsed expression via JEXL. */
  private Script parsed_expression;
  
  /**
   * Default ctor 
   * @param builder The builder to pull values from
   */
  protected Expression(Builder builder) {
    id = builder.id;
    expr = builder.expr;
    join = builder.join;
    fill_policy = builder.fillPolicy;
  }
  
  /** @return the id for this expression for use in output selection or 
   * nested expressions */
  public String getId() {
    return id;
  }

  /** @return the raw expression as a string */
  public String getExpr() {
    return expr;
  }

  /** @return he joiner operator */
  public Join getJoin() {
    return join;
  }
  
  /** @return the fill policy to use for ? */
  public NumericFillPolicy getFillPolicy() {
    return fill_policy;
  }
  
  /** @return A new builder for the expression */
  public static Builder Builder() {
    return new Builder();
  }

  /** Validates the expression
   * @throws IllegalArgumentException if one or more parameters were invalid
   */
  public void validate() {
    if (id == null || id.isEmpty()) {
      throw new IllegalArgumentException("missing or empty id");
    }
    Query.validateId(id);
    
    if (expr == null || expr.isEmpty()) {
      throw new IllegalArgumentException("missing or empty expr");
    }
    
    // parse it just to make sure we're happy and extract the variable names. 
    // Will throw JexlException
    parsed_expression = ExpressionIterator.JEXL_ENGINE.createScript(expr);
    variables = new HashSet<String>();
    for (final List<String> exp_list : 
      ExpressionIterator.JEXL_ENGINE.getVariables(parsed_expression)) {
      for (final String variable : exp_list) {
        variables.add(variable);
      }
    }
    
    // others are optional
    if (join == null) {
      join = Join.Builder().setOperator(SetOperator.UNION).build();
    }
  }

  /** @return The parsed expression. May be null if {@link validate} has not 
   * been called yet. */
  @JsonIgnore
  public Script getParsedExpression() {
    return parsed_expression;
  }
  
  /** @return A set of unique variables for the expression. May be null if 
   * {@link validate} has not been called yet. */
  @JsonIgnore
  public Set<String> getVariables() {
    return variables;
  }
  
  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    final Expression expression = (Expression) o;

    return Objects.equal(id, expression.id)
        && Objects.equal(expr, expression.expr)
        && Objects.equal(join, expression.join)
        && Objects.equal(fill_policy, expression.fill_policy);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(id, expr, join, fill_policy);
  }

  /**
   * A builder for the downsampler component of a query
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "")
  public static final class Builder {
    @JsonProperty
    private String id;
    @JsonProperty
    private String expr;
    @JsonProperty
    private Join join;
    @JsonProperty
    private NumericFillPolicy fillPolicy;
    
    public Builder setId(String id) {
      Query.validateId(id);
      this.id = id;
      return this;
    }

    public Builder setExpression(String expr) {
      this.expr = expr;
      return this;
    }

    public Builder setJoin(Join join) {
      this.join = join;
      return this;
    }
    
    public Builder setFillPolicy(NumericFillPolicy fill_policy) {
      this.fillPolicy = fill_policy;
      return this;
    }
    
    public Expression build() {
      return new Expression(this);
    }
  }
}
