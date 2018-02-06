// This file is part of OpenTSDB.
// Copyright (C) 2015-2017  The OpenTSDB Authors.
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
package net.opentsdb.query.pojo;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.Script;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;

import net.opentsdb.core.Const;
import net.opentsdb.query.pojo.Join.SetOperator;

/**
 * Pojo builder class used for serdes of the expression component of a query
 * @since 2.3
 */
@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonDeserialize(builder = Expression.Builder.class)
public class Expression extends Validatable implements Comparable<Expression> {
  /** Docs don't say whether this is thread safe or not. SOME methods are marked
   * as not thread safe, so I assume it's ok to instantiate one of these guys
   * and keep creating scripts from it.
   */
  public final static JexlEngine JEXL_ENGINE = new JexlEngine();
  
  /** An id for this expression for use in output selection or nested expressions */
  private String id;
  
  /** The raw expression as a string */
  private String expr;
  
  /** The joiner operator */
  private Join join;
  
  /** The fill policy to use if this output is included in a DS and nothing is present. */
  private NumericFillPolicy fill_policy;
  
  /** An optional map of variables to fill policies. */
  private Map<String, NumericFillPolicy> fill_policies;
  
  /** Set of unique variables used by this expression. */
  private Set<String> variables;
  
  /** The parsed expression via JEXL. */
  private Script parsed_expression;
  
  /**
   * Default ctor 
   * @param builder The builder to pull values from
   */
  protected Expression(final Builder builder) {
    id = builder.id;
    expr = builder.expr;
    join = builder.join;
    fill_policy = builder.fillPolicy;
    fill_policies = builder.fillPolicies;
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
  
  /** @return an optional list of variables to fill policies. May be null. */
  public Map<String, NumericFillPolicy> getFillPolicies() {
    return fill_policies == null ? null : 
        Collections.unmodifiableMap(fill_policies);
  }
  
  /** @return A new builder for the expression */
  public static Builder newBuilder() {
    return new Builder();
  }
  
  /**
   * Clones an expression into a new builder.
   * @param expression A non-null expression to pull values from
   * @return A new builder populated with values from the given expression.
   * @throws IllegalArgumentException if the expression was null.
   * @since 3.0
   */
  public static Builder newBuilder(final Expression expression) {
    if (expression == null) {
      throw new IllegalArgumentException("Expression cannot be null.");
    }
    final Builder builder = new Builder()
        .setId(expression.id)
        .setExpression(expression.expr);
    if (expression.fill_policy != null) {
      builder.setFillPolicy(expression.fill_policy);
    }
    if (expression.join != null) {
      builder.setJoin(expression.join);
    }
    if (expression.fill_policies != null) {
      builder.setFillPolicies(Maps.newHashMap(expression.fill_policies));
    }
    return builder;
  }

  /** Validates the expression
   * @throws IllegalArgumentException if one or more parameters were invalid
   */
  public void validate() {
    if (id == null || id.isEmpty()) {
      throw new IllegalArgumentException("missing or empty id");
    }
    TimeSeriesQuery.validateId(id);
    
    if (expr == null || expr.isEmpty()) {
      throw new IllegalArgumentException("missing or empty expr");
    }
    
    // parse it just to make sure we're happy and extract the variable names. 
    // Will throw JexlException
    parsed_expression = JEXL_ENGINE.createScript(expr);
    variables = new HashSet<String>();
    for (final List<String> exp_list : 
      JEXL_ENGINE.getVariables(parsed_expression)) {
      for (final String variable : exp_list) {
        variables.add(variable);
      }
    }
    
    // others are optional
    if (join == null) {
      join = Join.newBuilder().setOperator(SetOperator.UNION).build();
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
        && Objects.equal(fill_policy, expression.fill_policy)
        && Objects.equal(fill_policies, expression.fill_policies);
  }

  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }

  /** @return A HashCode object for deterministic, non-secure hashing */
  public HashCode buildHashCode() {
    final HashCode hc = Const.HASH_FUNCTION().newHasher()
        .putString(Strings.nullToEmpty(id), Const.UTF8_CHARSET)
        .putString(Strings.nullToEmpty(expr), Const.UTF8_CHARSET)
        .hash();
    final List<HashCode> hashes = Lists.newArrayListWithCapacity(3);
    hashes.add(hc);
    if (join != null) {
      hashes.add(join.buildHashCode());
    }
    if (fill_policy != null) {
      hashes.add(fill_policy.buildHashCode());
    }
    if (fill_policies != null) {
      // it's a tree map (via builder) so already sorted
      for (final Entry<String, NumericFillPolicy> entry : fill_policies.entrySet()) {
        hashes.add(Const.HASH_FUNCTION().newHasher()
            .putString(entry.getKey(), Const.UTF8_CHARSET).hash());
        hashes.add(entry.getValue().buildHashCode());
      }
    }
    return Hashing.combineOrdered(hashes);
  }

  @Override
  public int compareTo(final Expression o) {
    return ComparisonChain.start()
        .compare(id, o.id, Ordering.natural().nullsFirst())
        .compare(expr, o.expr, Ordering.natural().nullsFirst())
        .compare(join, o.join, Ordering.natural().nullsFirst())
        .compare(fill_policy, o.fill_policy, Ordering.natural().nullsFirst())
        .compare(fill_policies, o.fill_policies, FILL_CMP)
        .result();
  }

  /**
   * A builder for the expression component of a query
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
    @JsonProperty
    private Map<String, NumericFillPolicy> fillPolicies;
    
    public Builder setId(final String id) {
      TimeSeriesQuery.validateId(id);
      this.id = id;
      return this;
    }

    public Builder setExpression(final String expr) {
      this.expr = expr;
      return this;
    }

    public Builder setJoin(final Join join) {
      this.join = join;
      return this;
    }
    
    @JsonIgnore
    public Builder setJoin(final Join.Builder join) {
      this.join = join.build();
      return this;
    }
    
    public Builder setFillPolicy(final NumericFillPolicy fill_policy) {
      this.fillPolicy = fill_policy;
      return this;
    }
    
    @JsonIgnore
    public Builder setFillPolicy(final NumericFillPolicy.Builder fill_policy) {
      this.fillPolicy = fill_policy.build();
      return this;
    }
    
    public Builder setFillPolicies(final Map<String, NumericFillPolicy> fill_policies) {
      if (fill_policies != null) {
        fillPolicies = new TreeMap<String, NumericFillPolicy>(fill_policies);
      }
      return this;
    }
    
    public Builder addFillPolicy(final String variable, 
        final NumericFillPolicy fill_policy) {
      if (fillPolicies == null) {
        fillPolicies = Maps.newHashMapWithExpectedSize(1);
      }
      fillPolicies.put(variable, fill_policy);
      return this;
    }
    
    public Builder addFillPolicy(final String variable, 
        final NumericFillPolicy.Builder fill_policy) {
      if (fillPolicies == null) {
        fillPolicies = Maps.newHashMapWithExpectedSize(1);
      }
      fillPolicies.put(variable, fill_policy.build());
      return this;
    }
    
    public Expression build() {
      return new Expression(this);
    }
  }
  
  /** Little helper for comparing the variable to fills map. */
  private static FillPoliciesComparator FILL_CMP = new FillPoliciesComparator();
  
  /**
   * Little helper for comparing the variable to fills map.
   */
  private static class FillPoliciesComparator implements 
    Comparator<Map<String, NumericFillPolicy>> {

    @Override
    public int compare(final Map<String, NumericFillPolicy> a,
        final Map<String, NumericFillPolicy> b) {
      if (a == b || a == null && b == null) {
        return 0;
      }
      if (a == null && b != null) {
        return -1;
      }
      if (b == null && a != null) {
        return 1;
      }
      if (a.size() > b.size()) {
        return -1;
      }
      if (b.size() > a.size()) {
        return 1;
      }
      for (final Entry<String, NumericFillPolicy> entry : a.entrySet()) {
        final NumericFillPolicy b_value = b.get(entry.getKey());
        if (b_value == null && entry.getValue() != null) {
          return 1;
        }
        final int cmp = entry.getValue().compareTo(b_value);
        if (cmp != 0) {
          return cmp;
        }
      }
      return 0;
    }
    
  }
}
