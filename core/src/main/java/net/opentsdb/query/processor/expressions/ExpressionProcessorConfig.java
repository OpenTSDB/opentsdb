// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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

import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;

import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.query.pojo.Expression;
import net.opentsdb.query.pojo.Filter;
import net.opentsdb.query.processor.TimeSeriesProcessorConfig;
import net.opentsdb.query.processor.TimeSeriesProcessorConfigBuilder;

/**
 * Configuration class for the Jexl Expression processor.
 * Note that it will call Validate on the expression passed in as the processor
 * requires the variables.
 * 
 * @since 3.0
 */
@JsonInclude(Include.NON_NULL)
@JsonDeserialize(builder = ExpressionProcessorConfig.Builder.class)
public class ExpressionProcessorConfig implements 
    TimeSeriesProcessorConfig<JexlBinderProcessor>,
    Comparable<ExpressionProcessorConfig> {
  
  /** The expression this processor will work off of. */
  private Expression expression;
  
  /** Optional Filters pertinent to the expression join. */
  private List<Filter> filters;

  /** Optional list of tag keys to join on. 
   * NOTE: Do not account for this in equals, compare or hash code. Comes from 
   * Join and Filter. */
  private List<String> tag_keys;
  
  /**
   * Private CTor to construct from a builder.
   * @param builder A non-null builder.
   */
  protected ExpressionProcessorConfig(final Builder builder) {
    expression = builder.expression; 
    
    // TODO - proper encoding
    if ((expression.getJoin().getTags() != null && 
        !expression.getJoin().getTags().isEmpty()) || 
        expression.getJoin().getUseQueryTags()) {
      tag_keys = Lists.newArrayList();
      if (expression.getJoin().getTags() != null && 
          !expression.getJoin().getTags().isEmpty()) {
        tag_keys.addAll(expression.getJoin().getTags());
      } else {
        if (filters != null) {
          for (final Filter f : filters) {
            for (final TagVFilter filter : f.getTags()) {
              tag_keys.add(filter.getTagk());
            }
          }
        }
      }
      Collections.sort(tag_keys);
    }
  }
  
  /** @return The expression to work with. */
  public Expression getExpression() {
    return expression;
  }
  
  /** @return Optional filters to use if the join requires them. */
  public List<Filter> getFilters() {
    return filters;
  }
  
  /** @return An optional list of tag keys converted to byte arrays. */
  public List<String> getTagKeys() {
    return tag_keys;
  }
  
  /** @return A new builder for the expression config. */
  public static Builder newBuilder() {
    return new Builder();
  }
  
  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    final ExpressionProcessorConfig that = (ExpressionProcessorConfig) o;

    return Objects.equal(that.expression, expression) && 
           Objects.equal(that.filters, filters);
  }
  
  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }
  
  /** @return A HashCode object for deterministic, non-secure hashing */
  public HashCode buildHashCode() {
    final List<HashCode> hashes = Lists.newArrayListWithCapacity(2);
    if (expression != null) {
      hashes.add(expression.buildHashCode());
    }
    if (filters != null) {
      for (final Filter filter : filters) {
        hashes.add(filter.buildHashCode());
      }
    }
    return Hashing.combineOrdered(hashes);
  }
  
  @Override
  public int compareTo(final ExpressionProcessorConfig o) {
    return ComparisonChain.start()
        .compare(expression,  o.expression)
        .compare(filters, o.filters, 
            Ordering.<Filter>natural().lexicographical().nullsFirst())
        .result();
  }
  
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "")
  public static class Builder implements 
      TimeSeriesProcessorConfigBuilder<JexlBinderProcessor> {
    @JsonProperty
    private Expression expression;
    @JsonProperty
    private List<Filter> filters;
    
    public Builder setExpression(final Expression expression) {
      this.expression = expression;
      return this;
    }
    
    public Builder setFilters(final List<Filter> filters) {
      this.filters = filters;
      return this;
    }
    
    @Override
    public TimeSeriesProcessorConfig<JexlBinderProcessor> build() {
      if (expression == null) {
        throw new IllegalArgumentException("Expression cannot be null.");
      }
      expression.validate();
      if (expression.getJoin() != null) {
        if (expression.getJoin().getUseQueryTags() && 
            (filters == null || filters.isEmpty())) {
          throw new IllegalArgumentException("Filters cannot be null when "
              + "useQueryTags is true in the join config.");
        }
      }
      return new ExpressionProcessorConfig(this);
    }
  }
  
}
