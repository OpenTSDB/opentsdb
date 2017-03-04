// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.expressions;

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
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;

import net.opentsdb.query.pojo.Expression;
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
@JsonDeserialize(builder = JexlBinderProcessorConfig.Builder.class)
public class JexlBinderProcessorConfig implements 
    TimeSeriesProcessorConfig<JexlBinderProcessor>,
    Comparable<JexlBinderProcessorConfig> {
  
  /** The expression this processor will work off of. */
  private Expression expression;
  
  /**
   * Private CTor to construct from a builder.
   * @param builder A non-null builder.
   */
  protected JexlBinderProcessorConfig(final Builder builder) {
    expression = builder.expression; 
  }
  
  /** @return The expression to work with. */
  public Expression getExpression() {
    return expression;
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

    final JexlBinderProcessorConfig that = (JexlBinderProcessorConfig) o;

    return Objects.equal(that.expression, expression);
  }
  
  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }
  
  /** @return A HashCode object for deterministic, non-secure hashing */
  public HashCode buildHashCode() {
    final List<HashCode> hashes = Lists.newArrayListWithCapacity(1);
    if (expression != null) {
      hashes.add(expression.buildHashCode());
    }
    return Hashing.combineOrdered(hashes);
  }
  
  @Override
  public int compareTo(final JexlBinderProcessorConfig o) {
    return ComparisonChain.start()
        .compare(expression,  o.expression)
        .result();
  }
  
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "")
  public static class Builder implements 
      TimeSeriesProcessorConfigBuilder<JexlBinderProcessor> {
    @JsonProperty
    private Expression expression;
    
    public Builder setExpression(final Expression expression) {
      this.expression = expression;
      return this;
    }
    
    @Override
    public TimeSeriesProcessorConfig<JexlBinderProcessor> build() {
      if (expression == null) {
        throw new IllegalArgumentException("Expression cannot be null.");
      }
      expression.validate();
      return new JexlBinderProcessorConfig(this);
    }
  }
  
}
