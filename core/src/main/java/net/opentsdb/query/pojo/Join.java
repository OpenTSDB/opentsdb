// This file is part of OpenTSDB.
// Copyright (C) 2015-2017  The OpenTSDB Authors.
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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.common.base.Objects;

/**
 * Pojo builder class used for serdes of the join component of a query
 * @since 2.3
 */
@JsonDeserialize(builder = Join.Builder.class)
public class Join extends Validatable {
  /** An operator that determines how to sets of time series are merged via
   * expression. */
  public enum SetOperator {
    /** A union, meaning results from all sets will appear, using FillPolicies
     * for missing series */
    UNION("union"),
    
    /** Computes the intersection, returning results only for series that appear
     * in all sets */
    INTERSECTION("intersection"),
    
    /** Cross product. CAREFUL! We'll limit this possibility. */
    CROSS("cross"),
    ;
    
    /** The user-friendly name of this operator. */
    private final String name;
    
    /** @param the readable name of the operator */
    SetOperator(final String name) {
      this.name = name;
    }
    
    /** @return the readable name of the operator */
    @JsonValue
    public String getName() {
      return name;
    }
    
    /** 
     * Converts a string to lower case then looks up the operator
     * @param name The name to find an operator for
     * @return The operator if found.
     * @throws IllegalArgumentException if the operator wasn't found
     */
    @JsonCreator
    public static SetOperator fromString(final String name) {
      for (final SetOperator operator : SetOperator.values()) {
        if (operator.name.equalsIgnoreCase(name)) {
          return operator;
        }
      }
      throw new IllegalArgumentException("Unrecognized set operator: " + name);
    }
  }
  
  /** The set operator to use for joining sets */
  private SetOperator operator;
  
  /** Whether or not to use the original query tags instead of the resulting 
   * series tags when joining. */
  private boolean use_query_tags = false;
  
  /** Whether or not to use the aggregated tags in the results when joining. */
  private boolean include_agg_tags = true;
  
  /**
   * Default ctor
   * @param builder The builder to pull values from
   */
  public Join(final Builder builder) {
    operator = builder.operator;
    use_query_tags = builder.useQueryTags;
    include_agg_tags = builder.includeAggTags;
  }
  
  /** @return the set operator to use for joining sets */
  public SetOperator getOperator() {
    return operator;
  }
  
  /** @return whether or not to use the original query tags instead of the 
   * resulting series tags when joining. */
  public boolean getUseQueryTags() {
    return use_query_tags;
  }
  
  /** @return Whether or not to use the aggregated tags in the results 
   * when joining. */
  public boolean getIncludeAggTags() {
    return include_agg_tags;
  }
  
  /** @return A new builder for the joiner */
  public static Builder Builder() {
    return new Builder();
  }
  
  /** Validates the joiner
   * @throws IllegalArgumentException if one or more parameters were invalid
   */
  @Override
  public void validate() {
    if (operator == null) {
      throw new IllegalArgumentException("Missing join operator");
    }
  }
  
  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    final Join join = (Join) o;

    return Objects.equal(operator, join.operator)
        && Objects.equal(use_query_tags, join.use_query_tags)
    && Objects.equal(include_agg_tags, join.include_agg_tags);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(operator, use_query_tags, include_agg_tags);
  }
  
  /**
   * A builder for the downsampler component of a query
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "")
  public static final class Builder {
    @JsonProperty
    private SetOperator operator;
    @JsonProperty
    private boolean useQueryTags = false;
    @JsonProperty
    private boolean includeAggTags = true;
    
    public Builder setOperator(final SetOperator operator) {
      this.operator = operator;
      return this;
    }
    
    public Builder setUseQueryTags(final boolean use_query_tags) {
      this.useQueryTags = use_query_tags;
      return this;
    }
    
    public Builder setIncludeAggTags(final boolean include_agg_tags) {
      this.includeAggTags = include_agg_tags;
      return this;
    }
    
    public Join build() {
      return new Join(this);
    }
  }
}
