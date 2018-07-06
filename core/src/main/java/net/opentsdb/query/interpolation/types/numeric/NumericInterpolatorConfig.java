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
package net.opentsdb.query.interpolation.types.numeric;

import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hasher;

import net.opentsdb.core.Const;
import net.opentsdb.data.types.numeric.BaseNumericFillPolicy;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryFillPolicy;
import net.opentsdb.query.interpolation.BaseInterpolatorConfig;
import net.opentsdb.query.interpolation.QueryInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;

/**
 * A simple config for the base {@link NumericInterpolator}. Stores the real
 * fill policy.
 * 
 * @since 3.0
 */
@JsonInclude(Include.NON_NULL)
@JsonDeserialize(builder = NumericInterpolatorConfig.Builder.class)
public class NumericInterpolatorConfig extends BaseInterpolatorConfig {

  /** The numeric fill policy. */
  protected final FillPolicy fillPolicy;
  
  /** The real value fill policy. */
  protected final FillWithRealPolicy realFillPolicy;
  
  /**
   * Protected ctor for use with the builder.
   * @param builder A non-null builder.
   * @throws IllegalArgumentException if the fill policy was null or empty.
   */
  NumericInterpolatorConfig(final Builder builder) {
    super(builder);
    if (builder.fillPolicy == null) {
      throw new IllegalArgumentException("Fill policy cannot be null.");
    }
    if (builder.realFillPolicy == null) {
      throw new IllegalArgumentException("Real fill policy cannot be null.");
    }
    fillPolicy = builder.fillPolicy;
    realFillPolicy = builder.realFillPolicy;
  }
  
  /** @return The numeric fill policy. */
  public FillPolicy fillPolicy() {
    return fillPolicy;
  }
  
  /** @return The real fill policy. */
  public FillWithRealPolicy realFillPolicy() {
    return realFillPolicy;
  }
  
  /** @return The base numeric fill using the {@link #fillPolicy()}. */
  public QueryFillPolicy<NumericType> queryFill() {
    return new BaseNumericFillPolicy(this);
  }
  
  @Override
  public HashCode buildHashCode() {
    final Hasher hasher = Const.HASH_FUNCTION().newHasher()
        .putString(id != null ? id : "null", Const.UTF8_CHARSET)
        .putString(type, Const.ASCII_CHARSET)
        .putString(config_type != null ? config_type : "null", Const.ASCII_CHARSET)
        .putInt(fillPolicy.ordinal())
        .putInt(realFillPolicy.ordinal());
    return hasher.hash();
  }
  
  @Override
  public int compareTo(final QueryInterpolatorConfig o) {
    if (o == null) {
      return 1;
    }
    if (o == this) {
      return 0;
    }
    if (!(o instanceof NumericInterpolatorConfig)) {
      return 1;
    }
    
    return ComparisonChain.start()
        .compare(id, ((NumericInterpolatorConfig) o).id,
            Ordering.natural().nullsFirst())
        .compare(type, ((NumericInterpolatorConfig) o).type)
        .compare(config_type, ((NumericInterpolatorConfig) o).config_type,
            Ordering.natural().nullsFirst())
        .compare(fillPolicy, ((NumericInterpolatorConfig) o).fillPolicy)
        .compare(realFillPolicy, ((NumericInterpolatorConfig) o).realFillPolicy)
        .result();
  }

  @Override
  public boolean equals(final Object o) {
    if (o == null) {
      return false;
    }
    if (o == this) {
      return true;
    }
    if (!(o instanceof NumericInterpolatorConfig)) {
      return false;
    }
    
    final NumericInterpolatorConfig other = (NumericInterpolatorConfig) o;
    return Objects.equals(id, other.id) &&
           Objects.equals(type, other.type) &&
           Objects.equals(config_type, other.config_type) && 
           Objects.equals(fillPolicy, other.fillPolicy) &&
           Objects.equals(realFillPolicy, other.realFillPolicy);
  }

  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }
  
  @Override
  public String toString() {
    return new StringBuilder()
        .append("{id=").append(id)
        .append(", type=").append(type)
        .append(", configType=").append(config_type)
        .append(", fill=").append(fillPolicy)
        .append(", realFill=").append(realFillPolicy)
        .append("}")
        .toString();
  }
  
  /** @return A new builder for the config. */
  public static Builder newBuilder() {
    return new Builder();
  }
  
  /**
   * A builder class for the config.
   */
  public static class Builder extends BaseInterpolatorConfig.Builder {
    @JsonProperty
    private FillPolicy fillPolicy;
    @JsonProperty
    private FillWithRealPolicy realFillPolicy;
    
    /**
     * @param fill_policy A non-null numeric fill policy.
     * @return The builder.
     */
    public Builder setFillPolicy(final FillPolicy fill_policy) {
      this.fillPolicy = fill_policy;
      return this;
    }
    
    /**
     * @param real_fill A non-null real fill policy.
     * @return The builder.
     */
    public Builder setRealFillPolicy(final FillWithRealPolicy real_fill) {
      this.realFillPolicy = real_fill;
      return this;
    }
    
    /** @return An instantiated interpolator config. */
    public NumericInterpolatorConfig build() {
      return new NumericInterpolatorConfig(this);
    }
  }

}
