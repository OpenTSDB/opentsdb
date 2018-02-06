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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;
import com.google.common.hash.HashCode;

import net.opentsdb.core.Const;

/**
 * POJO for serdes of fill policies. It allows the user to pick either policies
 * with default values or a scalar that can be supplied with any number.
 * @since 2.3
 */
@JsonInclude(Include.NON_NULL)
@JsonDeserialize(builder = NumericFillPolicy.Builder.class)
public class NumericFillPolicy implements Comparable<NumericFillPolicy> {

  /** The fill policy to use. This is required */
  private FillPolicy policy;
  
  /** The value to store with the fill policy */
  private double value;

  /**
   * CTor to set the policy. Also calls {@link #validate()}
   * @param policy The policy to set.
   */
  public NumericFillPolicy(final FillPolicy policy) {
    this.policy = policy;
    validate();
  }
  
  /**
   * CTor to set the policy and value. Also calls {@link #validate()}
   * @param policy The name of the fill policy
   * @param value The value to use when filling
   * @throws IllegalArgumentException if the policy and value don't gel together
   */
  public NumericFillPolicy(final FillPolicy policy, final double value) {
    this.policy = policy;
    this.value = value;
    validate();
  }
  
  @Override
  public String toString() {
    return "policy=" + policy + ", value=" + value;
  }
  
  /** @return a NumericFillPolicy builder */
  public static Builder newBuilder() {
    return new Builder();
  }
  
  /** 
   * Clones a fill policy into a new builder.
   * @param fill_policy A non-null fill policy to pull values from. 
   * @return A builder populated with values from the given fill policy.
   * @throws IllegalArgumentException if the fill was null. 
   * @since 3.0
   */
  public static Builder newBuilder(final NumericFillPolicy fill_policy) {
    if (fill_policy == null) {
      throw new IllegalArgumentException("Fill policy cannot be null.");
    }
    return new Builder()
        .setPolicy(fill_policy.policy)
        .setValue(fill_policy.value);
  }
  
  /**
   * A builder class for deserialization via Jackson
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "")
  public static final class Builder {
    @JsonProperty
    private FillPolicy policy;
    @JsonProperty
    private double value;
    
    public Builder setPolicy(FillPolicy policy) {
      this.policy = policy;
      return this;
    }
    
    public Builder setValue(double value) {
      this.value = value;
      return this;
    }
    
    public NumericFillPolicy build() {
      return new NumericFillPolicy(policy, value);
    }
  }
  
  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof NumericFillPolicy)) {
      return false;
    }
    final NumericFillPolicy nfp = (NumericFillPolicy)obj;
    return Objects.equal(policy, nfp.policy) &&
           Objects.equal(value, nfp.value);
  }
  
  /** @return A HashCode object for deterministic, non-secure hashing */
  public HashCode buildHashCode() {
    return Const.HASH_FUNCTION().newHasher()
        .putString(policy.getName(), Const.ASCII_CHARSET)
        .putDouble(value)
        .hash();
  }
  
  @Override
  public int compareTo(final NumericFillPolicy o) {
    return ComparisonChain.start()
        .compare(policy.getName(), o.policy.getName())
        .compare(value, o.value)
        .result();
  }
  
  /** @return the fill policy */
  public FillPolicy getPolicy() {
    return policy;
  }

  /** @param policy the fill policy to use */
  public void setPolicy(final FillPolicy policy) {
    this.policy = policy;
  }

  /** @return the value to use when filling */
  public double getValue() {
    return value;
  }

  /** @param value the value to use when filling */
  public void setValue(final double value) {
    this.value = value;
  }
  
  /**
   * Makes sure the policy name and value are a suitable combination. If one
   * or the other is missing then we set the other with the proper value.
   * @throws IllegalArgumentException if the combination is bad
   */
  public void validate() {
    if (policy == null) {
      if (value == 0) {
        policy = FillPolicy.ZERO;
      } else if (Double.isNaN(value)) {
        policy = FillPolicy.NOT_A_NUMBER;
      } else {
        policy = FillPolicy.SCALAR;
      }
    } else {
      switch (policy) {
      case NONE:
      case NOT_A_NUMBER:
        if (value != 0 && !Double.isNaN(value)) {
          throw new IllegalArgumentException(
              "The value for NONE and NAN must be NaN");
        }
        value = Double.NaN;
        break;
      case ZERO:
        if (value != 0) {
          throw new IllegalArgumentException("The value for ZERO must be 0");
        }
        value = 0;
        break;
      case NULL:
        if (value != 0 && !Double.isNaN(value)) {
          throw new IllegalArgumentException("The value for NULL must be 0");
        }
        value = Double.NaN;
        break;
      case SCALAR: // it CAN be zero
        break;
      }
    }
  }
}
