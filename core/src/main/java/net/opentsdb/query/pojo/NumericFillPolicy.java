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
package net.opentsdb.query.expression;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.common.base.Objects;

import net.opentsdb.core.FillPolicy;

/**
 * POJO for serdes of fill policies. It allows the user to pick either policies
 * with default values or a scalar that can be supplied with any number.
 * @since 2.3
 */
@JsonDeserialize(builder = NumericFillPolicy.Builder.class)
public class NumericFillPolicy {

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
  
  /** @returns a NumericFillPolicy builder */
  public static Builder Builder() {
    return new Builder();
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
    return Objects.hashCode(policy, value);
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
