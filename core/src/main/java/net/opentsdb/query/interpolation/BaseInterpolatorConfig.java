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
package net.opentsdb.query.interpolation;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.hash.HashCode;
import net.opentsdb.core.Const;


/**
 * Base class for interpolator configs. If the type is null, we assume a default.
 * 
 * @since 3.0
 */
public abstract class BaseInterpolatorConfig implements QueryInterpolatorConfig {

  /** The non-null data type ID. */
  protected final String interpolator_type;
  
  /** The class name of the config for parsing. */
  protected final String data_type;
  
  /**
   * Default ctor.
   * @param builder A non-null builder
   */
  protected BaseInterpolatorConfig(final Builder builder) {
    if (Strings.isNullOrEmpty(builder.dataType)) {
      throw new IllegalArgumentException("Data type cannot be null "
          + "or empty.");
    }
    interpolator_type = builder.type;
    data_type = builder.dataType;
  }
  
  /** @return The ID. */
  @Override
  public String getType() {
    return interpolator_type;
  }



  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    final BaseInterpolatorConfig otherInterpolator = (BaseInterpolatorConfig) o;

    return Objects.equal(interpolator_type, otherInterpolator.getType())
            && Objects.equal(data_type, otherInterpolator.getDataType());

  }


  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }

  @Override
  /** @return A HashCode object for deterministic, non-secure hashing */
  public HashCode buildHashCode() {
    final HashCode hc = Const.HASH_FUNCTION().newHasher()
            .putString(Strings.nullToEmpty(interpolator_type), Const.UTF8_CHARSET)
            .putString(Strings.nullToEmpty(data_type), Const.UTF8_CHARSET)
            .hash();

    return hc;
  }
  
  /** @return The data type for this config. */
  @Override
  public String getDataType() {
    return data_type;
  }
    
  public static abstract class Builder {
    @JsonProperty
    protected String type;
    @JsonProperty
    protected String dataType;
    
    public Builder setType(final String type) {
      this.type = type;
      return this;
    }
    
    public Builder setDataType(final String data_type) {
      this.dataType = data_type;
      return this;
    }
    
    public abstract QueryInterpolatorConfig build();
  }
}
