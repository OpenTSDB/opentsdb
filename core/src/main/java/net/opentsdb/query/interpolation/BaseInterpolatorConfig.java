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
import com.google.common.base.Strings;

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
