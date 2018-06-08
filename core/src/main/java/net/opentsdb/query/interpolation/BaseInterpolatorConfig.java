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

import com.google.common.base.Strings;

import net.opentsdb.query.QueryInterpolatorConfig;

/**
 * Base class for interpolator configs. Validates that the type is set.
 * 
 * @since 3.0
 */
public abstract class BaseInterpolatorConfig implements QueryInterpolatorConfig {

  /** The ID, may be null. */
  protected final String id;
  
  /** The non-null data type ID. */
  protected final String type;
  
  /**
   * Default ctor.
   * @param builder A non-null builder
   * @throws IllegalArgumentException if the type was null or empty.
   */
  protected BaseInterpolatorConfig(final Builder builder) {
    if (Strings.isNullOrEmpty(builder.type)) {
      throw new IllegalArgumentException("Type cannot be null.");
    }
    id = builder.id;
    type = builder.type;
  }
  
  /** @return The ID. */
  @Override
  public String id() {
    return id;
  }
  
  /** @return The data type for this config. */
  @Override
  public String type() {
    return type;
  }
  
  public static abstract class Builder {
    protected String id;
    protected String type;
    
    public Builder setId(final String id) {
      this.id = id;
      return this;
    }
    
    public Builder setType(final String type) {
      this.type = type;
      return this;
    }
    
    public abstract QueryInterpolatorConfig build();
  }
}
