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
package net.opentsdb.query.filter;

import com.google.common.base.Strings;

/**
 * A default implementation of the NamedFilter.
 * 
 * @since 3.0
 */
public class DefaultNamedFilter implements NamedFilter {

  /** The non-null ID. */
  protected final String id;
  
  /** The non-null filter. */
  protected final QueryFilter filter;
  
  /**
   * Protected ctor.
   * @param builder A non-null builder.
   * @throws IllegalArgumentException if the id was null, empty or the 
   * filter was null.
   */
  protected DefaultNamedFilter(final Builder builder) {
    if (Strings.isNullOrEmpty(builder.id)) {
      throw new IllegalArgumentException("ID cannot be null.");
    }
    if (builder.filter == null) {
      throw new IllegalArgumentException("Filter cannot be null.");
    }
    id = builder.id;
    filter = builder.filter;
  }
  
  @Override
  public String id() {
    return id;
  }

  @Override
  public QueryFilter filter() {
    return filter;
  }

  public static Builder newBuilder() {
    return new Builder();
  }
  
  public static class Builder {
    private String id;
    private QueryFilter filter;
    
    public Builder setId(final String id) {
      this.id = id;
      return this;
    }
    
    public Builder setFilter(final QueryFilter filter) {
      this.filter = filter;
      return this;
    }
    
    public DefaultNamedFilter build() {
      return new DefaultNamedFilter(this);
    }
  }
}
