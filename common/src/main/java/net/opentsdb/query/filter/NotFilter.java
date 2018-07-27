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

/**
 * Inverts the match on a filter.
 * 
 * @since 3.0
 */
public class NotFilter implements NestedQueryFilter {

  /** The filter to invert. */
  private final QueryFilter filter;
  
  /**
   * Protected ctor.
   * @param builder The non-null filter.
   */
  protected NotFilter(final Builder builder) {
    if (builder.filter == null) {
      throw new IllegalArgumentException("Filter cannot be null.");
    }
    filter = builder.filter;
  }
  
  @Override
  public QueryFilter getFilter() {
    return filter;
  }
  
  public static Builder newBuilder() {
    return new Builder();
  }
  
  public static class Builder {
    private QueryFilter filter;
    
    public Builder setFilter(final QueryFilter filter) {
      this.filter = filter;
      return this;
    }
    
    public NotFilter build() {
      return new NotFilter(this);
    }
  }
}
