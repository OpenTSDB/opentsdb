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

import java.util.List;

import com.google.common.collect.Lists;

/**
 * A chain of two or more filters to evaluate. Filters can be nested
 * and at least two must be present for the builder to succeed. If no
 * operator is supplied during building, we default to AND.
 * 
 * @since 3.0
 */
public class ChainFilter implements QueryFilter {
  
  /** The logical operator for the filters in this chain. */
  public static enum FilterOp {
    AND,
    OR
  }
  
  /** The non-null and non-empty list of filters. */
  protected final List<QueryFilter> filters;
  
  /** The operator. */
  protected final FilterOp op;
  
  /**
   * Protected ctor.
   * @param builder The non-null builder.
   */
  protected ChainFilter(final Builder builder) {
    if (builder.filters == null || builder.filters.size() < 2) {
      throw new IllegalArgumentException("Filters list cannot be null "
          + "or have fewer than 2 filters.");
    }
    filters = builder.filters;
    if (builder.op == null) {
      op = FilterOp.AND;
    } else {
      op = builder.op;
    }
  }
  
  /** @return The operator to use for logical comparison. */
  public FilterOp getOp() {
    return op;
  }
  
  /** @return The non-null list of filters. */
  public List<QueryFilter> getFilters() {
    return filters;
  }
  
  @Override
  public String toString() {
    return new StringBuilder()
        .append("{type=")
        .append(getClass().getSimpleName())
        .append(", operator=")
        .append(op)
        .append(", filters=")
        .append(filters)
        .append("}")
        .toString();
  }
  
  public static Builder newBuilder() {
    return new Builder();
  }
  
  public static class Builder {
    private List<QueryFilter> filters;
    private FilterOp op;
    
    public Builder setFilters(final List<QueryFilter> filters) {
      this.filters = filters;
      return this;
    }
    
    public Builder addFilter(final QueryFilter filter) {
      if (filters == null) {
        filters = Lists.newArrayList();
      }
      filters.add(filter);
      return this;
    }
    
    public Builder setOp(final FilterOp op) {
      this.op = op;
      return this;
    }
    
    /** @return The number of filters present so far. */
    public int filtersCount() {
      return filters == null ? 0 : filters.size();
    }
    
    /** @return The current list of filters, may be null. */
    public List<QueryFilter> filters() {
      return filters;
    }
    
    public ChainFilter build() {
      return new ChainFilter(this);
    }
  }
  
}
