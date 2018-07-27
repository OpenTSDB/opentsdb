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
package net.opentsdb.storage.schemas.tsdb1x;

import java.util.List;

import net.opentsdb.query.filter.QueryFilter;

/**
 * A resolved chain filter. Look at the original filter for the operand.
 * 
 * @since 3.0
 */
public class ResolvedChainFilter implements ResolvedQueryFilter {

  /** The non-null original filter. */
  private final QueryFilter filter;
  
  /** The non-null and non-empty list of resolved filters in the same 
   * order as those in the original filter. */
  private final List<ResolvedQueryFilter> resolved;
  
  /**
   * Default ctor.
   * @param filter The non-null original filter.
   * @param resolved The non-null and non-empty list of resolved filters.
   */
  public ResolvedChainFilter(final QueryFilter filter, 
                             final List<ResolvedQueryFilter> resolved) {
    this.filter = filter;
    this.resolved = resolved;
  }
  
  @Override
  public QueryFilter filter() {
    return filter;
  }

  /** @return The non-null and non-empty list of resolved filters. */
  public List<ResolvedQueryFilter> resolved() {
    return resolved;
  }
  
}
