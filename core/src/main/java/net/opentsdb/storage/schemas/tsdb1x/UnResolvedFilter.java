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

import net.opentsdb.query.filter.QueryFilter;

/**
 * A filter that doesn't resolve to a type and doesn't have a nested
 * filter.
 * 
 * @since 3.0
 */
public class UnResolvedFilter implements ResolvedQueryFilter {

  /** The original filter. */
  private final QueryFilter filter;
  
  /**
   * Default ctor.
   * @param filter The non-null original filter.
   */
  public UnResolvedFilter(final QueryFilter filter) {
    this.filter = filter;
  }
  
  @Override
  public QueryFilter filter() {
    return filter;
  }
}
