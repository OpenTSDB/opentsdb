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
 * A resolved metric name filter.
 * 
 * @since 3.0
 */
public class ResolvedMetricLiteralFilter implements ResolvedQueryFilter {

  /** The non-null original filter. */
  private final QueryFilter filter;
  
  /** The resolved UID. May be null if not found. */
  private final byte[] uid;
  
  /**
   * Default ctor.
   * @param filter The non-null original filter.
   * @param uid The resolved UID. May be null if not found.
   */
  public ResolvedMetricLiteralFilter(final QueryFilter filter, 
                                     final byte[] uid) {
    this.filter = filter;
    this.uid = uid;
  }
  
  @Override
  public QueryFilter filter() {
    return filter;
  }
  
  /** @return The resolved UID. May be null if not found. */
  public byte[] uid() {
    return uid;
  }

}
