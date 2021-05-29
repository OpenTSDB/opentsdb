// This file is part of OpenTSDB.
// Copyright (C) 2019-2021  The OpenTSDB Authors.
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
package net.opentsdb.query;

import java.util.Map;

import net.opentsdb.auth.AuthState;
import net.opentsdb.core.TSDBPlugin;
import net.opentsdb.meta.BatchMetaQuery;

/**
 * The interface for a filter plugin that can modify or block a query coming
 * into the system. For example it could change the cache mode, redirect certain
 * metrics or block access to some queries.
 * 
 * @since 3.0
 */
public interface QueryContextFilter extends TSDBPlugin {

  /**
   * Filters the given query throwing an exception if it's not allowed.
   * @param query The non-null query.
   * @param auth_state The caller's auth state.
   * @param headers An optional map of headers.
   * @return The same query or unaltered, a modified query if filtered, or
   * throws an exception if something is not allowed.
   */
  public TimeSeriesQuery filter(final TimeSeriesQuery query, 
                                final AuthState auth_state, 
                                final Map<String, String> headers);

  /**
   * Filters the given query throwing an exception if it's not allowed.
   * @param query The non-null query.
   * @param auth_state The caller's auth state.
   * @param headers An optional map of headers.
   * @return The same query or unaltered, a modified query if filtered, or
   * throws an exception if something is not allowed.
   */
  public BatchMetaQuery filter(final BatchMetaQuery query,
                               final AuthState auth_state,
                               final Map<String, String> headers);
}
