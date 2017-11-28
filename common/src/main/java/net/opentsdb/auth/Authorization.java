// This file is part of OpenTSDB.
// Copyright (C) 2017-2018 The OpenTSDB Authors.
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
package net.opentsdb.auth;

import net.opentsdb.query.TimeSeriesQuery;

/**
 * A plugin interface for authorization calls, allowing or disallowing operations
 * in OpenTSDB.
 * 
 * @since 2.4
 */
public interface Authorization {

  /**
   * Determines if the user is allowed to execute the given query.
   * The returned state contains a status code regarding whether or not the query
   * is allowed. If the query IS allowed, the same user state passed as an 
   * argument maybe returned.
   * @param state A non-null auth state with the user and AuthStatus.SUCCESS.
   * @param query A non-null query.
   * @return An AuthState object with a valid AuthStatus to evaluate for 
   * permission.
   */
  public AuthState allowQuery(final AuthState state, 
                              final TimeSeriesQuery query);
  
  /**
   * Determines if the user has a specified permission
   * @param state
   * @param permission
   * @return
   */
  public abstract AuthState hasPermission(final AuthState state, 
                                          final Permissions permission);
  
  // TODO - authenticate other calls.
}
