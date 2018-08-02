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

import java.security.Principal;

/**
 * An interface for use in authentication and authorization of calls.
 * 
 * @since 2.4
 */
public interface AuthState {

  /**
   * A list of various authentication and authorization states.
   */
  public enum AuthStatus {
    SUCCESS,
    UNAUTHORIZED,
    FORBIDDEN,
    REDIRECTED,
    ERROR,
    REVOKED
  }
  
  /**
   * Returns the user associated with this state as a string object. This should
   * always return a non-null value. If authentication is enabled but anonymous
   * users or unauthenticated users are allowed, return something like "anonymous".
   * @return A non-null user ID.
   */
  public String getUser();
  
  /**
   * Returns the non-null Java principal representing the user.
   * @return A non-null principal.
   */
  public Principal getPrincipal();
  
  /**
   * The status associated with this authentication or authorization state.
   * @return A non-null auth status enumerator value.
   */
  public AuthStatus getStatus();
  
  /**
   * An optional message associated with the authentication or authorization 
   * attempt. If successful, this may be null. On an error or failure, this
   * message should be populated.
   * @return A useful message regarding the action taken. May be null.
   */
  public String getMessage();
  
  /**
   * An optional exception if an error occurred during the operation.
   * @return An optional exception; may be null.
   */
  public Throwable getException();
  
  /**
   * An optional token to use with an authentication system for validating that
   * the user key is still valid.
   * @return A byte array encoded depending on the authentication plugin. May
   * be null.
   */
  public byte[] getToken();
  
  /**
   * Whether or not the subject has the rights to take on the requested
   * role for Role Based Access Control.
   * @param role The non-null and non-empty role string.
   * @return True if the subject has the rights, false if not.
   */
  public boolean hasRole(final String role);
  
  /**
   * Whether or not the subject has permission to perform the given
   * action for Role Based Access Control.
   * @param action The non-null and non-empty action string.
   * @return True if the subject has permission, false if not.
   */
  public boolean hasPermission(final String action);
}
