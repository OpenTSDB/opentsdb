// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.auth;

import org.jboss.netty.channel.Channel;

/**
 * An interface for use in authentication and authorization of calls.
 * <p>
 * When used for successful authentication, this stage object will be attached 
 * to the Netty Channel via it's attachment method. It can then be read out of
 * the channel and used for authorization calls.
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
   * Sets the channel associated with this state when used for authentication
   * and attached to a channel. Useful for associating connection information
   * (IP and port) with the user.
   * @param channel A non-null channel.
   */
  public void setChannel(final Channel channel);
  
  /**
   * An optional token to use with an authentication system for validating that
   * the user key is still valid.
   * @return A byte array encoded depending on the authentication plugin. May
   * be null.
   */
  public byte[] getToken();
}
