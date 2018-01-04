// This file is part of OpenTSDB.
// Copyright (C) 2016-2017  The OpenTSDB Authors.
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

import com.stumbleupon.async.Deferred;
import net.opentsdb.core.TSDB;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.handler.codec.http.HttpRequest;

import java.util.HashSet;
import java.util.Set;

/**
 * The default authentication and authorization plugin. This plugin allows all users with no real authentication
 * or authorization. By default, users are considered in the ADMINISTRATOR role and have all of the
 * net.opentsdb.auth.Permissions
 * <p>
 * Warning: There is no good reason to use this, it exists mostly to demonstrate the functionality, and provide enough
 * code to identify that the authentication and authorization framework is sufficient for basic purposes.
 *
 * @author jonathan.creasy
 * @since 2.4.0
 */
public class AllowAllAuthenticator extends AbstractAuthentication {
  private final SimpleAuthStateImpl accessGranted = new SimpleAuthStateImpl("guest", AuthState.AuthStatus.SUCCESS, "Access Granted by SimpleAuthorizationImpl");

  @Override
  public void initialize(final TSDB tsdb) {
    Set<Roles> roles = new HashSet<Roles>();
    roles.add(SimpleRolesImpl.ADMINISTRATOR);
    accessGranted.setRoles(roles);
  }

  @Override
  public Deferred<Object> shutdown() {
    return null;
  }

  @Override
  public String version() {
    return "2.4.0";
  }

  @Override
  public AuthState authenticateTelnet(final Channel channel, final String[] command) {
    login_success.incrementAndGet();
    return accessGranted;
  }

  @Override
  public AuthState authenticateHTTP(final Channel channel, final HttpRequest req) {
    login_success.incrementAndGet();
    return accessGranted;
  }
}