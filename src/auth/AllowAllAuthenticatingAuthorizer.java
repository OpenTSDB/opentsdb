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
import net.opentsdb.core.TSQuery;
import net.opentsdb.query.pojo.Query;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.tsd.BadRequestException;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The default authentication and authorization plugin. This plugin allows all users with no real authentication
 * or authorization. By default, users are considered in the ADMINISTRATOR role and have all of the
 * net.opentsdb.auth.Permissions
 *
 * @author jonathan.creasy
 * @since 2.4.0
 */
@SuppressWarnings("unused")
public class AllowAllAuthenticatingAuthorizer implements Authentication, Authorization {
  private Roles roles;

  static AuthState accessDenied = new AuthState() {
    Channel channel;

    @Override
    public String getUser() {
      return "guest";
    }

    @Override
    public AuthStatus getStatus() {
      return AuthStatus.FORBIDDEN;
    }

    @Override
    public String getMessage() {
      return "Guest User forbidden by AllowAllAuthenticatingAuthorizer";
    }

    @Override
    public Throwable getException() {
      return null;
    }

    @Override
    public void setChannel(Channel channel) {
      this.channel = channel;
    }

    @Override
    public byte[] getToken() {
      return new byte[0];
    }
  };
  static AuthState accessGranted = new AuthState() {
    Channel channel;

    @Override
    public String getUser() {
      return "guest";
    }

    @Override
    public AuthStatus getStatus() {
      return AuthStatus.SUCCESS;
    }

    @Override
    public String getMessage() {
      return "Guest User allowed by AllowAllAuthenticatingAuthorizer";
    }

    @Override
    public Throwable getException() {
      return null;
    }

    @Override
    public void setChannel(Channel channel) {
      this.channel = channel;
    }

    @Override
    public byte[] getToken() {
      return new byte[0];
    }
  };

  private static final AtomicLong queries_allowed = new AtomicLong();
  private static final AtomicLong queries_denied = new AtomicLong();
  private static final AtomicLong authentication_http_allowed = new AtomicLong();
  private static final AtomicLong authentication_http_denied = new AtomicLong();
  private static final AtomicLong authentication_telnet_allowed = new AtomicLong();
  private static final AtomicLong authentication_telnet_denied = new AtomicLong();
  private static final AtomicLong authorization_role_allowed = new AtomicLong();
  private static final AtomicLong authorization_permission_allowed = new AtomicLong();
  private static final AtomicLong authorization_role_denied = new AtomicLong();
  private static final AtomicLong authorization_permission_denied = new AtomicLong();

  public Roles getRoles() {
    return roles;
  }

  public void setRoles(Roles roles) {
    this.roles = roles;
  }

  public AuthState getAccessDenied() {
    return accessDenied;
  }

  public AuthState getAccessGranted() {
    return accessGranted;
  }

  @Override
  public void initialize(final TSDB tsdb) {
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
  public void collectStats(final StatsCollector collector) {
    collector.record("authorization.queries.allowed", queries_allowed);
    collector.record("authorization.queries.denied", queries_denied);
    collector.record("authorization.allowed", authorization_role_allowed, "type=role");
    collector.record("authorization.denied", authorization_role_denied, "type=role");
    collector.record("authorization.allowed", authorization_permission_allowed, "type=permission");
    collector.record("authorization.denied", authorization_permission_denied, "type=permission");
    collector.record("authentication.succeeded", authentication_http_allowed, "type=http");
    collector.record("authentication.denied", authentication_http_denied, "type=http");
    collector.record("authentication.succeeded", authentication_telnet_allowed, "type=telnet");
    collector.record("authentication.denied", authentication_telnet_denied, "type=telnet");
  }

  @Override
  public AuthState authenticateTelnet(final Channel channel, final String[] command) {
    authentication_telnet_allowed.getAndIncrement();
    return accessGranted;
  }

  @Override
  public AuthState authenticateHTTP(final Channel channel, final HttpRequest req) {
    authentication_http_allowed.getAndIncrement();
    return accessGranted;
  }

  @Override
  public Authorization authorization() {
    return null;
  }

  @Override
  public boolean isReady(final TSDB tsdb, final Channel chan) {
    if (tsdb.getAuth() != null && tsdb.getAuth().authorization() != null) {
      if (chan.getAttachment() == null || !(chan.getAttachment() instanceof AuthState)) {
        throw new BadRequestException(HttpResponseStatus.INTERNAL_SERVER_ERROR,
            "Authentication was enabled but the authentication state for "
                + "this channel was not set properly");
      }
      return true;
    } else {
      return false;
    }
  }

  @Override
  public AuthState hasPermission(final AuthState state, final Permissions permission) {
    if (this.roles != null && this.roles.hasPermission(permission)) {
      authorization_permission_allowed.getAndIncrement();
      return accessGranted;
    }
    authorization_permission_denied.getAndIncrement();
    return accessDenied;
  }

  @Override
  public AuthState allowQuery(final AuthState state, final TSQuery query) {
    if (this.roles != null && this.roles.hasPermission(Permissions.HTTP_QUERY)) {
      queries_allowed.getAndIncrement();
      return accessGranted;
    }
    queries_denied.getAndIncrement();
    return accessDenied;
  }

  @Override
  public AuthState allowQuery(final AuthState state, final Query query) {
    if (this.roles != null && this.roles.hasPermission(Permissions.HTTP_QUERY)) {
      queries_allowed.getAndIncrement();
      return accessGranted;
    }
    queries_denied.getAndIncrement();
    return accessDenied;
  }
}