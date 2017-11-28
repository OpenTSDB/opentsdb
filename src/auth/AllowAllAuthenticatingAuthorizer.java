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

import java.util.HashSet;
import java.util.Set;

/**
 * The default authentication and authorization plugin. This plugin allows all users with no real authentication
 * or authorization. By default, users are considered in the ADMINISTRATOR role and have all of the
 * net.opentsdb.auth.Permissions
 *
 * @author jonathan.creasy
 * @since 2.4.0
 */
public class AllowAllAuthenticatingAuthorizer implements Authentication, Authorization {
    private Set<Roles> roles = new HashSet<Roles>();
    private AuthState accessDenied = new AuthState() {
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
    private AuthState accessGranted = new AuthState() {
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

    @Override
    public void initialize(TSDB tsdb) {
        roles.add(Roles.ADMINISTRATOR);
    }

    @Override
    public Deferred<Object> shutdown() {
        return null;
    }

    @Override
    public String version() {
        return null;
    }

    @Override
    public void collectStats(StatsCollector collector) {

    }

    @Override
    public AuthState authenticateTelnet(Channel channel, String[] command) {
        return accessGranted;
    }

    @Override
    public AuthState authenticateHTTP(Channel channel, HttpRequest req) {
        return accessGranted;
    }

    @Override
    public Authorization authorization() {
        return null;
    }

    @Override
    public boolean isReady(TSDB tsdb, Channel chan) {
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
    public AuthState hasPermission(AuthState state, Permissions permission) {
        for (Roles role : this.roles) {
            if (role.hasPermission(permission)) {
                return accessGranted;
            }
        }
        return accessDenied;
    }

    @Override
    public AuthState hasRole(AuthState state, Roles role) {
        if (roles.contains(role)) {
            return accessGranted;
        } else {
            return accessDenied;
        }
    }

    @Override
    public AuthState allowQuery(AuthState state, TSQuery query) {
        for (Roles role : this.roles) {
            if (role.hasPermission(Permissions.HTTP_QUERY)) {
                return accessGranted;
            }
        }
        return accessDenied;
    }

    @Override
    public AuthState allowQuery(AuthState state, Query query) {
        for (Roles role : this.roles) {
            if (role.hasPermission(Permissions.HTTP_QUERY)) {
                return accessGranted;
            }
        }
        return accessDenied;
    }
}