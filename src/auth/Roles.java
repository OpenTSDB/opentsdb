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

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

/**
 * Suggested standard Roles for OpenTSDB Users
 * @author jonathan.creasy
 * @since 2.4.0
 */
public enum Roles {
    ADMINISTRATOR(EnumSet.allOf(Permissions.class)),
    PUTONLY(EnumSet.of(Permissions.HTTP_PUT, Permissions.TELNET_PUT)),
    WRITER(EnumSet.of(Permissions.HTTP_PUT, Permissions.TELNET_PUT, Permissions.CREATE_TAGV)),
    READER(EnumSet.of(Permissions.HTTP_QUERY)),
    CREATOR(EnumSet.of(Permissions.CREATE_METRIC, Permissions.CREATE_TAGK, Permissions.CREATE_TAGV)),
    GUEST(EnumSet.noneOf(Permissions.class));

    private Set<EnumSet<Permissions>> grantedPermissions;

    Roles(final EnumSet<Permissions>... providedRoles) {
        Collections.addAll(this.grantedPermissions, providedRoles);
    }

    public Boolean hasPermission(final Permissions permission) {
        for(EnumSet<Permissions> permissions : grantedPermissions) {
            if (permissions.contains(permission)) {
                return true;
            }
        }
        return false;
    }
}
