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

import java.util.*;

/**
 * Suggested standard Roles for OpenTSDB Users
 *
 * @author jonathan.creasy
 * @since 2.4.0
 */
@SuppressWarnings({"WeakerAccess", "unused"})
public class Roles {
  final static EnumSet<Permissions> ADMINISTRATOR = EnumSet.allOf(Permissions.class);
  final static EnumSet<Permissions> PUTONLY = EnumSet.of(Permissions.HTTP_PUT, Permissions.TELNET_PUT);
  final static EnumSet<Permissions> WRITER = EnumSet.of(Permissions.HTTP_PUT, Permissions.TELNET_PUT, Permissions.CREATE_TAGV);
  final static EnumSet<Permissions> READER = EnumSet.of(Permissions.HTTP_QUERY);
  final static EnumSet<Permissions> CREATOR = EnumSet.of(Permissions.CREATE_METRIC, Permissions.CREATE_TAGK, Permissions.CREATE_TAGV);
  final static EnumSet<Permissions> GUEST = EnumSet.noneOf(Permissions.class);

  @SuppressWarnings("Convert2Diamond")
  private final Set<EnumSet<Permissions>> grantedPermissions = new HashSet<EnumSet<Permissions>>();

  public Roles() {
    this.grantedPermissions.add(GUEST);
  }

  public Roles(final EnumSet<Permissions> permissions) {
    this.grantedPermissions.add(permissions);
  }

  public void grantPermissions(final EnumSet<Permissions> permissions) {
    grantedPermissions.add(permissions);
  }

  public Boolean hasPermission(final Permissions permission) {
    for (EnumSet<Permissions> permissions : this.grantedPermissions) {
      if (permissions.contains(permission)) {
        return true;
      }
    }
    return false;
  }
}
