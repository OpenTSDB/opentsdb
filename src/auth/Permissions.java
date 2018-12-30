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

/**
 * The Permissions used within the OpenTSDB Code.
 * Any authorization plugins need to be able to respond to inquiries on these permissions. Plugins and other
 * third-party code may implement additional permissions.
 *
 * @author jonathan.creasy
 * @since 2.4.0
 *
 */
public enum Permissions {
    TELNET_PUT, HTTP_PUT, HTTP_QUERY,
    CREATE_TAGK,CREATE_TAGV, CREATE_METRIC;
}
