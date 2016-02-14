// This file is part of OpenTSDB.
// Copyright (C) 2015  The OpenTSDB Authors.
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
package net.opentsdb.core;

/**
 * Specification of how to deal with missing intervals when downsampling.
 * @since 2.2
 */
public enum FillPolicy {
  NONE("none"),
  ZERO("zero"),
  NOT_A_NUMBER("nan"),
  NULL("null");

  // The user-friendly name of this policy.
  private final String name;

  FillPolicy(final String name) {
    this.name = name;
  }

  /**
   * Get this fill policy's user-friendly name.
   * @return this fill policy's user-friendly name.
   */
  public String getName() {
    return name;
  }

  /**
   * Get an instance of this enumeration from a user-friendly name.
   * @param name The user-friendly name of a fill policy.
   * @return an instance of {@link FillPolicy}, or {@code null} if the name
   * does not match any instance.
   */
  public static FillPolicy fromString(final String name) {
    for (final FillPolicy policy : FillPolicy.values()) {
      if (policy.name.equalsIgnoreCase(name)) {
        return policy;
      }
    }

    return null;
  }
}

