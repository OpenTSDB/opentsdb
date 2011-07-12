// This file is part of OpenTSDB.
// Copyright (C) 2010  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.uid;

import java.util.NoSuchElementException;

/**
 * Exception used when a name's Unique ID can't be found.
 *
 * @see UniqueIdInterface
 */
public final class NoSuchUniqueName extends NoSuchElementException {

  /** The 'kind' of the table.  */
  private final String kind;
  /** The name that couldn't be found.  */
  private final String name;

  /**
   * Constructor.
   *
   * @param kind The kind of unique ID that triggered the exception.
   * @param name The name that couldn't be found.
   */
  public NoSuchUniqueName(final String kind, final String name) {
    super("No such name for '" + kind + "': '" + name + "'");
    this.kind = kind;
    this.name = name;
  }

  /** Returns the kind of unique ID that couldn't be found.  */
  public String kind() {
    return kind;
  }

  /** Returns the name for which the unique ID couldn't be found.  */
  public String name() {
    return name;
  }

  static final long serialVersionUID = 1266815261;

}
