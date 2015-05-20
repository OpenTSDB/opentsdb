// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
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
package net.opentsdb.uid;

import java.util.NoSuchElementException;

/**
 * Exception used when a Unique ID can't be found.
 *
 * @see IdException
 */
@Deprecated
public final class NoSuchUniqueId extends NoSuchElementException {

  /** The 'type' of the table.  */
  private final UniqueIdType type;
  /** The ID that couldn't be found.  */
  private final LabelId id;

  /**
   * Constructor.
   *
   * @param type The type of unique ID that triggered the exception.
   * @param id The ID that couldn't be found.
   */
  public NoSuchUniqueId(final UniqueIdType type, final LabelId id) {
    super("No such unique ID for '" + type + "': " + id);
    this.type = type;
    this.id = id;
  }

  /** Returns the type of unique ID that couldn't be found.  */
  public UniqueIdType type() {
    return type;
  }

  /** Returns the unique ID that couldn't be found.  */
  public LabelId id() {
    return id;
  }

  static final long serialVersionUID = 1266815251;

}
