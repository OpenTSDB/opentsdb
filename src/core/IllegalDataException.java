// This file is part of OpenTSDB.
// Copyright (C) 2011-2012  The OpenTSDB Authors.
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
 * Some illegal / malformed / corrupted data has been found in HBase.
 */
public final class IllegalDataException extends IllegalStateException {

  /**
   * Constructor.
   *
   * @param msg Message describing the problem.
   */
  public IllegalDataException(final String msg) {
    super(msg);
  }

  /**
   * Constructor.
   *
   * @param msg Message describing the problem.
   */
  public IllegalDataException(final String msg, final Throwable cause) {
    super(msg, cause);
  }

  static final long serialVersionUID = 1307719142;

}
