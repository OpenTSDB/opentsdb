// This file is part of OpenTSDB.
// Copyright (C) 2010  StumbleUpon, Inc.
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
package net.opentsdb;

/**
 * Exception used when there is a problem with HBase.
 */
public final class HBaseException extends RuntimeException {

  /**
   * Constructor.
   *
   * @param message The message explaining the context of the exception.
   * @param cause The original exception that cause this one to be thrown.
   */
  public HBaseException(final String message, final Throwable cause) {
    super(message, cause);
  }

  static final long serialVersionUID = 1266815233;

}
