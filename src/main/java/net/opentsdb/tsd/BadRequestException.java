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
package net.opentsdb.tsd;

/**
 * Exception thrown by the HTTP handlers when presented with a bad request.
 */
final class BadRequestException extends RuntimeException {

  public BadRequestException(final String message) {
    super(message);
  }

  public static BadRequestException missingParameter(final String paramname) {
    return new BadRequestException("Missing parameter <code>" + paramname
                                   + "</code>");
  }

  static final long serialVersionUID = 1276251669;

}
