// This file is part of OpenTSDB.
// Copyright (C) 2013  The OpenTSDB Authors.
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
package net.opentsdb.utils;

/**
 * Exception class used to wrap the myriad of typed exceptions thrown by 
 * Jackson.
 * @since 2.0
 */
public final class JSONException extends RuntimeException {

  /**
   * Constructor.
   * @param msg The message of the exception, potentially including a stack
   * trace.
   */
  public JSONException(final String msg) {
    super(msg);
  }
  
  /**
   * Constructor.
   * @param cause The exception that caused this one to be thrown.
   */
  public JSONException(final Throwable cause) {
    super(cause);
  }
  
  /**
   * Constructor.
   * @param msg The message of the exception, potentially including a stack
   * trace.
   * @param cause The exception that caused this one to be thrown.
   */
  public JSONException(final String msg, final Throwable cause) {
    super(msg, cause);
  }
  
  private static final long serialVersionUID = 1365518940;
}
