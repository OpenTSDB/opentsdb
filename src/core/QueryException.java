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

import org.jboss.netty.handler.codec.http.HttpResponseStatus;

/**
 * An exception thrown during query execution such as a timeout or other
 * type of error.
 * @since 2.2
 */
public final class QueryException extends RuntimeException {

  /** An optional, detailed error message  */
  private final String details;
  
  /** The HTTP status code to return to the user  */
  private final HttpResponseStatus status;
  
  /**
   * Default ctor
   * @param msg Message describing the problem.
   */
  public QueryException(final String msg) {
    super(msg);
    status = HttpResponseStatus.BAD_REQUEST;
    details = msg;
  }

  /**
   * Ctor setting the status
   * @param status The status code to respond with for HTTP requests
   * @param msg Message describing the problem.
   */
  public QueryException(final HttpResponseStatus status, final String msg) {
    super(msg);
    this.status = status;
    details = msg;
  }
  
  /**
   * Ctor setting status and the messages
   * @param status The status code to respond with for HTTP requests
   * @param msg Message describing the problem.
   * @param details Extra details for the error
   */
  public QueryException(final HttpResponseStatus status, final String msg, 
      final String details) {
    super(msg);
    this.status = status;
    this.details = details;
  }
  
  /** @return the HTTP status code */
  public final HttpResponseStatus getStatus() {
    return this.status;
  }
  
  /** @return the details, may be an empty string */
  public final String getDetails() { 
    return this.details;
  }
  
  private static final long serialVersionUID = 9040020770546069974L;
}
