// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
package net.opentsdb.exceptions;

import java.util.Collections;
import java.util.List;

/**
 * High level exception that should be thrown by any portion of a query pipeline
 * to bubble up to the end user.
 * 
 * @since 3.0
 */
public class QueryExecutionException extends RuntimeException {
  private static final long serialVersionUID = 6338254902243113267L;

  /** The order of the execution within a sliced query. E.g. if a query has
   * 4 slices, this order will be an integer from 0 to 3. */
  protected final int order;
  
  /** A status code associated with the exception. The code value depends on the
   * remote source. */
  protected final int status_code;
  
  /** An optional list of exceptions thrown. */
  protected final List<Exception> exceptions;
  
  /**
   * Default ctor that sets a message describing this exception.
   * @param msg A non-null message to be given.
   * @param status_code An optional status code reflecting the error state.
   */
  public QueryExecutionException(final String msg, final int status_code) {
    this(msg, status_code, -1);
  }
  
  /**
   * Ctor that sets a slice order for the exception.
   * @param msg A non-null message to be given.
   * @param status_code An optional status code reflecting the error state.
   * @param order An optional order for the result in a set of slices.
   */
  public QueryExecutionException(final String msg, 
                                 final int status_code, 
                                 final int order) {
    this(msg, status_code, order, (Exception) null);
  }
  
  /**
   * Ctor that sets a descriptive message, order and status code.
   * @param msg A non-null message to be given.
   * @param status_code An optional status code reflecting the error state.
   * @param exceptions An optional list of exceptions. May be null or empty.
   */
  public QueryExecutionException(final String msg, 
                                 final int status_code,
                                 final List<Exception> exceptions) {
    this(msg, status_code, -1, exceptions);
  }
  
  /**
   * Ctor that sets a descriptive message, order and status code.
   * @param msg A non-null message to be given.
   * @param status_code An optional status code reflecting the error state.
   * @param e The original exception that caused this to be thrown.
   */
  public QueryExecutionException(final String msg, 
                                 final int status_code,
                                 final Exception e) {
    this(msg, status_code, -1, e);
  }
  
  /**
   * Ctor that takes a descriptive message, order, status_code and optional list
   * of exceptions that triggered this.
   * @param msg A non-null message to be given.
   * @param status_code An optional status code reflecting the error state.
   * @param order An optional order for the result in a set of slices.
   * @param exceptions An optional list of exceptions. May be null or empty.
   */
  public QueryExecutionException(final String msg, 
                                 final int status_code, 
                                 final int order,
                                 final List<Exception> exceptions) {
    super(msg);
    this.status_code = status_code;
    this.order = order;
    this.exceptions = exceptions;
  }
  
  /**
   * Ctor setting a message, slice order, status code and exception.
   * @param msg A non-null message to be given.
   * @param status_code An optional status code reflecting the error state.
   * @param order An optional order for the result in a set of slices.
   * @param e The original exception that caused this to be thrown.
   */
  public QueryExecutionException(final String msg, 
                                 final int status_code, 
                                 final int order,
                                 final Exception e) {
    super(msg, e);
    this.status_code = status_code;
    this.order = order;
    exceptions = null;
  }
  
  /** @return The slice order if pertaining to a sliced query. */
  public int getOrder() {
    return order;
  }
  
  /** @return An optional status code, e.g. HTTP code. */
  public int getStatusCode() {
    return status_code;
  }

  /** @return A list of exceptions that triggered this or an empty list. */
  public List<Exception> getExceptions() {
    return exceptions == null ? Collections.<Exception>emptyList() : 
      Collections.<Exception>unmodifiableList(exceptions);
  }

  @Override
  public String toString() {
    final StringBuilder buf = new StringBuilder()
        .append(getClass())
        .append(": ")
        .append(getMessage());
    if (exceptions != null) {
      buf.append(" subExceptions[");
      for (int i = 0; i < exceptions.size(); i++) {
        if (i > 0) {
          buf.append(", ");
        }
        buf.append(exceptions.get(i).toString());
      }
      buf.append("]");
    }
    return buf.toString();
  }
}
