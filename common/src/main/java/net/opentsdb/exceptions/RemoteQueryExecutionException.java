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

import com.google.common.collect.Lists;

/**
 * An exception that occurred when querying a remote location such as a storage
 * server, cache or distributed query engine.
 * 
 * @since 3.0
 */
public class RemoteQueryExecutionException extends RuntimeException {
  private static final long serialVersionUID = 2967693539088677442L;

  /** The order of the execution within a sliced query. E.g. if a query has
   * 4 slices, this order will be an integer from 0 to 3. */
  private final int order;
  
  /** A status code associated with the exception. The code value depnds on the
   * remote source. */
  private final int status_code;
  
  /** An optional list of exceptions thrown. */
  private final List<Exception> exceptions;
  
  /**
   * Default ctor that sets a message describing this exception.
   * @param msg A non-null message to be given.
   */
  public RemoteQueryExecutionException(final String msg) {
    this(msg, -1, 0);
  }
  
  /**
   * Ctor that sets a slice order for the exception.
   * @param msg A non-null message to be given.
   * @param order An optional order for the result in a set of slices.
   */
  public RemoteQueryExecutionException(final String msg, final int order) {
    this(msg, order, 0);
  }
  
  /**
   * Ctor that sets a descriptive message, order and status code.
   * @param msg A non-null message to be given.
   * @param order An optional order for the result in a set of slices.
   * @param status_code An optional status code reflecting the error state.
   */
  public RemoteQueryExecutionException(final String msg, final int order,
      final int status_code) {
    this(msg, order, status_code, (List<Exception>) null);
  }
  
  /**
   * Ctor that takes a descriptive message, order, status_code and optional list
   * of exceptions that triggered this.
   * @param msg A non-null message to be given.
   * @param order An optional order for the result in a set of slices.
   * @param status_code An optional status code reflecting the error state.
   * @param exceptions An optional list of exceptions. May be null or empty.
   */
  public RemoteQueryExecutionException(final String msg, final int order,
      final int status_code, final List<Exception> exceptions) {
    super(msg);
    this.order = order;
    this.status_code = status_code;
    this.exceptions = exceptions;
  }
  
  /**
   * Ctor setting a message, slice order, status code and exception.
   * @param msg A non-null message to be given.
   * @param order An optional order for the result in a set of slices.
   * @param status_code An optional status code reflecting the error state.
   * @param e The oroginal exception that caused this to be thrown.
   */
  public RemoteQueryExecutionException(final String msg, final int order,
      final int status_code, final Exception e) {
    super(msg, e);
    this.order = order;
    this.status_code = status_code;
    exceptions = Lists.newArrayList(e);
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
}
