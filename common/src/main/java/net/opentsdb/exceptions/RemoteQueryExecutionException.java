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

import java.util.List;

/**
 * An exception that occurred when querying a remote location such as a storage
 * server, cache or distributed query engine.
 * 
 * @since 3.0
 */
public class RemoteQueryExecutionException extends QueryExecutionException {
  private static final long serialVersionUID = 2967693539088677442L;

  /** A description of the remote service that threw the exception. */
  private final String remote_endpoint;
  
  /**
   * Default ctor that sets a message describing this exception.
   * @param msg A non-null message to be given.
   * @param remote_endpoint A description of the remote that threw this exception.
   * @param status_code An optional status code reflecting the error state.
   */
  public RemoteQueryExecutionException(final String msg, 
                                       final String remote_endpoint,
                                       final int status_code) {
    this(msg, remote_endpoint, status_code, -1);
  }
  
  /**
   * Ctor that sets a descriptive message, order and status code.
   * @param msg A non-null message to be given.
   * @param remote_endpoint A description of the remote that threw this exception.
   * @param status_code An optional status code reflecting the error state.
   * @param order An optional order for the result in a set of slices.
   */
  public RemoteQueryExecutionException(final String msg, 
                                       final String remote_endpoint, 
                                       final int status_code,
                                       final int order) {
    this(msg, remote_endpoint, status_code, order, (List<Exception>) null);
  }
  
  /**
   * Ctor that takes a descriptive message, order, status_code and optional list
   * of exceptions that triggered this.
   * @param msg A non-null message to be given.
   * @param remote_endpoint A description of the remote that threw this exception.
   * @param status_code An optional status code reflecting the error state.
   * @param exceptions An optional list of exceptions. May be null or empty.
   */
  public RemoteQueryExecutionException(final String msg, 
                                       final String remote_endpoint,
                                       final int status_code, 
                                       final List<Exception> exceptions) {
    this(msg, remote_endpoint, status_code, -1, exceptions);
  }
  
  /**
   * Ctor that takes a descriptive message, order, status_code and optional list
   * of exceptions that triggered this.
   * @param msg A non-null message to be given.
   * @param remote_endpoint A description of the remote that threw this exception.
   * @param status_code An optional status code reflecting the error state.
   * @param e The original exception that caused this to be thrown.
   */
  public RemoteQueryExecutionException(final String msg, 
                                       final String remote_endpoint,
                                       final int status_code, 
                                       final Exception e) {
    this(msg, remote_endpoint, status_code, -1, e);
  }
  
  /**
   * Ctor that takes a descriptive message, order, status_code and optional list
   * of exceptions that triggered this.
   * @param msg A non-null message to be given.
   * @param remote_endpoint A description of the remote that threw this exception.
   * @param status_code An optional status code reflecting the error state.
   * @param order An optional order for the result in a set of slices.
   * @param exceptions An optional list of exceptions. May be null or empty.
   */
  public RemoteQueryExecutionException(final String msg, 
                                       final String remote_endpoint,
                                       final int status_code, 
                                       final int order,
                                       final List<Exception> exceptions) {
    super(msg, status_code, order, exceptions);
    this.remote_endpoint = remote_endpoint;
  }
  
  /**
   * Ctor setting a message, slice order, status code and exception.
   * @param msg A non-null message to be given.
   * @param remote_endpoint A description of the remote that threw this exception.
   * @param status_code An optional status code reflecting the error state.
   * @param order An optional order for the result in a set of slices.
   * @param e The original exception that caused this to be thrown.
   */
  public RemoteQueryExecutionException(final String msg, 
                                       final String remote_endpoint, 
                                       final int status_code,
                                       final int order,
                                       final Exception e) {
    super(msg, status_code, order, e);
    this.remote_endpoint = remote_endpoint;
  }
  
  /** @return The remote endpoint that threw this exception. */
  public String remoteEndpoint() {
    return remote_endpoint;
  }
}
