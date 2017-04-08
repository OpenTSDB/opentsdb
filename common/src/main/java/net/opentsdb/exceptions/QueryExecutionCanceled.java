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
 * Exception bubbled up when a query is canceled.
 * 
 * @since 3.0
 */
public class QueryExecutionCanceled extends QueryExecutionException {
  private static final long serialVersionUID = -2225712915698705683L;

  /**
   * Default ctor that sets a message describing this exception.
   * @param msg A non-null message to be given.
   * @param status_code An optional status code reflecting the error state.
   */
  public QueryExecutionCanceled(final String msg, final int status_code) {
    super(msg, status_code);
  }
  
  /**
   * Ctor that sets a descriptive message, order and status code.
   * @param msg A non-null message to be given.
   * @param status_code An optional status code reflecting the error state.
   * @param e The original exception that caused this to be thrown.
   */
  public QueryExecutionCanceled(final String msg, 
                                final int status_code, 
                                final Exception e) {
    super(msg, status_code, e);
  }

  /**
   * Ctor setting a message, slice order, status code and exception.
   * @param msg A non-null message to be given.
   * @param status_code An optional status code reflecting the error state.
   * @param order An optional order for the result in a set of slices.
   * @param e The original exception that caused this to be thrown.
   */
  public QueryExecutionCanceled(final String msg, 
                                final int status_code, 
                                final int order,
                                final Exception e) {
    super(msg, status_code, order, e);
  }

  /**
   * Ctor that takes a descriptive message, order, status_code and optional list
   * of exceptions that triggered this.
   * @param msg A non-null message to be given.
   * @param status_code An optional status code reflecting the error state.
   * @param order An optional order for the result in a set of slices.
   * @param exceptions An optional list of exceptions. May be null or empty.
   */
  public QueryExecutionCanceled(final String msg, 
                                final int status_code, 
                                final int order,
                                final List<Exception> exceptions) {
    super(msg, status_code, order, exceptions);
  }

  /**
   * Ctor that sets a slice order for the exception.
   * @param msg A non-null message to be given.
   * @param status_code An optional status code reflecting the error state.
   * @param order An optional order for the result in a set of slices.
   */
  public QueryExecutionCanceled(final String msg, 
                                final int status_code, 
                                final int order) {
    super(msg, status_code, order);
  }

  /**
   * Ctor that sets a descriptive message, order and status code.
   * @param msg A non-null message to be given.
   * @param status_code An optional status code reflecting the error state.
   * @param exceptions An optional list of exceptions. May be null or empty.
   */
  public QueryExecutionCanceled(final String msg, 
                                final int status_code,
                                final List<Exception> exceptions) {
    super(msg, status_code, exceptions);
  }

}
