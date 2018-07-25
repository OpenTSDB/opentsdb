// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
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
   * @param t The original exception that caused this to be thrown.
   */
  public QueryExecutionCanceled(final String msg, 
                                final int status_code, 
                                final Throwable t) {
    super(msg, status_code, t);
  }

  /**
   * Ctor setting a message, slice order, status code and exception.
   * @param msg A non-null message to be given.
   * @param status_code An optional status code reflecting the error state.
   * @param order An optional order for the result in a set of slices.
   * @param t The original exception that caused this to be thrown.
   */
  public QueryExecutionCanceled(final String msg, 
                                final int status_code, 
                                final int order,
                                final Throwable t) {
    super(msg, status_code, order, t);
  }

  /**
   * Ctor that takes a descriptive message, order, status_code and optional list
   * of throwables that triggered this.
   * @param msg A non-null message to be given.
   * @param status_code An optional status code reflecting the error state.
   * @param order An optional order for the result in a set of slices.
   * @param throwables An optional list of throwables. May be null or empty.
   */
  public QueryExecutionCanceled(final String msg, 
                                final int status_code, 
                                final int order,
                                final List<Throwable> throwables) {
    super(msg, status_code, order, throwables);
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
   * @param throwables An optional list of throwables. May be null or empty.
   */
  public QueryExecutionCanceled(final String msg, 
                                final int status_code,
                                final List<Throwable> throwables) {
    super(msg, status_code, throwables);
  }

}
