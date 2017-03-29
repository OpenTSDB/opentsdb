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
package net.opentsdb.query.execution;

import net.opentsdb.query.pojo.Query;

/**
 * An execution that can be returned if the query engine catches an exception
 * that should bubble upstream immediately. E.g. if a config was bad or an
 * operation not allowed.
 *  
 * @param <T> The type of data expected upstream.
 * 
 * @since 3.0
 */
public class FailedQueryExecution<T> extends QueryExecution<T> {
  
  /**
   * Default Ctor.
   * @param query A non-null query associated with the execution.
   * @param ex A non-null exception to pass upstream.
   * @throws IllegalArgumentException if the exception or query were null.
   */
  public FailedQueryExecution(final Query query, final Exception ex) {
    super(query);
    if (ex == null) {
      throw new IllegalArgumentException("The exception cannot be null.");
    }
    callback(ex);
  }
  
  @Override
  public void cancel() {
    // no-op
  }

}
