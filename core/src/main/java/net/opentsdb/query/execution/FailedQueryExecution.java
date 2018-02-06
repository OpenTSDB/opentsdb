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
package net.opentsdb.query.execution;

import net.opentsdb.query.pojo.TimeSeriesQuery;

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
  public FailedQueryExecution(final TimeSeriesQuery query, final Exception ex) {
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
