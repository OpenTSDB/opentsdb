// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
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
package net.opentsdb.storage;

import net.opentsdb.stats.Span;

/**
 * An executor to fetch data from HBase. E.g. via scan or multi-gets.
 */
public interface BigtableExecutor {
  
  /** The state of the scanners. */
  public static enum State {
    CONTINUE,
    COMPLETE,
    EXCEPTION
  }
  
  /**
   * Attempts to fetch the next set of data from HBase.
   * @param result A non-null query result to store data into.
   * @param span An optional tracer span.
   * @throws IllegalArgumentException if the result was null.
   * @throws IllegalStateException if current result was set.
   */
  public void fetchNext(final Tsdb1xBigtableQueryResult result, final Span span); 
  
  /**
   * Releases resources held by the executor.
   */
  public void close();
  
  /** @return The state of the executor. */
  public State state();
}
