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
package net.opentsdb.query;

import java.util.Collection;

import net.opentsdb.stats.QueryStats;

/**
 * The API used to interact with a query pipeline. This should be given to the
 * caller by a query builder and the caller must call {@link #fetchNext()} to
 * start the query.
 * 
 * @since 3.0
 */
public interface QueryContext {

  /**
   * Returns the current sinks for this component.
   * @return A non-null and non empty collection of sinks. Note that the 
   * collection cannot be null or empty as a query context must not be created
   * without at least one valid sinks.
   */
  public Collection<QuerySink> sinks();
  
  /**
   * Returns the mode the query is executing in.
   * @return The non-null query mode.
   */
  public QueryMode mode();
  
  /**
   * Travels downstream the pipeline to fetch the next set of results. 
   * @throws IllegalStateException if no sinks was set on this context.
   */
  public void fetchNext();
  
  /**
   * Closes the pipeline and releases all resources.
   */
  public void close();
  
  /**
   * @return An optional stats collector for the query, may be null.
   */
  public QueryStats stats();
  
  /** @return The original query. */
  public TimeSeriesQuery query();
}
