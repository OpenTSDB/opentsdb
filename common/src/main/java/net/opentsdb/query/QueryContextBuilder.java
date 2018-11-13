// This file is part of OpenTSDB.
// Copyright (C) 2017-2018  The OpenTSDB Authors.
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

import java.util.List;

import net.opentsdb.auth.AuthState;
import net.opentsdb.stats.QueryStats;

/**
 * The interface used to build a query context for query execution. This is the
 * main API for executing a query and merges the query itself with a mode, stats
 * and at least one listener.
 * 
 * @since 3.0
 */
public interface QueryContextBuilder {
  
  /**
   * Sets the query for this execution.
   * @param query A non-null query.
   * @return The builder.
   */
  public QueryContextBuilder setQuery(final TimeSeriesQuery query);
  
  /**
   * Sets the query mode for this execution.
   * @param mode A non-null mode.
   * @return The builder.
   */
  public QueryContextBuilder setMode(final QueryMode mode);
  
  /**
   * Sets the optional stats object for the query execution.
   * @param stats An optional stats object. If null, stats will not be recorded.
   * @return The builder.
   */
  public QueryContextBuilder setStats(final QueryStats stats);
  
  /**
   * Sets the list of sink configs for the query, taking the list reference.
   * @param configs A list of sink configs.
   * @return The builder.
   */
  public QueryContextBuilder setSinks(final List<QuerySinkConfig> configs);
  
  /**
   * Adds a non-null sink config to the list of sink configs. Does not
   * check for duplicates.
   * @param config A non-null config to add.
   * @return The builder.
   */
  public QueryContextBuilder addSink(final QuerySinkConfig config);
  
  /**
   * A direct sink for programmatic queries to avoid having to use a 
   * factory.
   * @param sink The non-null sink to call.
   * @return The builder.
   */
  public QueryContextBuilder addSink(final QuerySink sink);
  
  /**
   * Sets the authentication state for the query context.
   * @param auth_state A non-null authentication state object.
   * @return The builder.
   */
  public QueryContextBuilder setAuthState(final AuthState auth_state);
  
  /**
   * Returns a context ready for execution via {@link QueryContext#fetchNext()}.
   * If construction failed due to a validation error or some other problem this
   * will throw an exception.
   * @return A non-null context.
   */
  public QueryContext build();
}
