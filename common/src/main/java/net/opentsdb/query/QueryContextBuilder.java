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
package net.opentsdb.query;

import java.util.Collection;

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
   * Adds a listener to the collection of sinks.
   * @param sink A non-null query listener.
   * @return The builder.
   */
  public QueryContextBuilder addQuerySink(final QuerySink sink);
  
  /**
   * Sets the collection of query sinks. If any sinks have been set already, 
   * the existing sinks will be overwritten.
   * @param sinks A collection of one or more non-null query sinks.
   * @return The builder.
   */
  public QueryContextBuilder setQuerySinks(final Collection<QuerySink> sinks);
  
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
   * Returns a context ready for execution via {@link QueryContext#fetchNext()}.
   * If construction failed due to a validation error or some other problem this
   * will throw an exception.
   * @return A non-null context.
   */
  public QueryContext build();
}
