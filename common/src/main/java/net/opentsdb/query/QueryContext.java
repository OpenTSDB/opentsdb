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
 * The API used to interact with a query pipeline. This should be given to the
 * caller by a query builder and the caller must call {@link #fetchNext()} to
 * start the query.
 * 
 * @since 3.0
 */
public interface QueryContext {

  /**
   * Returns the current listeners for this component.
   * @return A non-null and non empty collection of listeners. Note that the 
   * collection cannot be null or empty as a query context must not be created
   * without at least one valid listener.
   */
  public Collection<QueryListener> getListeners();
  
  /**
   * Returns the mode the query is executing in.
   * @return The non-null query mode.
   */
  public QueryMode mode();
  
  /**
   * Travels downstream the pipeline to fetch the next set of results. 
   * @throws IllegalStateException if no listener was set on this context.
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
}