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

/**
 * The entry point or a component in a query pipeline.
 * 
 * @since 3.0
 */
public interface QueryPipeline {

  /**
   * Sets the listener for this pipeline. NOTE: If an existing listener has
   * been applied it will be orphaned.
   * @param listener A non-null listener to call back with results.
   */
  public void setListener(final QueryListener listener);
  
  /**
   * Returns the current listener for this component.
   * @return The listener if set, null if no listener has been set.
   */
  public QueryListener getListener();
  
  /**
   * Travels downstream the pipeline to fetch the next set of results. 
   * @throws IllegalStateException if no listener was set on this component.
   */
  public void fetchNext();
  
  /**
   * Returns a clone of all downstream components for multi-pass operations.
   * @param listener A non-null listener to use as the sink for the clone.
   * @param cache Whether or not the downstream clone should cache it's results.
   * @return A cloned downstream pipeline.
   */
  public QueryPipeline getMultiPassClone(final QueryListener listener, final boolean cache);
  
  /**
   * Closes the pipeline and releases all resources.
   */
  public void close();
}
