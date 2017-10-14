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

/**
 * The non-user facing pipeline context that provides the entry point and 
 * operations for executing a query. This is instantiated and called by the 
 * user facing {@link QueryContext}.
 * <p>
 * This class is responsible for creating the execution DAG.
 * 
 * @since 3.0
 */
public interface QueryPipelineContext extends QueryNode {

  /**
   * @return The original query passed by the user.
   */
  public TimeSeriesQuery getQuery();
  
  /**
   * @return The user's query context.
   */
  public QueryContext getContext();
  
  /**
   * Called by the user's {@link #context()} to initialize the pipeline. Must
   * be called before any of the node functions.
   */
  public void initialize();
  
  /**
   * Returns the upstream nodes for the requested node.
   * @param node A non-null query node.
   * @return A non-null collection of upstream nodes. May be empty if the node
   * is a sink node.
   */
  public Collection<QueryNode> upstream(final QueryNode node);
  
  /**
   * Returns the downstream nodes for the requested node.
   * @param node A non-null query node.
   * @return A non-null collection of downstream nodes. May be empty if the node
   * is a source node.
   */
  public Collection<QueryNode> downstream(final QueryNode node);
  
  /**
   * The collection of sinks given by the calling API that will receive final
   * results.
   * @return A non-null list of one or more sinks.
   */
  public Collection<QueryListener> sinks();
  
  /**
   * The list of "root" or "sink" query nodes for the DAG. These will be linked
   * to the {@link #sinks()}.
   * @return A non-null and non-empty collection of root query nodes.
   */
  public Collection<QueryNode> roots();
  
  /**
   * Releases all resources held by the query graph.
   */
  public void close();
  
}