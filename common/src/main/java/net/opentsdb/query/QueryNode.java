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
 * A node in the query execution DAG that fetches and/or processes time series
 * data in some form. A node can fetch from a data base, merge results from
 * multiple locations, compute a rate or group results. Nodes are created per-
 * query from stable factories.
 * 
 * @since 3.0
 */
public interface QueryNode {

  /**
   * The query context this node belongs to.
   * @return A non-null context.
   */
  public QueryPipelineContext context();
  
  /**
   * Called by the {@link QueryPipelineContext} after the DAG has been setup
   * to initialize the node. This is when the node should parse out bits it 
   * needs from the query and config as well as setup any structures it needs. 
   */
  public void initialize();
  
  /**
   * Called by the upstream context or nodes to fetch the next set of data.
   */
  public void fetchNext();
  
  /**
   * @return The config for this query node.
   */
  public QueryNodeConfig config();
  
  /**
   * @return The descriptive ID of this node.
   */
  public String id();
  
  /**
   * Closes the node and releases all resources locally.
   * TODO - I think I want it to close downstream nodes as well, but not sure.
   */
  public void close();
  
  /**
   * Called by downstream nodes when the downstream node has finished it's query.
   * @param downstream The non-null downstream node calling in.
   * @param final_sequence The final sequence ID of the downstream result.
   */
  public void onComplete(final QueryNode downstream, final long final_sequence);
  
  /**
   * Called by the downstream nodes when a new result is ready.
   * @param next A non-null result (that may be empty).
   */
  public void onNext(final QueryResult next);
  
  /**
   * Called by a downstream node when a non-recoverable error occurs. 
   * @param t A non-null exception.
   */
  public void onError(final Throwable t);
}