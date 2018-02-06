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
  public TimeSeriesQuery query();
  
  /**
   * @return The user's query context.
   */
  public QueryContext queryContext();
  
  /**
   * Called by the user's {@link #pipelineContext()} to initialize the pipeline. Must
   * be called before any of the node functions.
   */
  public void initialize();
  
  /**
   * Called by the upstream context or nodes to fetch the next set of data.
   */
  public void fetchNext();
  
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
  public Collection<QuerySink> sinks();
  
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
