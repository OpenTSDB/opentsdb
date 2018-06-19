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

import java.util.Collection;

import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.stats.Span;

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

  /** @return The TSDB this context runs under. */
  public TSDB tsdb();
  
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
   * @param span An optional tracing span.
   */
  public void initialize(final Span span);
  
  /**
   * Called by the upstream context or nodes to fetch the next set of data.
   * @param span An optional tracing span.
   */
  public void fetchNext(final Span span);
  
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
   * is a terminal node.
   */
  public Collection<QueryNode> downstream(final QueryNode node);
  
  /**
   * Returns the downstream source nodes for the requested node.
   * @param node A non-null query node.
   * @return A non-null collection of downstream data source nodes. May be
   * empty if the node is a terminal node.
   */
  public Collection<TimeSeriesDataSource> downstreamSources(final QueryNode node);
  
  /**
   * Returns the a collection of the <b>first</b> instance of the given
   * query node type per upstream branch from the given node, exclusive
   * of the given node.
   * @param node A non-null node to search from.
   * @param type A non-null query node type.
   * @return An empty collection if nothing was found or a list of the
   * nodes upstream.
   */
  public Collection<QueryNode> upstreamOfType(final QueryNode node, 
                                              final Class<? extends QueryNode> type);
  
  /**
   * Returns the a collection of the <b>first</b> instance of the given
   * query node type per downstream branch from the given node, exclusive
   * of the given node.
   * @param node A non-null node to search from.
   * @param type A non-null query node type.
   * @return An empty collection if nothing was found or a list of the
   * nodes downstream.
   */
  public Collection<QueryNode> downstreamOfType(final QueryNode node, 
                                                final Class<? extends QueryNode> type);
  
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
