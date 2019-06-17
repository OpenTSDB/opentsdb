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

import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.data.TimeSeriesId;
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
   * @return A deferred resolving to a null on success or an exception if
   * something went wrong during an async call.
   */
  public Deferred<Void> initialize(final Span span);
  
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
   * Returns the downstream source nodes for the requested node.
   * @param node A non-null query node.
   * @return A non-null collection of downstream data source node IDs in
   * the format "node:source" or "source". If the node is a source then
   * just the node's ID is returned.
   */
  public Collection<String> downstreamSourcesIds(final QueryNode node);
  
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
   * Adds the given ID to the context.
   * @param hash The hash of the ID.
   * @param id The non-null ID.
   * @throws IllegalArgumentException if the ID already exists for the given type.
   */
  public void addId(final long hash, final TimeSeriesId id);
  
  /**
   * Allows retrieval of the time series ID for the given hash and type from a
   * partial time series.
   * @param hash The hash to look up.
   * @param type The non-null type.
   * @return The time series ID if found, null if not.
   */
  public TimeSeriesId getId(final long hash, 
                            final TypeToken<? extends TimeSeriesId> type);
  
  /**
   * Determines if the time series ID for the given hash and type from a partial 
   * time series is present.
   * @param hash The hash to look up.
   * @param type The non-null type.
   * @return True if the ID is present, false if not.
   */
  public boolean hasId(final long hash, 
                       final TypeToken<? extends TimeSeriesId> type);
  
  /**
   * Releases all resources held by the query graph.
   */
  public void close();
  
}
