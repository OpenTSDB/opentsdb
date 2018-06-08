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

import net.opentsdb.stats.Span;

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
   * The pipeline context this node belongs to.
   * @return A non-null context.
   */
  public QueryPipelineContext pipelineContext();
  
  /**
   * Called by the {@link QueryPipelineContext} after the DAG has been setup
   * to initialize the node. This is when the node should parse out bits it 
   * needs from the query and config as well as setup any structures it needs.
   * @span An optional tracing span. 
   */
  public void initialize(final Span span);
    
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
   * @param total_sequences The number of sequences sent by this node.
   */
  public void onComplete(final QueryNode downstream, 
                         final long final_sequence,
                         final long total_sequences);
  
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
