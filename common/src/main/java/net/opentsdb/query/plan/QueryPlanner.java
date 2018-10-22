// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
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
package net.opentsdb.query.plan;

import com.google.common.graph.MutableGraph;
import com.stumbleupon.async.Deferred;

import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.stats.Span;

/**
 * An interface for planning a query execution. Creates the DAG and 
 * initializes the configurations.
 * 
 * @since 3.0
 */
public interface QueryPlanner {

  /**
   * Called by a context to compute the query plan.
   * @param span An optional tracing span.
   * @return A deferred resolving to a null value on success or an 
   * exception if something goes wrong.
   */
  public Deferred<Void> plan(final Span span);
  
  /**
   * Replaces the given config with the new config in the DAG.
   * @param old_config A non-null config to replace.
   * @param new_config The non-null config to store in it's place.
   */
  public void replace(final QueryNodeConfig old_config,
      final QueryNodeConfig new_config);
  
  /** @return The non-null and non-empty query node graph post 
   * {@link #plan(Span)}. */
  public MutableGraph<QueryNode> graph();
  
  /** @return The non-null and non-empty config graph post 
   * {@link #plan(Span)}. */
  public MutableGraph<QueryNodeConfig> configGraph();
  
  /** @return The non-null query context that owns this plan. */
  public QueryPipelineContext context();
}
