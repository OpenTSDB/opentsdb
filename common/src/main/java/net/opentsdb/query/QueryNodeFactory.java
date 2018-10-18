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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSDBPlugin;
import net.opentsdb.query.plan.QueryPlanner;

/**
 * The factory used to generate a {@link QueryNode} for a new query execution.
 * Implementations can be single or multi-node.
 * 
 * @since 3.0
 */
public interface QueryNodeFactory extends TSDBPlugin {
  
  /**
   * The descriptive ID of the factory used when parsing queries.
   * @return A non-null unique ID of the factory.
   */
  public String id();
  
  /**
   * Parse the given JSON or YAML into the proper node config.
   * @param mapper A non-null mapper to use for parsing.
   * @param tsdb The non-null TSD to pull factories from.
   * @param node The non-null node to parse.
   * @return An instantiated node config if successful.
   */
  public QueryNodeConfig parseConfig(final ObjectMapper mapper,
                                     final TSDB tsdb, 
                                     final JsonNode node);

  /**
   * During query planning, this is called to allow the node factory to
   * modify the user provided configuration and/or the graph, particularly
   * for multi-node factories.
   * @param query The non-null user given query.
   * @param config An optional user provided config for the node. 
   * @param planner The non-null query planner to mutate if necessary.
   */
  public void setupGraph(
      final TimeSeriesQuery query, 
      final QueryNodeConfig config, 
      final QueryPlanner planner);
  
  /**
   * Instantiates a new node using the given context and the default
   * configuration for this node.
   * @param context A non-null query pipeline context.
   * @return An instantiated node if successful.
   */
  public QueryNode newNode(final QueryPipelineContext context);
  
  /**
   * Instantiates a new node using the given context and config.
   * @param context A non-null query pipeline context.
   * @param config A query node config. May be null if the node does not
   * require a configuration.
   * @return An instantiated node if successful.
   */
  public QueryNode newNode(final QueryPipelineContext context, 
                           final QueryNodeConfig config);
  
}
