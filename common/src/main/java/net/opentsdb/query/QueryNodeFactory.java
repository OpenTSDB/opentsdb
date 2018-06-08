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

/**
 * The factory used to generate a {@link QueryNode} for a new query execution.
 * 
 * @since 3.0
 */
public interface QueryNodeFactory {

  /**
   * Instantiates a new node using the given context and the default
   * configuration for this node.
   * @param context A non-null query pipeline context.
   * @param id An ID for this node.
   * @return An instantiated node if successful.
   */
  public QueryNode newNode(final QueryPipelineContext context, 
                           final String id);
  
  /**
   * Instantiates a new node using the given context and config.
   * @param context A non-null query pipeline context.
   * @param id An ID for this node.
   * @param config A query node config. May be null if the node does not
   * require a configuration.
   * @return An instantiated node if successful.
   */
  public QueryNode newNode(final QueryPipelineContext context, 
                           final String id,
                           final QueryNodeConfig config);
  
  /**
   * The descriptive ID of the factory used when parsing queries.
   * @return A non-null unique ID of the factory.
   */
  public String id();
 
  /** @return A class to use for serdes for configuring nodes of this
   * type. */
  public Class<? extends QueryNodeConfig> nodeConfigClass();
}
