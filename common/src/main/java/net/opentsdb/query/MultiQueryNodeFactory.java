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
package net.opentsdb.query;

import java.util.Collection;
import java.util.List;

import net.opentsdb.query.execution.graph.ExecutionGraphNode;

/**
 * An interface for a node factory that will generate more than one node
 * from the configuration. An example might be an expression node that 
 * would spawn it's own DAG of binary input nodes with a single output.
 * Also multi-source queries.
 * 
 * @since 3.0
 */
public interface MultiQueryNodeFactory extends QueryNodeFactory {

  /**
   * Instantiates a new node using the given context and config.
   * @param context A non-null query pipeline context.
   * @param id An ID for this node.
   * @param config A query node config. May be null if the node does not
   * require a configuration.
   * @param nodes A non-null list of the execution graph nodes that will
   * be populated with new configs from this factory.
   * @return A non-null and non-empty collection of instantiated nodes 
   * if successful.
   */
  public Collection<QueryNode> newNodes(final QueryPipelineContext context, 
                                        final String id,
                                        final QueryNodeConfig config,
                                        final List<ExecutionGraphNode> nodes);
  
}