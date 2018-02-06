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
package net.opentsdb.query.execution;

import com.google.common.reflect.TypeToken;

import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.query.context.QueryContext;
import net.opentsdb.query.execution.graph.ExecutionGraphNode;

/**
 * A factory used to generate a {@link QueryExecutor} for a new context. These
 * factories can be instantiated once per JVM and the 
 * {@link #newExecutor(ExecutionGraphNode)} method must be thread safe.
 * <p>
 * The constructor must have only the following parameters:
 * Ctor({@link QueryContext}, {@link QueryExecutorConfig}); 
 * 
 * @param <T> The type of data returned by the executor.
 * 
 * @since 3.0
 */
public abstract class QueryExecutorFactory<T> extends BaseTSDBPlugin {

  /** @return The ID of the executor instantiated by this factory. */
  public abstract String id();
  
  /** @return The type of executor instantiated. */
  public abstract TypeToken<?> type();
  
  /**
   * Returns a new instance of the executor using the config from the
   * graph node.
   * @param node A non-null node.
   * @return An instantiated executor if successful.
   * @throws IllegalArgumentException if the node was null or the node ID was 
   * null or empty.
   * @throws IllegalStateException if the instantiation failed.
   */
  public abstract QueryExecutor<T> newExecutor(final ExecutionGraphNode node);

}
