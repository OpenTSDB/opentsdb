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
