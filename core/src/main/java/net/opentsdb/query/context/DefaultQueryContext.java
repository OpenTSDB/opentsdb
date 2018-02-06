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
package net.opentsdb.query.context;

import io.netty.util.Timer;
import io.opentracing.Tracer;
import net.opentsdb.core.DefaultTSDB;
import net.opentsdb.query.execution.graph.ExecutionGraph;

/**
 * Simply allows instantiation of the {@link QueryContext} without overrides.
 * 
 * @since 3.0
 */
public class DefaultQueryContext extends QueryContext {

  /**
   * Default ctor.
   * @param tsdb A non-null TSDB this context belongs to.
   * @param executor_graph A non-null executor to use for running the query.
   */
  public DefaultQueryContext(final DefaultTSDB tsdb, 
                             final ExecutionGraph executor_graph) {
    super(tsdb, executor_graph);
  }
  
  /**
   * Ctor that stores a tracer.
   * @param tsdb The TSDB to which this context belongs. May not be null.
   * @param tracer An optional tracer to use for tracking queries.
   * @throws IllegalArgumentException if the TSDB was null.
   */
  public DefaultQueryContext(final DefaultTSDB tsdb, 
                             final ExecutionGraph executor_graph,
                             final Tracer tracer) {
    super(tsdb, executor_graph, tracer);
  }
  
  @Override
  public Timer getTimer() {
    // TODO Auto-generated method stub
    return null;
  }

}
