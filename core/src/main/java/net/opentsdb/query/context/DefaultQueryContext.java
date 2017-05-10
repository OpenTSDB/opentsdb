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
package net.opentsdb.query.context;

import io.netty.util.Timer;
import io.opentracing.Tracer;
import net.opentsdb.core.TSDB;
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
  public DefaultQueryContext(final TSDB tsdb, 
                             final ExecutionGraph executor_graph) {
    super(tsdb, executor_graph);
  }
  
  /**
   * Ctor that stores a tracer.
   * @param tsdb The TSDB to which this context belongs. May not be null.
   * @param tracer An optional tracer to use for tracking queries.
   * @throws IllegalArgumentException if the TSDB was null.
   */
  public DefaultQueryContext(final TSDB tsdb, 
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
