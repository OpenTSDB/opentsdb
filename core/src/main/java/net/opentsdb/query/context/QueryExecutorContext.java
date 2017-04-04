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

import java.util.Set;

import org.jgrapht.experimental.dag.DirectedAcyclicGraph;
import org.jgrapht.experimental.dag.DirectedAcyclicGraph.CycleFoundException;
import org.jgrapht.graph.DefaultEdge;

import com.google.common.base.Strings;

import net.opentsdb.query.execution.QueryExecutor;
import net.opentsdb.query.execution.QueryExecutorFactory;

/**
 * A graph of query executors that can be instantiated at JVM startup and passed
 * around to various {@link QueryContext} for instantiating executors using their
 * {@link QueryExecutorFactory}.
 * <p>
 * <b>Invariants:</b>
 * <ul>
 * <li>There can only be one sink or starting point for the graph, reached via
 * {@link #newSinkExecutor(QueryContext)}. Add the sink factory via 
 * {@link #registerFactory(QueryExecutorFactory)}. </li>
 * <li>Each factory may have only one downstream factory. Add downstream factories
 * via {@link #registerFactory(QueryExecutorFactory, QueryExecutorFactory)} and
 * instantiate them via {@link #newDownstreamExecutor(QueryContext, QueryExecutorFactory)}.
 * </ul>
 * 
 * @since 3.0
 */
public class QueryExecutorContext {

  /** The graph for generating a query execution path. */
  protected final DirectedAcyclicGraph<QueryExecutorFactory<?>, 
    DefaultEdge> executor_graph;

  /** An id for the context. */
  protected final String id;
  
  /** The sink (first) factory. */
  protected QueryExecutorFactory<?> sink_factory;
  
  /**
   * Default ctor instantiates an empty graph.
   * @param id A descriptive ID for the context (may be chosen at query time)
   */
  public QueryExecutorContext(final String id) {
    if (Strings.isNullOrEmpty(id)) {
      throw new IllegalArgumentException("ID cannot be null or empty.");
    }
    executor_graph = new DirectedAcyclicGraph<QueryExecutorFactory<?>,
        DefaultEdge>(DefaultEdge.class);
    this.id = id;
  }
  
  /** @return A descriptive ID for the context. */
  public String id() {
    return id;
  }
  
  /**
   * Registers the given factory in the graph as a sink. Can only be called once.
   * @param factory A non-null factory.
   * @throws IllegalArgumentException if the factory was null or if a sink has
   * already been added.
   */
  public void registerFactory(final QueryExecutorFactory<?> factory) {
    registerFactory(factory, null);
  }
  
  /**
   * Registers the factory and downstream factory, creating an edge from the 
   * factory to the downstream factory.
   * @param factory A non-null factory.
   * @param downstream A non-null downstream factory. (If null, registers the
   * factory as the sink).
   * @throws IllegalArgumentException if the factory was null or if a sink has
   * already been added or there would be more than one downstream executor for
   * a factory.
   * @throws IllegalStateException if a cycle would have been created.
   */
  public void registerFactory(final QueryExecutorFactory<?> factory, 
                              final QueryExecutorFactory<?> downstream) {
    if (factory == null) {
      throw new IllegalArgumentException("Factory cannot be null.");
    }
    executor_graph.addVertex(factory);
    if (downstream != null) {
      executor_graph.addVertex(downstream);
      if (executor_graph.outgoingEdgesOf(downstream).size() > 1) {
        throw new IllegalArgumentException("An executor can only have one "
            + "downstream executor.");
      }
      try {
        executor_graph.addDagEdge(factory, downstream);
      } catch (CycleFoundException e) {
        throw new IllegalStateException("Context cycle was found", e);
      }
    }
    if (executor_graph.incomingEdgesOf(factory).isEmpty()) {
      // make sure we don't leave an executor hanging
      if (sink_factory != null && sink_factory != factory) {
        if (executor_graph.incomingEdgesOf(sink_factory).isEmpty()) {
          executor_graph.removeVertex(factory);
          throw new IllegalArgumentException("Can't replace the existing sink: " 
              + sink_factory + " without adding an edge.");
        }
      }
      sink_factory = factory;
    }
  }
  
  /**
   * Returns an executor using the sink factory. Factory exceptions bubble up.
   * @param query_context A non-null query context.
   * @return The sink executor for the context.
   * @throws IllegalStateException if the context is uninitialized (no sink) or
   * the factory returned a null executor.
   */
  public QueryExecutor<?> newSinkExecutor(final QueryContext query_context) {
    if (sink_factory == null) {
      throw new IllegalStateException("Sink factory wasn't populated.");
    }
    final QueryExecutor<?> new_executor = sink_factory.newExecutor(query_context);
    if (new_executor == null) {
      throw new IllegalStateException("Factory returned a null executor!");
    }
    return new_executor;
  }
  
  /**
   * Returns an executor from the factory immediately downstream of the given
   * factory. Factory exceptions bubble up.
   * @param query_context A non-null query context.
   * @param factory A non-null factory registered with the context that has a 
   * downstream factory.
   * @return An executor from the downstream factory.
   * @throws IllegalArgumentException if the factory was null.
   * @throws IllegalStateException if the factory was not in the graph, if the
   * factory did not have any downstream factories, or the downstream factory
   * returned a null executor.
   */
  public QueryExecutor<?> newDownstreamExecutor(final QueryContext query_context, 
      final QueryExecutorFactory<?> factory) {
    if (factory == null) {
      throw new IllegalArgumentException("Factory cannot be null.");
    }
    if (!executor_graph.containsVertex(factory)) {
      throw new IllegalStateException("Asked for an executor that doesn't exist.");
    }
    final Set<DefaultEdge> edges = executor_graph.outgoingEdgesOf(factory);
    if (edges.isEmpty()) {
      throw new IllegalStateException("Executor doesn't have any child "
          + "executors: " + factory);
    }
    final QueryExecutor<?> new_executor = 
        executor_graph.getEdgeTarget(edges.iterator().next())
          .newExecutor(query_context);
    if (new_executor == null) {
      throw new IllegalStateException("Factory returned a null executor!");
    }
    return new_executor;
  }

}
