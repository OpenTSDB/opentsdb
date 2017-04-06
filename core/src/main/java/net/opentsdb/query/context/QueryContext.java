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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jgrapht.experimental.dag.DirectedAcyclicGraph;
import org.jgrapht.experimental.dag.DirectedAcyclicGraph.CycleFoundException;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.traverse.DepthFirstIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import io.netty.util.Timer;
import io.opentracing.Tracer;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.TimeStampComparator;
import net.opentsdb.data.iterators.IteratorStatus;
import net.opentsdb.data.iterators.TimeSeriesIterator;
import net.opentsdb.query.context.QueryExecutorContext;
import net.opentsdb.query.processor.TimeSeriesProcessor;
import net.opentsdb.utils.Deferreds;

/**
 * TODO - complete and doc
 * 
 * <b>Warning:</b> This class is not intended to be thread safe. ONLY work with
 * a context from a single thread at any time.
 * <p>
 * Calls to {@link #initialize()} and {@link #fetchNext()} should be performed
 * asynchronously and a callback should be applied to the deferred from either
 * method.  
 * 
 * @since 3.0
 */
public abstract class QueryContext {
  private static final Logger LOG = LoggerFactory.getLogger(QueryContext.class);
  
  /** The TSDB to which we belong. */
  protected final TSDB tsdb;
  
  /** The "current" timestamp returned when {@link #syncTimestamp()} is 
   * called. */
  // TODO - allow for a choice of TimeStamps
  protected TimeStamp sync_time = new MillisecondTimeStamp(Long.MAX_VALUE);
  
  /** The "next" timestamp updated when child iterators call into 
   * {@link #updateContext(IteratorStatus, TimeStamp)}. */
  // TODO - allow for a choice of TimeStamps
  protected TimeStamp next_sync_time = new MillisecondTimeStamp(Long.MAX_VALUE);
  
  /** The current context status. */
  protected IteratorStatus status = IteratorStatus.END_OF_DATA;
  
  /** The next status that will be returned by the context. */
  protected IteratorStatus next_status = IteratorStatus.END_OF_DATA;
  
  /** The parent context if this is a child. */
  protected QueryContext parent;

  /** Convenience list of this context's offspring. */
  protected List<QueryContext> children;
  
  /** The tracer used for tracking the query. */
  protected final Tracer tracer; 
  
  /** The context graph so we can find links when sub-contexts are in play. */
  protected final DirectedAcyclicGraph<QueryContext, DefaultEdge> context_graph;
  
  /** The processor graph. */
  protected final DirectedAcyclicGraph<TimeSeriesProcessor, DefaultEdge> processor_graph;
  
  /** The iterator graph. Should just be a parallel list of iterators. */
  protected final DirectedAcyclicGraph<TimeSeriesIterator<?>, DefaultEdge> iterator_graph;
  
  protected final QueryExecutorContext executor_context;
  
  /** The list of terminal iterators in the iterator graph. Initialization and
   * close methods can be called on these to handle all iterators on the chain.
   */
  protected Set<TimeSeriesIterator<?>> iterator_sinks = Sets.newHashSet();
  
  /** The list of terminal processors in the processor graph that doen't have any
   * incoming connections. Used for initialization and closing.
   */
  protected Set<TimeSeriesProcessor> processor_sinks = Sets.newHashSet();
  
  /** A list of zero or more exceptions if something went wrong during operation. */
  protected List<Exception> exceptions;
  
  /**
   * Default ctor initializes the graphs and registers this context to the 
   * context graph.
   * @param tsdb The TSDB to which this context belongs. May not be null.
   * @throws IllegalArgumentException if the TSDB was null.
   */
  public QueryContext(final TSDB tsdb) {
    this(tsdb, (Tracer) null);
  }
  
  /**
   * Ctor that stores a tracer.
   * @param tsdb The TSDB to which this context belongs. May not be null.
   * @param tracer An optional tracer to use for tracking queries.
   * @throws IllegalArgumentException if the TSDB was null.
   */
  public QueryContext(final TSDB tsdb, final Tracer tracer) {
    if (tsdb == null) {
      throw new IllegalArgumentException("TSDB cannot be null.");
    }
    this.tsdb = tsdb;
    this.tracer = tracer;
    context_graph = new DirectedAcyclicGraph<QueryContext, 
        DefaultEdge>(DefaultEdge.class);
    processor_graph = new DirectedAcyclicGraph<TimeSeriesProcessor, 
        DefaultEdge>(DefaultEdge.class);
    iterator_graph = new DirectedAcyclicGraph<TimeSeriesIterator<?>,
        DefaultEdge>(DefaultEdge.class);
    executor_context = new QueryExecutorContext("Testing");
    context_graph.addVertex(this);
  }
  
  /**
   * Ctor for use when splitting or creating sub graphs. 
   * @param context A non-null context to use as the parent.
   * @throws IllegalArgumentException if the context was null.
   */
  public QueryContext(final QueryContext context) {
    if (context == null) {
      throw new IllegalArgumentException("Parent context cannot be null.");
    }
    tsdb = context.tsdb;
    tracer = context.tracer;
    this.context_graph = context.context_graph;
    processor_graph = new DirectedAcyclicGraph<TimeSeriesProcessor, 
        DefaultEdge>(DefaultEdge.class);
    iterator_graph = new DirectedAcyclicGraph<TimeSeriesIterator<?>,
        DefaultEdge>(DefaultEdge.class);
    executor_context = context.executor_context;
    parent = context;
    context_graph.addVertex(this);
    try {
      context.context_graph.addDagEdge(context, this);
    } catch (CycleFoundException e) {
      // Note that this *should* be impossible unless implementers override
      // the hashCode() or equals() methods so that this context points to 
      // another context that points back to the same hash.
      throw new IllegalStateException("Context cycle was found", e);
    }
  }
  
  @Override
  public String toString() {
    final StringBuilder buf = new StringBuilder()
        .append("currentStatus=")
        .append(status)
        .append(", nextStatus=")
        .append(next_status)
        .append(", syncTimestamp=")
        .append(sync_time)
        .append(", nextSyncTimestamp=")
        .append(next_sync_time)
        .append(", processorGraph=")
        .append(processor_graph)
        .append(", iteratorGraph=")
        .append(iterator_graph)
        .append(", children=")
        .append(children)
        .append(", processorSinks=")
        .append(processor_sinks)
        .append(", iteratorSinks=")
        .append(iterator_sinks);
    return buf.toString();
  }
  
  /**
   * Initializes the processors in order, depth first. Chains the callbacks 
   * of incoming processors to their downstream children. 
   * <b>WARNING:</b> Iterators are NOT initialized via this method. Instead,
   * each processor (starting with the sources) is responsible for calling
   * {@link TimeSeriesIterator#initialize()} on their own iterator set. As the
   * processor callbacks are triggered in order, this should be ok.
   * 
   * @return A deferred to wait on that resolves to null on success or an 
   * exception on failure.
   */
  public Deferred<Object> initialize() {
    // The deferred called at the end of the run with a null on success or
    // an exception.
    final Deferred<Object> deferred = new Deferred<Object>();
    
    // Used to fail-fast by catching exceptions on grouped deferreds. Without 
    // an error callback on each init, if one throws an exception without 
    // passing it upstream, the initialization will hang as the group waits for
    // all of the deferreds to report in.
    final AtomicBoolean deferred_called = new AtomicBoolean();
    
    final DepthFirstIterator<TimeSeriesProcessor, DefaultEdge> df_iterator = 
        new DepthFirstIterator<TimeSeriesProcessor, DefaultEdge>(processor_graph);
    final Set<TimeSeriesProcessor> sources = Sets.newHashSet();
    final List<Deferred<Object>> finals = Lists.newArrayList();
    
    try {
      class ErrBack implements Callback<Object, Exception> {
        @Override
        public Object call(final Exception e) throws Exception {
          if (deferred_called.compareAndSet(false, true)) {
            deferred.callback(e);
            handleException(e);
          } else {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Additional exceptions were caught when initializing "
                  + "the context: " + QueryContext.this, e);
            }
            handleException(e);
          }
          // let it bubble up to cancel other calls.
          return e;
        }
        @Override
        public String toString() {
          return "Context initialization error callback.";
        }
      }
      final ErrBack error_callback = new ErrBack();
      
      while (df_iterator.hasNext()) {
        final TimeSeriesProcessor processor = df_iterator.next();
        final Set<DefaultEdge> downstream = processor_graph.outgoingEdgesOf(processor);
        if (downstream.isEmpty()) {
          sources.add(processor);
          continue;
        }
  
        if (downstream.size() == 1) {
          final TimeSeriesProcessor child = 
              processor_graph.getEdgeTarget(downstream.iterator().next());
          child
            .initializationDeferred()
            .addBothDeferring(processor.initializationCallback())
            .addErrback(error_callback);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Linking callback of " + processor 
                + " to init deferred of " + child);
          }
        } else {
          final List<Deferred<Object>> child_deferreds = 
              Lists.newArrayListWithExpectedSize(downstream.size());
          for (final DefaultEdge edge : downstream) {
            final TimeSeriesProcessor child = processor_graph.getEdgeTarget(edge);
            child_deferreds.add(child.initializationDeferred()
                .addErrback(error_callback));
            if (LOG.isDebugEnabled()) {
              LOG.debug("Adding callback of " + processor 
                  + " to grouped init deferred of " + child);
            }
          }
          Deferred.group(child_deferreds)
            .addCallback(Deferreds.NULL_GROUP_CB)
            .addBoth(processor.initializationCallback())
            .addErrback(error_callback);
        }
      }
      
      for (final TimeSeriesProcessor processor : sources) {
        // Note that if we have properly configured the initialization chain
        // then we don't need to worry about the output of these initializations
        // an only need to look at the sink initializations.
        processor.initialize().addErrback(error_callback);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Initialized processor " + processor);
        }
      }
      
      // These are the sinks that we need to wait on for initialization.
      for (final TimeSeriesProcessor sink : processor_sinks) {
        finals.add(sink.initializationDeferred()
              .addErrback(error_callback));
      }
      
      /** Helper class that updates the parent's context if there is one. */
      class InitCB implements Callback<Object, Object> {
        @Override
        public Object call(final Object result_or_exception) throws Exception {
          if (!deferred_called.compareAndSet(false, true)) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("InitCB was still called for context " + QueryContext.this 
                  + " though an exception handler caught it previously.");
            }
            return null;
          }
          
          if (result_or_exception instanceof Exception) {
            handleException((Exception) result_or_exception);
            deferred.callback((Exception) result_or_exception);
          } else {
            if (parent != null) {
              parent.updateContext(next_status, QueryContext.this.next_sync_time);
            }
            deferred.callback(null);
          }
          return null;
        }
        @Override
        public String toString() {
          return "Initialization Complete Callback for context: " 
              + QueryContext.this;
        }
      }
      
      Deferred.group(finals)
          .addCallback(Deferreds.NULL_GROUP_CB)
          .addBoth(new InitCB());
    } catch (Exception e) {
      handleException(e);
      deferred.callback(e);
    }
    return deferred;
  }
  
  /** @return The set of terminating processors to consume from. */
  public Set<TimeSeriesProcessor> processorSinks() {
    return Collections.unmodifiableSet(processor_sinks);
  }
  
  /** @return A view into the iterator sinks. Primarily for unit testing. */
  public Set<TimeSeriesIterator<?>> iteratorSinks() {
    return Collections.unmodifiableSet(iterator_sinks);
  }
  
  /**
   * Updates the {@link #nextStatus()} according to 
   * {@link IteratorStatus#updateStatus(IteratorStatus, IteratorStatus)} and 
   * updates {@link #nextTimestamp()} only if the incoming timestamp is less
   * than {@link #nextTimestamp()}.
   * @param status A non-null status to process.
   * @param timestamp An optional timestamp to compare against when the next
   * value is {@link IteratorStatus#HAS_DATA}.
   * @throws IllegalArgumentException if the status was null.
   */
  public void updateContext(final IteratorStatus status, 
      final TimeStamp timestamp) {
    if (status == null) {
      throw new IllegalArgumentException("Status cannot be null.");
    }
    next_status = IteratorStatus.updateStatus(next_status, status);
    if (timestamp != null && 
        timestamp.compare(TimeStampComparator.LT, next_sync_time)) {
      next_sync_time.update(timestamp);
    }
  }
  
  /** @return The current status of the iterator. This echos what was returned
   * by the last call to {@link #advance()}. */
  public IteratorStatus currentStatus() {
    return status;
  }
  
  /** @return The next status that should be returned by {@link #advance()}. */
  public IteratorStatus nextStatus() {
    return next_status;
  }
  
  /** @return The current timestamp for iterators to sync to when their 
   * {@link TimeSeriesIterator#next()} method is called. */
  public TimeStamp syncTimestamp() {
    return sync_time;
  }

  /** @return The next timestamp that is updated each time 
   * {@link TimeSeriesIterator#next()} is called. The value will be the next
   * {@link #syncTimestamp()}. */
  public TimeStamp nextTimestamp() {
    return next_sync_time;
  }
  
  /**
   * Advances the context state by moving {@link #nextTimestamp()} to 
   * {@link #syncTimestamp()} and {@link #nextStatus()} to {@link #currentStatus()}.
   * At the end of the call, {@link #nextStatus()} will equal 
   * {@link IteratorStatus#END_OF_DATA} and {@link TimeStamp#setMax()} will be
   * called.
   * @return The current context status, the same as {@link #currentStatus()}.
   */
  public IteratorStatus advance() {
    sync_time.update(next_sync_time);
    next_sync_time.setMax();
    status = next_status;
    if (next_status != IteratorStatus.END_OF_CHUNK) {
      next_status = IteratorStatus.END_OF_DATA;
    }
    return status;
  }
  
  /**
   * Executes {@link TimeSeriesIterator#fetchNext()} on the terminal iterators
   * belonging to this context and all child contexts.
   * Note that the {@link #nextStatus()} is set to {@link IteratorStatus#END_OF_DATA}
   * so that child iterators can update the status.
   * 
   * @return A non-null deferred to wait on that will resolve to null on success
   * or an exception if there was an error.
   */
  public Deferred<Object> fetchNext() {
    final List<Deferred<Object>> deferreds = Lists.newArrayListWithExpectedSize(
        children != null ? children.size() + 1 : 1);
    try {
      next_status = IteratorStatus.END_OF_DATA;
      if (children != null) {
        for (final QueryContext child : children) {
          deferreds.add(child.fetchNext());
          next_status = IteratorStatus.updateStatus(next_status, child.nextStatus());
        }
      }
      for (final TimeSeriesIterator<?> iterator : iterator_sinks) {
        deferreds.add(iterator.fetchNext());
      }
      return Deferred.group(deferreds).addCallback(Deferreds.NULL_GROUP_CB);
    } catch (Exception e) {
      return Deferred.fromError(e);
    }
  }
  
  /**
   * Executes {@link TimeSeriesIterator#close()} on the terminal iterators
   * belonging to this context and {@link #close()} on all child contexts.
   * 
   * @return A non-null deferred to wait on that will resolve to null on success
   * or an exception if there was an error.
   */
  public Deferred<Object> close() {
    final List<Deferred<Object>> deferreds = Lists.newArrayListWithExpectedSize(
        children != null ? children.size() + 1 : 1);
    try {
      next_status = IteratorStatus.END_OF_DATA;
      if (children != null) {
        for (final QueryContext child : children) {
          deferreds.add(child.close());
        }
      }
      for (final TimeSeriesIterator<?> iterator : iterator_sinks) {
        deferreds.add(iterator.close());
      }
      return Deferred.group(deferreds).addCallback(Deferreds.NULL_GROUP_CB);
    } catch (Exception e) {
      return Deferred.fromError(e);
    }
  }
  
  /**
   * Registers the given iterator with the iterator graph. Does not create an
   * edge.
   * @param iterator A non-null iterator.
   * @throws IllegalArgumentException if the iterator was null.
   */
  public void register(final TimeSeriesIterator<?> iterator) {
    register(iterator, null);
  }
  
  /**
   * Registers the given iterator with the iterator graph. If child is not null
   * then it creates a DAG edge from the iterator to the child.
   * @param iterator A non-null iterator.
   * @param child An optional child iterator to register and create a DAG
   * edge to.
   * @throws IllegalArgumentException if the iterator was null.
   * @throws IllegalStateException if the edge for the iterator to child would
   * generate a cycle.
   */
  public void register(final TimeSeriesIterator<?> iterator, 
      final TimeSeriesIterator<?> child) {
    if (iterator == null) {
      throw new IllegalArgumentException("Iterator cannot be null.");
    }
    iterator_graph.addVertex(iterator);
    if (child != null) {
      iterator_graph.addVertex(child);
      try {
        iterator_graph.addDagEdge(iterator, child);
      } catch (CycleFoundException e) {
        throw new IllegalStateException("Iterator cycle detected", e);
      }
    }
    
    // recalculate the sinks
    final Iterator<TimeSeriesIterator<?>> sink_iterator = iterator_sinks.iterator();
    while (sink_iterator.hasNext()) {
      if (!(iterator_graph.incomingEdgesOf(sink_iterator.next())).isEmpty()) {
        sink_iterator.remove();
      }
    }
    if (iterator_graph.incomingEdgesOf(iterator).isEmpty()) {
      iterator_sinks.add(iterator);
    }
  }
  
  /**
   * TODO
   * @param it
   */
  public void unregister(final TimeSeriesIterator<?> it) {
    throw new UnsupportedOperationException("Not implemented yet.");
  }
  
  /**
   * Registers the processor with the processor graph. Does not create an edge.
   * @param processor A non-null processor.
   * @throws IllegalArgumentException if the processor was null.
   */
  public void register(final TimeSeriesProcessor processor) {
    register(processor, null);
  }
  
  /**
   * Registers the processor with the processor graph. If a child is provided,
   * creates a DAG edge from the processor to the child.
   * @param processor A non-null processor.
   * @param child An optional child processor to register and create a DAG edge
   * to.
   * @throws IllegalArgumentException if the processor was null.
   * @throws IllegalStateException if the edge for the processor to child would
   * generate a cycle.
   */
  public void register(final TimeSeriesProcessor processor, 
      final TimeSeriesProcessor child) {
    if (processor == null) {
      throw new IllegalArgumentException("Processor cannot be null.");
    }
    processor_graph.addVertex(processor);
    if (child != null) {
      processor_graph.addVertex(child);
      try {
        processor_graph.addDagEdge(processor, child);
      } catch (CycleFoundException e) {
        throw new IllegalStateException("Processor cycle detected", e);
      }
    }
    
    final Iterator<TimeSeriesProcessor> iterator = processor_sinks.iterator();
    while (iterator.hasNext()) {
      if (!(processor_graph.incomingEdgesOf(iterator.next())).isEmpty()) {
        iterator.remove();
      }
    }
    if (processor_graph.incomingEdgesOf(processor).isEmpty()) {
      processor_sinks.add(processor);
    }
  }
  
  /**
   * TODO
   * @param processor
   */
  public void unregister(final TimeSeriesProcessor processor) {
    throw new UnsupportedOperationException("Not implemented yet.");
  }
  
  /** @return The parent of this context if it has one. May be null. */
  public QueryContext getParent() {
    return parent;
  }

  /** @return An unmodifiable list of exceptions thrown during operation. May 
   * be empty. */
  public List<Exception> getExceptions() {
    return exceptions != null ? Collections.unmodifiableList(exceptions) :
      Collections.<Exception>emptyList();
  }
  
  /**
   * Splits the context at the given processor, assigning the new context to
   * the processor and all of it's outgoing processors.
   * <b>Note:</b> The current context still maintains the graph of processors
   * and iterators despite the split. It's simply that the new iterators will
   * update the status of the given context instead of this one.
   * 
   * TODO - we need to validate that split processors do NOT have a connection
   * to non-split processors or there will be time sync issues.
   * 
   * @param context A context to associate the processors with.
   * @param processor A non-null processor to find in the graph.
   */
  public void splitContext(final QueryContext context, 
      final TimeSeriesProcessor processor) {
    if (processor == null) {
      throw new IllegalArgumentException("Processor cannot be null.");
    }
    if (!processor_graph.containsVertex(processor)) {
      throw new IllegalArgumentException("Processor was not a part of this graph.");
    }
    context.parent = this;
    if (children == null) {
      children = Lists.newArrayListWithExpectedSize(1);
    }
    children.add(context);
    context.processor_sinks.add(processor);
    context_graph.addVertex(context);
    try {
      context_graph.addDagEdge(this, context);
    } catch (CycleFoundException e) {
      throw new IllegalStateException("Unexpected cycle found while "
          + "splitting the graph.", e);
    }
    processor.setContext(context);
    context.register(processor);
    
    final Set<DefaultEdge> outgoing = processor_graph.outgoingEdgesOf(processor);
    for (final DefaultEdge edge : outgoing) {
      final TimeSeriesProcessor child = processor_graph.getEdgeTarget(edge);
      child.setContext(context);
      context.register(processor, child);
      recursivelySplit(context, child);
    }
  }
  
  /**
   * If the service is configured to execute queries remotely, returns the
   * remote context used for querying external systems. Otherwise returns null.
   * @return A remote context or null.
   */
  public abstract RemoteContext getRemoteContext();
  
  /**
   * Return a non-null timer for scheduling timer related tasks.
   * @return A non-null timer.
   */
  public abstract Timer getTimer();
  
  /** @return The Tracer to use as a component of this query. 
   * <b>WARNING:</b> This may be null if tracing is disabled. */
  public Tracer getTracer() {
    return tracer;
  }
  
  /** @return The TSDB this context is owned by. */
  public TSDB getTSDB() {
    return tsdb;
  }
  
  public QueryExecutorContext getQueryExecutorContext() {
    return executor_context;
  }
  
  /**
   * Utility method to traverse all outgoing connections of the processor and
   * assign their context to the new context.
   * @param context A new context to assign the processors to.
   * @param processor The current processor to parse for outgoing connections.
   */
  private void recursivelySplit(final QueryContext context, 
      final TimeSeriesProcessor processor) {
    final Set<DefaultEdge> outgoing = processor_graph.outgoingEdgesOf(processor);
    for (final DefaultEdge edge : outgoing) {
      final TimeSeriesProcessor child = processor_graph.getEdgeTarget(edge);
      child.setContext(context);
      context.register(processor, child);
      recursivelySplit(context, child);
    }
  }

  /**
   * Helper to load exceptions for later serialization or debugging.
   * @param e A non-null exception.
   */
  private void handleException(final Exception e) {
    if (exceptions == null) {
      exceptions = Lists.newArrayListWithExpectedSize(1);
    }
    exceptions.add(e);
    if (parent != null) {
      parent.status = IteratorStatus.EXCEPTION;
      parent.updateContext(IteratorStatus.EXCEPTION, null);
    } else {
      status = IteratorStatus.EXCEPTION;
      updateContext(IteratorStatus.EXCEPTION, null);
    }
  }
}
