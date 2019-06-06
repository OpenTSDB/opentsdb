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
package net.opentsdb.query;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.graph.Traverser;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.data.PartialTimeSeries;
import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.query.plan.DefaultQueryPlanner;
import net.opentsdb.stats.Span;

/**
 * A useful base class for {@link QueryPipelineContext}s that stores references
 * to the TSDB, query, graph, roots and sinks.
 * <b>Warning:</b> Don't forgot to add an edge from this to the root of the 
 * query nodes in your graph or the query results won't make it to the sinks.
 * 
 * TODO - need to handle the case where the sources fail to call on complete.
 * 
 * TODO - assumptions made: All query results in the SINGLE mode will have the 
 * same timespecs. This may not be the case.
 * 
 * <b>Invariants:</b>
 * <ul>
 * <li>Each {@link ExecutionGraphNode} must have a unique ID within the plan.graph().</li>
 * <li>The graph must have at most one sink that will be used to execution a
 * query.</li>
 * <li>The graph must be a non-cyclical DAG.</li>
 * </ul>
 * 
 * @since 3.0
 */
public abstract class AbstractQueryPipelineContext implements QueryPipelineContext, QuerySinkCallback {
  private static final Logger LOG = LoggerFactory.getLogger(
      AbstractQueryPipelineContext.class);
  
  /** The list of sink nodes. */
  protected final List<QuerySink> sinks;
  
  /** A list of query results expected per type so we can call close on
   * sinks when we've passed them through. */
  protected final Map<String, AtomicInteger> countdowns;
  
  /** The upstream query context this pipeline context belongs to. */
  protected final QueryContext context;

  /** The query plan. */
  protected final DefaultQueryPlanner plan;
  
  /** Used to iterate over sources when in a client streaming mode. */
  protected int source_idx = 0;
  
  /** A map of source IDs to maps of set timestamps to time series counters.
   * i.e. <source_id, <timestamp, time series counter>> */
  protected Map<String, Map<Long, PartialTimeSetWrapper>> pts;
  
  /** A map of source IDs to counters of sets. Indicates when a source ID has
   * sent all of it's sets. */
  protected Map<String, AtomicInteger> finished_sources;
  
  /** A counter of completed sets. When this matches the plan sources we're done. */
  protected AtomicInteger total_finished;
  
  /**
   * Default ctor.
   * @param context The user's query context.
   * @throws IllegalArgumentException if any argument was null.
   */
  public AbstractQueryPipelineContext(final QueryContext context) {
    if (context == null) {
      throw new IllegalArgumentException("The context cannot be null.");
    }
    this.context = context;
    plan = new DefaultQueryPlanner(this, (QueryNode) this);
    sinks = Lists.newArrayListWithExpectedSize(1);
    countdowns = Maps.newHashMap();
    pts = Maps.newConcurrentMap();
    finished_sources = Maps.newConcurrentMap();
    total_finished = new AtomicInteger();
  }
  
  @Override
  public TSDB tsdb() {
    return context.tsdb();
  }
  
  @Override
  public QueryContext queryContext() {
    return context;
  }

  @Override
  public QueryPipelineContext pipelineContext() {
    return this;
  }
  
  @Override
  public Collection<QueryNode> upstream(final QueryNode node) {
    if (node == null) {
      throw new IllegalArgumentException("Node cannot be null.");
    }
    final Set<QueryNode> upstream = plan.graph().predecessors(node);
    if (upstream.isEmpty()) {
      return Collections.emptyList();
    }
    final List<QueryNode> listeners = Lists.newArrayListWithCapacity(
        upstream.size());
    for (final QueryNode e : upstream) {
      listeners.add(e);
    }
    return listeners;
  }
  
  @Override
  public Collection<QueryNode> downstream(final QueryNode node) {
    if (node == null) {
      throw new IllegalArgumentException("Node cannot be null.");
    }
    final Set<QueryNode> downstream = plan.graph().successors(node);
    if (downstream.isEmpty()) {
      return Collections.emptyList();
    }
    final List<QueryNode> downstreams = Lists.newArrayListWithCapacity(
        downstream.size());
    for (final QueryNode n : downstream) {
      downstreams.add(n);
    }
    return downstreams;
  }
  
  @Override
  public Collection<TimeSeriesDataSource> downstreamSources(final QueryNode node) {
    if (node == null) {
      throw new IllegalArgumentException("Node cannot be null.");
    }
    final Set<QueryNode> downstream = plan.graph().successors(node);
    if (downstream.isEmpty()) {
      return Collections.emptyList();
    }
    final Set<TimeSeriesDataSource> downstreams = Sets.newHashSetWithExpectedSize(
        downstream.size());
    for (final QueryNode n : downstream) {
      if (downstreams.contains(n)) {
        continue;
      }
      
      if (n instanceof TimeSeriesDataSource) {
        downstreams.add((TimeSeriesDataSource) n);
      } else {
        downstreams.addAll(downstreamSources(n));
      }
    }
    return downstreams;
  }
  
  @Override
  public Collection<String> downstreamSourcesIds(final QueryNode node) {
    if (node == null) {
      throw new IllegalArgumentException("Node cannot be null.");
    }
    
    Collection<TimeSeriesDataSource> downstreams = downstreamSources(node);
    final Set<String> ids = Sets.newHashSetWithExpectedSize(downstreams.size());
    for (final QueryNode downstream : downstreams) {
      ids.add(downstream.config().getId());
    }
    return ids;
  }
  
  @Override
  public Collection<QueryNode> upstreamOfType(final QueryNode node, 
                                              final Class<? extends QueryNode> type) {
    if (node == null) {
      throw new IllegalArgumentException("Node cannot be null.");
    }
    if (type == null) {
      throw new IllegalArgumentException("Type cannot be null.");
    }
    
    final Set<QueryNode> upstream = plan.graph().predecessors(node);
    if (upstream.isEmpty()) {
      return Collections.emptyList();
    }
    
    List<QueryNode> upstreams = null;
    for (final QueryNode source : upstream) {
      if (source.getClass().equals(type)) {
        if (upstreams == null) {
          upstreams = Lists.newArrayList();
        }
        upstreams.add(source);
      } else {
        final Collection<QueryNode> upstream_of_source = 
            upstreamOfType(source, type);
        if (!upstream_of_source.isEmpty()) {
          if (upstreams == null) {
            upstreams = Lists.newArrayList();
          }
          upstreams.addAll(upstream_of_source);
        }
      }
    }
    return upstreams == null ? Collections.emptyList() : upstreams;
  }
  
  @Override
  public Collection<QueryNode> downstreamOfType(final QueryNode node, 
                                                final Class<? extends QueryNode> type) {
    if (node == null) {
      throw new IllegalArgumentException("Node cannot be null.");
    }
    if (type == null) {
      throw new IllegalArgumentException("Type cannot be null.");
    }
    
    final Set<QueryNode> downstream = plan.graph().successors(node);
    if (downstream.isEmpty()) {
      return Collections.emptyList();
    }
    
    List<QueryNode> downstreams = null;
    for (final QueryNode n : downstream) {
      if (n.getClass().equals(type)) {
        if (downstreams == null) {
          downstreams = Lists.newArrayList();
        }
        downstreams.add(n);
      } else {
        final Collection<QueryNode> downstream_of_target = 
            downstreamOfType(n, type);
        if (!downstream_of_target.isEmpty()) {
          if (downstreams == null) {
            downstreams = Lists.newArrayList();
          }
          downstreams.addAll(downstream_of_target);
        }
      }
    }
    return downstreams == null ? Collections.emptyList() : downstreams;
  }
  
  @Override
  public Collection<QuerySink> sinks() {
    return sinks;
  }
  
  @Override
  public TimeSeriesQuery query() {
    return context.query();
  }
  
  @Override
  public void close() {
    if (plan != null && !plan.graph().edges().isEmpty()) {
      Traverser<QueryNode> traverser = Traverser.forGraph(plan.graph());
      for (final QueryNode node : traverser.breadthFirst(this)) {
        if (node == this) {
          continue;
        }
        try {
          node.close();
        } catch (Exception e) {
          LOG.warn("Failed to close query node: " + node, e);
        }
      }
    }
  }
  
  @Override
  public void fetchNext(final Span span) {
    context.tsdb().getQueryThreadPool().submit(new Runnable() {
      @Override
      public void run() {
        if (context.mode() == QueryMode.SINGLE ||
            context.mode() == QueryMode.BOUNDED_SERVER_SYNC_STREAM || 
            context.mode() == QueryMode.CONTINOUS_SERVER_SYNC_STREAM ||
            context.mode() == QueryMode.BOUNDED_SERVER_ASYNC_STREAM ||
            context.mode() == QueryMode.CONTINOUS_SERVER_ASYNC_STREAM) {
          for (final TimeSeriesDataSource source : plan.sources()) {
            try {
              source.fetchNext(span);
            } catch (Exception e) {
              LOG.error("Failed to fetch next from source: " 
                  + source, e);
              onError(e);
              break;
            }
          }
          return;
        }
        
        synchronized(this) {
          if (source_idx >= plan.sources().size()) {
            source_idx = 0;
          }
          try {
            plan.sources().get(source_idx++).fetchNext(span);
          } catch (Exception e) {
            LOG.error("Failed to fetch next from source: " 
                + plan.sources().get(source_idx - 1), e);
            onError(e);
          }
        }
      }
    });
  }
  
  @Override
  public void onComplete(final QueryNode downstream, 
                         final long final_sequence,
                         final long total_sequences) {
    // TODO - handle this with streaming.
  }
  
  @Override
  public void onNext(final QueryResult next) {
    final ResultWrapper wrapped = new ResultWrapper(next);
    for (final QuerySink sink : sinks) {
      try {
        sink.onNext(wrapped);
      } catch (Throwable e) {
        LOG.error("Exception thrown passing results to sink: " + sink, e);
        // TODO - should we kill the query here?
      }
    }
  }
  
  @Override
  public void onNext(final PartialTimeSeries series) {
    for (final QuerySink sink : sinks) {
      try {
        sink.onNext(series, this);
      } catch (Throwable t) {
        LOG.error("Failed to send data to sink: " + sink, t);
      }
    }
  }
  
  @Override
  public void onError(final Throwable t) {
    for (final QuerySink sink : sinks) {
      try {
        sink.onError(t);
      } catch (Exception e) {
        LOG.error("Exception thrown passing exception to sink: " + sink, e);
      }
    }
    // TODO - decide if we should *auto* close here.
  }
  
  @Override
  public QueryNodeConfig config() {
    return null;
  }
  
  /** @return The planner. */
  public DefaultQueryPlanner plan() {
    return plan;
  }
  
  @Override
  public void onComplete(final PartialTimeSeries series) {
    // TODO - need to track multiple sinks eventually.
    try {
      final String set_id = series.set().node().config().getId() + ":" 
          + series.set().dataSource();
      
      Map<Long, PartialTimeSetWrapper> sets = pts.get(set_id);
      if (sets == null) {
        sets = Maps.newConcurrentMap();
        Map<Long, PartialTimeSetWrapper> extant = pts.putIfAbsent(set_id, sets);
        if (extant != null) {
          sets = extant;
        }
      }
      
      PartialTimeSetWrapper wrapper = null;
      long ts = series.set().start().epoch();
      wrapper = sets.get(ts);
      if (wrapper == null) {
        wrapper = new PartialTimeSetWrapper();
        PartialTimeSetWrapper extant = sets.putIfAbsent(ts, wrapper);
        if (extant != null) {
          wrapper = extant;
        }
      }
      
      int cnt = 0;
      int max = -1;
      //synchronized (wrapper) {
        if (series.set().timeSeriesCount() > 0) {
          cnt = wrapper.counter.incrementAndGet();
        }
        if (series.set().complete()) {
          if (series.set().timeSeriesCount() > wrapper.max.get()) {
            wrapper.max.set(series.set().timeSeriesCount());
          }
        }
        max = wrapper.max.get();
      //}
      
      if (max >= 0 && max == cnt) {
        AtomicInteger ctr = finished_sources.get(set_id);
        if (ctr == null) {
          ctr = new AtomicInteger();
          AtomicInteger extant = finished_sources.putIfAbsent(set_id, ctr);
          if (extant != null) {
            ctr = extant;
          }
        }
        
        final int f = ctr.incrementAndGet();
        if (series.set().totalSets() == f) {
          final int tf = total_finished.incrementAndGet();
          if (tf == plan.serializationSources().size()) {
            for (final QuerySink sink : sinks) {
              sink.onComplete();
            }
          }
        }
      }
    } catch (Throwable t) {
      LOG.error("Unexpected exception processing PTS post Sink: " + series + "  SET: " + series.set(), t);
      onError(t);
    } finally {
      try {
        series.close();
      } catch (Exception e) {
        LOG.error("Failed to close series", e);
      }
    }
  }
  
  @Override
  public void onError(final PartialTimeSeries pts, final Throwable t) {
    
  }
  
  /**
   * A helper to initialize the nodes in depth-first order.
   */
  protected Deferred<Void> initializeGraph(final Span span) {
    final Span child;
    if (span != null) {
      child = span.newChild(getClass() + ".initializeGraph")
        .start();
    } else {
      child = null;
    }
    
    class PlanCB implements Callback<Void, Void> {
      @Override
      public Void call(final Void ignored) throws Exception {

        // setup sinks if the graph is happy
        if (context.sinkConfigs() != null) {
          for (final QuerySinkConfig config : context.sinkConfigs()) {
            final QuerySinkFactory factory = context.tsdb().getRegistry()
                .getPlugin(QuerySinkFactory.class, config.getId());
            if (factory == null) {
              throw new IllegalArgumentException("No sink factory found for: " 
                  + config.getId());
            }
            
            final QuerySink sink = factory.newSink(context, config);
            if (sink == null) {
              throw new IllegalArgumentException("Factory returned a null sink for: " 
                  + config.getId());
            }
            sinks.add(sink);
            if (sinks.size() > 1) {
              throw new UnsupportedOperationException("Only one sink allowed for now, sorry!");
            }
          }
        }
        
        for (final String source : plan.serializationSources()) {
          countdowns.put(source, new AtomicInteger(sinks.size()));
        }
        
        if (child != null) {
          child.setSuccessTags().finish();
        }
        
        return null;
      }
    }
    
    return plan.plan(child)
               .addCallback(new PlanCB());
  }
  
  /**
   * A helper to determine if the stream is finished and calls the sink's 
   * {@link QuerySink#onComplete()} method.
   * <b>NOTE:</b> This method must be synchronized.
   * @return True if complete, false if not.
   */
  protected boolean checkComplete() {
    for (final AtomicInteger integer : countdowns.values()) {
      if (integer.get() > 0) {
        return false;
      }
    }
    
    // done!
    for (final QuerySink sink : sinks) {
      try {
        sink.onComplete();
      } catch (Throwable t) {
        LOG.error("Failed to close sink: " + sink, t);
      }
    }
    return true;
  }
  
  /**
   * A simple pass-through wrapper that will decrement the proper counter
   * when the result is closed.
   */
  private class ResultWrapper extends BaseWrappedQueryResult {
    
    ResultWrapper(final QueryResult result) {
      super(result);
    }
    
    @Override
    public QueryNode source() {
      return result.source();
    }
    
    @Override
    public void close() {
      if (result.source().config() instanceof TimeSeriesDataSourceConfig ||
          result.source().config().joins()) {
        AtomicInteger cntr = countdowns.get(result.dataSource());
        if (cntr == null) {
          LOG.error("Unexpected result source, no counter for: " 
              + result.dataSource());
        } else {
          cntr.decrementAndGet();
        }
      } else {
        AtomicInteger cntr = countdowns.get(result.source().config().getId() + ":" 
            + result.dataSource());
        if (cntr == null) {
          LOG.error("Unexpected result source, noo counter for: " 
              + result.source().config().getId() + ":" 
              + result.dataSource());
        } else {
          cntr.decrementAndGet();
        }
      }
      checkComplete();
      try {
        result.close();
      } catch (Throwable t) {
        LOG.error("Failed to close result: " + result, t);
      }
    }
  }
  
  /**
   * Simple class used to keep track of a partial time series set stats.
   */
  class PartialTimeSetWrapper {
    AtomicInteger counter = new AtomicInteger();
    AtomicInteger max = new AtomicInteger(-1);
  }
}
