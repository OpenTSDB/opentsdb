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

import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Deferred;

import net.opentsdb.data.PartialTimeSeries;
import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.exceptions.QueryUpstreamException;
import net.opentsdb.stats.Span;

/**
 * A base class for nodes that holds a link to the context, upstream and 
 * downstream nodes.
 * 
 * @since 3.0
 */
public abstract class AbstractQueryNode implements QueryNode {
  private static final Logger LOG = 
      LoggerFactory.getLogger(AbstractQueryNode.class);
  
  /** Return if nothing happens in the {@link #initialize(Span)} method
   * asynchronously. */
  protected static final Deferred<Void> INITIALIZED = 
      Deferred.fromResult(null);
  
  /** A reference to the query node factory that generated this node. */
  protected QueryNodeFactory factory;
  
  /** The pipeline context. */
  protected QueryPipelineContext context;
  
  /** The upstream query nodes. */
  protected Collection<QueryNode> upstream;
  
  /** The downstream query nodes. */
  protected Collection<QueryNode> downstream;
  
  /** The downstream source nodes. */
  protected Collection<TimeSeriesDataSource> downstream_sources;
  
  /**
   * The default ctor.
   * @param factory A non-null factory to generate iterators from.
   * @param context A non-null query context.
   * @throws IllegalArgumentException if the context was null.
   */
  public AbstractQueryNode(final QueryNodeFactory factory,
                           final QueryPipelineContext context) {
    if (context == null) {
      throw new IllegalArgumentException("Context cannot be null.");
    }
    this.factory = factory;
    this.context = context;
  }
  
  @Override
  public Deferred<Void> initialize(final Span span) {
    final Span child;
    if (span != null) {
      child = span.newChild(getClass() + ".initialize()").start();
    } else {
      child = null;
    }
    upstream = context.upstream(this);
    downstream = context.downstream(this);
    downstream_sources = context.downstreamSources(this);
    if (child != null) {
      child.setSuccessTags().finish();
    }
    return INITIALIZED;
  }
  
  @Override
  public QueryPipelineContext pipelineContext() {
    return context;
  }

  public QueryNodeFactory factory() {
    return factory;
  }

  @Override
  public void onComplete(final QueryNode downstream, 
                         final long final_sequence,
                         final long total_sequences) {
    for (final QueryNode us : upstream) {
      try {
        us.onComplete(downstream == null ? this : downstream, 
            final_sequence, total_sequences);
      } catch (Exception e) {
        LOG.error("Failed to call upstream onComplete on Node: " + us, e);
      }
    }
  }
  
  @Override
  public void onNext(final PartialTimeSeries next) {
    throw new IllegalStateException("The node has not implemented this yet");
  }
  
  @Override
  public void onError(final Throwable t) {
    for (final QueryNode us : upstream) {
      try {
        us.onError(t);
      } catch (Exception e) {
        LOG.error("Failed to call upstream onError on Node: " + us, e);
      }
    }
  }
  
  /**
   * Calls {@link #fetchNext(Span)} on all of the downstream nodes.
   * @param span An optional tracing span.
   */
  protected void fetchDownstream(final Span span) {
    for (final TimeSeriesDataSource source : downstream_sources) {
      source.fetchNext(span);
    }
  }
  
  /**
   * Sends the result to each of the upstream subscribers.
   * 
   * @param result A non-null result.
   * @throws QueryUpstreamException if the upstream 
   * {@link #onNext(QueryResult)} handler throws an exception. I hate
   * checked exceptions but each node needs to be able to handle this
   * ideally by cancelling the query.
   * @throws IllegalArgumentException if the result was null.
   */
  protected void sendUpstream(final QueryResult result) 
        throws QueryUpstreamException {
    if (result == null) {
      throw new QueryUpstreamException("Result cannot be null.");
    }
    
    for (final QueryNode node : upstream) {
      try {
        node.onNext(result);
      } catch (Exception e) {
        throw new QueryUpstreamException("Failed to send results "
            + "upstream to node: " + node, e);
      }
    }
  }
  
  /**
   * Sends the series upstream to every node that's linked. If an upstream node
   * throws an exception, it's caught and thrown as a 
   * {@link QueryUpstreamException}.
   * 
   * @param series A non-null series to send upstream.
   * @throws QueryUpstreamException If the series was null or an upstream node
   * threw an exception.
   */
  protected void sendUpstream(final PartialTimeSeries series) 
      throws QueryUpstreamException {
    if (series == null) {
      throw new QueryUpstreamException("Series cannot be null.");
    }
    
    for (final QueryNode node : upstream) {
      try {
        node.onNext(series);
      } catch (Exception e) {
        throw new QueryUpstreamException("Failed to send series "
            + "upstream to node: " + node, e);
      }
    }
  }
  
  /**
   * Sends the throwable upstream to each of the subscribing nodes. If 
   * one or more upstream consumers throw an exception, it's caught and
   * logged as a warning.
   * 
   * @param t A non-null throwable.
   * @throws IllegalArgumentException if the throwable was null.
   */
  protected void sendUpstream(final Throwable t) {
    if (t == null) {
      throw new IllegalArgumentException("Throwable cannot be null.");
    }
    
    for (final QueryNode node : upstream) {
      try {
        node.onError(t);
      } catch (Exception e) {
        LOG.warn("Failed to send exception upstream to node: " + node, e);
      }
    }
  }
  
  /**
   * Passes the sequence info upstream to all subscribers. If one or 
   * more upstream consumers throw an exception, it's caught and logged 
   * as a warning.
   * 
   * @param final_sequence The final sequence number to pass.
   * @param total_sequences The total sequence count to pass.
   */
  protected void completeUpstream(final long final_sequence,
                                  final long total_sequences) {
    for (final QueryNode node : upstream) {
      try {
        node.onComplete(this, final_sequence, total_sequences);
      } catch (Exception e) {
        LOG.warn("Failed to mark upstream node complete: " + node, e);
      }
    }
  }
  
}
