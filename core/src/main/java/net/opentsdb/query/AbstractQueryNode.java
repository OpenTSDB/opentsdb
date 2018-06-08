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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

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
  
  /** A reference to the query node factory that generated this node. */
  protected QueryNodeFactory factory;
  
  /** The name of this node. */
  protected final String id;
  
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
   * @param id The ID of this node.
   * @throws IllegalArgumentException if the context was null.
   */
  public AbstractQueryNode(final QueryNodeFactory factory,
                           final QueryPipelineContext context,
                           final String id) {
//    if (factory == null) {
//      throw new IllegalArgumentException("Factory cannot be null.");
//    }
    if (context == null) {
      throw new IllegalArgumentException("Context cannot be null.");
    }
    this.factory = factory;
    this.context = context;
    this.id = id;
  }
  
  @Override
  public void initialize(final Span span) {
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
  }

  @Override
  public String id() {
    return id;
  }
  
  @Override
  public QueryPipelineContext pipelineContext() {
    return context;
  }

  public QueryNodeFactory factory() {
    return factory;
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
      throw new IllegalArgumentException("Result cannot be null.");
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

  @Override
  public boolean equals(final Object obj) {
    if (obj == null) {
      return false;
    }
    if (!obj.getClass().equals(this.getClass())) {
      return false;
    }
    
    final AbstractQueryNode other = (AbstractQueryNode) obj;
    if (Strings.isNullOrEmpty(id) && !Strings.isNullOrEmpty(other.id)) {
      return false;
    }
    if (!Strings.isNullOrEmpty(id) && Strings.isNullOrEmpty(other.id)) {
      return false;
    }
    if (Strings.isNullOrEmpty(id) && Strings.isNullOrEmpty(other.id)) {
      return true;
    }
    return id.equals(other.id);
  }
  
  @Override
  public int hashCode() {
    return (getClass().getCanonicalName() + id).hashCode();
  }
}
