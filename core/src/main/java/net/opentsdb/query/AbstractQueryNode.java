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
package net.opentsdb.query;

import java.util.Collection;

/**
 * A base class for nodes that holds a link to the context, upstream and 
 * downstream nodes.
 * 
 * @since 3.0
 */
public abstract class AbstractQueryNode implements QueryNode {
  /** A reference to the query node factory that generated this node. */
  protected QueryNodeFactory factory;
  
  /** The pipeline context. */
  protected QueryPipelineContext context;
  
  /** The upstream query nodes. */
  protected Collection<QueryNode> upstream;
  
  /** The downstream query nodes. */
  protected Collection<QueryNode> downstream;
  
  /**
   * The default ctor.
   * @param factory A non-null factory to generate iterators from.
   * @param context A non-null query context.
   * @throws IllegalArgumentException if the context was null.
   */
  public AbstractQueryNode(final QueryNodeFactory factory,
                           final QueryPipelineContext context) {
    if (factory == null) {
      throw new IllegalArgumentException("Factory cannot be null.");
    }
    if (context == null) {
      throw new IllegalArgumentException("Context cannot be null.");
    }
    this.factory = factory;
    this.context = context;
  }
  
  @Override
  public void initialize() {
    upstream = context.upstream(this);
    downstream = context.downstream(this);
  }
  
  @Override
  public QueryPipelineContext pipelineContext() {
    return context;
  }

  public QueryNodeFactory factory() {
    return factory;
  }
}