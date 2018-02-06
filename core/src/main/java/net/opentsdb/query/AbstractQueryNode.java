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
