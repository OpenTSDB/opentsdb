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
package net.opentsdb.query.execution;

import java.lang.reflect.Constructor;

import com.google.common.base.Strings;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.DefaultRegistry;
import net.opentsdb.core.TSDB;
import net.opentsdb.query.execution.graph.ExecutionGraphNode;

/**
 * Simple {@link QueryExecutorFactory} that takes the ctor and config.
 * 
 * @param <T> The type of data returned by the executor.
 * 
 * @since 3.0
 */
public class DefaultQueryExecutorFactory<T> extends QueryExecutorFactory<T> {

  /** The type this executor handles. */
  protected final TypeToken<?> type;
  
  /** The constructor to use. */
  protected final Constructor<QueryExecutor<?>> ctor;

  /** A unique identifier for the factory within the context. */
  protected final String id;
  
  /**
   * DO NOT USE: only present to satisfy plugin instantiation. Use 
   * {@link #DefaultQueryExecutorFactory(Constructor, Class, String)}.
   */
  public DefaultQueryExecutorFactory() {
    type = null;
    ctor = null;
    id = null;
  }
  
  /**
   * Default ctor
   * @param ctor A non-null constructor to use when instantiating the executor.
   * @param type The type of data returned by the executor.
   * @param id a non-null ID for the factory.
   * @throws IllegalArgumentException if the ctor was null or did not have
   * the proper parameters.
   */
  public DefaultQueryExecutorFactory(final Constructor<QueryExecutor<?>> ctor,
                                     final Class<?> type,  
                                     final String id) {
    if (ctor == null) {
      throw new IllegalArgumentException("Constructor cannot be null.");
    }
    if (ctor.getParameterCount() != 1) {
      throw new IllegalArgumentException("Constructor can only have one type: " 
          + ctor.getParameterCount());
    }
    if (ctor.getGenericParameterTypes()[0] != ExecutionGraphNode.class) {
      throw new IllegalArgumentException("First constructor parameter must be "
          + "a ExecutionGraphNode: " + 
          ctor.getGenericParameterTypes()[0].getTypeName());
    }
    if (Strings.isNullOrEmpty(id)) {
      throw new IllegalArgumentException("ID cannot be null.");
    }
    this.ctor = ctor;
    this.id = id;
    this.type = TypeToken.of(type);
  }
  
  @Override
  public Deferred<Object> initialize(final TSDB tsdb) {
    try {
      if (!Strings.isNullOrEmpty(id)) {
        ((DefaultRegistry) tsdb.getRegistry()).registerFactory(this);
      }
    } catch (Exception e) {
      return Deferred.fromError(e);
    }
    return Deferred.fromResult(null);
  }
  
  @Override
  public String version() {
    return "3.0.0";
  }
  
  /** @return The ID of the executor instantiated by this factory. */
  public String id() {
    return id;
  }
  
  /** @return The type of executor instantiated. */
  public TypeToken<?> type() {
    return type;
  }
  
  /**
   * Returns a new instance of the executor using the config from the
   * graph node.
   * @param node A non-null node.
   * @return An instantiated executor if successful.
   * @throws IllegalArgumentException if the node was null or the node ID was 
   * null or empty.
   * @throws IllegalStateException if the instantiation failed.
   */
  @SuppressWarnings("unchecked")
  public QueryExecutor<T> newExecutor(final ExecutionGraphNode node) {
    if (node == null) {
      throw new IllegalArgumentException("Node cannot be null.");
    }
    if (Strings.isNullOrEmpty(node.getId())) {
      throw new IllegalArgumentException("Node ID cannot be null.");
    }
    try {
      return (QueryExecutor<T>) ctor.newInstance(node);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to instaniate executor for: " 
          + ctor, e);
    }
  }

}
