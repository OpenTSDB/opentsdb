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
package net.opentsdb.query.execution;

import java.lang.reflect.Constructor;

import net.opentsdb.query.context.QueryContext;

/**
 * A factory used to generate a {@link QueryExecutor} for a new context. These
 * factories can be instantiated once per JVM and the 
 * {@link #newExecutor(QueryContext)} method must be thread safe.
 * <p>
 * The constructor must have only the following parameters:
 * Ctor({@link QueryContext}, {@link QueryExecutorConfig}); 
 * 
 * @param <T> The type of data returned by the executor.
 * 
 * @since 3.0
 */
public abstract class QueryExecutorFactory<T> {

  /** The constructor to use. */
  protected final Constructor<QueryExecutor<?>> ctor;
  
  /** The config to pass to each instance. */
  protected final QueryExecutorConfig config;
  
  /**
   * Default ctor
   * @param ctor A non-null ctor with the parameters {@link QueryContext} and
   * {@link QueryExecutorConfig}.
   * @param config An optional config. May be null if the executor does not
   * require a config.
   * @throws IllegalArgumentException if the ctor was null or did not have
   * the proper parameters.
   */
  public QueryExecutorFactory(final Constructor<QueryExecutor<?>> ctor, 
                              final QueryExecutorConfig config) {
    if (ctor == null) {
      throw new IllegalArgumentException("Constructor cannot be null.");
    }
    if (ctor.getParameterCount() != 2) {
      throw new IllegalArgumentException("Constructor can only have two types: " 
          + ctor.getParameterCount());
    }
    if (ctor.getGenericParameterTypes()[0] != QueryContext.class) {
      throw new IllegalArgumentException("First constructor parameter must be "
          + "a QueryContext: " + ctor.getGenericParameterTypes()[0].getTypeName());
    }
    if (ctor.getGenericParameterTypes()[1] != QueryExecutorConfig.class) {
      throw new IllegalArgumentException("First constructor parameter must be "
          + "a QueryExecutorConfig: " 
          + ctor.getGenericParameterTypes()[1].getTypeName());
    }
    this.ctor = ctor;
    this.config = config;
  }
  
  /**
   * Returns a new instance of a query executor that handles the specified
   * data type.
   * @param context A non-null context to pass to the executor's ctor.
   * @return A non-null query executor.
   * @throws IllegalStateException if the instance couldn't be constructed.
   */
  @SuppressWarnings("unchecked")
  public QueryExecutor<T> newExecutor(final QueryContext context) {
    try {
      return (QueryExecutor<T>) ctor.newInstance(context, config);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to instaniate executor for: " 
          + ctor, e);
    }
  }
}
