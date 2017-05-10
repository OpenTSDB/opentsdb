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
package net.opentsdb.query.plan;

import java.lang.reflect.Constructor;

import com.google.common.base.Strings;
import com.google.common.reflect.TypeToken;
import net.opentsdb.query.pojo.TimeSeriesQuery;

/**
 * A query planner factory.
 *
 * @param <T> The type of data returned by the planner in case it modifies
 * results.
 * 
 * @since 3.0
 */
public abstract class QueryPlannnerFactory<T> {

  /** The type this plan handles. */
  protected final TypeToken<?> type;
  
  /** The constructor to use. */
  protected final Constructor<QueryPlanner<?>> ctor;
  
  /** A unique identifier for the factory within the context. */
  protected final String id;
  
  /**
   * Default ctor
   * @param ctor A non-null ctor with the parameter {@link TimeSeriesQuery}.
   * @param type The type of data returned by the executor.
   * @param id An ID for the executor factory.
   * @throws IllegalArgumentException if the ctor was null or did not have
   * the proper parameters.
   */
  public QueryPlannnerFactory(final Constructor<QueryPlanner<?>> ctor,
                          final Class<?> type,
                          final String id) {
    if (ctor == null) {
      throw new IllegalArgumentException("Constructor cannot be null.");
    }
    if (ctor.getParameterCount() != 1) {
      throw new IllegalArgumentException("Constructor can only have one type: " 
          + ctor.getParameterCount());
    }
    if (ctor.getGenericParameterTypes()[0] != TimeSeriesQuery.class) {
      throw new IllegalArgumentException("First constructor parameter must be "
          + "a TimeSeriesQuery: " + 
          ctor.getGenericParameterTypes()[0].getTypeName());
    }
    if (Strings.isNullOrEmpty(id)) {
      throw new IllegalArgumentException("ID cannot be null.");
    }
    this.ctor = ctor;
    this.id = id;
    this.type = TypeToken.of(type);
  }
  
  /**
   * Returns a new instance of the query planer.
   * @param query a non-null query.
   * @return An instantiated planner if successful.
   * @throws IllegalArgumentException if the node was null or the node ID was 
   * null or empty.
   * @throws IllegalStateException if the instantiation failed.
   */
  @SuppressWarnings("unchecked")
  public QueryPlanner<T> newPlanner(final TimeSeriesQuery query) {
    if (query == null) {
      throw new IllegalArgumentException("Query cannot be null.");
    }
    try {
      return (QueryPlanner<T>) ctor.newInstance(query);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to instaniate planner for: " 
          + ctor, e);
    }
  }

  /** @return The ID of the executor instantiated by this factory. */
  public String id() {
    return id;
  }
  
  /** @return The type of executor instantiated. */
  public TypeToken<?> type() {
    return type;
  }
  
}
