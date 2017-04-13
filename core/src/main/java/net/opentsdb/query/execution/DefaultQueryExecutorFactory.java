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

/**
 * Simple {@link QueryExecutorFactory} that takes the ctor and config.
 * 
 * @param <T> The type of data returned by the executor.
 * 
 * @since 3.0
 */
public class DefaultQueryExecutorFactory<T> extends QueryExecutorFactory<T> {

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
    super(ctor, type, id);
  }
  
}
