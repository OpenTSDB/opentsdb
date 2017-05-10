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

/**
 * Simple default class for registering a Query planner factory.
 *
 * @param <T> The type of data returned by the planner in case it modifies
 * results.
 * 
 * @since 3.0
 */
public class DefaultQueryPlannerFactory<T> extends QueryPlannnerFactory<T> {

  /**
   * Default ctor.
   * @param ctor A non-null constructor.
   * @param type A non-null type handled by the planner.
   * @param id A non-null and non-empty Id for the planner.
   */
  public DefaultQueryPlannerFactory(final Constructor<QueryPlanner<?>> ctor,
                                 final Class<?> type,
                                 final String id) {
    super(ctor, type, id);
  }

}
