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
