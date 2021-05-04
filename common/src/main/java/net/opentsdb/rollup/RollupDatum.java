// This file is part of OpenTSDB.
// Copyright (C) 2021  The OpenTSDB Authors.
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
package net.opentsdb.rollup;

import net.opentsdb.data.TimeSeriesDatum;

/**
 * A rollup data point.
 *
 * @since 3.0
 */
public interface RollupDatum extends TimeSeriesDatum {

  /** @return The optional interval string. This is only used for time-based
   * rollup values.
   */
  public String intervalString();

  /** @return The non-null aggregation string. */
  public String groupByAggregatorString();

  /** @return The optional numeric ID of the group aggregation from the rollup
   * configuration. -1 if not set.
   */
  public int groupByAggregator();

}
