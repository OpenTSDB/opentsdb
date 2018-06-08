// This file is part of OpenTSDB.
// Copyright (C) 2018 The OpenTSDB Authors.
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

import java.util.Map;

public interface RollupConfig {

  /** @return The immutable map of aggregations to IDs for serialization. */
  public Map<String, Integer> getAggregationIds();
  
  /**
   * @param id The ID of an aggregator to search for.
   * @return The aggregator if found, null if it was not mapped.
   */
  public String getAggregatorForId(final int id);
  
  /**
   * @param aggregator The non-null and non-empty aggregator to search for.
   * @return The ID of the aggregator if found.
   * @throws IllegalArgumentException if the aggregator was not found or if the
   * aggregator was null or empty.
   */
  public int getIdForAggregator(final String aggregator);
  
}
