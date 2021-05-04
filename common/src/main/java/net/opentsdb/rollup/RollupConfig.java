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

import net.opentsdb.exceptions.IllegalDataException;

import java.util.List;
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
  
  /**
   * An ordered set of resolutions supported by this source starting with the 
   * lowest resolution working towards the highest.
   * @return A non-null list of intervals in TSDB Duration formation, e.g. "1h"
   */
  public List<String> getIntervals();
  
  /**
   * Returns a list of applicable intervals given a downsample interval. If no
   * intervals apply then it will return an empty list.
   * @param interval A non-null and non-empty string.
   * @return A non-null, possibly empty list of intervals from lowest resolution
   * to highest.
   */
  public List<String> getPossibleIntervals(final String interval);

  /**
   * Fetches the RollupInterval corresponding to the forward interval string map
   * @param interval The interval to lookup
   * @return The RollupInterval object configured for the given interval
   * @throws IllegalArgumentException if the interval is null or empty
   * @throws NoSuchRollupForIntervalException if the interval was not configured
   */
  public RollupInterval getRollupInterval(final String interval);

  /**
   * @return The non-null default interval.
   */
  public RollupInterval getDefaultInterval();

  /**
   * Converts the old 2.x style qualifiers from {@code sum:<offset>} to
   * the assigned ID.
   * @param qualifier A non-null qualifier of at least 6 bytes.
   * @return The mapped ID if configured or IllegalArgumentException if
   * the mapping wasn't found.
   * @throws IllegalArgumentException if the qualifier as null, less than
   * 6 bytes or the aggregation was not assigned an ID.
   * @throws IllegalDataException if the aggregation was unrecognized.
   */
  public int getIdForAggregator(final byte[] qualifier);

  /**
   * Returns the index of the time offset in the qualifier given an
   * older 2.x style rollup of the form {@code sum:<offset>}.
   * @param qualifier A non-null qualifier of at least 6 bytes.
   * @return A 0 based index in the array when the offset begins.
   * @throws IllegalArgumentException if the qualifier as null or less
   * than 6 bytes
   * @throws IllegalDataException if the aggregation was unrecognized.
   */
  public int getOffsetStartFromQualifier(final byte[] qualifier);
}
