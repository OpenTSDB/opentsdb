// This file is part of OpenTSDB.
// Copyright (C) 2021 The OpenTSDB Authors.
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

import com.google.common.hash.HashCode;

/**
 * Holds information about a rollup interval.
 *
 * @since 2.4
 */
public interface RollupInterval {

  /** @return the string name of the temporal rollup table */
  public String getTable();

  /** @return the temporal rollup table name as a byte array */
  public byte[] getTemporalTable();

  /** @return the string name of the group by rollup table */
  public String getPreAggregationTable();

  /** @return the group by table name as a byte array */
  public byte[] getGroupbyTable();

  /** @return the configured interval as a string */
  public String getInterval();

  /** @return the character describing the span of this interval */
  public char getUnits();

  /** @return the unit multiplier */
  public int getUnitMultiplier();

  /** @return the interval units character */
  public char getIntervalUnits();

  /** @return the interval for this span in seconds */
  public int getIntervalSeconds();

  /** @return the count of intervals in this span */
  public int getIntervals();

  /**
   * Is it the default roll up interval that need to be written to default
   * tsdb data table. So if true, which means the raw cell column qualifier
   * is not encoded with the aggregate function and the cell might have been
   * compacted
   * @return true if it is default rollup interval
   */
  public boolean isDefaultInterval();

  /** @return The width of each row as an interval string. */
  public String getRowSpan();

  /** @return The rollup config this interval belongs to. */
  public RollupConfig rollupConfig();

  @Override
  public int hashCode();

  @Override
  public String toString();

  @Override
  public boolean equals(final Object obj);

  /** @return A HashCode object for deterministic, non-secure hashing */
  public HashCode buildHashCode();
}
