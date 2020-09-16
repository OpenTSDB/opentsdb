// This file is part of OpenTSDB.
// Copyright (C) 2020  The OpenTSDB Authors.
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
package net.opentsdb.query.joins;

import java.util.List;

import com.google.common.collect.Lists;

import gnu.trove.map.hash.TLongObjectHashMap;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.query.joins.JoinConfig.JoinType;
import net.opentsdb.query.joins.Joiner.Operand;

/**
 * A default implementation for the {@link BaseHashedJoinSet} that simply
 * routes a time series to the left or right map based on a string key.
 * Handles appending series to a list in the maps.
 * <p>
 * <bNote:</b> The implementation doesn't check to see if the same 
 * time series is in the left or right map.
 * 
 * @since 3.0
 */
public class TernaryKeyedHashedJoinSet extends KeyedHashedJoinSet {

  
  protected TernaryKeyedHashedJoinSet(final JoinType type, 
                                      final int expected_sets) {
    super(type, expected_sets, true);
  }
  
  @Override
  void add(final Operand operand, final long hash, final TimeSeries ts) {
    if (ts == null) {
      throw new IllegalArgumentException("Time series can't be null.");
    }
    if (operand == Operand.LEFT) {
      if (left_map == null) {
        left_map = new TLongObjectHashMap<List<TimeSeries>>();
      }
      List<TimeSeries> series = left_map.get(hash);
      if (series == null) {
        series = Lists.newArrayList();
        left_map.put(hash, series);
      }
      series.add(ts);
    } else if (operand == Operand.RIGHT) {
      if (right_map == null) {
        right_map = new TLongObjectHashMap<List<TimeSeries>>();
      }
      List<TimeSeries> series = right_map.get(hash);
      if (series == null) {
        series = Lists.newArrayList();
        right_map.put(hash, series);
      }
      series.add(ts);
    } else {
      if (condition_map == null) {
        condition_map = new TLongObjectHashMap<List<TimeSeries>>();
      }
      List<TimeSeries> series = condition_map.get(hash);
      if (series == null) {
        series = Lists.newArrayList();
        condition_map.put(hash, series);
      }
      series.add(ts);
    }
  }
  
  @Override
  public String toString() {
    return "L: " + (left_map != null ? left_map.size() : 0) + "  R: " +
  (right_map != null ? right_map.size() : 0) + " C: " 
        + (condition_map != null ? condition_map.size() : 0);
  }
}