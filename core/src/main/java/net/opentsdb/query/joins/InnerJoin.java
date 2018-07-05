// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
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

import net.opentsdb.data.TimeSeries;
import net.opentsdb.utils.Pair;

/**
 * Performs an inner join where both left and right must have one or more
 * entries in the list for the same hash. If either map is null or empty
 * then the join will always return false for {@link #hasNext()}.
 * <p>
 * For hashes where more than one series satisfied the algorithm, a 
 * Cartesian product is produced.
 * 
 * @since 3.0
 */
public class InnerJoin extends BaseJoin {

  /**
   * Default ctor.
   * @param join A non-null join set.
   * @throws IllegalArgumentException if the set was null.
   */
  protected InnerJoin(final BaseHashedJoinSet join) {
    super(join);
    left_iterator = join.left_map == null ? null : join.left_map.iterator();
    if (left_iterator != null && join.right_map != null) {
      pair = new Pair<TimeSeries, TimeSeries>(null, null);  
      next = new Pair<TimeSeries, TimeSeries>(null, null);
      advance();
    } else {
      pair = null;
      next = null;
    }
  }
  
  @Override
  public boolean hasNext() {
    return next != null;
  }

  @Override
  public Pair<TimeSeries, TimeSeries> next() {
    pair.setKey(next.getKey());
    pair.setValue(next.getValue());
    advance();
    return pair;
  }
  
  @Override
  protected void advance() {
    while (left_iterator.hasNext() || 
          (left_series != null && left_idx < left_series.size())) {
      
      // see if there are leftovers in the right array to cross on.
      if (left_series != null && 
          right_series != null && 
          right_idx + 1 < right_series.size()) {
        right_idx++;
        next.setKey(left_series.get(left_idx));
        next.setValue(right_series.get(right_idx));
        return;
      }
      
      // advance if necessary.
      if (left_series == null || left_idx + 1 >= left_series.size()) {
        if (left_iterator.hasNext()) {
          left_iterator.advance();
          left_series = left_iterator.value();
          // check for nulls and empties. Shouldn't happen but can.
          if (left_series == null || left_series.isEmpty()) {
            continue;
          }
          right_series = null;
        } else {
          left_series = null;
          continue;
        }
        left_idx = 0;
      }
      
      if (right_series == null) {
        right_series = join.right_map.get(left_iterator.key());
        right_idx = -1;
        if (right_series == null || right_series.isEmpty()) {
          // no match from left to right, iterate to the next left
          left_series = null;
          continue;
        }
      }
      
      // matched a right series..
      if (right_idx + 1 >= right_series.size()) {
        // inc left_idx and start over
        left_idx++;
        right_idx = -1;
      }
      
      if (left_idx >= left_series.size()) {
        left_series = null;
        // exhausted this series, move to the next.
        continue;
      }
      
      // matched!
      right_idx++;
      next.setKey(left_series.get(left_idx));
      next.setValue(right_series.get(right_idx));
      
      if (left_idx + 1 >= left_series.size() && 
          right_idx + 1 >= right_series.size()) {
        right_series = null;
        right_idx = -1;
      }
      return;
    }
    
    // all done!
    next = null;
  }
}