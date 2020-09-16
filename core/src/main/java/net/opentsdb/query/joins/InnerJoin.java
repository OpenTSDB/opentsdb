// This file is part of OpenTSDB.
// Copyright (C) 2018-2020  The OpenTSDB Authors.
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
    if (join.condition_map != null && 
        (join.left_map != null || join.right_map != null)) {
      ternaryAdvance();
    } else if (join.left_map != null && join.right_map != null) {
      advance();
    } else {
      current = null;
      next = null;
    }
  }
  
  @Override
  protected void advance() {
    while (left_iterator.hasNext() || 
          (left_series != null && left_idx < left_series.size())) {
      
      if (left_series != null && 
          right_series != null) {
        if (left_idx >= left_series.size()) {
          // all done with the cross join.
          left_series = null;
          right_series = null;
        } else if (right_idx < right_series.size()) {
          next[0] = left_series.get(left_idx);
          next[1] = right_series.get(right_idx);
          right_idx++;
          return;
        }
        
        right_idx = 0;
        left_idx++;
        if (left_series != null &&
            left_idx < left_series.size()) {
          next[0] = left_series.get(left_idx);
          next[1] = right_series.get(right_idx);
          right_idx++;
          return;
        } else {
          left_series = null;
          right_series = null;
          // fall through
        }
      }
      
      if (!left_iterator.hasNext()) {
        // all done!
        break;
      }
      
      left_iterator.advance();
      left_series = left_iterator.value();
      // check for nulls and empties. Shouldn't happen but can.
      if (left_series == null) {
        continue;
      }
      right_series = join.right_map.remove(left_iterator.key());
      if (left_series == null || right_series == null) {
        continue;
      }
      left_idx = 0;
      right_idx = 0;
    }
    
    // all done!
    next = null;
  }
  
  @Override
  protected void ternaryAdvance() {
    // sigh, could happen in which case we can still follow the ternary logic
    // but one side will always be null.
    if (left_iterator == null || right_iterator == null) {
      naturalTernaryLeftOrRightAdvance();
      return;
    }
    
    while (ternary_iterator.hasNext() || 
          (ternary_series != null && ternary_idx < ternary_series.size()) ||
          (left_series != null && left_idx < left_series.size())) {
      
      // see if there are leftovers in the right array to cross on.
      if (ternary_series != null && 
          left_series != null && 
          right_series != null) {
        
        if (left_idx < left_series.size() && 
            right_idx < right_series.size()) {
          next[0] = left_series.get(left_idx);
          next[1] = right_series.get(right_idx);
          next[2] = ternary_series.get(ternary_idx);
          right_idx++;
          return;
        }
        right_idx = 0;
        left_idx++;
        if (left_idx < left_series.size()) {
          next[0] = left_series.get(left_idx);
          next[1] = right_series.get(right_idx);
          next[2] = ternary_series.get(ternary_idx);
          right_idx++;
          return;
        }
        
        left_idx = 0;
        right_idx = 0;
        ternary_idx++;
        if (ternary_idx < ternary_series.size()) {
          next[0] = left_series.get(left_idx);
          next[1] = right_series.get(right_idx);
          next[2] = ternary_series.get(ternary_idx);
          right_idx++;
          return;
        } else {
          // all done with the cross join.
          ternary_series = null;
          left_series = null;
          right_series = null;
        }
      }
      
      if (!ternary_iterator.hasNext()) {
        // all done!
        break;
      }
      
      ternary_idx = 0;
      left_idx = 0;
      right_idx = 0;
      ternary_iterator.advance();
      ternary_series = ternary_iterator.value();
      // check for nulls and empties. Shouldn't happen but can.
      if (ternary_series == null || ternary_series.isEmpty()) {
        continue;
      }
      left_series = join.left_map.remove(ternary_iterator.key());
      right_series = join.right_map.remove(ternary_iterator.key());
      if (left_series == null || left_series.isEmpty() || 
          right_series == null || right_series.isEmpty()) {
        // reset to null in case we had an empty list.
        left_series = null;
        right_series = null;
        continue;
      }
    }
    
    // all done!
    next = null;
  }
  
}