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
 * Performs a full Cartesian product for all series. Be careful as this 
 * can be huge! 
 * 
 * <b>NOTE:</b> When used in a ternary we just use the 
 * {@link BaseJoin#naturalTernaryLeftOrRightAdvance()} method for now.
 * 
 * @since 3.0
 */
public class CrossJoin extends BaseJoin {

  /**
   * Default ctor.
   * @param join A non-null join set.
   * @throws IllegalArgumentException if the set was null.
   */
  protected CrossJoin(final BaseHashedJoinSet join) {
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
      
      // check the right side first
      if (left_series != null && 
          right_series != null && 
          right_idx + 1 < right_series.size()) {
        right_idx++;
        next[0] = left_series.get(left_idx);
        next[1] = right_series.get(right_idx);
        
        // move to the next right 
        if (right_idx >= right_series.size()) {
          nextRight();
          if (right_series == null) {
            // done with all the rights.
            left_idx++;
          }
        }
        return;
      }
      
      if (left_series == null || left_idx >= left_series.size()) {
        if (left_iterator.hasNext()) {
          left_iterator.advance();
          left_series = left_iterator.value();
          // check for nulls and empties. Shouldn't happen but can.
          if (left_series == null || left_series.isEmpty()) {
            left_series = null;
            continue;
          }
          
          // reset the iterator.
          right_iterator = join.right_map.iterator();
          nextRight();
        } else {
          left_series = null;
          break;
        }
        left_idx = 0;
      }
      
      if (right_series == null || right_idx + 1 >= right_series.size()) {
        nextRight();
        if (right_series == null && !right_iterator.hasNext()) {
          // inc left_idx and start over
          left_idx++;
          right_iterator = join.right_map.iterator();
          nextRight();
        }
      }
      
      if (left_idx >= left_series.size()) {
        left_series = null;
        // exhausted this series, move to the next.
        continue;
      }
      
      right_idx++;
      next[0] = left_series.get(left_idx);
      next[1] = right_series.get(right_idx);
      return;
    }
    
    // all done!
    next = null;
  }
  
  @Override
  protected void ternaryAdvance() {
    naturalTernaryLeftOrRightAdvance();
  }
  
  /**
   * Helper to iterate to the next non-null and non-empty right value.
   */
  private void nextRight() {
    right_series = null;
    while (right_iterator.hasNext()) {
      right_iterator.advance();
      right_series = right_iterator.value();
      if (right_series == null || right_series.isEmpty()) {
        right_series = null;
        continue;
      }
      break;
    }
    right_idx = -1;
  }
}