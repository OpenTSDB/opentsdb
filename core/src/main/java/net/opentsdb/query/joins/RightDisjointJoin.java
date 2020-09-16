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
 * A right join that only returns the values from the right hash map that
 * do <b>not</b> match a value in the left map.
 * 
 * <b>NOTE:</b> When used in a ternary we just use the 
 * {@link BaseJoin#naturalTernaryLeftOrRightAdvance()} method for now. 
 * 
 * @since 3.0
 */
public class RightDisjointJoin extends BaseJoin {

  /**
   * Default ctor.
   * @param join A non-null joint.
   * @throws IllegalArgumentException if the join was null.
   */
  protected RightDisjointJoin(final BaseHashedJoinSet join) {
    super(join);
    if (join.condition_map != null && 
        join.right_map != null) {
      ternaryAdvance();
    } else if (right_iterator != null) {
      advance();
    } else {
      current = null;
      next = null;
    }
  }
  
  @Override
  protected void advance() {
    while (right_iterator.hasNext() || 
          (right_series != null && right_idx < right_series.size())) {
      if (right_series != null && right_idx + 1 < right_series.size()) {
        right_idx++;
        next[0] = null;
        next[1] = right_series.get(right_idx);
        return;
      }
      
      // advance if necessary.
      if (right_series == null || right_idx + 1 >= right_series.size()) {
        if (right_iterator.hasNext()) {
          right_iterator.advance();
          right_series = right_iterator.value();
          if (right_series == null || right_series.isEmpty()) {
            right_series = null;
            continue;
          }
        } else {
          right_series = null;
          continue;
        }
        right_idx = 0;
      }
      
      if (join.left_map == null || 
          !join.left_map.containsKey(right_iterator.key())) {
        // no match from right to left, that's what we want
        next[0] = null;
        next[1] = right_series.get(right_idx);
        return;
      }
      
      // otherwise it matched and we need to skip it.
      right_series = null;
    }
    
    // all done!
    next = null;
  }


  @Override
  protected void ternaryAdvance() {
    naturalTernaryLeftOrRightAdvance();
  }
}