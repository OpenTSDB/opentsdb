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
 * A left join that only returns the values from the left hash map that
 * do <b>not</b> match a value in the right map.
 * 
 * <b>NOTE:</b> When used in a ternary we just use the 
 * {@link BaseJoin#naturalTernaryLeftOrRightAdvance()} method for now.
 * 
 * @since 3.0
 */
public class LeftDisjointJoin extends BaseJoin {

  /**
   * Default ctor.
   * @param join A non-null joint.
   * @throws IllegalArgumentException if the join was null.
   */
  protected LeftDisjointJoin(final BaseHashedJoinSet join) {
    super(join);
    if (join.condition_map != null && 
        join.left_map != null) {
      ternaryAdvance();
    } else if (left_iterator != null) {
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
      if (left_series != null && left_idx + 1 < left_series.size()) {
        left_idx++;
        next[0] = left_series.get(left_idx);
        next[1] = null;
        return;
      }
      
      // advance if necessary.
      if (left_series == null || left_idx + 1 >= left_series.size()) {
        if (left_iterator.hasNext()) {
          left_iterator.advance();
          left_series = left_iterator.value();
          if (left_series == null || left_series.isEmpty()) {
            left_series = null;
            continue;
          }
        } else {
          left_series = null;
          continue;
        }
        left_idx = 0;
      }
      
      if (join.right_map == null || 
          !join.right_map.containsKey(left_iterator.key())) {
        // no match from left to right, that's what we want
        next[0] = left_series.get(left_idx);
        next[1] = null;
        return; 
      }
      
      // otherwise it matched and we need to skip it.
      left_series = null;
    }
    
    // all done!
    next = null;
  }

  @Override
  protected void ternaryAdvance() {
    naturalTernaryLeftOrRightAdvance();
  }
}