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
 * Computes the outer join, i.e. the full join with nulls on either side
 * if no matches on a hash were found.
 * <p>
 * Also capable of performing a disjoint join meaning it will return only
 * the series that do *not* have a matching value on the other side.
 * <p>
 * For hashes where more than one series satisfied the algorithm, a 
 * Cartesian product is produced.
 * 
 * @since 3.0
 */
public class NaturalOuterJoin extends BaseJoin {
  
  /**
   * Default ctor.
   * @param join A non-null joint.
   * @throws IllegalArgumentException if the join was null.
   */
  protected NaturalOuterJoin(final BaseHashedJoinSet join) {
    super(join);
    if (join.is_ternary &&
        (join.left_map != null || join.right_map != null) &&
        join.condition_map != null) {
      System.out.println(join.left_map +"\n" + join.right_map + "\n" + join.condition_map);
      ternaryAdvance();
    } else if (!join.is_ternary && 
        (join.left_map != null || join.right_map != null)){
      advance();
    } else {
      current = null;
      next = null;
    }
  }
  
  @Override
  protected void advance() {
    // exhaust the left hand side first.
    if (left_iterator != null) {
      while ((left_iterator != null && left_iterator.hasNext()) || 
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
        } else if (left_series != null && left_idx < left_series.size()) {
          next[0] = left_series.get(left_idx);
          next[1] = null;
          left_idx++;
          return;
        }

        if (left_iterator.hasNext()) {
          left_idx = 0;
          right_idx = 0;
          left_iterator.advance();
          left_series = left_iterator.value();
          if (left_series == null || left_series.isEmpty()) {
            left_series = null;
            continue;
          }
          if (join.right_map != null) {
            right_series = join.right_map.remove(left_iterator.key());
          }
        } else {
          left_iterator = null; // fall through and reset the iterator!
          if (join.right_map != null) {
            right_iterator = join.right_map.iterator();
          }
        }
      }
    }
    
    while ((right_iterator != null && right_iterator.hasNext()) ||
        (right_series != null && right_idx < right_series.size())) {
      if (right_series != null && right_idx < right_series.size()) {
        next[0] = null;
        next[1] = right_series.get(right_idx);
        right_idx++;
        return;
      }
      
      if (right_iterator.hasNext()) {
        right_iterator.advance();
        right_series = right_iterator.value();
        right_idx = 0;
      }
    }
    
    // all done!
    next = null;
  }
  
  @Override
  protected void ternaryAdvance() {
    naturalTernaryLeftOrRightAdvance();
  }
}