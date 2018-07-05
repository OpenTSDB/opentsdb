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

import gnu.trove.set.hash.TLongHashSet;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.utils.Pair;

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
public class OuterJoin extends BaseJoin {
  
  /** Whether or not we're processing this as a disjoint set. */
  private final boolean disjoint;
  
  /**
   * Default ctor.
   * @param join A non-null joint.
   * @param disjoint Whether or not to return the disjoint outer join.
   * @throws IllegalArgumentException if the join was null.
   */
  protected OuterJoin(final BaseHashedJoinSet join, 
                      final boolean disjoint) {
    super(join);
    this.disjoint = disjoint;
    left_iterator = join.left_map == null ? null : join.left_map.iterator();
    right_iterator = join.right_map == null ? null : join.right_map.iterator();
    completed = new TLongHashSet();
    if (left_iterator != null || right_iterator != null) {
      pair = new Pair<TimeSeries, TimeSeries>(null, null);  
      next = new Pair<TimeSeries, TimeSeries>(null, null);
      advance();
    } else {
      pair = null;
      next = null;
    }
  }
  
  @Override
  protected void advance() {
    // exhaust the left hand side first.
    if (left_iterator != null) {
      while (left_iterator.hasNext() || 
          (left_series != null && left_idx < left_series.size())) {
        // see if there are leftovers in the right array to cross on.
        if (!disjoint && 
            right_series != null && 
            right_idx + 1 < right_series.size()) {
          right_idx++;
          next.setKey(left_series.get(left_idx));
          next.setValue(right_series.get(right_idx));
          return;
        } else if (left_series != null && 
                   left_idx + 1 < left_series.size() &&
                   right_series == null) {
          left_idx++;
          next.setKey(left_series.get(left_idx));
          next.setValue(null);
          return;
        }
        
        // advance if necessary.
        if (left_series == null || left_idx + 1 >= left_series.size()) {
          if (left_iterator.hasNext()) {
            left_iterator.advance();
            left_series = left_iterator.value();
            right_series = null;
            completed.add(left_iterator.key());
            // null check
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
        
        // pull out the matching series on the right
        if (right_series == null) {
          right_series = join.right_map != null ? 
              join.right_map.get(left_iterator.key()) : null;
          right_idx = -1;
          if (right_series == null) {
            // no match from left to right, return a null
            next.setKey(left_series.get(left_idx));
            next.setValue(null);
            return;
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
        if (disjoint) {
          // so we Don't want the pairs.
          left_series = null;
          right_series = null;
          continue;
        } else {
          right_idx++;
          next.setKey(left_series.get(left_idx));
          next.setValue(right_series.get(right_idx));
        }
        
        // clear out the series if we've reached the end of the arrays.
        if (left_idx + 1 >= left_series.size() && 
            right_idx + 1 >= right_series.size()) {
          left_series = null;
          right_series = null;
          right_idx = -1;
        }
        return;
      }
    
      // all done!
      left_iterator = null;
      left_series = null;
      // reset the right to be safe
      right_idx = -1;
      right_series = null;
    } 
    
    // WORK RIGHT SIDE!
    if (right_iterator != null) {
      while (right_iterator.hasNext() || 
          (right_series != null && right_idx < right_series.size())) {
        // see if we have a left series and more to work with.
        if (!disjoint && 
            left_series != null && 
            left_idx + 1 < left_series.size()) {
          left_idx++;
          next.setKey(left_series.get(left_idx));
          next.setValue(right_series.get(right_idx));
          return;
        } else if (right_series != null && 
                   right_idx + 1 < right_series.size()) {
          right_idx++;
          next.setKey(null);
          next.setValue(right_series.get(right_idx));
          return;
        }
        
        // advance if necessary.
        if (right_series == null || right_idx + 1 >= right_series.size()) {
          if (right_iterator.hasNext()) {
            right_iterator.advance();
            right_series = right_iterator.value();
            // see if this has been processed already.
            // TODO - this is a trade-off between making copies of the
            // source maps where we could delete the entries as we work
            // vs just keeping a copy of the processed hashes and skipping
            // them as we go.
            if (completed.contains(right_iterator.key())) {
              right_series = null;
              continue;
            }
          } else {
            right_series = null;
            continue;
          }
          right_idx = 0;
        }
        
        // pull out the matching series on the left
        if (left_series == null) {
          left_series = join.left_map != null ? 
              join.left_map.get(right_iterator.key()) : null;
          left_idx = -1;
          if (left_series == null) {
            // no match from right to left so return a null;
            next.setKey(null);
            next.setValue(right_series.get(right_idx));
            return;
          }
        }
        
        // matched a right series..
        if (left_idx + 1 >= left_series.size()) {
          // inc right_idx and start over
          right_idx++;
          left_idx = -1;
        }
        
        if (right_idx >= right_series.size()) {
          right_series = null;
          // exhausted this series, move to the next.
          continue;
        }
        
        // matched!
        if (disjoint) {
          left_series = null;
          right_series = null;
          continue;
        } else {
          left_idx++;
          next.setKey(left_series.get(left_idx));
          next.setValue(right_series.get(right_idx));
        }
        
        if (left_idx + 1 >= left_series.size() && 
            right_idx + 1 >= right_series.size()) {
          left_series = null;
          right_series = null;
          left_idx = -1;
        }
        return;
      }
    }
    
    // all done!
    next = null;
  }
}