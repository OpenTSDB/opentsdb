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

import java.util.Iterator;
import java.util.List;

import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.set.TLongSet;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.utils.Pair;

/**
 * A base class for Join methods that handles iteration and storing of
 * common variables like list references and iterators.
 * 
 * <b>WARNING:</b> The {@link Pair} returned by the iterator will be the
 * same object on each iteration with the time series references changed.
 * 
 * @since 3.0
 */
public abstract class BaseJoin implements 
    Iterator<TimeSeries[]> {
  
  /** The join set to pull from. */
  protected final BaseHashedJoinSet join;
  
  /** The left hand iterator. May be null. */
  protected TLongObjectIterator<List<TimeSeries>> left_iterator;
  
  /** The current left hand series reference. May be null. */
  protected List<TimeSeries> left_series;
  
  /** An index into the {@link #left_series}. */
  protected int left_idx;
  
  /** The right hand iterator. May be null. */
  protected TLongObjectIterator<List<TimeSeries>> right_iterator;
  
  /** The current right hand series reference. May be null. */
  protected List<TimeSeries> right_series;
  
  /** An index into the {@link #right_series}. */
  protected int right_idx;
  
  /** The ternary iterator. May be null. */
  protected TLongObjectIterator<List<TimeSeries>> ternary_iterator;
  
  /** The current ternary series reference. May be null. */
  protected List<TimeSeries> ternary_series;
  
  /** An index into the {@link #ternary_series}. */
  protected int ternary_idx;
  
  /** A set of completed hashes used by the outer join when processing
   * the right iterator after finishing the left to avoid dupes. */
  protected TLongSet completed;
  
  /** The value to be populated and returned in the {@link #next()} call. */
  protected TimeSeries[] current;
  
  /** A value to be populated in the {@link #advance()} implementation
   * and used by {@link #hasNext()} to determine if more data is available
   * or not. If null, no more data available. */
  protected TimeSeries[] next;
  
  /**
   * Default ctor.
   * @param join A non-null join set to pull the maps from.
   * @throws IllegalArgumentException if the join was null.
   */
  protected BaseJoin(final BaseHashedJoinSet join) {
    if (join == null) {
      throw new IllegalArgumentException("Hashed join set cannot be null.");
    }
    this.join = join;
    current = new TimeSeries[join.is_ternary ? 3 : 2];
    next = new TimeSeries[join.is_ternary ? 3 : 2];
    left_iterator = join.left_map == null ? null : join.left_map.iterator();
    right_iterator = join.right_map == null ? null : join.right_map.iterator();
    if (join.is_ternary) {
      ternary_iterator = join.condition_map == null ? null : 
        join.condition_map.iterator();
    }
  }
  
  @Override
  public boolean hasNext() {
    return next != null;
  }

  @Override
  public TimeSeries[] next() {
    for (int i = 0; i < current.length; i++) {
      current[i] = next[i];
    }
    if (join.is_ternary) {
      ternaryAdvance();
    } else {
      advance();
    }
    return current;
  }
  
  /**
   * Advances to the next pair in the join, setting {@link #next()} to a 
   * value if there is more data or nulling {@link #next()} if we're out
   * of data.
   */
  protected abstract void advance();
  
  /**
   * Advances to the next tupple in the join, setting {@link #next()} to a 
   * value if there is more data or nulling {@link #next()} if we're out
   * of data.
   */
  protected abstract void ternaryAdvance();

  /**
   * Performs a natural left and/or right join for use in joins where the 
   * explicit join doesn't make sense in a ternary condition.
   */
  protected void naturalTernaryLeftOrRightAdvance() {
    // A left OR right must be present for the given conditions, otherwise 
    // what's the point?
    while (ternary_iterator.hasNext() || 
          (ternary_series != null && ternary_idx < ternary_series.size())) {
      
      // see if there are leftovers in the right array to cross on.
      if (ternary_series != null && 
          (left_series != null || 
          right_series != null)) {
        
        if (left_series != null &&
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
        } else if (left_series != null) {
          // left only
          if (left_idx < left_series.size()) {
            next[0] = left_series.get(left_idx);
            next[1] = null;
            next[2] = ternary_series.get(ternary_idx);
            left_idx++;
            return;
          }
          
          left_idx = 0;
          ternary_idx++;
          if (ternary_idx < ternary_series.size()) {
            next[0] = left_series.get(left_idx);
            next[1] = null;
            next[2] = ternary_series.get(ternary_idx);
            left_idx++;
            return;
          } else {
            // all done with the cross join.
            ternary_series = null;
            left_series = null;
            right_series = null;
          }
        } else {
          // right only
          if (right_idx < right_series.size()) {
            next[0] = null;
            next[1] = right_series.get(right_idx);
            next[2] = ternary_series.get(ternary_idx);
            right_idx++;
            return;
          }
          
          right_idx = 0;
          ternary_idx++;
          if (ternary_idx < ternary_series.size()) {
            next[0] = null;
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
        ternary_series = null;
        left_series = null;
        right_series = null;
        continue;
      }
      
      left_series = join.left_map != null ? 
          join.left_map.remove(ternary_iterator.key()) : null;
      right_series = join.right_map != null ? 
          join.right_map.remove(ternary_iterator.key()) : null;
      if ((left_series == null || left_series.isEmpty()) && 
          (right_series == null || right_series.isEmpty())) {
        // reset to null in case we had an empty list.
        left_series = null;
        right_series = null;
      } else if (left_series != null && left_series.isEmpty()) {
        left_series = null;
      } else if (right_series != null && right_series.isEmpty()) {
        right_series = null;
      }
    }
    
    // all done!
    next = null;
  }
}