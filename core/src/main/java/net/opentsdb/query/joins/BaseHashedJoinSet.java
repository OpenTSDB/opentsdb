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

import gnu.trove.map.TLongObjectMap;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.query.joins.JoinConfig.JoinType;

/**
 * The base class for binary joins with a left and a right map of lists
 * of time series keyed on the Long hash based off the JoinConfig.
 * 
 * @since 3.0
 */
public abstract class BaseHashedJoinSet implements Iterable<TimeSeries[]> {

  /** The type of join. */
  protected final JoinType type;
  
  /** The left join map. May be null. */
  protected TLongObjectMap<List<TimeSeries>> left_map;
  
  /** The right join map. May be null. */
  protected TLongObjectMap<List<TimeSeries>> right_map;
  
  /** The ternary or condition map. May be null. */
  protected TLongObjectMap<List<TimeSeries>> condition_map;
  
  /** The number of expected result sets (left, right, condition) */
  protected final int expected_sets;
  
  /** Whether or not this is a ternary set. */
  protected final boolean is_ternary;
  
  /**
   * Default ctor.
   * @param type A non-null type.
   * @param expected_sets The number of expected sets (left, right, condition).
   * #param is_ternary Whether or not this set should have condition data.
   */
  public BaseHashedJoinSet(final JoinType type, 
                           final int expected_sets,
                           final boolean is_ternary) {
    this.type = type;
    this.expected_sets = expected_sets;
    this.is_ternary = is_ternary;
    if (type == null) {
      throw new IllegalArgumentException("Join type cannot be null.");
    }
  }
  
  @Override
  public Iterator<TimeSeries[]> iterator() {
    int non_null_maps = 0;
    if (left_map != null) {
      non_null_maps++;
    }
    if (right_map != null) {
      non_null_maps++;
    }
    if (condition_map != null) {
      non_null_maps++;
    }
    
    // don't bother.
    if (is_ternary && non_null_maps == 1 && condition_map == null) {
      return EMPTY_ITERATOR;
    }
    
    // shouldn't happen, but...
    if (non_null_maps == 0) {
      return EMPTY_ITERATOR;
    }
    
    switch (type) {
    case INNER:
      if (non_null_maps == 1 && expected_sets == 1) {
        return new SingleResultJoin(this);
      } else if (non_null_maps == 1 && expected_sets > 1) {
        return EMPTY_ITERATOR;
      }
      // fall-through
    case NATURAL:
      return new InnerJoin(this);
    case LEFT:
      if (left_map == null) {
        return EMPTY_ITERATOR;
      }
      if (non_null_maps == 1) {
        return new SingleResultJoin(this);
      }
    case RIGHT:
      if (right_map == null) {
        return EMPTY_ITERATOR;
      }
      if (non_null_maps == 1) {
        return new SingleResultJoin(this);
      }
      return new InnerJoin(this);
    case NATURAL_OUTER:
      if (non_null_maps == 1 && expected_sets == 1) {
        return new SingleResultJoin(this);
      }
      return new NaturalOuterJoin(this);
    case OUTER:
      if (non_null_maps == 1 && expected_sets == 1) {
        return new SingleResultJoin(this);
      }
      return new OuterJoin(this, false);
    case OUTER_DISJOINT:
      if (non_null_maps == 1 && expected_sets == 1) {
        return new SingleResultJoin(this);
      }
      return new OuterJoin(this, true);
    case LEFT_DISJOINT:
      if (left_map == null) {
        return EMPTY_ITERATOR;
      } else if (non_null_maps == 1 && expected_sets == 1) {
        return new SingleResultJoin(this);
      }
      return new LeftDisjointJoin(this);
    case RIGHT_DISJOINT:
      if (right_map == null) {
        return EMPTY_ITERATOR;
      } else if (non_null_maps == 1 && expected_sets == 1) {
        return new SingleResultJoin(this);
      }
      return new RightDisjointJoin(this);
    case CROSS:
      if (non_null_maps == 1 && expected_sets == 1) {
        return new SingleResultJoin(this);
      }
      return new CrossJoin(this);
    default:
      throw new UnsupportedOperationException("Unsupported join type: " 
          + type);
    }
  }

  @Override
  public String toString() {
    return new StringBuilder()
        .append("{leftMap=")
        .append(left_map == null ? "null" : left_map.size())
        .append(", rightMap=")
        .append(right_map == null ? "null" : right_map.size())
        .append(", conditionMap=")
        .append(condition_map == null ? "null" : condition_map.size())
        .append(", expectedSets=")
        .append(expected_sets)
        .append(", isTernary=")
        .append(is_ternary)
        .append("}")
        .toString();
  }

  /**
   * Super simple iterator to return when we're missing the required sets to
   * compute a proper join.
   */
  public static class EmptyIterator implements Iterator<TimeSeries[]> {

    @Override
    public boolean hasNext() {
      return false;
    }

    @Override
    public TimeSeries[] next() {
      return null;
    }
    
  }
  static final EmptyIterator EMPTY_ITERATOR = new EmptyIterator();
}