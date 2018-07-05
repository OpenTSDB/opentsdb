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

import java.util.Iterator;
import java.util.List;

import gnu.trove.map.TLongObjectMap;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.query.joins.JoinConfig.JoinType;
import net.opentsdb.utils.Pair;

/**
 * The base class for binary joins with a left and a right map of lists
 * of time series keyed on the Long hash based off the JoinConfig.
 * 
 * @since 3.0
 */
public abstract class BaseHashedJoinSet implements 
    Iterable<Pair<TimeSeries, TimeSeries>> {

  /** The type of join. */
  protected final JoinType type;
  
  /** The left join map. May be null. */
  protected TLongObjectMap<List<TimeSeries>> left_map;
  
  /** The right join map. May be null. */
  protected TLongObjectMap<List<TimeSeries>> right_map;
  
  /**
   * Default ctor.
   * @param type A non-null type.
   */
  public BaseHashedJoinSet(final JoinType type) {
    this.type = type;
  }
  
  @Override
  public Iterator<Pair<TimeSeries, TimeSeries>> iterator() {
    switch (type) {
    case INNER:
    case NATURAL:
    case LEFT:
    case RIGHT:
      return new InnerJoin(this);
    case OUTER:
      return new OuterJoin(this, false);
    case OUTER_DISJOINT:
      return new OuterJoin(this, true);
    case LEFT_DISJOINT:
      return new LeftDisjointJoin(this);
    case RIGHT_DISJOINT:
      return new RightDisjointJoin(this);
    case CROSS:
      return new CrossJoin(this);
      default:
        throw new UnsupportedOperationException("Unsupported join type: " 
            + type);
    }
  }
}