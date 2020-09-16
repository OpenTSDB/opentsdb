// This file is part of OpenTSDB.
// Copyright (C) 2020  The OpenTSDB Authors.
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
import net.opentsdb.query.joins.Joiner.Operand;

/**
 * A simple join when there is only one result, the left, right or condition.
 * Helps avoid a ton of redundant branches in the other joins.
 * 
 * @since 3.0
 */
public class SingleResultJoin extends BaseJoin {

  // move everything to the left iterator to keep things simple.
  final Operand operand;
  
  protected SingleResultJoin(final BaseHashedJoinSet join) {
    super(join);
    if (join.left_map != null) {
      operand = Operand.LEFT;
    } else if (join.right_map != null) {
      operand = Operand.RIGHT;
      left_iterator = right_iterator;
    } else {
      // well, yeah this shouldn't happen.
      operand = Operand.CONDITION;
      left_iterator = ternary_iterator;
    }
    advance();
  }

  @Override
  protected void advance() {
    while (true) {
      if (left_series != null && left_idx < left_series.size()) {
        TimeSeries value = left_series.get(left_idx);
        if (value == null) {
          left_idx++;
          continue;
        }
        switch(operand) {
        case LEFT:
          next[0] = value;
          break;
        case RIGHT:
          next[1] = value;
          break;
        case CONDITION:
          next[2] = value;
          break;
        }
        left_idx++;
        return;
      }
    
      if (!left_iterator.hasNext()) {
        next = null;
        return;
      }
      
      left_iterator.advance();
      left_series = left_iterator.value();
      left_idx = 0;
      if (left_series == null) {
        return;
      }
    }
  }

  @Override
  protected void ternaryAdvance() {
    // no-op, we use Advance here for everything.
    advance();
  }

}