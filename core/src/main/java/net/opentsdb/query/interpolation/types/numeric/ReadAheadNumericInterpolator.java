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
package net.opentsdb.query.interpolation.types.numeric;

import java.util.LinkedList;
import java.util.NoSuchElementException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.Op;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryFillPolicy;
import net.opentsdb.query.QueryInterpolator;

/**
 * A class for {@link NumericType}s that allows for read-ahead buffering
 * of values in case multiple values are available for an iterator and
 * the values have to be synced over time when searching for non-null and
 * non-NaN values. E.g. with summaries, if an average is computed from
 * sum and count, the sums and counts must all be present to compute. If
 * one of them is not then it should be nulled out.
 * 
 * @since 3.0
 */
public class ReadAheadNumericInterpolator implements 
    QueryInterpolator<NumericType> {

  /** The fill policy to use when there isn't any data matching the timestamp. */
  protected final QueryFillPolicy<NumericType> fill_policy;
  
  /** The list of previously read values. */
  protected LinkedList<MutableNumericValue> previous;
  
  /** The next value for the iterator. */
  protected MutableNumericValue next;
  
  /** The response we'll populate on calls to {@link #fill(TimeStamp)}. */
  protected MutableNumericValue response;
  
  /** Whether or not there is more data. */
  protected boolean has_next;
  
  /**
   * Default ctor.
   * @param fill_policy A non-null fill policy.
   */
  public ReadAheadNumericInterpolator(
      final QueryFillPolicy<NumericType> fill_policy) {
    if (fill_policy == null) {
      throw new IllegalArgumentException("Fill policy cannot be null.");
    }
    this.fill_policy = fill_policy;
    previous = Lists.newLinkedList();
    response = new MutableNumericValue();
  }
  
  @Override
  public TimeSeriesValue<NumericType> next(final TimeStamp timestamp) {
    int idx = 0;
    for (final MutableNumericValue prev : previous ) {
      if (prev.timestamp().compare(Op.EQ, timestamp)) {
        response.reset(prev);
        if (idx > 0) {
          // cleanup previous entries we no longer need.
          for (int i = 0; i < idx; i++) {
            previous.removeFirst();
          }
        }
        return response;
      }
      idx++;
    }
    
    has_next = false;
    if (next != null && timestamp.compare(Op.EQ, next.timestamp())) {
      response.reset(next);
      previous.clear();
      previous.add(next);
      next = null;
      return response;
    }
    has_next = next != null;
    
    // fill
    return fill(timestamp);
  }

  @Override
  public boolean hasNext() {
    return has_next;
  }

  @Override
  public TimeStamp nextReal() {
    if (!has_next) {
      throw new NoSuchElementException();
    }
    return next.timestamp();
  }

  /**
   * Called with the next data point for a series, bumping the current
   * value to the previous list. Nulls are not bumped to the previous
   * list.
   * <b>NOTE:</b> This method does not check timestamps for ordering.
   * @param next A value or null. When null, if a next value was present, 
   * it is moved to the previous list.
   */
  public void setNext(final TimeSeriesValue<NumericType> next) {
    if (this.next == null) {
      if (next == null) {
        // no-op
        return;
      }
      this.next = new MutableNumericValue(next);
      has_next = true;
      return;
    }
    
    previous.add(this.next);
    if (next == null) {
      this.next = null;
      has_next = false;
    } else {
      this.next = new MutableNumericValue(next);
      has_next = true;
    }
  }
  
  @Override
  public QueryFillPolicy<NumericType> fillPolicy() {
    return fill_policy;
  }

  /**
   * Local method to fill the value from previous or next.
   * @param timestamp A non-null timestamp to fill with.
   * @return A filled value with the given timestamp.
   */
  @VisibleForTesting
  protected TimeSeriesValue<NumericType> fill(final TimeStamp timestamp) {
    MutableNumericValue prev = null;
    MutableNumericValue nxt = null;
    for (final MutableNumericValue p : previous) {
      if (p.timestamp().compare(Op.LT, timestamp)) {
        prev = p;
      } else if (p.timestamp().compare(Op.GT, timestamp)) {
        nxt = p;
        break;
      }
    }
    if (nxt == null) {
      nxt = next;
    }
    
    switch (fill_policy.realPolicy()) {
    case PREVIOUS_ONLY:
      if (prev != null) {
        response.reset(timestamp, prev);
        return response;
      }
      break;
    case PREFER_PREVIOUS:
      if (prev != null) {
        response.reset(timestamp, prev);
        return response;
      }
      if (nxt != null) {
        response.reset(timestamp, nxt);
        return response;
      }
      break;
    case NEXT_ONLY:
      if (nxt != null) {
        response.reset(timestamp, nxt);
        return response;
      }
      break;
    case PREFER_NEXT:
      if (nxt != null) {
        response.reset(timestamp, nxt);
        return response;
      }
      if (prev != null) {
        response.reset(timestamp, prev);
        return response;
      }
      break;
    case NONE:
      break;
    }
    
    final NumericType fill = fill_policy.fill();
    if (fill == null) {
      response.resetNull(timestamp);
    } else {
      response.reset(timestamp, fill);
    }
    return response;
  }
}