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
    Iterator<Pair<TimeSeries, TimeSeries>> {
  
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
  
  /** A set of completed hashes used by the outer join when processing
   * the right iterator after finishing the left to avoid dupes. */
  protected TLongSet completed;
  
  /** The value to be populated and returned in the {@link #next()} call. */
  protected Pair<TimeSeries, TimeSeries> pair;
  
  /** A value to be populated in the {@link #advance()} implementation
   * and used by {@link #hasNext()} to determine if more data is available
   * or not. If null, no more data available. */
  protected Pair<TimeSeries, TimeSeries> next;
  
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
  }
  
  @Override
  public boolean hasNext() {
    return next != null;
  }

  @Override
  public Pair<TimeSeries, TimeSeries> next() {
    pair.setKey(next.getKey());
    pair.setValue(next.getValue());
    advance();
    return pair;
  }
  
  /**
   * Advances to the next pair in the join, setting {@link #next()} to a 
   * value if there is more data or nulling {@link #next()} if we're out
   * of data.
   */
  protected abstract void advance();
}