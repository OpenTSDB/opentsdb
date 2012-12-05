// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.core;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Provides a <em>zero-copy view</em> to iterate through data points.
 * <p>
 * The iterator returned by classes that implement this interface must return
 * each {@link DataPoint} in {@code O(1)} and does not support {@link #remove}.
 * <p>
 * Because no data is copied during iteration and no new object gets created,
 * <b>the {@link DataPoint} returned must not be stored</b> and gets
 * invalidated as soon as {@link #next} is called on the iterator (actually it
 * doesn't get invalidated but rather its contents changes).  If you want to
 * store individual data points, you need to copy the timestamp and value out
 * of each {@link DataPoint} into your own data structures.
 * <p>
 * In the vast majority of cases, the iterator will be used to go once through
 * all the data points, which is why it's not a problem if the iterator acts
 * just as a transient "view".  Iterating will be very cheap since no memory
 * allocation is required (except to instantiate the actual iterator at the
 * beginning).
 */
public interface SeekableView extends Iterator<DataPoint> {

  /**
   * Returns {@code true} if this view has more elements.
   */
  boolean hasNext();

  /**
   * Returns a <em>view</em> on the next data point.
   * No new object gets created, the referenced returned is always the same
   * and must not be stored since its internal data structure will change the
   * next time {@code next()} is called.
   * @throws NoSuchElementException if there were no more elements to iterate
   * on (in which case {@link #hasNext} would have returned {@code false}.
   */
  DataPoint next();

  /**
   * Unsupported operation.
   * @throws UnsupportedOperationException always.
   */
  void remove();

  /**
   * Advances the iterator to the given point in time.
   * <p>
   * This allows the iterator to skip all the data points that are strictly
   * before the given timestamp.
   * @param timestamp A strictly positive 32 bit UNIX timestamp (in seconds).
   * @throws IllegalArgumentException if the timestamp is zero, or negative,
   * or doesn't fit on 32 bits (think "unsigned int" -- yay Java!).
   */
  void seek(long timestamp);

}
