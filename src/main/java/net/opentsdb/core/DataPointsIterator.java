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

import java.util.NoSuchElementException;

/** Default iterator for simple implementations of {@link DataPoints}. */
final class DataPointsIterator implements SeekableView, DataPoint {

  /** Instance to iterate on. */
  private final DataPoints dp;

  /** Where are we in the iteration. */
  private short index = -1;

  /**
   * Ctor.
   * @param dp The data points to iterate on.
   */
  DataPointsIterator(final DataPoints dp) {
    this.dp = dp;
  }

  // ------------------ //
  // Iterator interface //
  // ------------------ //

  public boolean hasNext() {
    return index < dp.size() - 1;
  }

  public DataPoint next() {
    if (hasNext()) {
      index++;
      return this;
    }
    throw new NoSuchElementException("no more elements in " + this);
  }

  public void remove() {
    throw new UnsupportedOperationException();
  }

  // ---------------------- //
  // SeekableView interface //
  // ---------------------- //

  public void seek(final long timestamp) {
    if ((timestamp & 0xFFFFFFFF00000000L) != 0) {  // negative or not 32 bits
      throw new IllegalArgumentException("invalid timestamp: " + timestamp);
    }
    // Do a binary search to find the timestamp given or the one right before.
    short lo = 0;
    short hi = (short) dp.size();

    while (lo <= hi) {
      index = (short) ((lo + hi) >>> 1);
      long cmp = dp.timestamp(index) - timestamp;

      if (cmp < 0) {
        lo = (short) (index + 1);
      } else if (cmp > 0) {
        hi = (short) (index - 1);
      } else {
        index--;  // 'index' is exactly on the timestamp wanted.
        return;   // So let's go right before that for next().
      }
    }
    // We found the timestamp right before as there was no exact match.
    // We take that position - 1 so the next call to next() returns it.
    index = (short) (lo - 1);
    // If the index we found was not the first or the last point, let's
    // do a small extra sanity check to ensure the position we found makes
    // sense: the timestamp we're at must not be >= what we're looking for.
    if (0 < index && index < dp.size() && dp.timestamp(index) >= timestamp) {
      throw new AssertionError("seeked after the time wanted!"
          + " timestamp=" + timestamp
          + ", index=" + index
          + ", dp.timestamp(index)=" + dp.timestamp(index)
          + ", this=" + this);
    }
  }

  /** Package-private helper to find the current index of this iterator. */
  int index() {
    return index;
  }

  // ------------------- //
  // DataPoint interface //
  // ------------------- //

  public long timestamp() {
    return dp.timestamp(index);
  }

  public boolean isInteger() {
    return dp.isInteger(index);
  }

  public long longValue() {
    return dp.longValue(index);
  }

  public double doubleValue() {
    return dp.doubleValue(index);
  }

  public double toDouble() {
    return isInteger() ? longValue() : doubleValue();
  }

  public String toString() {
    return "DataPointsIterator(index=" + index
      + (index >= 0
         ? ", current type: " + (isInteger() ? "long" : "float")
         + ", current value=" + (isInteger() ? longValue() : doubleValue())
         : " (iteration not started)")
      + ", dp=" + dp + ')';
  }

}
