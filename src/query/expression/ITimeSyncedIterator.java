// This file is part of OpenTSDB.
// Copyright (C) 2015  The OpenTSDB Authors.
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
package net.opentsdb.query.expression;

import net.opentsdb.utils.ByteSet;

/**
 * An interface for expressions or queries that operate across time series
 * and require point-by-point timestamp synchronization. 
 * @since 2.3
 */
public interface ITimeSyncedIterator {

  /** @return true if any of the series in the set has another value */
  public boolean hasNext();
  
  /**
   * @param timestamp The timestamp to fastforward to
   * @return The data point array for the given timestamp. Implementations
   * may throw an exception if the timestamp is invliad or they may return an
   * empty array.
   */
  public ExpressionDataPoint[] next(final long timestamp);
  
  /**
   * @return the next timestamp available in this set.
   */
  public long nextTimestamp();

  /** @return the number of series in this set */
  public int size();
  
  /** @return an array of the emitters populated during iteration */
  public ExpressionDataPoint[] values();

  /** @param index the index to null. Nulls the given object so we don't use it
   * in timestamps.
   */
  public void nullIterator(final int index);
  
  /** @return the index in the ExpressionIterator */
  public int getIndex();
  
  /** @param the index in the ExpressionIterator */
  public void setIndex(final int index);
  
  /** @return the ID of this set given by the user */
  public String getId();

  /** @return a set of unique tag key UIDs from the filter list. If no filters
   * were defined then the set may be empty.  */
  public ByteSet getQueryTagKs();

  /** @param A fill policy for the iterator. Iterators should implement a default */
  public void setFillPolicy(final NumericFillPolicy policy);
  
  /** @return the fill policy for the iterator */
  public NumericFillPolicy getFillPolicy();
  
  /** @return a copy of the iterator. This should return references to 
   * underlying data objects but not necessarily copy all of the underlying 
   * data (to avoid memory explosions). This is useful for creating other
   * iterators that operate over the same data. */
  public ITimeSyncedIterator getCopy();
}
