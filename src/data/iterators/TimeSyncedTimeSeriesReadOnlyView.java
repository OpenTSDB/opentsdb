// This file is part of OpenTSDB.
// Copyright (C) 2016  The OpenTSDB Authors.
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
package net.opentsdb.data.iterators;

/**
 * An extension of a time series set that enforces iteration over the time 
 * series in the set synced on timestamps.
 * <p>
 * For time synced sets, calling {@code next} on a time series iterator does not
 * advance the set. Instead it will return the "current" value for the series
 * and repeated calls will return the same value. To advance the value, call
 * {@link #next(long)} on the set (after checking the set's {@link #streamStatus()}.
 * 
 * TODO - may tweak this class a bit before release
 * @since 3.0
 */
public interface TimeSyncedTimeSeriesReadOnlyView extends TimeSeriesReadOnlyView {
  
  /**
   * The next timestamp available for this set to be passed to {@link #next(long)}.
   * This is effectively the minimum next available timestamp across all time 
   * series in the set.
   * <p>
   * Invariate: The timestamp must be from zero to {@code Long.MAX_VALUE}.
   * <p>
   * Invariate: If the next timestamp is equal to {@code Long.MAX_VALUE} then
   * {@link #streamStatus()} must return {@code StreamStatus.END_OF_STREAM}.
   * 
   * @return The next available timestamp to synch across sets on.
   */
  public long nextTimestamp();
  
  /**
   * The status of the time synced stream:
   * HAS_DATA - call {@link next(long)} to get the next data point as at least 
   * one of the time series has a "real" value.
   * END_OF_CHUNK - We've read all the data in memory but there may be more 
   * from the source, so call {@link fetchNext(long)}. This will be returned
   * any time one or more of the underlying time series has reached the end of
   * it's current chunk.
   * WAITING - Returned after {@link #fetchNext(long)} has been called and we're 
   * waiting on the response
   * END_OF_STREAM - No more data is available from the source. All done.
   * EXCEPTION - Something went pear shaped and the stream is unrecoverable. 
   * The client should try again with a new stream.
   * @return The current status of the time synced stream.
   */
  public StreamStatus streamStatus();
  
  /**
   * Advances the underlying iterators to the given timestamp. If an underlying
   * iterator does not have a value at the given timestamp then it should fill
   * with the proper value. If the timestamp is beyond the data in memory, the 
   * stream state may be set to END_OF_CHUNK or END_OF_STREAM.
   * 
   * @param timestamp The timestamp in milliseconds to advance to. Must be 
   * greater than or equal to zero and less than {@code Long.MAX_VALUE}
   * 
   * @throws IllegalStateException if {@link #views()} has not been called to
   * return a set of iterators.
   */
  public void next(final long timestamp);
  
  /**
   * Fetches the next chunk of data for any underlying iterators that need to.
   * 
   * @param timestamp The timestamp in milliseconds to use in determining if an
   * iterator needs to fetch data. Must be greater than or equal to zero and 
   * less than {@code Long.MAX_VALUE}
   * 
   * @return The number of new time series discovered when fetching new data.
   */
  public int fetchNext(final long timestamp);
}
