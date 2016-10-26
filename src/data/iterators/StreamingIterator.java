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

import net.opentsdb.data.TimeSeriesId;

/**
 * Provides a streaming iterator view over the data in a single time series.
 * 
 * The streaming component differs from a regular iterator in that the data
 * source can return "chunks" of data instead of trying to load all of it in
 * memory. Each time the end of a chunk has been reached, the iterator consumer
 * can choose to fetch another chunk. If no more chunks are available the stream
 * will be marked as EOS.
 * 
 * @param <T> The type of data represented by the time series (must return
 * a {@link TimeSeriesValue}
 */
public interface StreamingIterator<T extends TimeSeriesValue<?>> {
  /**
   * The time series ID for this time series.
   * @return A non-null time series ID
   */
  public TimeSeriesId id();
  
  /**
   * Stream status:
   * HAS_DATA - call {@link next()} to get the next data point, we have data
   * END_OF_CHUNK - We've read all the data in memory but there may be more 
   * from the source, so call {@link fetchNext()}
   * WAITING - Returned after {@link fetchNext()} has been called and we're 
   * waiting on the response
   * END_OF_STREAM - No more data is available from the source. All done.
   * EXCEPTION - Something went pear shaped and the stream is unrecoverable. 
   * The client should try again with a new stream.
   * @return The current status of the stream.
   */
  public StreamStatus streamStatus(); 
  
  /**
   * Fetch the next value that's in memory and ready for consumption.
   * NOTE: Always call {@link #streamStatus()} before calling this method as it may
   * throw an exception if the stream status is in any state other than HAS_DATA.
   * @return The next {@link TimeSeriesValue} if available.
   * @throws IllegalStateException if the no more data is available in memory.
   */
  public T next(); 
  
  /**
   * Fetch the next set of data. If the set has found more time series in the next
   * fetch, then this will return the number of new series so the client can
   * restart their set iterating and not run into a concurrent modification exception
   * TODO - may need to move this or change it
   * @return The number of new series discovered.
   */
  public int fetchNext();
  
  /**
   * Seek forward to the millisecond Unix Epoch timestamp
   * @param timestamp the timestamp as Unix Epoch in milliseconds
   */
  void seek(long timestamp);
}
