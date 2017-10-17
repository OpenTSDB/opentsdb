// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
package net.opentsdb.query;

/**
 * Describes the behavior of the query execution.
 * 
 * @since 3.0
 */
public enum QueryMode {
  /**
   * The default query wherein the complete set of results are compiled into a 
   * single {@link QueryResult} object and {@link QuerySink#onComplete()} is
   * called immediately after {@link QuerySink#onNext(QueryResult)}.
   * This mode may fail a query early if it would exceed memory limits.
   */
  SINGLE,
  
  /**
   * A bounded stream query (e.g. a query for historical data) that will return
   * subsets of data in each call to {@link QuerySink#onNext(QueryResult)}
   * from the context. In order to receive the next set of data, the caller
   * must call {@link QueryContext#fetchNext()}. On the final call to 
   * {@link QueryContext#fetchNext()}, if the query is finished, the 
   * {@link QuerySink#onComplete()} method will be called.
   */
  BOUNDED_CLIENT_STREAM,
  
  /**
   * An un-bounded stream of data similar to the {@link #BOUNDED_CLIENT_STREAM}
   * except that each call to {@link QueryContext#fetchNext()} will return the
   * next available set of data to {@link QueryContext#fetchNext()}, possibly
   * with an empty (but never null) result if the stream does not have any data
   * available.
   * <b>Note:</b> In this mode the system may drop data if the client isn't
   * processing results and calling {@link QueryContext#fetchNext()} quickly 
   * enough.
   */
  CONTINOUS_CLIENT_STREAM,
  
  /**
   * A bounded stream query that will send the next subset of data as soon as 
   * the result passed in {@link QuerySink#onNext(QueryResult)} has been 
   * closed by the caller via {@link QueryResult#close()}. This provides for
   * quicker streaming to the client but prevents buffering large amounts of 
   * data in memory.
   * <b>Note:</b> If the caller fails to close the results quickly enough, the
   * query may timeout and fail.
   */
  BOUNDED_SERVER_SYNC_STREAM,
  
  /**
   * A continuous version of {@link #BOUNDED_SERVER_SYNC_STREAM} that will return
   * the next set of real-time results as soon as they're available and the
   * caller has closed the previous result set.
   */
  CONTINOUS_SERVER_SYNC_STREAM,
  
  /**
   * A bounded query stream that will send results as soon as their available to
   * the {@link QuerySink#onNext(QueryResult)} callback. This means the 
   * client is responsible for thread safety in their listener.
   * <b>Note:</b> If too much data has been buffered in memory the query may be
   * marked as failed.
   */
  BOUNDED_SERVER_ASYNC_STREAM,
  
  /**
   * A continous version of {@link #BOUNDED_SERVER_ASYNC_STREAM} where real-time
   * results are sent immediately upstream regardless of the previous query 
   * result's closed state.
   */
  CONTINOUS_SERVER_ASYNC_STREAM
}