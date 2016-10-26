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
 * An enumerator that describes the state of the stream.
 * <h1>State Transitions</h1>
 * <p>
 * A new stream can start in either {@link #HAS_DATA}, {@link #END_OF_CHUNK} or
 * {@link #EXCEPTION}.
 * <p>
 * <pre>
 * {@code
 * 
 *        +--------+
 * +------+HAS DATA<-----+
 * |      +--------+     |
 * |          |          |
 * |          +----------^
 * |          |          |
 * |    +-----v------+   |
 * v----+END OF CHUNK+---^
 * |    +------------+   |
 * |          |          |
 * |      +---v---+      |
 * |      |WAITING+------+
 * |      +-------+
 * |          |
 * |    +-----v-------+
 * +---->END OF STREAM|
 * |    +-------------+
 * |
 * |    +-------------+
 * +---->  EXCEPTION  |
 *      +-------------+
 * 
 * Courtesy: http://asciiflow.com/
 * }
 * </pre>
 * <p>
 * From {@link #HAS_DATA} the state can transition to {@link #HAS_DATA}, 
 * {@link #END_OF_CHUNK}, {@link #END_OF_STREAM} or {@link #EXCEPTION}.
 * <p>
 * From {@link #END_OF_CHUNK} the state must transition to {@link #WAITING} if
 * the implementation has made an asynchronous call to check for more data. If
 * the implementation does not need to make an async call, it can transition to
 * {@link #HAS_DATA}, {@link #END_OF_STREAM} or {@link #EXCEPTION}.
 * <p> 
 * {@link #END_OF_STREAM} is a termination state. No more data can be read from
 * the stream and it can be closed.
 * <p>
 * {@link #EXCEPTION} is a termination state. An unrecoverable problem occurred
 * and the stream must be closed. Another stream can be created to retry the
 * operation.
 * 
 * @since 3.0
 */
public enum StreamStatus {
  /** The stream has more data in memory ready for processing. */
  HAS_DATA,
  
  /** The stream does not have any more data in memory but there may be more
   * at the source. Call {@code fetch()} on the iterator to check for and load
   * additional data. */
  END_OF_CHUNK,
  
  /** The stream is waiting on an asynchronous call for more data from the 
   * source. */
  WAITING,
  
  /** A terminal condition that indicates no more data is available for this
   * stream and it can be closed. */
  END_OF_STREAM,
  
  /** A terminal condition that indicates that an unrecoverable exception occurred
   * and the stream must be closed. A new stream can be instantiated for a retry. */
  EXCEPTION
}
