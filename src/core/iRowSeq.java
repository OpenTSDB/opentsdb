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
package net.opentsdb.core;

import org.hbase.async.KeyValue;

/**
 * An interface that defines methods implemented by row sequence implementations
 * that represent a row of data in storage.
 * @since 2.4
 */
public interface iRowSeq extends DataPoints {

  /**
   * Sets the initial column in the sequence. The row must be empty and this 
   * must be called before {@link #addRow(KeyValue)}.
   * @param row A non-null KeyValue representing a column in the row.
   * @throws IllegalStateException if {@link #setRow(KeyValue)} or 
   * {@link #addRow(KeyValue)} has already been called.
   */
  public void setRow(final KeyValue row);
  
  /**
   * Adds a column in the proper sequence in the row. Must be called after
   * {@link #setRow(KeyValue)} has been called.
   * @param row A non-null KeyValue representing a column in the row.
   * @throws IllegalStateException if {@link #setRow(KeyValue)} has not been
   * called first.
   */
  public void addRow(final KeyValue row);
  
  /**
   * Returns the row key this sequence represents. May be null if 
   * {@link #setRow(KeyValue)} has not been called.
   * @return The row key for this sequence.
   */
  public byte[] key();
  
  /**
   * Returns the base time for the row in Unix epoch seconds.
   * @return The base time for the row.
   * @throws NullPointerException if {@link #setRow(KeyValue)} has not been 
   * called.
   */
  public long baseTime();
  
  /** @return an internal iterator for this row sequence. */
  public Iterator internalIterator();
  
  /**
   * An interface for an iterator that all row sequences must implement.
   */
  public interface Iterator extends SeekableView, DataPoint {
    
  }
}
