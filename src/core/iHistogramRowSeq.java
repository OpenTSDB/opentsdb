// This file is part of OpenTSDB.
// Copyright (C) 2016-2017  The OpenTSDB Authors.
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

import java.util.List;

/**
 * Clone of the {@link iRowSeq} interface but for histograms.
 *  
 * @since 2.4
 */
public interface iHistogramRowSeq extends HistogramDataPoints {
  
  /**
   * Sets the initial column in the sequence. The key cannot be empty.
   * @param key The row key.
   * @param row A non-null list of histogram points.
   * @throws IllegalStateException if {@link #setRow(byte[], List)} or 
   * {@link #addRow(List)} has already been called.
   */
  public void setRow(final byte[] key, final List<HistogramDataPoint> row);

  /**
   * Adds a column in the proper sequence in the row. Must be called after
   * {@link #setRow(byte[], List)} has been called.
   * @param row A non-null list of histogram points.
   * @throws IllegalStateException if {@link #setRow(byte[], List)} has not been
   * called first.
   */
  public void addRow(final List<HistogramDataPoint> row);

  /**
   * Returns the row key this sequence represents. May be null if 
   * {@link #setRow(byte[], List)} has not been called.
   * @return The row key for this sequence.
   */
  public byte[] key();

  /**
   * Returns the base time for the row in Unix epoch seconds.
   * @return The base time for the row.
   * @throws NullPointerException if {@link #setRow(byte[], List)} has not been 
   * called.
   */
  public long baseTime();

  /** @return an internal iterator for this row sequence. */
  public Iterator internalIterator();

  /**
   * An interface for an iterator that all row sequences must implement.
   */
  public interface Iterator extends HistogramSeekableView, HistogramDataPoint {

  }
}
