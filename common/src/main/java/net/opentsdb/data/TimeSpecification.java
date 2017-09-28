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
package net.opentsdb.data;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

/**
 * This interface is used with downsampled query results as a means of saving
 * memory and bandwidth by collapsing individual, regularly space timestamps 
 * into a calculation. For example, if a query returns a years worth of values
 * spaced every 60 seconds, it doesn't make sense to emit a timestamp with 
 * every data point when we can simply calculate the proper time given the 
 * value offset.
 * 
 * @since 3.0
 */
public interface TimeSpecification {
  
  /**
   * The start timestamp of the interval bound by this specification, inclusive.
   * @return A non-null timestamp object.
   */
  public TimeStamp start();
  
  /**
   * The end timestamp of the interval bound by this specification, inclusive.
   * @return A non-null timestamp object.
   */
  public TimeStamp end();
  
  /**
   * The interval between timestamps in {@link #units()}. Must be a value greater
   * than zero.
   * @return The non-null interval between timestamps.
   */
  public Duration interval();
  
  /**
   * The total number of intervals within this specification, including start
   * and end time stamps. Must be greater than zero.
   * @return The number of intervals in the specification.
   */
  public int intervals();
  
  /**
   * The units of time the interval represents.
   * @return A non-null unit.
   */
  public ChronoUnit units();
  
  /**
   * A helper that updates the given timestamp with a new time based on the
   * interval offset from the start time. E.g. an offset of zero would set the
   * time at {@link #start()}. An offset of 1 would set the time at {@link #start()}
   * + {@link #interval()}.
   * @param offset A zero or greater interval offset.
   * @param timestamp A non-null timestamp to update with the new time.
   */
  public void updateTimestamp(final int offset, final TimeStamp timestamp);
}
