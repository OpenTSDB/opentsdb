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
// see <http://www.gnu.org/licenses/>.package net.opentsdb.data;
package net.opentsdb.data;

import java.time.temporal.ChronoUnit;

/**
 * Provides common methods for interacting with timestamps. Implementations can
 * provide as high or low resolution as desired.
 * <p>
 * The {@link #units()} describes what resolution this timestamps is encoded at.
 * Possible values range from {@link ChronoUnit#SECONDS} to {@link ChronoUnit#NANOS}.
 * 
 * @since 3.0
 */
public interface TimeStamp {

  /** Comparator used when evaluating TimeStamp order. */
  public enum TimeStampComparator {
    /** Less Than */
    LT,
    
    /** Less Than 0r Equal To */
    LTE,
    
    /** Greater Than */
    GT,
    
    /** Greater Than or Equal To */
    GTE,
    
    /** Equal To */
    EQ,
    
    /** Not Equal To */
    NE
  }
  
  /**
   * Returns the timestamp in the Unix epoch format with millisecond resolution.
   * @return A Unix epoch timestamp in milliseconds.
   */
  public long msEpoch();

  /**
   * Returns the timestamp in Unix epoch format with second resolution.
   * @return A Unix epoch timestamp in seconds.
   */
  public long epoch();
  
  /**
   * Updates the timestamp to the given Unix epoch timestamp in milliseconds.
   * @param timestamp The timestamp in Unix epoch milliseconds.
   */
  public void updateMsEpoch(final long timestamp);
  
  /**
   * Updates the timestamp to the given Unix epoch timestamp in seconds.
   * @param timestamp The timestamp in Unix epoch seconds.
   */
  public void updateEpoch(final long timestamp);

  /**
   * Resets the value of this timestamp to the given timestamp. Useful to avoid
   * object generation.
   * @param timestamp The timestamp to update to.
   * @throws IllegalArgumentException if the timestamp was null.
   */
  public void update(final TimeStamp timestamp);
  
  /**
   * Provides a deep copy of the timestamp so that updates to this object will 
   * not affect the copy.
   * @return A deep copy of this timestamp.
   */
  public TimeStamp getCopy();
  
  /**
   * Compares this timestamp to the given timestamp using the proper comparator.
   * @param comparator A comparison operator.
   * @param compareTo The timestamp to compare this against.
   * @return True if the comparison was successful, false if not.
   * @throws IllegalArgumentException if either argument was null.
   * @throws UnsupportedOperationException if the comparator was not supported.
   */
  public boolean compare(final TimeStampComparator comparator, 
      final TimeStamp compareTo);
  
  /**
   * Sets the timestamp to the maximum possible value so that when a processor
   * starts to search for the next timestamp, it *should* be less than this 
   * timestamps value.
   */
  public void setMax();
  
  /** @return The base units of time this timestamp was encoded with. */
  public ChronoUnit units();
  
  /** @return The raw timestamp in {@link #units()}. */
  public long timestamp();
}
