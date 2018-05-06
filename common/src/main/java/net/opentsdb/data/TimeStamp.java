// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.data;

import java.time.DayOfWeek;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.util.Comparator;

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

  /** A singleton instance of the TimeStampComparator. */
  public static TimeStampComparator COMPARATOR = new TimeStampComparator();
  
  /** Comparator used when evaluating TimeStamp order. */
  public enum Op {
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
  
  /** @return The number of nanoseconds past the start of the epoch. 
   * May be zero. */
  public long nanos();
  
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
   * Resets the vaue of this timestamp using the given epoch (in seconds) and
   * nanosecond offset.
   * @param epoch The Unix Epoch time in seconds.
   * @param nano The nanosecond offset.
   */
  public void update(final long epoch, final long nano);
  
  /**
   * Provides a deep copy of the timestamp so that updates to this object will 
   * not affect the copy.
   * @return A deep copy of this timestamp.
   */
  public TimeStamp getCopy();
  
  /** @return The non-null zone ID. Should default to UTC if no zone was set. */
  public ZoneId timezone();
  
  /**
   * Compares this timestamp to the given timestamp using the proper comparator.
   * @param comparator A comparison operator.
   * @param compareTo The timestamp to compare this against.
   * @return True if the comparison was successful, false if not.
   * @throws IllegalArgumentException if either argument was null.
   * @throws UnsupportedOperationException if the comparator was not supported.
   */
  public boolean compare(final Op comparator, 
      final TimeStamp compareTo);
  
  /**
   * Sets the timestamp to the maximum possible value so that when a processor
   * starts to search for the next timestamp, it *should* be less than this 
   * timestamps value.
   */
  public void setMax();
  
  /** @return The base units of time this timestamp was encoded with. */
  public ChronoUnit units();
  
  /**
   * @param amount A non-null duration to add to the current timestamp, 
   * accounting for time zone, DST, leap years and leap seconds when applicable.
   */
  public void add(final TemporalAmount amount);
  
  /**
   * Moves to the timestamp to a well defined time at the start of an interval
   * given a unit of time and number of those units. For example, an interval
   * of 60 {@link ChronoUnit#SECONDS} would snap to the top of the minute, 
   * setting the seconds and nanos to 0. For snaps to the start of the week,
   * the week starts on {@link DayOfWeek#SUNDAY}.
   * @param interval A value of 1 or greater representing the number of units
   * in the interval. 
   * @param unit A non-null unit between {@link ChronoUnit#NANOS} and 
   * {@link ChronoUnit#YEARS} excluding {@link ChronoUnit#HALF_DAYS}.
   */
  public void snapToPreviousInterval(final long interval, final ChronoUnit unit);
  
  /**
   * Moves to the timestamp to a well defined time at the start of an interval
   * given a unit of time and number of those units. For example, an interval
   * of 60 {@link ChronoUnit#SECONDS} would snap to the top of the minute, 
   * setting the seconds and nanos to 0.
   * @param interval A value of 1 or greater representing the number of units
   * in the interval. 
   * @param unit A non-null unit between {@link ChronoUnit#NANOS} and 
   * {@link ChronoUnit#YEARS} excluding {@link ChronoUnit#HALF_DAYS}.
   * @param day_of_week The starting day of a weeky boundary when the interval
   * would snap to the top of a week. For example in the US, weeks start on
   * {@link DayOfWeek#SUNDAY}.
   */
  public void snapToPreviousInterval(final long interval, 
                                     final ChronoUnit unit, 
                                     final DayOfWeek day_of_week);

  /**
   * A comparator to evaluate timestamp ordering. Accepts nulls.
   */
  public static class TimeStampComparator implements Comparator<TimeStamp> {

    @Override
    public int compare(final TimeStamp v1, final TimeStamp v2) {
      if (v1 == null && v2 == null) {
        return 0;
      }
      if (v1 != null && v2 == null) {
        return -1;
      }
      if (v1 == null && v2 != null) {
        return 1;
      }
      if (v1 == v2) {
        return 0;
      }
      if (v1.compare(Op.EQ, v2)) {
        return 0;
      }
      return v1.compare(Op.LT, v2) ? -1 : 1;
    }

  }
}
