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

import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;

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
  public TemporalAmount interval();
  
  /**
   * The units of time the interval represents.
   * @return A non-null unit.
   */
  public ChronoUnit units();
  
  /**
   * An optional timezone for use when aligning on calendar boundaries.
   * @return A non-null timezone defaulting to UTC.
   */
  public ZoneId timezone();
  
  /**
   * A helper that updates the given timestamp with a new time based on the
   * interval offset from the start time. E.g. an offset of zero would set the
   * time at {@link #start()}. An offset of 1 would set the time at {@link #start()}
   * + {@link #interval()}.
   * @param offset A zero or greater interval offset.
   * @param timestamp A non-null timestamp to update with the new time.
   * @throws IllegalArgumentException if the timestamp was null or offset was
   * less than zero.
   */
  public void updateTimestamp(final int offset, final TimeStamp timestamp);
  
  /**
   * Increments the given timestamp by the {@link #interval()}.
   * @param timestamp A non-null timestamp to update with the new time.
   * @throws IllegalArgumentException if the timestamp was null
   */
  public void nextTimestamp(final TimeStamp timestamp);
}
