// This file is part of OpenTSDB.
// Copyright (C) 2015-2017  The OpenTSDB Authors.
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
package net.opentsdb.utils;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.concurrent.atomic.AtomicInteger;

import io.netty.util.HashedWheelTimer;
import io.netty.util.concurrent.DefaultThreadFactory;

/**
 * Utilities dealing with threads, timers and the like.
 */
public class Threads {
  /** Used to count HashedWheelTimers */
  final static AtomicInteger TIMER_ID = new AtomicInteger();

  /**
   * Returns a new HashedWheelTimer with a name and default ticks
   * @param name The name to add to the thread name
   * @return A timer
   */
  public static HashedWheelTimer newTimer(final String name) {
    return newTimer(100, name);
  }
  
  /**
   * Returns a new HashedWheelTimer with a name and default ticks
   * @param ticks How many ticks per second to sleep between executions, in ms
   * @param name The name to add to the thread name
   * @return A timer
   */
  public static HashedWheelTimer newTimer(final int ticks, final String name) {
    return newTimer(ticks, 512, name);
  }
  
  /**
   * Returns a new HashedWheelTimer with a name and default ticks
   * @param ticks How many ticks per second to sleep between executions, in ms
   * @param ticks_per_wheel The size of the wheel
   * @param name The name to add to the thread name
   * @return A timer
   */
  public static HashedWheelTimer newTimer(final int ticks, 
      final int ticks_per_wheel, final String name) {
    return new HashedWheelTimer(new DefaultThreadFactory(name), 
        ticks, MILLISECONDS, ticks_per_wheel);
  }
  
}
