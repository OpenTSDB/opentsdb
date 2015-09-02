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
package net.opentsdb.utils;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.ThreadNameDeterminer;

/**
 * Utilities dealing with threads, timers and the like.
 */
public class Threads {
  /** Used to count HashedWheelTimers */
  final static AtomicInteger TIMER_ID = new AtomicInteger();
  
  /** Helps give useful names to the Netty threads */
  public static class BossThreadNamer implements ThreadNameDeterminer {
    final static AtomicInteger tid = new AtomicInteger();
    @Override
    public String determineThreadName(String currentThreadName,
        String proposedThreadName) throws Exception {
      return "OpenTSDB I/O Boss #" + tid.incrementAndGet();
    }
  }
  
  /** Helps give useful names to the Netty threads */
  public static class WorkerThreadNamer implements ThreadNameDeterminer {
    final static AtomicInteger tid = new AtomicInteger();
    @Override
    public String determineThreadName(String currentThreadName,
        String proposedThreadName) throws Exception {
      return "OpenTSDB I/O Worker #" + tid.incrementAndGet();
    }
  }
  
  /** Simple prepends "OpenTSDB" to all threads */
  public static class PrependThreadNamer implements ThreadNameDeterminer {
    @Override
    public String determineThreadName(String currentThreadName, String proposedThreadName)
        throws Exception {
      return "OpenTSDB " + proposedThreadName;
    }
  }
  
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
    class TimerThreadNamer implements ThreadNameDeterminer {
      @Override
      public String determineThreadName(String currentThreadName,
          String proposedThreadName) throws Exception {
        return "OpenTSDB Timer " + name + " #" + TIMER_ID.incrementAndGet();
      }
    }
    return new HashedWheelTimer(Executors.defaultThreadFactory(), 
        new TimerThreadNamer(), ticks, MILLISECONDS, ticks_per_wheel);
  }
}
