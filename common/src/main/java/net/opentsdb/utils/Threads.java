// This file is part of OpenTSDB.
// Copyright (C) 2015-2017  The OpenTSDB Authors.
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
