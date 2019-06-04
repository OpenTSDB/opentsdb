// This file is part of OpenTSDB.
// Copyright (C) 2018-2019  The OpenTSDB Authors.
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
package net.opentsdb.stats;

import java.time.temporal.ChronoUnit;

import net.opentsdb.core.TSDBPlugin;

/**
 * A metric collection class.
 * 
 * WIP
 * 
 * @since 3.0
 */
public interface StatsCollector extends TSDBPlugin {

  /**
   * Increments a monotonically increasing counter by one.
   * @param metric The non-null and non-empty metric name.
   * @param tags An optional set of tag key, value, key, value pairs.
   */
  public void incrementCounter(final String metric, 
                               final String... tags);
  
  /**
   * Adds the given positive amount to a monotonically increasing counter.
   * @param metric The non-null and non-empty metric name.
   * @param amount The amount to add.
   * @param tags An optional set of tag key, value, key, value pairs.
   */
  public void incrementCounter(final String metric, 
                               final long amount, 
                               final String... tags);
  
  /**
   * Sets the gauge value.
   * @param metric The non-null and non-empty metric name.
   * @param value Gauge value.
   * @param tags An optional set of tag key, value, key, value pairs.
   */
  public void setGauge(final String metric, 
                       final long value, 
                       final String... tags);
  
  /**
   * Sets the gauge value.
   * @param metric The non-null and non-empty metric name.
   * @param value Gauge value.
   * @param tags An optional set of tag key, value, key, value pairs.
   */
  public void setGauge(final String metric, 
                       final double value, 
                       final String... tags);
  
  /**
   * Configures and returns a timer to measure a latency. Starts the clock
   * on the timer on return.
   * @param metric The non-null and non-empty metric name.
   * @param units The units to track the time in.
   * @return A non-null timer.
   */
  public StatsTimer startTimer(final String metric, final ChronoUnit units);
  
  /**
   * Similar to {@link #startTimer(String, boolean)} but used when the duration
   * has already been measured and we can't track the start and end times.
   * @param metric The non-null and non-empty metric name.
   * @param duration The numeric duration.
   * @param units The units of the duration.
   * @param histo Whether or not to track the latency in a histogram.
   * @param tags An optional set of tag key, value, key, value pairs.
   */
  public void addTime(final String metric, 
                      final long duration, 
                      final ChronoUnit units,
                      final String... tags);
  
  /**
   * A latency tracking class.
   */
  public interface StatsTimer {
    
    /**
     * Stops and records the latency in the timer.
     * @param tags An optional set of tag key, value, key, value pairs.
     */
    public void stop(final String... tags);
    
    /** @return The start time in the given time units.. */
    public long startTime();
    
    /** @return The units the time is measured in. */
    public ChronoUnit units();
  }
}
