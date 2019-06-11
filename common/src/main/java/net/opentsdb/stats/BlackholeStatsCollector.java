// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
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

import net.opentsdb.core.BaseTSDBPlugin;

/**
 * A default implementation of the stats collector that simply ignores
 * the measurements. This can be used as the default as we guarantee not
 * to return a null collector object to measurements.
 * 
 * @since 3.0
 */
public class BlackholeStatsCollector extends BaseTSDBPlugin implements 
    StatsCollector {

  public static final String TYPE = "BlackholeStatsCollector";
  
  @Override
  public String type() {
    return TYPE;
  }
  
  @Override
  public String version() {
    return "3.0.0";
  }

  @Override
  public void incrementCounter(final String metric, final String... tags) {
    // Muahaha!
  }

  @Override
  public void incrementCounter(final String metric, 
                               final long amount, 
                               final String... tags) {
    // Muahaha!
  }

  @Override
  public void setGauge(final String metric, 
                       final long value, 
                       final String... tags) {
    // Muahaha!
  }

  @Override
  public void setGauge(final String metric, 
                       final double value, 
                       final String... tags) {
    // Muahaha!
  }

  @Override
  public StatsTimer startTimer(final String metric, final ChronoUnit units) {
    return TMR;
  }

  public void addTime(final String metric, 
                      final long duration, 
                      final ChronoUnit units,
                      final String... tags) {
    // Asta la vista!
  }
  
  static class BlackholeTimer implements StatsTimer {
    @Override
    public void stop(final String... tags) {
      // Muahaha!
    }

    @Override
    public long startTime() {
      return 0;
    }
    
    @Override
    public ChronoUnit units() {
      return ChronoUnit.MILLIS;
    }
  }
  private static final BlackholeTimer TMR = new BlackholeTimer();
}
