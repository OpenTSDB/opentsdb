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
package net.opentsdb.stats;

import com.stumbleupon.async.Deferred;
import net.opentsdb.utils.Config;

/**
 * Used to calculate latency stats for a given operation. May work synchronously by collecting measurements and
 * recording directly into a {@link StatsCollector} or may emit events to an external system which writes back
 * via TSDB write channels (e.g. Telnet or HTTP).
 */
public abstract class LatencyStatsPlugin {

  /**
   * Called by TSDB to initialize the plugin
   * Implementations are responsible for setting up any IO they need as well
   * as starting any required background threads.
   * <b>Note:</b> Implementations should throw exceptions if they can't start
   * up properly. The TSD will then shutdown so the operator can fix the
   * problem. Please use IllegalArgumentException for configuration issues.
   *
   * @param config The TSDB configuration
   * @throws IllegalArgumentException if required configuration parameters are
   *                                  missing
   */
  public abstract void initialize(final Config config);

  /**
   * Called when this plugin is live. Calls to {@link #add(int)} will not be made
   * before this method is called. Under race conditions it's possible this method will never
   * be called on a given instance of this class.
   */
  public abstract void start();

  /**
   * Called when the TSD is shutting down to gracefully flush any buffers or
   * close open connections.
   */
  public abstract Deferred<Object> shutdown();

  /**
   * Should return the version of this plugin in the format:
   * MAJOR.MINOR.MAINT, e.g. 2.0.1. The MAJOR version should match the major
   * version of OpenTSDB the plugin is meant to work with.
   *
   * @return A version string used to log the loaded version
   */
  public abstract String version();

  /**
   * Called by the TSD when a request for statistics collection has come in. The
   * implementation may provide one or more statistics. If no statistics are
   * available for the implementation, simply stub the method. This method is responsible
   * both for collecting stats about the plugin, as well as emitting aggregations of the
   * measurements it has been collecting
   *
   * @param collector  The collector used for emitting statistics
   * @param metricName The name of the metric to emit aggregations to (should not be used for internal plugin metrics)
   * @param xtratag    Extra tags to use when emitting aggregations (should not be used for internal plugin metrics)
   */
  public abstract void collectStats(final StatsCollector collector, String metricName, String xtratag);

  /**
   * Adds a value to be measured.
   *
   * @param value The value to add.
   * @throws IllegalArgumentException may be thrown if the value given is negative.
   */
  public abstract void add(final int value);
}
