// This file is part of OpenTSDB.
// Copyright (C) 2016  The OpenTSDB Authors.
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
package net.opentsdb.core;

import java.util.Map;

import com.stumbleupon.async.Deferred;

import net.opentsdb.stats.StatsCollector;

/**
 * A filter that can determine whether or not time series should be allowed 
 * assignment based on their metric and tags. This is useful for such 
 * situations as:
 * <ul><li>Enforcing naming standards</li>
 * <li>Blacklisting certain names or properties</li>
 * <li>Preventing cardinality explosions</li></ul>
 * <b>Note:</b> Implementations must have a parameterless constructor. The 
 * {@link #initialize(TSDB)} method will be called immediately after the plugin is
 * instantiated and before any other methods are called.
 * @since 2.3
 */
public abstract class WriteableDataPointFilterPlugin {
  
  /**
   * Called by TSDB to initialize the plugin
   * Implementations are responsible for setting up any IO they need as well
   * as starting any required background threads.
   * <b>Note:</b> Implementations should throw exceptions if they can't start
   * up properly. The TSD will then shutdown so the operator can fix the 
   * problem. Please use IllegalArgumentException for configuration issues.
   * @param tsdb The parent TSDB object
   * @throws IllegalArgumentException if required configuration parameters are 
   * missing
   * @throws Exception if something else goes wrong
   */
  public abstract void initialize(final TSDB tsdb);

  /**
   * Called to gracefully shutdown the plugin. Implementations should close 
   * any IO they have open
   * @return A deferred object that indicates the completion of the request.
   * The {@link Object} has not special meaning and can be {@code null}
   * (think of it as {@code Deferred<Void>}).
   */
  public abstract Deferred<Object> shutdown();
  
  /**
   * Should return the version of this plugin in the format:
   * MAJOR.MINOR.MAINT, e.g. "2.3.1". The MAJOR version should match the major
   * version of OpenTSDB the plugin is meant to work with.
   * @return A version string used to log the loaded version
   */
  public abstract String version();
  
  /**
   * Called by the TSD when a request for statistics collection has come in. The
   * implementation may provide one or more statistics. If no statistics are
   * available for the implementation, simply stub the method.
   * @param collector The collector used for emitting statistics
   */
  public abstract void collectStats(final StatsCollector collector);
  
  /**
   * Determine whether or not the data point should be stored.
   * If the data should not be stored, the implementation can return false or an 
   * exception in the deferred object. Otherwise it should return true and the
   * data point will be written to storage.
   * @param metric The metric name for the data point
   * @param timestamp The timestamp of the data
   * @param value The value encoded as either an integer or floating point value
   * @param tags The tags associated with the data point
   * @param flags Encoding flags for the value
   * @return True if the data should be written, false if it should be rejected.
   */
  public abstract Deferred<Boolean> allowDataPoint(
      final String metric,
      final long timestamp,
      final byte[] value,
      final Map<String, String> tags,
      final short flags);
  
  /**
   * Whether or not the filter should process data points.
   * @return False if {@link #allowDataPoint(String, long, byte[], Map, short)}
   * should NOT be called, true if it should.
   */
  public abstract boolean filterDataPoints();
}
